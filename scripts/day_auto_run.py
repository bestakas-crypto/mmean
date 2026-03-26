# MMEAN/day_auto_run.py
"""
정규장 시뮬레이션 자동화 루틴 (day 버전)

night_auto_run.py 와 구조 동일.
차이점:
    - DaySimRunner 사용
    - 정규장 세션 날짜 탐지 (08:45~15:35)
    - 기본 study prefix = "day_auto"

사용 예:
    python day_auto_run.py                          # 무한 루프
    python day_auto_run.py --loops 3                # 3회
    python day_auto_run.py --dry-run --loops 1      # 계획만 출력
    python day_auto_run.py --skip-random --loops 2  # random 건너뜀
"""
from __future__ import annotations

import os, sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import path_setup  # noqa: F401

import argparse
import logging
import sqlite3
import sys
import time
from datetime import datetime, timedelta
from typing import Dict, List, Tuple

# ── night_auto_run 에서 공용 구조 임포트 ─────────────────────────────
from night_auto_run import (
    AutoRunConfig,
    _split_train_valid,
    _next_study_no,
    _print_loop_summary,
    _print_final_summary,
)

from pathlib import Path

log = logging.getLogger("MMEAN.DayAutoRun")

_PROJ    = Path(__file__).resolve().parent.parent
_STORAGE = _PROJ / "storage"
_DEFAULT_SOURCE_DB = str(_STORAGE / "mmean.db")
_DEFAULT_SIM_DB    = str(_STORAGE / "sim.db")
_DEFAULT_DAY_JSON  = str(_PROJ / "workspace" / "day_levels.json")


# ─────────────────────────────────────────────────────────────────────
# 정규장 유효 날짜 탐지
# ─────────────────────────────────────────────────────────────────────

def _detect_valid_session_dates(source_db: str, n_total: int) -> List[str]:
    """
    regime_ticks 에서 정규장 데이터가 있는 최근 n_total 개 날짜 반환.
    정규장 조건: 08:45:00 ~ 15:35:00 구간에 틱이 존재하는 날짜.
    반환: 오름차순 ['2026-03-01', ...]
    """
    try:
        conn = sqlite3.connect(
            f"file:{source_db}?mode=ro", uri=True, timeout=30
        )
        rows = conn.execute("""
            SELECT DISTINCT date(ts) AS session_date
            FROM   regime_ticks
            WHERE  time(ts) >= '08:45:00' AND time(ts) < '15:35:00'
            ORDER  BY session_date DESC
            LIMIT  ?
        """, (n_total,)).fetchall()
        conn.close()
        dates = sorted(r[0] for r in rows if r[0])
        log.info("유효 날짜 탐지(day): %d일 (요청 %d일)", len(dates), n_total)
        return dates
    except Exception as exc:
        log.error("유효 날짜 탐지 실패: %s", exc)
        return []


# ─────────────────────────────────────────────────────────────────────
# Day 자동 루너
# ─────────────────────────────────────────────────────────────────────

class DayAutoRunner:
    """DaySimRunner 를 직접 호출하는 정규장 자동화 루너."""

    def __init__(self, cfg: AutoRunConfig):
        self.cfg              = cfg
        self._stop_requested  = False

        if not cfg.dry_run:
            from day_sim import DaySimRunner
            self._runner = DaySimRunner(
                source_db = cfg.source_db,
                sim_db    = cfg.sim_db,
                day_json  = cfg.day_json,   # AutoRunConfig.day_json 전용 필드
            )
        else:
            self._runner = None

    # ── 단계 실행 헬퍼 ────────────────────────────────────────────

    def _run_phase(
        self,
        phase:    str,
        fn,
        loop_log: List[Dict],
        **kwargs,
    ) -> bool:
        if self.cfg.dry_run:
            print(f"  [DRY-RUN] {phase}  args={kwargs}")
            loop_log.append({"phase": phase, "status": "dry-run"})
            return True

        started = datetime.now()
        print(
            f"\n  [{phase}] 시작 @ {started.strftime('%H:%M:%S')}  "
            + "  ".join(f"{k}={v}" for k, v in kwargs.items()),
            flush=True,
        )
        try:
            result  = fn(**kwargs)
            elapsed = (datetime.now() - started).total_seconds()
            loop_log.append({
                "phase":   phase,
                "status":  "ok",
                "elapsed": elapsed,
                "result":  result,
            })
            print(f"  [{phase}] 완료 ({elapsed:.0f}s)", flush=True)
            return True
        except KeyboardInterrupt:
            self._stop_requested = True
            loop_log.append({"phase": phase, "status": "interrupted"})
            raise
        except Exception as exc:
            elapsed = (datetime.now() - started).total_seconds()
            loop_log.append({
                "phase":   phase,
                "status":  "error",
                "elapsed": elapsed,
                "error":   str(exc),
            })
            log.error("[%s] 실패: %s", phase, exc, exc_info=True)
            print(f"  [{phase}] 오류 (건너뜀): {exc}", flush=True)
            return False

    # ── 루프 1회 ──────────────────────────────────────────────────

    def run_loop(self, loop_no: int) -> Dict:
        cfg      = self.cfg
        loop_log: List[Dict] = []

        # ── 날짜 결정 (직접 지정 우선, 없으면 자동 탐지) ────────
        if cfg.train_dates_override or cfg.valid_dates_override:
            train_dates = sorted(cfg.train_dates_override or [])
            valid_dates = sorted(cfg.valid_dates_override or [])
            log.info("날짜 직접 지정 | train=%d valid=%d",
                     len(train_dates), len(valid_dates))
        else:
            n_total   = cfg.n_train_days + cfg.n_valid_days
            all_dates = _detect_valid_session_dates(cfg.source_db, n_total)
            if not all_dates:
                print("  [ERROR] 유효 날짜 없음 — regime_ticks 확인 필요", flush=True)
                return {"loop_no": loop_no, "error": "no_dates"}
            train_dates, valid_dates = _split_train_valid(all_dates, cfg.n_valid_days)

        if not train_dates:
            print("  [ERROR] train 날짜 부족", flush=True)
            return {"loop_no": loop_no, "error": "no_train_dates"}

        # ── study 이름 채번 ──────────────────────────────────────
        study_no   = _next_study_no(cfg.sim_db, cfg.study_prefix)
        study_name = f"{cfg.study_prefix}_{study_no:03d}"
        seed       = cfg.seed_base + loop_no

        print(
            f"\n  study={study_name}  seed={seed}\n"
            f"  train={train_dates}  ({len(train_dates)}일)\n"
            f"  valid={valid_dates}  ({len(valid_dates)}일)",
            flush=True,
        )

        r = self._runner

        train_session_date = train_dates[0] if train_dates else None

        # ── 1. random 탐색 ────────────────────────────────────────
        if cfg.do_random and cfg.random_trials > 0:
            self._run_phase(
                "random", r.run_random_search, loop_log,
                dates        = train_dates,
                n_trials     = cfg.random_trials,
                study_name   = study_name,
                seed         = seed,
                force        = cfg.force,
                session_date = train_session_date,
                batch_no     = loop_no,
            )

        # ── 2. Bayes 최적화 ───────────────────────────────────────
        if cfg.do_bayes and cfg.bayes_trials > 0:
            self._run_phase(
                "bayes", r.run_bayes_opt, loop_log,
                dates            = train_dates,
                n_trials         = cfg.bayes_trials,
                study_name       = study_name,
                seed             = seed,
                force            = cfg.force,
                n_startup_trials = cfg.bayes_startup,
                session_date     = train_session_date,
                batch_no         = loop_no,
            )

        # ── 3. Validation ─────────────────────────────────────────
        if cfg.do_validate and valid_dates:
            self._run_phase(
                "validate", r.run_validation, loop_log,
                study_name  = study_name,
                valid_dates = valid_dates,
                top_n       = cfg.validate_top_n,
                force       = cfg.force,
            )

        # ── 4. adopt_best ─────────────────────────────────────────
        if cfg.do_adopt and valid_dates:
            self._run_phase(
                "adopt", r.adopt_best, loop_log,
                study_name  = study_name,
                valid_dates = valid_dates,
                top_n       = cfg.validate_top_n,
                force       = cfg.force,
            )

        # ── 5. slippage stress ────────────────────────────────────
        if cfg.do_stress and valid_dates:
            self._run_phase(
                "stress", r.run_slippage_stress, loop_log,
                study_name  = study_name,
                valid_dates = valid_dates,
                force       = cfg.force,
            )

        # ── 6. cluster ────────────────────────────────────────────
        if cfg.do_cluster:
            self._run_phase(
                "cluster", r.cluster_top_configs, loop_log,
                study_name = study_name,
                n_clusters = cfg.cluster_k,
            )

        _print_loop_summary(loop_no, study_name, train_dates, valid_dates, loop_log)

        return {
            "loop_no":     loop_no,
            "study_name":  study_name,
            "train_dates": train_dates,
            "valid_dates": valid_dates,
            "loop_log":    loop_log,
        }

    # ── 메인 루프 진입점 ──────────────────────────────────────────

    def run(self) -> None:
        cfg        = self.cfg
        loop_no    = 0
        all_results: List[Dict] = []
        inf        = (cfg.n_loops == 0)
        total_str  = "∞" if inf else str(cfg.n_loops)

        print(f"\n{'='*72}")
        print(f"  MMEAN Day Auto-Run  (정규장)")
        print(f"  loops={total_str}  random={cfg.random_trials}  bayes={cfg.bayes_trials}")
        if cfg.train_dates_override or cfg.valid_dates_override:
            td = cfg.train_dates_override or []
            vd = cfg.valid_dates_override or []
            print(f"  train=직접지정 {len(td)}일  valid=직접지정 {len(vd)}일"
                  f"  cluster_k={cfg.cluster_k}")
            if td:
                print(f"  train_dates: {td[0]} ~ {td[-1]}")
            if vd:
                print(f"  valid_dates: {vd[0]} ~ {vd[-1]}")
        else:
            print(f"  train=자동 {cfg.n_train_days}일  valid=자동 {cfg.n_valid_days}일"
                  f"  cluster_k={cfg.cluster_k}")
        print(f"  prefix={cfg.study_prefix}  seed_base={cfg.seed_base}")
        if cfg.dry_run:
            print("  *** DRY-RUN 모드: 실제 실행 없음 ***")
        print(f"{'='*72}\n")

        try:
            while inf or loop_no < cfg.n_loops:
                loop_no += 1
                start = datetime.now()
                print(
                    f"\n{'─'*72}\n"
                    f"  LOOP {loop_no}/{total_str}  @  {start.strftime('%Y-%m-%d %H:%M:%S')}\n"
                    f"{'─'*72}",
                    flush=True,
                )
                result = self.run_loop(loop_no)
                all_results.append(result)

                if self._stop_requested:
                    break

                if cfg.loop_sleep_sec > 0 and (inf or loop_no < cfg.n_loops):
                    until = datetime.now() + timedelta(seconds=cfg.loop_sleep_sec)
                    print(
                        f"\n  다음 루프까지 {cfg.loop_sleep_sec}초 대기 "
                        f"(~{until.strftime('%H:%M:%S')})  Ctrl+C 로 중단",
                        flush=True,
                    )
                    try:
                        time.sleep(cfg.loop_sleep_sec)
                    except KeyboardInterrupt:
                        print("\n  대기 중 중단됨.", flush=True)
                        break

        except KeyboardInterrupt:
            print("\n\n  Ctrl+C 감지 — 종료합니다.", flush=True)

        _print_final_summary(all_results)


# ─────────────────────────────────────────────────────────────────────
# CLI
# ─────────────────────────────────────────────────────────────────────

def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="MMEAN 정규장 시뮬레이션 자동화 루틴",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )

    p.add_argument("--source-db",  default=_DEFAULT_SOURCE_DB,  metavar="PATH")
    p.add_argument("--sim-db",     default=_DEFAULT_SIM_DB,     metavar="PATH")
    p.add_argument("--day-json",   default=_DEFAULT_DAY_JSON,   metavar="PATH",
                   help="day_levels.json 경로")

    p.add_argument("--train",  type=int, default=15, metavar="N",
                   help="탐색 날짜 수 (--train-dates 지정 시 무시)")
    p.add_argument("--valid",  type=int, default=5,  metavar="N",
                   help="검증 날짜 수 (--valid-dates 지정 시 무시)")
    p.add_argument("--train-dates", nargs="+", metavar="YYYY-MM-DD", default=None,
                   help="train 날짜 직접 지정")
    p.add_argument("--valid-dates", nargs="+", metavar="YYYY-MM-DD", default=None,
                   help="valid 날짜 직접 지정")

    p.add_argument("--random",   type=int, default=300, metavar="N")
    p.add_argument("--bayes",    type=int, default=200, metavar="N")
    p.add_argument("--startup",  type=int, default=40,  metavar="N")
    p.add_argument("--top",      type=int, default=10,  metavar="N")
    p.add_argument("--cluster",  type=int, default=5,   metavar="K")

    p.add_argument("--loops",   type=int, default=0,  metavar="N")
    p.add_argument("--sleep",   type=int, default=0,  metavar="SEC")

    p.add_argument("--skip-random",   action="store_true")
    p.add_argument("--skip-bayes",    action="store_true")
    p.add_argument("--skip-validate", action="store_true")
    p.add_argument("--skip-adopt",    action="store_true")
    p.add_argument("--skip-stress",   action="store_true")
    p.add_argument("--skip-cluster",  action="store_true")

    p.add_argument("--prefix",   type=str, default="day_auto", metavar="STR")
    p.add_argument("--seed",     type=int, default=42,         metavar="N")
    p.add_argument("--force",    action="store_true")
    p.add_argument("--dry-run",  action="store_true")
    p.add_argument("--log-level", default="INFO",
                   choices=["DEBUG", "INFO", "WARNING", "ERROR"])

    return p.parse_args()


def main() -> None:
    args = _parse_args()
    logging.basicConfig(
        level  = getattr(logging, args.log_level),
        format = "%(asctime)s [%(levelname)s] %(name)s - %(message)s",
    )

    cfg = AutoRunConfig(
        source_db        = args.source_db,
        sim_db           = args.sim_db,
        day_json         = args.day_json,   # AutoRunConfig.day_json 전용 필드
        n_train_days     = args.train,
        n_valid_days     = args.valid,
        random_trials    = args.random,
        bayes_trials     = args.bayes,
        bayes_startup    = args.startup,
        validate_top_n   = args.top,
        cluster_k        = args.cluster,
        n_loops          = args.loops,
        loop_sleep_sec   = args.sleep,
        study_prefix     = args.prefix,
        seed_base        = args.seed,
        do_random        = not args.skip_random,
        do_bayes         = not args.skip_bayes,
        do_validate      = not args.skip_validate,
        do_adopt         = not args.skip_adopt,
        do_stress        = not args.skip_stress,
        do_cluster       = not args.skip_cluster,
        dry_run              = args.dry_run,
        force                = args.force,
        train_dates_override = args.train_dates or None,
        valid_dates_override = args.valid_dates or None,
    )

    runner = DayAutoRunner(cfg)
    runner.run()


if __name__ == "__main__":
    main()
