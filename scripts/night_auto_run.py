# MMEAN/night_auto_run.py
"""
야간 시뮬레이션 완전 자동화 루틴

흐름 (루프 1회):
    유효 날짜 탐지 → train/valid 분할
    → random 탐색 → Bayes 최적화
    → validation → adopt_best
    → slippage stress → cluster
    → 다음 루프 반복

특징:
    - NightSimRunner 직접 임포트 (os.system 없음)
    - DB에서 유효 날짜 자동 탐지 (regime_ticks 기준)
    - study 이름 자동 채번 (prefix_001, prefix_002, ...)
    - 루프 간 날짜 슬라이딩 윈도우 (최신 N일 자동 갱신)
    - 각 단계 try/except → 실패해도 다음 단계 / 다음 루프 계속
    - Ctrl+C → 현재 루프 완료 후 깔끔하게 종료
    - --dry-run 으로 실제 실행 없이 계획만 출력

사용 예:
    # 기본 (무한 루프, 기본 파라미터)
    python night_auto_run.py

    # 루프 3회, 탐색 300+200, 학습 20일/검증 5일
    python night_auto_run.py --loops 3 --random 300 --bayes 200 --train 20 --valid 5

    # 루프 없이 1회만, prefix 지정
    python night_auto_run.py --loops 1 --prefix myrun

    # 실행 계획만 출력 (DB 변경 없음)
    python night_auto_run.py --dry-run --loops 2
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
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple

from pathlib import Path

log = logging.getLogger("MMEAN.AutoRun")

_PROJ    = Path(__file__).resolve().parent.parent
_STORAGE = _PROJ / "storage"
_DEFAULT_SOURCE_DB  = str(_STORAGE / "mmean.db")
_DEFAULT_SIM_DB     = str(_STORAGE / "sim.db")
_DEFAULT_NIGHT_JSON = str(_STORAGE / "night_levels.json")


# ─────────────────────────────────────────────────────────────────────
# 설정 (dataclass)
# ─────────────────────────────────────────────────────────────────────

@dataclass
class AutoRunConfig:
    # DB / 파일 경로
    source_db:        str  = _DEFAULT_SOURCE_DB
    sim_db:           str  = _DEFAULT_SIM_DB
    night_json:       str  = _DEFAULT_NIGHT_JSON   # 야간장 레벨 JSON (NightSimRunner 전용)
    day_json:         str  = ""                    # 정규장 레벨 JSON (DaySimRunner 전용)

    # 날짜 창 (유효 거래일 기준)
    n_train_days:     int  = 15   # 탐색 날짜 수
    n_valid_days:     int  = 5    # 검증 날짜 수

    # 탐색 파라미터
    random_trials:    int  = 300
    bayes_trials:     int  = 200
    bayes_startup:    int  = 40
    validate_top_n:   int  = 10
    cluster_k:        int  = 5

    # 루프 제어
    n_loops:          int  = 0    # 0 = 무한
    loop_sleep_sec:   int  = 0    # 루프 간 대기 초 (0=즉시)

    # study 명칭
    study_prefix:     str  = "auto"
    seed_base:        int  = 42   # loop i 에서 seed = seed_base + i

    # 단계 선택 (False 로 건너뜀)
    do_random:        bool = True
    do_bayes:         bool = True
    do_validate:      bool = True
    do_adopt:         bool = True
    do_stress:        bool = True
    do_cluster:       bool = True

    # 날짜 직접 지정 (None = n_train_days / n_valid_days 기반 자동 탐지)
    train_dates_override: Optional[List[str]] = None
    valid_dates_override: Optional[List[str]] = None

    # 기타
    dry_run:          bool = False  # True = 계획만 출력, 실행 없음
    force:            bool = False  # 기존 실행 덮어쓰기

    # 내부 상태 (자동 설정됨)
    _phase_log:       List[Dict] = field(default_factory=list, repr=False)


# ─────────────────────────────────────────────────────────────────────
# 유효 거래일 탐지 (regime_ticks 기준)
# ─────────────────────────────────────────────────────────────────────

def _detect_valid_session_dates(
    source_db: str,
    n_total:   int,
) -> List[str]:
    """
    regime_ticks 에서 야간 세션 데이터가 실제로 존재하는
    최근 n_total 개 거래일(session_date) 을 반환.

    야간 세션 조건: 해당 날짜 18:00 이후 틱 존재 여부로 판별.
    반환: 오름차순 정렬 ['2026-03-01', ...]
    """
    try:
        conn = sqlite3.connect(
            f"file:{source_db}?mode=ro", uri=True, timeout=30
        )
        # ts 형식: 'YYYY-MM-DD HH:MM:SS...'
        # time(ts) >= '18:00:00' 인 날짜 → 야간 세션 시작일
        rows = conn.execute("""
            SELECT DISTINCT date(ts) AS session_date
            FROM   regime_ticks
            WHERE  time(ts) >= '18:00:00'
            ORDER  BY session_date DESC
            LIMIT  ?
        """, (n_total,)).fetchall()
        conn.close()

        dates = sorted(r[0] for r in rows if r[0])
        log.info("유효 날짜 탐지: %d일 (요청 %d일)", len(dates), n_total)
        return dates

    except Exception as exc:
        log.error("유효 날짜 탐지 실패: %s", exc)
        return []


def _split_train_valid(
    dates:        List[str],
    n_valid_days: int,
) -> Tuple[List[str], List[str]]:
    """
    dates (오름차순) 를 train / valid 로 분할.
    valid = 마지막 n_valid_days 일, train = 나머지.
    """
    if len(dates) <= n_valid_days:
        # 날짜가 부족하면 전부 train, valid 없음
        return list(dates), []
    split     = len(dates) - n_valid_days
    train_d   = dates[:split]
    valid_d   = dates[split:]
    return train_d, valid_d


# ─────────────────────────────────────────────────────────────────────
# 채번 헬퍼
# ─────────────────────────────────────────────────────────────────────

def _next_study_no(sim_db: str, prefix: str) -> int:
    """
    sim_observations 에서 prefix 로 시작하는 study 의 최대 번호 + 1 반환.
    예) auto_003 이 최대면 → 4 반환.
    """
    try:
        conn = sqlite3.connect(sim_db, timeout=10)
        row = conn.execute("""
            SELECT MAX(
                CAST(SUBSTR(study_name, LENGTH(?) + 2) AS INTEGER)
            )
            FROM sim_observations
            WHERE study_name LIKE ? || '_%'
        """, (prefix, prefix)).fetchone()
        conn.close()
        val = row[0] if row and row[0] else 0
        return int(val) + 1
    except Exception:
        return 1


# ─────────────────────────────────────────────────────────────────────
# 자동 루너
# ─────────────────────────────────────────────────────────────────────

class NightAutoRunner:
    """
    NightSimRunner 를 직접 호출해 완전 자동화된 루프를 실행한다.

    루프 1회 = random → bayes → validate → adopt → stress → cluster
    """

    def __init__(self, cfg: AutoRunConfig):
        self.cfg = cfg
        self._stop_requested = False   # Ctrl+C 핸들러 연동

        # 실제 실행용 runner (dry_run=True 면 호출은 하되 아무 것도 안 함)
        if not cfg.dry_run:
            from night_sim import NightSimRunner
            self._runner = NightSimRunner(
                source_db  = cfg.source_db,
                sim_db     = cfg.sim_db,
                night_json = cfg.night_json,
            )
        else:
            self._runner = None

    # ── 단계 실행 헬퍼 ────────────────────────────────────────────

    def _run_phase(
        self,
        phase:     str,
        fn,
        loop_log:  List[Dict],
        **kwargs,
    ) -> bool:
        """단계 1개 실행 + 결과/오류 로깅. 성공 → True, 실패 → False."""
        if self.cfg.dry_run:
            print(f"  [DRY-RUN] {phase}  args={kwargs}")
            loop_log.append({"phase": phase, "status": "dry-run"})
            return True

        started = datetime.now()
        print(f"\n  [{phase}] 시작 @ {started.strftime('%H:%M:%S')}  "
              + "  ".join(f"{k}={v}" for k, v in kwargs.items()),
              flush=True)
        try:
            result = fn(**kwargs)
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
        """루프 1회 실행. 반환: {study_name, train_dates, valid_dates, loop_log}"""
        cfg       = self.cfg
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

        r = self._runner   # 편의상

        # train_dates 의 대표 날짜 (cross-study 검색 키)
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

        # ── 루프 요약 ─────────────────────────────────────────────
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
        """
        루프 실행 진입점.
        n_loops=0 → 무한 반복 (Ctrl+C 로 종료).
        n_loops=N → N회 후 종료.
        """
        cfg        = self.cfg
        loop_no    = 0
        all_results: List[Dict] = []

        inf        = (cfg.n_loops == 0)
        total_str  = "∞" if inf else str(cfg.n_loops)

        print(f"\n{'='*72}")
        print(f"  MMEAN Night Auto-Run  (야간장)")
        print(f"  loops={total_str}  random={cfg.random_trials}  bayes={cfg.bayes_trials}")
        if cfg.train_dates_override or cfg.valid_dates_override:
            td = cfg.train_dates_override or []
            vd = cfg.valid_dates_override or []
            print(f"  train=직접지정 {len(td)}일  valid=직접지정 {len(vd)}일"
                  f"  cluster_k={cfg.cluster_k}")
            if td:
                print(f"  train_dates: {td[0]} ~ {td[-1]}"
                      + (f"  ({td[0]} 18:00 ~ 익일 06:00)" if len(td) == 1 else ""))
            if vd:
                print(f"  valid_dates: {vd[0]} ~ {vd[-1]}"
                      + (f"  ({vd[0]} 18:00 ~ 익일 06:00)" if len(vd) == 1 else ""))
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

                # ── 루프 간 대기 ─────────────────────────────────
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
            print("\n\n  Ctrl+C 감지 — 현재 루프 후 종료합니다.", flush=True)

        # ── 전체 요약 ─────────────────────────────────────────────
        _print_final_summary(all_results)


# ─────────────────────────────────────────────────────────────────────
# 출력 헬퍼
# ─────────────────────────────────────────────────────────────────────

def _print_loop_summary(
    loop_no:     int,
    study_name:  str,
    train_dates: List[str],
    valid_dates: List[str],
    loop_log:    List[Dict],
) -> None:
    ok  = sum(1 for p in loop_log if p["status"] in ("ok", "dry-run"))
    err = sum(1 for p in loop_log if p["status"] == "error")
    elapsed_total = sum(p.get("elapsed", 0) for p in loop_log)

    print(f"\n  ── LOOP {loop_no} 요약 ──────────────────────────────────────────")
    print(f"  study={study_name}")
    print(f"  train={len(train_dates)}일  valid={len(valid_dates)}일")
    print(f"  단계: {ok}성공 / {err}실패  총 {elapsed_total:.0f}s")
    for p in loop_log:
        mark  = "✓" if p["status"] in ("ok", "dry-run") else ("✗" if p["status"] == "error" else "~")
        extra = f"  err={p['error']}" if p.get("error") else ""
        print(f"    {mark} {p['phase']:<12}  {p['status']:<10}"
              f"  {p.get('elapsed', 0):5.0f}s{extra}")
    print()


def _print_final_summary(all_results: List[Dict]) -> None:
    print(f"\n{'='*72}")
    print(f"  MMEAN Auto-Run 종료  |  총 {len(all_results)} 루프")
    print(f"{'='*72}")
    errors = [r for r in all_results if "error" in r and "loop_log" not in r]
    if errors:
        print(f"  조기 종료된 루프: {[r['loop_no'] for r in errors]}")
    for r in all_results:
        if "loop_log" not in r:
            continue
        ok  = sum(1 for p in r["loop_log"] if p["status"] in ("ok", "dry-run"))
        err = sum(1 for p in r["loop_log"] if p["status"] == "error")
        print(f"  Loop {r['loop_no']:>3}  study={r.get('study_name',''):>14}"
              f"  {ok}성공/{err}실패")
    print(f"{'='*72}\n")


# ─────────────────────────────────────────────────────────────────────
# CLI
# ─────────────────────────────────────────────────────────────────────

def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="MMEAN 야간 시뮬레이션 자동화 루틴",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )

    # ── DB / 경로 ─────────────────────────────────────────────────
    p.add_argument("--source-db",  default=_DEFAULT_SOURCE_DB,  metavar="PATH",
                   help="mmean.db 경로 (읽기 전용)")
    p.add_argument("--sim-db",     default=_DEFAULT_SIM_DB,     metavar="PATH",
                   help="sim.db 경로 (쓰기)")
    p.add_argument("--night-json", default=_DEFAULT_NIGHT_JSON, metavar="PATH",
                   help="night_levels.json 경로")

    # ── 날짜 창 ───────────────────────────────────────────────────
    p.add_argument("--train",  type=int, default=15, metavar="N",
                   help="탐색(train) 날짜 수 (--train-dates 지정 시 무시)")
    p.add_argument("--valid",  type=int, default=5,  metavar="N",
                   help="검증(valid) 날짜 수 (--valid-dates 지정 시 무시)")
    p.add_argument("--train-dates", nargs="+", metavar="YYYY-MM-DD", default=None,
                   help="train 날짜 직접 지정 (지정 시 --train 무시)")
    p.add_argument("--valid-dates", nargs="+", metavar="YYYY-MM-DD", default=None,
                   help="valid 날짜 직접 지정 (지정 시 --valid 무시)")

    # ── 탐색 파라미터 ─────────────────────────────────────────────
    p.add_argument("--random",   type=int, default=300, metavar="N",
                   help="random 탐색 횟수 (0=건너뜀)")
    p.add_argument("--bayes",    type=int, default=200, metavar="N",
                   help="Bayesian 탐색 횟수 (0=건너뜀)")
    p.add_argument("--startup",  type=int, default=40,  metavar="N",
                   help="Optuna startup trials")
    p.add_argument("--top",      type=int, default=10,  metavar="N",
                   help="validation 상위 N개")
    p.add_argument("--cluster",  type=int, default=5,   metavar="K",
                   help="군집 수")

    # ── 루프 제어 ─────────────────────────────────────────────────
    p.add_argument("--loops",   type=int, default=0,  metavar="N",
                   help="루프 횟수 (0=무한)")
    p.add_argument("--sleep",   type=int, default=0,  metavar="SEC",
                   help="루프 간 대기 시간 (초)")

    # ── 단계 건너뜀 ───────────────────────────────────────────────
    p.add_argument("--skip-random",   action="store_true", help="random 탐색 건너뜀")
    p.add_argument("--skip-bayes",    action="store_true", help="BO 탐색 건너뜀")
    p.add_argument("--skip-validate", action="store_true", help="validation 건너뜀")
    p.add_argument("--skip-adopt",    action="store_true", help="adopt 건너뜀")
    p.add_argument("--skip-stress",   action="store_true", help="stress 건너뜀")
    p.add_argument("--skip-cluster",  action="store_true", help="cluster 건너뜀")

    # ── 기타 ──────────────────────────────────────────────────────
    p.add_argument("--prefix",   type=str, default="auto",  metavar="STR",
                   help="study 이름 prefix (auto -> auto_001, auto_002, ...)")
    p.add_argument("--seed",     type=int, default=42,      metavar="N",
                   help="기본 seed (루프 i에서 seed+i 사용)")
    p.add_argument("--force",    action="store_true",
                   help="기존 실행 덮어쓰기")
    p.add_argument("--dry-run",  action="store_true",
                   help="실제 실행 없이 계획만 출력")
    p.add_argument("--log-level", default="INFO",
                   choices=["DEBUG", "INFO", "WARNING", "ERROR"],
                   help="로그 레벨")

    return p.parse_args()


def main() -> None:
    args = _parse_args()

    logging.basicConfig(
        level   = getattr(logging, args.log_level),
        format  = "%(asctime)s [%(levelname)s] %(name)s - %(message)s",
    )

    cfg = AutoRunConfig(
        source_db        = args.source_db,
        sim_db           = args.sim_db,
        night_json       = args.night_json,
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

    runner = NightAutoRunner(cfg)
    runner.run()


if __name__ == "__main__":
    main()
