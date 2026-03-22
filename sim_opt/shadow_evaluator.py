
# MMEAN/sim_opt/shadow_evaluator.py
"""
ShadowEvaluator — paper_shadow 채택 후보 성과 평가기

흐름:
    validated  →  (수동 승격)  →  paper_shadow  →  (기준 충족)  →  adopted

평가 방식:
    세션 종료 후 (sim.bat 호출) 배치 실행.
    mmean.db의 judgment_events 테이블을 읽어 paper_shadow 채택 후보의
    entry config가 해당 이벤트에 적용됐을 경우의 가상 성과를 계산.

    ※ "실제 체결" 아님 — judgment_event가 있는 트레이드만 대상.
       shadow_entered=1: 설정 기준 충족 → 실제 pnl 그대로 적용
       shadow_entered=0: 설정 기준 미달 → 진입 안 함 (pnl=0)

승격 기준 (check_promotion):
    - shadow_trade_count >= PROMO_MIN_TRADES (기본 20)
    - shadow_total_pnl > 0
    - shadow_win_rate >= PROMO_MIN_WIN_RATE (기본 0.45)
    - shadow_obj >= PROMO_MIN_OBJ (기본 0.0)
    - 과적합 보호: shadow_obj <= valid_obj * PROMO_MAX_OVERFIT_RATIO (기본 1.5)

사용:
    ev = ShadowEvaluator(sim_db_path, source_db_path)
    ev.run_session("2026-03-22")     # 세션별 평가 — sim.bat 호출
    ev.print_shadow_status()         # 현황 출력 — sim.py [7s]
    ev.promote(adoption_id)          # 수동 승격 — sim.py [7a]
"""
from __future__ import annotations

import json
import logging
import math
import sqlite3
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

log = logging.getLogger("MMEAN.ShadowEvaluator")

# ── 경로 기본값 ───────────────────────────────────────────────────────
_STORAGE = Path(__file__).resolve().parent.parent / "storage"
_DEFAULT_SIM_DB    = str(_STORAGE / "sim.db")
_DEFAULT_SOURCE_DB = str(_STORAGE / "mmean.db")

# ── 승격 기준 ─────────────────────────────────────────────────────────
PROMO_MIN_TRADES        = 20      # 최소 shadow 거래 수
PROMO_MIN_WIN_RATE      = 0.45    # 최소 승률
PROMO_MIN_OBJ           = 0.0     # 최소 objective score (0 이상)
PROMO_MAX_OVERFIT_RATIO = 1.5     # shadow_obj / valid_obj 최대 비율 (과적합 방지)

# ── objective 계산용 ──────────────────────────────────────────────────
_MIN_TRADE_PENALTY_THRESHOLD = 10   # 거래 수 미달 패널티 기준


# ────────────────────────────────────────────────────────────────────

class ShadowEvaluator:
    """paper_shadow 채택 후보 성과 평가 및 승격."""

    def __init__(
        self,
        sim_db_path:    str = _DEFAULT_SIM_DB,
        source_db_path: str = _DEFAULT_SOURCE_DB,
    ) -> None:
        self.sim_db    = sim_db_path
        self.source_db = source_db_path

    # ── 공개 API ─────────────────────────────────────────────────────

    def run_session(self, session_date: str) -> List[Dict]:
        """
        session_date 에 해당하는 paper_shadow 후보들을 평가하고
        shadow_trades 에 저장.  중복 실행 safe (해당 날짜 기존 레코드 교체).
        반환: [{adoption_id, shadow_entered, total_pnl, win_rate, trade_count}, ...]
        """
        paper_shadows = self._load_paper_shadows()
        if not paper_shadows:
            log.info("[SHADOW] paper_shadow 후보 없음 — 평가 건너뜀")
            return []

        events = self._load_judgment_events(session_date)
        if not events:
            log.info("[SHADOW] %s 판단 이벤트 없음 — 평가 건너뜀", session_date)
            return []

        pattern_label = self._load_pattern(session_date)
        now_str       = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        results       = []

        sim_conn = self._sim_conn()
        try:
            for adoption in paper_shadows:
                adoption_id = adoption["id"]
                config      = json.loads(adoption["config_json"])

                # 기존 해당 날짜 레코드 삭제 (재평가 시)
                sim_conn.execute(
                    "DELETE FROM shadow_trades WHERE adoption_id=? AND session_date=?",
                    (adoption_id, session_date),
                )

                trades_inserted = []
                for ev in events:
                    shadow_entered, direction = self._would_enter(ev, config)
                    pnl_pt       = float(ev["pnl_pt"] or 0.0)
                    shadow_pnl   = pnl_pt if shadow_entered else 0.0
                    baseline_pnl = pnl_pt  # 실제 시스템 손익

                    sim_conn.execute("""
                        INSERT INTO shadow_trades
                        (adoption_id, session_date, judgment_event_id,
                         direction, entry_price, exit_price, pnl_pt,
                         baseline_pnl_pt, shadow_entered,
                         long_score, short_score, flow_score, volume_burst,
                         pattern_label, evaluated_at)
                        VALUES (?,?,?, ?,?,?,?, ?,?,  ?,?,?,?, ?,?)
                    """, (
                        adoption_id, session_date, ev["id"],
                        direction,
                        float(ev["entry_price"] or 0.0),
                        float(ev["exit_price"]  or 0.0),
                        shadow_pnl,
                        baseline_pnl,
                        1 if shadow_entered else 0,
                        float(ev["long_score"]    or 0.0),
                        float(ev["short_score"]   or 0.0),
                        float(ev["flow_score"]    or 0.0),
                        float(ev["volume_burst"]  or 0.0),
                        pattern_label,
                        now_str,
                    ))
                    trades_inserted.append(shadow_pnl)

                sim_conn.commit()

                # shadow 집계 업데이트
                summary = self._compute_summary(adoption_id, sim_conn)
                sim_conn.execute("""
                    UPDATE sim_adoptions
                       SET shadow_trade_count  = ?,
                           shadow_total_pnl    = ?,
                           shadow_win_rate      = ?,
                           shadow_obj          = ?,
                           shadow_evaluated_at = ?
                     WHERE id = ?
                """, (
                    summary["trade_count"],
                    summary["total_pnl"],
                    summary["win_rate"],
                    summary["obj_score"],
                    now_str,
                    adoption_id,
                ))
                sim_conn.commit()

                log.info(
                    "[SHADOW] id=%d study=%s | date=%s entered=%d/%d "
                    "pnl=%.2fpt wr=%.0f%% obj=%.2f",
                    adoption_id, adoption["study_name"],
                    session_date,
                    sum(1 for t in trades_inserted if t != 0),
                    len(trades_inserted),
                    summary["total_pnl"],
                    summary["win_rate"] * 100,
                    summary["obj_score"],
                )
                results.append({
                    "adoption_id":    adoption_id,
                    "study_name":     adoption["study_name"],
                    "session_date":   session_date,
                    "trade_count":    summary["trade_count"],
                    "total_pnl":      summary["total_pnl"],
                    "win_rate":       summary["win_rate"],
                    "obj_score":      summary["obj_score"],
                })
        finally:
            sim_conn.close()

        return results

    def check_promotion(self, adoption_id: int) -> Dict[str, Any]:
        """
        승격 기준 충족 여부 반환.
        반환: {eligible: bool, reason: str, stats: dict}
        """
        sim_conn = self._sim_conn()
        try:
            row = sim_conn.execute(
                """SELECT shadow_trade_count, shadow_total_pnl, shadow_win_rate,
                          shadow_obj, valid_obj, status
                   FROM sim_adoptions WHERE id=?""",
                (adoption_id,),
            ).fetchone()
        finally:
            sim_conn.close()

        if not row:
            return {"eligible": False, "reason": "adoption not found", "stats": {}}

        status = row["status"]
        if status not in ("validated", "paper_shadow"):
            return {
                "eligible": False,
                "reason":   f"status={status} (validated 또는 paper_shadow만 승격 가능)",
                "stats":    dict(row),
            }

        n       = int(row["shadow_trade_count"] or 0)
        tot     = float(row["shadow_total_pnl"]  or 0.0)
        wr      = float(row["shadow_win_rate"]    or 0.0)
        obj     = float(row["shadow_obj"]         or 0.0)
        v_obj   = float(row["valid_obj"]          or 0.0)

        checks: List[Tuple[bool, str]] = [
            (n >= PROMO_MIN_TRADES,
             f"거래 수 {n}/{PROMO_MIN_TRADES}"),
            (tot > 0,
             f"누적손익 {tot:+.2f}pt (>0 필요)"),
            (wr >= PROMO_MIN_WIN_RATE,
             f"승률 {wr:.0%} (>= {PROMO_MIN_WIN_RATE:.0%} 필요)"),
            (obj >= PROMO_MIN_OBJ,
             f"shadow_obj {obj:.2f} (>= {PROMO_MIN_OBJ} 필요)"),
        ]
        # 과적합 방지: validation 대비 너무 좋으면 불안정 신호
        if v_obj > 0:
            ratio = obj / v_obj if v_obj > 0 else 0
            checks.append((
                ratio <= PROMO_MAX_OVERFIT_RATIO,
                f"과적합비 {ratio:.2f} (<= {PROMO_MAX_OVERFIT_RATIO})",
            ))

        failed  = [(ok, msg) for ok, msg in checks if not ok]
        passed  = [(ok, msg) for ok, msg in checks if ok]
        eligible = len(failed) == 0

        reason_parts = []
        for ok, msg in checks:
            mark = "✓" if ok else "✗"
            reason_parts.append(f"{mark} {msg}")
        reason = "  /  ".join(reason_parts)

        return {
            "eligible": eligible,
            "reason":   reason,
            "stats": {
                "shadow_trade_count": n,
                "shadow_total_pnl":   tot,
                "shadow_win_rate":    wr,
                "shadow_obj":         obj,
                "valid_obj":          v_obj,
            },
        }

    def promote(self, adoption_id: int, force: bool = False) -> bool:
        """
        paper_shadow → adopted 로 승격.
        force=True: 기준 미충족이어도 승격 (수동 Override).
        반환: 성공 여부
        """
        result = self.check_promotion(adoption_id)
        if not force and not result["eligible"]:
            log.warning(
                "[SHADOW] id=%d 승격 기준 미충족: %s",
                adoption_id, result["reason"],
            )
            return False

        now_str  = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        sim_conn = self._sim_conn()
        try:
            sim_conn.execute(
                "UPDATE sim_adoptions SET status='adopted', notes=? WHERE id=?",
                (f"adopted via shadow (force={force}) at {now_str}", adoption_id),
            )
            sim_conn.commit()
        finally:
            sim_conn.close()

        log.info("[SHADOW] id=%d → adopted  (force=%s)", adoption_id, force)
        return True

    def promote_to_shadow(self, adoption_id: int) -> bool:
        """
        validated → paper_shadow 로 승격 (shadow 트래킹 시작).
        반환: 성공 여부
        """
        sim_conn = self._sim_conn()
        try:
            row = sim_conn.execute(
                "SELECT status FROM sim_adoptions WHERE id=?", (adoption_id,)
            ).fetchone()
            if not row:
                log.warning("[SHADOW] id=%d 없음", adoption_id)
                return False
            if row["status"] != "validated":
                log.warning(
                    "[SHADOW] id=%d status=%s (validated만 paper_shadow로 이동 가능)",
                    adoption_id, row["status"],
                )
                return False

            now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            sim_conn.execute(
                """UPDATE sim_adoptions
                      SET status='paper_shadow', shadow_started_at=?, notes=?
                    WHERE id=?""",
                (now_str, f"shadow started at {now_str}", adoption_id),
            )
            sim_conn.commit()
        finally:
            sim_conn.close()

        log.info("[SHADOW] id=%d validated → paper_shadow", adoption_id)
        return True

    def print_shadow_status(self) -> None:
        """shadow 현황 출력 — sim.py [7s]."""
        sim_conn = self._sim_conn()
        try:
            rows = sim_conn.execute("""
                SELECT
                    id, study_name, train_pattern, status,
                    valid_obj, robustness, stress_passed,
                    shadow_trade_count, shadow_total_pnl,
                    shadow_win_rate, shadow_obj,
                    shadow_started_at, shadow_evaluated_at
                FROM sim_adoptions
                WHERE status IN ('validated','paper_shadow','adopted')
                ORDER BY status DESC, valid_obj DESC
            """).fetchall()
        finally:
            sim_conn.close()

        if not rows:
            print("  paper_shadow 후보 없음 (validated 또는 그 이상 상태 없음)")
            return

        print()
        print("═" * 100)
        print(f"  {'ID':>4}  {'study':^18}  {'pattern':^15}  {'status':^12}  "
              f"{'valid_obj':>9}  {'n_shd':>5}  {'shd_pnl':>8}  {'shd_wr':>6}  {'shd_obj':>7}  "
              f"{'stress':^6}  {'evaluated'}  ")
        print("─" * 100)

        for r in rows:
            status_mark = {
                "validated":   "🔵",
                "paper_shadow": "🟡",
                "adopted":     "🟢",
            }.get(r["status"], "  ")
            stress_mark = "✓" if r["stress_passed"] else "✗"
            shd_wr_str  = f"{r['shadow_win_rate']*100:.0f}%" if r["shadow_trade_count"] else "-"
            shd_pnl_str = f"{r['shadow_total_pnl']:+.2f}" if r["shadow_trade_count"] else "-"
            shd_obj_str = f"{r['shadow_obj']:.2f}"         if r["shadow_trade_count"] else "-"
            eval_str    = (r["shadow_evaluated_at"] or "")[:10]

            print(
                f"  {r['id']:>4}  {(r['study_name'] or '')[:18]:^18}  "
                f"{(r['train_pattern'] or 'N/A')[:15]:^15}  "
                f"{status_mark}{r['status']:^10}  "
                f"{r['valid_obj']:>9.2f}  "
                f"{r['shadow_trade_count']:>5}  "
                f"{shd_pnl_str:>8}  "
                f"{shd_wr_str:>6}  "
                f"{shd_obj_str:>7}  "
                f"{stress_mark:^6}  "
                f"{eval_str}"
            )

        print("═" * 100)
        print()
        print("  [범례] 🔵 validated — stress 통과, shadow 미시작")
        print("         🟡 paper_shadow — shadow 트래킹 중")
        print("         🟢 adopted — 운영 채택 완료")
        print()

    # ── 내부 헬퍼 ─────────────────────────────────────────────────────

    def _would_enter(
        self, event: sqlite3.Row, config: Dict
    ) -> Tuple[bool, str]:
        """
        shadow config 기준으로 진입 여부 결정.
        반환: (would_enter, direction_str)

        ※ final_signal=WAIT인 경우 실제 시스템이 진입 안 했으므로
           pnl_pt가 없다 → 이런 이벤트는 run_session에서 이미 필터됨.
        """
        signal  = event["entry_signal"] or ""
        if signal not in ("LONG_READY", "SHORT_READY"):
            return False, ""

        direction = "LONG" if signal == "LONG_READY" else "SHORT"
        ls = float(event["long_score"]  or 0.0)
        ss = float(event["short_score"] or 0.0)
        fs = float(event["flow_score"]  or 0.0)

        # 진입 점수 기준
        enter_score = float(config.get("enter_score", 4.0))
        enter_gap   = float(config.get("enter_gap",   1.0))
        min_flow    = float(config.get("flow_score_gate_thr", 0.5))

        if signal == "LONG_READY":
            score_ok = ls >= enter_score
            gap_ok   = (ls - ss) >= enter_gap
        else:  # SHORT_READY
            score_ok = ss >= enter_score
            gap_ok   = (ss - ls) >= enter_gap

        flow_required = bool(config.get("flow_required", True))
        flow_ok = (fs >= min_flow) if flow_required else True

        would_enter = score_ok and gap_ok and flow_ok
        return would_enter, direction

    def _load_paper_shadows(self) -> List[sqlite3.Row]:
        sim_conn = self._sim_conn()
        try:
            return sim_conn.execute("""
                SELECT id, study_name, config_json, valid_obj
                FROM sim_adoptions
                WHERE status = 'paper_shadow'
                ORDER BY valid_obj DESC
            """).fetchall()
        finally:
            sim_conn.close()

    def _load_judgment_events(self, session_date: str) -> List[sqlite3.Row]:
        """
        mmean.db에서 session_date의 완료된 판단 이벤트 조회.
        (pnl_pt IS NOT NULL = 거래 완료)
        """
        try:
            conn = sqlite3.connect(
                f"file:{self.source_db}?mode=ro", uri=True, timeout=10
            )
            conn.row_factory = sqlite3.Row
            rows = conn.execute("""
                SELECT id, entry_signal, final_signal,
                       long_score, short_score, flow_score, volume_burst,
                       entry_price, exit_price, direction, pnl_pt,
                       mfe_pt, mae_pt, exit_reason
                FROM judgment_events
                WHERE date(ts) = ?
                  AND pnl_pt IS NOT NULL
                  AND entry_signal IN ('LONG_READY', 'SHORT_READY')
                ORDER BY id
            """, (session_date,)).fetchall()
            conn.close()
            return rows
        except Exception as e:
            log.warning("[SHADOW] judgment_events 조회 실패: %s", e)
            return []

    def _load_pattern(self, session_date: str) -> Optional[str]:
        try:
            conn = sqlite3.connect(
                f"file:{self.source_db}?mode=ro", uri=True, timeout=5
            )
            row = conn.execute(
                "SELECT pattern_type FROM session_patterns "
                "WHERE date=? AND session_mode='day'",
                (session_date,),
            ).fetchone()
            conn.close()
            return row[0] if row else None
        except Exception:
            return None

    def _compute_summary(
        self, adoption_id: int, conn: sqlite3.Connection
    ) -> Dict[str, float]:
        """shadow_trades 집계 → trade_count, total_pnl, win_rate, obj_score."""
        rows = conn.execute("""
            SELECT shadow_entered, pnl_pt
            FROM shadow_trades
            WHERE adoption_id = ? AND shadow_entered = 1
        """, (adoption_id,)).fetchall()

        if not rows:
            return {"trade_count": 0, "total_pnl": 0.0,
                    "win_rate": 0.0, "obj_score": 0.0}

        pnls       = [float(r["pnl_pt"] or 0.0) for r in rows]
        total_pnl  = sum(pnls)
        wins       = sum(1 for p in pnls if p > 0)
        losses     = sum(1 for p in pnls if p < 0)
        n          = len(pnls)
        win_rate   = wins / n if n > 0 else 0.0

        # 간이 objective score
        gross_profit = sum(p for p in pnls if p > 0)
        gross_loss   = abs(sum(p for p in pnls if p < 0))
        pf           = (gross_profit / gross_loss) if gross_loss > 0 else (
                        float("inf") if gross_profit > 0 else 0.0)

        # 패널티: 거래 수 부족
        low_trade_penalty = max(0.0, (_MIN_TRADE_PENALTY_THRESHOLD - n) * 0.5)

        obj_score = (
            total_pnl * 0.4
            + win_rate * 10.0
            + min(pf, 4.0) * 2.0
            - low_trade_penalty
        )

        return {
            "trade_count": n,
            "total_pnl":   round(total_pnl,  4),
            "win_rate":    round(win_rate,    4),
            "obj_score":   round(obj_score,   4),
        }

    def _sim_conn(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.sim_db, timeout=30, check_same_thread=False)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA busy_timeout=30000")
        return conn


# ────────────────────────────────────────────────────────────────────
# CLI (sim.bat 직접 호출용)
# ────────────────────────────────────────────────────────────────────

def _run_cli() -> None:
    import argparse
    import sys
    import logging as _logging
    _logging.basicConfig(
        level=_logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    )

    ap = argparse.ArgumentParser(description="ShadowEvaluator CLI")
    ap.add_argument("--session-date",  default=None,
                    help="평가 날짜 YYYY-MM-DD (생략 시 오늘)")
    ap.add_argument("--status",        action="store_true",
                    help="shadow 현황 출력")
    ap.add_argument("--promote-shadow", type=int, default=None, metavar="ID",
                    help="validated → paper_shadow 승격 (adoption id)")
    ap.add_argument("--promote-adopted", type=int, default=None, metavar="ID",
                    help="paper_shadow → adopted 승격 (adoption id)")
    ap.add_argument("--force",         action="store_true",
                    help="기준 미충족이어도 강제 승격")
    ap.add_argument("--sim-db",        default=_DEFAULT_SIM_DB)
    ap.add_argument("--source-db",     default=_DEFAULT_SOURCE_DB)
    args = ap.parse_args()

    ev = ShadowEvaluator(args.sim_db, args.source_db)

    if args.status:
        ev.print_shadow_status()

    if args.promote_shadow is not None:
        ok = ev.promote_to_shadow(args.promote_shadow)
        print(f"promote_to_shadow id={args.promote_shadow}: {'OK' if ok else 'FAIL'}")

    if args.promote_adopted is not None:
        ok = ev.promote(args.promote_adopted, force=args.force)
        print(f"promote_to_adopted id={args.promote_adopted}: {'OK' if ok else 'FAIL'}")

    if not (args.status or args.promote_shadow or args.promote_adopted):
        # 세션 평가 실행
        from datetime import date as _date
        sess_date = args.session_date or _date.today().isoformat()
        print(f"[SHADOW] 세션 평가 시작: {sess_date}")
        results = ev.run_session(sess_date)
        if results:
            for r in results:
                print(
                    f"  id={r['adoption_id']}  study={r['study_name']}  "
                    f"n={r['trade_count']}  pnl={r['total_pnl']:+.2f}pt  "
                    f"wr={r['win_rate']:.0%}  obj={r['obj_score']:.2f}"
                )
        else:
            print("  평가 결과 없음")


if __name__ == "__main__":
    _run_cli()
