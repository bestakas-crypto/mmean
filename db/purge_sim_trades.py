"""
purge_sim_trades.py — 검증 완료된 sim_trades 정리 스크립트

목적:
  sim_trades는 sim_runs당 수백~수천 행을 생성해 sim.db 용량의 대부분을 차지.
  검증 완료(status=done + sim_run_summary 존재) 후에는 원시 틱 데이터가 불필요.
  해당 run_id의 sim_trades만 삭제하고 VACUUM으로 용량을 회수한다.

삭제 기준 (AND 조건):
  ① sim_runs.status = 'done'
  ② sim_run_summary에 해당 run_id 존재 (집계 완료 확인)
  ③ run_finished_at < NOW - keep_days  (기본 0일 = 오늘 완료분도 포함)

보존 대상 (건드리지 않음):
  sim_runs / sim_run_summary / sim_adoptions / sim_observations / sim_run_sessions
  → 승격·RAG·파라미터 이력에 필요

실행 예시:
  python purge_sim_trades.py --status              # 현황만 출력
  python purge_sim_trades.py --dry-run             # 삭제 대상 건수만 확인
  python purge_sim_trades.py                       # 실행 (기본: 완료된 전체)
  python purge_sim_trades.py --keep-days 3         # 최근 3일 완료분은 보존
  python purge_sim_trades.py --run-type validation # 특정 run_type만 정리
  python purge_sim_trades.py --run-ids 10 11 12    # 특정 run_id만 정리
"""

from __future__ import annotations

import argparse
import logging
import os
import sqlite3
import sys
from datetime import datetime, timedelta
from pathlib import Path

# ── 로깅 ──────────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(
            Path(__file__).resolve().parent.parent / "storage" / "purge_sim_trades.log",
            encoding="utf-8",
        ),
    ],
)
log = logging.getLogger("PurgeSimTrades")

# ── 경로 ──────────────────────────────────────────────────────────────────────
_SIM_DB = str(Path(__file__).resolve().parent.parent / "storage" / "sim.db")


# ── DB 연결 ───────────────────────────────────────────────────────────────────

def _connect(path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(path, timeout=60)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.row_factory = sqlite3.Row
    return conn


# ── 현황 출력 ─────────────────────────────────────────────────────────────────

def show_status(conn: sqlite3.Connection, db_path: str) -> None:
    size_mb = os.path.getsize(db_path) / 1024 / 1024

    rows = conn.execute("SELECT COUNT(*) FROM sim_trades").fetchone()[0]
    runs = conn.execute("SELECT COUNT(*) FROM sim_runs").fetchone()[0]
    done_runs = conn.execute(
        "SELECT COUNT(*) FROM sim_runs WHERE status = 'done'"
    ).fetchone()[0]
    summarized = conn.execute(
        "SELECT COUNT(*) FROM sim_run_summary"
    ).fetchone()[0]

    log.info("=" * 56)
    log.info("  sim.db  크기  : %.1f MB  (%s)", size_mb, db_path)
    log.info("  sim_trades 행  : %d건", rows)
    log.info("  sim_runs  전체 : %d건  (done=%d  summarized=%d)",
             runs, done_runs, summarized)
    log.info("=" * 56)


# ── 정리 대상 run_id 조회 ──────────────────────────────────────────────────────

def _find_purgeable_run_ids(
    conn: sqlite3.Connection,
    keep_days: int,
    run_type: str | None,
    run_ids: list[int] | None,
) -> list[int]:
    """
    삭제 조건:
      - sim_runs.status = 'done'
      - sim_run_summary 에 run_id 존재
      - run_finished_at < cutoff  (keep_days=0 이면 전체)
      - run_type 필터 (옵션)
      - run_ids 직접 지정 (옵션, 지정 시 다른 조건 무시)
    """
    if run_ids:
        placeholders = ",".join("?" * len(run_ids))
        rows = conn.execute(
            f"SELECT id FROM sim_runs WHERE id IN ({placeholders})",
            run_ids,
        ).fetchall()
        return [r["id"] for r in rows]

    cutoff = (datetime.now() - timedelta(days=keep_days)).strftime("%Y-%m-%d %H:%M:%S")

    params: list = []
    type_clause = ""
    if run_type:
        type_clause = "AND r.run_type = ? "
        params.append(run_type)

    cutoff_clause = ""
    if keep_days > 0:
        cutoff_clause = "AND r.run_finished_at < ? "
        params.append(cutoff)

    sql = f"""
        SELECT r.id
        FROM   sim_runs r
        WHERE  r.status = 'done'
          AND  EXISTS (
               SELECT 1 FROM sim_run_summary s WHERE s.run_id = r.id
          )
          {type_clause}
          {cutoff_clause}
        ORDER BY r.id
    """
    rows = conn.execute(sql, params).fetchall()
    return [r["id"] for r in rows]


# ── 정리 실행 ──────────────────────────────────────────────────────────────────

def purge(
    db_path: str,
    keep_days: int,
    run_type: str | None,
    run_ids: list[int] | None,
    dry_run: bool,
) -> None:
    if not os.path.exists(db_path):
        log.error("sim.db 없음: %s", db_path)
        return

    conn = _connect(db_path)
    try:
        log.info("── 정리 전 현황 ──────────────────────────────────────────")
        show_status(conn, db_path)

        target_ids = _find_purgeable_run_ids(conn, keep_days, run_type, run_ids)
        if not target_ids:
            log.info("정리 대상 run_id 없음 — 조건을 확인하세요.")
            return

        log.info("정리 대상 run_id : %d건  (id %d ~ %d)",
                 len(target_ids), target_ids[0], target_ids[-1])

        # 대상 run_id의 sim_trades 건수 확인
        placeholders = ",".join("?" * len(target_ids))
        trade_count = conn.execute(
            f"SELECT COUNT(*) FROM sim_trades WHERE run_id IN ({placeholders})",
            target_ids,
        ).fetchone()[0]

        log.info("삭제 대상 sim_trades : %d건", trade_count)

        if trade_count == 0:
            log.info("삭제할 sim_trades 없음.")
            return

        if dry_run:
            log.info("★ DRY-RUN — 실제 삭제 없음 ★")
            return

        # ── 삭제 ──────────────────────────────────────────────────────
        # 한 번에 너무 많은 id를 IN()에 넣으면 SQLite 한계 초과 가능
        # → 1,000개씩 나눠서 처리
        chunk_size = 1000
        deleted_total = 0
        for i in range(0, len(target_ids), chunk_size):
            chunk = target_ids[i : i + chunk_size]
            ph = ",".join("?" * len(chunk))
            conn.execute(
                f"DELETE FROM sim_trades WHERE run_id IN ({ph})", chunk
            )
            conn.commit()
            deleted_total += len(chunk)
            log.info("  삭제 진행 %d / %d run_id 처리 완료",
                     min(i + chunk_size, len(target_ids)), len(target_ids))

        log.info("sim_trades 삭제 완료 — %d건", trade_count)

        # ── VACUUM ────────────────────────────────────────────────────
        log.info("VACUUM 실행 중 (용량 회수)...")
        conn.execute("VACUUM")
        log.info("VACUUM 완료")

        log.info("── 정리 후 현황 ──────────────────────────────────────────")
        show_status(conn, db_path)

    finally:
        conn.close()


# ── CLI ───────────────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(
        description="검증 완료된 sim_trades 정리 (용량 회수)",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
예시:
  python purge_sim_trades.py --status
  python purge_sim_trades.py --dry-run
  python purge_sim_trades.py
  python purge_sim_trades.py --keep-days 3
  python purge_sim_trades.py --run-type validation
  python purge_sim_trades.py --run-ids 10 11 12
        """,
    )
    parser.add_argument(
        "--status", action="store_true",
        help="현황만 출력 (삭제 없음)",
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="삭제 대상 건수만 확인 (실제 삭제 없음)",
    )
    parser.add_argument(
        "--keep-days", type=int, default=0,
        help="최근 N일 완료 run은 보존 (기본: 0 = 전체 삭제)",
    )
    parser.add_argument(
        "--run-type",
        choices=["profile", "random", "bayes_opt", "validation", "stress"],
        default=None,
        help="특정 run_type만 정리",
    )
    parser.add_argument(
        "--run-ids", type=int, nargs="+", default=None,
        metavar="ID",
        help="특정 run_id만 정리 (지정 시 다른 조건 무시)",
    )
    parser.add_argument(
        "--db", default=_SIM_DB,
        help=f"sim.db 경로 (기본: {_SIM_DB})",
    )
    args = parser.parse_args()

    if args.status:
        conn = _connect(args.db)
        try:
            show_status(conn, args.db)
        finally:
            conn.close()
        return

    if args.dry_run:
        log.info("★ DRY-RUN 모드 — 실제 데이터 변경 없음 ★")

    purge(
        db_path=args.db,
        keep_days=args.keep_days,
        run_type=args.run_type,
        run_ids=args.run_ids,
        dry_run=args.dry_run,
    )


if __name__ == "__main__":
    main()
