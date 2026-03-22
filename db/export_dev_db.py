"""
export_dev_db.py — 개발 PC 전송용 경량 DB 추출

목적:
  운영 서버(노트북)의 sim.db / mmean.db 에서
  개발에 필요한 테이블만 추출해 경량 DB 파일 생성.

출력:
  storage/dev_sim.db    ← FileZilla로 개발 PC에 전송
  storage/dev_mmean.db  ← FileZilla로 개발 PC에 전송

실행 예시:
  python export_dev_db.py                  # 기본 (sim_trades 최근 10,000건)
  python export_dev_db.py --trades 50000   # sim_trades 최근 50,000건
  python export_dev_db.py --status         # 현황만 출력 (DB 생성 안 함)
  python export_dev_db.py --dry-run        # 대상 건수만 확인
"""

from __future__ import annotations

import argparse
import logging
import os
import shutil
import sqlite3
import sys
from datetime import datetime
from pathlib import Path

# ── 로깅 ─────────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
log = logging.getLogger("ExportDevDB")

# ── 경로 ─────────────────────────────────────────────────────────────────────
_STORAGE      = Path(__file__).parent / "storage"
SRC_SIM_DB    = _STORAGE / "sim.db"
SRC_MMEAN_DB  = _STORAGE / "mmean.db"
DST_SIM_DB    = _STORAGE / "dev_sim.db"
DST_MMEAN_DB  = _STORAGE / "dev_mmean.db"

# ── sim.db 추출 정의 ──────────────────────────────────────────────────────────
# (table, query)  query=None 이면 전체 복사
SIM_TABLES = [
    # 전체 복사
    ("sim_runs",          None),
    ("sim_run_summary",   None),
    ("sim_observations",  None),
    ("sim_run_sessions",  None),
    ("sim_adoptions",     None),
    # 샘플만 (최근 N건 — _TRADES_LIMIT 으로 조정)
    ("sim_trades",        "SAMPLE"),   # 별도 처리
]

# ── mmean.db 추출 정의 ────────────────────────────────────────────────────────
MMEAN_TABLES = [
    # 전체 복사
    ("market_features",    None),
    ("pattern_docs",       None),
    ("regime_ticks",       None),
    ("regime_events",      None),
    ("trades",             None),
    ("entry_events",       None),
    ("config_snapshots",   None),
    ("llm_calls",          None),
    ("llm_daily_baseline", None),
    ("llm_decision_log",   None),
    ("llm_param_events",   None),
    ("prompt_snapshots",   None),
    ("replay_runs",        None),
    ("replay_trades",      None),
    # 제외 (크고 개발에 불필요)
    # sim_equity          284K행 — 단순 수익곡선
    # ab_sim_equity       439K행 — 단순 수익곡선
    # llm_evaluations     비어있음
    # champion_vs_challenger 비어있음
    # promotion_*         비어있음
    # sim_open_pos        비어있음
    # pattern_memory      비어있음
]


# ════════════════════════════════════════════════════════════════════════════
# 헬퍼
# ════════════════════════════════════════════════════════════════════════════

def _connect(path: str | Path, readonly: bool = False) -> sqlite3.Connection:
    uri = f"file:{path}{'?mode=ro' if readonly else ''}".replace("\\", "/")
    conn = sqlite3.connect(f"file:{path}", uri=True, timeout=30) if readonly \
        else sqlite3.connect(str(path), timeout=30)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.row_factory = sqlite3.Row
    return conn


def _table_exists(conn: sqlite3.Connection, table: str) -> bool:
    return conn.execute(
        "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name=?",
        (table,)
    ).fetchone()[0] > 0


def _copy_schema(src: sqlite3.Connection, dst: sqlite3.Connection, table: str) -> None:
    """테이블 + 인덱스 DDL 복사."""
    row = src.execute(
        "SELECT sql FROM sqlite_master WHERE type='table' AND name=?", (table,)
    ).fetchone()
    if not row:
        return
    dst.execute(row[0])
    for idx in src.execute(
        "SELECT sql FROM sqlite_master WHERE type='index' AND tbl_name=? AND sql IS NOT NULL",
        (table,)
    ).fetchall():
        try:
            dst.execute(idx[0])
        except sqlite3.OperationalError:
            pass
    dst.commit()


def _row_count(conn: sqlite3.Connection, table: str) -> int:
    if not _table_exists(conn, table):
        return 0
    return conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]


def _copy_table_full(src: sqlite3.Connection, dst: sqlite3.Connection, table: str) -> int:
    """테이블 전체 복사. 반환값: 복사 건수."""
    if not _table_exists(src, table):
        log.warning("  테이블 없음 (건너뜀): %s", table)
        return 0
    _copy_schema(src, dst, table)
    src.row_factory = sqlite3.Row
    rows = src.execute(f"SELECT * FROM {table}").fetchall()
    if not rows:
        return 0
    cols = rows[0].keys()
    placeholders = ",".join("?" * len(cols))
    col_str = ",".join(cols)
    dst.executemany(
        f"INSERT OR IGNORE INTO {table} ({col_str}) VALUES ({placeholders})",
        [tuple(r) for r in rows],
    )
    dst.commit()
    return len(rows)


def _copy_sim_trades_sample(
    src: sqlite3.Connection,
    dst: sqlite3.Connection,
    limit: int,
) -> int:
    """sim_trades 최근 N건만 복사."""
    if not _table_exists(src, "sim_trades"):
        log.warning("  테이블 없음 (건너뜀): sim_trades")
        return 0
    _copy_schema(src, dst, "sim_trades")
    # run_id 기준 최근 N개 run 의 trades 만 추출
    rows = src.execute(
        f"""
        SELECT t.* FROM sim_trades t
        INNER JOIN (
            SELECT DISTINCT run_id FROM sim_trades
            ORDER BY run_id DESC
            LIMIT (
                SELECT COUNT(DISTINCT run_id) FROM (
                    SELECT run_id FROM sim_trades ORDER BY run_id DESC LIMIT {limit}
                )
            )
        ) r ON t.run_id = r.run_id
        ORDER BY t.run_id DESC
        LIMIT {limit}
        """
    ).fetchall()
    if not rows:
        return 0
    cols = rows[0].keys()
    placeholders = ",".join("?" * len(cols))
    col_str = ",".join(cols)
    dst.executemany(
        f"INSERT OR IGNORE INTO sim_trades ({col_str}) VALUES ({placeholders})",
        [tuple(r) for r in rows],
    )
    dst.commit()
    return len(rows)


def _db_size_mb(path: Path) -> float:
    return path.stat().st_size / 1024 / 1024 if path.exists() else 0.0


# ════════════════════════════════════════════════════════════════════════════
# 메인 로직
# ════════════════════════════════════════════════════════════════════════════

def show_status() -> None:
    """현황 출력."""
    print("=" * 60)
    print("  DB 현황")
    print("=" * 60)
    for label, path in [("sim.db (원본)", SRC_SIM_DB),
                        ("mmean.db (원본)", SRC_MMEAN_DB),
                        ("dev_sim.db (개발용)", DST_SIM_DB),
                        ("dev_mmean.db (개발용)", DST_MMEAN_DB)]:
        if path.exists():
            sz = _db_size_mb(path)
            print(f"  {label:25s}  {sz:8.1f} MB")
        else:
            print(f"  {label:25s}  (없음)")

    if SRC_SIM_DB.exists():
        conn = _connect(SRC_SIM_DB)
        print()
        print("  sim.db 테이블별 행수:")
        for table, _ in SIM_TABLES:
            cnt = _row_count(conn, table)
            print(f"    {table:30s}  {cnt:>12,} 행")
        conn.close()

    if SRC_MMEAN_DB.exists():
        conn = _connect(SRC_MMEAN_DB)
        print()
        print("  mmean.db 테이블별 행수:")
        for table, _ in MMEAN_TABLES:
            cnt = _row_count(conn, table)
            print(f"    {table:30s}  {cnt:>12,} 행")
        conn.close()
    print("=" * 60)


def export_sim(trades_limit: int, dry_run: bool) -> None:
    if not SRC_SIM_DB.exists():
        log.error("원본 없음: %s", SRC_SIM_DB)
        return

    log.info("── sim.db 추출 시작 (trades 최근 %d건) ──", trades_limit)

    src = _connect(SRC_SIM_DB)
    if dry_run:
        for table, mode in SIM_TABLES:
            if mode == "SAMPLE":
                total = _row_count(src, table)
                log.info("  [DRY] sim_trades 전체=%d → 추출예정=%d", total, min(trades_limit, total))
            else:
                cnt = _row_count(src, table)
                log.info("  [DRY] %s → %d 행 전체 복사", table, cnt)
        src.close()
        return

    # 기존 dev_sim.db 삭제
    if DST_SIM_DB.exists():
        DST_SIM_DB.unlink()
        log.info("  기존 dev_sim.db 삭제")

    dst = _connect(DST_SIM_DB)
    total_rows = 0

    for table, mode in SIM_TABLES:
        if mode == "SAMPLE":
            n = _copy_sim_trades_sample(src, dst, trades_limit)
            log.info("  sim_trades (샘플)  %d 건 복사", n)
        else:
            n = _copy_table_full(src, dst, table)
            log.info("  %-30s %d 건 복사", table, n)
        total_rows += n

    # VACUUM
    dst.execute("VACUUM")
    dst.close()
    src.close()

    sz = _db_size_mb(DST_SIM_DB)
    log.info("  dev_sim.db 생성 완료 | 총 %d 건 | %.1f MB", total_rows, sz)


def export_mmean(dry_run: bool) -> None:
    if not SRC_MMEAN_DB.exists():
        log.error("원본 없음: %s", SRC_MMEAN_DB)
        return

    log.info("── mmean.db 추출 시작 ──")

    src = _connect(SRC_MMEAN_DB)
    if dry_run:
        for table, _ in MMEAN_TABLES:
            cnt = _row_count(src, table)
            log.info("  [DRY] %s → %d 행 전체 복사", table, cnt)
        src.close()
        return

    # 기존 dev_mmean.db 삭제
    if DST_MMEAN_DB.exists():
        DST_MMEAN_DB.unlink()
        log.info("  기존 dev_mmean.db 삭제")

    dst = _connect(DST_MMEAN_DB)
    total_rows = 0

    for table, _ in MMEAN_TABLES:
        n = _copy_table_full(src, dst, table)
        log.info("  %-30s %d 건 복사", table, n)
        total_rows += n

    dst.execute("VACUUM")
    dst.close()
    src.close()

    sz = _db_size_mb(DST_MMEAN_DB)
    log.info("  dev_mmean.db 생성 완료 | 총 %d 건 | %.1f MB", total_rows, sz)


# ════════════════════════════════════════════════════════════════════════════
# CLI
# ════════════════════════════════════════════════════════════════════════════

def main() -> None:
    parser = argparse.ArgumentParser(description="개발용 경량 DB 추출")
    parser.add_argument("--status",    action="store_true", help="현황만 출력")
    parser.add_argument("--dry-run",   action="store_true", help="대상 건수만 확인 (DB 생성 안 함)")
    parser.add_argument("--sim-only",  action="store_true", help="sim.db 만 처리")
    parser.add_argument("--mmean-only",action="store_true", help="mmean.db 만 처리")
    parser.add_argument("--trades",    type=int, default=10_000,
                        help="sim_trades 추출 건수 (기본: 10000)")
    args = parser.parse_args()

    if args.status:
        show_status()
        return

    log.info("개발용 DB 추출 시작 | %s", datetime.now().strftime("%Y-%m-%d %H:%M:%S"))

    if args.dry_run:
        log.info("[DRY-RUN 모드] 실제 파일 생성 없음")

    if not args.mmean_only:
        export_sim(trades_limit=args.trades, dry_run=args.dry_run)

    if not args.sim_only:
        export_mmean(dry_run=args.dry_run)

    if not args.dry_run:
        log.info("완료. FileZilla로 전송할 파일:")
        log.info("  %s  (%.1f MB)", DST_SIM_DB,   _db_size_mb(DST_SIM_DB))
        log.info("  %s  (%.1f MB)", DST_MMEAN_DB, _db_size_mb(DST_MMEAN_DB))


if __name__ == "__main__":
    main()
