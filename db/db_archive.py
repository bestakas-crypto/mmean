"""
db_archive.py — C드라이브 DB → F드라이브 누적 아카이브 이전 스크립트

목적:
  <프로젝트루트>/storage/sim.db   → F:\db\sim.db   (영구 누적)
  <프로젝트루트>/storage/mmean.db → F:\db\mmean.db (영구 누적)

동작:
  1. F드라이브 DB에 C드라이브 신규 데이터를 INSERT (중복 제외)
  2. F드라이브에 인덱스 복사
  3. C드라이브 DB에서 오래된 데이터 삭제 (최근 N일 유지)
  4. VACUUM으로 용량 회수
  5. (선택) F드라이브 아카이브 DB 중복 데이터 정리

실행 예시:
  python db_archive.py --status           # 용량 현황만 확인
  python db_archive.py --dry-run          # 실제 변경 없이 대상 건수 확인
  python db_archive.py                    # 기본 실행 (최근 7일 유지)
  python db_archive.py --keep-days 3      # 최근 3일 유지
  python db_archive.py --sim-only         # sim.db만 처리
  python db_archive.py --mmean-only       # mmean.db만 처리
  python db_archive.py --dedup            # F드라이브 중복 데이터 정리
  python db_archive.py --dedup --dry-run  # 중복 건수만 확인
"""

import sqlite3
import argparse
import logging
import os
import sys
from datetime import datetime, timedelta
from pathlib import Path

# ── 로깅 설정 ────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(
            Path(__file__).parent / "storage" / "db_archive.log",
            encoding="utf-8"
        ),
    ]
)
log = logging.getLogger("DBArchive")

# ── 경로 설정 ────────────────────────────────────────────────
# __file__ 기준 상대경로 → 프로젝트를 어느 드라이브/경로에 놓아도 동작
_STORAGE = Path(__file__).resolve().parent.parent / "storage"
C_SIM_DB   = str(_STORAGE / "sim.db")
C_MMEAN_DB = str(_STORAGE / "mmean.db")
F_DB_DIR   = r"F:\db"
F_SIM_DB   = os.path.join(F_DB_DIR, "sim.db")
F_MMEAN_DB = os.path.join(F_DB_DIR, "mmean.db")


# ════════════════════════════════════════════════════════════
# 공용 헬퍼
# ════════════════════════════════════════════════════════════

def _connect(path: str, timeout: int = 60) -> sqlite3.Connection:
    conn = sqlite3.connect(path, timeout=timeout)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.row_factory = sqlite3.Row
    return conn


def _table_exists(conn: sqlite3.Connection, table: str) -> bool:
    r = conn.execute(
        "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name=?",
        (table,)
    ).fetchone()
    return r[0] > 0


def _copy_schema(src: sqlite3.Connection, dst: sqlite3.Connection, table: str) -> None:
    """src의 테이블 DDL + 인덱스를 dst에 복사 (없는 경우만)."""
    row = src.execute(
        "SELECT sql FROM sqlite_master WHERE type='table' AND name=?", (table,)
    ).fetchone()
    if row and row["sql"]:
        try:
            dst.execute(row["sql"])
            dst.commit()
            log.info("    [스키마] 테이블 생성: %s", table)
        except sqlite3.OperationalError:
            pass

    idx_rows = src.execute(
        "SELECT sql FROM sqlite_master WHERE type='index' AND tbl_name=? AND sql IS NOT NULL",
        (table,)
    ).fetchall()
    for idx_row in idx_rows:
        if idx_row["sql"]:
            try:
                dst.execute(idx_row["sql"])
                dst.commit()
            except sqlite3.OperationalError:
                pass


# ════════════════════════════════════════════════════════════
# sim.db 아카이브
# ════════════════════════════════════════════════════════════

# (테이블명, id컬럼, 날짜컬럼)  None = sim_runs 연동 orphan 정리
_SIM_TABLES = [
    ("sim_runs",         "id",     "run_finished_at"),
    ("sim_observations", "id",     "created_at"),
    ("sim_run_summary",  "run_id", None),
    ("sim_run_sessions", "run_id", None),
    ("sim_adoptions",    "id",     "adopted_at"),
    ("rag_documents",    "id",     "created_at"),
]


def archive_sim_db(c_path: str, f_path: str, keep_days: int, dry_run: bool) -> None:
    """sim.db 아카이브: F드라이브 복사 → C드라이브 삭제 → VACUUM."""
    cutoff = (datetime.now() - timedelta(days=keep_days)).strftime("%Y-%m-%d %H:%M:%S")
    log.info("=" * 60)
    log.info("sim.db 아카이브 시작")
    log.info("  C드라이브: %s", c_path)
    log.info("  F드라이브: %s", f_path)
    log.info("  보존 기준: 최근 %d일 (cutoff=%s)", keep_days, cutoff)
    log.info("  DRY-RUN:   %s", dry_run)
    log.info("=" * 60)

    if not os.path.exists(c_path):
        log.error("C드라이브 sim.db 없음: %s", c_path)
        return

    os.makedirs(os.path.dirname(f_path), exist_ok=True)

    src = _connect(c_path)
    dst = _connect(f_path)
    total_copied = 0
    total_deleted = 0

    try:
        for table, id_col, date_col in _SIM_TABLES:
            if not _table_exists(src, table):
                log.debug("  [SKIP] %s — 테이블 없음", table)
                continue

            _copy_schema(src, dst, table)

            if date_col is None:
                continue  # orphan 정리에서 처리

            # ── 복사: NOT EXISTS로 중복 방지 (NOT IN 대비 성능 우수) ──
            try:
                dst.execute(f"ATTACH DATABASE '{c_path}' AS src_db")

                cand_count = dst.execute(f"""
                    SELECT COUNT(*) FROM src_db.{table} s
                    WHERE s.{date_col} IS NOT NULL
                      AND s.{date_col} < '{cutoff}'
                      AND NOT EXISTS (
                          SELECT 1 FROM main.{table} d
                          WHERE d.{id_col} = s.{id_col}
                      )
                """).fetchone()[0]
                log.info("  [%s] 복사 대상: %d건", table, cand_count)

                if cand_count > 0 and not dry_run:
                    dst.execute(f"""
                        INSERT INTO main.{table}
                        SELECT s.* FROM src_db.{table} s
                        WHERE s.{date_col} IS NOT NULL
                          AND s.{date_col} < '{cutoff}'
                          AND NOT EXISTS (
                              SELECT 1 FROM main.{table} d
                              WHERE d.{id_col} = s.{id_col}
                          )
                    """)
                    dst.commit()
                    log.info("  [%s] 복사 완료", table)
                    total_copied += cand_count
            finally:
                try:
                    dst.execute("DETACH DATABASE src_db")
                except Exception:
                    pass

            # ── 삭제: F드라이브에 존재 확인 후 삭제 ──────────────────
            try:
                src.execute(f"ATTACH DATABASE '{f_path}' AS dst_db")

                del_count = src.execute(f"""
                    SELECT COUNT(*) FROM {table} s
                    WHERE s.{date_col} IS NOT NULL
                      AND s.{date_col} < '{cutoff}'
                      AND EXISTS (
                          SELECT 1 FROM dst_db.{table} d
                          WHERE d.{id_col} = s.{id_col}
                      )
                """).fetchone()[0]
                log.info("  [%s] 삭제 대상: %d건", table, del_count)

                if del_count > 0 and not dry_run:
                    src.execute(f"""
                        DELETE FROM {table}
                        WHERE {date_col} IS NOT NULL
                          AND {date_col} < '{cutoff}'
                          AND EXISTS (
                              SELECT 1 FROM dst_db.{table} d
                              WHERE d.{id_col} = {table}.{id_col}
                          )
                    """)
                    src.commit()
                    log.info("  [%s] 삭제 완료", table)
                    total_deleted += del_count
            finally:
                try:
                    src.execute("DETACH DATABASE dst_db")
                except Exception:
                    pass

        # ── orphan 정리 ───────────────────────────────────────────────
        if not dry_run:
            for ref_table in ("sim_run_summary", "sim_run_sessions"):
                if _table_exists(src, ref_table):
                    r = src.execute(
                        f"SELECT COUNT(*) FROM {ref_table} "
                        f"WHERE NOT EXISTS ("
                        f"  SELECT 1 FROM sim_runs WHERE sim_runs.id = {ref_table}.run_id"
                        f")"
                    ).fetchone()[0]
                    if r > 0:
                        src.execute(
                            f"DELETE FROM {ref_table} "
                            f"WHERE NOT EXISTS ("
                            f"  SELECT 1 FROM sim_runs WHERE sim_runs.id = {ref_table}.run_id"
                            f")"
                        )
                        log.info("  [%s] orphan 삭제: %d건", ref_table, r)
            src.commit()

        if not dry_run:
            log.info("  [VACUUM] C드라이브 sim.db 용량 회수 중...")
            src.execute("VACUUM")
            log.info("  [VACUUM] 완료")

        log.info("sim.db 완료 — 복사=%d건  삭제=%d건", total_copied, total_deleted)

    finally:
        src.close()
        dst.close()


# ════════════════════════════════════════════════════════════
# mmean.db 아카이브
# ════════════════════════════════════════════════════════════

def archive_mmean_db(c_path: str, f_path: str, keep_days: int, dry_run: bool) -> None:
    """
    mmean.db 아카이브.
    regime_ticks에 UNIQUE 제약 없음 → ts 기준 NOT EXISTS로 중복 방지.
    """
    cutoff_date = (datetime.now() - timedelta(days=keep_days)).strftime("%Y-%m-%d")
    log.info("=" * 60)
    log.info("mmean.db 아카이브 시작")
    log.info("  C드라이브: %s", c_path)
    log.info("  F드라이브: %s", f_path)
    log.info("  보존 기준: 최근 %d일 (cutoff=%s)", keep_days, cutoff_date)
    log.info("=" * 60)

    if not os.path.exists(c_path):
        log.error("C드라이브 mmean.db 없음: %s", c_path)
        return

    os.makedirs(os.path.dirname(f_path), exist_ok=True)

    src = _connect(c_path)
    dst = _connect(f_path)
    total_copied = 0
    total_deleted = 0

    try:
        table  = "regime_ticks"
        ts_col = "ts"

        if not _table_exists(src, table):
            log.warning("  regime_ticks 테이블 없음")
            return

        _copy_schema(src, dst, table)

        # ── 복사: ts 기준 NOT EXISTS (UNIQUE 제약 없으므로) ──────────
        try:
            dst.execute(f"ATTACH DATABASE '{c_path}' AS src_db")

            cand_count = dst.execute(f"""
                SELECT COUNT(*) FROM src_db.{table} s
                WHERE date(s.{ts_col}) < '{cutoff_date}'
                  AND NOT EXISTS (
                      SELECT 1 FROM main.{table} d
                      WHERE d.{ts_col} = s.{ts_col}
                  )
            """).fetchone()[0]
            log.info("  [%s] 복사 대상: %d건 (date < %s)", table, cand_count, cutoff_date)

            if cand_count > 0 and not dry_run:
                dst.execute(f"""
                    INSERT INTO main.{table}
                    SELECT s.* FROM src_db.{table} s
                    WHERE date(s.{ts_col}) < '{cutoff_date}'
                      AND NOT EXISTS (
                          SELECT 1 FROM main.{table} d
                          WHERE d.{ts_col} = s.{ts_col}
                      )
                """)
                dst.commit()
                log.info("  [%s] 복사 완료", table)
                total_copied += cand_count
        finally:
            try:
                dst.execute("DETACH DATABASE src_db")
            except Exception:
                pass

        # ── 삭제: F드라이브 확인 후 C드라이브 삭제 ──────────────────
        try:
            src.execute(f"ATTACH DATABASE '{f_path}' AS dst_db")

            del_count = src.execute(f"""
                SELECT COUNT(*) FROM {table} s
                WHERE date(s.{ts_col}) < '{cutoff_date}'
                  AND EXISTS (
                      SELECT 1 FROM dst_db.{table} d
                      WHERE d.{ts_col} = s.{ts_col}
                  )
            """).fetchone()[0]
            log.info("  [%s] 삭제 대상: %d건", table, del_count)

            if del_count > 0 and not dry_run:
                src.execute(f"""
                    DELETE FROM {table}
                    WHERE date({ts_col}) < '{cutoff_date}'
                      AND EXISTS (
                          SELECT 1 FROM dst_db.{table} d
                          WHERE d.{ts_col} = {table}.{ts_col}
                      )
                """)
                src.commit()
                log.info("  [%s] 삭제 완료", table)
                total_deleted += del_count
        finally:
            try:
                src.execute("DETACH DATABASE dst_db")
            except Exception:
                pass

        if not dry_run:
            log.info("  [VACUUM] C드라이브 mmean.db 용량 회수 중...")
            src.execute("VACUUM")
            log.info("  [VACUUM] 완료")

        log.info("mmean.db 완료 — 복사=%d건  삭제=%d건", total_copied, total_deleted)

    finally:
        src.close()
        dst.close()


# ════════════════════════════════════════════════════════════
# F드라이브 아카이브 DB 중복 정리
# ════════════════════════════════════════════════════════════

def dedup_archive(dry_run: bool) -> None:
    """F드라이브 아카이브 DB의 중복 데이터 정리."""
    log.info("=" * 60)
    log.info("F드라이브 아카이브 DB 중복 정리")
    log.info("  DRY-RUN: %s", dry_run)
    log.info("=" * 60)

    # ── sim.db 중복 정리 ──────────────────────────────────────
    if os.path.exists(F_SIM_DB):
        conn = _connect(F_SIM_DB)
        try:
            log.info("  [F:sim.db] 중복 검사 중...")
            for table, id_col, _ in _SIM_TABLES:
                if not _table_exists(conn, table) or id_col is None:
                    continue
                # id 기준 중복: 동일 id가 2개 이상인 경우 rowid가 작은 것 유지
                dup = conn.execute(f"""
                    SELECT COUNT(*) FROM {table}
                    WHERE rowid NOT IN (
                        SELECT MIN(rowid) FROM {table} GROUP BY {id_col}
                    )
                """).fetchone()[0]
                log.info("  [%s] 중복: %d건", table, dup)
                if dup > 0 and not dry_run:
                    conn.execute(f"""
                        DELETE FROM {table}
                        WHERE rowid NOT IN (
                            SELECT MIN(rowid) FROM {table} GROUP BY {id_col}
                        )
                    """)
                    conn.commit()
                    log.info("  [%s] 중복 삭제 완료: %d건", table, dup)

            if not dry_run:
                log.info("  [VACUUM] F:sim.db 정리 중...")
                conn.execute("VACUUM")
                log.info("  [VACUUM] 완료")
        finally:
            conn.close()
    else:
        log.info("  [SKIP] F:sim.db 없음")

    # ── mmean.db 중복 정리 (ts 기준) ─────────────────────────
    if os.path.exists(F_MMEAN_DB):
        conn = _connect(F_MMEAN_DB)
        try:
            log.info("  [F:mmean.db] regime_ticks 중복 검사 중...")
            if _table_exists(conn, "regime_ticks"):
                dup = conn.execute("""
                    SELECT COUNT(*) FROM regime_ticks
                    WHERE rowid NOT IN (
                        SELECT MIN(rowid) FROM regime_ticks GROUP BY ts
                    )
                """).fetchone()[0]
                log.info("  [regime_ticks] 중복: %d건 (ts 기준)", dup)
                if dup > 0 and not dry_run:
                    conn.execute("""
                        DELETE FROM regime_ticks
                        WHERE rowid NOT IN (
                            SELECT MIN(rowid) FROM regime_ticks GROUP BY ts
                        )
                    """)
                    conn.commit()
                    log.info("  [regime_ticks] 중복 삭제 완료: %d건", dup)

                if not dry_run:
                    log.info("  [VACUUM] F:mmean.db 정리 중...")
                    conn.execute("VACUUM")
                    log.info("  [VACUUM] 완료")
        finally:
            conn.close()
    else:
        log.info("  [SKIP] F:mmean.db 없음")

    log.info("F드라이브 중복 정리 완료")


# ════════════════════════════════════════════════════════════
# 용량 현황 출력
# ════════════════════════════════════════════════════════════

def show_status() -> None:
    log.info("=" * 60)
    log.info("DB 용량 현황")
    for label, path in [
        ("C sim.db  ",  C_SIM_DB),
        ("C mmean.db",  C_MMEAN_DB),
        ("F sim.db  ",  F_SIM_DB),
        ("F mmean.db",  F_MMEAN_DB),
    ]:
        if os.path.exists(path):
            size_mb = os.path.getsize(path) / 1024 / 1024
            log.info("  %s  %8.1f MB  (%s)", label, size_mb, path)
        else:
            log.info("  %s  [없음]       (%s)", label, path)
    log.info("=" * 60)


# ════════════════════════════════════════════════════════════
# CLI
# ════════════════════════════════════════════════════════════

def main() -> None:
    parser = argparse.ArgumentParser(
        description="C드라이브 DB → F드라이브 누적 아카이브"
    )
    parser.add_argument("--keep-days",  type=int, default=7,
                        help="C드라이브에 유지할 최근 일수 (기본: 7일)")
    parser.add_argument("--dry-run",    action="store_true",
                        help="실제 삭제 없이 대상 건수만 출력")
    parser.add_argument("--sim-only",   action="store_true",
                        help="sim.db만 처리")
    parser.add_argument("--mmean-only", action="store_true",
                        help="mmean.db만 처리")
    parser.add_argument("--status",     action="store_true",
                        help="용량 현황만 출력")
    parser.add_argument("--dedup",      action="store_true",
                        help="F드라이브 아카이브 DB 중복 정리")
    args = parser.parse_args()

    if args.status:
        show_status()
        return

    if args.dedup:
        show_status()
        dedup_archive(dry_run=args.dry_run)
        show_status()
        return

    show_status()

    if args.dry_run:
        log.info("★ DRY-RUN 모드 — 실제 데이터 변경 없음 ★")

    do_sim   = not args.mmean_only
    do_mmean = not args.sim_only

    if do_sim:
        archive_sim_db(C_SIM_DB, F_SIM_DB, args.keep_days, args.dry_run)

    if do_mmean:
        archive_mmean_db(C_MMEAN_DB, F_MMEAN_DB, args.keep_days, args.dry_run)

    show_status()
    log.info("전체 완료")


if __name__ == "__main__":
    main()
