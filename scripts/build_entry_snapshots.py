#!/usr/bin/env python3
"""
scripts/build_entry_snapshots.py
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Entry RAG — Phase 0: 진입 직전 스냅샷 추출 파이프라인

데이터 소스 2종 (--source):
  sim       [기본] sim.db의 sim_trades (batch 최적화 결과)
                   ← day_auto_run.py / night_auto_run.py 실행 후 생성
                   ← 컬럼: open_ts, pnl_ticks, run_id, session_date
                   ← 하루 수만 건 발생 → RAG 학습에 주력 소스
  trades           mmean.db의 trades (paper/live 실거래)
                   ← 현재 진입 신호 없어 비어있음

sim.db 위치: storage/sim.db  (day_auto_run.py / night_auto_run.py 실행 시 생성)
  직접 지정: python scripts/build_entry_snapshots.py --sim-db /path/to/sim.db

실행 예:
  python scripts/build_entry_snapshots.py                    # sim 기본
  python scripts/build_entry_snapshots.py --source trades    # 실거래
  python scripts/build_entry_snapshots.py --since 2026-03-01
  python scripts/build_entry_snapshots.py --dry-run
  python scripts/build_entry_snapshots.py --reset
  python scripts/build_entry_snapshots.py --summary
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
"""
from __future__ import annotations

import argparse
import json
import logging
import os
import sqlite3
import sys
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional

# ── 경로 ───────────────────────────────────────────────────────────────
_ROOT     = Path(__file__).resolve().parent.parent
_STORAGE  = _ROOT / "storage"
_MMEAN_DB = str(_STORAGE / "mmean.db")
_SIM_DB   = str(_STORAGE / "sim.db")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("entry_snap")

# ── 상수 ───────────────────────────────────────────────────────────────
WINDOW_SEC   = 45
TICK_VALUE   = 12_500   # mmean_sim의 pnl(원) → ticks 변환

_PHASE_THRESH: Dict[str, int] = {
    "opening": 15,
    "midday":  10,
    "closing":  6,
}
_DEFAULT_THRESH  = 10
_MAE_CLEAN_RATIO = 1.5

_DIR_MAP = {1: "LONG", -1: "SHORT"}


# ─────────────────────────────────────────────────────────────────────
# DB 초기화
# ─────────────────────────────────────────────────────────────────────

_CREATE_SQL = """
CREATE TABLE IF NOT EXISTS entry_snapshots (
    id                    INTEGER PRIMARY KEY AUTOINCREMENT,
    source                TEXT    NOT NULL DEFAULT 'sim',
    source_trade_id       INTEGER NOT NULL,
    source_run_id         INTEGER,
    UNIQUE(source, source_trade_id),

    session_date          TEXT    NOT NULL,
    entry_ts              TEXT    NOT NULL,
    direction             TEXT    NOT NULL,
    session_phase         TEXT,

    -- 결과 레이블
    pnl_ticks             REAL,
    max_adverse_excursion REAL,
    exit_reason           TEXT,
    outcome_class         INTEGER,
    pnl_atr_ratio         REAL,
    mae_atr_ratio         REAL,

    -- 진입 시점 컨텍스트
    bias_at_entry         TEXT,
    entry_signal_at       TEXT,
    atr_at_entry          REAL,
    futures_price         REAL,
    price_vs_vwap         REAL,
    flow_score_at         REAL,
    long_score_at         REAL,
    short_score_at        REAL,

    -- 45초 윈도우 집계
    tick_count            INTEGER,
    vb_last   REAL, vb_mean   REAL, vb_std  REAL, vb_slope  REAL,
    bed_last  REAL, bed_mean  REAL, bed_slope REAL,
    pvv_last  REAL, pvv_mean  REAL, pvv_slope REAL,
    fs_last   REAL, fs_mean   REAL, fs_slope  REAL,
    ffgn_last REAL, ffgn_sum  REAL, ffgn_slope REAL,
    efs_last  REAL, efs_mean  REAL,
    oid_last  REAL, oid_sum   REAL,

    features_json         TEXT,
    created_at            TEXT NOT NULL DEFAULT (datetime('now','localtime'))
);
CREATE INDEX IF NOT EXISTS idx_es_session   ON entry_snapshots(session_date);
CREATE INDEX IF NOT EXISTS idx_es_outcome   ON entry_snapshots(outcome_class);
CREATE INDEX IF NOT EXISTS idx_es_source    ON entry_snapshots(source);
CREATE INDEX IF NOT EXISTS idx_es_direction ON entry_snapshots(direction);
"""


def _init_table(conn: sqlite3.Connection) -> None:
    conn.executescript(_CREATE_SQL)
    conn.commit()


def _reset_source(conn: sqlite3.Connection, source: str) -> None:
    cnt = conn.execute(
        "SELECT COUNT(*) FROM entry_snapshots WHERE source=?", (source,)
    ).fetchone()[0]
    conn.execute("DELETE FROM entry_snapshots WHERE source=?", (source,))
    conn.commit()
    log.info("entry_snapshots[%s] %d건 초기화", source, cnt)


# ─────────────────────────────────────────────────────────────────────
# 피처 유틸
# ─────────────────────────────────────────────────────────────────────

def _col(rows: List[sqlite3.Row], c: str) -> List[float]:
    out = []
    for r in rows:
        try:
            v = r[c]
            if v is not None:
                out.append(float(v))
        except (KeyError, TypeError, ValueError):
            pass
    return out

def _safe(v: List[float]) -> float: return v[-1] if v else 0.0
def _mean(v: List[float]) -> float: return sum(v)/len(v) if v else 0.0
def _std(v: List[float]) -> float:
    if len(v) < 2: return 0.0
    m = _mean(v)
    return (sum((x-m)**2 for x in v)/len(v))**0.5
def _slope(v: List[float]) -> float:
    return (v[-1]-v[0])/len(v) if len(v) >= 2 else 0.0


# ─────────────────────────────────────────────────────────────────────
# session_phase 추론
# ─────────────────────────────────────────────────────────────────────

def _infer_phase(ts: str) -> str:
    try:
        t = datetime.strptime(ts, "%Y-%m-%d %H:%M:%S").time()
    except ValueError:
        return "midday"
    if t.hour == 9 and t.minute < 30: return "opening"
    if t.hour >= 14 and t.minute >= 30: return "closing"
    return "midday"


# ─────────────────────────────────────────────────────────────────────
# 레이블
# ─────────────────────────────────────────────────────────────────────

def _outcome(pnl: float, mae: float, phase: str) -> int:
    thr = _PHASE_THRESH.get(phase, _DEFAULT_THRESH)
    if pnl >= thr and mae <= max(pnl * _MAE_CLEAN_RATIO, thr): return 1
    if pnl <= -thr: return -1
    return 0


# ─────────────────────────────────────────────────────────────────────
# 소스별 체결 조회 → 통일된 dict 리스트
# ─────────────────────────────────────────────────────────────────────

def _fetch(
    source:     str,
    mmean_conn: sqlite3.Connection,
    sim_conn:   Optional[sqlite3.Connection],
    since:      Optional[str],
    until:      Optional[str],
) -> List[Dict[str, Any]]:

    rows = []
    params: List[Any] = []

    if source == "sim":
        # sim.db: open_ts, pnl_ticks, session_date, run_id
        where = ["pnl_ticks IS NOT NULL", "open_ts IS NOT NULL"]
        if since: where.append("session_date >= ?"); params.append(since)
        if until: where.append("session_date <= ?"); params.append(until)
        sql = f"""
            SELECT id, run_id, open_ts AS entry_ts,
                   direction,
                   pnl_ticks,
                   COALESCE(max_adverse_excursion,0) AS mae,
                   COALESCE(exit_reason,'') AS exit_reason,
                   entry_session_phase AS phase,
                   substr(open_ts,1,10) AS session_date
            FROM sim_trades
            WHERE {' AND '.join(where)}
            ORDER BY open_ts
        """
        raw = sim_conn.execute(sql, params).fetchall()
        for r in raw:
            rows.append({
                "id":         r["id"],
                "run_id":     r["run_id"],
                "entry_ts":   r["entry_ts"],
                "direction":  str(r["direction"] or ""),
                "pnl_ticks":  float(r["pnl_ticks"]),
                "mae":        float(r["mae"]),
                "exit_reason":str(r["exit_reason"]),
                "phase":      r["phase"] or _infer_phase(r["entry_ts"] or ""),
                "session_date": r["session_date"],
            })

    else:  # trades
        # mmean.db trades: open_ts, pnl_ticks, direction(정수)
        where = ["pnl_ticks IS NOT NULL", "open_ts IS NOT NULL"]
        if since: where.append("substr(open_ts,1,10) >= ?"); params.append(since)
        if until: where.append("substr(open_ts,1,10) <= ?"); params.append(until)
        sql = f"""
            SELECT id, NULL AS run_id, open_ts AS entry_ts,
                   direction,
                   pnl_ticks,
                   COALESCE(max_adverse_excursion,0) AS mae,
                   COALESCE(exit_reason,'') AS exit_reason,
                   NULL AS phase,
                   substr(open_ts,1,10) AS session_date
            FROM trades
            WHERE {' AND '.join(where)}
            ORDER BY open_ts
        """
        raw = mmean_conn.execute(sql, params).fetchall()
        for r in raw:
            try: dir_str = _DIR_MAP.get(int(r["direction"]), str(r["direction"]))
            except (TypeError, ValueError): dir_str = str(r["direction"] or "")
            rows.append({
                "id":         r["id"],
                "run_id":     None,
                "entry_ts":   r["entry_ts"],
                "direction":  dir_str,
                "pnl_ticks":  float(r["pnl_ticks"]),
                "mae":        float(r["mae"]),
                "exit_reason":str(r["exit_reason"]),
                "phase":      _infer_phase(r["entry_ts"] or ""),
                "session_date": r["session_date"],
            })

    return rows


# ─────────────────────────────────────────────────────────────────────
# 스냅샷 빌드 (체결 1건 → dict)
# ─────────────────────────────────────────────────────────────────────

def _build(
    t:          Dict[str, Any],
    source:     str,
    mmean_conn: sqlite3.Connection,
    window_sec: int,
) -> Optional[Dict[str, Any]]:

    entry_ts = t["entry_ts"]
    s_date   = t["session_date"]

    try:
        entry_dt = datetime.strptime(entry_ts, "%Y-%m-%d %H:%M:%S")
    except ValueError:
        return None

    win_start = (entry_dt - timedelta(seconds=window_sec)).strftime(
        "%Y-%m-%d %H:%M:%S"
    )

    ticks = mmean_conn.execute(
        """
        SELECT ts, bias, entry_signal,
               futures_price, basis_ema_delta, volume_burst,
               oi_delta, long_score, short_score,
               ema_fast_slope, vwap, atr, price_vs_vwap,
               flow_score, fut_fgn_delta
        FROM regime_ticks
        WHERE ts >= ?
          AND ts <= ?
          AND substr(ts,1,10) = ?
          AND futures_price > 100
        ORDER BY ts
        """,
        (win_start, entry_ts, s_date),
    ).fetchall()

    if len(ticks) < 10:
        return None

    vb   = _col(ticks, "volume_burst")
    bed  = _col(ticks, "basis_ema_delta")
    pvv  = _col(ticks, "price_vs_vwap")
    fs   = _col(ticks, "flow_score")
    ffgn = _col(ticks, "fut_fgn_delta")
    efs  = _col(ticks, "ema_fast_slope")
    oid  = _col(ticks, "oi_delta")

    last    = ticks[-1]
    atr_val = float(last["atr"] or 0.0)
    pnl     = t["pnl_ticks"]
    mae     = t["mae"]
    phase   = t["phase"]
    oc      = _outcome(pnl, mae, phase)

    return {
        "source":                source,
        "source_trade_id":       t["id"],
        "source_run_id":         t["run_id"],
        "session_date":          s_date,
        "entry_ts":              entry_ts,
        "direction":             t["direction"],
        "session_phase":         phase,
        "pnl_ticks":             pnl,
        "max_adverse_excursion": mae,
        "exit_reason":           t["exit_reason"],
        "outcome_class":         oc,
        "pnl_atr_ratio":         round(pnl/atr_val, 4) if atr_val > 0 else None,
        "mae_atr_ratio":         round(mae/atr_val, 4) if atr_val > 0 else None,
        "bias_at_entry":         str(last["bias"] or ""),
        "entry_signal_at":       str(last["entry_signal"] or ""),
        "atr_at_entry":          atr_val,
        "futures_price":         float(last["futures_price"] or 0.0),
        "price_vs_vwap":         float(last["price_vs_vwap"] or 0.0),
        "flow_score_at":         float(last["flow_score"] or 0.0),
        "long_score_at":         float(last["long_score"] or 0.0),
        "short_score_at":        float(last["short_score"] or 0.0),
        "tick_count":            len(ticks),
        "vb_last":  _safe(vb),  "vb_mean":  round(_mean(vb),4),
        "vb_std":   round(_std(vb),4), "vb_slope": round(_slope(vb),6),
        "bed_last": _safe(bed), "bed_mean": round(_mean(bed),5),
        "bed_slope":round(_slope(bed),7),
        "pvv_last": _safe(pvv), "pvv_mean": round(_mean(pvv),4),
        "pvv_slope":round(_slope(pvv),6),
        "fs_last":  _safe(fs),  "fs_mean":  round(_mean(fs),4),
        "fs_slope": round(_slope(fs),6),
        "ffgn_last":_safe(ffgn),"ffgn_sum": round(sum(ffgn),2),
        "ffgn_slope":round(_slope(ffgn),4),
        "efs_last": _safe(efs), "efs_mean": round(_mean(efs),6),
        "oid_last": _safe(oid), "oid_sum":  round(sum(oid),2),
        "features_json": json.dumps(
            [{"ts":  r["ts"],
              "fp":  r["futures_price"],
              "vb":  r["volume_burst"],
              "bed": r["basis_ema_delta"],
              "pvv": r["price_vs_vwap"],
              "fs":  r["flow_score"],
              "ffgn":r["fut_fgn_delta"],
              "efs": r["ema_fast_slope"],
              "oid": r["oi_delta"],
              "atr": r["atr"],
              "bias":r["bias"],
              "sig": r["entry_signal"]}
             for r in ticks],
            ensure_ascii=False,
        ),
    }


# ─────────────────────────────────────────────────────────────────────
# INSERT
# ─────────────────────────────────────────────────────────────────────

_INSERT = """
INSERT OR IGNORE INTO entry_snapshots (
    source, source_trade_id, source_run_id,
    session_date, entry_ts, direction, session_phase,
    pnl_ticks, max_adverse_excursion, exit_reason,
    outcome_class, pnl_atr_ratio, mae_atr_ratio,
    bias_at_entry, entry_signal_at, atr_at_entry,
    futures_price, price_vs_vwap, flow_score_at, long_score_at, short_score_at,
    tick_count,
    vb_last, vb_mean, vb_std, vb_slope,
    bed_last, bed_mean, bed_slope,
    pvv_last, pvv_mean, pvv_slope,
    fs_last, fs_mean, fs_slope,
    ffgn_last, ffgn_sum, ffgn_slope,
    efs_last, efs_mean, oid_last, oid_sum,
    features_json
) VALUES (
    :source, :source_trade_id, :source_run_id,
    :session_date, :entry_ts, :direction, :session_phase,
    :pnl_ticks, :max_adverse_excursion, :exit_reason,
    :outcome_class, :pnl_atr_ratio, :mae_atr_ratio,
    :bias_at_entry, :entry_signal_at, :atr_at_entry,
    :futures_price, :price_vs_vwap, :flow_score_at, :long_score_at, :short_score_at,
    :tick_count,
    :vb_last, :vb_mean, :vb_std, :vb_slope,
    :bed_last, :bed_mean, :bed_slope,
    :pvv_last, :pvv_mean, :pvv_slope,
    :fs_last, :fs_mean, :fs_slope,
    :ffgn_last, :ffgn_sum, :ffgn_slope,
    :efs_last, :efs_mean, :oid_last, :oid_sum,
    :features_json
)
"""


# ─────────────────────────────────────────────────────────────────────
# 메인
# ─────────────────────────────────────────────────────────────────────

def build_snapshots(
    mmean_db:   str  = _MMEAN_DB,
    sim_db:     str  = _SIM_DB,
    source:     str  = "sim",
    since:      Optional[str] = None,
    until:      Optional[str] = None,
    window_sec: int  = WINDOW_SEC,
    dry_run:    bool = False,
    reset:      bool = False,
) -> Dict[str, int]:

    if not os.path.exists(mmean_db):
        log.error("mmean.db 없음: %s", mmean_db); sys.exit(1)

    if source == "sim" and not os.path.exists(sim_db):
        log.error(
            "sim.db 없음: %s\n"
            "  sim.db는 배치 최적화 실행 시 자동 생성됩니다.\n"
            "  생성 방법: cd C:\\mmean && python scripts/day_auto_run.py\n"
            "  경로 직접 지정: --sim-db /path/to/sim.db\n"
            "  실거래 데이터를 쓰려면: --source trades",
            sim_db,
        )
        sys.exit(1)

    # ── 연결 분리: read(regime_ticks 조회) / write(entry_snapshots INSERT) ──
    read_conn  = sqlite3.connect(f"file:{mmean_db}?mode=ro", uri=True, timeout=30)
    read_conn.row_factory = sqlite3.Row
    write_conn = sqlite3.connect(mmean_db, timeout=30)
    write_conn.row_factory = sqlite3.Row
    write_conn.execute("PRAGMA journal_mode=WAL")

    sim_conn: Optional[sqlite3.Connection] = None
    if source == "sim":
        sim_conn = sqlite3.connect(f"file:{sim_db}?mode=ro", uri=True, timeout=10)
        sim_conn.row_factory = sqlite3.Row

    try:
        _init_table(write_conn)
        if reset:
            _reset_source(write_conn, source)

        existing = {
            r[0] for r in write_conn.execute(
                "SELECT source_trade_id FROM entry_snapshots WHERE source=?",
                (source,),
            ).fetchall()
        }
        log.info("기존 entry_snapshots[%s]: %d건", source, len(existing))

        trades = _fetch(source, read_conn, sim_conn, since, until)
        total  = len(trades)
        log.info(
            "처리 대상 [%s]: %d건 | %s ~ %s | 윈도우 %d초",
            source, total, since or "전체", until or "전체", window_sec,
        )

        saved = skipped = no_tick = err = 0
        batch: List[Dict] = []

        for i, t in enumerate(trades, 1):
            tid = t["id"]
            if tid in existing:
                skipped += 1
                continue
            try:
                snap = _build(t, source, read_conn, window_sec)
            except Exception as e:
                log.warning("빌드 오류 id=%s: %s", tid, e); err += 1; continue
            if snap is None:
                no_tick += 1; continue
            batch.append(snap)
            if len(batch) >= 200:
                if not dry_run:
                    write_conn.executemany(_INSERT, batch)
                    write_conn.commit()
                saved += len(batch)
                log.info("[%d/%d] 저장 %d | 스킵 %d | 틱부족 %d | 오류 %d",
                         i, total, saved, skipped, no_tick, err)
                batch.clear()

        if batch:
            if not dry_run:
                write_conn.executemany(_INSERT, batch)
                write_conn.commit()
            saved += len(batch)

    finally:
        read_conn.close()
        write_conn.close()
        if sim_conn:
            sim_conn.close()

    return {"processed": total-skipped, "saved": saved,
            "skipped": skipped, "no_tick": no_tick, "error": err}


# ─────────────────────────────────────────────────────────────────────
# 요약 출력
# ─────────────────────────────────────────────────────────────────────

def _print_summary(mmean_db: str) -> None:
    if not os.path.exists(mmean_db):
        return
    conn = sqlite3.connect(mmean_db, timeout=10)
    conn.row_factory = sqlite3.Row
    try:
        total = conn.execute(
            "SELECT COUNT(*) FROM entry_snapshots"
        ).fetchone()[0]
    except sqlite3.OperationalError:
        print("entry_snapshots 테이블 없음 — 먼저 빌드를 실행하세요.")
        conn.close(); return

    if total == 0:
        print("\nentry_snapshots: 0건")
        print("  → sim.db 생성: python scripts/day_auto_run.py")
        print("  → 스냅샷 빌드: python scripts/build_entry_snapshots.py")
        conn.close(); return

    print(f"\n{'='*60}")
    print(f"  entry_snapshots 요약  (총 {total:,}건)")
    print(f"{'='*60}")

    for r in conn.execute(
        "SELECT source, COUNT(*) cnt FROM entry_snapshots GROUP BY source"
    ).fetchall():
        print(f"  소스: {r['source']:12}  {r['cnt']:,}건")

    label = {1:"SUCCESS(+1)", 0:"NEUTRAL( 0)", -1:"FAILURE(-1)"}
    print(f"\n  {'결과':12} {'건수':>6}  {'비율':>6}  {'avg_pnl':>8}  {'avg_MAE':>8}")
    print(f"  {'-'*50}")
    for r in conn.execute("""
        SELECT outcome_class, COUNT(*) cnt,
               ROUND(AVG(pnl_ticks),2) avg_pnl,
               ROUND(AVG(max_adverse_excursion),2) avg_mae
        FROM entry_snapshots GROUP BY outcome_class ORDER BY outcome_class DESC
    """).fetchall():
        print(f"  {label.get(r['outcome_class'],str(r['outcome_class'])):12}"
              f" {r['cnt']:6,}  {r['cnt']/total*100:5.1f}%"
              f"  {r['avg_pnl']:8.2f}  {r['avg_mae']:8.2f}")

    print(f"\n  {'날짜':12} {'건수':>5}  {'SUC':>5}  {'FAI':>5}  {'avg_pnl':>8}")
    print(f"  {'-'*42}")
    for r in conn.execute("""
        SELECT session_date, COUNT(*) cnt,
               SUM(CASE WHEN outcome_class= 1 THEN 1 ELSE 0 END) s,
               SUM(CASE WHEN outcome_class=-1 THEN 1 ELSE 0 END) f,
               ROUND(AVG(pnl_ticks),2) avg_pnl
        FROM entry_snapshots GROUP BY session_date
        ORDER BY session_date DESC LIMIT 10
    """).fetchall():
        print(f"  {r['session_date']:12} {r['cnt']:5,}"
              f"  {r['s']:5,}  {r['f']:5,}  {r['avg_pnl']:8.2f}")

    print(f"\n  tick_count 분포")
    print(f"  {'-'*30}")
    for r in conn.execute("""
        SELECT CASE
            WHEN tick_count < 10 THEN '< 10'
            WHEN tick_count < 20 THEN '10~19'
            WHEN tick_count < 30 THEN '20~29'
            WHEN tick_count < 40 THEN '30~39'
            ELSE '40+'
        END band,
        COUNT(*) cnt
        FROM entry_snapshots GROUP BY 1 ORDER BY MIN(tick_count)
    """).fetchall():
        print(f"  {r['band']:8}  {r['cnt']:,}건")

    print(f"{'='*60}\n")
    conn.close()


# ─────────────────────────────────────────────────────────────────────
# CLI
# ─────────────────────────────────────────────────────────────────────

def main() -> None:
    p = argparse.ArgumentParser(description="Entry RAG Phase 0 — 스냅샷 추출")
    p.add_argument("--mmean-db", default=_MMEAN_DB)
    p.add_argument("--sim-db",   default=_SIM_DB)
    p.add_argument("--source",   default="sim",
                   choices=["sim", "trades"],
                   help="sim=sim.db배치최적화(기본,수만건) / trades=mmean.db실거래")
    p.add_argument("--since",    default=None, metavar="YYYY-MM-DD")
    p.add_argument("--until",    default=None, metavar="YYYY-MM-DD")
    p.add_argument("--window",   type=int, default=WINDOW_SEC)
    p.add_argument("--dry-run",  action="store_true")
    p.add_argument("--reset",    action="store_true")
    p.add_argument("--summary",  action="store_true")
    args = p.parse_args()

    if args.summary:
        _print_summary(args.mmean_db); return

    if args.dry_run:
        log.info("=== DRY-RUN (DB 저장 안함) ===")

    t0    = datetime.now()
    stats = build_snapshots(
        mmean_db=args.mmean_db, sim_db=args.sim_db,
        source=args.source, since=args.since, until=args.until,
        window_sec=args.window, dry_run=args.dry_run, reset=args.reset,
    )
    elapsed = (datetime.now() - t0).total_seconds()

    print(f"\n{'='*55}")
    print(f"  완료  source={args.source}  ({elapsed:.1f}초)")
    print(f"  처리 {stats['processed']:,}  저장 {stats['saved']:,}  "
          f"스킵 {stats['skipped']:,}  틱부족 {stats['no_tick']:,}  오류 {stats['error']:,}")
    print(f"{'='*55}")

    if not args.dry_run and stats["saved"] > 0:
        _print_summary(args.mmean_db)


if __name__ == "__main__":
    main()
