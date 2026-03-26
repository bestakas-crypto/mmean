# MMEAN/replay_sim.py
"""
장 마감 후 레벨별 재시뮬레이션 — 독립 실행 프로그램

사용법:
    python replay_sim.py --date 2026-03-17 --all-levels
    python replay_sim.py --date 2026-03-17 --level 7
    python replay_sim.py --date 2026-03-17 --level 7 --report-only
    python replay_sim.py --date 2026-03-17 --all-levels --force

원리:
    1. regime_ticks (session_date 기준) 읽기
       → BiasRegimeEngine 없이 저장된 bias/score/llm 값 그대로 재사용
    2. levels_v2.json 에서 레벨 config 로드
    3. ReplaySimEngine이 각 틱마다 진입/청산 판단
    4. replay_runs / replay_trades 에 결과 저장
    5. 레벨별 성적표 출력
"""
from __future__ import annotations

import argparse
import json
import logging
import os
import sqlite3
import sys
from datetime import datetime
from typing import Dict, List, Optional, Any

log = logging.getLogger("MMEAN.ReplaySim")

# levels_v2.json 기본 경로
_DEFAULT_LEVEL_JSON = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "workspace", "levels_v2.json",
)

# DB 기본 경로 (sim_opt/ → 프로젝트 루트 / storage)
_DEFAULT_DB = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
                           "storage", "mmean.db")

# 재생 시 레짐 엔진 웜업 제외 틱 수 (BiasRegimeEngine history_limit 기준)
_WARMUP_TICKS = 200

# 배치 커밋 주기
_BATCH_SIZE = 50

# 진입 허용 session_phase (None이면 전 구간)
_ENTRY_PHASES = {"mid"}

# 강제 청산 시각 (HH:MM 이상이면 청산)
_FORCE_EXIT_TIME = "14:45"

# 틱당 선물 1포인트 = 250,000원 (KOSPI200 선물 기준)
_TICK_VALUE = 0.05   # 0.05포인트 = 1틱


# ────────────────────────────────────────────────────────────────────
# 레벨 config 로더
# ────────────────────────────────────────────────────────────────────

def load_levels(level_json_path: str = _DEFAULT_LEVEL_JSON) -> Dict[int, Dict]:
    """levels_v2.json → {level_no: merged_config} 반환 (fixed + level 병합).

    JSON 구조 두 가지 지원:
      A) { "fixed": {...}, "1": {...}, "2": {...} }          ← 최상위 숫자 키
      B) { "fixed": {...}, "levels": { "1": {...}, ... } }   ← levels 중첩 구조
    """
    with open(level_json_path, encoding="utf-8") as f:
        raw = json.load(f)

    fixed: Dict = raw.get("fixed", {})

    # B 구조 우선, 없으면 A 구조 폴백
    if "levels" in raw and isinstance(raw["levels"], dict):
        levels_raw: Dict = raw["levels"]
    else:
        levels_raw = {k: v for k, v in raw.items() if str(k).isdigit()}

    result = {}
    for k, level_data in levels_raw.items():
        cfg = dict(fixed)
        cfg.update({ky: v for ky, v in level_data.items()
                    if ky not in ("label", "style", "desc")})
        result[int(k)] = cfg
    return result


# ────────────────────────────────────────────────────────────────────
# 단일 포지션 상태
# ────────────────────────────────────────────────────────────────────

class _Position:
    __slots__ = (
        "direction", "entry_price", "entry_ts", "entry_tick_idx",
        "entry_long_score", "entry_short_score", "entry_confidence",
        "entry_llm_score", "entry_llm_valid", "entry_session_phase",
        "max_favorable", "max_adverse",
        "trailing_active", "extreme_price",
        "neutral_tick_count",
    )

    def __init__(self, direction: str, price: float, ts: str, tick_idx: int,
                 row: Dict):
        self.direction          = direction      # "LONG" / "SHORT"
        self.entry_price        = price
        self.entry_ts           = ts
        self.entry_tick_idx     = tick_idx
        self.entry_long_score   = row.get("long_score", 0.0) or 0.0
        self.entry_short_score  = row.get("short_score", 0.0) or 0.0
        self.entry_confidence   = row.get("confidence", 0.0) or 0.0
        self.entry_llm_score    = row.get("llm_filter_score") or -1.0
        self.entry_llm_valid    = int(row.get("llm_filter_valid") or 0)
        self.entry_session_phase= row.get("session_phase") or ""
        self.max_favorable      = 0.0
        self.max_adverse        = 0.0
        self.trailing_active    = False
        self.extreme_price      = price
        self.neutral_tick_count = 0

    def pnl_ticks(self, current_price: float) -> float:
        diff = current_price - self.entry_price
        if self.direction == "LONG":
            return round(diff / _TICK_VALUE, 2)
        else:
            return round(-diff / _TICK_VALUE, 2)

    def update_extremes(self, current_price: float) -> None:
        pnl = self.pnl_ticks(current_price)
        if pnl > self.max_favorable:
            self.max_favorable = pnl
        if pnl < self.max_adverse:
            self.max_adverse = pnl
        # trailing용 extreme price 갱신
        if self.direction == "LONG":
            if current_price > self.extreme_price:
                self.extreme_price = current_price
        else:
            if current_price < self.extreme_price:
                self.extreme_price = current_price


# ────────────────────────────────────────────────────────────────────
# ReplaySimEngine — 단일 레벨 / 단일 날짜 재생
# ────────────────────────────────────────────────────────────────────

class ReplaySimEngine:
    """
    저장된 regime_ticks 행을 순서대로 받아 진입·청산을 판단한다.
    BiasRegimeEngine 없음 — regime_ticks의 bias/score가 이미 연산된 값.
    """

    def __init__(self, config: Dict):
        self.cfg    = config
        self._pos: Optional[_Position] = None
        self.trades: List[Dict] = []

    # ── 설정 헬퍼 ────────────────────────────────────────────────────

    def _cf(self, key: str, default=0.0) -> float:
        v = self.cfg.get(key, default)
        try:
            return float(v) if v is not None else default
        except (TypeError, ValueError):
            return default

    def _ci(self, key: str, default: int = 0) -> int:
        v = self.cfg.get(key, default)
        try:
            return int(v) if v is not None else default
        except (TypeError, ValueError):
            return default

    # ── 진입 조건 ────────────────────────────────────────────────────

    @staticmethod
    def _calc_phase(ts: str) -> str:
        """ts 문자열 'YYYY-MM-DD HH:MM:...'에서 session_phase 계산."""
        hhmm = ts[11:16] if len(ts) > 15 else "00:00"
        if   hhmm < "09:30": return "opening"
        elif hhmm < "14:30": return "mid"
        else:                 return "closing"

    def _check_entry(self, row: Dict, tick_idx: int) -> Optional[str]:
        """진입 가능하면 'LONG'/'SHORT', 불가면 None."""
        entry_signal = str(row.get("entry_signal") or "")
        bias         = str(row.get("bias") or "")
        long_score   = float(row.get("long_score")  or 0.0)
        short_score  = float(row.get("short_score") or 0.0)
        confidence   = float(row.get("confidence")  or 0.0)
        llm_valid    = int(row.get("llm_filter_valid") or 0)
        llm_score    = float(row.get("llm_filter_score") or -1.0)
        # session_phase: DB 저장값 우선, None이면 ts에서 자동 계산
        raw_phase = row.get("session_phase")
        if raw_phase:
            phase = str(raw_phase)
        else:
            phase = self._calc_phase(str(row.get("ts") or ""))

        # ① entry_signal 확인
        if entry_signal not in ("LONG_READY", "SHORT_READY"):
            return None

        # ② session_phase 필터
        if phase not in _ENTRY_PHASES:
            return None

        direction = "LONG" if entry_signal == "LONG_READY" else "SHORT"

        # ③ 점수 임계값
        enter_score = self._cf("enter_score", 0.0)
        if direction == "LONG"  and long_score  < enter_score:
            return None
        if direction == "SHORT" and short_score < enter_score:
            return None

        # ④ 점수 차 임계값
        enter_gap = self._cf("enter_gap", 0.0)
        if abs(long_score - short_score) < enter_gap:
            return None

        # ⑤ confidence 임계값
        min_confidence = self._cf("min_confidence", 0.0)
        if confidence < min_confidence:
            return None

        # ⑥ LLM 필터 (레벨 config에 llm_required=true인 경우만)
        if self.cfg.get("llm_required"):
            if not llm_valid:
                return None
            llm_gate = self._cf("llm_min_score", 0.0)
            if llm_score < llm_gate:
                return None

        return direction

    # ── 청산 조건 ────────────────────────────────────────────────────

    def _check_exit(self, row: Dict, pos: _Position) -> Optional[str]:
        """청산 사유 문자열 반환, 유지면 None."""
        price = float(row.get("futures_price") or pos.entry_price)
        bias  = str(row.get("bias") or "")
        ts    = str(row.get("ts") or "")
        pos.update_extremes(price)
        pnl = pos.pnl_ticks(price)

        # ① 강제 청산 시각
        if ts[11:16] >= _FORCE_EXIT_TIME:
            return "force_exit"

        # ② TP
        tp = self._cf("sim_tp_ticks", 0.0)
        if tp > 0 and pnl >= tp:
            return "tp"

        # ③ SL
        sl = self._cf("sim_sl_ticks", 0.0)
        if sl > 0 and pnl <= -sl:
            return "sl"

        # ④ Trailing stop
        trailing = self._cf("sim_trailing_ticks", 0.0)
        activate = self._cf("sim_trailing_activate", 0.0)
        if trailing > 0:
            if not pos.trailing_active and pnl >= activate:
                pos.trailing_active = True
            if pos.trailing_active:
                if pos.direction == "LONG":
                    retracement = pos.extreme_price - price
                    if retracement / _TICK_VALUE >= trailing:
                        return "trailing"
                else:
                    retracement = price - pos.extreme_price
                    if retracement / _TICK_VALUE >= trailing:
                        return "trailing"

        # ⑤ 레짐 중립 유지 N틱
        neutral_exit = self._ci("sim_neutral_exit_ticks", 0)
        if neutral_exit > 0:
            if bias == "NEUTRAL":
                pos.neutral_tick_count += 1
                if pos.neutral_tick_count >= neutral_exit:
                    return "regime_neutral"
            else:
                pos.neutral_tick_count = 0

        return None

    # ── 메인 on_tick ─────────────────────────────────────────────────

    def on_tick(self, row: Dict, tick_idx: int) -> Optional[Dict]:
        """포지션 없으면 진입 체크, 있으면 청산 체크. 청산 시 trade dict 반환."""
        price = float(row.get("futures_price") or 0.0)
        ts    = str(row.get("ts") or "")

        if self._pos is None:
            direction = self._check_entry(row, tick_idx)
            if direction:
                slippage = self._cf("sim_slippage_ticks", 1.0) * _TICK_VALUE
                entry_px = price + (slippage if direction == "LONG" else -slippage)
                self._pos = _Position(direction, entry_px, ts, tick_idx, row)
        else:
            exit_reason = self._check_exit(row, self._pos)
            if exit_reason:
                pos = self._pos
                slippage = self._cf("sim_slippage_ticks", 1.0) * _TICK_VALUE
                exit_px  = price - (slippage if pos.direction == "LONG" else -slippage)
                hold = tick_idx - pos.entry_tick_idx
                trade = {
                    "open_ts":               pos.entry_ts,
                    "close_ts":              ts,
                    "direction":             pos.direction,
                    "entry_price":           round(pos.entry_price, 4),
                    "exit_price":            round(exit_px, 4),
                    "pnl_ticks":             pos.pnl_ticks(exit_px),
                    "exit_reason":           exit_reason,
                    "hold_ticks":            hold,
                    "max_favorable_pt":      round(pos.max_favorable, 2),
                    "max_adverse_excursion": round(pos.max_adverse, 2),
                    "entry_long_score":      round(pos.entry_long_score, 4),
                    "entry_short_score":     round(pos.entry_short_score, 4),
                    "entry_confidence":      round(pos.entry_confidence, 4),
                    "entry_llm_score":       round(pos.entry_llm_score, 4),
                    "entry_llm_valid":       pos.entry_llm_valid,
                    "entry_session_phase":   pos.entry_session_phase,
                }
                self.trades.append(trade)
                self._pos = None
                return trade
        return None

    def force_close(self, row: Dict, tick_idx: int) -> Optional[Dict]:
        """장 마감 시 미청산 포지션 강제 청산."""
        if self._pos:
            row2 = dict(row)
            row2["ts"] = row2.get("ts", "") [:16].replace(" ", "T") + ":00"
            return self.on_tick({**row, "entry_signal": ""}, tick_idx)
        return None


# ────────────────────────────────────────────────────────────────────
# ReplaySimRunner — 날짜 × 레벨 실행기
# ────────────────────────────────────────────────────────────────────

class ReplaySimRunner:
    def __init__(self, db_path: str = _DEFAULT_DB,
                 level_json_path: str = _DEFAULT_LEVEL_JSON):
        self.db_path        = db_path
        self.level_json_path = level_json_path
        self._levels: Optional[Dict[int, Dict]] = None

    def _get_levels(self) -> Dict[int, Dict]:
        if self._levels is None:
            self._levels = load_levels(self.level_json_path)
        return self._levels

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path, timeout=30, check_same_thread=False)
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL")
        conn.execute("PRAGMA busy_timeout=15000")
        conn.row_factory = sqlite3.Row
        return conn

    # ── tick 로드 ────────────────────────────────────────────────────

    def load_ticks(self, session_date: str) -> List[Dict]:
        conn = self._connect()
        try:
            cur = conn.execute(
                "SELECT * FROM regime_ticks WHERE ts LIKE ? ORDER BY ts",
                (f"{session_date}%",)
            )
            rows = [dict(r) for r in cur.fetchall()]
            return rows
        finally:
            conn.close()

    # ── 멱등성 체크 ──────────────────────────────────────────────────

    def _is_done(self, conn: sqlite3.Connection,
                 session_date: str, level_no: int) -> Optional[int]:
        row = conn.execute(
            "SELECT id FROM replay_runs WHERE session_date=? AND level_no=? AND status='done'",
            (session_date, level_no)
        ).fetchone()
        return row[0] if row else None

    # ── 단일 레벨 실행 ───────────────────────────────────────────────

    def run_level(self, session_date: str, level_no: int,
                  force: bool = False) -> Dict:
        ticks = self.load_ticks(session_date)
        if not ticks:
            log.warning("틱 없음 | date=%s", session_date)
            return {"status": "no_data", "level_no": level_no}

        levels = self._get_levels()
        if level_no not in levels:
            raise ValueError(f"레벨 {level_no} 없음 (levels_v2.json)")
        config = levels[level_no]

        conn = self._connect()
        try:
            # 멱등성: 이미 완료된 경우
            if not force:
                done_id = self._is_done(conn, session_date, level_no)
                if done_id:
                    log.info("이미 완료 | date=%s level=%d run_id=%d",
                             session_date, level_no, done_id)
                    return {"status": "skipped", "run_id": done_id}

            # 기존 미완료 run 삭제 (재실행 시)
            old = conn.execute(
                "SELECT id FROM replay_runs WHERE session_date=? AND level_no=?",
                (session_date, level_no)
            ).fetchone()
            if old:
                conn.execute("DELETE FROM replay_trades WHERE run_id=?", (old[0],))
                conn.execute("DELETE FROM replay_runs WHERE id=?", (old[0],))
                conn.commit()

            # replay_runs 레코드 생성
            started_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            import hashlib
            config_hash = hashlib.md5(
                json.dumps(config, sort_keys=True).encode()
            ).hexdigest()[:8]

            cur = conn.execute(
                """INSERT INTO replay_runs
                   (session_date, level_no, config_hash, config_json,
                    run_started_at, tick_count, warmup_ticks, status)
                   VALUES (?,?,?,?,?,?,?,'running')""",
                (session_date, level_no, config_hash,
                 json.dumps(config, ensure_ascii=False),
                 started_at, len(ticks), _WARMUP_TICKS)
            )
            run_id = cur.lastrowid
            conn.commit()

            # 재생
            engine = ReplaySimEngine(config)
            batch: List[tuple] = []

            for idx, row in enumerate(ticks):
                if idx < _WARMUP_TICKS:
                    continue
                trade = engine.on_tick(row, idx)
                if trade:
                    batch.append((
                        run_id, session_date, level_no,
                        trade["open_ts"], trade["close_ts"],
                        trade["direction"],
                        trade["entry_price"], trade["exit_price"],
                        trade["pnl_ticks"], trade["exit_reason"],
                        trade["hold_ticks"],
                        trade["max_favorable_pt"], trade["max_adverse_excursion"],
                        trade["entry_long_score"], trade["entry_short_score"],
                        trade["entry_confidence"],
                        trade["entry_llm_score"], trade["entry_llm_valid"],
                        trade["entry_session_phase"],
                    ))
                    if len(batch) >= _BATCH_SIZE:
                        _insert_trades_batch(conn, batch)
                        batch.clear()

            # 잔여 배치 flush
            if batch:
                _insert_trades_batch(conn, batch)

            # run 완료 업데이트
            finished_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            trade_count = len(engine.trades)
            conn.execute(
                """UPDATE replay_runs
                   SET status='done', run_finished_at=?, trade_count=?
                   WHERE id=?""",
                (finished_at, trade_count, run_id)
            )
            conn.commit()

            log.info("완료 | date=%s level=%02d run_id=%d trades=%d",
                     session_date, level_no, run_id, trade_count)
            return {
                "status":      "done",
                "run_id":      run_id,
                "level_no":    level_no,
                "tick_count":  len(ticks),
                "trade_count": trade_count,
                "trades":      engine.trades,
            }

        except Exception:
            conn.execute(
                "UPDATE replay_runs SET status='error' WHERE id=?", (run_id,)
            ) if 'run_id' in dir() else None
            conn.commit()
            raise
        finally:
            conn.close()

    # ── 전체 레벨 실행 ───────────────────────────────────────────────

    def run_all_levels(self, session_date: str, force: bool = False) -> List[Dict]:
        levels = self._get_levels()
        results = []
        for lvl in sorted(levels.keys()):
            try:
                r = self.run_level(session_date, lvl, force=force)
                results.append(r)
            except Exception as e:
                log.error("레벨 %d 실패: %s", lvl, e)
                results.append({"status": "error", "level_no": lvl, "error": str(e)})
        return results

    # ── 성적표 출력 ──────────────────────────────────────────────────

    def print_scorecard(self, session_date: str) -> None:
        conn = self._connect()
        try:
            rows = conn.execute("""
                SELECT
                    r.level_no,
                    COUNT(t.id)                                          AS trades,
                    ROUND(AVG(CASE WHEN t.pnl_ticks > 0 THEN 1.0 ELSE 0.0 END) * 100, 1) AS win_pct,
                    ROUND(SUM(t.pnl_ticks), 2)                          AS total_pnl,
                    ROUND(AVG(t.pnl_ticks), 2)                          AS avg_pnl,
                    ROUND(MIN(t.pnl_ticks), 2)                          AS worst,
                    ROUND(AVG(t.hold_ticks), 0)                         AS avg_hold,
                    ROUND(
                        CASE WHEN SUM(CASE WHEN t.pnl_ticks < 0 THEN ABS(t.pnl_ticks) ELSE 0 END) = 0
                        THEN 99.9
                        ELSE SUM(CASE WHEN t.pnl_ticks > 0 THEN t.pnl_ticks ELSE 0 END) /
                             SUM(CASE WHEN t.pnl_ticks < 0 THEN ABS(t.pnl_ticks) ELSE 0 END)
                        END, 2
                    )                                                    AS profit_factor
                FROM replay_runs r
                LEFT JOIN replay_trades t ON t.run_id = r.id
                WHERE r.session_date = ? AND r.status = 'done'
                GROUP BY r.level_no
                ORDER BY r.level_no
            """, (session_date,)).fetchall()

            if not rows:
                print(f"성적 데이터 없음 | {session_date}")
                return

            print(f"\n{'='*80}")
            print(f"  REPLAY SCORECARD  |  {session_date}")
            print(f"{'='*80}")
            print(f"{'레벨':>5}  {'거래':>5}  {'승%':>6}  {'총PnL':>9}  "
                  f"{'평균':>7}  {'최저':>7}  {'P/F':>5}  {'평균보유':>6}")
            print(f"{'-'*80}")

            best_row = max(rows, key=lambda r: (r["total_pnl"] or -9999))
            worst_row = min(rows, key=lambda r: (r["total_pnl"] or 9999))

            for r in rows:
                flag = ""
                if r["level_no"] == best_row["level_no"]:
                    flag = " ◀ BEST"
                elif r["level_no"] == worst_row["level_no"]:
                    flag = " ◀ WORST"
                low_data = " (데이터부족)" if (r["trades"] or 0) < 5 else ""
                print(
                    f"  L{r['level_no']:02d}  "
                    f"{r['trades'] or 0:5d}  "
                    f"{r['win_pct'] or 0:5.1f}%  "
                    f"{r['total_pnl'] or 0:+9.2f}t  "
                    f"{r['avg_pnl'] or 0:+7.2f}t  "
                    f"{r['worst'] or 0:+7.2f}t  "
                    f"{r['profit_factor'] or 0:5.2f}  "
                    f"{r['avg_hold'] or 0:6.0f}"
                    f"{flag}{low_data}"
                )
            print(f"{'='*80}\n")

        finally:
            conn.close()


# ────────────────────────────────────────────────────────────────────
# DB 헬퍼
# ────────────────────────────────────────────────────────────────────

def _insert_trades_batch(conn: sqlite3.Connection, batch: List[tuple]) -> None:
    conn.executemany("""
        INSERT INTO replay_trades (
            run_id, session_date, level_no,
            open_ts, close_ts, direction,
            entry_price, exit_price, pnl_ticks, exit_reason,
            hold_ticks, max_favorable_pt, max_adverse_excursion,
            entry_long_score, entry_short_score, entry_confidence,
            entry_llm_score, entry_llm_valid, entry_session_phase
        ) VALUES (?,?,?, ?,?,?, ?,?,?,?, ?,?,?, ?,?,?, ?,?,?)
    """, batch)
    conn.commit()


# ────────────────────────────────────────────────────────────────────
# CLI
# ────────────────────────────────────────────────────────────────────

def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="MMEAN 장 마감 후 레벨별 재시뮬레이션"
    )
    p.add_argument("--date",  required=True,
                   help="시뮬레이션 날짜 (YYYY-MM-DD)")
    p.add_argument("--level", type=int, default=None,
                   help="단일 레벨 번호 (1~20)")
    p.add_argument("--all-levels", action="store_true",
                   help="L01~L20 전체 실행")
    p.add_argument("--report-only", action="store_true",
                   help="재생 없이 기존 결과 성적표만 출력")
    p.add_argument("--force", action="store_true",
                   help="이미 완료된 날짜도 재실행")
    p.add_argument("--db",   default=_DEFAULT_DB,
                   help=f"DB 경로 (기본: {_DEFAULT_DB})")
    p.add_argument("--levels-json", default=_DEFAULT_LEVEL_JSON,
                   help=f"levels_v2.json 경로 (기본: {_DEFAULT_LEVEL_JSON})")
    return p.parse_args()


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    )
    args = _parse_args()
    runner = ReplaySimRunner(db_path=args.db, level_json_path=args.levels_json)

    if args.report_only:
        runner.print_scorecard(args.date)
        return

    if args.all_levels:
        print(f"[replay_sim] 전체 레벨 실행 | date={args.date} force={args.force}")
        runner.run_all_levels(args.date, force=args.force)
        runner.print_scorecard(args.date)
    elif args.level is not None:
        result = runner.run_level(args.date, args.level, force=args.force)
        print(f"[replay_sim] 완료 | level={args.level} "
              f"trades={result.get('trade_count', 0)} "
              f"status={result.get('status')}")
        runner.print_scorecard(args.date)
    else:
        print("--level N 또는 --all-levels 를 지정하세요.")
        sys.exit(1)


if __name__ == "__main__":
    main()
