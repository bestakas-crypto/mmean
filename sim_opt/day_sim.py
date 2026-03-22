
# MMEAN/day_sim.py
"""
정규장 시뮬레이션 엔진 — night_sim.py 구조 복제/분기판

정규장 세션: session_date 08:45:00 ~ 15:35:00
- basis / foreign / LLM 사용 가능
- 진입 신호: entry_signal (LONG_READY / SHORT_READY)
- 진입 필터: long_score, short_score, confidence, flow_score,
             trade_strength, volume_burst, price_vs_vwap, LLM
- DB 분리 원칙:
    source_db (mmean.db) : regime_ticks 읽기 전용
    sim_db    (sim.db)   : sim_runs / sim_trades / sim_* 쓰기 전용

주의:
- 본 파일은 night_sim.py의 현재 구조를 day 세션 기준으로 분기한 버전이다.
- db_setup_sim.py는 기존 파일을 그대로 재사용한다.
- sim.db 스키마는 기존 night 파이프라인과 공용으로 사용한다.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import logging
import math
import os
import random
import sqlite3
import sys
from datetime import datetime
from typing import Dict, List, Optional, Tuple

log = logging.getLogger("MMEAN.DaySim")


# ── DB 처리 상세 로그 헬퍼 ─────────────────────────────────────────────

def _log_db(tag: str, **kv) -> None:
    """DB 조회/삽입/선택 단계 상세 로그 — 테이블명·조건·row수 포함."""
    parts = "  ".join(f"{k}={v!r}" for k, v in kv.items() if v is not None)
    log.info("[%s] %s", tag, parts)


def _log_db_preview(tag: str, rows: list, limit: int = 5) -> None:
    """상위 N개 row 미리보기 (study_name, obj, session_date, session_mode 출력)."""
    log.info("[%s] 상위 %d건 미리보기 (전체 %d건):", tag, min(limit, len(rows)), len(rows))
    for i, row in enumerate(rows[:limit], 1):
        sn  = row.get("study_name", "")
        obj = row.get("train_obj") or row.get("objective_score") or row.get("obj_score") or 0
        sd  = row.get("session_date", "")
        sm  = row.get("session_mode", "")
        ch  = (row.get("config_hash") or "")[:8]
        log.info("  [%s] %d. study=%r  obj=%+.2f  date=%s  mode=%s  hash=%s",
                 tag, i, sn, float(obj), sd, sm, ch)


# ── 경로 기본값 ──────────────────────────────────────────────────────
_STORAGE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "storage")

_DEFAULT_SOURCE_DB = os.path.join(_STORAGE, "mmean.db")   # 읽기 전용
_DEFAULT_SIM_DB    = os.path.join(_STORAGE, "sim.db")     # 쓰기 전용

_DEFAULT_DAY_JSON = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "workspace", "day_levels.json",
)

# ── 정규장 세션 시간 경계 ─────────────────────────────────────────────
_DAY_START       = "08:45:00"
_DAY_END         = "15:35:00"
_FORCE_EXIT_HHMM = "15:20"

# ── 재생 설정 ────────────────────────────────────────────────────────
_WARMUP_TICKS = 120
_BATCH_SIZE   = 50

# ── KOSPI200 선물 틱 값 ───────────────────────────────────────────────
_TICK_VALUE = 0.05  # 0.05pt = 1틱

# ── validation 기준 ──────────────────────────────────────────────────
_VALID_CRITERIA = {
    "min_objective":          0.0,
    "min_trade_count":        5,
    "max_drawdown_ticks":   -20.0,
    "min_session_pos_ratio":  0.50,
    "min_robustness":         0.40,
}

# ── objective 설정 ───────────────────────────────────────────────────
_MIN_TRADE_COUNT      = 5
_MAX_SAMPLE_ATTEMPTS  = 30

# ── 군집화용 파라미터 그룹 ────────────────────────────────────────────
_CLUSTER_FLOAT_PARAMS = [
    "enter_score",
    "enter_gap",
    "min_confidence",
    "flow_score_gate_thr",
    "llm_min_score",
    "sim_sl_ticks",
    "sim_tp_ticks",
    "sim_trailing_activate",
    "sim_trailing_ticks",
    "max_hold_ticks",
]
_CLUSTER_BOOL_PARAMS = [
    "llm_required",
    "llm_direction_match_required",
    "price_vs_vwap_gate",
    "flow_required",
]

# ── 정규장 랜덤 탐색 공간 ───────────────────────────────────────────
# 형식: {key: ("float"|"int"|"bool", [min, max])}
_DAY_SEARCH_SPACE: Dict[str, tuple] = {
    # 진입 필터
    "enter_score":                  ("float", 3.0, 7.0),
    "enter_gap":                    ("float", 0.5, 3.0),
    "min_confidence":               ("float", 0.45, 0.85),
    "flow_score_gate_thr":          ("float", 0.50, 1.50),
    "price_vs_vwap_gate":           ("bool",),
    "flow_required":                ("bool",),

    # LLM
    "llm_required":                 ("bool",),
    "llm_min_score":                ("float", 0.45, 0.85),
    "llm_direction_match_required": ("bool",),

    # 청산
    "day_regime_exit":              ("bool",),
    "sim_neutral_exit_ticks":       ("int",   0, 12),
    "sim_sl_ticks":                 ("float", 5.0, 25.0),
    "sim_tp_ticks":                 ("float", 8.0, 40.0),
    "sim_trailing_activate":        ("float", 3.0, 20.0),
    "sim_trailing_ticks":           ("float", 1.0,  8.0),
    "max_hold_ticks":               ("int",   50, 600),

    # 슬리피지
    "sim_slippage_ticks":           ("int",    1,   2),
}

# ── 기본 정규장 프로파일 ─────────────────────────────────────────────
_DEFAULT_PROFILES: Dict[int, Dict] = {
    1: dict(
        label="정규장 초보수형",
        enter_score=5.8,
        enter_gap=1.8,
        min_confidence=0.72,
        flow_score_gate_thr=0.95,
        price_vs_vwap_gate=True,
        flow_required=True,
        llm_required=True,
        llm_min_score=0.72,
        llm_direction_match_required=True,
        day_regime_exit=True,
        sim_neutral_exit_ticks=3,
        sim_sl_ticks=8,
        sim_tp_ticks=12,
        sim_trailing_activate=6,
        sim_trailing_ticks=4,
        max_hold_ticks=180,
        sim_slippage_ticks=1,
    ),
    2: dict(
        label="정규장 보수형",
        enter_score=5.0,
        enter_gap=1.4,
        min_confidence=0.66,
        flow_score_gate_thr=0.85,
        price_vs_vwap_gate=True,
        flow_required=True,
        llm_required=True,
        llm_min_score=0.65,
        llm_direction_match_required=True,
        day_regime_exit=True,
        sim_neutral_exit_ticks=4,
        sim_sl_ticks=10,
        sim_tp_ticks=16,
        sim_trailing_activate=8,
        sim_trailing_ticks=5,
        max_hold_ticks=220,
        sim_slippage_ticks=1,
    ),
    3: dict(
        label="정규장 중간형",
        enter_score=4.5,
        enter_gap=1.1,
        min_confidence=0.60,
        flow_score_gate_thr=0.75,
        price_vs_vwap_gate=False,
        flow_required=True,
        llm_required=True,
        llm_min_score=0.58,
        llm_direction_match_required=False,
        day_regime_exit=True,
        sim_neutral_exit_ticks=5,
        sim_sl_ticks=12,
        sim_tp_ticks=20,
        sim_trailing_activate=10,
        sim_trailing_ticks=6,
        max_hold_ticks=300,
        sim_slippage_ticks=1,
    ),
    4: dict(
        label="정규장 적극형",
        enter_score=4.0,
        enter_gap=0.9,
        min_confidence=0.55,
        flow_score_gate_thr=0.65,
        price_vs_vwap_gate=False,
        flow_required=False,
        llm_required=False,
        llm_min_score=0.55,
        llm_direction_match_required=False,
        day_regime_exit=True,
        sim_neutral_exit_ticks=6,
        sim_sl_ticks=14,
        sim_tp_ticks=24,
        sim_trailing_activate=12,
        sim_trailing_ticks=7,
        max_hold_ticks=360,
        sim_slippage_ticks=2,
    ),
    5: dict(
        label="정규장 공격형",
        enter_score=3.6,
        enter_gap=0.7,
        min_confidence=0.50,
        flow_score_gate_thr=0.55,
        price_vs_vwap_gate=False,
        flow_required=False,
        llm_required=False,
        llm_min_score=0.50,
        llm_direction_match_required=False,
        day_regime_exit=False,
        sim_neutral_exit_ticks=0,
        sim_sl_ticks=16,
        sim_tp_ticks=28,
        sim_trailing_activate=15,
        sim_trailing_ticks=8,
        max_hold_ticks=450,
        sim_slippage_ticks=2,
    ),
}


def load_day_levels(path: str = _DEFAULT_DAY_JSON) -> Dict[int, Dict]:
    """day_levels.json 로드 (없으면 기본 5개 프로파일 반환)."""
    if os.path.exists(path):
        with open(path, encoding="utf-8") as f:
            raw = json.load(f)
        fixed = raw.get("fixed", {})
        result = {}
        for k, v in raw.items():
            if str(k).isdigit():
                cfg = dict(fixed)
                cfg.update({ky: val for ky, val in v.items()
                            if ky not in ("label", "style", "desc")})
                result[int(k)] = cfg
        return result
    log.info("day_levels.json 없음 — 기본 프로파일 5개 사용")
    return _DEFAULT_PROFILES


class _DayPosition:
    __slots__ = (
        "direction", "entry_price", "entry_ts", "entry_tick_idx",
        "entry_confidence", "entry_long_score", "entry_short_score",
        "entry_llm_score", "entry_llm_valid",
        "max_favorable", "max_adverse",
        "trailing_active", "extreme_price",
        "neutral_tick_count",
    )

    def __init__(self, direction: str, price: float, ts: str,
                 tick_idx: int, row: Dict):
        self.direction         = direction
        self.entry_price       = price
        self.entry_ts          = ts
        self.entry_tick_idx    = tick_idx
        self.entry_confidence  = float(row.get("confidence") or 0.0)
        self.entry_long_score  = float(row.get("long_score") or 0.0)
        self.entry_short_score = float(row.get("short_score") or 0.0)
        self.entry_llm_score   = float(row.get("llm_filter_score") or -1.0)
        self.entry_llm_valid   = int(row.get("llm_filter_valid") or 0)
        self.max_favorable     = 0.0
        self.max_adverse       = 0.0
        self.trailing_active   = False
        self.extreme_price     = price
        self.neutral_tick_count = 0

    def pnl_ticks(self, current_price: float) -> float:
        diff = current_price - self.entry_price
        if self.direction == "LONG":
            return round(diff / _TICK_VALUE, 2)
        return round(-diff / _TICK_VALUE, 2)

    def update_extremes(self, current_price: float) -> None:
        pnl = self.pnl_ticks(current_price)
        if pnl > self.max_favorable:
            self.max_favorable = pnl
        if pnl < self.max_adverse:
            self.max_adverse = pnl

        if self.direction == "LONG":
            if current_price > self.extreme_price:
                self.extreme_price = current_price
        else:
            if current_price < self.extreme_price:
                self.extreme_price = current_price


class DaySimEngine:
    """
    저장된 regime_ticks 정규장 구간을 순서대로 받아 진입·청산 판단.
    진입: entry_signal + score/confidence + flow + VWAP + LLM 게이트
    청산: TP / SL / trailing / day neutral / max_hold / 강제(15:20)
    """

    def __init__(self, config: Dict, session_date: str):
        self.cfg = config
        self.session_date = session_date
        self._pos: Optional[_DayPosition] = None
        self.trades: List[Dict] = []

    def _cf(self, key: str, default: float = 0.0) -> float:
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

    def _score_gap(self, long_score: float, short_score: float, direction: str) -> float:
        return (long_score - short_score) if direction == "LONG" else (short_score - long_score)

    def _check_entry(self, row: Dict) -> Optional[str]:
        entry_signal   = str(row.get("entry_signal") or "")
        bias           = str(row.get("bias") or "")
        long_score     = float(row.get("long_score") or 0.0)
        short_score    = float(row.get("short_score") or 0.0)
        confidence     = float(row.get("confidence") or 0.0)
        trade_strength = float(row.get("trade_strength") or 100.0)
        volume_burst   = float(row.get("volume_burst") or 0.0)
        futures_price  = float(row.get("futures_price") or 0.0)
        vwap           = float(row.get("vwap") or 0.0)
        flow_score     = float(row.get("flow_score") or 0.0)
        llm_valid      = int(row.get("llm_filter_valid") or 0)
        llm_score      = float(row.get("llm_filter_score") or -1.0)
        llm_direction  = str(row.get("llm_filter_direction") or "").upper()

        if entry_signal not in ("LONG_READY", "SHORT_READY"):
            return None

        direction = "LONG" if entry_signal == "LONG_READY" else "SHORT"

        # bias 기본 방향 확인 (있으면 참고, 없으면 skip)
        if bias:
            if direction == "LONG" and bias.upper().startswith("BEAR"):
                return None
            if direction == "SHORT" and bias.upper().startswith("BULL"):
                return None

        # 점수/갭/신뢰도
        required_score = self._cf("enter_score")
        gap_req = self._cf("enter_gap")
        score_a = long_score if direction == "LONG" else short_score
        if score_a < required_score:
            return None
        if self._score_gap(long_score, short_score, direction) < gap_req:
            return None
        if confidence < self._cf("min_confidence"):
            return None

        # flow
        if self.cfg.get("flow_required") and flow_score < self._cf("flow_score_gate_thr"):
            return None

        # 체결강도/거래량 필터 (day는 완만)
        ts_min = self._cf("trade_strength_min", 0.0)
        if ts_min > 0:
            if direction == "LONG" and trade_strength < ts_min:
                return None
            if direction == "SHORT" and trade_strength > (200.0 - ts_min):
                return None

        vb_min = self._cf("volume_burst_min", 0.0)
        if vb_min > 0 and volume_burst < vb_min:
            return None

        # VWAP
        if self.cfg.get("price_vs_vwap_gate") and vwap > 0 and futures_price > 0:
            if direction == "LONG" and futures_price < vwap:
                return None
            if direction == "SHORT" and futures_price > vwap:
                return None

        # LLM
        if self.cfg.get("llm_required"):
            if llm_valid != 1:
                return None
            if llm_score < self._cf("llm_min_score"):
                return None
            if self.cfg.get("llm_direction_match_required"):
                if llm_direction not in ("LONG", "SHORT"):
                    return None
                if llm_direction != direction:
                    return None

        return direction

    def _check_exit(self, row: Dict, pos: _DayPosition, tick_idx: int) -> Optional[str]:
        price = float(row.get("futures_price") or pos.entry_price)
        ts    = str(row.get("ts") or "")
        pos.update_extremes(price)
        pnl = pos.pnl_ticks(price)

        # 강제 청산: 당일 15:20 이후
        ts_date = ts[:10]
        ts_hhmm = ts[11:16]
        if ts_date >= self.session_date and ts_hhmm >= _FORCE_EXIT_HHMM:
            return "force_exit"

        # TP
        tp = self._cf("sim_tp_ticks")
        if tp > 0 and pnl >= tp:
            return "tp"

        # SL
        sl = self._cf("sim_sl_ticks")
        if sl > 0 and pnl <= -sl:
            return "sl"

        # Trailing
        trailing = self._cf("sim_trailing_ticks")
        activate = self._cf("sim_trailing_activate")
        if trailing > 0:
            if not pos.trailing_active and pnl >= activate:
                pos.trailing_active = True
            if pos.trailing_active:
                if pos.direction == "LONG":
                    if (pos.extreme_price - price) / _TICK_VALUE >= trailing:
                        return "trailing"
                else:
                    if (price - pos.extreme_price) / _TICK_VALUE >= trailing:
                        return "trailing"

        # day neutral exit
        neutral_exit = self._ci("sim_neutral_exit_ticks")
        if neutral_exit > 0 and self.cfg.get("day_regime_exit"):
            entry_signal = str(row.get("entry_signal") or "")
            bias = str(row.get("bias") or "").upper()

            still_favorable = False
            if pos.direction == "LONG":
                still_favorable = entry_signal == "LONG_READY" or bias.startswith("BULL")
            else:
                still_favorable = entry_signal == "SHORT_READY" or bias.startswith("BEAR")

            if not still_favorable:
                pos.neutral_tick_count += 1
                if pos.neutral_tick_count >= neutral_exit:
                    return "day_regime_neutral"
            else:
                pos.neutral_tick_count = 0

        max_hold = self._ci("max_hold_ticks")
        if max_hold > 0 and (tick_idx - pos.entry_tick_idx) >= max_hold:
            return "max_hold"

        return None

    def on_tick(self, row: Dict, tick_idx: int) -> Optional[Dict]:
        price = float(row.get("futures_price") or 0.0)
        ts    = str(row.get("ts") or "")

        if self._pos is None:
            direction = self._check_entry(row)
            if direction:
                slippage = self._cf("sim_slippage_ticks", 1.0) * _TICK_VALUE
                entry_px = price + (slippage if direction == "LONG" else -slippage)
                self._pos = _DayPosition(direction, entry_px, ts, tick_idx, row)
        else:
            exit_reason = self._check_exit(row, self._pos, tick_idx)
            if exit_reason:
                pos = self._pos
                slippage = self._cf("sim_slippage_ticks", 1.0) * _TICK_VALUE
                exit_px = price - (slippage if pos.direction == "LONG" else -slippage)
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
                    "entry_session_phase":   "day",
                }
                self.trades.append(trade)
                self._pos = None
                return trade
        return None


def _canonical_dates(dates: List[str]) -> str:
    return json.dumps(sorted(set(dates)))


def _sample_random_config(rng: Optional[random.Random] = None) -> Dict:
    if rng is None:
        rng = random.Random()

    for _ in range(_MAX_SAMPLE_ATTEMPTS):
        cfg: Dict = {}
        for key, spec in _DAY_SEARCH_SPACE.items():
            if spec[0] == "float":
                cfg[key] = round(rng.uniform(spec[1], spec[2]), 3)
            elif spec[0] == "int":
                cfg[key] = rng.randint(int(spec[1]), int(spec[2]))
            elif spec[0] == "bool":
                cfg[key] = rng.choice([True, False])

        # 제약
        if cfg["sim_tp_ticks"] < cfg["sim_sl_ticks"] + 2:
            continue
        if cfg["sim_trailing_activate"] >= cfg["sim_tp_ticks"]:
            cfg["sim_trailing_activate"] = round(cfg["sim_tp_ticks"] * rng.uniform(0.4, 0.7), 1)
        if cfg["sim_trailing_ticks"] > cfg["sim_trailing_activate"]:
            cfg["sim_trailing_ticks"] = max(1.0, round(cfg["sim_trailing_activate"] * 0.5, 1))
        if not cfg.get("day_regime_exit"):
            cfg["sim_neutral_exit_ticks"] = 0

        cfg["label"] = "random"
        return cfg

    raise RuntimeError("랜덤 샘플링 제약 충족 실패 — 탐색 공간을 확인하세요")


def _insert_trades_batch(conn: sqlite3.Connection, batch: List[tuple]) -> None:
    conn.executemany("""
        INSERT INTO sim_trades (
            run_id, session_date, session_mode,
            open_ts, close_ts, direction,
            entry_price, exit_price, pnl_ticks, exit_reason,
            hold_ticks, max_favorable_pt, max_adverse_excursion,
            entry_score_a, entry_score_b, entry_confidence,
            entry_llm_score, entry_llm_valid, entry_session_phase
        ) VALUES (?,?,?, ?,?,?, ?,?,?,?, ?,?,?, ?,?,?, ?,?,?)
    """, batch)
    conn.commit()


def _insert_session_record(conn: sqlite3.Connection,
                           run_id: int,
                           session_date: str,
                           tick_count: int,
                           trades: List[Dict]) -> None:
    if not trades:
        conn.execute("""
            INSERT INTO sim_run_sessions
            (run_id, session_date, tick_count, trade_count,
             total_pnl, win_rate, profit_factor, max_drawdown, worst_trade)
            VALUES (?,?,?,0, 0.0,0.0,0.0,0.0,0.0)
        """, (run_id, session_date, tick_count))
        conn.commit()
        return

    pnl_list    = [t["pnl_ticks"] for t in trades]
    trade_count = len(pnl_list)
    total_pnl   = sum(pnl_list)
    win_rate    = sum(1 for p in pnl_list if p > 0) / trade_count
    worst_trade = min(pnl_list)
    wins   = [p for p in pnl_list if p > 0]
    losses = [abs(p) for p in pnl_list if p < 0]
    profit_factor = (sum(wins) / sum(losses)) if losses else 99.9

    peak = max_dd = cum = 0.0
    for p in pnl_list:
        cum += p
        if cum > peak:
            peak = cum
        dd = peak - cum
        if dd > max_dd:
            max_dd = dd

    conn.execute("""
        INSERT INTO sim_run_sessions
        (run_id, session_date, tick_count, trade_count,
         total_pnl, win_rate, profit_factor, max_drawdown, worst_trade)
        VALUES (?,?,?,?, ?,?,?,?,?)
    """, (run_id, session_date, tick_count, trade_count,
          total_pnl, win_rate, profit_factor, -max_dd, worst_trade))
    conn.commit()


def _compute_and_insert_summary(conn: sqlite3.Connection,
                                run_id: int,
                                trades: List[Dict],
                                session_count: int = 1,
                                session_pnl_list: Optional[List[float]] = None) -> float:
    now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    if not trades:
        conn.execute("""
            INSERT OR REPLACE INTO sim_run_summary
            (run_id, total_pnl, win_rate, profit_factor, avg_pnl,
             trade_count, avg_hold_ticks, worst_trade,
             max_drawdown, max_consecutive_loss,
             session_count, session_positive_ratio, pnl_std,
             low_trade_penalty, slippage_sensitivity,
             force_exit_ratio, mfe_realization_rate,
             objective_score, computed_at)
            VALUES (?, 0,0,0,0, 0,0,0, 0,0, ?,0,0, ?,0, 0,0, ?,?)
        """, (run_id, session_count, float(_MIN_TRADE_COUNT * 2), -999.0, now_str))
        conn.execute("UPDATE sim_runs SET objective_score=? WHERE id=?", (-999.0, run_id))
        conn.commit()
        return -999.0

    pnl_list  = [t["pnl_ticks"] for t in trades]
    hold_list = [t.get("hold_ticks", 0) for t in trades]
    trade_count = len(pnl_list)
    total_pnl   = sum(pnl_list)
    win_count   = sum(1 for p in pnl_list if p > 0)
    win_rate    = win_count / trade_count
    avg_pnl     = total_pnl / trade_count
    worst_trade = min(pnl_list)
    avg_hold    = sum(hold_list) / trade_count

    wins   = [p for p in pnl_list if p > 0]
    losses = [abs(p) for p in pnl_list if p < 0]
    profit_factor = (sum(wins) / sum(losses)) if losses else 99.9

    # ── 리포팅 전용 지표 (objective_score 에는 포함 안 함) ──────────────
    # force_exit_ratio: 강제청산(FORCE_EXIT_EOD) 의존도 — 높을수록 전략이 자체청산 못함
    _force_count = sum(
        1 for t in trades
        if str(t.get("exit_reason", "")).upper().startswith("FORCE")
    )
    force_exit_ratio = _force_count / trade_count

    # mfe_realization_rate: 유리 구간 실현률 — 낮으면 수익을 덜 챙기고 나옴
    _mfe_pairs = [
        (t["pnl_ticks"], t.get("max_favorable_pt", 0.0))
        for t in trades
        if (t.get("max_favorable_pt") or 0.0) > 0
    ]
    mfe_realization_rate = (
        sum(p / m for p, m in _mfe_pairs) / len(_mfe_pairs)
        if _mfe_pairs else 0.0
    )

    peak = max_dd = cum = 0.0
    for p in pnl_list:
        cum += p
        if cum > peak:
            peak = cum
        dd = peak - cum
        if dd > max_dd:
            max_dd = dd
    max_drawdown = -max_dd

    max_consec = curr_consec = 0
    for p in pnl_list:
        if p < 0:
            curr_consec += 1
            max_consec = max(max_consec, curr_consec)
        else:
            curr_consec = 0

    if session_pnl_list and len(session_pnl_list) > 1:
        session_positive_ratio = sum(1 for s in session_pnl_list if s > 0) / len(session_pnl_list)
        mean_s = sum(session_pnl_list) / len(session_pnl_list)
        pnl_std = math.sqrt(sum((s - mean_s) ** 2 for s in session_pnl_list) / len(session_pnl_list))
    else:
        session_positive_ratio = 1.0 if total_pnl > 0 else 0.0
        pnl_std = 0.0

    # 거래수 패널티 2계층
    if trade_count < 5:
        low_trade_penalty = float((5 - trade_count) * 4.0)
    elif trade_count < 10:
        low_trade_penalty = float((10 - trade_count) * 1.5)
    else:
        low_trade_penalty = 0.0

    slippage_sensitivity = float(trade_count * 2)

    pf_bonus   = (profit_factor - 1.0) * 5.0
    dd_penalty = abs(max_drawdown) * 0.3
    instability = pnl_std * 0.2
    objective_score = total_pnl + pf_bonus - dd_penalty - low_trade_penalty - instability

    conn.execute("""
        INSERT OR REPLACE INTO sim_run_summary
        (run_id, total_pnl, win_rate, profit_factor, avg_pnl,
         trade_count, avg_hold_ticks, worst_trade,
         max_drawdown, max_consecutive_loss,
         session_count, session_positive_ratio, pnl_std,
         low_trade_penalty, slippage_sensitivity,
         force_exit_ratio, mfe_realization_rate,
         objective_score, computed_at)
        VALUES (?,?,?,?,?, ?,?,?, ?,?, ?,?,?, ?,?, ?,?, ?,?)
    """, (
        run_id, total_pnl, win_rate, profit_factor, avg_pnl,
        trade_count, avg_hold, worst_trade,
        max_drawdown, max_consec,
        session_count, session_positive_ratio, pnl_std,
        low_trade_penalty, slippage_sensitivity,
        force_exit_ratio, mfe_realization_rate,
        objective_score, now_str,
    ))
    conn.execute("UPDATE sim_runs SET objective_score=? WHERE id=?", (objective_score, run_id))
    conn.commit()
    return objective_score


class DaySimRunner:
    """
    source_db (mmean.db) : regime_ticks 읽기 전용
    sim_db    (sim.db)   : sim_runs / sim_trades / sim_* 쓰기 전용
    """

    def __init__(self,
                 source_db: str = _DEFAULT_SOURCE_DB,
                 sim_db: str = _DEFAULT_SIM_DB,
                 day_json: str = _DEFAULT_DAY_JSON):
        self.source_db = source_db
        self.sim_db = sim_db
        self.day_json = day_json
        self._profiles: Optional[Dict[int, Dict]] = None

        from db_setup_sim import setup_sim_db
        setup_sim_db(sim_db)

    def _get_profiles(self) -> Dict[int, Dict]:
        if self._profiles is None:
            self._profiles = load_day_levels(self.day_json)
        return self._profiles

    def _connect_source(self) -> sqlite3.Connection:
        conn = sqlite3.connect(f"file:{self.source_db}?mode=ro", uri=True, timeout=30, check_same_thread=False)
        conn.row_factory = sqlite3.Row
        return conn

    def _connect_sim(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.sim_db, timeout=30, check_same_thread=False)
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL")
        conn.execute("PRAGMA busy_timeout=15000")
        conn.row_factory = sqlite3.Row
        return conn

    def load_day_ticks(self, session_date: str) -> List[Dict]:
        conn = self._connect_source()
        try:
            cur = conn.execute(
                "SELECT * FROM regime_ticks WHERE ts >= ? AND ts < ? ORDER BY ts",
                (f"{session_date} {_DAY_START}", f"{session_date} {_DAY_END}")
            )
            return [dict(r) for r in cur.fetchall()]
        finally:
            conn.close()

    def load_day_sessions(self, dates: List[str]) -> Dict[str, List[Dict]]:
        result: Dict[str, List[Dict]] = {}
        for d in sorted(set(dates)):
            ticks = self.load_day_ticks(d)
            result[d] = ticks
            log.info("정규장 틱 로드 | date=%s count=%d", d, len(ticks))
        return result

    def _is_done(self, conn: sqlite3.Connection, dates: List[str], profile_no: int) -> Optional[int]:
        row = conn.execute(
            """SELECT id FROM sim_runs
               WHERE source_dates=? AND profile_no=?
               AND run_type='profile' AND session_mode='day'
               AND status='done'""",
            (_canonical_dates(dates), profile_no)
        ).fetchone()
        return row[0] if row else None

    def _run_config_on_sessions(self,
                                conn: sqlite3.Connection,
                                run_id: int,
                                config: Dict,
                                dates_sorted: List[str],
                                sessions_ticks: Dict[str, List[Dict]]) -> Tuple[List[Dict], List[float]]:
        all_trades: List[Dict] = []
        session_pnl_list: List[float] = []
        batch: List[tuple] = []

        for session_date in dates_sorted:
            ticks = sessions_ticks[session_date]
            if not ticks:
                _insert_session_record(conn, run_id, session_date, 0, [])
                session_pnl_list.append(0.0)
                continue

            engine = DaySimEngine(config, session_date)

            for idx, row in enumerate(ticks):
                if idx < _WARMUP_TICKS:
                    continue
                trade = engine.on_tick(row, idx)
                if trade:
                    batch.append((
                        run_id, session_date, "day",
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

            session_pnl = sum(t["pnl_ticks"] for t in engine.trades)
            _insert_session_record(conn, run_id, session_date, len(ticks), engine.trades)
            session_pnl_list.append(session_pnl)
            all_trades.extend(engine.trades)

        if batch:
            _insert_trades_batch(conn, batch)

        return all_trades, session_pnl_list

    def run_profile(self, session_date: str, profile_no: int, force: bool = False) -> Dict:
        return self.run_profile_multi([session_date], profile_no, force=force)

    def run_profile_multi(self, dates: List[str], profile_no: int, force: bool = False) -> Dict:
        if not dates:
            raise ValueError("dates 목록이 비어 있음")

        dates_sorted = sorted(set(dates))
        source_dates_json = _canonical_dates(dates_sorted)
        profiles = self._get_profiles()
        if profile_no not in profiles:
            raise ValueError(f"정규장 프로파일 {profile_no} 없음")
        config = profiles[profile_no]

        conn = self._connect_sim()
        run_id = None
        try:
            if not force:
                done_id = self._is_done(conn, dates_sorted, profile_no)
                if done_id:
                    log.info("이미 완료 (멀티) | dates=%s profile=%d run_id=%d", dates_sorted, profile_no, done_id)
                    return {"status": "skipped", "run_id": done_id}

            old = conn.execute(
                """SELECT id FROM sim_runs
                   WHERE source_dates=? AND profile_no=?
                   AND run_type='profile' AND session_mode='day'""",
                (source_dates_json, profile_no)
            ).fetchone()
            if old:
                conn.execute("DELETE FROM sim_trades WHERE run_id=?", (old[0],))
                conn.execute("DELETE FROM sim_run_summary WHERE run_id=?", (old[0],))
                conn.execute("DELETE FROM sim_run_sessions WHERE run_id=?", (old[0],))
                conn.execute("DELETE FROM sim_runs WHERE id=?", (old[0],))
                conn.commit()

            sessions_ticks = self.load_day_sessions(dates_sorted)
            total_ticks = sum(len(v) for v in sessions_ticks.values())

            started_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            config_hash = hashlib.md5(json.dumps(config, sort_keys=True).encode()).hexdigest()[:8]

            cur = conn.execute(
                """INSERT INTO sim_runs
                   (run_type, session_mode, source_dates, date_count,
                    profile_no, config_hash, config_json,
                    run_started_at, tick_count_total, warmup_ticks, status)
                   VALUES ('profile','day',?,?, ?,?,?, ?,?,?,'running')""",
                (source_dates_json, len(dates_sorted), profile_no, config_hash,
                 json.dumps(config, sort_keys=True, ensure_ascii=False),
                 started_at, total_ticks, _WARMUP_TICKS)
            )
            run_id = cur.lastrowid
            conn.commit()

            all_trades, session_pnl_list = self._run_config_on_sessions(
                conn, run_id, config, dates_sorted, sessions_ticks
            )

            finished_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            trade_count = len(all_trades)
            conn.execute(
                """UPDATE sim_runs
                   SET status='done', run_finished_at=?, trade_count=?
                   WHERE id=?""",
                (finished_at, trade_count, run_id)
            )
            conn.commit()

            obj_score = _compute_and_insert_summary(
                conn, run_id, all_trades,
                session_count=len(dates_sorted),
                session_pnl_list=session_pnl_list,
            )

            log.info("완료 (day 멀티) | dates=%d개 profile=%02d run_id=%d trades=%d score=%.2f",
                     len(dates_sorted), profile_no, run_id, trade_count, obj_score)
            return {
                "status": "done",
                "run_id": run_id,
                "profile_no": profile_no,
                "date_count": len(dates_sorted),
                "tick_count": total_ticks,
                "trade_count": trade_count,
                "objective_score": obj_score,
                "session_pnl": session_pnl_list,
            }

        except Exception as exc:
            if run_id:
                try:
                    conn.execute(
                        """UPDATE sim_runs
                           SET status='error', error_message=?
                           WHERE id=?""",
                        (str(exc)[:500], run_id)
                    )
                    conn.commit()
                except Exception:
                    pass
            raise
        finally:
            conn.close()

    def run_all_profiles(self, session_date: str, force: bool = False) -> List[Dict]:
        return self.run_all_profiles_multi([session_date], force=force)

    def run_all_profiles_multi(self, dates: List[str], force: bool = False) -> List[Dict]:
        profiles = self._get_profiles()
        results = []
        for pno in sorted(profiles.keys()):
            try:
                r = self.run_profile_multi(dates, pno, force=force)
                results.append(r)
            except Exception as e:
                log.error("프로파일 %d 실패 (day 멀티): %s", pno, e)
                results.append({"status": "error", "profile_no": pno, "error": str(e)})
        return results

    # ── 탐색 공용: top configs 로드 ──────────────────────────────────
    def load_top_configs(self, study_name: str, top_n: int = 10) -> List[Dict]:
        conn = self._connect_sim()
        try:
            _log_db("LOAD_TOP_CONFIGS",
                    table="sim_observations+sim_runs+sim_run_summary",
                    study_name=study_name,
                    top_n=top_n,
                    session_mode="day",
                    run_type="random|bayes_opt")

            rows = conn.execute("""
                SELECT
                    o.trial_no,
                    o.run_id,
                    r.config_hash,
                    r.config_json,
                    s.objective_score,
                    s.trade_count,
                    s.total_pnl,
                    s.profit_factor,
                    s.max_drawdown,
                    s.session_positive_ratio
                FROM sim_observations o
                JOIN sim_runs r ON r.id = o.run_id
                LEFT JOIN sim_run_summary s ON s.run_id = o.run_id
                WHERE o.study_name=? AND r.status='done'
                  AND r.session_mode='day'
                  AND r.run_type IN ('random','bayes_opt')
                ORDER BY o.objective_score DESC
                LIMIT ?
            """, (study_name, top_n)).fetchall()

            _log_db("LOAD_TOP_CONFIGS",
                    result_count=len(rows),
                    note="0건이면 study_name 오류 또는 day run 없음")
            if not rows:
                log.warning("[LOAD_TOP_CONFIGS] 결과 0건 | study=%s", study_name)

            result = []
            for row in rows:
                result.append({
                    "trial_no": row["trial_no"],
                    "run_id": row["run_id"],
                    "config_hash": row["config_hash"],
                    "config_json": row["config_json"],
                    "config": json.loads(row["config_json"]),
                    "objective_score": float(row["objective_score"] or 0.0),
                    "trade_count": int(row["trade_count"] or 0),
                    "total_pnl": float(row["total_pnl"] or 0.0),
                    "profit_factor": float(row["profit_factor"] or 0.0),
                    "max_drawdown": float(row["max_drawdown"] or 0.0),
                    "session_positive_ratio": float(row["session_positive_ratio"] or 0.0),
                })
            return result
        finally:
            conn.close()

    def run_random_search(self,
                          dates: List[str],
                          n_trials: int = 100,
                          study_name: Optional[str] = None,
                          seed: Optional[int] = None,
                          force: bool = False,
                          session_date: Optional[str] = None,
                          batch_no: int = 1) -> List[Dict]:
        if not dates:
            raise ValueError("dates 목록이 비어 있음")

        rng = random.Random(seed)
        dates_sorted = sorted(set(dates))
        src_dates = _canonical_dates(dates_sorted)

        if study_name is None:
            dates_hash = hashlib.md5(src_dates.encode()).hexdigest()[:6]
            study_name = f"day_random_{dates_hash}_{datetime.now().strftime('%Y%m%d_%H%M%S')}"

        log.info("정규장 랜덤 탐색 시작 | study=%s n=%d dates=%s seed=%s",
                 study_name, n_trials, dates_sorted, seed)
        _log_db("RANDOM_SEARCH",
                step="start",
                table="sim_runs+sim_observations",
                study_name=study_name,
                n_trials=n_trials,
                dates=dates_sorted,
                session_date=session_date or dates_sorted[0],
                session_mode="day",
                batch_no=batch_no)

        sessions_ticks = self.load_day_sessions(dates_sorted)
        total_ticks = sum(len(v) for v in sessions_ticks.values())

        conn = self._connect_sim()
        results: List[Dict] = []
        try:
            if force:
                old_ids = [r[0] for r in conn.execute(
                    "SELECT id FROM sim_runs WHERE study_name=? AND run_type='random' AND session_mode='day'",
                    (study_name,)
                ).fetchall()]
                if old_ids:
                    placeholders = ",".join("?" * len(old_ids))
                    conn.execute(f"DELETE FROM sim_trades WHERE run_id IN ({placeholders})", old_ids)
                    conn.execute(f"DELETE FROM sim_run_summary WHERE run_id IN ({placeholders})", old_ids)
                    conn.execute(f"DELETE FROM sim_run_sessions WHERE run_id IN ({placeholders})", old_ids)
                    conn.execute(f"DELETE FROM sim_runs WHERE id IN ({placeholders})", old_ids)
                    conn.execute("DELETE FROM sim_observations WHERE study_name=?", (study_name,))
                    conn.commit()

            for trial_no in range(n_trials):
                if not force:
                    done = conn.execute(
                        """SELECT id FROM sim_runs
                           WHERE study_name=? AND trial_no=?
                             AND run_type='random' AND session_mode='day'
                             AND status='done'""",
                        (study_name, trial_no)
                    ).fetchone()
                    if done:
                        continue

                config = _sample_random_config(rng)
                cfg_json = json.dumps(config, sort_keys=True, ensure_ascii=False)
                config_hash = hashlib.md5(cfg_json.encode()).hexdigest()[:8]
                started_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

                cur = conn.execute(
                    """INSERT INTO sim_runs
                       (run_type, session_mode, source_dates, date_count,
                        trial_no, study_name, config_hash, config_json,
                        run_started_at, tick_count_total, warmup_ticks, status)
                       VALUES ('random','day',?,?, ?,?,?,?, ?,?,?,'running')""",
                    (src_dates, len(dates_sorted), trial_no, study_name, config_hash, cfg_json,
                     started_at, total_ticks, _WARMUP_TICKS)
                )
                run_id = cur.lastrowid
                conn.commit()

                try:
                    all_trades, session_pnl_list = self._run_config_on_sessions(
                        conn, run_id, config, dates_sorted, sessions_ticks
                    )

                    finished_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                    trade_count = len(all_trades)
                    conn.execute(
                        "UPDATE sim_runs SET status='done', run_finished_at=?, trade_count=? WHERE id=?",
                        (finished_at, trade_count, run_id)
                    )
                    conn.commit()

                    obj_score = _compute_and_insert_summary(
                        conn, run_id, all_trades,
                        session_count=len(dates_sorted),
                        session_pnl_list=session_pnl_list,
                    )

                    _sd = session_date or dates_sorted[0]
                    conn.execute(
                        """INSERT INTO sim_observations
                           (study_name, trial_no, run_id,
                            session_date, session_mode, batch_no,
                            config_json, objective_score, created_at)
                           VALUES (?,?,?, ?,?,?, ?,?,?)""",
                        (study_name, trial_no, run_id,
                         _sd, "day", batch_no,
                         cfg_json, obj_score, finished_at)
                    )
                    conn.commit()
                    log.debug("[RANDOM] INSERT obs trial_no=%s obj=%.4f run_id=%s",
                              trial_no, obj_score, run_id)

                    results.append({
                        "trial_no": trial_no,
                        "run_id": run_id,
                        "objective_score": obj_score,
                        "trade_count": trade_count,
                        "config": config,
                    })

                    if len(results) % 10 == 0:
                        best_score = max(r["objective_score"] for r in results)
                        print(f"  [{len(results):4d}/{n_trials}] 완료 | 최고={best_score:+.2f}", flush=True)

                except Exception as exc:
                    conn.execute(
                        "UPDATE sim_runs SET status='error', error_message=? WHERE id=?",
                        (str(exc)[:500], run_id)
                    )
                    conn.commit()
                    log.warning("Trial %d 실패: %s", trial_no, exc)

            results.sort(key=lambda r: r["objective_score"], reverse=True)
            _log_db("RANDOM_SEARCH",
                    step="done",
                    study_name=study_name,
                    completed=len(results),
                    n_trials=n_trials)
            return results
        finally:
            conn.close()

    def run_bayes_opt(self,
                      dates: List[str],
                      n_trials: int = 100,
                      study_name: Optional[str] = None,
                      seed: Optional[int] = None,
                      force: bool = False,
                      n_startup_trials: int = 30,
                      session_date: Optional[str] = None,
                      batch_no: int = 1) -> List[Dict]:
        try:
            import optuna
        except ImportError as e:
            raise RuntimeError("optuna 가 필요합니다. pip install optuna") from e

        dates_sorted = sorted(set(dates))
        src_dates = _canonical_dates(dates_sorted)
        if study_name is None:
            dates_hash = hashlib.md5(src_dates.encode()).hexdigest()[:6]
            study_name = f"day_bo_{dates_hash}_{datetime.now().strftime('%Y%m%d_%H%M%S')}"

        _log_db("BAYES_OPT",
                step="start",
                table="sim_runs+sim_observations",
                study_name=study_name,
                n_trials=n_trials,
                dates=dates_sorted,
                session_date=session_date or dates_sorted[0],
                session_mode="day",
                batch_no=batch_no)

        sessions_ticks = self.load_day_sessions(dates_sorted)
        total_ticks = sum(len(v) for v in sessions_ticks.values())
        conn = self._connect_sim()

        if force:
            old_ids = [r[0] for r in conn.execute(
                "SELECT id FROM sim_runs WHERE study_name=? AND run_type='bayes_opt' AND session_mode='day'",
                (study_name,)
            ).fetchall()]
            if old_ids:
                placeholders = ",".join("?" * len(old_ids))
                conn.execute(f"DELETE FROM sim_trades WHERE run_id IN ({placeholders})", old_ids)
                conn.execute(f"DELETE FROM sim_run_summary WHERE run_id IN ({placeholders})", old_ids)
                conn.execute(f"DELETE FROM sim_run_sessions WHERE run_id IN ({placeholders})", old_ids)
                conn.execute(f"DELETE FROM sim_runs WHERE id IN ({placeholders})", old_ids)
                conn.execute("DELETE FROM sim_observations WHERE study_name=?", (study_name,))
                conn.commit()

        optuna.logging.set_verbosity(optuna.logging.WARNING)
        sampler = optuna.samplers.TPESampler(seed=seed, n_startup_trials=n_startup_trials)
        study = optuna.create_study(direction="maximize", sampler=sampler)

        results: List[Dict] = []

        def _suggest_config(trial) -> Dict:
            cfg = {
                "enter_score": trial.suggest_float("enter_score", 3.0, 7.0),
                "enter_gap": trial.suggest_float("enter_gap", 0.5, 3.0),
                "min_confidence": trial.suggest_float("min_confidence", 0.45, 0.85),
                "flow_score_gate_thr": trial.suggest_float("flow_score_gate_thr", 0.50, 1.50),
                "price_vs_vwap_gate": trial.suggest_categorical("price_vs_vwap_gate", [True, False]),
                "flow_required": trial.suggest_categorical("flow_required", [True, False]),
                "llm_required": trial.suggest_categorical("llm_required", [True, False]),
                "llm_min_score": trial.suggest_float("llm_min_score", 0.45, 0.85),
                "llm_direction_match_required": trial.suggest_categorical("llm_direction_match_required", [True, False]),
                "day_regime_exit": trial.suggest_categorical("day_regime_exit", [True, False]),
                "sim_neutral_exit_ticks": trial.suggest_int("sim_neutral_exit_ticks", 0, 12),
                "sim_sl_ticks": trial.suggest_float("sim_sl_ticks", 5.0, 25.0),
                "sim_tp_ticks": trial.suggest_float("sim_tp_ticks", 8.0, 40.0),
                "sim_trailing_activate": trial.suggest_float("sim_trailing_activate", 3.0, 20.0),
                "sim_trailing_ticks": trial.suggest_float("sim_trailing_ticks", 1.0, 8.0),
                "max_hold_ticks": trial.suggest_int("max_hold_ticks", 50, 600),
                "sim_slippage_ticks": trial.suggest_int("sim_slippage_ticks", 1, 2),
                "label": "bayes_opt",
            }

            # clamping
            cfg["sim_tp_ticks"] = max(cfg["sim_tp_ticks"], cfg["sim_sl_ticks"] + 2.0)
            if cfg["sim_trailing_activate"] >= cfg["sim_tp_ticks"]:
                cfg["sim_trailing_activate"] = round(cfg["sim_tp_ticks"] * 0.6, 1)
            if cfg["sim_trailing_ticks"] > cfg["sim_trailing_activate"]:
                cfg["sim_trailing_ticks"] = max(1.0, round(cfg["sim_trailing_activate"] * 0.5, 1))
            if not cfg["day_regime_exit"]:
                cfg["sim_neutral_exit_ticks"] = 0
            return cfg

        def objective(trial) -> float:
            config = _suggest_config(trial)
            cfg_json = json.dumps(config, sort_keys=True, ensure_ascii=False)
            config_hash = hashlib.md5(cfg_json.encode()).hexdigest()[:8]
            started_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

            cur = conn.execute(
                """INSERT INTO sim_runs
                   (run_type, session_mode, source_dates, date_count,
                    trial_no, study_name, config_hash, config_json,
                    run_started_at, tick_count_total, warmup_ticks, status)
                   VALUES ('bayes_opt','day',?,?, ?,?,?,?, ?,?,?,'running')""",
                (src_dates, len(dates_sorted), trial.number, study_name, config_hash, cfg_json,
                 started_at, total_ticks, _WARMUP_TICKS)
            )
            run_id = cur.lastrowid
            conn.commit()

            try:
                all_trades, session_pnl_list = self._run_config_on_sessions(
                    conn, run_id, config, dates_sorted, sessions_ticks
                )

                finished_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                trade_count = len(all_trades)
                conn.execute(
                    "UPDATE sim_runs SET status='done', run_finished_at=?, trade_count=? WHERE id=?",
                    (finished_at, trade_count, run_id)
                )
                conn.commit()

                obj_score = _compute_and_insert_summary(
                    conn, run_id, all_trades,
                    session_count=len(dates_sorted),
                    session_pnl_list=session_pnl_list,
                )

                _sd = session_date or dates_sorted[0]
                conn.execute(
                    """INSERT INTO sim_observations
                       (study_name, trial_no, run_id,
                        session_date, session_mode, batch_no,
                        config_json, objective_score, created_at)
                       VALUES (?,?,?, ?,?,?, ?,?,?)""",
                    (study_name, trial.number, run_id,
                     _sd, "day", batch_no,
                     cfg_json, obj_score, finished_at)
                )
                conn.commit()
                log.debug("[BAYES] INSERT obs trial_no=%s obj=%.4f run_id=%s",
                          trial.number, obj_score, run_id)

                results.append({
                    "trial_no": trial.number,
                    "run_id": run_id,
                    "objective_score": obj_score,
                    "trade_count": trade_count,
                    "config": config,
                })

                if (trial.number + 1) % 10 == 0:
                    best_score = max(r["objective_score"] for r in results)
                    print(f"  [{trial.number + 1:4d}/{n_trials}] 완료 | 최고={best_score:+.2f}", flush=True)

                return obj_score

            except Exception as exc:
                conn.execute(
                    "UPDATE sim_runs SET status='error', error_message=? WHERE id=?",
                    (str(exc)[:500], run_id)
                )
                conn.commit()
                raise

        try:
            study.optimize(objective, n_trials=n_trials, catch=(Exception,))
        finally:
            conn.close()

        results.sort(key=lambda r: r["objective_score"], reverse=True)
        return results

    def print_random_topN(self, study_name: str, top_n: int = 10) -> None:
        conn = self._connect_sim()
        try:
            rows = conn.execute("""
                SELECT
                    o.trial_no,
                    o.run_id,
                    ROUND(COALESCE(o.objective_score, 0), 2) AS obj_score,
                    COALESCE(s.trade_count, 0) AS trades,
                    ROUND(COALESCE(s.win_rate, 0) * 100, 1) AS win_pct,
                    ROUND(COALESCE(s.total_pnl, 0), 2) AS total_pnl,
                    ROUND(COALESCE(s.profit_factor, 0), 2) AS profit_factor,
                    ROUND(COALESCE(s.max_drawdown, 0), 2) AS max_drawdown,
                    ROUND(COALESCE(s.pnl_std, 0), 2) AS pnl_std,
                    ROUND(COALESCE(s.session_positive_ratio, 0) * 100, 1) AS pos_pct,
                    r.date_count,
                    o.config_json
                FROM sim_observations o
                JOIN sim_runs r ON r.id = o.run_id
                LEFT JOIN sim_run_summary s ON s.run_id = o.run_id
                WHERE o.study_name=? AND r.status='done' AND r.session_mode='day'
                ORDER BY o.objective_score DESC
                LIMIT ?
            """, (study_name, top_n)).fetchall()

            if not rows:
                print(f"결과 없음 | study={study_name}")
                return

            W = 112
            print(f"\n{'='*W}")
            print(f"  DAY SEARCH TOP {top_n}  |  study={study_name}")
            print(f"{'='*W}")
            print(f"  {'순위':>3}  {'trial':>6}  {'run':>6}  {'목표점수':>8}  "
                  f"{'거래':>5}  {'승%':>6}  {'총PnL':>9}  "
                  f"{'P/F':>5}  {'낙폭':>7}  {'편차':>6}  {'양봉%':>6}")
            print(f"  {'-'*108}")
            for rank, r in enumerate(rows, 1):
                print(
                    f"  {rank:3d}  {r['trial_no']:6d}  {r['run_id']:6d}  "
                    f"{r['obj_score'] or 0:+8.2f}  "
                    f"{r['trades'] or 0:5d}  "
                    f"{r['win_pct'] or 0:5.1f}%  "
                    f"{r['total_pnl'] or 0:+9.2f}t  "
                    f"{r['profit_factor'] or 0:5.2f}  "
                    f"{r['max_drawdown'] or 0:+7.2f}t  "
                    f"{r['pnl_std'] or 0:6.2f}  "
                    f"{r['pos_pct'] or 0:5.1f}%"
                )
            print(f"{'='*W}")
            best_cfg = json.loads(rows[0]["config_json"])
            print(f"\n  ★ Best Config (trial={rows[0]['trial_no']}, run_id={rows[0]['run_id']}, score={rows[0]['obj_score']:+.2f}):")
            for k in sorted(best_cfg):
                if k != "label":
                    print(f"    {k:<34} = {best_cfg[k]}")
            print()
        finally:
            conn.close()

    def load_top_configs_by_date(
        self,
        session_date: str,
        session_mode: str = "day",
        top_n: int = 10,
    ) -> List[Dict]:
        """session_date + session_mode 기준 cross-study top-N 반환."""
        conn = self._connect_sim()
        try:
            _log_db("LOAD_TOP_BY_DATE",
                    table="sim_observations+sim_runs+sim_run_summary",
                    session_date=session_date,
                    session_mode=session_mode,
                    top_n=top_n,
                    filter="status=done, GROUP BY config_hash HAVING MAX(obj)")

            # 전체 관측치 수 먼저 확인 (0건이면 원인 즉시 판별)
            total_obs = conn.execute(
                "SELECT COUNT(*) FROM sim_observations WHERE session_date=? AND session_mode=?",
                (session_date, session_mode)
            ).fetchone()[0]
            _log_db("LOAD_TOP_BY_DATE",
                    sim_observations_total=total_obs,
                    note="0이면 run_random_search/run_bayes_opt에서 session_date/session_mode 미전달 또는 INSERT 누락")

            done_obs = conn.execute(
                """SELECT COUNT(*) FROM sim_observations o
                   JOIN sim_runs r ON r.id = o.run_id
                   WHERE o.session_date=? AND o.session_mode=? AND r.status='done'""",
                (session_date, session_mode)
            ).fetchone()[0]
            _log_db("LOAD_TOP_BY_DATE",
                    done_obs_count=done_obs,
                    note="0이면 탐색 run이 status=done 아님 또는 session_date 불일치")

            rows = conn.execute("""
                SELECT
                    r.id                      AS run_id,
                    r.trial_no,
                    r.config_hash,
                    r.config_json,
                    r.study_name,
                    COALESCE(o.objective_score, 0) AS train_obj,
                    COALESCE(s.trade_count,     0) AS train_trades,
                    COALESCE(s.total_pnl,       0) AS train_total_pnl
                FROM sim_observations o
                JOIN sim_runs r ON r.id = o.run_id
                LEFT JOIN sim_run_summary s ON s.run_id = r.id
                WHERE o.session_date = ?
                  AND o.session_mode = ?
                  AND r.status = 'done'
                GROUP BY r.config_hash
                HAVING MAX(o.objective_score)
                ORDER BY train_obj DESC
                LIMIT ?
            """, (session_date, session_mode, top_n)).fetchall()

            _log_db("LOAD_TOP_BY_DATE",
                    result_count=len(rows),
                    note="0건이면 GROUP BY 중복제거 후 top_n 미만이거나 모든 run 실패")

            if not rows:
                log.warning("[LOAD_TOP_BY_DATE] 결과 0건 — "
                            "session_date=%s mode=%s total_obs=%d done_obs=%d",
                            session_date, session_mode, total_obs, done_obs)

            result = []
            for row in rows:
                result.append({
                    "run_id":          row["run_id"],
                    "trial_no":        row["trial_no"],
                    "config_hash":     row["config_hash"] or "",
                    "config_json":     row["config_json"],
                    "config":          json.loads(row["config_json"]),
                    "study_name":      row["study_name"],
                    "train_obj":       float(row["train_obj"]),
                    "train_trades":    int(row["train_trades"]),
                    "train_total_pnl": float(row["train_total_pnl"]),
                })

            if result:
                _log_db_preview("LOAD_TOP_BY_DATE_PREVIEW", result)

            log.info("[LOAD_TOP_BY_DATE] day cross-study top-%d | session_date=%s → %d개",
                     top_n, session_date, len(result))
            return result
        finally:
            conn.close()

    def run_validation_by_date(
        self,
        train_date: str,
        session_mode: str,
        valid_dates: List[str],
        top_n: int = 10,
        force: bool = False,
    ) -> List[Dict]:
        """날짜/세션 단위 validation (cross-study). day용."""
        _log_db("VALIDATION_BY_DATE",
                step="1_load_top_configs",
                table="sim_observations+sim_runs",
                train_date=train_date,
                session_mode=session_mode,
                top_n=top_n)

        top_configs = self.load_top_configs_by_date(train_date, session_mode, top_n)

        _log_db("VALIDATION_BY_DATE",
                step="1_load_top_configs_result",
                top_configs_count=len(top_configs),
                note="0건이면 해당 날짜에 탐색 결과 없음 — run_random_search/run_bayes_opt 먼저 실행 필요")

        if not top_configs:
            log.warning("[VALIDATION_BY_DATE] session_date=%s mode=%s → 결과 0건 — validation 건너뜀",
                        train_date, session_mode)
            return []

        if len(top_configs) >= 3:
            _log_db_preview("VALIDATION_BY_DATE_TOP", top_configs)

        date_study_name = f"{session_mode}_{train_date}"
        dates_sorted    = sorted(set(valid_dates))
        src_dates_key   = _canonical_dates(dates_sorted)

        _log_db("VALIDATION_BY_DATE",
                step="2_load_sessions",
                table="regime_ticks",
                valid_dates=dates_sorted,
                date_count=len(dates_sorted))

        sessions_ticks = self.load_day_sessions(dates_sorted)
        total_ticks    = sum(len(v) for v in sessions_ticks.values())

        _log_db("VALIDATION_BY_DATE",
                step="2_sessions_loaded",
                session_count=len(sessions_ticks),
                total_ticks=total_ticks,
                note="세션 틱 0이면 regime_ticks에 해당 날짜 데이터 없음")

        conn    = self._connect_sim()
        results: List[Dict] = []
        ok_count  = 0
        skip_count = 0

        try:
            for rank, item in enumerate(top_configs, 1):
                cfg_hash = item["config_hash"]
                config   = item["config"]
                cfg_json = item["config_json"]

                if not force:
                    existing = conn.execute(
                        """SELECT id FROM sim_runs
                           WHERE study_name=? AND config_hash=?
                             AND run_type='validation'
                             AND source_dates=? AND status='done'""",
                        (date_study_name, cfg_hash, src_dates_key)
                    ).fetchone()
                    if existing:
                        _log_db("VALIDATION_BY_DATE",
                                step=f"rank_{rank}_skip",
                                config_hash=cfg_hash,
                                existing_run_id=existing[0],
                                note="이미 완료된 validation — force=True로 재실행 가능")
                        skip_count += 1
                        continue

                _log_db("VALIDATION_BY_DATE",
                        step=f"rank_{rank}_start",
                        table="sim_runs",
                        config_hash=cfg_hash,
                        study_name=date_study_name,
                        valid_dates=dates_sorted)

                started_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                cur = conn.execute(
                    """INSERT INTO sim_runs
                       (run_type, session_mode, source_dates, date_count,
                        study_name, config_hash, config_json,
                        run_started_at, tick_count_total, warmup_ticks, status)
                       VALUES ('validation',?,?,?, ?,?,?, ?,?,?,'running')""",
                    (session_mode, src_dates_key, len(dates_sorted),
                     date_study_name, cfg_hash, cfg_json,
                     started_at, total_ticks, _WARMUP_TICKS)
                )
                run_id = cur.lastrowid
                conn.commit()

                try:
                    all_trades, session_pnl_list = self._run_config_on_sessions(
                        conn, run_id, config, dates_sorted, sessions_ticks
                    )

                    finished_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                    trade_count = len(all_trades)
                    conn.execute(
                        "UPDATE sim_runs SET status='done', run_finished_at=?, trade_count=? WHERE id=?",
                        (finished_at, trade_count, run_id)
                    )
                    conn.commit()

                    obj_score = _compute_and_insert_summary(
                        conn, run_id, all_trades,
                        session_count=len(dates_sorted),
                        session_pnl_list=session_pnl_list,
                    )

                    _log_db("VALIDATION_BY_DATE",
                            step=f"rank_{rank}_done",
                            run_id=run_id,
                            config_hash=cfg_hash,
                            trade_count=trade_count,
                            obj_score=round(obj_score, 3))

                    results.append({
                        "rank":        rank,
                        "config_hash": cfg_hash,
                        "run_id":      run_id,
                        "obj_score":   obj_score,
                        "trades":      trade_count,
                        "study_name":  date_study_name,
                    })
                    ok_count += 1

                except Exception as exc:
                    conn.execute(
                        "UPDATE sim_runs SET status='error', error_message=? WHERE id=?",
                        (str(exc)[:500], run_id)
                    )
                    conn.commit()
                    log.warning("[VALIDATION_BY_DATE] rank=%d hash=%s 실패: %s",
                                rank, cfg_hash, exc)

        finally:
            conn.close()

        _log_db("VALIDATION_BY_DATE",
                step="DONE",
                train_date=train_date,
                mode=session_mode,
                total_top=len(top_configs),
                executed=ok_count,
                skipped=skip_count,
                results=len(results))
        return results

    def run_validation(self,
                       study_name: str,
                       valid_dates: List[str],
                       top_n: int = 10,
                       force: bool = False) -> List[Dict]:
        dates_sorted = sorted(set(valid_dates))
        src_dates_key = _canonical_dates(dates_sorted)

        _log_db("VALIDATION_STUDY",
                step="1_load_top_configs",
                table="sim_observations+sim_runs",
                study_name=study_name,
                top_n=top_n,
                valid_dates=dates_sorted)

        top_cfgs = self.load_top_configs(study_name, top_n=top_n)

        _log_db("VALIDATION_STUDY",
                step="1_result",
                top_cfgs_count=len(top_cfgs),
                note="0건이면 study_name 오류 또는 day run 없음")

        if not top_cfgs:
            log.warning("[VALIDATION_STUDY] validation 대상 config 없음 | study=%s", study_name)
            return []

        sessions_ticks = self.load_day_sessions(dates_sorted)
        total_ticks = sum(len(v) for v in sessions_ticks.values())

        _log_db("VALIDATION_STUDY",
                step="2_sessions",
                total_ticks=total_ticks,
                session_count=len(dates_sorted))

        conn = self._connect_sim()
        results: List[Dict] = []
        try:
            for rank, item in enumerate(top_cfgs, 1):
                cfg_hash = item["config_hash"]
                config = item["config"]
                train_run_id = item["run_id"]

                if not force:
                    done = conn.execute(
                        """SELECT id FROM sim_runs
                           WHERE study_name=? AND run_type='validation'
                             AND session_mode='day' AND source_dates=?
                             AND config_hash=? AND status='done'""",
                        (study_name, src_dates_key, cfg_hash)
                    ).fetchone()
                    if done:
                        existing = conn.execute(
                            "SELECT objective_score, trade_count FROM sim_run_summary WHERE run_id=?",
                            (done["id"],)
                        ).fetchone()
                        if existing:
                            _log_db("VALIDATION_STUDY",
                                    step=f"rank_{rank}_cached",
                                    config_hash=cfg_hash,
                                    run_id=done["id"],
                                    objective_score=round(float(existing["objective_score"] or 0.0), 3))
                            results.append({
                                "rank": rank,
                                "config_hash": cfg_hash,
                                "run_id": done["id"],
                                "objective_score": float(existing["objective_score"] or 0.0),
                                "trade_count": int(existing["trade_count"] or 0),
                            })
                        continue

                started_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                cur = conn.execute(
                    """INSERT INTO sim_runs
                       (run_type, session_mode, source_dates, date_count,
                        study_name, config_hash, config_json,
                        parent_run_id, run_started_at, tick_count_total, warmup_ticks, status)
                       VALUES ('validation','day',?,?, ?,?,?, ?,?,?,?,'running')""",
                    (src_dates_key, len(dates_sorted), study_name, cfg_hash,
                     json.dumps(config, sort_keys=True, ensure_ascii=False),
                     train_run_id, started_at, total_ticks, _WARMUP_TICKS)
                )
                run_id = cur.lastrowid
                conn.commit()

                try:
                    all_trades, session_pnl_list = self._run_config_on_sessions(
                        conn, run_id, config, dates_sorted, sessions_ticks
                    )
                    finished_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                    trade_count = len(all_trades)
                    conn.execute(
                        "UPDATE sim_runs SET status='done', run_finished_at=?, trade_count=? WHERE id=?",
                        (finished_at, trade_count, run_id)
                    )
                    conn.commit()

                    obj_score = _compute_and_insert_summary(
                        conn, run_id, all_trades,
                        session_count=len(dates_sorted),
                        session_pnl_list=session_pnl_list,
                    )

                    results.append({
                        "rank": rank,
                        "config_hash": cfg_hash,
                        "run_id": run_id,
                        "objective_score": obj_score,
                        "trade_count": trade_count,
                    })

                except Exception as exc:
                    conn.execute(
                        "UPDATE sim_runs SET status='error', error_message=? WHERE id=?",
                        (str(exc)[:500], run_id)
                    )
                    conn.commit()
                    log.warning("validation 실패 | hash=%s err=%s", cfg_hash, exc)

            return results
        finally:
            conn.close()

    def print_validation_report(self,
                                study_name: str,
                                valid_dates: List[str],
                                top_n: int = 10) -> None:
        dates_sorted = sorted(set(valid_dates))
        src_dates_key = _canonical_dates(dates_sorted)
        conn = self._connect_sim()
        try:
            train_rows = conn.execute("""
                SELECT
                    r.config_hash,
                    s.objective_score as train_obj
                FROM sim_observations o
                JOIN sim_runs r ON r.id = o.run_id
                JOIN sim_run_summary s ON s.run_id = r.id
                WHERE o.study_name=? AND r.session_mode='day'
                  AND r.run_type IN ('random','bayes_opt') AND r.status='done'
                ORDER BY o.objective_score DESC
                LIMIT ?
            """, (study_name, top_n)).fetchall()

            if not train_rows:
                print(f"validation 대상 train 결과 없음 | study={study_name}")
                return

            W = 98
            print(f"\n{'='*W}")
            print(f"  VALIDATION REPORT | study={study_name} | valid: {dates_sorted[0]} ~ {dates_sorted[-1]} ({len(dates_sorted)}일)")
            print(f"{'='*W}")
            print("  순   hash       train     valid   강건성  v거래  v승%   v낙폭    v양봉%  강청%  MFE%  판정")
            print("  " + "─"*100)

            passed_count = 0
            best_adopted = None

            for rank, trow in enumerate(train_rows, 1):
                cfg_hash = trow["config_hash"]
                train_obj = float(trow["train_obj"] or 0.0)

                row = conn.execute("""
                    SELECT
                        r.id,
                        COALESCE(s.objective_score, 0) AS valid_obj,
                        COALESCE(s.trade_count, 0) AS valid_trades,
                        ROUND(COALESCE(s.win_rate, 0) * 100, 1) AS valid_win_pct,
                        COALESCE(s.max_drawdown, 0) AS valid_max_dd,
                        ROUND(COALESCE(s.session_positive_ratio, 0) * 100, 1) AS valid_pos_pct,
                        ROUND(COALESCE(s.force_exit_ratio, 0) * 100, 1) AS force_exit_pct,
                        ROUND(COALESCE(s.mfe_realization_rate, 0) * 100, 1) AS mfe_real_pct
                    FROM sim_runs r
                    LEFT JOIN sim_run_summary s ON s.run_id = r.id
                    WHERE r.study_name=? AND r.run_type='validation'
                      AND r.session_mode='day' AND r.source_dates=?
                      AND r.config_hash=? AND r.status='done'
                    ORDER BY r.id DESC LIMIT 1
                """, (study_name, src_dates_key, cfg_hash)).fetchone()

                if not row:
                    print(f"  {rank:3d}  {cfg_hash:>8}  {train_obj:+8.2f}   {'(없음)':>8}   {'-':>6}  {'-':>5}  {'-':>5}  {'-':>7}  {'-':>6}  NO✗  [검증없음]")
                    continue

                v_obj      = float(row["valid_obj"]      or 0.0)
                v_trades   = int(row["valid_trades"]     or 0)
                v_win_pct  = float(row["valid_win_pct"]  or 0.0)
                v_dd       = float(row["valid_max_dd"]   or 0.0)
                v_pos_pct  = float(row["valid_pos_pct"]  or 0.0)
                fe_pct     = float(row["force_exit_pct"] or 0.0)
                mfe_pct    = float(row["mfe_real_pct"]   or 0.0)
                robustness = (v_obj / train_obj) if train_obj > 0 else float("-inf")

                crit = _VALID_CRITERIA
                reasons: List[str] = []
                ok = True
                if v_obj <= crit["min_objective"]:
                    reasons.append("obj음수"); ok = False
                if v_trades < crit["min_trade_count"]:
                    reasons.append("거래부족"); ok = False
                if v_dd < crit["max_drawdown_ticks"]:
                    reasons.append("낙폭초과"); ok = False
                if (v_pos_pct / 100) < crit["min_session_pos_ratio"]:
                    reasons.append("세션불안정"); ok = False
                if robustness < crit["min_robustness"]:
                    reasons.append("강건성미달"); ok = False

                verdict = "YES✓" if ok else f"NO✗  [{', '.join(reasons)}]"
                if ok:
                    passed_count += 1
                    if best_adopted is None:
                        best_adopted = (rank, cfg_hash, v_obj)

                # 강제청산 의존도 경고 표시 (>50% → ⚠️)
                fe_warn  = "⚠️" if fe_pct  > 50 else "  "
                # MFE 회수율 낮음 경고 (<40% → ⚠️)
                mfe_warn = "⚠️" if mfe_pct > 0 and mfe_pct < 40 else "  "

                print(
                    f"  {rank:3d}  {cfg_hash:>8}  "
                    f"{train_obj:+8.2f}  {v_obj:+8.2f}  "
                    f"{robustness*100:+6.1f}%  "
                    f"{v_trades:5d}  {v_win_pct:5.1f}%  "
                    f"{v_dd:+8.2f}t  {v_pos_pct:6.1f}%  "
                    f"{fe_warn}{fe_pct:4.0f}%  {mfe_warn}{mfe_pct:4.0f}%  "
                    f"{verdict}"
                )

            print(f"{'='*W}")
            total = len(train_rows)
            print(f"  통과: {passed_count}/{total}", end="")
            if best_adopted:
                r, h, vo = best_adopted
                print(f"  |  권장 config: #{r} hash={h}  valid_obj={vo:+.2f}", end="")
            print("\n")
        finally:
            conn.close()

    def adopt_best(self,
                   study_name: str,
                   valid_dates: List[str],
                   top_n: int = 10,
                   force: bool = False) -> List[Dict]:
        dates_sorted = sorted(set(valid_dates))
        src_dates_key = _canonical_dates(dates_sorted)

        _log_db("ADOPT_BEST",
                step="1_check_validation",
                table="sim_runs",
                study_name=study_name,
                run_type="validation",
                source_dates=src_dates_key,
                session_mode="day")

        conn = self._connect_sim()
        try:
            existing_valid = conn.execute(
                """SELECT COUNT(*) FROM sim_runs
                   WHERE study_name=? AND run_type='validation'
                     AND source_dates=? AND session_mode='day' AND status='done'""",
                (study_name, src_dates_key)
            ).fetchone()[0]
        finally:
            conn.close()

        _log_db("ADOPT_BEST",
                step="1_validation_count",
                existing_valid_count=existing_valid,
                note="0이면 run_validation 자동 실행")

        if existing_valid == 0:
            log.info("[ADOPT_BEST] validation run 없음 → run_validation 자동 실행 | study=%s", study_name)
            self.run_validation(study_name, valid_dates, top_n=top_n, force=False)

        conn = self._connect_sim()
        try:
            if force:
                conn.execute("DELETE FROM sim_adoptions WHERE study_name=?", (study_name,))
                conn.commit()

            _log_db("ADOPT_BEST",
                    step="2_query_candidates",
                    table="sim_observations+sim_runs+sim_run_summary (train+valid JOIN)",
                    study_name=study_name,
                    valid_src_dates=src_dates_key,
                    top_n=top_n)

            rows = conn.execute("""
                SELECT
                    r_t.id AS train_run_id,
                    r_t.config_hash,
                    r_t.config_json,
                    s_t.objective_score AS train_obj,
                    s_t.trade_count AS train_trades,
                    s_t.total_pnl AS train_total_pnl,

                    r_v.id AS valid_run_id,
                    s_v.objective_score AS valid_obj,
                    s_v.trade_count AS valid_trades,
                    s_v.total_pnl AS valid_total_pnl,
                    s_v.max_drawdown AS valid_max_dd,
                    s_v.session_positive_ratio AS valid_pos_ratio
                FROM sim_observations o
                JOIN sim_runs r_t ON r_t.id = o.run_id
                JOIN sim_run_summary s_t ON s_t.run_id = r_t.id
                LEFT JOIN sim_runs r_v
                    ON r_v.study_name = o.study_name
                   AND r_v.run_type = 'validation'
                   AND r_v.source_dates = ?
                   AND r_v.config_hash = r_t.config_hash
                   AND r_v.session_mode = 'day'
                LEFT JOIN sim_run_summary s_v ON s_v.run_id = r_v.id
                WHERE o.study_name = ?
                  AND r_t.session_mode = 'day'
                  AND r_t.run_type IN ('random','bayes_opt')
                  AND r_t.status = 'done'
                ORDER BY o.objective_score DESC
                LIMIT ?
            """, (src_dates_key, study_name, top_n)).fetchall()

            _log_db("ADOPT_BEST",
                    step="2_candidates_found",
                    candidate_rows=len(rows),
                    note="valid_obj=NULL이면 validation run 없거나 source_dates 불일치")

            now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            crit = _VALID_CRITERIA
            adopted = []

            for row in rows:
                cfg_hash = row["config_hash"] or ""
                train_obj = float(row["train_obj"] or 0.0)
                valid_obj = float(row["valid_obj"] or 0.0)
                v_trades = int(row["valid_trades"] or 0)
                v_dd = float(row["valid_max_dd"] or 0.0)
                v_pos_r = float(row["valid_pos_ratio"] or 0.0)
                robustness = (valid_obj / train_obj) if train_obj > 0 else float("-inf")

                ok = (
                    valid_obj > crit["min_objective"]
                    and v_trades >= crit["min_trade_count"]
                    and v_dd >= crit["max_drawdown_ticks"]
                    and v_pos_r >= crit["min_session_pos_ratio"]
                    and robustness >= crit["min_robustness"]
                )
                if not ok:
                    continue

                if not force:
                    dup = conn.execute(
                        "SELECT id FROM sim_adoptions WHERE study_name=? AND config_hash=?",
                        (study_name, cfg_hash)
                    ).fetchone()
                    if dup:
                        continue

                # 레짐 패턴 조회 (session_patterns from mmean.db)
                train_pattern  = None
                valid_pats_map = {}
                try:
                    src_conn = sqlite3.connect(
                        f"file:{self.source_db}?mode=ro", uri=True, timeout=5
                    )
                    # 훈련 날짜: study_name = "day_YYYY-MM-DD"
                    train_date = study_name[4:] if study_name.startswith("day_") else None
                    if train_date:
                        r_pat = src_conn.execute(
                            "SELECT pattern_type FROM session_patterns "
                            "WHERE date=? AND session_mode='day'",
                            (train_date,)
                        ).fetchone()
                        train_pattern = r_pat[0] if r_pat else None
                    # 검증일별 패턴
                    for vd in dates_sorted:
                        r_vp = src_conn.execute(
                            "SELECT pattern_type FROM session_patterns "
                            "WHERE date=? AND session_mode='day'",
                            (vd,)
                        ).fetchone()
                        if r_vp:
                            valid_pats_map[vd] = r_vp[0]
                    src_conn.close()
                except Exception:
                    pass
                valid_patterns_json = json.dumps(valid_pats_map, ensure_ascii=False) if valid_pats_map else None

                conn.execute("""
                    INSERT INTO sim_adoptions
                    (study_name, config_hash, config_json, adopted_at,
                     train_run_id, train_obj, train_trades, train_total_pnl,
                     valid_run_id, valid_dates,
                     valid_obj, valid_trades, valid_total_pnl,
                     valid_max_dd, valid_pos_ratio, robustness,
                     train_pattern, valid_patterns,
                     status, notes)
                    VALUES (?,?,?,?, ?,?,?,?, ?,?, ?,?,?, ?,?,?, ?,?, 'candidate', ?)
                """, (
                    study_name, cfg_hash, row["config_json"], now_str,
                    row["train_run_id"], row["train_obj"], row["train_trades"], row["train_total_pnl"],
                    row["valid_run_id"], src_dates_key,
                    row["valid_obj"], row["valid_trades"], row["valid_total_pnl"],
                    row["valid_max_dd"], row["valid_pos_ratio"], robustness,
                    train_pattern, valid_patterns_json,
                    "day validation passed",
                ))
                adopted.append({
                    "config_hash": cfg_hash,
                    "train_obj": train_obj,
                    "valid_obj": valid_obj,
                    "robustness": robustness,
                })

            conn.commit()
            return adopted
        finally:
            conn.close()

    def run_slippage_stress(self,
                            study_name: str,
                            valid_dates: List[str],
                            slippage_values: Tuple[int, ...] = (1, 2),
                            force: bool = False) -> List[Dict]:
        dates_sorted = sorted(set(valid_dates))
        src_dates_key = _canonical_dates(dates_sorted)

        _log_db("STRESS",
                step="1_query_candidates",
                table="sim_adoptions",
                study_name=study_name,
                status="candidate",
                slippage_values=list(slippage_values))

        conn = self._connect_sim()
        try:
            candidates = conn.execute(
                """SELECT id, config_hash, config_json, valid_run_id
                   FROM sim_adoptions
                   WHERE study_name=? AND status='candidate'
                   ORDER BY valid_obj DESC""",
                (study_name,)
            ).fetchall()

            _log_db("STRESS",
                    step="1_candidates",
                    candidate_count=len(candidates),
                    note="0이면 adopt_best 먼저 실행 필요")
            if not candidates:
                log.warning("[STRESS] 채택 후보 없음 | study=%s", study_name)
                return []

            sessions_ticks = self.load_day_sessions(dates_sorted)
            total_ticks = sum(len(v) for v in sessions_ticks.values())
            results = []

            for cand in candidates:
                adoption_id = cand["id"]
                cfg_hash = cand["config_hash"]
                base_config = json.loads(cand["config_json"])
                valid_run_id = cand["valid_run_id"]
                stress_objs: Dict[int, float] = {}

                for slip in slippage_values:
                    if not force:
                        done = conn.execute(
                            """SELECT id FROM sim_runs
                               WHERE study_name=? AND config_hash=?
                                 AND run_type='stress' AND source_dates=?
                                 AND session_mode='day' AND status='done'
                                 AND config_json LIKE ?""",
                            (study_name, cfg_hash, src_dates_key,
                             f'%"sim_slippage_ticks": {slip}%')
                        ).fetchone()
                        if done:
                            existing_obj = conn.execute(
                                "SELECT objective_score FROM sim_run_summary WHERE run_id=?",
                                (done["id"],)
                            ).fetchone()
                            stress_objs[slip] = float(existing_obj["objective_score"] if existing_obj else 0.0)
                            continue

                    cfg_stress = dict(base_config)
                    cfg_stress["sim_slippage_ticks"] = slip
                    cfg_stress_json = json.dumps(cfg_stress, sort_keys=True, ensure_ascii=False)
                    started_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

                    cur = conn.execute(
                        """INSERT INTO sim_runs
                           (run_type, session_mode, source_dates, date_count,
                            study_name, config_hash, config_json,
                            parent_run_id, run_started_at, tick_count_total, warmup_ticks, status)
                           VALUES ('stress','day',?,?, ?,?,?, ?,?,?,?,'running')""",
                        (src_dates_key, len(dates_sorted), study_name, cfg_hash, cfg_stress_json,
                         valid_run_id, started_at, total_ticks, _WARMUP_TICKS)
                    )
                    run_id = cur.lastrowid
                    conn.commit()

                    try:
                        all_trades, session_pnl_list = self._run_config_on_sessions(
                            conn, run_id, cfg_stress, dates_sorted, sessions_ticks
                        )
                        finished_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                        trade_count = len(all_trades)
                        conn.execute(
                            "UPDATE sim_runs SET status='done', run_finished_at=?, trade_count=? WHERE id=?",
                            (finished_at, trade_count, run_id)
                        )
                        conn.commit()

                        obj_score = _compute_and_insert_summary(
                            conn, run_id, all_trades,
                            session_count=len(dates_sorted),
                            session_pnl_list=session_pnl_list,
                        )
                        stress_objs[slip] = obj_score
                    except Exception as exc:
                        conn.execute(
                            "UPDATE sim_runs SET status='error', error_message=? WHERE id=?",
                            (str(exc)[:500], run_id)
                        )
                        conn.commit()
                        stress_objs[slip] = -999.0

                stress_1t = stress_objs.get(1)
                stress_2t = stress_objs.get(2)
                stress_passed = int(all(v > 0 for v in stress_objs.values())) if stress_objs else 0

                # stress 통과 시 validated로 승격
                new_status = "validated" if stress_passed else "candidate"
                conn.execute("""
                    UPDATE sim_adoptions
                       SET stress_1t_obj=?, stress_2t_obj=?, stress_passed=?, status=?
                     WHERE id=?
                """, (stress_1t, stress_2t, stress_passed, new_status, adoption_id))
                conn.commit()
                log.info(
                    "[STRESS] id=%d cfg=%s stress_passed=%d → status=%s",
                    adoption_id, cfg_hash[:8], stress_passed, new_status,
                )

                results.append({
                    "adoption_id": adoption_id,
                    "config_hash": cfg_hash,
                    "stress_1t_obj": stress_1t,
                    "stress_2t_obj": stress_2t,
                    "stress_passed": stress_passed,
                })

            return results
        finally:
            conn.close()

    def cluster_top_configs(self,
                            study_name: str,
                            top_n: int = 50,
                            n_clusters: int = 5) -> List[Dict]:
        configs = self.load_top_configs(study_name, top_n)
        if not configs:
            log.warning("cluster_top_configs: study=%s 결과 없음", study_name)
            return []

        k = min(n_clusters, len(configs))

        def _dist(a: Dict, b: Dict) -> float:
            ca, cb = a["config"], b["config"]
            sq = 0.0
            for param in _CLUSTER_FLOAT_PARAMS:
                spec = _DAY_SEARCH_SPACE.get(param)
                if spec and len(spec) >= 3:
                    lo, hi = float(spec[1]), float(spec[2])
                    rng = hi - lo
                    if rng > 0:
                        da = (float(ca.get(param, lo)) - lo) / rng
                        db = (float(cb.get(param, lo)) - lo) / rng
                        sq += (da - db) ** 2
            for param in _CLUSTER_BOOL_PARAMS:
                da = 1.0 if ca.get(param) else 0.0
                db = 1.0 if cb.get(param) else 0.0
                sq += (da - db) ** 2
            return sq ** 0.5

        centers_idx = [0]
        while len(centers_idx) < k:
            max_d, max_i = -1.0, -1
            for i, cfg in enumerate(configs):
                if i in centers_idx:
                    continue
                d = min(_dist(cfg, configs[c]) for c in centers_idx)
                if d > max_d:
                    max_d, max_i = d, i
            if max_i == -1:
                break
            centers_idx.append(max_i)

        clusters: List[List[int]] = [[] for _ in range(len(centers_idx))]
        for i, cfg in enumerate(configs):
            nearest = min(range(len(centers_idx)), key=lambda ci: _dist(cfg, configs[centers_idx[ci]]))
            clusters[nearest].append(i)

        result = []
        conn = self._connect_sim()
        try:
            for cid, members_idx in enumerate(clusters, 1):
                if not members_idx:
                    continue
                best_item = max((configs[i] for i in members_idx), key=lambda x: x["objective_score"])
                rep_hash = best_item["config_hash"]
                rep_obj  = best_item["objective_score"]

                members = []
                for i in members_idx:
                    members.append({
                        "config_hash": configs[i]["config_hash"],
                        "objective_score": configs[i]["objective_score"],
                    })

                conn.execute(
                    "UPDATE sim_adoptions SET cluster_id=? WHERE study_name=? AND config_hash=?",
                    (cid, study_name, rep_hash)
                )

                result.append({
                    "cluster_id": cid,
                    "representative": best_item["config"],
                    "rep_hash": rep_hash,
                    "rep_obj": rep_obj,
                    "size": len(members_idx),
                    "members": members,
                })
            conn.commit()
            return result
        finally:
            conn.close()

    def print_adoptions(self, study_name: str) -> None:
        conn = self._connect_sim()
        try:
            rows = conn.execute("""
                SELECT
                    config_hash, train_obj, valid_obj, robustness,
                    stress_1t_obj, stress_2t_obj, stress_passed,
                    status, cluster_id, notes
                FROM sim_adoptions
                WHERE study_name=?
                ORDER BY valid_obj DESC
            """, (study_name,)).fetchall()

            if not rows:
                print(f"채택 데이터 없음 | study={study_name}")
                return

            W = 118
            print(f"\n{'='*W}")
            print(f"  DAY ADOPTIONS | study={study_name}")
            print(f"{'='*W}")
            print("  hash       train     valid   강건성   s1_obj   s2_obj  stress  status     cluster  notes")
            print("  " + "─"*108)
            for r in rows:
                print(
                    f"  {str(r['config_hash'] or '')[:8]:>8}  "
                    f"{float(r['train_obj'] or 0):+8.2f}  "
                    f"{float(r['valid_obj'] or 0):+8.2f}  "
                    f"{float(r['robustness'] or 0)*100:+6.1f}%  "
                    f"{float(r['stress_1t_obj'] or 0):+7.2f}  "
                    f"{float(r['stress_2t_obj'] or 0):+7.2f}  "
                    f"{int(r['stress_passed'] or 0):6d}  "
                    f"{str(r['status'] or ''):<9}  "
                    f"{str(r['cluster_id'] or '-'):>7}  "
                    f"{str(r['notes'] or '')[:20]}"
                )
            print(f"{'='*W}\n")
        finally:
            conn.close()


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="MMEAN 정규장 프로파일별 재시뮬레이션",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
예시:
  # 단일 날짜
  python day_sim.py --date 2026-03-17 --all-profiles
  python day_sim.py --date 2026-03-17 --profile 2
  python day_sim.py --date 2026-03-17 --report-only

  # 멀티 날짜 (공백으로 구분)
  python day_sim.py --dates 2026-03-17 2026-03-18 2026-03-19 --all-profiles
  python day_sim.py --dates 2026-03-17 2026-03-18 --profile 3 --force

  # 탐색 (train)
  python day_sim.py --dates D1 D2 D3 --random 300 --study v1 --seed 42
  python day_sim.py --dates D1 D2 D3 --bayes 200 --study v1 --seed 42

  # Validation (dates = validation 날짜)
  python day_sim.py --dates D4 D5 --validate-study v1 --validate-top 10
  python day_sim.py --dates D4 D5 --validate-study v1 --report-only

  # 채택/스트레스/군집
  python day_sim.py --dates D4 D5 --validate-study v1 --adopt
  python day_sim.py --dates D4 D5 --validate-study v1 --stress
  python day_sim.py --date 2026-03-17 --adoptions --study v1
  python day_sim.py --date 2026-03-17 --cluster 5 --study v1 --top 50
""",
    )

    date_group = p.add_mutually_exclusive_group(required=True)
    date_group.add_argument("--date", metavar="YYYY-MM-DD", help="단일 날짜")
    date_group.add_argument("--dates", nargs="+", metavar="YYYY-MM-DD", help="멀티 날짜 목록")

    p.add_argument("--profile", type=int, default=None, help="단일 프로파일 번호")
    p.add_argument("--all-profiles", action="store_true", help="전체 프로파일 순차 실행")
    p.add_argument("--report-only", action="store_true", help="재생 없이 기존 결과 성적표만 출력")
    p.add_argument("--force", action="store_true", help="이미 완료된 run도 재실행")

    p.add_argument("--random", type=int, default=None, metavar="N", help="N개 랜덤 config 탐색")
    p.add_argument("--bayes", type=int, default=None, metavar="N", help="N개 Bayesian 탐색")
    p.add_argument("--startup", type=int, default=30, metavar="N", help="BO startup random trial 수")
    p.add_argument("--study", type=str, default=None, metavar="NAME", help="탐색 그룹 이름")
    p.add_argument("--seed", type=int, default=None, metavar="N", help="난수 시드")
    p.add_argument("--top", type=int, default=10, metavar="N", help="상위 N개 출력")

    p.add_argument("--validate-study", type=str, default=None, metavar="NAME", help="validation 대상 study 이름 (study 기반)")
    p.add_argument("--validate-date", type=str, default=None, metavar="YYYY-MM-DD",
                   help="날짜기반 cross-study validation (train 기준 날짜)")
    p.add_argument("--validate-top", type=int, default=10, metavar="N", help="validation top N")
    p.add_argument("--adopt", action="store_true", help="validation 통과 config 를 sim_adoptions 에 저장")
    p.add_argument("--stress", action="store_true", help="slippage stress 실행")
    p.add_argument("--adoptions", action="store_true", help="채택 현황 출력")
    p.add_argument("--cluster", type=int, default=None, metavar="K", help="상위 설정 군집화")

    p.add_argument("--source-db", default=_DEFAULT_SOURCE_DB,
                   help=f"원본 DB 읽기 전용 (기본: {_DEFAULT_SOURCE_DB})")
    p.add_argument("--sim-db", default=_DEFAULT_SIM_DB,
                   help=f"SIM DB 쓰기 전용 (기본: {_DEFAULT_SIM_DB})")
    p.add_argument("--day-json", default=_DEFAULT_DAY_JSON,
                   help=f"day_levels.json 경로 (기본: {_DEFAULT_DAY_JSON})")
    return p.parse_args()


def _print_scorecard_rows(rows, title: str, multi: bool = False) -> None:
    W = 108
    print(f"\n{'='*W}")
    if multi:
        print(f"  DAY MULTI-SESSION SCORECARD  |  {title}")
    else:
        print(f"  DAY SESSION SCORECARD  |  {title} 08:45 ~ 15:35")
    print(f"{'='*W}")

    hdr = (f"{'프로파일':>14}  {'거래':>5}  {'승%':>6}  {'총PnL':>9}  "
           f"{'평균':>7}  {'최저':>7}  {'P/F':>5}  {'낙폭':>7}  {'목표점수':>8}")
    if multi:
        hdr += f"  {'양봉%':>6}  {'표준편차':>8}"
    print(hdr)
    print(f"{'-'*W}")

    valid = [r for r in rows if (r["trades"] or 0) >= 3]
    best = max(valid, key=lambda r: (r["obj_score"] or -9999)) if valid else None
    worst = min(valid, key=lambda r: (r["obj_score"] or 9999)) if valid else None

    profiles = load_day_levels()
    for r in rows:
        pno = r["profile_no"]
        label = profiles.get(pno, {}).get("label", f"P{pno:02d}")
        flag = ""
        if best and pno == best["profile_no"]:
            flag = " ◀ BEST"
        elif worst and pno == worst["profile_no"]:
            flag = " ◀ WORST"
        low = " ⚠부족" if (r["trades"] or 0) < 3 else ""

        line = (
            f"  P{pno:02d} {label[:8]:8s}  "
            f"{r['trades'] or 0:5d}  "
            f"{r['win_pct'] or 0:5.1f}%  "
            f"{r['total_pnl'] or 0:+9.2f}t  "
            f"{r['avg_pnl'] or 0:+7.2f}t  "
            f"{r['worst'] or 0:+7.2f}t  "
            f"{r['profit_factor'] or 0:5.2f}  "
            f"{r['max_drawdown'] or 0:+7.2f}t  "
            f"{r['obj_score'] or 0:+8.2f}"
        )
        if multi:
            line += f"  {r['pos_pct'] or 0:5.1f}%  {r['pnl_std'] or 0:8.2f}"
        print(line + flag + low)

    print(f"{'='*W}\n")


def main() -> None:
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s [%(levelname)s] %(name)s - %(message)s")
    args = _parse_args()
    runner = DaySimRunner(source_db=args.source_db, sim_db=args.sim_db, day_json=args.day_json)

    if args.dates:
        dates = sorted(set(args.dates))
        is_multi = len(dates) > 1
        first_date = dates[0]
    else:
        dates = [args.date]
        is_multi = False
        first_date = args.date

    if args.report_only:
        if args.study and args.validate_study:
            runner.print_validation_report(args.validate_study, dates, top_n=args.validate_top)
        elif args.study and args.adoptions:
            runner.print_adoptions(args.study)
        elif args.study:
            runner.print_random_topN(args.study, top_n=args.top)
        else:
            conn = runner._connect_sim()
            try:
                if is_multi:
                    rows = conn.execute("""
                        SELECT
                            r.profile_no,
                            COALESCE(s.trade_count, 0) AS trades,
                            ROUND(COALESCE(s.win_rate,0)*100,1) AS win_pct,
                            ROUND(COALESCE(s.total_pnl,0),2) AS total_pnl,
                            ROUND(COALESCE(s.avg_pnl,0),2) AS avg_pnl,
                            ROUND(COALESCE(s.worst_trade,0),2) AS worst,
                            ROUND(COALESCE(s.profit_factor,0),2) AS profit_factor,
                            ROUND(COALESCE(s.max_drawdown,0),2) AS max_drawdown,
                            ROUND(COALESCE(s.objective_score,0),2) AS obj_score,
                            ROUND(COALESCE(s.session_positive_ratio,0)*100,1) AS pos_pct,
                            ROUND(COALESCE(s.pnl_std,0),2) AS pnl_std
                        FROM sim_runs r
                        LEFT JOIN sim_run_summary s ON s.run_id = r.id
                        WHERE r.source_dates=? AND r.status='done'
                          AND r.run_type='profile' AND r.session_mode='day'
                        ORDER BY r.profile_no
                    """, (_canonical_dates(dates),)).fetchall()
                    if rows:
                        _print_scorecard_rows(rows, f"{dates[0]} ~ {dates[-1]} ({len(dates)}일)", multi=True)
                    else:
                        print("정규장 멀티세션 성적 데이터 없음")
                else:
                    rows = conn.execute("""
                        SELECT
                            r.profile_no,
                            COALESCE(s.trade_count, 0) AS trades,
                            ROUND(COALESCE(s.win_rate,0)*100,1) AS win_pct,
                            ROUND(COALESCE(s.total_pnl,0),2) AS total_pnl,
                            ROUND(COALESCE(s.avg_pnl,0),2) AS avg_pnl,
                            ROUND(COALESCE(s.worst_trade,0),2) AS worst,
                            ROUND(COALESCE(s.profit_factor,0),2) AS profit_factor,
                            ROUND(COALESCE(s.max_drawdown,0),2) AS max_drawdown,
                            ROUND(COALESCE(s.objective_score,0),2) AS obj_score
                        FROM sim_runs r
                        LEFT JOIN sim_run_summary s ON s.run_id = r.id
                        WHERE r.source_dates LIKE ? AND r.status='done'
                          AND r.run_type='profile' AND r.session_mode='day'
                        ORDER BY r.profile_no
                    """, (f'%"{first_date}"%',)).fetchall()
                    if rows:
                        _print_scorecard_rows(rows, first_date, multi=False)
                    else:
                        print("정규장 성적 데이터 없음")
            finally:
                conn.close()
        return

    if args.random is not None:
        runner.run_random_search(dates=dates, n_trials=args.random, study_name=args.study, seed=args.seed, force=args.force)
        study = args.study
        if study is None:
            conn = runner._connect_sim()
            try:
                row = conn.execute(
                    "SELECT study_name FROM sim_runs WHERE run_type='random' AND session_mode='day' ORDER BY id DESC LIMIT 1"
                ).fetchone()
                study = row["study_name"] if row else None
            finally:
                conn.close()
        if study:
            runner.print_random_topN(study, top_n=args.top)
        return

    if args.bayes is not None:
        runner.run_bayes_opt(dates=dates, n_trials=args.bayes, study_name=args.study, seed=args.seed,
                             force=args.force, n_startup_trials=args.startup)
        study = args.study
        if study is None:
            conn = runner._connect_sim()
            try:
                row = conn.execute(
                    "SELECT study_name FROM sim_runs WHERE run_type='bayes_opt' AND session_mode='day' ORDER BY id DESC LIMIT 1"
                ).fetchone()
                study = row["study_name"] if row else None
            finally:
                conn.close()
        if study:
            runner.print_random_topN(study, top_n=args.top)
        return

    if args.validate_date:
        # 날짜기반 cross-study validation
        train_d = args.validate_date
        results = runner.run_validation_by_date(
            train_date=train_d,
            session_mode="day",
            valid_dates=dates,
            top_n=args.validate_top,
            force=args.force,
        )
        date_study = f"day_{train_d}"
        print(f"날짜기반 Validation 완료 | train={train_d} valid={dates} 결과={len(results)}건")
        runner.print_validation_report(date_study, dates, top_n=args.validate_top)
        return

    if args.validate_study:
        if args.adopt:
            adopted = runner.adopt_best(args.validate_study, dates, top_n=args.validate_top, force=args.force)
            print(f"채택 완료 | study={args.validate_study} adopted={len(adopted)}")
        elif args.stress:
            results = runner.run_slippage_stress(args.validate_study, dates, force=args.force)
            print(f"stress 완료 | study={args.validate_study} candidates={len(results)}")
        else:
            runner.run_validation(args.validate_study, dates, top_n=args.validate_top, force=args.force)
            runner.print_validation_report(args.validate_study, dates, top_n=args.validate_top)
        return

    if args.adoptions:
        if not args.study:
            print("--adoptions 는 --study 와 함께 사용하세요.")
            sys.exit(1)
        runner.print_adoptions(args.study)
        return

    if args.cluster is not None:
        if not args.study:
            print("--cluster 는 --study 와 함께 사용하세요.")
            sys.exit(1)
        clusters = runner.cluster_top_configs(args.study, top_n=args.top, n_clusters=args.cluster)
        print(f"cluster 완료 | study={args.study} clusters={len(clusters)}")
        for c in clusters:
            print(f"  cluster={c['cluster_id']} rep={c['rep_hash']} obj={c['rep_obj']:+.2f} size={c['size']}")
        return

    if args.all_profiles:
        if is_multi:
            runner.run_all_profiles_multi(dates, force=args.force)
            conn = runner._connect_sim()
            try:
                rows = conn.execute("""
                    SELECT
                        r.profile_no,
                        COALESCE(s.trade_count, 0) AS trades,
                        ROUND(COALESCE(s.win_rate,0)*100,1) AS win_pct,
                        ROUND(COALESCE(s.total_pnl,0),2) AS total_pnl,
                        ROUND(COALESCE(s.avg_pnl,0),2) AS avg_pnl,
                        ROUND(COALESCE(s.worst_trade,0),2) AS worst,
                        ROUND(COALESCE(s.profit_factor,0),2) AS profit_factor,
                        ROUND(COALESCE(s.max_drawdown,0),2) AS max_drawdown,
                        ROUND(COALESCE(s.objective_score,0),2) AS obj_score,
                        ROUND(COALESCE(s.session_positive_ratio,0)*100,1) AS pos_pct,
                        ROUND(COALESCE(s.pnl_std,0),2) AS pnl_std
                    FROM sim_runs r
                    LEFT JOIN sim_run_summary s ON s.run_id = r.id
                    WHERE r.source_dates=? AND r.status='done'
                      AND r.run_type='profile' AND r.session_mode='day'
                    ORDER BY r.profile_no
                """, (_canonical_dates(dates),)).fetchall()
                if rows:
                    _print_scorecard_rows(rows, f"{dates[0]} ~ {dates[-1]} ({len(dates)}일)", multi=True)
            finally:
                conn.close()
        else:
            runner.run_all_profiles(first_date, force=args.force)
            conn = runner._connect_sim()
            try:
                rows = conn.execute("""
                    SELECT
                        r.profile_no,
                        COALESCE(s.trade_count, 0) AS trades,
                        ROUND(COALESCE(s.win_rate,0)*100,1) AS win_pct,
                        ROUND(COALESCE(s.total_pnl,0),2) AS total_pnl,
                        ROUND(COALESCE(s.avg_pnl,0),2) AS avg_pnl,
                        ROUND(COALESCE(s.worst_trade,0),2) AS worst,
                        ROUND(COALESCE(s.profit_factor,0),2) AS profit_factor,
                        ROUND(COALESCE(s.max_drawdown,0),2) AS max_drawdown,
                        ROUND(COALESCE(s.objective_score,0),2) AS obj_score
                    FROM sim_runs r
                    LEFT JOIN sim_run_summary s ON s.run_id = r.id
                    WHERE r.source_dates LIKE ? AND r.status='done'
                      AND r.run_type='profile' AND r.session_mode='day'
                    ORDER BY r.profile_no
                """, (f'%"{first_date}"%',)).fetchall()
                if rows:
                    _print_scorecard_rows(rows, first_date, multi=False)
            finally:
                conn.close()
        return

    if args.profile is not None:
        if is_multi:
            result = runner.run_profile_multi(dates, args.profile, force=args.force)
            print(f"[day_sim] 완료 | profile={args.profile} dates={len(dates)}개 trades={result.get('trade_count', 0)} status={result.get('status')}")
        else:
            result = runner.run_profile(first_date, args.profile, force=args.force)
            print(f"[day_sim] 완료 | profile={args.profile} trades={result.get('trade_count', 0)} status={result.get('status')}")
        return

    print("--profile N 또는 --all-profiles 또는 탐색/validation 옵션을 지정하세요.")
    sys.exit(1)


if __name__ == "__main__":
    main()
