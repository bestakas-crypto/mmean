# MMEAN/night_sim.py
"""
야간장 마감 후 프로파일별 재시뮬레이션 — 독립 실행 프로그램

야간 세션: session_date 18:00 ~ 익일 06:00
- LLM 없음  (야간 LLM 차단)
- basis / foreign_buy 무의미 → 미사용
- 진입 신호: night_entry (LONG_READY / SHORT_READY)
- 진입 필터: night_confidence, night_raw_score, volume_burst,
             trade_strength, price_vs_vwap

DB 분리 원칙:
    source_db (mmean.db) : regime_ticks 읽기 전용
    sim_db    (sim.db)   : sim_runs / sim_trades 쓰기 전용

사용법:
    python night_sim.py --date 2026-03-17 --all-profiles
    python night_sim.py --date 2026-03-17 --profile 2
    python night_sim.py --date 2026-03-17 --report-only
    python night_sim.py --date 2026-03-17 --all-profiles --force
"""
from __future__ import annotations

import argparse
import hashlib
import json
import logging
import os
import sqlite3
import sys
from datetime import datetime, timedelta
from typing import Dict, List, Optional

log = logging.getLogger("MMEAN.NightSim")

# ── 경로 기본값 ──────────────────────────────────────────────────────
_STORAGE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "storage")

_DEFAULT_SOURCE_DB  = os.path.join(_STORAGE, "mmean.db")   # 읽기 전용
_DEFAULT_SIM_DB     = os.path.join(_STORAGE, "sim.db")     # 쓰기 전용

_DEFAULT_NIGHT_JSON = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "workspace", "night_levels.json",
)

# ── 야간 세션 시간 경계 ───────────────────────────────────────────────
_NIGHT_START        = "18:00:00"   # session_date 당일
_NIGHT_END          = "06:00:00"   # 익일
_FORCE_EXIT_HHMM    = "05:45"      # 익일 기준 강제청산 시각

# ── 재생 설정 ─────────────────────────────────────────────────────────
_WARMUP_TICKS  = 100   # 야간은 장중보다 틱 수가 적으므로 100
_BATCH_SIZE    = 50    # DB 배치 커밋 주기

# ── KOSPI200 선물 틱 값 ───────────────────────────────────────────────
_TICK_VALUE    = 0.05  # 0.05pt = 1틱

# ── 랜덤 탐색 파라미터 공간 ───────────────────────────────────────────
# 형식: {key: ("float"|"int"|"bool", [min, max])}
_NIGHT_SEARCH_SPACE: Dict[str, tuple] = {
    # 진입 필터
    "night_confidence_min":     ("float", 0.40, 0.80),
    "night_raw_score_min":      ("float", 0.30, 3.00),
    "volume_burst_min":         ("float", 0.00, 1.50),
    "trade_strength_min":       ("float", 83.0, 102.0),
    "price_vs_vwap_gate":       ("bool",),
    # 청산
    "night_regime_exit":        ("bool",),
    "night_neutral_exit_ticks": ("int",   0,   8),
    "sim_sl_ticks":             ("float", 5.0, 25.0),
    "sim_tp_ticks":             ("float", 8.0, 40.0),
    "sim_trailing_activate":    ("float", 3.0, 20.0),
    "sim_trailing_ticks":       ("float", 1.0,  8.0),
    "max_hold_ticks":           ("int",  100, 600),
    # 슬리피지
    "sim_slippage_ticks":       ("int",    1,   2),
}

# ── 슬리피지 스트레스 기본 값 (class 정의 전에 위치해야 default arg로 사용 가능) ──
_STRESS_SLIPPAGE_VALUES = (1, 2)   # 기본: 1틱 / 2틱


# ─────────────────────────────────────────────────────────────────────
# DB 처리 상세 로그 헬퍼 (추측형 로그 금지 — 실제 테이블/조건/row수만 출력)
# ─────────────────────────────────────────────────────────────────────

def _log_db(tag: str, **kv) -> None:
    """
    DB 처리 단계 전용 로그 출력.

    tag  : "DB" | "VALIDATION" | "ADOPT" | "STRESS" | "RANDOM" | "BAYES"
    kv   : 출력할 key=value 쌍 (None 값은 생략)
    """
    parts = "  ".join(f"{k}={v!r}" for k, v in kv.items() if v is not None)
    log.info("[%s] %s", tag, parts)


def _log_db_preview(tag: str, rows: list, limit: int = 5) -> None:
    """
    조회된 상위 row 미리보기 (최대 limit건).
    row는 dict 또는 sqlite3.Row — study_name / objective_score / session_date / session_mode 사용.
    """
    for i, row in enumerate(rows[:limit], 1):
        study = row.get("study_name") if isinstance(row, dict) else row["study_name"]
        obj   = row.get("train_obj",  row.get("objective_score", 0)) \
                if isinstance(row, dict) else (row["train_obj"] if "train_obj" in row.keys() else row["objective_score"])
        sd    = row.get("session_date", "?") if isinstance(row, dict) else (
                    row["session_date"] if "session_date" in row.keys() else "?")
        sm    = row.get("session_mode", "?") if isinstance(row, dict) else (
                    row["session_mode"] if "session_mode" in row.keys() else "?")
        log.info("[%s] preview #%d | study=%s | session_date=%s | session_mode=%s | obj=%.4f",
                 tag, i, study, sd, sm, float(obj or 0))

# ── 군집화 대상 파라미터 (class 정의 전에 위치해야 _dist 클로저에서 참조 가능) ──
_CLUSTER_FLOAT_PARAMS = [
    "sim_sl_ticks",
    "sim_tp_ticks",
    "sim_trailing_activate",
    "sim_trailing_ticks",
    "night_confidence_min",
    "night_raw_score_min",
    "max_hold_ticks",
    "volume_burst_min",
    "trade_strength_min",
]
_CLUSTER_BOOL_PARAMS = [
    "price_vs_vwap_gate",
    "night_regime_exit",
]


# ────────────────────────────────────────────────────────────────────
# 기본 야간 프로파일 (night_levels.json 없을 때 사용)
# ────────────────────────────────────────────────────────────────────

_DEFAULT_PROFILES: Dict[int, Dict] = {
    1: dict(
        label="야간 초보수형",
        night_confidence_min=0.70,  night_raw_score_min=2.0,
        volume_burst_min=0.8,       trade_strength_min=102.0,
        price_vs_vwap_gate=True,    night_regime_exit=True,
        night_neutral_exit_ticks=3,
        sim_sl_ticks=8,             sim_tp_ticks=12,
        sim_trailing_activate=6,    sim_trailing_ticks=4,
        max_hold_ticks=200,         sim_slippage_ticks=1,
    ),
    2: dict(
        label="야간 보수형",
        night_confidence_min=0.65,  night_raw_score_min=1.5,
        volume_burst_min=0.5,       trade_strength_min=102.0,
        price_vs_vwap_gate=True,    night_regime_exit=True,
        night_neutral_exit_ticks=3,
        sim_sl_ticks=10,            sim_tp_ticks=15,
        sim_trailing_activate=8,    sim_trailing_ticks=5,
        max_hold_ticks=300,         sim_slippage_ticks=1,
    ),
    3: dict(
        label="야간 중간형",
        night_confidence_min=0.55,  night_raw_score_min=1.0,
        volume_burst_min=0.3,       trade_strength_min=100.5,
        price_vs_vwap_gate=False,   night_regime_exit=True,
        night_neutral_exit_ticks=5,
        sim_sl_ticks=12,            sim_tp_ticks=20,
        sim_trailing_activate=10,   sim_trailing_ticks=6,
        max_hold_ticks=400,         sim_slippage_ticks=1,
    ),
    4: dict(
        label="야간 적극형",
        night_confidence_min=0.50,  night_raw_score_min=0.8,
        volume_burst_min=0.0,       trade_strength_min=100.0,
        price_vs_vwap_gate=False,   night_regime_exit=True,
        night_neutral_exit_ticks=5,
        sim_sl_ticks=15,            sim_tp_ticks=22,
        sim_trailing_activate=12,   sim_trailing_ticks=7,
        max_hold_ticks=450,         sim_slippage_ticks=2,
    ),
    5: dict(
        label="야간 공격형",
        night_confidence_min=0.45,  night_raw_score_min=0.5,
        volume_burst_min=0.0,       trade_strength_min=99.0,
        price_vs_vwap_gate=False,   night_regime_exit=False,
        night_neutral_exit_ticks=0,
        sim_sl_ticks=18,            sim_tp_ticks=28,
        sim_trailing_activate=15,   sim_trailing_ticks=8,
        max_hold_ticks=500,         sim_slippage_ticks=2,
    ),
}


# ────────────────────────────────────────────────────────────────────
# 야간 프로파일 로더
# ────────────────────────────────────────────────────────────────────

def load_night_levels(path: str = _DEFAULT_NIGHT_JSON) -> Dict[int, Dict]:
    """night_levels.json 로드 (없으면 기본 5개 프로파일 반환)."""
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
    log.info("night_levels.json 없음 — 기본 프로파일 5개 사용")
    return _DEFAULT_PROFILES


# ────────────────────────────────────────────────────────────────────
# 야간 포지션 상태
# ────────────────────────────────────────────────────────────────────

class _NightPosition:
    __slots__ = (
        "direction", "entry_price", "entry_ts", "entry_tick_idx",
        "entry_night_confidence", "entry_night_raw_score", "entry_night_regime",
        "max_favorable", "max_adverse",
        "trailing_active", "extreme_price",
        "neutral_tick_count",
    )

    def __init__(self, direction: str, price: float, ts: str,
                 tick_idx: int, row: Dict):
        self.direction               = direction
        self.entry_price             = price
        self.entry_ts                = ts
        self.entry_tick_idx          = tick_idx
        self.entry_night_confidence  = float(row.get("night_confidence") or 0.0)
        self.entry_night_raw_score   = float(row.get("night_raw_score")  or 0.0)
        self.entry_night_regime      = str(row.get("night_regime")       or "")
        self.max_favorable           = 0.0
        self.max_adverse             = 0.0
        self.trailing_active         = False
        self.extreme_price           = price
        self.neutral_tick_count      = 0

    def pnl_ticks(self, current_price: float) -> float:
        diff = current_price - self.entry_price
        return round(diff / _TICK_VALUE if self.direction == "LONG"
                     else -diff / _TICK_VALUE, 2)

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


# ────────────────────────────────────────────────────────────────────
# NightSimEngine — 단일 프로파일 재생
# ────────────────────────────────────────────────────────────────────

class NightSimEngine:
    """
    저장된 regime_ticks 야간 구간을 순서대로 받아 진입·청산 판단.
    LLM / basis / foreign_buy 미사용.
    진입: night_entry + night_confidence + night_raw_score + 보조 필터
    청산: TP / SL / Trailing / 야간 레짐 중립 / max_hold / 강제(05:45)
    """

    def __init__(self, config: Dict, next_date: str):
        self.cfg        = config
        self.next_date  = next_date   # 익일 "YYYY-MM-DD" — 강제청산 기준
        self._pos: Optional[_NightPosition] = None
        self.trades: List[Dict] = []

    # ── config 헬퍼 ──────────────────────────────────────────────────

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

    # ── 진입 조건 ────────────────────────────────────────────────────

    def _check_entry(self, row: Dict) -> Optional[str]:
        """진입 가능하면 'LONG'/'SHORT', 불가면 None."""
        night_entry = str(row.get("night_entry") or "")
        if night_entry not in ("LONG_READY", "SHORT_READY"):
            return None

        night_confidence = float(row.get("night_confidence") or 0.0)
        night_raw_score  = float(row.get("night_raw_score")  or 0.0)
        volume_burst     = float(row.get("volume_burst")     or 0.0)
        trade_strength   = float(row.get("trade_strength")   or 100.0)
        price            = float(row.get("futures_price")    or 0.0)
        vwap             = float(row.get("vwap")             or 0.0)

        direction = "LONG" if night_entry == "LONG_READY" else "SHORT"

        # ① 야간 신뢰도 임계값
        if night_confidence < self._cf("night_confidence_min"):
            return None

        # ② 야간 점수 크기 (절대값 — BULL/BEAR 공통 기준)
        if abs(night_raw_score) < self._cf("night_raw_score_min"):
            return None

        # ③ 거래량 급증 필터
        vol_min = self._cf("volume_burst_min")
        if vol_min > 0 and volume_burst < vol_min:
            return None

        # ④ 체결강도 필터 (100 = 중립, LONG: >= ts_min, SHORT: <= 200-ts_min)
        ts_min = self._cf("trade_strength_min")
        if ts_min > 0:
            if direction == "LONG"  and trade_strength < ts_min:
                return None
            if direction == "SHORT" and trade_strength > (200.0 - ts_min):
                return None

        # ⑤ price vs VWAP gate (LONG: 현재가 > VWAP, SHORT: 현재가 < VWAP)
        if self.cfg.get("price_vs_vwap_gate") and vwap > 0:
            if direction == "LONG"  and price < vwap:
                return None
            if direction == "SHORT" and price > vwap:
                return None

        return direction

    # ── 청산 조건 ────────────────────────────────────────────────────

    def _check_exit(self, row: Dict, pos: _NightPosition,
                    tick_idx: int) -> Optional[str]:
        """청산 사유 반환, 유지면 None."""
        price        = float(row.get("futures_price") or pos.entry_price)
        ts           = str(row.get("ts") or "")
        night_regime = str(row.get("night_regime") or "")
        pos.update_extremes(price)
        pnl = pos.pnl_ticks(price)

        # ① 강제 청산: 익일 05:45 이후
        #    ts 날짜가 next_date 이상이고 HH:MM >= "05:45"
        ts_date = ts[:10]
        ts_hhmm = ts[11:16]
        if ts_date >= self.next_date and ts_hhmm >= _FORCE_EXIT_HHMM:
            return "force_exit"

        # ② TP
        tp = self._cf("sim_tp_ticks")
        if tp > 0 and pnl >= tp:
            return "tp"

        # ③ SL
        sl = self._cf("sim_sl_ticks")
        if sl > 0 and pnl <= -sl:
            return "sl"

        # ④ Trailing stop
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

        # ⑤ 야간 레짐 중립 N틱 유지 시 청산
        neutral_exit = self._ci("night_neutral_exit_ticks")
        if neutral_exit > 0 and self.cfg.get("night_regime_exit"):
            if night_regime == "NEUTRAL":
                pos.neutral_tick_count += 1
                if pos.neutral_tick_count >= neutral_exit:
                    return "night_regime_neutral"
            else:
                pos.neutral_tick_count = 0

        # ⑥ 최대 보유 틱 초과
        max_hold = self._ci("max_hold_ticks")
        if max_hold > 0 and (tick_idx - pos.entry_tick_idx) >= max_hold:
            return "max_hold"

        return None

    # ── on_tick ──────────────────────────────────────────────────────

    def on_tick(self, row: Dict, tick_idx: int) -> Optional[Dict]:
        """포지션 없으면 진입, 있으면 청산 체크. 청산 시 trade dict 반환."""
        price = float(row.get("futures_price") or 0.0)
        ts    = str(row.get("ts") or "")

        if self._pos is None:
            direction = self._check_entry(row)
            if direction:
                slippage = self._cf("sim_slippage_ticks", 1.0) * _TICK_VALUE
                entry_px = price + (slippage if direction == "LONG" else -slippage)
                self._pos = _NightPosition(direction, entry_px, ts, tick_idx, row)
        else:
            exit_reason = self._check_exit(row, self._pos, tick_idx)
            if exit_reason:
                pos      = self._pos
                slippage = self._cf("sim_slippage_ticks", 1.0) * _TICK_VALUE
                exit_px  = price - (slippage if pos.direction == "LONG" else -slippage)
                hold     = tick_idx - pos.entry_tick_idx
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
                    # replay_trades 재사용: night_confidence → long_score 자리
                    "entry_long_score":      round(pos.entry_night_confidence, 4),
                    # night_raw_score → short_score 자리
                    "entry_short_score":     round(pos.entry_night_raw_score,  4),
                    "entry_confidence":      round(pos.entry_night_confidence, 4),
                    "entry_llm_score":       -1.0,   # 야간 LLM 없음
                    "entry_llm_valid":       0,
                    "entry_session_phase":   "night",
                }
                self.trades.append(trade)
                self._pos = None
                return trade
        return None


# ────────────────────────────────────────────────────────────────────
# NightSimRunner — 날짜 × 프로파일 실행기 (DB 분리)
# ────────────────────────────────────────────────────────────────────

class NightSimRunner:
    """
    source_db (mmean.db) : regime_ticks 읽기 전용
    sim_db    (sim.db)   : sim_runs / sim_trades 쓰기 전용
    """

    def __init__(self,
                 source_db:  str = _DEFAULT_SOURCE_DB,
                 sim_db:     str = _DEFAULT_SIM_DB,
                 night_json: str = _DEFAULT_NIGHT_JSON):
        self.source_db  = source_db
        self.sim_db     = sim_db
        self.night_json = night_json
        self._profiles: Optional[Dict[int, Dict]] = None

        # sim.db 초기화 (테이블 없으면 생성)
        from db_setup_sim import setup_sim_db
        setup_sim_db(sim_db)

    def _get_profiles(self) -> Dict[int, Dict]:
        if self._profiles is None:
            self._profiles = load_night_levels(self.night_json)
        return self._profiles

    def _connect_source(self) -> sqlite3.Connection:
        """mmean.db — 읽기 전용."""
        conn = sqlite3.connect(
            f"file:{self.source_db}?mode=ro", uri=True,
            timeout=30, check_same_thread=False
        )
        conn.row_factory = sqlite3.Row
        return conn

    def _connect_sim(self) -> sqlite3.Connection:
        """sim.db — 쓰기 전용."""
        conn = sqlite3.connect(self.sim_db, timeout=30, check_same_thread=False)
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL")
        conn.execute("PRAGMA busy_timeout=15000")
        conn.row_factory = sqlite3.Row
        return conn

    # ── 틱 로드 (source_db) ──────────────────────────────────────────

    def load_night_ticks(self, session_date: str) -> List[Dict]:
        """session_date 18:00 ~ 익일 06:00 틱을 mmean.db에서 로드."""
        next_date = (datetime.strptime(session_date, "%Y-%m-%d")
                     + timedelta(days=1)).strftime("%Y-%m-%d")
        conn = self._connect_source()
        try:
            cur = conn.execute(
                "SELECT * FROM regime_ticks WHERE ts >= ? AND ts < ? ORDER BY ts",
                (f"{session_date} {_NIGHT_START}",
                 f"{next_date} {_NIGHT_END}")
            )
            return [dict(r) for r in cur.fetchall()]
        finally:
            conn.close()

    # ── 멱등성 체크 (sim_db) ─────────────────────────────────────────

    def _is_done(self, conn: sqlite3.Connection,
                 dates: List[str], profile_no: int) -> Optional[int]:
        row = conn.execute(
            """SELECT id FROM sim_runs
               WHERE source_dates=? AND profile_no=?
               AND run_type='profile' AND session_mode='night'
               AND status='done'""",
            (_canonical_dates(dates), profile_no)
        ).fetchone()
        return row[0] if row else None

    # ── 단일 프로파일 실행 ───────────────────────────────────────────

    def run_profile(self, session_date: str, profile_no: int,
                    force: bool = False) -> Dict:
        next_date = (datetime.strptime(session_date, "%Y-%m-%d")
                     + timedelta(days=1)).strftime("%Y-%m-%d")
        ticks = self.load_night_ticks(session_date)
        if not ticks:
            log.warning("야간 틱 없음 | date=%s (18:00~익일 06:00)", session_date)
            return {"status": "no_data", "profile_no": profile_no}

        profiles = self._get_profiles()
        if profile_no not in profiles:
            raise ValueError(f"야간 프로파일 {profile_no} 없음")
        config            = profiles[profile_no]
        source_dates_json = _canonical_dates([session_date])  # 정렬된 canonical JSON

        conn   = self._connect_sim()
        run_id = None
        try:
            # 멱등성: 이미 완료된 경우
            if not force:
                done_id = self._is_done(conn, [session_date], profile_no)
                if done_id:
                    log.info("이미 완료 | date=%s profile=%d run_id=%d",
                             session_date, profile_no, done_id)
                    return {"status": "skipped", "run_id": done_id}

            # 기존 run 삭제 (재실행)
            old = conn.execute(
                """SELECT id FROM sim_runs
                   WHERE source_dates=? AND profile_no=?
                   AND run_type='profile' AND session_mode='night'""",
                (source_dates_json, profile_no)
            ).fetchone()
            if old:
                conn.execute("DELETE FROM sim_trades WHERE run_id=?",   (old[0],))
                conn.execute("DELETE FROM sim_run_summary WHERE run_id=?", (old[0],))
                conn.execute("DELETE FROM sim_runs   WHERE id=?",       (old[0],))
                conn.commit()

            started_at  = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            config_hash = hashlib.md5(
                json.dumps(config, sort_keys=True).encode()
            ).hexdigest()[:8]

            cur = conn.execute(
                """INSERT INTO sim_runs
                   (run_type, session_mode, source_dates, date_count,
                    profile_no, config_hash, config_json,
                    run_started_at, tick_count_total, warmup_ticks, status)
                   VALUES ('profile','night',?,1, ?,?,?,  ?,?,?,'running')""",
                (source_dates_json, profile_no, config_hash,
                 json.dumps(config, sort_keys=True, ensure_ascii=False),  # 재현성
                 started_at, len(ticks), _WARMUP_TICKS)
            )
            run_id = cur.lastrowid
            conn.commit()

            # 재생
            engine = NightSimEngine(config, next_date)
            batch:  List[tuple] = []

            for idx, row in enumerate(ticks):
                if idx < _WARMUP_TICKS:
                    continue
                trade = engine.on_tick(row, idx)
                if trade:
                    batch.append((
                        run_id, session_date, "night",
                        trade["open_ts"],     trade["close_ts"],
                        trade["direction"],
                        trade["entry_price"], trade["exit_price"],
                        trade["pnl_ticks"],   trade["exit_reason"],
                        trade["hold_ticks"],
                        trade["max_favorable_pt"], trade["max_adverse_excursion"],
                        # entry_score_a = night_confidence, entry_score_b = night_raw_score
                        trade["entry_long_score"],  trade["entry_short_score"],
                        trade["entry_confidence"],
                        trade["entry_llm_score"],   trade["entry_llm_valid"],
                        trade["entry_session_phase"],
                    ))
                    if len(batch) >= _BATCH_SIZE:
                        _insert_trades_batch(conn, batch)
                        batch.clear()

            if batch:
                _insert_trades_batch(conn, batch)

            finished_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            trade_count = len(engine.trades)
            conn.execute(
                """UPDATE sim_runs
                   SET status='done', run_finished_at=?, trade_count=?
                   WHERE id=?""",
                (finished_at, trade_count, run_id)
            )
            conn.commit()

            # ── sim_run_summary 계산 및 저장 ─────────────────────────
            obj_score = _compute_and_insert_summary(
                conn, run_id, engine.trades,
                session_count=1,
            )

            log.info("완료 | date=%s profile=%02d run_id=%d trades=%d score=%.2f",
                     session_date, profile_no, run_id, trade_count, obj_score)
            return {
                "status":          "done",
                "run_id":          run_id,
                "profile_no":      profile_no,
                "tick_count":      len(ticks),
                "trade_count":     trade_count,
                "objective_score": obj_score,
                "trades":          engine.trades,
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

    # ── 전체 프로파일 실행 ───────────────────────────────────────────

    def run_all_profiles(self, session_date: str,
                         force: bool = False) -> List[Dict]:
        profiles = self._get_profiles()
        results  = []
        for pno in sorted(profiles.keys()):
            try:
                r = self.run_profile(session_date, pno, force=force)
                results.append(r)
            except Exception as e:
                log.error("프로파일 %d 실패: %s", pno, e)
                results.append({
                    "status": "error", "profile_no": pno, "error": str(e)
                })
        return results

    # ── 멀티 세션 틱 로드 ────────────────────────────────────────────

    def load_night_sessions(self, dates: List[str]) -> Dict[str, List[Dict]]:
        """여러 날짜의 야간 틱을 로드. {session_date: [rows...]} 반환."""
        result: Dict[str, List[Dict]] = {}
        for d in sorted(set(dates)):
            ticks = self.load_night_ticks(d)
            result[d] = ticks
            log.info("야간 틱 로드 | date=%s count=%d", d, len(ticks))
        return result

    # ── 멀티 세션 단일 프로파일 실행 ────────────────────────────────

    def run_profile_multi(self, dates: List[str], profile_no: int,
                          force: bool = False) -> Dict:
        """
        여러 날짜(multi-session) 기준으로 단일 프로파일 재생.
        sim_runs 1개 row에 전체 날짜 묶음, sim_run_sessions에 날짜별 상세.
        objective_score는 멀티세션 session_pnl_list로 강건성 포함 계산.
        """
        if not dates:
            raise ValueError("dates 목록이 비어 있음")

        dates_sorted      = sorted(set(dates))
        source_dates_json = _canonical_dates(dates_sorted)

        profiles = self._get_profiles()
        if profile_no not in profiles:
            raise ValueError(f"야간 프로파일 {profile_no} 없음")
        config = profiles[profile_no]

        conn   = self._connect_sim()
        run_id = None
        try:
            # 멱등성 체크
            if not force:
                done_id = self._is_done(conn, dates_sorted, profile_no)
                if done_id:
                    log.info("이미 완료 (멀티) | dates=%s profile=%d run_id=%d",
                             dates_sorted, profile_no, done_id)
                    return {"status": "skipped", "run_id": done_id}

            # 기존 run 삭제 (재실행)
            old = conn.execute(
                """SELECT id FROM sim_runs
                   WHERE source_dates=? AND profile_no=?
                   AND run_type='profile' AND session_mode='night'""",
                (source_dates_json, profile_no)
            ).fetchone()
            if old:
                conn.execute("DELETE FROM sim_trades       WHERE run_id=?", (old[0],))
                conn.execute("DELETE FROM sim_run_summary  WHERE run_id=?", (old[0],))
                conn.execute("DELETE FROM sim_run_sessions WHERE run_id=?", (old[0],))
                conn.execute("DELETE FROM sim_runs         WHERE id=?",     (old[0],))
                conn.commit()

            # 틱 로드 (총 tick_count_total 파악)
            sessions_ticks = self.load_night_sessions(dates_sorted)
            total_ticks    = sum(len(v) for v in sessions_ticks.values())

            started_at  = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            config_hash = hashlib.md5(
                json.dumps(config, sort_keys=True).encode()
            ).hexdigest()[:8]

            cur = conn.execute(
                """INSERT INTO sim_runs
                   (run_type, session_mode, source_dates, date_count,
                    profile_no, config_hash, config_json,
                    run_started_at, tick_count_total, warmup_ticks, status)
                   VALUES ('profile','night',?,?, ?,?,?,  ?,?,?,'running')""",
                (source_dates_json, len(dates_sorted),
                 profile_no, config_hash,
                 json.dumps(config, sort_keys=True, ensure_ascii=False),
                 started_at, total_ticks, _WARMUP_TICKS)
            )
            run_id = cur.lastrowid
            conn.commit()

            all_trades:       List[Dict]  = []
            session_pnl_list: List[float] = []
            batch:            List[tuple] = []

            for session_date in dates_sorted:
                ticks = sessions_ticks[session_date]
                if not ticks:
                    log.warning("야간 틱 없음 | date=%s — 빈 세션 기록", session_date)
                    _insert_session_record(conn, run_id, session_date, 0, [])
                    session_pnl_list.append(0.0)
                    continue

                next_date = (datetime.strptime(session_date, "%Y-%m-%d")
                             + timedelta(days=1)).strftime("%Y-%m-%d")
                engine = NightSimEngine(config, next_date)

                for idx, row in enumerate(ticks):
                    if idx < _WARMUP_TICKS:
                        continue
                    trade = engine.on_tick(row, idx)
                    if trade:
                        batch.append((
                            run_id, session_date, "night",
                            trade["open_ts"],     trade["close_ts"],
                            trade["direction"],
                            trade["entry_price"], trade["exit_price"],
                            trade["pnl_ticks"],   trade["exit_reason"],
                            trade["hold_ticks"],
                            trade["max_favorable_pt"], trade["max_adverse_excursion"],
                            trade["entry_long_score"],  trade["entry_short_score"],
                            trade["entry_confidence"],
                            trade["entry_llm_score"],   trade["entry_llm_valid"],
                            trade["entry_session_phase"],
                        ))
                        if len(batch) >= _BATCH_SIZE:
                            _insert_trades_batch(conn, batch)
                            batch.clear()

                # 세션별 요약 기록
                session_pnl = sum(t["pnl_ticks"] for t in engine.trades)
                _insert_session_record(conn, run_id, session_date,
                                       len(ticks), engine.trades)
                session_pnl_list.append(session_pnl)
                all_trades.extend(engine.trades)

            if batch:
                _insert_trades_batch(conn, batch)

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

            log.info("완료 (멀티) | dates=%d개 profile=%02d run_id=%d "
                     "trades=%d score=%.2f",
                     len(dates_sorted), profile_no, run_id,
                     trade_count, obj_score)
            return {
                "status":          "done",
                "run_id":          run_id,
                "profile_no":      profile_no,
                "date_count":      len(dates_sorted),
                "tick_count":      total_ticks,
                "trade_count":     trade_count,
                "objective_score": obj_score,
                "session_pnl":     session_pnl_list,
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

    # ── 멀티 세션 전체 프로파일 실행 ────────────────────────────────

    def run_all_profiles_multi(self, dates: List[str],
                               force: bool = False) -> List[Dict]:
        """L01~LN 전체 프로파일을 멀티세션으로 순차 실행."""
        profiles = self._get_profiles()
        results  = []
        for pno in sorted(profiles.keys()):
            try:
                r = self.run_profile_multi(dates, pno, force=force)
                results.append(r)
            except Exception as e:
                log.error("프로파일 %d 실패 (멀티): %s", pno, e)
                results.append({
                    "status": "error", "profile_no": pno, "error": str(e)
                })
        return results

    # ── 랜덤 탐색 ────────────────────────────────────────────────────

    def run_random_search(
        self,
        dates: List[str],
        n_trials: int = 100,
        study_name: Optional[str] = None,
        seed: Optional[int] = None,
        force: bool = False,
        session_date: Optional[str] = None,   # 날짜 기반 cross-study 검색용 메타
        batch_no: int = 1,
    ) -> List[Dict]:
        """
        n_trials개의 랜덤 config를 멀티세션으로 평가.

        - sim_runs  (run_type='random') 에 trial별 row 저장
        - sim_observations 에 (study_name, trial_no, obj_score) 저장
        - 틱은 최초 1회 로드 후 전 trial 재사용 (속도 최적화)

        반환: 완료된 trial 결과 리스트 (objective_score 내림차순)
        """
        import random

        if not dates:
            raise ValueError("dates 목록이 비어 있음")

        rng          = random.Random(seed)
        dates_sorted = sorted(set(dates))
        src_dates    = _canonical_dates(dates_sorted)

        # ── study_name 자동 생성 ─────────────────────────────────────
        if study_name is None:
            dates_hash = hashlib.md5(src_dates.encode()).hexdigest()[:6]
            study_name = (f"night_random_{dates_hash}_"
                          f"{datetime.now().strftime('%Y%m%d_%H%M%S')}")

        log.info("랜덤 탐색 시작 | study=%s n=%d dates=%s seed=%s",
                 study_name, n_trials, dates_sorted, seed)

        # ── 틱 1회 로드 (전 trial 공유) ──────────────────────────────
        sessions_ticks = self.load_night_sessions(dates_sorted)
        total_ticks    = sum(len(v) for v in sessions_ticks.values())

        conn    = self._connect_sim()
        results: List[Dict] = []

        try:
            # force=True → 기존 study 전체 삭제
            if force:
                old_ids = [r[0] for r in conn.execute(
                    "SELECT id FROM sim_runs WHERE study_name=? AND run_type='random'",
                    (study_name,)
                ).fetchall()]
                if old_ids:
                    placeholders = ",".join("?" * len(old_ids))
                    conn.execute(f"DELETE FROM sim_trades       WHERE run_id IN ({placeholders})", old_ids)
                    conn.execute(f"DELETE FROM sim_run_summary  WHERE run_id IN ({placeholders})", old_ids)
                    conn.execute(f"DELETE FROM sim_run_sessions WHERE run_id IN ({placeholders})", old_ids)
                    conn.execute(f"DELETE FROM sim_runs         WHERE id     IN ({placeholders})", old_ids)
                    conn.execute("DELETE FROM sim_observations  WHERE study_name=?", (study_name,))
                    conn.commit()
                    log.info("기존 study 삭제 | study=%s runs=%d", study_name, len(old_ids))

            for trial_no in range(n_trials):
                # 멱등성 체크 (force=False 시 이미 완료된 trial 건너뜀)
                if not force:
                    done = conn.execute(
                        """SELECT id FROM sim_runs
                           WHERE study_name=? AND trial_no=?
                           AND run_type='random' AND status='done'""",
                        (study_name, trial_no)
                    ).fetchone()
                    if done:
                        log.debug("Trial %d 이미 완료 — 건너뜀", trial_no)
                        continue

                config          = _sample_random_config(rng)
                cfg_json        = json.dumps(config, sort_keys=True, ensure_ascii=False)
                config_hash     = hashlib.md5(cfg_json.encode()).hexdigest()[:8]
                started_at      = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

                cur = conn.execute(
                    """INSERT INTO sim_runs
                       (run_type, session_mode, source_dates, date_count,
                        trial_no, study_name, config_hash, config_json,
                        run_started_at, tick_count_total, warmup_ticks, status)
                       VALUES ('random','night',?,?, ?,?,?,?,  ?,?,?,'running')""",
                    (src_dates, len(dates_sorted),
                     trial_no, study_name, config_hash, cfg_json,
                     started_at, total_ticks, _WARMUP_TICKS)
                )
                run_id = cur.lastrowid
                conn.commit()

                all_trades:       List[Dict]  = []
                session_pnl_list: List[float] = []
                batch:            List[tuple] = []

                try:
                    for session_date in dates_sorted:
                        ticks = sessions_ticks[session_date]
                        if not ticks:
                            _insert_session_record(conn, run_id, session_date, 0, [])
                            session_pnl_list.append(0.0)
                            continue

                        next_date = (datetime.strptime(session_date, "%Y-%m-%d")
                                     + timedelta(days=1)).strftime("%Y-%m-%d")
                        engine = NightSimEngine(config, next_date)

                        for idx, row in enumerate(ticks):
                            if idx < _WARMUP_TICKS:
                                continue
                            trade = engine.on_tick(row, idx)
                            if trade:
                                batch.append((
                                    run_id, session_date, "night",
                                    trade["open_ts"],     trade["close_ts"],
                                    trade["direction"],
                                    trade["entry_price"], trade["exit_price"],
                                    trade["pnl_ticks"],   trade["exit_reason"],
                                    trade["hold_ticks"],
                                    trade["max_favorable_pt"], trade["max_adverse_excursion"],
                                    trade["entry_long_score"],  trade["entry_short_score"],
                                    trade["entry_confidence"],
                                    trade["entry_llm_score"],   trade["entry_llm_valid"],
                                    trade["entry_session_phase"],
                                ))
                                if len(batch) >= _BATCH_SIZE:
                                    _insert_trades_batch(conn, batch)
                                    batch.clear()

                        session_pnl = sum(t["pnl_ticks"] for t in engine.trades)
                        _insert_session_record(conn, run_id, session_date,
                                               len(ticks), engine.trades)
                        session_pnl_list.append(session_pnl)
                        all_trades.extend(engine.trades)

                    if batch:
                        _insert_trades_batch(conn, batch)

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

                    # ── sim_observations 저장 ────────────────────────
                    _sd = session_date or dates_sorted[0]
                    conn.execute(
                        """INSERT INTO sim_observations
                           (study_name, trial_no, run_id,
                            session_date, session_mode, batch_no,
                            config_json, objective_score, created_at)
                           VALUES (?,?,?, ?,?,?, ?,?,?)""",
                        (study_name, trial_no, run_id,
                         _sd, "night", batch_no,
                         cfg_json, obj_score, finished_at)
                    )
                    conn.commit()
                    log.debug("[RANDOM] INSERT obs trial_no=%s obj=%.4f run_id=%s",
                              trial_no, obj_score, run_id)

                    results.append({
                        "trial_no":        trial_no,
                        "run_id":          run_id,
                        "objective_score": obj_score,
                        "trade_count":     trade_count,
                        "config":          config,
                        "study_name":      study_name,   # CLI 역추출용
                    })

                    # 진행 로그 (10 trial마다)
                    done_n = len(results)
                    if done_n % 10 == 0:
                        best_score = max(r["objective_score"] for r in results)
                        print(f"  [{done_n:4d}/{n_trials}] "
                              f"완료 | 최고={best_score:+.2f}",
                              flush=True)

                except Exception as exc:
                    conn.execute(
                        "UPDATE sim_runs SET status='error', error_message=? WHERE id=?",
                        (str(exc)[:500], run_id)
                    )
                    conn.commit()
                    log.warning("Trial %d 실패: %s", trial_no, exc)

            results.sort(key=lambda r: r["objective_score"], reverse=True)
            log.info("랜덤 탐색 완료 | study=%s 유효=%d/%d",
                     study_name, len(results), n_trials)
            return results

        finally:
            conn.close()

    # ── Bayesian Optimization (Optuna) ──────────────────────────────

    def run_bayes_opt(
        self,
        dates: List[str],
        n_trials: int = 200,
        study_name: Optional[str] = None,
        seed: Optional[int] = None,
        force: bool = False,
        n_startup_trials: int = 30,
        session_date: Optional[str] = None,   # 날짜 기반 cross-study 검색용 메타
        batch_no: int = 1,
    ) -> List[Dict]:
        """
        Optuna TPESampler 기반 Bayesian Optimization.

        - n_startup_trials : 초기 랜덤 탐색 수 (이후 TPE 수렴)
        - run_type='bayes_opt', sim_observations 저장
        - 틱 1회 로드 → 전 trial 재사용

        pip install optuna  (사전 설치 필요)
        """
        try:
            import optuna
            optuna.logging.set_verbosity(optuna.logging.WARNING)
        except ImportError:
            raise ImportError(
                "Optuna 미설치: pip install optuna"
            )

        if not dates:
            raise ValueError("dates 목록이 비어 있음")

        dates_sorted = sorted(set(dates))
        src_dates    = _canonical_dates(dates_sorted)

        if study_name is None:
            dates_hash = hashlib.md5(src_dates.encode()).hexdigest()[:6]
            study_name = (f"night_bo_{dates_hash}_"
                          f"{datetime.now().strftime('%Y%m%d_%H%M%S')}")

        log.info("BO 탐색 시작 | study=%s n=%d startup=%d dates=%s seed=%s",
                 study_name, n_trials, n_startup_trials, dates_sorted, seed)

        sessions_ticks = self.load_night_sessions(dates_sorted)
        total_ticks    = sum(len(v) for v in sessions_ticks.values())

        results:       List[Dict] = []
        trial_counter: List[int]  = [0]   # 클로저용 가변 카운터

        conn = self._connect_sim()
        try:
            # force=True → 기존 study 전체 삭제
            if force:
                old_ids = [r[0] for r in conn.execute(
                    "SELECT id FROM sim_runs WHERE study_name=? AND run_type='bayes_opt'",
                    (study_name,)
                ).fetchall()]
                if old_ids:
                    ph = ",".join("?" * len(old_ids))
                    conn.execute(f"DELETE FROM sim_trades       WHERE run_id IN ({ph})", old_ids)
                    conn.execute(f"DELETE FROM sim_run_summary  WHERE run_id IN ({ph})", old_ids)
                    conn.execute(f"DELETE FROM sim_run_sessions WHERE run_id IN ({ph})", old_ids)
                    conn.execute(f"DELETE FROM sim_runs         WHERE id     IN ({ph})", old_ids)
                    conn.execute("DELETE FROM sim_observations  WHERE study_name=?", (study_name,))
                    conn.commit()
                    log.info("기존 BO study 삭제 | study=%s runs=%d", study_name, len(old_ids))

            # ── Optuna objective (클로저) ─────────────────────────────
            def _objective(trial: "optuna.Trial") -> float:
                trial_no = trial_counter[0]
                trial_counter[0] += 1

                # ── 파라미터 제안 ────────────────────────────────────
                cfg: Dict = {}
                cfg["night_confidence_min"]     = trial.suggest_float(
                    "night_confidence_min", 0.40, 0.80)
                cfg["night_raw_score_min"]      = trial.suggest_float(
                    "night_raw_score_min", 0.30, 3.00)
                cfg["volume_burst_min"]         = trial.suggest_float(
                    "volume_burst_min", 0.00, 1.50)
                cfg["trade_strength_min"]       = trial.suggest_float(
                    "trade_strength_min", 83.0, 102.0)
                cfg["price_vs_vwap_gate"]       = bool(trial.suggest_categorical(
                    "price_vs_vwap_gate", [0, 1]))
                cfg["night_regime_exit"]        = bool(trial.suggest_categorical(
                    "night_regime_exit", [0, 1]))
                cfg["night_neutral_exit_ticks"] = trial.suggest_int(
                    "night_neutral_exit_ticks", 0, 8)
                cfg["sim_sl_ticks"]             = trial.suggest_float(
                    "sim_sl_ticks", 5.0, 25.0)
                cfg["sim_tp_ticks"]             = trial.suggest_float(
                    "sim_tp_ticks", 8.0, 40.0)
                cfg["sim_trailing_activate"]    = trial.suggest_float(
                    "sim_trailing_activate", 3.0, 20.0)
                cfg["sim_trailing_ticks"]       = trial.suggest_float(
                    "sim_trailing_ticks", 1.0, 8.0)
                cfg["max_hold_ticks"]           = trial.suggest_int(
                    "max_hold_ticks", 100, 600)
                cfg["sim_slippage_ticks"]       = trial.suggest_categorical(
                    "sim_slippage_ticks", [1, 2])
                cfg["label"] = "bayes_opt"

                # ── 제약 조건 (clamp) ────────────────────────────────
                # TPE가 실패 영역을 점차 학습하도록 클랭핑 방식 사용
                if cfg["sim_tp_ticks"] < cfg["sim_sl_ticks"] + 2:
                    cfg["sim_tp_ticks"] = cfg["sim_sl_ticks"] + 2.0
                if cfg["sim_trailing_activate"] >= cfg["sim_tp_ticks"]:
                    cfg["sim_trailing_activate"] = cfg["sim_tp_ticks"] * 0.6
                if cfg["sim_trailing_ticks"] > cfg["sim_trailing_activate"]:
                    cfg["sim_trailing_ticks"] = max(
                        1.0, cfg["sim_trailing_activate"] * 0.5
                    )
                if not cfg["night_regime_exit"]:
                    cfg["night_neutral_exit_ticks"] = 0

                cfg_json    = json.dumps(cfg, sort_keys=True, ensure_ascii=False)
                config_hash = hashlib.md5(cfg_json.encode()).hexdigest()[:8]
                started_at  = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

                cur = conn.execute(
                    """INSERT INTO sim_runs
                       (run_type, session_mode, source_dates, date_count,
                        trial_no, study_name, config_hash, config_json,
                        run_started_at, tick_count_total, warmup_ticks, status)
                       VALUES ('bayes_opt','night',?,?, ?,?,?,?,  ?,?,?,'running')""",
                    (src_dates, len(dates_sorted),
                     trial_no, study_name, config_hash, cfg_json,
                     started_at, total_ticks, _WARMUP_TICKS)
                )
                run_id = cur.lastrowid
                conn.commit()

                all_trades:       List[Dict]  = []
                session_pnl_list: List[float] = []
                batch:            List[tuple] = []

                for session_date in dates_sorted:
                    ticks = sessions_ticks[session_date]
                    if not ticks:
                        _insert_session_record(conn, run_id, session_date, 0, [])
                        session_pnl_list.append(0.0)
                        continue

                    next_date = (datetime.strptime(session_date, "%Y-%m-%d")
                                 + timedelta(days=1)).strftime("%Y-%m-%d")
                    engine = NightSimEngine(cfg, next_date)

                    for idx, row in enumerate(ticks):
                        if idx < _WARMUP_TICKS:
                            continue
                        trade = engine.on_tick(row, idx)
                        if trade:
                            batch.append((
                                run_id, session_date, "night",
                                trade["open_ts"],     trade["close_ts"],
                                trade["direction"],
                                trade["entry_price"], trade["exit_price"],
                                trade["pnl_ticks"],   trade["exit_reason"],
                                trade["hold_ticks"],
                                trade["max_favorable_pt"], trade["max_adverse_excursion"],
                                trade["entry_long_score"],  trade["entry_short_score"],
                                trade["entry_confidence"],
                                trade["entry_llm_score"],   trade["entry_llm_valid"],
                                trade["entry_session_phase"],
                            ))
                            if len(batch) >= _BATCH_SIZE:
                                _insert_trades_batch(conn, batch)
                                batch.clear()

                    session_pnl = sum(t["pnl_ticks"] for t in engine.trades)
                    _insert_session_record(conn, run_id, session_date,
                                           len(ticks), engine.trades)
                    session_pnl_list.append(session_pnl)
                    all_trades.extend(engine.trades)

                if batch:
                    _insert_trades_batch(conn, batch)

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

                _sd = session_date or dates_sorted[0]
                conn.execute(
                    """INSERT INTO sim_observations
                       (study_name, trial_no, run_id,
                        session_date, session_mode, batch_no,
                        config_json, objective_score, created_at)
                       VALUES (?,?,?, ?,?,?, ?,?,?)""",
                    (study_name, trial_no, run_id,
                     _sd, "night", batch_no,
                     cfg_json, obj_score, finished_at)
                )
                conn.commit()
                log.debug("[BAYES] INSERT obs trial_no=%s obj=%.4f run_id=%s",
                          trial_no, obj_score, run_id)

                results.append({
                    "trial_no":        trial_no,
                    "run_id":          run_id,
                    "objective_score": obj_score,
                    "trade_count":     trade_count,
                    "config":          cfg,
                    "study_name":      study_name,
                })

                done_n = len(results)
                if done_n % 10 == 0:
                    best_score = max(r["objective_score"] for r in results)
                    print(f"  [{done_n:4d}/{n_trials}] "
                          f"BO | best={best_score:+.2f}",
                          flush=True)

                return obj_score

            # ── Optuna 탐색 실행 ─────────────────────────────────────
            optuna.logging.set_verbosity(optuna.logging.WARNING)
            sampler = optuna.samplers.TPESampler(
                seed=seed,
                n_startup_trials=n_startup_trials,
            )
            optuna_study = optuna.create_study(
                direction="maximize",
                sampler=sampler,
                study_name=study_name,
            )
            optuna_study.optimize(
                _objective,
                n_trials=n_trials,
                catch=(Exception,),   # trial 실패 시 건너뛰고 계속
            )

            results.sort(key=lambda r: r["objective_score"], reverse=True)
            log.info("BO 탐색 완료 | study=%s 유효=%d/%d",
                     study_name, len(results), n_trials)
            return results

        finally:
            conn.close()

    # ── 랜덤 탐색 결과 조회 ──────────────────────────────────────────

    def print_random_topN(self, study_name: str, top_n: int = 10) -> None:
        """랜덤 탐색 결과 상위 N개 출력 (sim_observations JOIN)."""
        conn = self._connect_sim()
        try:
            rows = conn.execute("""
                SELECT
                    o.trial_no,
                    o.run_id,
                    ROUND(COALESCE(o.objective_score, 0), 2)   AS obj_score,
                    COALESCE(s.trade_count,    0)               AS trades,
                    ROUND(COALESCE(s.win_rate, 0)*100, 1)       AS win_pct,
                    ROUND(COALESCE(s.total_pnl,0), 2)           AS total_pnl,
                    ROUND(COALESCE(s.profit_factor,0), 2)       AS profit_factor,
                    ROUND(COALESCE(s.max_drawdown,  0), 2)      AS max_drawdown,
                    ROUND(COALESCE(s.pnl_std,       0), 2)      AS pnl_std,
                    ROUND(COALESCE(s.session_positive_ratio,0)*100,1) AS pos_pct,
                    r.date_count,
                    o.config_json
                FROM sim_observations o
                JOIN sim_runs r ON r.id = o.run_id
                LEFT JOIN sim_run_summary s ON s.run_id = o.run_id
                WHERE o.study_name = ? AND r.status = 'done'
                ORDER BY o.objective_score DESC
                LIMIT ?
            """, (study_name, top_n)).fetchall()

            total = conn.execute(
                "SELECT COUNT(*) FROM sim_observations WHERE study_name=?",
                (study_name,)
            ).fetchone()[0]

            if not rows:
                print(f"결과 없음 | study={study_name}")
                return

            W = 112
            print(f"\n{'='*W}")
            print(f"  RANDOM SEARCH TOP {top_n}  |  "
                  f"study={study_name}  총trials={total}")
            print(f"{'='*W}")
            print(
                f"  {'순위':>3}  {'trial':>6}  {'run':>6}  {'목표점수':>8}  "
                f"{'거래':>5}  {'승%':>6}  {'총PnL':>9}  "
                f"{'P/F':>5}  {'낙폭':>7}  {'편차':>6}  {'양봉%':>6}"
            )
            print(f"  {'-'*108}")

            for rank, r in enumerate(rows, 1):
                print(
                    f"  {rank:3d}  "
                    f"{r['trial_no']:6d}  "
                    f"{r['run_id']:6d}  "
                    f"{r['obj_score']      or 0:+8.2f}  "
                    f"{r['trades']         or 0:5d}  "
                    f"{r['win_pct']        or 0:5.1f}%  "
                    f"{r['total_pnl']      or 0:+9.2f}t  "
                    f"{r['profit_factor']  or 0:5.2f}  "
                    f"{r['max_drawdown']   or 0:+7.2f}t  "
                    f"{r['pnl_std']        or 0:6.2f}  "
                    f"{r['pos_pct']        or 0:5.1f}%"
                )

            print(f"{'='*W}")

            # 최고 config 상세 출력
            if rows:
                best     = rows[0]
                best_cfg = json.loads(best["config_json"])
                print(f"\n  ★ Best Config "
                      f"(trial={best['trial_no']}, run_id={best['run_id']}, "
                      f"score={best['obj_score']:+.2f}):")
                for k in sorted(best_cfg):
                    if k != "label":
                        print(f"    {k:<30} = {best_cfg[k]}")
            print()

        finally:
            conn.close()

    # ── Train / Validation 분리 ──────────────────────────────────────

    def load_top_configs(
        self,
        study_name: str,
        top_n: int = 10,
    ) -> List[Dict]:
        """
        study_name 기준 상위 top_n 개 config 반환.

        반환 형식 (list of dict):
            [{
                "run_id":          int,
                "trial_no":        int,
                "config_hash":     str,
                "config_json":     str,
                "config":          dict,
                "train_obj":       float,
                "train_trades":    int,
                "train_total_pnl": float,
            }, ...]
        """
        conn = self._connect_sim()
        try:
            rows = conn.execute("""
                SELECT
                    r.id                      AS run_id,
                    r.trial_no,
                    r.config_hash,
                    r.config_json,
                    COALESCE(o.objective_score, 0) AS train_obj,
                    COALESCE(s.trade_count,     0) AS train_trades,
                    COALESCE(s.total_pnl,       0) AS train_total_pnl
                FROM sim_observations o
                JOIN sim_runs r ON r.id = o.run_id
                LEFT JOIN sim_run_summary s ON s.run_id = r.id
                WHERE o.study_name = ? AND r.status = 'done'
                ORDER BY o.objective_score DESC
                LIMIT ?
            """, (study_name, top_n)).fetchall()

            result = []
            for row in rows:
                result.append({
                    "run_id":          row["run_id"],
                    "trial_no":        row["trial_no"],
                    "config_hash":     row["config_hash"] or "",
                    "config_json":     row["config_json"],
                    "config":          json.loads(row["config_json"]),
                    "train_obj":       float(row["train_obj"]),
                    "train_trades":    int(row["train_trades"]),
                    "train_total_pnl": float(row["train_total_pnl"]),
                })
            return result
        finally:
            conn.close()

    def load_top_configs_by_date(
        self,
        session_date: str,
        session_mode: str = "night",
        top_n: int = 10,
    ) -> List[Dict]:
        """
        session_date + session_mode 기준 전체 study를 통합해 top-N 반환.

        study_name 경계를 넘어 해당 날짜에 수행된 모든 탐색 결과에서
        objective_score 기준 상위 N개를 뽑는다.
        같은 config_hash가 여러 study에 있으면 가장 높은 score 1개만 사용.
        """
        _log_db("DB", msg="sim_observations 테이블을 사용하여 날짜 기반 top config를 조회합니다.")
        _log_db("DB", 검색필드="study_name, session_date, session_mode, batch_no, objective_score, config_json")
        _log_db("DB", 검색조건_session_date=session_date, 검색조건_session_mode=session_mode, top_n=top_n)
        _log_db("DB", 정렬="objective_score DESC  GROUP BY config_hash (중복 제거)")

        conn = self._connect_sim()
        try:
            # ── 전체 row 수 ──────────────────────────────────────────
            total_rows = conn.execute(
                "SELECT COUNT(*) FROM sim_observations"
            ).fetchone()[0]
            _log_db("DB", 테이블="sim_observations", 전체_row수=total_rows)

            # ── 조건 일치 row 수 ─────────────────────────────────────
            match_rows = conn.execute(
                "SELECT COUNT(*) FROM sim_observations "
                "WHERE session_date=? AND session_mode=?",
                (session_date, session_mode)
            ).fetchone()[0]
            _log_db("DB", 조건일치_row수=match_rows,
                    조건=f"session_date={session_date!r} AND session_mode={session_mode!r}")

            if match_rows == 0:
                _log_db("DB", 결과="0건",
                        사용조건=f"session_date={session_date!r}, session_mode={session_mode!r}")
                _log_db("DB", msg="validation 후보를 만들 수 없어 처리를 중단합니다.")
                return []

            _log_db("DB", msg=f"상위 {top_n}건을 objective_score DESC 기준으로 선택합니다.")

            rows = conn.execute("""
                SELECT
                    r.id                      AS run_id,
                    r.trial_no,
                    r.config_hash,
                    r.config_json,
                    r.study_name,
                    o.session_date,
                    o.session_mode,
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

            _log_db("DB", 최종_선택_row수=len(rows))
            _log_db_preview("DB", rows)

            result = []
            for row in rows:
                result.append({
                    "run_id":          row["run_id"],
                    "trial_no":        row["trial_no"],
                    "config_hash":     row["config_hash"] or "",
                    "config_json":     row["config_json"],
                    "config":          json.loads(row["config_json"]),
                    "study_name":      row["study_name"],
                    "session_date":    row["session_date"],
                    "session_mode":    row["session_mode"],
                    "train_obj":       float(row["train_obj"]),
                    "train_trades":    int(row["train_trades"]),
                    "train_total_pnl": float(row["train_total_pnl"]),
                })
            return result
        except Exception as exc:
            _log_db("DB ERROR", 테이블="sim_observations",
                    조건=f"session_date={session_date!r}, session_mode={session_mode!r}",
                    detail=str(exc))
            raise
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
        """
        날짜/세션 단위 validation.

        train_date + session_mode 기준으로 모든 study를 통합해 top-N을 뽑고
        valid_dates 에서 재검증한다.
        study_name 하나에 종속되지 않는 날짜 기반 집계 구조.

        내부적으로 run_validation() 을 재사용하되,
        study_name 대신 cross-study 통합 top-N 을 먼저 뽑는다.
        """
        top_configs = self.load_top_configs_by_date(train_date, session_mode, top_n)
        if not top_configs:
            log.warning("run_validation_by_date: session_date=%s mode=%s 결과 없음",
                        train_date, session_mode)
            return []

        # study_name이 각각 다를 수 있으므로 대표 study_name 설정
        # validation run의 study_name 은 날짜 기반으로 통일
        date_study_name = f"{session_mode}_{train_date}"

        dates_sorted  = sorted(set(valid_dates))
        src_dates_key = _canonical_dates(dates_sorted)

        _log_db("VALIDATION", msg="날짜 기반 validation을 시작합니다.")
        _log_db("VALIDATION",
                train_session_date=train_date,
                valid_dates=dates_sorted,
                session_mode=session_mode,
                top_n=top_n)
        _log_db("VALIDATION", 조회된_후보_config수=len(top_configs))

        if not top_configs:
            _log_db("VALIDATION", msg="후보가 없어 validation을 종료합니다.")
            return []

        _log_db("VALIDATION", msg="validation 실행을 시작합니다.")

        sessions_ticks = self.load_night_sessions(dates_sorted)
        _log_db("VALIDATION", 로드된_세션수=len(sessions_ticks),
                 총_틱수=sum(len(v) for v in sessions_ticks.values()))

        conn    = self._connect_sim()
        results: List[Dict] = []

        try:
            for rank, item in enumerate(top_configs, 1):
                cfg_hash = item["config_hash"]
                config   = item["config"]
                cfg_json = item["config_json"]

                _log_db("VALIDATION", msg=f"candidate #{rank} 처리 시작",
                        study=item.get("study_name", "?"),
                        obj=round(item.get("train_obj", 0), 4),
                        config_hash=cfg_hash[:12])

                if not force:
                    existing = conn.execute(
                        """SELECT id FROM sim_runs
                           WHERE study_name=? AND config_hash=?
                             AND run_type='validation'
                             AND source_dates=? AND status='done'""",
                        (date_study_name, cfg_hash, src_dates_key)
                    ).fetchone()
                    if existing:
                        _log_db("VALIDATION", msg="이미 완료된 validation — 건너뜀",
                                config_hash=cfg_hash[:12], existing_run_id=existing[0])
                        continue

                started_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                run_type   = "validation"
                cur = conn.execute(
                    """INSERT INTO sim_runs
                       (run_type, session_mode, source_dates, date_count,
                        study_name, config_hash, config_json,
                        run_started_at, status)
                       VALUES (?,?,?,?,?,?,?,?,?)""",
                    (run_type, "night", src_dates_key, len(dates_sorted),
                     date_study_name, cfg_hash, cfg_json,
                     started_at, "running")
                )
                run_id = cur.lastrowid
                conn.commit()

                # 세션별 재생
                all_trades: List[Dict] = []
                session_pnl_list: List[float] = []

                for session_date in dates_sorted:
                    ticks = sessions_ticks[session_date]
                    if not ticks:
                        _insert_session_record(conn, run_id, session_date, 0, [])
                        continue

                    next_date = (datetime.strptime(session_date, "%Y-%m-%d")
                                 + timedelta(days=1)).strftime("%Y-%m-%d")
                    engine = NightSimEngine(config, next_date)
                    for idx, row in enumerate(ticks):
                        if idx < _WARMUP_TICKS:
                            continue
                        trade = engine.on_tick(row, idx)
                        if trade:
                            trade["run_id"] = run_id
                    session_pnl = sum(t["pnl_ticks"] for t in engine.trades)
                    session_pnl_list.append(session_pnl)
                    all_trades.extend(engine.trades)
                    _insert_session_record(conn, run_id, session_date,
                                           len(ticks), engine.trades)

                finished_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                obj_score   = _compute_and_insert_summary(
                    conn, run_id, all_trades,
                    session_count=len(dates_sorted),
                    session_pnl_list=session_pnl_list,
                )
                conn.execute(
                    "UPDATE sim_runs SET status='done', objective_score=?,"
                    " run_finished_at=?, trade_count=? WHERE id=?",
                    (obj_score, finished_at, len(all_trades), run_id)
                )
                conn.commit()

                _log_db("VALIDATION", msg="validation run 생성 완료",
                        run_id=run_id, config_hash=cfg_hash[:12])

                results.append({
                    "rank":        rank,
                    "config_hash": cfg_hash,
                    "config_json": cfg_json,
                    "config":      config,
                    "run_id":      run_id,
                    "obj_score":   obj_score,
                    "trades":      len(all_trades),
                    "train_obj":   item["train_obj"],
                    "study_name":  date_study_name,
                })
                _log_db("VALIDATION",
                        msg=f"candidate #{rank}/{len(top_configs)} 완료",
                        run_id=run_id, obj_score=round(obj_score, 4),
                        trades=len(all_trades))

        finally:
            conn.close()

        ok  = len(results)
        _log_db("VALIDATION", msg="날짜 기반 validation 완료",
                train_date=train_date, valid_dates=dates_sorted,
                성공=ok, 실패=(len(top_configs) - ok))
        return results

    def run_validation(
        self,
        study_name: str,
        valid_dates: List[str],
        top_n: int = 10,
        force: bool = False,
    ) -> List[Dict]:
        """
        study_name 상위 top_n config 를 valid_dates 에서 재실행.

        - sim_runs.run_type = 'validation'
        - sim_runs.study_name = study_name  (train study 와 동일 → config_hash 매칭)
        - sim_observations 미사용 (최적화 이력이 아님)
        - 틱 1회 로드 후 모든 config 공유

        반환: 각 config 결과 dict 리스트 (config_hash 키 포함)
        """
        if not valid_dates:
            raise ValueError("valid_dates 목록이 비어 있음")

        _log_db("VALIDATION", msg="study 기반 validation을 시작합니다.")
        _log_db("VALIDATION", 테이블="sim_observations",
                검색조건_study_name=study_name, top_n=top_n)

        top_configs  = self.load_top_configs(study_name, top_n)
        _log_db("VALIDATION", 조회된_후보_config수=len(top_configs))

        if not top_configs:
            _log_db("VALIDATION", msg="후보가 없어 validation을 종료합니다.",
                    테이블="sim_observations", 조건=f"study_name={study_name!r}")
            return []

        dates_sorted  = sorted(set(valid_dates))
        src_dates_key = _canonical_dates(dates_sorted)

        _log_db("VALIDATION", msg="validation 실행을 시작합니다.",
                study_name=study_name, top_n=top_n,
                valid_dates=dates_sorted)

        # ── 틱 1회 로드 ──────────────────────────────────────────────
        sessions_ticks = self.load_night_sessions(dates_sorted)
        total_ticks    = sum(len(v) for v in sessions_ticks.values())

        conn    = self._connect_sim()
        results: List[Dict] = []

        try:
            for rank, item in enumerate(top_configs, 1):
                cfg_hash = item["config_hash"]
                config   = item["config"]
                cfg_json = item["config_json"]

                # ── 멱등성 체크 ──────────────────────────────────────
                if not force:
                    existing = conn.execute(
                        """SELECT id FROM sim_runs
                           WHERE study_name=? AND config_hash=?
                             AND run_type='validation'
                             AND source_dates=? AND status='done'""",
                        (study_name, cfg_hash, src_dates_key)
                    ).fetchone()
                    if existing:
                        log.debug("Validation 이미 완료 | hash=%s — 건너뜀", cfg_hash)
                        continue

                started_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                cur = conn.execute(
                    """INSERT INTO sim_runs
                       (run_type, session_mode, source_dates, date_count,
                        study_name, config_hash, config_json,
                        run_started_at, tick_count_total, warmup_ticks, status)
                       VALUES ('validation','night',?,?,  ?,?,?,  ?,?,?,'running')""",
                    (src_dates_key, len(dates_sorted),
                     study_name, cfg_hash, cfg_json,
                     started_at, total_ticks, _WARMUP_TICKS)
                )
                run_id = cur.lastrowid
                conn.commit()

                all_trades:       List[Dict]  = []
                session_pnl_list: List[float] = []
                batch:            List[tuple] = []

                try:
                    for session_date in dates_sorted:
                        ticks = sessions_ticks[session_date]
                        if not ticks:
                            _insert_session_record(conn, run_id, session_date, 0, [])
                            session_pnl_list.append(0.0)
                            continue

                        next_date = (datetime.strptime(session_date, "%Y-%m-%d")
                                     + timedelta(days=1)).strftime("%Y-%m-%d")
                        engine = NightSimEngine(config, next_date)

                        for idx, row in enumerate(ticks):
                            if idx < _WARMUP_TICKS:
                                continue
                            trade = engine.on_tick(row, idx)
                            if trade:
                                batch.append((
                                    run_id, session_date, "night",
                                    trade["open_ts"],     trade["close_ts"],
                                    trade["direction"],
                                    trade["entry_price"], trade["exit_price"],
                                    trade["pnl_ticks"],   trade["exit_reason"],
                                    trade["hold_ticks"],
                                    trade["max_favorable_pt"],
                                    trade["max_adverse_excursion"],
                                    trade["entry_long_score"],
                                    trade["entry_short_score"],
                                    trade["entry_confidence"],
                                    trade["entry_llm_score"],
                                    trade["entry_llm_valid"],
                                    trade["entry_session_phase"],
                                ))
                                if len(batch) >= _BATCH_SIZE:
                                    _insert_trades_batch(conn, batch)
                                    batch.clear()

                        session_pnl = sum(t["pnl_ticks"] for t in engine.trades)
                        _insert_session_record(conn, run_id, session_date,
                                               len(ticks), engine.trades)
                        session_pnl_list.append(session_pnl)
                        all_trades.extend(engine.trades)

                    if batch:
                        _insert_trades_batch(conn, batch)

                    finished_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                    trade_count = len(all_trades)
                    conn.execute(
                        """UPDATE sim_runs
                           SET status='done', run_finished_at=?, trade_count=?
                           WHERE id=?""",
                        (finished_at, trade_count, run_id)
                    )
                    conn.commit()

                    valid_obj = _compute_and_insert_summary(
                        conn, run_id, all_trades,
                        session_count=len(dates_sorted),
                        session_pnl_list=session_pnl_list,
                    )

                    # validation 요약 row 읽기 (판정용)
                    summ = conn.execute(
                        """SELECT trade_count, total_pnl, max_drawdown,
                                  session_positive_ratio
                           FROM sim_run_summary WHERE run_id=?""",
                        (run_id,)
                    ).fetchone()

                    v_trades  = int(summ["trade_count"]            or 0) if summ else 0
                    v_dd      = float(summ["max_drawdown"]         or 0) if summ else 0.0
                    v_pos_r   = float(summ["session_positive_ratio"] or 0) if summ else 0.0
                    train_obj = item["train_obj"]

                    robustness = (
                        (valid_obj / train_obj) if train_obj > 0 else float("-inf")
                    )

                    # ── 채택 판정 ─────────────────────────────────────
                    reasons: List[str] = []
                    ok = True
                    crit = _VALID_CRITERIA
                    if valid_obj <= crit["min_objective"]:
                        reasons.append("obj음수")
                        ok = False
                    if v_trades < crit["min_trade_count"]:
                        reasons.append("거래부족")
                        ok = False
                    if v_dd < crit["max_drawdown_ticks"]:
                        reasons.append("낙폭초과")
                        ok = False
                    if v_pos_r < crit["min_session_pos_ratio"]:
                        reasons.append("세션불안정")
                        ok = False
                    if robustness < crit["min_robustness"]:
                        reasons.append("강건성미달")
                        ok = False

                    results.append({
                        "rank":       rank,
                        "run_id":     run_id,
                        "config_hash": cfg_hash,
                        "config":     config,
                        "train_obj":  train_obj,
                        "valid_obj":  valid_obj,
                        "robustness": robustness,
                        "v_trades":   v_trades,
                        "v_dd":       v_dd,
                        "v_pos_r":    v_pos_r,
                        "adopted":    ok,
                        "reasons":    reasons,
                    })

                    status_str = "YES" if ok else f"NO  [{', '.join(reasons)}]"
                    print(f"  [{rank:2d}/{len(top_configs)}] "
                          f"hash={cfg_hash}  "
                          f"train={train_obj:+.2f}  "
                          f"valid={valid_obj:+.2f}  "
                          f"robust={robustness*100:+.1f}%  "
                          f"=> {status_str}",
                          flush=True)

                except Exception as exc:
                    conn.execute(
                        "UPDATE sim_runs SET status='error', error_message=? WHERE id=?",
                        (str(exc)[:500], run_id)
                    )
                    conn.commit()
                    log.warning("Validation 실패 | hash=%s: %s", cfg_hash, exc)

            log.info("Validation 완료 | study=%s passed=%d/%d",
                     study_name, sum(1 for r in results if r["adopted"]), len(results))
            return results

        finally:
            conn.close()

    def print_validation_report(
        self,
        study_name: str,
        valid_dates: List[str],
        top_n: int = 10,
    ) -> None:
        """
        Validation 결과 성적표 출력.
        run_validation() 이 먼저 실행되어 sim_runs 에 'validation' 레코드가
        있어야 함.
        """
        dates_sorted  = sorted(set(valid_dates))
        src_dates_key = _canonical_dates(dates_sorted)
        date_range    = (f"{dates_sorted[0]} ~ {dates_sorted[-1]}"
                         f" ({len(dates_sorted)}일)")

        conn = self._connect_sim()
        try:
            # ── validation 레코드 조회 ─────────────────────────────
            vrows = conn.execute("""
                SELECT
                    r.id                               AS run_id,
                    r.config_hash,
                    COALESCE(s.trade_count,            0) AS v_trades,
                    ROUND(COALESCE(s.win_rate,         0)*100, 1) AS v_win_pct,
                    ROUND(COALESCE(s.total_pnl,        0), 2) AS v_total_pnl,
                    ROUND(COALESCE(s.profit_factor,    0), 2) AS v_pf,
                    ROUND(COALESCE(s.max_drawdown,     0), 2) AS v_dd,
                    ROUND(COALESCE(s.session_positive_ratio, 0)*100, 1) AS v_pos_pct,
                    ROUND(COALESCE(s.objective_score,  0), 2) AS v_obj
                FROM sim_runs r
                LEFT JOIN sim_run_summary s ON s.run_id = r.id
                WHERE r.study_name=? AND r.run_type='validation'
                  AND r.source_dates=? AND r.status='done'
            """, (study_name, src_dates_key)).fetchall()

            if not vrows:
                print(f"Validation 결과 없음 | study={study_name} | {date_range}")
                print("  -> 먼저 --validate-study 실행 필요")
                return

            # config_hash → validation 매핑
            val_by_hash = {row["config_hash"]: row for row in vrows}

            # ── train 상위 config 조회 ─────────────────────────────
            train_rows = conn.execute("""
                SELECT
                    o.trial_no,
                    r.config_hash,
                    ROUND(COALESCE(o.objective_score, 0), 2) AS train_obj,
                    COALESCE(s.trade_count, 0)               AS train_trades
                FROM sim_observations o
                JOIN sim_runs r ON r.id = o.run_id
                LEFT JOIN sim_run_summary s ON s.run_id = r.id
                WHERE o.study_name=? AND r.status='done'
                ORDER BY o.objective_score DESC
                LIMIT ?
            """, (study_name, top_n)).fetchall()

            if not train_rows:
                print(f"Train 결과 없음 | study={study_name}")
                return

            W = 118
            print(f"\n{'='*W}")
            print(f"  VALIDATION REPORT  |  study={study_name}  |  valid: {date_range}")
            print(f"  채택 기준: obj>{_VALID_CRITERIA['min_objective']:.1f}  "
                  f"거래>={int(_VALID_CRITERIA['min_trade_count'])}  "
                  f"낙폭>={_VALID_CRITERIA['max_drawdown_ticks']:.0f}t  "
                  f"양봉%>={_VALID_CRITERIA['min_session_pos_ratio']*100:.0f}%  "
                  f"강건성>={_VALID_CRITERIA['min_robustness']*100:.0f}%")
            print(f"{'='*W}")
            print(
                f"  {'순':>3}  {'hash':>8}  {'train':>8}  {'valid':>8}  "
                f"{'강건성':>7}  {'v거래':>5}  {'v승%':>6}  "
                f"{'v낙폭':>8}  {'v양봉%':>7}  {'판정':>10}"
            )
            print(f"  {'-'*114}")

            passed_count  = 0
            best_adopted  = None

            for rank, tr in enumerate(train_rows, 1):
                cfg_hash  = tr["config_hash"] or ""
                train_obj = float(tr["train_obj"])
                vr        = val_by_hash.get(cfg_hash)

                if vr is None:
                    print(
                        f"  {rank:3d}  {cfg_hash:>8}  "
                        f"{train_obj:+8.2f}  {'N/A':>8}  "
                        f"{'N/A':>7}  {'N/A':>5}  {'N/A':>6}  "
                        f"{'N/A':>8}  {'N/A':>7}  {'미실행':>10}"
                    )
                    continue

                v_obj      = float(vr["v_obj"])
                v_trades   = int(vr["v_trades"])
                v_win_pct  = float(vr["v_win_pct"])
                v_dd       = float(vr["v_dd"])
                v_pos_pct  = float(vr["v_pos_pct"])
                v_pf       = float(vr["v_pf"])
                robustness = (v_obj / train_obj) if train_obj > 0 else float("-inf")

                # 채택 판정
                crit    = _VALID_CRITERIA
                reasons: List[str] = []
                ok      = True
                if v_obj   <= crit["min_objective"]:        reasons.append("obj음수");     ok = False
                if v_trades < crit["min_trade_count"]:      reasons.append("거래부족");   ok = False
                if v_dd    < crit["max_drawdown_ticks"]:    reasons.append("낙폭초과");   ok = False
                if (v_pos_pct/100) < crit["min_session_pos_ratio"]:
                    reasons.append("세션불안정");  ok = False
                if robustness < crit["min_robustness"]:     reasons.append("강건성미달"); ok = False

                verdict = "YES✓" if ok else f"NO✗  [{', '.join(reasons)}]"
                if ok:
                    passed_count += 1
                    if best_adopted is None:
                        best_adopted = (rank, cfg_hash, v_obj)

                print(
                    f"  {rank:3d}  {cfg_hash:>8}  "
                    f"{train_obj:+8.2f}  {v_obj:+8.2f}  "
                    f"{robustness*100:+6.1f}%  "
                    f"{v_trades:5d}  {v_win_pct:5.1f}%  "
                    f"{v_dd:+8.2f}t  {v_pos_pct:6.1f}%  "
                    f"{verdict}"
                )

            print(f"{'='*W}")
            total = len(train_rows)
            print(f"  통과: {passed_count}/{total}", end="")
            if best_adopted:
                r, h, vo = best_adopted
                print(f"  |  권장 config: #{r} hash={h}  valid_obj={vo:+.2f}", end="")
            print()
            print()

        finally:
            conn.close()

    # ── 채택 자동화 ──────────────────────────────────────────────────

    def adopt_best(
        self,
        study_name: str,
        valid_dates: List[str],
        top_n: int = 10,
        force: bool = False,
    ) -> List[Dict]:
        """
        validation YES 인 config 를 sim_adoptions 에 저장.

        - validation 이 아직 실행 안 된 경우 run_validation() 먼저 호출.
        - force=True 면 기존 study 의 모든 sim_adoptions 삭제 후 재삽입.
        - 반환: 채택된 레코드 list (dict, sim_adoptions row 기준).
        """
        dates_sorted  = sorted(set(valid_dates))
        src_dates_key = _canonical_dates(dates_sorted)

        # ── validation 결과 확인 → 없으면 자동 실행 ─────────────────
        _log_db("ADOPT", msg="sim_runs 테이블에서 validation 완료 여부를 확인합니다.",
                테이블="sim_runs",
                검색조건=f"study_name={study_name!r}, run_type='validation', source_dates={src_dates_key!r}")
        conn = self._connect_sim()
        try:
            existing_valid = conn.execute(
                """SELECT COUNT(*) FROM sim_runs
                   WHERE study_name=? AND run_type='validation'
                     AND source_dates=? AND status='done'""",
                (study_name, src_dates_key)
            ).fetchone()[0]
        finally:
            conn.close()

        _log_db("ADOPT", 조건일치_validation_run수=existing_valid)
        if existing_valid == 0:
            _log_db("ADOPT", msg="validation 결과 없음 → run_validation() 자동 실행")
            self.run_validation(study_name, valid_dates, top_n, force=False)

        # ── 채택 작업 ─────────────────────────────────────────────────
        conn = self._connect_sim()
        try:
            if force:
                conn.execute(
                    "DELETE FROM sim_adoptions WHERE study_name=?",
                    (study_name,)
                )
                conn.commit()
                log.info("adopt_best: 기존 sim_adoptions 삭제 | study=%s", study_name)

            # train 상위 config + validation 결과 JOIN
            rows = conn.execute("""
                SELECT
                    o.run_id          AS train_run_id,
                    r_t.config_hash,
                    r_t.config_json,
                    COALESCE(o.objective_score,   0) AS train_obj,
                    COALESCE(st.trade_count,      0) AS train_trades,
                    COALESCE(st.total_pnl,        0) AS train_total_pnl,

                    r_v.id            AS valid_run_id,
                    COALESCE(sv.objective_score,  0) AS valid_obj,
                    COALESCE(sv.trade_count,      0) AS valid_trades,
                    COALESCE(sv.total_pnl,        0) AS valid_total_pnl,
                    COALESCE(sv.max_drawdown,     0) AS valid_max_dd,
                    COALESCE(sv.session_positive_ratio, 0) AS valid_pos_ratio
                FROM sim_observations o
                JOIN sim_runs r_t ON r_t.id = o.run_id
                LEFT JOIN sim_run_summary st ON st.run_id = r_t.id
                -- validation run 매칭 (같은 study_name + config_hash + valid dates)
                LEFT JOIN sim_runs r_v
                    ON  r_v.study_name    = o.study_name
                    AND r_v.config_hash   = r_t.config_hash
                    AND r_v.run_type      = 'validation'
                    AND r_v.session_mode  = 'night'
                    AND r_v.source_dates  = ?
                    AND r_v.status        = 'done'
                LEFT JOIN sim_run_summary sv ON sv.run_id = r_v.id
                WHERE o.study_name = ?
                  AND r_t.run_type    IN ('random', 'bayes_opt')
                  AND r_t.session_mode = 'night'
                  AND r_t.status      = 'done'
                ORDER BY o.objective_score DESC
                LIMIT ?
            """, (src_dates_key, study_name, top_n)).fetchall()

            now_str   = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            crit      = _VALID_CRITERIA
            adopted   = []

            for row in rows:
                cfg_hash    = row["config_hash"] or ""
                train_obj   = float(row["train_obj"])
                valid_obj   = float(row["valid_obj"])
                v_trades    = int(row["valid_trades"])
                v_dd        = float(row["valid_max_dd"])
                v_pos_r     = float(row["valid_pos_ratio"])
                robustness  = (valid_obj / train_obj) if train_obj > 0 else float("-inf")

                # 채택 기준 판정
                ok = (
                    valid_obj   >  crit["min_objective"]
                    and v_trades >= crit["min_trade_count"]
                    and v_dd     >= crit["max_drawdown_ticks"]
                    and v_pos_r  >= crit["min_session_pos_ratio"]
                    and robustness >= crit["min_robustness"]
                )
                if not ok:
                    continue

                # 중복 체크 (force=False 시)
                if not force:
                    dup = conn.execute(
                        "SELECT id FROM sim_adoptions WHERE study_name=? AND config_hash=?",
                        (study_name, cfg_hash)
                    ).fetchone()
                    if dup:
                        log.debug("adopt_best: 이미 채택됨 | hash=%s — 건너뜀", cfg_hash)
                        continue

                # 레짐 패턴 조회 (session_patterns from mmean.db)
                train_pattern  = None
                valid_pats_map = {}
                try:
                    src_conn = sqlite3.connect(
                        f"file:{self.source_db}?mode=ro", uri=True, timeout=5
                    )
                    # 훈련 날짜: study_name = "night_YYYY-MM-DD"
                    train_date = study_name[6:] if study_name.startswith("night_") else None
                    if train_date:
                        r_pat = src_conn.execute(
                            "SELECT pattern_type FROM session_patterns "
                            "WHERE date=? AND session_mode='night'",
                            (train_date,)
                        ).fetchone()
                        train_pattern = r_pat[0] if r_pat else None
                    for vd in dates_sorted:
                        r_vp = src_conn.execute(
                            "SELECT pattern_type FROM session_patterns "
                            "WHERE date=? AND session_mode='night'",
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
                     status)
                    VALUES (?,?,?,?, ?,?,?,?, ?,?, ?,?,?, ?,?,?, ?,?, 'candidate')
                """, (
                    study_name, cfg_hash, row["config_json"], now_str,
                    row["train_run_id"], train_obj,
                    row["train_trades"], float(row["train_total_pnl"]),
                    row["valid_run_id"], src_dates_key,
                    valid_obj, v_trades, float(row["valid_total_pnl"]),
                    v_dd, v_pos_r, robustness,
                    train_pattern, valid_patterns_json,
                ))
                conn.commit()
                adopted.append({
                    "config_hash":  cfg_hash,
                    "train_obj":    train_obj,
                    "valid_obj":    valid_obj,
                    "robustness":   robustness,
                    "v_trades":     v_trades,
                })
                log.info("adopt_best: 채택 | hash=%s  train=%.2f  valid=%.2f  robust=%.1f%%",
                         cfg_hash, train_obj, valid_obj, robustness * 100)

            log.info("adopt_best 완료 | study=%s 채택=%d/%d",
                     study_name, len(adopted), len(rows))
            return adopted

        finally:
            conn.close()

    # ── 슬리피지 스트레스 검증 ───────────────────────────────────────

    def run_slippage_stress(
        self,
        study_name:      str,
        valid_dates:     List[str],
        slippage_values: tuple = _STRESS_SLIPPAGE_VALUES,
        force:           bool  = False,
    ) -> List[Dict]:
        """
        sim_adoptions 후보에 대해 슬리피지 값을 달리해 재실행.

        각 후보 × 슬리피지 값 → sim_runs (run_type='stress',
        parent_run_id=valid_run_id) 저장.
        완료 후 sim_adoptions.stress_1t_obj / stress_2t_obj / stress_passed 갱신.

        stress_passed = slippage_values 모두에서 objective_score > 0

        반환: 갱신된 adoption 결과 list
        """
        dates_sorted  = sorted(set(valid_dates))
        src_dates_key = _canonical_dates(dates_sorted)

        _log_db("STRESS", msg="sim_adoptions 테이블에서 stress 후보를 조회합니다.",
                테이블="sim_adoptions",
                검색조건=f"study_name={study_name!r}, status='candidate'",
                정렬="valid_obj DESC")
        conn = self._connect_sim()
        try:
            # ── 후보 조회 ─────────────────────────────────────────────
            candidates = conn.execute(
                """SELECT id, config_hash, config_json, valid_run_id
                   FROM sim_adoptions
                   WHERE study_name=? AND status='candidate'
                   ORDER BY valid_obj DESC""",
                (study_name,)
            ).fetchall()

            _log_db("STRESS", 조회된_후보수=len(candidates))
            if not candidates:
                _log_db("STRESS", msg="후보 없음 — adopt_best 먼저 실행 필요",
                        테이블="sim_adoptions",
                        조건=f"study_name={study_name!r}, status='candidate'")
                return []

            _log_db("STRESS", msg="슬리피지 스트레스 시작",
                    study_name=study_name, 후보수=len(candidates),
                    slippage_values=slippage_values, valid_dates=dates_sorted)

            # ── 틱 1회 로드 ──────────────────────────────────────────
            sessions_ticks = self.load_night_sessions(dates_sorted)
            total_ticks    = sum(len(v) for v in sessions_ticks.values())

            results = []

            for cand in candidates:
                adoption_id = cand["id"]
                cfg_hash    = cand["config_hash"]
                base_config = json.loads(cand["config_json"])
                valid_run_id = cand["valid_run_id"]

                stress_objs: Dict[int, float] = {}   # {slippage_val: obj_score}

                for slip in slippage_values:
                    # ── 멱등성 체크 ──────────────────────────────────
                    if not force:
                        done = conn.execute(
                            """SELECT id FROM sim_runs
                               WHERE study_name=? AND config_hash=?
                                 AND run_type='stress' AND source_dates=?
                                 AND status='done'
                                 AND config_json LIKE ?""",
                            (study_name, cfg_hash, src_dates_key,
                             f'%"sim_slippage_ticks": {slip}%')
                        ).fetchone()
                        if done:
                            # 기존 objective 읽기
                            existing_obj = conn.execute(
                                "SELECT objective_score FROM sim_run_summary WHERE run_id=?",
                                (done["id"],)
                            ).fetchone()
                            stress_objs[slip] = float(
                                existing_obj["objective_score"] if existing_obj else 0
                            )
                            log.debug("Stress 이미 완료 | hash=%s slip=%d — 건너뜀",
                                      cfg_hash, slip)
                            continue

                    # ── config 슬리피지 오버라이드 ────────────────────
                    cfg_stress       = dict(base_config)
                    cfg_stress["sim_slippage_ticks"] = slip
                    cfg_stress_json  = json.dumps(cfg_stress, sort_keys=True,
                                                  ensure_ascii=False)

                    started_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                    cur = conn.execute(
                        """INSERT INTO sim_runs
                           (run_type, session_mode, source_dates, date_count,
                            study_name, config_hash, config_json,
                            parent_run_id,
                            run_started_at, tick_count_total, warmup_ticks, status)
                           VALUES ('stress','night',?,?,  ?,?,?,  ?,  ?,?,?,'running')""",
                        (src_dates_key, len(dates_sorted),
                         study_name, cfg_hash, cfg_stress_json,
                         valid_run_id,
                         started_at, total_ticks, _WARMUP_TICKS)
                    )
                    run_id = cur.lastrowid
                    conn.commit()

                    all_trades:       List[Dict]  = []
                    session_pnl_list: List[float] = []
                    batch:            List[tuple] = []

                    try:
                        for session_date in dates_sorted:
                            ticks = sessions_ticks[session_date]
                            if not ticks:
                                _insert_session_record(conn, run_id, session_date, 0, [])
                                session_pnl_list.append(0.0)
                                continue

                            next_date = (datetime.strptime(session_date, "%Y-%m-%d")
                                         + timedelta(days=1)).strftime("%Y-%m-%d")
                            engine = NightSimEngine(cfg_stress, next_date)

                            for idx, row in enumerate(ticks):
                                if idx < _WARMUP_TICKS:
                                    continue
                                trade = engine.on_tick(row, idx)
                                if trade:
                                    batch.append((
                                        run_id, session_date, "night",
                                        trade["open_ts"],     trade["close_ts"],
                                        trade["direction"],
                                        trade["entry_price"], trade["exit_price"],
                                        trade["pnl_ticks"],   trade["exit_reason"],
                                        trade["hold_ticks"],
                                        trade["max_favorable_pt"],
                                        trade["max_adverse_excursion"],
                                        trade["entry_long_score"],
                                        trade["entry_short_score"],
                                        trade["entry_confidence"],
                                        trade["entry_llm_score"],
                                        trade["entry_llm_valid"],
                                        trade["entry_session_phase"],
                                    ))
                                    if len(batch) >= _BATCH_SIZE:
                                        _insert_trades_batch(conn, batch)
                                        batch.clear()

                            session_pnl = sum(t["pnl_ticks"] for t in engine.trades)
                            _insert_session_record(conn, run_id, session_date,
                                                   len(ticks), engine.trades)
                            session_pnl_list.append(session_pnl)
                            all_trades.extend(engine.trades)

                        if batch:
                            _insert_trades_batch(conn, batch)

                        finished_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                        conn.execute(
                            """UPDATE sim_runs
                               SET status='done', run_finished_at=?, trade_count=?
                               WHERE id=?""",
                            (finished_at, len(all_trades), run_id)
                        )
                        conn.commit()

                        obj = _compute_and_insert_summary(
                            conn, run_id, all_trades,
                            session_count=len(dates_sorted),
                            session_pnl_list=session_pnl_list,
                        )
                        stress_objs[slip] = obj
                        log.info("Stress 완료 | hash=%s slip=%d obj=%.2f",
                                 cfg_hash, slip, obj)

                    except Exception as exc:
                        conn.execute(
                            "UPDATE sim_runs SET status='error', error_message=? WHERE id=?",
                            (str(exc)[:500], run_id)
                        )
                        conn.commit()
                        log.warning("Stress 실패 | hash=%s slip=%d: %s",
                                    cfg_hash, slip, exc)
                        stress_objs[slip] = float("-inf")

                # ── sim_adoptions 갱신 ───────────────────────────────
                s1 = stress_objs.get(1)
                s2 = stress_objs.get(2)
                # stress_passed: 모든 실행 슬리피지에서 objective > 0
                s_passed = int(all(
                    v is not None and v > 0
                    for v in stress_objs.values()
                ) and len(stress_objs) > 0)

                conn.execute(
                    """UPDATE sim_adoptions
                       SET stress_1t_obj=?, stress_2t_obj=?, stress_passed=?
                       WHERE id=?""",
                    (s1, s2, s_passed, adoption_id)
                )
                conn.commit()

                results.append({
                    "config_hash":  cfg_hash,
                    "stress_objs":  stress_objs,
                    "stress_passed": bool(s_passed),
                })

                pass_str = "PASS" if s_passed else "FAIL"
                obj_parts = "  ".join(
                    f"slip{k}={v:+.2f}" for k, v in sorted(stress_objs.items())
                )
                print(f"  stress | hash={cfg_hash}  {obj_parts}  => {pass_str}",
                      flush=True)

            log.info("슬리피지 스트레스 완료 | study=%s passed=%d/%d",
                     study_name,
                     sum(1 for r in results if r["stress_passed"]),
                     len(results))
            return results

        finally:
            conn.close()

    # ── 군집화 ───────────────────────────────────────────────────────

    def cluster_top_configs(
        self,
        study_name: str,
        top_n:      int = 50,
        n_clusters: int = 5,
    ) -> List[Dict]:
        """
        study 상위 top_n config 를 n_clusters 개 군집으로 분류.

        외부 의존성 없음 (순수 Python).
        알고리즘: Farthest-Point Sampling 초기화 → 1회 할당 → 대표 선정.

        반환: 군집 목록 (list of dict):
            [{
                "cluster_id":    int,
                "representative": dict (config),  # 군집 내 best objective
                "rep_hash":      str,
                "rep_obj":       float,
                "size":          int,
                "members":       [{"config_hash", "objective_score"}, ...]
            }, ...]
        """
        configs = self.load_top_configs(study_name, top_n)
        if not configs:
            log.warning("cluster_top_configs: study=%s 결과 없음", study_name)
            return []

        k = min(n_clusters, len(configs))

        # ── 거리 함수 ─────────────────────────────────────────────
        def _dist(a: Dict, b: Dict) -> float:
            ca, cb = a["config"], b["config"]
            sq = 0.0
            for param in _CLUSTER_FLOAT_PARAMS:
                spec = _NIGHT_SEARCH_SPACE.get(param)
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

        # ── Farthest-Point Sampling 으로 k개 중심 선택 ────────────
        centers_idx = [0]   # 첫 번째 중심 = objective 최고 config
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

        # ── 모든 config → 가장 가까운 중심에 할당 ─────────────────
        clusters: List[List[int]] = [[] for _ in range(len(centers_idx))]
        for i, cfg in enumerate(configs):
            nearest = min(
                range(len(centers_idx)),
                key=lambda ci: _dist(cfg, configs[centers_idx[ci]])
            )
            clusters[nearest].append(i)

        # ── 군집별 대표 선정 (objective 최고값) ─────────────────────
        result = []
        for cid, members_idx in enumerate(clusters):
            if not members_idx:
                continue
            best_item = max(
                (configs[i] for i in members_idx),
                key=lambda c: c["train_obj"]
            )
            result.append({
                "cluster_id":     cid + 1,
                "representative": best_item["config"],
                "rep_hash":       best_item["config_hash"],
                "rep_obj":        best_item["train_obj"],
                "size":           len(members_idx),
                "members": [
                    {
                        "config_hash":     configs[i]["config_hash"],
                        "objective_score": configs[i]["train_obj"],
                    }
                    for i in members_idx
                ],
            })

        # cluster_id 를 sim_adoptions 에도 반영
        conn = self._connect_sim()
        try:
            for cl in result:
                for m in cl["members"]:
                    conn.execute(
                        """UPDATE sim_adoptions SET cluster_id=?
                           WHERE study_name=? AND config_hash=?""",
                        (cl["cluster_id"], study_name, m["config_hash"])
                    )
            conn.commit()
        finally:
            conn.close()

        return result

    def print_adoptions(self, study_name: str) -> None:
        """sim_adoptions 현황 출력 (채택 현황 + 슬리피지 스트레스 결과)."""
        conn = self._connect_sim()
        try:
            rows = conn.execute("""
                SELECT
                    id,
                    config_hash,
                    status,
                    cluster_id,
                    ROUND(train_obj,      2) AS train_obj,
                    train_trades,
                    ROUND(valid_obj,      2) AS valid_obj,
                    valid_trades,
                    ROUND(valid_max_dd,   2) AS v_dd,
                    ROUND(valid_pos_ratio*100, 1) AS v_pos_pct,
                    ROUND(robustness*100, 1) AS robust_pct,
                    stress_1t_obj,
                    stress_2t_obj,
                    stress_passed,
                    adopted_at
                FROM sim_adoptions
                WHERE study_name=?
                ORDER BY valid_obj DESC
            """, (study_name,)).fetchall()

            if not rows:
                print(f"채택 후보 없음 | study={study_name}")
                print("  -> --validate-study 후 --adopt 실행 필요")
                return

            W = 128
            print(f"\n{'='*W}")
            print(f"  ADOPTIONS  |  study={study_name}  ({len(rows)}개)")
            print(f"{'='*W}")
            print(
                f"  {'#':>3}  {'hash':>8}  {'status':>10}  "
                f"{'cl':>3}  {'train':>8}  {'valid':>8}  "
                f"{'robust':>7}  {'v거래':>5}  {'v낙폭':>8}  "
                f"{'v양봉%':>7}  {'stress1t':>9}  {'stress2t':>9}  {'st통과':>6}"
            )
            print(f"  {'-'*124}")

            best_adopted = None
            for r in rows:
                s1 = r["stress_1t_obj"]
                s2 = r["stress_2t_obj"]
                sp = r["stress_passed"]
                s1_str = f"{s1:+9.2f}" if s1 is not None else "      N/A"
                s2_str = f"{s2:+9.2f}" if s2 is not None else "      N/A"
                sp_str = "PASS" if sp == 1 else ("FAIL" if sp == 0 and s1 is not None else "   -")

                print(
                    f"  {r['id']:3d}  "
                    f"{r['config_hash'] or '':>8}  "
                    f"{r['status']:>10}  "
                    f"{r['cluster_id'] or 0:3d}  "
                    f"{r['train_obj'] or 0:+8.2f}  "
                    f"{r['valid_obj'] or 0:+8.2f}  "
                    f"{r['robust_pct'] or 0:+6.1f}%  "
                    f"{r['valid_trades'] or 0:5d}  "
                    f"{r['v_dd'] or 0:+8.2f}t  "
                    f"{r['v_pos_pct'] or 0:6.1f}%  "
                    f"{s1_str}  {s2_str}  {sp_str:>6}"
                )

                if best_adopted is None or (r["valid_obj"] or 0) > (best_adopted["valid_obj"] or 0):
                    best_adopted = dict(r)

            print(f"{'='*W}")
            if best_adopted:
                stress_ok = (best_adopted["stress_passed"] == 1)
                stress_tag = " | stress=PASS" if stress_ok else ""
                print(
                    f"  ★ 권장: hash={best_adopted['config_hash']}  "
                    f"train={best_adopted['train_obj']:+.2f}  "
                    f"valid={best_adopted['valid_obj']:+.2f}  "
                    f"robust={best_adopted['robust_pct']:+.1f}%"
                    f"{stress_tag}"
                )
            print()

        finally:
            conn.close()

    # ── 성적표 출력 (sim_db) ─────────────────────────────────────────

    def print_scorecard(self, session_date: str) -> None:
        """단일 날짜 기준 성적표 — sim_run_summary에서 집계값 직접 조회."""
        next_date = (datetime.strptime(session_date, "%Y-%m-%d")
                     + timedelta(days=1)).strftime("%Y-%m-%d")
        conn = self._connect_sim()
        try:
            rows = conn.execute("""
                SELECT
                    r.profile_no,
                    r.date_count,
                    COALESCE(s.trade_count,     0)     AS trades,
                    ROUND(COALESCE(s.win_rate,  0)*100,1) AS win_pct,
                    ROUND(COALESCE(s.total_pnl, 0),2)  AS total_pnl,
                    ROUND(COALESCE(s.avg_pnl,   0),2)  AS avg_pnl,
                    ROUND(COALESCE(s.worst_trade,0),2) AS worst,
                    ROUND(COALESCE(s.profit_factor,0),2) AS profit_factor,
                    ROUND(COALESCE(s.max_drawdown,  0),2) AS max_drawdown,
                    ROUND(COALESCE(s.avg_hold_ticks,0),0) AS avg_hold,
                    ROUND(COALESCE(s.objective_score,0),2) AS obj_score
                FROM sim_runs r
                LEFT JOIN sim_run_summary s ON s.run_id = r.id
                WHERE r.source_dates LIKE ? AND r.status='done'
                  AND r.run_type='profile' AND r.session_mode='night'
                ORDER BY r.profile_no
            """, (f'%"{session_date}"%',)).fetchall()

            if not rows:
                print(f"야간 성적 데이터 없음 | {session_date}")
                return

            self._print_scorecard_rows(rows, session_date, next_date)
        finally:
            conn.close()

    def print_multi_scorecard(self, dates: List[str]) -> None:
        """멀티 세션 성적표 — source_dates canonical key 기준 조회."""
        source_dates_json = _canonical_dates(dates)
        dates_sorted = sorted(set(dates))
        title = f"{dates_sorted[0]} ~ {dates_sorted[-1]} ({len(dates_sorted)}일)"
        conn = self._connect_sim()
        try:
            rows = conn.execute("""
                SELECT
                    r.profile_no,
                    r.date_count,
                    COALESCE(s.trade_count,       0)      AS trades,
                    ROUND(COALESCE(s.win_rate,    0)*100,1) AS win_pct,
                    ROUND(COALESCE(s.total_pnl,   0),2)   AS total_pnl,
                    ROUND(COALESCE(s.avg_pnl,     0),2)   AS avg_pnl,
                    ROUND(COALESCE(s.worst_trade, 0),2)   AS worst,
                    ROUND(COALESCE(s.profit_factor,0),2)  AS profit_factor,
                    ROUND(COALESCE(s.max_drawdown,  0),2) AS max_drawdown,
                    ROUND(COALESCE(s.avg_hold_ticks,0),0) AS avg_hold,
                    ROUND(COALESCE(s.objective_score,0),2) AS obj_score,
                    ROUND(COALESCE(s.session_positive_ratio,0)*100,1) AS pos_pct,
                    ROUND(COALESCE(s.pnl_std,0),2)        AS pnl_std
                FROM sim_runs r
                LEFT JOIN sim_run_summary s ON s.run_id = r.id
                WHERE r.source_dates=? AND r.status='done'
                  AND r.run_type='profile' AND r.session_mode='night'
                ORDER BY r.profile_no
            """, (source_dates_json,)).fetchall()

            if not rows:
                print(f"멀티세션 성적 데이터 없음 | {title}")
                return

            self._print_scorecard_rows(rows, title, "", multi=True)
        finally:
            conn.close()

    def _print_scorecard_rows(self, rows, title: str, next_date: str,
                              multi: bool = False) -> None:
        """공통 성적표 출력 로직."""
        W = 100
        print(f"\n{'='*W}")
        if multi:
            print(f"  NIGHT MULTI-SESSION SCORECARD  |  {title}")
        else:
            print(f"  NIGHT SESSION SCORECARD  |  {title} 18:00 ~ {next_date} 06:00")
        print(f"{'='*W}")

        hdr = (f"{'프로파일':>14}  {'거래':>5}  {'승%':>6}  {'총PnL':>9}  "
               f"{'평균':>7}  {'최저':>7}  {'P/F':>5}  "
               f"{'낙폭':>7}  {'목표점수':>8}")
        if multi:
            hdr += f"  {'양봉%':>6}  {'표준편차':>8}"
        print(hdr)
        print(f"{'-'*W}")

        profiles = self._get_profiles()
        valid = [r for r in rows if (r["trades"] or 0) >= 3]
        best  = max(valid, key=lambda r: (r["obj_score"] or -9999)) if valid else None
        worst = min(valid, key=lambda r: (r["obj_score"] or  9999)) if valid else None

        for r in rows:
            pno   = r["profile_no"]
            label = profiles.get(pno, {}).get("label", f"P{pno:02d}")
            flag  = ""
            if   best  and pno == best["profile_no"]:  flag = " ◀ BEST"
            elif worst and pno == worst["profile_no"]: flag = " ◀ WORST"
            low = " ⚠부족" if (r["trades"] or 0) < 3 else ""

            line = (
                f"  P{pno:02d} {label[:8]:8s}  "
                f"{r['trades']        or 0:5d}  "
                f"{r['win_pct']       or 0:5.1f}%  "
                f"{r['total_pnl']     or 0:+9.2f}t  "
                f"{r['avg_pnl']       or 0:+7.2f}t  "
                f"{r['worst']         or 0:+7.2f}t  "
                f"{r['profit_factor'] or 0:5.2f}  "
                f"{r['max_drawdown']  or 0:+7.2f}t  "
                f"{r['obj_score']     or 0:+8.2f}"
            )
            if multi:
                line += (
                    f"  {r['pos_pct'] or 0:5.1f}%  "
                    f"{r['pnl_std']  or 0:8.2f}"
                )
            print(line + flag + low)

        print(f"{'='*W}")
        if best:
            print(f"  ★ BEST : P{best['profile_no']:02d}  "
                  f"총PnL={best['total_pnl'] or 0:+.2f}t  "
                  f"승율={best['win_pct'] or 0:.1f}%  "
                  f"목표점수={best['obj_score'] or 0:+.2f}")
        if worst:
            print(f"  ☆ WORST: P{worst['profile_no']:02d}  "
                  f"총PnL={worst['total_pnl'] or 0:+.2f}t  "
                  f"목표점수={worst['obj_score'] or 0:+.2f}")
        print()


# ────────────────────────────────────────────────────────────────────
# 헬퍼 함수
# ────────────────────────────────────────────────────────────────────

# 최소 거래 수 패널티 기준
_MIN_TRADE_COUNT  = 5    # 미만: 강한 패널티 (× 4.0)
_SOFT_MIN_TRADES  = 10   # 미만: 약한 패널티 (× 1.5)  ← 운 좋은 소수거래 차단

# 랜덤 샘플링 최대 재시도 횟수 (제약 조건 충족 시까지)
_MAX_SAMPLE_ATTEMPTS = 30

# ── Validation 채택 기준 ───────────────────────────────────────────────
# run_validation() 에서 각 config 의 validation 결과를 판정할 때 사용.
# 모든 기준을 동시에 충족해야 YES 판정.
_VALID_CRITERIA: Dict[str, float] = {
    "min_objective":          0.0,    # validation objective_score > 0
    "min_trade_count":        5,      # validation 최소 거래 수
    "max_drawdown_ticks":   -20.0,    # validation 최대 낙폭 (음수, 절댓값 20t 이내)
    "min_session_pos_ratio":  0.50,   # validation 수익 세션 비율 >= 50%
    "min_robustness":         0.40,   # valid_obj / train_obj >= 40%  (강건성)
}


def _sample_random_config(rng=None) -> Dict:
    """
    _NIGHT_SEARCH_SPACE 기준으로 랜덤 config 1개 샘플링.

    제약 조건:
      - sim_tp_ticks  > sim_sl_ticks  (최소 2틱 이상 차이)
      - sim_trailing_activate < sim_tp_ticks
      - sim_trailing_ticks    <= sim_trailing_activate
      - not night_regime_exit → night_neutral_exit_ticks = 0
    """
    import random

    if rng is None:
        rng = random.Random()

    for _ in range(_MAX_SAMPLE_ATTEMPTS):
        cfg: Dict = {}
        for key, spec in _NIGHT_SEARCH_SPACE.items():
            if spec[0] == "float":
                cfg[key] = round(rng.uniform(spec[1], spec[2]), 3)
            elif spec[0] == "int":
                cfg[key] = rng.randint(int(spec[1]), int(spec[2]))
            elif spec[0] == "bool":
                cfg[key] = rng.choice([True, False])

        # ── 제약 ① : TP > SL + 2 ────────────────────────────────────
        if cfg["sim_tp_ticks"] < cfg["sim_sl_ticks"] + 2:
            continue  # 재샘플

        # ── 제약 ② : trailing_activate < TP ─────────────────────────
        if cfg["sim_trailing_activate"] >= cfg["sim_tp_ticks"]:
            cfg["sim_trailing_activate"] = round(
                cfg["sim_tp_ticks"] * rng.uniform(0.4, 0.7), 1
            )

        # ── 제약 ③ : trailing_ticks <= trailing_activate ─────────────
        if cfg["sim_trailing_ticks"] > cfg["sim_trailing_activate"]:
            cfg["sim_trailing_ticks"] = max(
                1.0, round(cfg["sim_trailing_activate"] * 0.5, 1)
            )

        # ── 제약 ④ : night_regime_exit OFF → neutral_exit = 0 ───────
        if not cfg.get("night_regime_exit"):
            cfg["night_neutral_exit_ticks"] = 0

        cfg["label"] = "random"
        return cfg

    raise RuntimeError("랜덤 샘플링 제약 충족 실패 — 탐색 공간을 확인하세요")

def _canonical_dates(dates: List[str]) -> str:
    """날짜 목록을 정렬 후 JSON 직렬화 — 멱등성 키로 사용."""
    return json.dumps(sorted(set(dates)))


def _compute_and_insert_summary(conn: sqlite3.Connection,
                                 run_id: int,
                                 trades: List[Dict],
                                 session_count: int = 1,
                                 session_pnl_list: Optional[List[float]] = None) -> float:
    """
    trade 목록으로 sim_run_summary를 계산하고 DB에 저장.
    objective_score를 반환한다.

    session_count      : 평가에 사용된 세션 수 (단일=1, 멀티>1)
    session_pnl_list   : 세션별 총 PnL 리스트 (멀티세션 강건성 계산용)
    """
    import math

    now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    if not trades:
        conn.execute("""
            INSERT OR REPLACE INTO sim_run_summary
            (run_id, total_pnl, win_rate, profit_factor, avg_pnl,
             trade_count, avg_hold_ticks, worst_trade,
             max_drawdown, max_consecutive_loss,
             session_count, session_positive_ratio, pnl_std,
             low_trade_penalty, slippage_sensitivity,
             objective_score, computed_at)
            VALUES (?,  0,0,0,0, 0,0,0, 0,0, ?,0,0, ?,0, ?,?)
        """, (run_id, session_count, float(_MIN_TRADE_COUNT * 2), -999.0, now_str))
        conn.execute("UPDATE sim_runs SET objective_score=? WHERE id=?",
                     (-999.0, run_id))
        conn.commit()
        return -999.0

    pnl_list    = [t["pnl_ticks"] for t in trades]
    hold_list   = [t.get("hold_ticks", 0) for t in trades]
    trade_count = len(pnl_list)

    total_pnl     = sum(pnl_list)
    win_count     = sum(1 for p in pnl_list if p > 0)
    win_rate      = win_count / trade_count
    avg_pnl       = total_pnl / trade_count
    worst_trade   = min(pnl_list)
    avg_hold      = sum(hold_list) / trade_count

    wins   = [p for p in pnl_list if p > 0]
    losses = [abs(p) for p in pnl_list if p < 0]
    profit_factor = (sum(wins) / sum(losses)) if losses else 99.9

    # ── 최대 낙폭 (누적 PnL 기준) ───────────────────────────────────
    peak, max_dd = 0.0, 0.0
    cum = 0.0
    for p in pnl_list:
        cum += p
        if cum > peak:
            peak = cum
        dd = peak - cum
        if dd > max_dd:
            max_dd = dd
    max_drawdown = -max_dd   # 음수로 저장

    # ── 연속 손실 최대 횟수 ──────────────────────────────────────────
    max_consec, curr_consec = 0, 0
    for p in pnl_list:
        if p < 0:
            curr_consec += 1
            max_consec = max(max_consec, curr_consec)
        else:
            curr_consec = 0

    # ── 세션 강건성 (멀티세션 시 의미 있음) ─────────────────────────
    if session_pnl_list and len(session_pnl_list) > 1:
        session_positive_ratio = sum(1 for s in session_pnl_list if s > 0) / len(session_pnl_list)
        mean_s  = sum(session_pnl_list) / len(session_pnl_list)
        pnl_std = math.sqrt(sum((s - mean_s) ** 2 for s in session_pnl_list)
                            / len(session_pnl_list))
    else:
        session_positive_ratio = 1.0 if total_pnl > 0 else 0.0
        pnl_std = 0.0

    # ── 거래 수 부족 패널티 (2계층) ─────────────────────────────────
    # < _MIN_TRADE_COUNT  : 강한 패널티 — 운 좋은 극소수 거래 차단
    # < _SOFT_MIN_TRADES  : 약한 패널티 — 소수 거래 할인
    if trade_count < _MIN_TRADE_COUNT:
        low_trade_penalty = float((_MIN_TRADE_COUNT - trade_count) * 4.0)
    elif trade_count < _SOFT_MIN_TRADES:
        low_trade_penalty = float((_SOFT_MIN_TRADES - trade_count) * 1.5)
    else:
        low_trade_penalty = 0.0

    # ── 슬리피지 민감도 ──────────────────────────────────────────────
    # 슬리피지 +1틱 추가 시 총 PnL 변동 근사값
    # (진입/청산 각 1틱 = 거래당 2틱 비용 추가; 청산 조건 변화는 미반영)
    slippage_sensitivity = float(trade_count * 2)

    # ── composite objective score ────────────────────────────────────
    # total_pnl + PF보너스 - 낙폭패널티 - 거래부족패널티 - 세션불안정패널티
    # (슬리피지 민감도는 저장만 하고 objective에서는 제외 — 이미 시뮬에 반영됨)
    pf_bonus        = (profit_factor - 1.0) * 5.0
    dd_penalty      = abs(max_drawdown) * 0.3
    instability     = pnl_std * 0.2
    objective_score = (total_pnl + pf_bonus
                       - dd_penalty - low_trade_penalty - instability)

    conn.execute("""
        INSERT OR REPLACE INTO sim_run_summary
        (run_id, total_pnl, win_rate, profit_factor, avg_pnl,
         trade_count, avg_hold_ticks, worst_trade,
         max_drawdown, max_consecutive_loss,
         session_count, session_positive_ratio, pnl_std,
         low_trade_penalty, slippage_sensitivity,
         objective_score, computed_at)
        VALUES (?,?,?,?,?, ?,?,?, ?,?, ?,?,?, ?,?, ?,?)
    """, (
        run_id, total_pnl, win_rate, profit_factor, avg_pnl,
        trade_count, avg_hold, worst_trade,
        max_drawdown, max_consec,
        session_count, session_positive_ratio, pnl_std,
        low_trade_penalty, slippage_sensitivity,
        objective_score, now_str,
    ))
    conn.execute("UPDATE sim_runs SET objective_score=? WHERE id=?",
                 (objective_score, run_id))
    conn.commit()
    return objective_score


def _insert_trades_batch(conn: sqlite3.Connection,
                         batch: List[tuple]) -> None:
    """sim_trades에 배치 삽입."""
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
    """sim_run_sessions에 날짜별 요약 INSERT."""
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

    # 최대 낙폭
    peak, max_dd, cum = 0.0, 0.0, 0.0
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


# ────────────────────────────────────────────────────────────────────
# CLI
# ────────────────────────────────────────────────────────────────────

def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="MMEAN 야간장 프로파일별 재시뮬레이션",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
예시:
  # 단일 날짜
  python night_sim.py --date 2026-03-17 --all-profiles
  python night_sim.py --date 2026-03-17 --profile 2
  python night_sim.py --date 2026-03-17 --report-only

  # 멀티 날짜 (공백으로 구분)
  python night_sim.py --dates 2026-03-17 2026-03-18 2026-03-19 --all-profiles
  python night_sim.py --dates 2026-03-17 2026-03-18 --profile 3 --force

  # 탐색 (train)
  python night_sim.py --dates D1 D2 D3 --random 300 --study v1 --seed 42
  python night_sim.py --dates D1 D2 D3 --bayes 200 --study v1 --seed 42

  # Validation (dates = validation 날짜)
  python night_sim.py --dates D4 D5 --validate-study v1 --validate-top 10
  python night_sim.py --dates D4 D5 --validate-study v1 --report-only

  # 채택/스트레스/군집
  python night_sim.py --dates D4 D5 --validate-study v1 --adopt
  python night_sim.py --dates D4 D5 --validate-study v1 --stress
  python night_sim.py --date 2026-03-17 --adoptions --study v1
  python night_sim.py --date 2026-03-17 --cluster 5 --study v1 --top 50
""",
    )

    # ── 날짜 지정 (단일 or 멀티, 상호 배타) ─────────────────────────
    date_group = p.add_mutually_exclusive_group(required=True)
    date_group.add_argument(
        "--date",
        metavar="YYYY-MM-DD",
        help="단일 날짜 (당일 18:00 기준 야간 세션)",
    )
    date_group.add_argument(
        "--dates",
        nargs="+",
        metavar="YYYY-MM-DD",
        help="멀티 날짜 목록 (공백 구분) | 멀티세션 강건성 평가",
    )

    # ── 실행 모드 ─────────────────────────────────────────────────────
    p.add_argument("--profile",      type=int, default=None,
                   help="단일 프로파일 번호")
    p.add_argument("--all-profiles", action="store_true",
                   help="전체 프로파일 순차 실행")
    p.add_argument("--report-only",  action="store_true",
                   help="재생 없이 기존 결과 성적표만 출력")
    p.add_argument("--force",        action="store_true",
                   help="이미 완료된 run도 재실행")

    # ── 랜덤 탐색 ─────────────────────────────────────────────────────
    p.add_argument("--random",   type=int,  default=None, metavar="N",
                   help="N개 랜덤 config 탐색 후 상위 결과 출력")
    p.add_argument("--bayes",    type=int,  default=None, metavar="N",
                   help="N개 Optuna BO 탐색 (pip install optuna 필요)")
    p.add_argument("--startup",  type=int,  default=30,   metavar="N",
                   help="BO 초기 랜덤 탐색 수 (기본 30)")
    p.add_argument("--study",    type=str,  default=None, metavar="NAME",
                   help="탐색 그룹 이름 (자동생성 가능; --random/--bayes/--report-only와 함께)")
    p.add_argument("--seed",     type=int,  default=None, metavar="N",
                   help="난수 시드 (재현성)")
    p.add_argument("--top",      type=int,  default=10,   metavar="N",
                   help="상위 N개 결과 출력 (기본 10)")

    # ── Validation ────────────────────────────────────────────────────
    p.add_argument("--validate-study", type=str, default=None, metavar="NAME",
                   help="검증할 탐색 study 이름 (--dates = validation 날짜) [구방식]")
    p.add_argument("--validate-date",  type=str, default=None, metavar="YYYY-MM-DD",
                   help="날짜 기반 validation: 해당 날짜 전체 탐색 결과에서 top-N 추출 "
                        "(--dates = validation 날짜, --validate-top = N) [권장]")
    p.add_argument("--validate-top",   type=int, default=10,   metavar="N",
                   help="Validation 에 사용할 train 상위 N개 config (기본 10)")

    # ── 채택 자동화 ───────────────────────────────────────────────────
    p.add_argument("--adopt",   action="store_true",
                   help="validation YES config 자동 채택 (--validate-study 필요)")
    p.add_argument("--stress",  action="store_true",
                   help="채택 후보 슬리피지 스트레스 재실행 (--validate-study 필요)")
    p.add_argument("--adoptions", action="store_true",
                   help="채택 현황 출력 (--validate-study 필요)")

    # ── 군집화 ───────────────────────────────────────────────────────
    p.add_argument("--cluster",  type=int, default=None, metavar="K",
                   help="상위 설정 K개 군집화 후 대표값 출력 (--study 필요)")

    # ── DB / JSON 경로 ────────────────────────────────────────────────
    p.add_argument("--source-db",  default=_DEFAULT_SOURCE_DB,
                   help=f"원본 DB 읽기 전용 (기본: {_DEFAULT_SOURCE_DB})")
    p.add_argument("--sim-db",     default=_DEFAULT_SIM_DB,
                   help=f"SIM DB 쓰기 전용 (기본: {_DEFAULT_SIM_DB})")
    p.add_argument("--night-json", default=_DEFAULT_NIGHT_JSON,
                   help=f"night_levels.json 경로 (기본: {_DEFAULT_NIGHT_JSON})")
    return p.parse_args()


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
    )
    args   = _parse_args()
    runner = NightSimRunner(
        source_db  = args.source_db,
        sim_db     = args.sim_db,
        night_json = args.night_json,
    )

    # ── 날짜 목록 정규화 ──────────────────────────────────────────────
    if args.dates:
        dates      = sorted(set(args.dates))
        is_multi   = len(dates) > 1
        first_date = dates[0]
    else:
        dates      = [args.date]
        is_multi   = False
        first_date = args.date

    # ── report-only ───────────────────────────────────────────────────
    if args.report_only:
        if args.adoptions:
            study = args.validate_study or args.study
            if not study:
                print("--adoptions 와 함께 --validate-study NAME 또는 --study NAME 을 지정하세요.")
                sys.exit(1)
            runner.print_adoptions(study)
        elif args.validate_study:
            runner.print_validation_report(
                args.validate_study, dates,
                top_n=args.validate_top,
            )
        elif args.study:
            # 랜덤/BO 탐색 결과 조회
            runner.print_random_topN(args.study, top_n=args.top)
        elif is_multi:
            runner.print_multi_scorecard(dates)
        else:
            runner.print_scorecard(first_date)
        return

    # ── 채택 현황 출력 ─────────────────────────────────────────────────
    if args.adoptions:
        study = args.validate_study or args.study
        if not study:
            print("--adoptions 와 함께 --validate-study NAME 또는 --study NAME 을 지정하세요.")
            sys.exit(1)
        runner.print_adoptions(study)
        return

    # ── 날짜 기반 Validation [권장 방식] ─────────────────────────────────
    if args.validate_date:
        if not dates:
            print("--validate-date 와 함께 --dates (validation 날짜)를 지정하세요.")
            sys.exit(1)
        print(f"[night_sim] 날짜기반 Validation | train={args.validate_date} "
              f"valid={dates} top_n={args.validate_top}")
        results = runner.run_validation_by_date(
            train_date   = args.validate_date,
            session_mode = "night",
            valid_dates  = dates,
            top_n        = args.validate_top,
            force        = args.force,
        )
        print(f"[night_sim] 날짜기반 Validation 완료 | {len(results)}건")
        return

    # ── study 기반 Validation / 채택 / 스트레스 [구방식] ─────────────────
    if args.validate_study:
        study_name = args.validate_study

        if args.adopt:
            print(f"[night_sim] Adopt | study={study_name} valid_dates={dates} top_n={args.validate_top} force={args.force}")
            adopted = runner.adopt_best(
                study_name=study_name,
                valid_dates=dates,
                top_n=args.validate_top,
                force=args.force,
            )
            print(f"[night_sim] adopt_best 완료 | adopted={len(adopted)}")
            runner.print_adoptions(study_name)
            return

        if args.stress:
            print(f"[night_sim] Stress | study={study_name} valid_dates={dates} force={args.force}")
            stressed = runner.run_slippage_stress(
                study_name=study_name,
                valid_dates=dates,
                force=args.force,
            )
            print(f"[night_sim] stress 완료 | candidates={len(stressed)}")
            runner.print_adoptions(study_name)
            return

        print(f"[night_sim] Validation | study={study_name} valid_dates={dates} top_n={args.validate_top} force={args.force}")
        runner.run_validation(
            study_name  = study_name,
            valid_dates = dates,
            top_n       = args.validate_top,
            force       = args.force,
        )
        runner.print_validation_report(
            study_name, dates,
            top_n=args.validate_top,
        )
        return

    # ── 군집화 ─────────────────────────────────────────────────────────
    if args.cluster is not None:
        if not args.study:
            print("--cluster K 와 함께 --study NAME 을 지정하세요.")
            sys.exit(1)
        clusters = runner.cluster_top_configs(
            study_name=args.study,
            top_n=args.top,
            n_clusters=args.cluster,
        )
        if not clusters:
            print(f"군집화 결과 없음 | study={args.study}")
        else:
            print(f"\n{'='*100}")
            print(f"  CLUSTERS | study={args.study}  top_n={args.top}  k={args.cluster}")
            print(f"{'='*100}")
            for cl in clusters:
                print(
                    f"  Cluster {cl['cluster_id']:>2} | size={cl['size']:>3} | "
                    f"rep_hash={cl['rep_hash']} | rep_obj={cl['rep_obj']:+.2f}"
                )
            print(f"{'='*100}\n")
        return

    # ── 랜덤 탐색 ─────────────────────────────────────────────────────
    if args.random is not None:
        if args.random < 1:
            # 0 → 기존 study 결과만 출력
            if args.study:
                runner.print_random_topN(args.study, top_n=args.top)
            else:
                print("--study NAME 을 지정하세요.")
                sys.exit(1)
            return

        print(f"[night_sim] 랜덤 탐색 | n={args.random} "
              f"dates={dates} study={args.study or '자동'} "
              f"seed={args.seed} force={args.force}")
        results = runner.run_random_search(
            dates      = dates,
            n_trials   = args.random,
            study_name = args.study,
            seed       = args.seed,
            force      = args.force,
        )
        study_used = (results[0]["study_name"]
                      if results else (args.study or ""))

        runner.print_random_topN(study_used, top_n=args.top)
        return

    # ── Bayesian Optimization ─────────────────────────────────────────
    if args.bayes is not None:
        if args.bayes < 1:
            if args.study:
                runner.print_random_topN(args.study, top_n=args.top)
            else:
                print("--study NAME 을 지정하세요.")
                sys.exit(1)
            return

        print(f"[night_sim] BO 탐색 | n={args.bayes} startup={args.startup} "
              f"dates={dates} study={args.study or '자동'} "
              f"seed={args.seed} force={args.force}")
        results = runner.run_bayes_opt(
            dates            = dates,
            n_trials         = args.bayes,
            study_name       = args.study,
            seed             = args.seed,
            force            = args.force,
            n_startup_trials = args.startup,
        )
        study_used = (results[0]["study_name"]
                      if results else (args.study or ""))
        runner.print_random_topN(study_used, top_n=args.top)
        return

    # ── 실행 분기 (멀티 vs 단일) ──────────────────────────────────────
    if is_multi:
        print(f"[night_sim] 멀티세션 실행 | dates={dates} force={args.force}")
        if args.all_profiles:
            runner.run_all_profiles_multi(dates, force=args.force)
            runner.print_multi_scorecard(dates)
        elif args.profile is not None:
            result = runner.run_profile_multi(dates, args.profile, force=args.force)
            print(f"[night_sim] 완료 | profile={args.profile} "
                  f"dates={len(dates)}개 "
                  f"trades={result.get('trade_count', 0)} "
                  f"status={result.get('status')}")
            runner.print_multi_scorecard(dates)
        else:
            print("--profile N 또는 --all-profiles 를 지정하세요.")
            sys.exit(1)
    else:
        if args.all_profiles:
            print(f"[night_sim] 전체 프로파일 실행 | date={first_date} force={args.force}")
            runner.run_all_profiles(first_date, force=args.force)
            runner.print_scorecard(first_date)
        elif args.profile is not None:
            result = runner.run_profile(first_date, args.profile, force=args.force)
            print(f"[night_sim] 완료 | profile={args.profile} "
                  f"trades={result.get('trade_count', 0)} "
                  f"status={result.get('status')}")
            runner.print_scorecard(first_date)
        else:
            print("--profile N 또는 --all-profiles 를 지정하세요.")
            sys.exit(1)


if __name__ == "__main__":
    main()
