# engines/pulse_engine.py
"""
PulseEngine — 5분 규칙 기반 전략 방향 엔진

역할:
  - LLMPulse를 대체하는 규칙 기반 전략 방향 엔진
  - 5분 인터벌로 A(방향)/B(신뢰) 그룹 지표를 스코어링
  - BULLISH / NEUTRAL / BEARISH + 신뢰도 + 추세 확정 상태 반환
  - mmean.db pulse_signals 테이블에 매 판정 기록
  - LLM은 30분마다 검증 역할만 담당 (PulseValidator)

지표 역할 분류 (설계 확정):
  [A] 방향 지표  — 시장 방향 직접 결정
      A1: 외국인 선물 delta (가중치 ×2, 최대 ±4)
      A2: 외국인 절대+delta 조합 (±2)
      A3: flow_score (±2)
      A4: long_score - short_score 차이 (±1)
      합계 최대: ±9

  [B] 신뢰 보정 지표 — 방향의 지속성·신뢰도 확인
      B1: OI delta × 가격 방향 조합 (±2)
      B2: EMA 기울기 방향 일치 (±1)
      B3: 베이시스 방향 일치 (±1)
      합계 최대: ±4 (candidate 방향 기준 정렬)

방향 판정 임계값 (v1 초기값):
  A-score ≥ +4  → BULLISH 후보
  A-score ≤ -4  → BEARISH 후보
  그 외          → NEUTRAL 후보

신뢰 보정 (B-score, candidate 방향 기준으로 정렬):
  aligned_B ≥ +1  → confirmed
  aligned_B  = 0  → weak
  aligned_B ≤ -1  → NEUTRAL로 격하

추세 확정:
  동일 direction confirmed 3회 연속 → trend_confirmed = True
  반대 direction confirmed 3회 연속 → 추세 전환

데이터 fallback (확정):
  핵심 지표 누락 → confidence -0.3 per item, 방향 확정 금지
  보조 지표 누락 → confidence -0.1 per item
  confidence < CONFIDENCE_MIN_CONFIRM → confirmed 불가
  confidence < CONFIDENCE_MIN_ENTRY   → 진입 전면 금지
"""
from __future__ import annotations

import json
import logging
import os
import sqlite3
import threading
import time
from collections import deque
from dataclasses import asdict, dataclass, field
from datetime import datetime
from typing import Any, Deque, Dict, List, Optional

log = logging.getLogger("MMEAN.PulseEngine")

# ── 상수 (확정값) ──────────────────────────────────────────────────────────
INTERVAL_SEC     = 300    # 5분 인터벌
WARMUP_SEC       = 30     # 시작 후 첫 실행 대기

# 데이터 fallback 상수 (확정 — 최적화 대상 아님)
CONFIDENCE_PENALTY_CORE = 0.3
CONFIDENCE_PENALTY_SUB  = 0.1
DATA_STALE_SEC          = 120
B_WEAK                  = 0      # weak 기준 (고정)

# 인메모리 이력
HISTORY_MAXLEN = 48  # 4시간

# 파라미터 자동 갱신 주기 (초) — pulse_sim.db에서 주기적으로 리로드
PARAM_RELOAD_SEC = 1800  # 30분

# ── 기본 파라미터 (pulse_sim.db 결과로 자동 교체) ──────────────────────────
_DEFAULT_PARAMS: dict = {
    "a_bullish_thresh":       4.0,   # A-score ≥ thresh → BULLISH
    "fgn_delta_strong":    1000.0,   # 외국인 선물 delta 강도 기준 (계약수)
    "flow_upper":             0.5,   # flow_score 강세 상단 임계
    "flow_lower":             0.1,   # flow_score 강세 하단 임계
    "ls_diff_thresh":         0.2,   # long-short 차이 유의미 기준
    "ema_slope_thresh":       0.05,  # EMA 기울기 유의미 기준
    "basis_high":             0.3,   # 베이시스 고/저평가 기준
    "b_confirm_min":          1,     # B-score confirmed 최소값
    "trend_confirm_count":    3,     # 추세 확정 연속 횟수
    "confidence_min_confirm": 0.5,   # confirmed 가능 최소 신뢰도
    "confidence_min_entry":   0.3,   # 진입 허용 최소 신뢰도
}

_CREATE_SQL = """
CREATE TABLE IF NOT EXISTS pulse_signals (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    date            TEXT    NOT NULL,
    ts              TEXT    NOT NULL,
    datetime_full   TEXT    NOT NULL,
    -- 방향 판정
    direction       TEXT    NOT NULL,
    confidence      REAL    NOT NULL,
    status          TEXT    NOT NULL,
    consecutive     INTEGER NOT NULL DEFAULT 0,
    trend_confirmed INTEGER NOT NULL DEFAULT 0,
    -- A-group 점수
    score_fgn_delta   REAL,
    score_fgn_combo   REAL,
    score_flow        REAL,
    score_ls_diff     REAL,
    score_direction   REAL,
    -- B-group 점수
    score_oi          REAL,
    score_ema         REAL,
    score_basis       REAL,
    score_confidence  REAL,
    -- 원본 지표값
    fgn_futures_abs   REAL,
    fgn_futures_delta REAL,
    oi_value          REAL,
    oi_delta          REAL,
    flow_score        REAL,
    basis             REAL,
    ema_fast_slope    REAL,
    futures_price     REAL,
    vwap              REAL,
    -- 채점 결과 (사후)
    price_at_signal   REAL,
    price_5m_later    REAL,
    price_10m_later   REAL,
    price_15m_later   REAL,
    result_5m         TEXT,
    result_10m        TEXT,
    result_15m        TEXT,
    fail_type         TEXT
);
CREATE INDEX IF NOT EXISTS idx_pulse_signals_date ON pulse_signals(date);
CREATE INDEX IF NOT EXISTS idx_pulse_signals_dt   ON pulse_signals(datetime_full);
"""


@dataclass
class PulseSignal:
    """외부에 노출되는 5분 펄스 판정 스냅샷."""
    ts:              str   = ""
    direction:       str   = "NEUTRAL"   # BULLISH|NEUTRAL|BEARISH
    confidence:      float = 0.0
    status:          str   = "weak"      # confirmed|weak|transition
    consecutive:     int   = 0
    trend_confirmed: bool  = False
    trend_direction: str   = "NEUTRAL"   # 현재 확정된 추세 방향
    # 스코어 상세
    score_direction: float = 0.0
    score_confidence:float = 0.0
    score_detail:    Dict  = field(default_factory=dict)
    # 진입 허용 여부
    entry_allowed:   bool  = False
    entry_block_reason: str = ""
    row_id:          Optional[int] = None


class PulseEngine:
    """
    5분 규칙 기반 전략 방향 엔진.

    사용:
        engine = PulseEngine(runtime, interval_sec=300)
        engine.start()
        signal = engine.get_signal()     # 현재 신호
        data   = engine.get_data()       # 이력 + 통계
    """

    def __init__(self, runtime: Any, interval_sec: int = INTERVAL_SEC) -> None:
        self._runtime   = runtime
        self._db_path   = runtime.db_path
        self._interval  = interval_sec
        self._stop      = threading.Event()
        self._thread: Optional[threading.Thread] = None
        self._lock      = threading.Lock()

        # ── 동적 파라미터 (pulse_sim.db 결과로 자동 교체) ──────────────────
        import copy
        self._p: dict = copy.deepcopy(_DEFAULT_PARAMS)
        self._p_lock  = threading.Lock()
        self._last_param_reload: float = 0.0   # 마지막 리로드 시각

        # 현재 신호
        self._current: PulseSignal = PulseSignal()

        # 추세 추적
        self._consecutive   = 0       # 현재 방향 연속 횟수
        self._trend_dir     = "NEUTRAL"
        self._trend_confirmed = False
        self._opposite_cnt  = 0       # 반대 방향 연속 횟수 (전환 감지)

        # 인메모리 이력
        self._history: Deque[Dict] = deque(maxlen=HISTORY_MAXLEN)

        # 외국인 delta 계산용 직전값
        self._prev_foreign: Optional[float] = None

        self._init_db()
        self._load_today()

    def update_params(self, params: dict) -> None:
        """pulse_sim.db 최적화 결과를 실시간 반영."""
        with self._p_lock:
            for k, v in params.items():
                if k in self._p:
                    self._p[k] = v
        log.info(
            "PulseEngine 파라미터 갱신 | a_bull=%.1f fgn_str=%.0f "
            "flow=%.2f/%.2f ls=%.3f ema=%.3f basis=%.2f "
            "b_min=%d trend_cnt=%d conf=%.2f/%.2f",
            self._p["a_bullish_thresh"], self._p["fgn_delta_strong"],
            self._p["flow_upper"], self._p["flow_lower"],
            self._p["ls_diff_thresh"], self._p["ema_slope_thresh"],
            self._p["basis_high"], self._p["b_confirm_min"],
            self._p["trend_confirm_count"],
            self._p["confidence_min_confirm"], self._p["confidence_min_entry"],
        )

    def get_params(self) -> dict:
        """현재 적용 중인 파라미터 반환."""
        with self._p_lock:
            return dict(self._p)

    # ── DB ─────────────────────────────────────────────────────────────────

    def _init_db(self) -> None:
        try:
            conn = sqlite3.connect(self._db_path, timeout=30)
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute("PRAGMA synchronous=NORMAL")
            conn.executescript(_CREATE_SQL)
            conn.close()
            log.info("PulseEngine DB 초기화 완료 | %s", self._db_path)
        except Exception as e:
            log.error("PulseEngine DB 초기화 실패: %s", e)

    def _load_today(self) -> None:
        today = datetime.now().strftime("%Y-%m-%d")
        try:
            conn = sqlite3.connect(self._db_path, timeout=5)
            conn.row_factory = sqlite3.Row
            rows = conn.execute(
                """SELECT id, ts, direction, confidence, status,
                          consecutive, trend_confirmed,
                          score_direction, score_confidence,
                          score_fgn_delta, score_fgn_combo,
                          score_flow, score_ls_diff,
                          score_oi, score_ema, score_basis
                   FROM pulse_signals WHERE date=? ORDER BY id""",
                (today,),
            ).fetchall()
            conn.close()
            with self._lock:
                self._history.clear()
                for r in rows:
                    self._history.append(dict(r))
                # 마지막 신호로 상태 복원
                if rows:
                    last = rows[-1]
                    self._consecutive    = last["consecutive"]
                    self._trend_confirmed = bool(last["trend_confirmed"])
                    self._trend_dir      = last["direction"]
            log.info("PulseEngine 오늘 이력 복원 | %d건", len(rows))
        except Exception as e:
            log.warning("PulseEngine 오늘 이력 복원 실패: %s", e)

    def _db_insert(self, sig: PulseSignal, raw: Dict) -> Optional[int]:
        now = datetime.now()
        try:
            conn = sqlite3.connect(self._db_path, timeout=15)
            d   = sig.score_detail
            cur = conn.execute(
                """INSERT INTO pulse_signals
                   (date, ts, datetime_full,
                    direction, confidence, status, consecutive, trend_confirmed,
                    score_fgn_delta, score_fgn_combo, score_flow, score_ls_diff, score_direction,
                    score_oi, score_ema, score_basis, score_confidence,
                    fgn_futures_abs, fgn_futures_delta,
                    oi_value, oi_delta, flow_score, basis, ema_fast_slope,
                    futures_price, vwap, price_at_signal)
                   VALUES (?,?,?, ?,?,?,?,?, ?,?,?,?,?, ?,?,?,?, ?,?, ?,?,?,?,?, ?,?,?)""",
                (
                    now.strftime("%Y-%m-%d"),
                    sig.ts,
                    now.strftime("%Y-%m-%d %H:%M:%S"),
                    sig.direction, sig.confidence, sig.status,
                    sig.consecutive, 1 if sig.trend_confirmed else 0,
                    d.get("a1"), d.get("a2"), d.get("a3"), d.get("a4"), sig.score_direction,
                    d.get("b1"), d.get("b2"), d.get("b3"), sig.score_confidence,
                    raw.get("fgn_abs"), raw.get("fgn_delta"),
                    raw.get("oi_value"), raw.get("oi_delta"),
                    raw.get("flow_score"), raw.get("basis"), raw.get("ema_fast_slope"),
                    raw.get("futures_price"), raw.get("vwap"),
                    raw.get("futures_price"),  # price_at_signal
                ),
            )
            row_id = cur.lastrowid
            conn.commit()
            conn.close()
            return row_id
        except Exception as e:
            log.warning("PulseEngine DB INSERT 실패: %s", e)
            return None

    # ── 수명 주기 ───────────────────────────────────────────────────────────

    def start(self) -> None:
        if self._thread and self._thread.is_alive():
            return
        self._stop.clear()
        self._thread = threading.Thread(
            target=self._loop, daemon=True, name="MMEAN-PulseEngine",
        )
        self._thread.start()
        log.info("PulseEngine 시작 | interval=%ds", self._interval)

    def stop(self) -> None:
        self._stop.set()
        if self._thread:
            self._thread.join(timeout=5)
        log.info("PulseEngine 종료")

    # ── 메인 루프 ───────────────────────────────────────────────────────────

    def _loop(self) -> None:
        self._stop.wait(WARMUP_SEC)
        if not self._stop.is_set():
            try:
                self._run_once()
            except Exception as e:
                log.warning("PulseEngine 초기 실행 오류: %s", e)

        while not self._stop.is_set():
            self._stop.wait(self._interval)
            if self._stop.is_set():
                break
            try:
                self._run_once()
            except Exception as e:
                log.warning("PulseEngine 루프 오류: %s", e)

    def _auto_reload_params(self) -> None:
        """30분마다 pulse_sim.db에서 파라미터 자동 갱신."""
        now = time.time()
        if now - self._last_param_reload < PARAM_RELOAD_SEC:
            return
        self._last_param_reload = now
        loader = getattr(self._runtime, "pulse_param_loader", None)
        if loader is None:
            return
        try:
            params = loader.load()
            if params:
                self.update_params(params)
        except Exception as e:
            log.warning("PulseEngine 자동 파라미터 갱신 실패: %s", e)

    def _run_once(self) -> None:
        now_hhmm = datetime.now().strftime("%H:%M")
        if now_hhmm < "09:00" or now_hhmm > "15:45":
            return

        # 30분마다 파라미터 자동 갱신
        self._auto_reload_params()

        state = self._runtime.state
        ts    = datetime.now().strftime("%H:%M:%S")

        # ── 원본 지표 수집 ────────────────────────────────────────────────
        futures_price = float(state.get("futures_price", 0.0))
        flow_score    = float(state.get("flow_score",    0.0))
        long_score    = float(state.get("long_score",    0.0))
        short_score   = float(state.get("short_score",   0.0))
        ema_slope     = float(state.get("ema_fast_slope",0.0))
        oi_value      = float(state.get("oi_value",      0.0))
        oi_delta      = float(state.get("oi_delta",      0.0))
        basis         = float(state.get("basis",         0.0))
        vwap          = float(state.get("vwap",          0.0))
        fgn_abs       = float(state.get("foreign_buy",   0.0))

        # 외국인 delta
        fgn_delta = 0.0 if self._prev_foreign is None else fgn_abs - self._prev_foreign
        self._prev_foreign = fgn_abs

        raw = {
            "fgn_abs": fgn_abs, "fgn_delta": fgn_delta,
            "oi_value": oi_value, "oi_delta": oi_delta,
            "flow_score": flow_score, "basis": basis,
            "ema_fast_slope": ema_slope,
            "futures_price": futures_price, "vwap": vwap,
        }

        # ── 데이터 품질 체크 & confidence 초기값 ─────────────────────────
        confidence, missing_core, missing_sub = self._data_quality(
            futures_price, flow_score, fgn_abs, oi_value, ema_slope, basis
        )

        # ── A-group 스코어 ────────────────────────────────────────────────
        a1 = self._score_a1(fgn_delta)           # 외국인 delta ×2
        a2 = self._score_a2(fgn_abs, fgn_delta)  # 외국인 절대+delta 조합
        a3 = self._score_a3(flow_score)           # flow_score
        a4 = self._score_a4(long_score, short_score)  # LS 차이
        a_total = a1 + a2 + a3 + a4

        # 핵심 지표 누락 시 방향 스코어 약화
        if missing_core > 0:
            a_total *= (1.0 - missing_core * 0.3)

        # ── 방향 후보 결정 ────────────────────────────────────────────────
        with self._p_lock:
            _thresh = self._p["a_bullish_thresh"]
            _b_confirm_min     = self._p["b_confirm_min"]
            _trend_confirm_cnt = self._p["trend_confirm_count"]
            _conf_min_confirm  = self._p["confidence_min_confirm"]
            _conf_min_entry    = self._p["confidence_min_entry"]
        if a_total >= _thresh:
            candidate = "BULLISH"
        elif a_total <= -_thresh:
            candidate = "BEARISH"
        else:
            candidate = "NEUTRAL"

        # ── B-group 스코어 (candidate 방향 기준 정렬) ─────────────────────
        sign = +1 if candidate == "BULLISH" else (-1 if candidate == "BEARISH" else 0)

        b1 = self._score_b1(oi_delta, futures_price, vwap) * sign
        b2 = self._score_b2(ema_slope) * sign
        b3 = self._score_b3(basis) * sign
        b_total = b1 + b2 + b3

        # 보조 지표 누락 시 B-score 약화
        if missing_sub > 0:
            b_total = max(-4, b_total - missing_sub * 0.5)

        # ── 상태 결정 ─────────────────────────────────────────────────────
        if candidate == "NEUTRAL":
            direction = "NEUTRAL"
            status    = "weak"
        elif b_total >= _b_confirm_min:
            direction = candidate
            status    = "confirmed"
        elif b_total == B_WEAK:
            direction = candidate
            status    = "weak"
        else:  # b_total <= -1
            direction = "NEUTRAL"
            status    = "transition"

        # confidence 최종 보정
        confidence = max(0.0, min(1.0, confidence))
        if status != "confirmed":
            confidence *= 0.7

        # confirmed 불가 조건
        if confidence < _conf_min_confirm:
            status    = "weak"
            direction = "NEUTRAL" if status == "confirmed" else direction

        # ── 연속 카운터 & 추세 관리 ──────────────────────────────────────
        with self._lock:
            self._update_trend(direction, status)
            consecutive     = self._consecutive
            trend_confirmed = self._trend_confirmed
            trend_dir       = self._trend_dir

        # 진입 허용 여부
        entry_allowed, block_reason = self._check_entry(
            direction, status, trend_confirmed, confidence
        )

        # ── 신호 조립 ─────────────────────────────────────────────────────
        sig = PulseSignal(
            ts              = ts,
            direction       = direction,
            confidence      = round(confidence, 2),
            status          = status,
            consecutive     = consecutive,
            trend_confirmed = trend_confirmed,
            trend_direction = trend_dir,
            score_direction = round(a_total, 2),
            score_confidence= round(b_total, 2),
            score_detail    = {
                "a1": round(a1, 2), "a2": round(a2, 2),
                "a3": round(a3, 2), "a4": round(a4, 2),
                "b1": round(b1, 2), "b2": round(b2, 2),
                "b3": round(b3, 2),
            },
            entry_allowed   = entry_allowed,
            entry_block_reason = block_reason,
        )

        with self._lock:
            self._current = sig

        # ── DB & 이력 기록 (비동기) ───────────────────────────────────────
        # 이력 기록 (DB insert 성공 여부와 무관하게 먼저 추가)
        with self._lock:
            self._history.append(asdict(sig))

        # DB INSERT (동기)
        row_id = self._db_insert(sig, raw)
        if row_id:
            sig.row_id = row_id
            with self._lock:
                for s in self._history:
                    if s.get("ts") == sig.ts and s.get("direction") == sig.direction:
                        s["row_id"] = row_id
                        break

        log.info(
            "PulseEngine | %s dir=%-8s status=%-12s conf=%.2f "
            "A=%.1f B=%.1f trend=%s(%d/%d)",
            ts, direction, status, confidence,
            a_total, b_total, trend_dir, consecutive, _trend_confirm_cnt,
        )

    # ── A-group 스코어 계산 ────────────────────────────────────────────────

    def _score_a1(self, fgn_delta: float) -> float:
        """외국인 선물 delta (가중치 ×2, 최대 ±4)."""
        thr = self._p["fgn_delta_strong"]
        if fgn_delta > thr:    return +4.0
        elif fgn_delta > 0:    return +2.0
        elif fgn_delta == 0:   return  0.0
        elif fgn_delta >= -thr:return -2.0
        else:                  return -4.0

    def _score_a2(self, fgn_abs: float, fgn_delta: float) -> float:
        """외국인 절대+delta 조합 (최대 ±2)."""
        if fgn_abs > 0 and fgn_delta > 0:
            return +2.0   # 누적 매수 + 증가
        elif fgn_abs > 0 and fgn_delta <= 0:
            return +1.0   # 누적 매수 + 감소/보합
        elif fgn_abs < 0 and fgn_delta >= 0:
            return -1.0   # 누적 매도 + 감소/보합
        elif fgn_abs < 0 and fgn_delta < 0:
            return -2.0   # 누적 매도 + 증가
        return 0.0

    def _score_a3(self, flow_score: float) -> float:
        """flow_score (최대 ±2)."""
        u, l = self._p["flow_upper"], self._p["flow_lower"]
        if flow_score > u:    return +2.0
        elif flow_score > l:  return +1.0
        elif flow_score >= -l:return  0.0
        elif flow_score >= -u:return -1.0
        else:                 return -2.0

    def _score_a4(self, long_score: float, short_score: float) -> float:
        """long-short 차이 (최대 ±1)."""
        thr = self._p["ls_diff_thresh"]
        diff = long_score - short_score
        if diff >= thr:  return +1.0
        elif diff <= -thr: return -1.0
        return 0.0

    # ── B-group 스코어 계산 (방향 정렬 전 원본 부호) ──────────────────────

    def _score_b1(self, oi_delta: float, price: float, vwap: float) -> float:
        """OI delta × 가격 방향 조합 (최대 ±2)."""
        if oi_delta == 0:
            return 0.0
        price_up = price >= vwap
        if oi_delta > 0 and price_up:
            return +2.0   # 신규 롱 진입 → BULLISH 확인
        elif oi_delta > 0 and not price_up:
            return -2.0   # 신규 숏 진입 → BEARISH 확인
        elif oi_delta < 0 and price_up:
            return +1.0   # 숏 청산 (반등)
        else:
            return -1.0   # 롱 청산

    def _score_b2(self, ema_slope: float) -> float:
        """EMA 기울기 (최대 ±1)."""
        thr = self._p["ema_slope_thresh"]
        if ema_slope >= thr:         return +1.0
        elif ema_slope >= thr * 0.5: return +0.5
        elif ema_slope <= -thr:      return -1.0
        elif ema_slope <= -thr * 0.5:return -0.5
        return 0.0

    def _score_b3(self, basis: float) -> float:
        """베이시스 상태 (최대 ±1)."""
        bh = self._p["basis_high"]
        if basis > bh:   return -1.0   # 고평가 → 매도 압력 (BEARISH)
        elif basis < -bh:return +1.0   # 저평가 → 매수 압력 (BULLISH)
        return 0.0

    # ── 데이터 품질 체크 ───────────────────────────────────────────────────

    def _data_quality(
        self,
        futures_price: float,
        flow_score: float,
        fgn_abs: float,
        oi_value: float,
        ema_slope: float,
        basis: float,
    ):
        """
        returns (confidence_base, missing_core_count, missing_sub_count)
        핵심 지표: futures_price, flow_score, fgn_abs
        보조 지표: oi_value, ema_slope, basis
        """
        confidence = 1.0
        missing_core = 0
        missing_sub  = 0

        if futures_price <= 0:
            confidence   -= CONFIDENCE_PENALTY_CORE
            missing_core += 1
        if flow_score == 0.0:
            confidence   -= CONFIDENCE_PENALTY_CORE
            missing_core += 1
        if fgn_abs == 0.0:
            confidence   -= CONFIDENCE_PENALTY_CORE
            missing_core += 1

        if oi_value == 0.0:
            confidence  -= CONFIDENCE_PENALTY_SUB
            missing_sub += 1
        if ema_slope == 0.0:
            confidence  -= CONFIDENCE_PENALTY_SUB
            missing_sub += 1
        if basis == 0.0:
            confidence  -= CONFIDENCE_PENALTY_SUB
            missing_sub += 1

        return max(0.0, confidence), missing_core, missing_sub

    # ── 추세 관리 ─────────────────────────────────────────────────────────

    def _update_trend(self, direction: str, status: str) -> None:
        """연속 카운터 및 추세 방향 갱신 (lock 내부에서 호출)."""
        if status != "confirmed":
            # confirmed 아니면 카운터 갱신 안 함
            return

        with self._p_lock:
            _tcc = self._p["trend_confirm_count"]

        if direction == self._trend_dir or not self._trend_confirmed:
            if direction == self._trend_dir:
                self._consecutive  += 1
                self._opposite_cnt  = 0
            else:
                # 반대 방향 시작
                if self._trend_confirmed:
                    self._opposite_cnt += 1
                    if self._opposite_cnt >= _tcc:
                        # 추세 전환
                        log.info(
                            "PulseEngine ↺ 추세 전환 | %s → %s",
                            self._trend_dir, direction,
                        )
                        self._trend_dir       = direction
                        self._trend_confirmed = True
                        with self._p_lock:
                            self._consecutive = self._p["trend_confirm_count"]
                        self._opposite_cnt    = 0
                else:
                    # 아직 추세 미확정 — 방향 바뀌면 리셋
                    self._trend_dir    = direction
                    self._consecutive  = 1
                    self._opposite_cnt = 0
        else:
            self._trend_dir    = direction
            self._consecutive  = 1
            self._opposite_cnt = 0

        if self._consecutive >= _tcc and not self._trend_confirmed:
            self._trend_confirmed = True
            log.info(
                "PulseEngine ★ 추세 확정 | direction=%s | %d회 연속",
                self._trend_dir, self._consecutive,
            )

    # ── 진입 허용 체크 ────────────────────────────────────────────────────

    def _check_entry(
        self,
        direction: str,
        status: str,
        trend_confirmed: bool,
        confidence: float,
    ):
        if not trend_confirmed:
            return False, "추세 미확정"
        if status != "confirmed":
            return False, f"신호 상태={status}"
        with self._p_lock:
            _min_entry = self._p["confidence_min_entry"]
        if confidence < _min_entry:
            return False, f"신뢰도 부족({confidence:.2f})"
        return True, ""

    # ── 퍼블릭 API ────────────────────────────────────────────────────────

    def get_signal(self) -> PulseSignal:
        """현재 최신 신호 반환."""
        with self._lock:
            return self._current

    def get_data(self) -> Dict:
        """이력 + 통계 반환 (/api/pulse)."""
        with self._lock:
            history = [
                {k: v for k, v in s.items() if k != "row_id"}
                for s in self._history
            ]
        stats = self._compute_stats(history)
        sig   = self.get_signal()
        return {
            "active":           bool(self._thread and self._thread.is_alive()),
            "interval_sec":     self._interval,
            "current":          asdict(sig),
            "history":          history[-10:],  # 최근 10개
            "history_full":     history,
            "stats":            stats,
        }

    def _compute_stats(self, history: List[Dict]) -> Dict:
        if not history:
            return {
                "total": 0, "confirmed_count": 0,
                "trend_direction": self._trend_dir,
                "trend_confirmed": self._trend_confirmed,
                "consecutive": self._consecutive,
                "bullish_pct": 0, "neutral_pct": 0, "bearish_pct": 0,
            }
        total     = len(history)
        confirmed = sum(1 for s in history if s.get("status") == "confirmed")
        bullish   = sum(1 for s in history if s.get("direction") == "BULLISH")
        neutral   = sum(1 for s in history if s.get("direction") == "NEUTRAL")
        bearish   = sum(1 for s in history if s.get("direction") == "BEARISH")
        return {
            "total":           total,
            "confirmed_count": confirmed,
            "trend_direction": self._trend_dir,
            "trend_confirmed": self._trend_confirmed,
            "consecutive":     self._consecutive,
            "bullish_pct":     round(bullish / total * 100),
            "neutral_pct":     round(neutral / total * 100),
            "bearish_pct":     round(bearish / total * 100),
        }
