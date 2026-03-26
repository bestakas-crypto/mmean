# engines/market_analyst.py
"""
MarketAnalyst — 이벤트 기반 장중 시장 상태 분석기 (추세 추적 상태머신 포함)

■ 동작 방식
  1. IDLE      : 트리거 조건 감시
  2. TRACKING  : 트리거 후 추세 건강도 30초마다 체크
  3. WEAKENING : 건강도 < 0.5 → 1회 경고 후 계속 모니터링
  4. → 건강도 < 0.3 or 반대 트리거 → LLM 재호출 (추세 소멸/반전 판단)

■ 트리거 조건 (OR)
  1. EMA 기울기 부호 전환  (양→음, 음→양) + |slope| > EMA_SLOPE_FLIP
  2. EMA 기울기 급가속     |slope| > EMA_SLOPE_ACCEL
  3. 거래량 폭발           volume_burst > VOL_BURST_THRESH
  4. flow_score 부호 전환  |delta| > FLOW_SIGN_THRESH
  5. 신고가 / 신저가 갱신

■ Trend Health Score (추세 건강도)
  EMA slope    0.40 — 트리거 방향 slope 유지 비율
  flow_score   0.30 — 트리거 방향 flow 유지 비율
  가격 되돌림  0.20 — 트리거 시점 대비 역방향 이동
  volume_burst 0.10 — 거래량 유지 여부
  합산 > 0.6 : 추세 유지
  0.3~0.6    : WEAKENING (경고)
  < 0.3      : 추세 소멸 → LLM 재호출

■ state 키
  market_view         : dict | None  {view, direction, confidence, reason}
  market_view_ts      : str          마지막 갱신 시각 HH:MM:SS
  market_view_history : list         최근 5개 이력
  market_view_trigger : str          마지막 트리거 사유
  market_view_track   : dict | None  {status, health, elapsed_sec}

■ view 분류값
  TREND_UP / TREND_DOWN / VOLATILE / CHOPPY
  FLAT_HIGH / FLAT_LOW
  SPIKE_REVERSAL / REVERSAL_UP / REVERSAL_DOWN
"""
from __future__ import annotations

import json
import logging
import os
import sqlite3
import threading
import time
from collections import deque
from datetime import datetime, timedelta
from typing import Any, Deque, Dict, List, Optional

import requests

log = logging.getLogger("MMEAN.MarketAnalyst")

# ── 분류값 ──────────────────────────────────────────────────────────────────
VALID_VIEWS = {
    "TREND_UP", "TREND_DOWN", "VOLATILE", "CHOPPY",
    "FLAT_HIGH", "FLAT_LOW",
    "SPIKE_REVERSAL", "REVERSAL_UP", "REVERSAL_DOWN",
}
VALID_DIRECTIONS = {"LONG_BIAS", "SHORT_BIAS", "NEUTRAL"}
VALID_CONFIDENCE = {"HIGH", "MID", "LOW"}

# ── 모델 ────────────────────────────────────────────────────────────────────
_HAIKU_MODEL   = "claude-haiku-4-5-20251001"
_HAIKU_URL     = "https://api.anthropic.com/v1/messages"
_HAIKU_TIMEOUT = 10
_MAX_TOKENS    = 300
_HISTORY_LEN   = 5

# ── 트리거 임계치 ────────────────────────────────────────────────────────────
COOLDOWN_SEC        = int(os.getenv("MMEAN_MA_COOLDOWN", "120"))  # LLM 호출 최소 간격 (초)
TRACK_CHECK_SEC     = int(os.getenv("MMEAN_MA_TRACK_CHECK", "30"))  # 건강도 체크 간격 (초)
EMA_SLOPE_FLIP      = 0.08   # EMA 기울기 부호 전환 최소 크기
EMA_SLOPE_ACCEL     = 0.45   # EMA 기울기 급가속 임계치
VOL_BURST_THRESH    = 2.2    # 거래량 폭발 임계치
FLOW_SIGN_THRESH    = 0.06   # flow_score 부호 전환 최소 크기

# ── 추세 추적 설정 ────────────────────────────────────────────────────────────
HEALTH_WEAK         = 0.5    # 이 미만 → WEAKENING
HEALTH_DEAD         = 0.3    # 이 미만 → 추세 소멸 → LLM 재호출
PRICE_RETRACE_PT    = 1.5    # 이 이상 역방향 이동 시 되돌림 신호 (pt)
TRACK_MAX_SEC       = 1800   # 최대 추적 시간 30분 (이후 자동 해제)

# ── 상태 머신 ────────────────────────────────────────────────────────────────
_ST_IDLE      = "IDLE"
_ST_TRACKING  = "TRACKING"
_ST_WEAKENING = "WEAKENING"

# ── 시스템 프롬프트 ──────────────────────────────────────────────────────────
_SYSTEM_PROMPT = """\
KOSPI200 선물 장중 시장 분석가.
엔진이 감지한 이벤트(신규 트리거 또는 추세 소멸/반전)를 받아 현재 시장 상태를 분류한다.

반드시 아래 JSON 형식으로만 응답 (다른 텍스트 없이):
{"view": "...", "direction": "...", "confidence": "...", "reason": "..."}

view 선택지: TREND_UP, TREND_DOWN, VOLATILE, CHOPPY, FLAT_HIGH, FLAT_LOW,
             SPIKE_REVERSAL, REVERSAL_UP, REVERSAL_DOWN
direction: LONG_BIAS, SHORT_BIAS, NEUTRAL
confidence: HIGH, MID, LOW
reason: 한 줄 핵심 근거 (한국어, 이벤트 원인 포함)"""


class MarketAnalyst:
    """
    이벤트 기반 + 추세 추적 장중 시장 상태 분석기.

    사용:
        analyst = MarketAnalyst(runtime)
        analyst.start()
        # 매 틱마다:
        analyst.check_trigger(state)
    """

    def __init__(self, runtime: Any, interval_sec: int = 180) -> None:
        # interval_sec: 하위 호환성 유지용 (미사용)
        self._runtime    = runtime
        self._stop_event = threading.Event()
        self._api_key    = os.getenv("ANTHROPIC_API_KEY", "").strip()
        self._call_lock  = threading.Lock()

        import os as _os
        self._intraday_db = _os.path.join(
            _os.path.dirname(runtime.db_path), "intraday.db"
        )
        self._init_intraday_db()

        self._view_history: Deque[Dict] = deque(maxlen=_HISTORY_LEN)

        # ── 트리거 감지용 이전값 ────────────────────────────────────────────
        self._prev_ema_slope:     float = 0.0
        self._prev_flow_score:    float = 0.0
        self._prev_vol_burst:     float = 1.0
        self._prev_session_high:  float = 0.0
        self._prev_session_low:   float = 0.0   # 0이면 조건 불성립 → 초기 오트리거 방지
        self._last_trigger_ts:    float = 0.0
        self._start_ts:           float = time.time()   # 시작 시각 → warmup 60초 억제

        # ── 추세 추적 상태머신 ──────────────────────────────────────────────
        self._track_status:      str            = _ST_IDLE
        self._track_state:       Optional[Dict] = None   # 기준점 스냅샷
        self._track_last_check:  float          = 0.0

        if not self._api_key:
            log.warning("MarketAnalyst: ANTHROPIC_API_KEY 없음 — 비활성")

    # ── 수명 주기 ─────────────────────────────────────────────────────────────

    def _init_intraday_db(self) -> None:
        try:
            conn = sqlite3.connect(self._intraday_db, timeout=5)
            conn.execute("""
                CREATE TABLE IF NOT EXISTS market_view_log (
                    id           INTEGER PRIMARY KEY AUTOINCREMENT,
                    session_date TEXT NOT NULL,
                    ts           TEXT NOT NULL,
                    view         TEXT NOT NULL,
                    direction    TEXT NOT NULL,
                    confidence   TEXT NOT NULL,
                    reason       TEXT,
                    trigger      TEXT,
                    ema_slope    REAL,
                    flow_score   REAL,
                    futures_price REAL
                )
            """)
            conn.execute("CREATE INDEX IF NOT EXISTS idx_mvl_date ON market_view_log(session_date)")
            conn.commit()
            conn.close()
        except Exception as e:
            log.warning("intraday.db 초기화 실패: %s", e)

    def _save_market_view(self, result: Dict, trigger_reason: str, state_snap: Dict) -> None:
        try:
            conn = sqlite3.connect(self._intraday_db, timeout=3)
            conn.execute(
                "INSERT INTO market_view_log "
                "(session_date, ts, view, direction, confidence, reason, trigger, ema_slope, flow_score, futures_price) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    datetime.now().strftime("%Y-%m-%d"),
                    datetime.now().strftime("%H:%M:%S"),
                    result.get("view", ""),
                    result.get("direction", ""),
                    result.get("confidence", ""),
                    result.get("reason", ""),
                    trigger_reason,
                    float(state_snap.get("ema_slope", 0.0)),
                    float(state_snap.get("flow_score", 0.0)),
                    float(state_snap.get("futures_price", 0.0)),
                )
            )
            conn.commit()
            conn.close()
        except Exception as e:
            log.warning("market_view_log 저장 실패: %s", e)

    def start(self) -> None:
        log.info("MarketAnalyst 시작 | 모드=이벤트+추세추적 | model=%s | cooldown=%ds",
                 _HAIKU_MODEL, COOLDOWN_SEC)

    def stop(self) -> None:
        self._stop_event.set()
        self._track_status = _ST_IDLE
        self._track_state  = None
        log.info("MarketAnalyst 종료")

    # ── 매 틱 호출 진입점 ─────────────────────────────────────────────────────

    def check_trigger(self, state: Dict) -> None:
        """
        엔진이 매 틱 호출. 즉시 반환 (틱 루프 차단 없음).
          1) 신규 트리거 조건 검사
          2) TRACKING/WEAKENING 중이면 건강도 체크 (30초마다)
        """
        if not self._api_key:
            return

        now_hhmm = datetime.now().strftime("%H:%M")
        if now_hhmm < "09:00" or now_hhmm > "15:45":
            return

        now = time.time()

        # ── ① 추세 추적 건강도 체크 (우선 처리) ──────────────────────────────
        if self._track_status in (_ST_TRACKING, _ST_WEAKENING):
            if now - self._track_last_check >= TRACK_CHECK_SEC:
                self._track_last_check = now
                self._do_health_check(state, now)

        # ── ② 신규 트리거 감지 ────────────────────────────────────────────────
        # 시작 후 300초: EMA·VWAP·volume 히스토리 미정착으로 오트리거 억제
        # (VOLUME_WINDOW=20 × 10초 폴링 = 200초 필요 → 300초로 여유 확보)
        if now - self._start_ts < 300.0:
            self._update_prev(state)
            return

        if now - self._last_trigger_ts < COOLDOWN_SEC:
            self._update_prev(state)
            return

        trigger_reason = self._check_new_trigger(state)
        if not trigger_reason:
            return

        self._fire_llm(state, trigger_reason, now)

    # ── 추세 건강도 체크 ──────────────────────────────────────────────────────

    def _do_health_check(self, state: Dict, now: float) -> None:
        """30초마다 호출. 건강도 계산 후 상태 전이."""
        if self._track_state is None:
            self._track_status = _ST_IDLE
            return

        ts   = self._track_state
        elapsed = now - ts["ts"]

        # 최대 추적 시간 초과 → 해제
        if elapsed > TRACK_MAX_SEC:
            log.info("MarketAnalyst TRACKING 만료 (%.0f초) → IDLE", elapsed)
            self._reset_tracking()
            return

        health, detail = self._calc_health(state, ts)

        # 상태 업데이트 (대시보드용)
        try:
            with self._runtime.state_obj.engine_lock:
                self._runtime.state["market_view_track"] = {
                    "status":      self._track_status,
                    "health":      round(health, 2),
                    "elapsed_sec": int(elapsed),
                    "detail":      detail,
                }
        except Exception:
            pass

        log.debug("MarketAnalyst HEALTH | status=%s health=%.2f elapsed=%.0fs | %s",
                  self._track_status, health, elapsed, detail)

        if self._track_status == _ST_TRACKING and health < HEALTH_WEAK:
            self._track_status = _ST_WEAKENING
            log.info("MarketAnalyst → WEAKENING | health=%.2f | %s", health, detail)

        elif self._track_status == _ST_WEAKENING and health < HEALTH_DEAD:
            # 추세 소멸 → LLM 재호출
            reason = f"추세소멸 health={health:.2f} | {detail}"
            log.info("MarketAnalyst 추세소멸 → LLM 재호출 | %s", reason)
            if not self._call_lock.locked() and self._call_lock.acquire(blocking=False):
                self._last_trigger_ts = now
                self._reset_tracking()
                snap = dict(state)
                threading.Thread(
                    target=self._run_triggered,
                    args=(snap, reason),
                    daemon=True,
                    name="MMEAN-MA-dead",
                ).start()

        elif self._track_status == _ST_WEAKENING and health >= HEALTH_WEAK:
            # 건강도 회복
            self._track_status = _ST_TRACKING
            log.info("MarketAnalyst WEAKENING → TRACKING 회복 | health=%.2f", health)

    def _calc_health(self, state: Dict, ts: Dict) -> tuple[float, str]:
        """
        현재 state와 트리거 기준점(ts)을 비교해 추세 건강도(0~1) 반환.
        (score, detail_str)
        """
        curr_slope = float(state.get("ema_fast_slope", 0.0))
        curr_flow  = float(state.get("flow_score", 0.0))
        curr_vol   = float(state.get("volume_burst", 1.0))
        curr_price = float(state.get("futures_price", 0.0))

        ref_slope  = ts["slope"]
        ref_flow   = ts["flow"]
        ref_price  = ts["price"]

        parts = []

        # ① EMA slope 건강도 (가중 0.40)
        if abs(ref_slope) < 0.01:
            s_slope = 0.5
        elif ref_slope * curr_slope < 0:
            # 부호 반전 → 완전 역전
            s_slope = 0.0
        else:
            ratio = abs(curr_slope) / (abs(ref_slope) + 1e-6)
            s_slope = min(ratio, 1.0)
        parts.append(f"slope={s_slope:.2f}({curr_slope:+.3f})")

        # ② flow_score 건강도 (가중 0.30)
        if abs(ref_flow) < 0.01:
            s_flow = 0.5
        elif ref_flow * curr_flow < 0:
            s_flow = 0.0
        else:
            ratio = abs(curr_flow) / (abs(ref_flow) + 1e-6)
            s_flow = min(ratio, 1.0)
        parts.append(f"flow={s_flow:.2f}({curr_flow:+.3f})")

        # ③ 가격 되돌림 건강도 (가중 0.20)
        if ref_price <= 0 or curr_price <= 0:
            s_price = 0.5
        else:
            price_move = curr_price - ref_price
            # slope 방향 기준: 양수 slope → 상승 기대, 역방향이면 페널티
            expected_dir = 1.0 if ref_slope >= 0 else -1.0
            retrace = -price_move * expected_dir   # 역방향이면 양수
            if retrace >= PRICE_RETRACE_PT * 2:
                s_price = 0.0
            elif retrace >= PRICE_RETRACE_PT:
                s_price = 0.3
            elif retrace > 0:
                s_price = 0.7
            else:
                s_price = 1.0
        parts.append(f"price={s_price:.2f}({curr_price:.1f})")

        # ④ volume_burst 건강도 (가중 0.10)
        # 거래량이 정상(1.0)으로 돌아오면 추세 동력 약화
        ref_vol = ts.get("vol", 1.0)
        if ref_vol >= VOL_BURST_THRESH:
            # 폭발로 시작된 경우 정상화 여부 측정
            s_vol = min(curr_vol / ref_vol, 1.0)
        else:
            s_vol = 0.8   # 폭발이 아닌 트리거는 거래량 기여도 낮게 고정
        parts.append(f"vol={s_vol:.2f}")

        health = (s_slope * 0.40 + s_flow * 0.30
                  + s_price * 0.20 + s_vol * 0.10)
        detail = " | ".join(parts)
        return round(health, 3), detail

    # ── 신규 트리거 감지 ─────────────────────────────────────────────────────

    def _check_new_trigger(self, state: Dict) -> str:
        """트리거 조건 검사. 사유 반환 (없으면 '')."""
        ema_slope  = float(state.get("ema_fast_slope", 0.0))
        flow_score = float(state.get("flow_score", 0.0))
        vol_burst  = float(state.get("volume_burst", 1.0))
        sess_high  = float(state.get("session_high", 0.0))
        sess_low   = float(state.get("session_low", 999999.0))

        reason = ""

        if (self._prev_ema_slope * ema_slope < 0
                and abs(ema_slope) > EMA_SLOPE_FLIP
                and abs(self._prev_ema_slope) > EMA_SLOPE_FLIP):
            d = "양→음" if ema_slope < 0 else "음→양"
            reason = f"EMA기울기 부호전환({d}) {self._prev_ema_slope:.3f}→{ema_slope:.3f}"

        elif abs(ema_slope) > EMA_SLOPE_ACCEL and abs(self._prev_ema_slope) <= EMA_SLOPE_ACCEL:
            reason = f"EMA기울기 급가속 slope={ema_slope:.3f}"

        elif vol_burst > VOL_BURST_THRESH and self._prev_vol_burst <= VOL_BURST_THRESH:
            reason = f"거래량폭발 vol_burst={vol_burst:.2f}"

        elif (self._prev_flow_score * flow_score < 0
              and abs(flow_score - self._prev_flow_score) > FLOW_SIGN_THRESH):
            d = "매수→매도" if flow_score < 0 else "매도→매수"
            reason = f"수급반전({d}) flow={self._prev_flow_score:.3f}→{flow_score:.3f}"

        elif sess_high > self._prev_session_high > 0:
            reason = f"신고가 {self._prev_session_high:.2f}→{sess_high:.2f}"

        elif 0 < sess_low < self._prev_session_low:
            reason = f"신저가 {self._prev_session_low:.2f}→{sess_low:.2f}"

        self._update_prev(state)
        return reason

    def _update_prev(self, state: Dict) -> None:
        self._prev_ema_slope    = float(state.get("ema_fast_slope", 0.0))
        self._prev_flow_score   = float(state.get("flow_score", 0.0))
        self._prev_vol_burst    = float(state.get("volume_burst", 1.0))
        self._prev_session_high = float(state.get("session_high", 0.0))
        _sl = float(state.get("session_low", 0.0))
        if 0 < _sl < 999990.0:   # 999999 초기값 제외 → 오트리거 방지
            self._prev_session_low = _sl

    # ── LLM 호출 발사 ────────────────────────────────────────────────────────

    def _fire_llm(self, state: Dict, trigger_reason: str, now: float) -> None:
        if not self._call_lock.acquire(blocking=False):
            return
        self._last_trigger_ts = now
        snap = dict(state)

        # 트리거 기준점 기록 → TRACKING 진입
        self._track_state = {
            "ts":    now,
            "slope": float(state.get("ema_fast_slope", 0.0)),
            "flow":  float(state.get("flow_score", 0.0)),
            "vol":   float(state.get("volume_burst", 1.0)),
            "price": float(state.get("futures_price", 0.0)),
            "trigger": trigger_reason,
        }
        self._track_status     = _ST_TRACKING
        self._track_last_check = now

        log.info("MarketAnalyst 트리거 → TRACKING | %s", trigger_reason)

        threading.Thread(
            target=self._run_triggered,
            args=(snap, trigger_reason),
            daemon=True,
            name="MMEAN-MA-call",
        ).start()

    def _reset_tracking(self) -> None:
        self._track_status = _ST_IDLE
        self._track_state  = None
        try:
            with self._runtime.state_obj.engine_lock:
                self._runtime.state["market_view_track"] = None
        except Exception:
            pass

    # ── LLM 실행 (별도 스레드) ───────────────────────────────────────────────

    def _run_triggered(self, state_snap: Dict, trigger_reason: str) -> None:
        try:
            summary   = self._summarize_from_state(state_snap, trigger_reason)
            prev_view = self._view_history[-1] if self._view_history else None
            result    = self._call_haiku(summary, prev_view, trigger_reason)
            if result is None:
                return

            ts = datetime.now().strftime("%H:%M:%S")
            entry = {**result, "ts": ts, "trigger": trigger_reason}
            self._view_history.append(entry)

            with self._runtime.state_obj.engine_lock:
                self._runtime.state["market_view"]         = result
                self._runtime.state["market_view_ts"]      = ts
                self._runtime.state["market_view_history"] = list(self._view_history)
                self._runtime.state["market_view_trigger"] = trigger_reason

            self._save_market_view(result, trigger_reason, state_snap)
            log.info(
                "MarketAnalyst | %s view=%-15s dir=%-11s conf=%s | trigger=%s | %s",
                ts, result["view"], result["direction"],
                result["confidence"], trigger_reason[:40], result["reason"],
            )
        finally:
            self._call_lock.release()

    # ── 요약 생성 ────────────────────────────────────────────────────────────

    def _summarize_from_state(self, state: Dict, trigger_reason: str) -> Dict:
        fp         = float(state.get("futures_price", 0.0))
        vwap       = float(state.get("vwap", fp) or fp)
        sess_high  = float(state.get("session_high", fp) or fp)
        sess_low   = float(state.get("session_low", fp) or fp)
        ema_slope  = float(state.get("ema_fast_slope", 0.0))
        flow_score = float(state.get("flow_score", 0.0))
        vol_burst  = float(state.get("volume_burst", 1.0))
        long_score = float(state.get("long_score", 0.0))
        short_score= float(state.get("short_score", 0.0))

        sess_range   = max(sess_high - sess_low, 0.01)
        pos_in_range = round((fp - sess_low) / sess_range * 100) if sess_range > 0 else 50
        high_dist    = round(sess_high - fp, 2)
        low_dist     = round(fp - sess_low, 2)

        # 추적 중이었다면 기준점 대비 변화량 포함
        track_info = ""
        if self._track_state:
            ts = self._track_state
            elapsed = int(time.time() - ts["ts"])
            slope_chg  = round(ema_slope  - ts["slope"], 3)
            flow_chg   = round(flow_score - ts["flow"],  3)
            price_chg  = round(fp         - ts["price"], 2)
            track_info = (
                f"\n[추세 추적 결과 ({elapsed}초 경과)]"
                f"\n  slope변화: {ts['slope']:+.3f}→{ema_slope:+.3f} (Δ{slope_chg:+.3f})"
                f"\n  flow변화:  {ts['flow']:+.3f}→{flow_score:+.3f} (Δ{flow_chg:+.3f})"
                f"\n  가격변화:  {ts['price']:.2f}→{fp:.2f} ({price_chg:+.2f}pt)"
                f"\n  트리거: {ts['trigger']}"
            )

        db_summary = self._fetch_recent_summary(minutes=3)

        return {
            "trigger_reason": trigger_reason,
            "ema_slope":      round(ema_slope, 3),
            "flow_score":     round(flow_score, 3),
            "vol_burst":      round(vol_burst, 2),
            "long_score":     round(long_score, 2),
            "short_score":    round(short_score, 2),
            "price_vs_vwap":  round(fp - vwap, 2),
            "high_dist_pt":   high_dist,
            "low_dist_pt":    low_dist,
            "pos_in_range":   pos_in_range,
            "sess_range_pt":  round(sess_range, 2),
            "track_info":     track_info,
            **db_summary,
        }

    def _fetch_recent_summary(self, minutes: int = 3) -> Dict:
        cutoff = (datetime.now() - timedelta(minutes=minutes)).strftime("%Y-%m-%d %H:%M:%S")
        today  = datetime.now().strftime("%Y-%m-%d")
        try:
            conn = sqlite3.connect(
                f"file:{self._runtime.db_path}?mode=ro",
                uri=True, timeout=15,
            )
            conn.row_factory = sqlite3.Row
            rows = conn.execute("""
                SELECT bias, flow_score, volume_burst, futures_price
                FROM regime_ticks
                WHERE ts >= ? AND substr(ts,1,10) = ? AND futures_price > 100
                ORDER BY ts
            """, (cutoff, today)).fetchall()
            conn.close()

            if len(rows) < 3:
                return {"tick_count": len(rows), "price_change": 0.0,
                        "long_ratio": 0.5, "flow_trend": "데이터부족"}

            prices = [float(r["futures_price"]) for r in rows]
            biases = [r["bias"] for r in rows if r["bias"]]
            flows  = [float(r["flow_score"]) for r in rows if r["flow_score"] is not None]

            long_ratio = biases.count("LONG") / len(biases) if biases else 0.5
            price_chg  = round(prices[-1] - prices[0], 2)
            flow_trend = "데이터부족"
            if len(flows) >= 4:
                mid = len(flows) // 2
                flow_trend = "상승" if (
                    sum(flows[mid:]) / len(flows[mid:]) > sum(flows[:mid]) / len(flows[:mid])
                ) else "하락"

            return {
                "tick_count":   len(rows),
                "price_change": price_chg,
                "long_ratio":   round(long_ratio, 2),
                "flow_trend":   flow_trend,
            }
        except Exception as e:
            log.debug("MarketAnalyst DB 조회 실패: %s", e)
            return {"tick_count": 0, "price_change": 0.0,
                    "long_ratio": 0.5, "flow_trend": "조회실패"}

    # ── Haiku 호출 ───────────────────────────────────────────────────────────

    def _call_haiku(
        self,
        summary: Dict,
        prev_view: Optional[Dict],
        trigger_reason: str,
    ) -> Optional[Dict]:

        prev_str = ""
        if prev_view:
            prev_str = (
                f"\n[직전 분석({prev_view.get('ts','')})] "
                f"view={prev_view.get('view')} "
                f"direction={prev_view.get('direction')} "
                f"reason={prev_view.get('reason','')}"
            )

        user_prompt = (
            f"[이벤트] {trigger_reason}\n\n"
            f"현재 지표:\n"
            f"- EMA 기울기: {summary['ema_slope']:+.3f}\n"
            f"- flow_score: {summary['flow_score']:+.3f} (3분 추이: {summary.get('flow_trend','N/A')})\n"
            f"- 거래량 폭발: {summary['vol_burst']:.2f}\n"
            f"- LONG점수: {summary['long_score']:.2f} / SHORT점수: {summary['short_score']:.2f}\n"
            f"- 가격 vs VWAP: {summary['price_vs_vwap']:+.2f}pt\n"
            f"- 일중 범위 내 위치: {summary['pos_in_range']}% "
            f"(고점까지 -{summary['high_dist_pt']:.1f}pt / 저점에서 +{summary['low_dist_pt']:.1f}pt)\n"
            f"- 일중 범위: {summary['sess_range_pt']:.1f}pt\n"
            f"- 3분 가격변동: {summary.get('price_change',0):+.1f}pt "
            f"/ LONG비율: {summary.get('long_ratio',0.5):.0%}"
            f"{summary.get('track_info', '')}"
            f"{prev_str}"
        )

        try:
            resp = requests.post(
                _HAIKU_URL,
                headers={
                    "x-api-key":         self._api_key,
                    "anthropic-version": "2023-06-01",
                    "content-type":      "application/json",
                },
                json={
                    "model":      _HAIKU_MODEL,
                    "max_tokens": _MAX_TOKENS,
                    "system":     _SYSTEM_PROMPT,
                    "messages":   [{"role": "user", "content": user_prompt}],
                },
                timeout=_HAIKU_TIMEOUT,
            )
            resp.raise_for_status()
            raw = resp.json()["content"][0]["text"].strip()
        except Exception as e:
            log.warning("MarketAnalyst Haiku 오류: %s", e)
            return None

        if raw.startswith("```"):
            lines = raw.split("\n")
            raw = "\n".join(lines[1:-1] if lines[-1].strip() == "```" else lines[1:])

        try:
            parsed = json.loads(raw)
        except (json.JSONDecodeError, ValueError) as e:
            log.warning("MarketAnalyst JSON 파싱 실패: %s | raw=%s", e, raw[:200])
            return None

        view       = str(parsed.get("view",       "")).strip().upper()
        direction  = str(parsed.get("direction",  "")).strip().upper()
        confidence = str(parsed.get("confidence", "")).strip().upper()
        reason     = str(parsed.get("reason",     "")).strip()

        if view not in VALID_VIEWS:
            log.warning("MarketAnalyst 유효하지 않은 view=%s", view)
            return None
        if direction  not in VALID_DIRECTIONS: direction  = "NEUTRAL"
        if confidence not in VALID_CONFIDENCE: confidence = "LOW"

        return {"view": view, "direction": direction,
                "confidence": confidence, "reason": reason}
