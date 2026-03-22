# engines/market_analyst.py
"""
MarketAnalyst — 장중 3분 인터벌 시장 상태 분석기

기능:
  - 09:00 이후부터 3분마다 Haiku LLM 호출
  - 최근 3분 regime_ticks 요약 → 시장 상태(view) 분류
  - 직전 view를 컨텍스트로 전달 → 연속적 분석 (스파이크/반전 감지)
  - runtime.state["market_view"] 갱신 → EntryAdvisor(Sonnet)이 컨텍스트로 활용

state 키:
  market_view         : dict | None  — {view, direction, confidence, reason}
  market_view_ts      : str          — 마지막 갱신 시각 (HH:MM:SS)
  market_view_history : list         — 최근 3개 view 이력

view 분류값:
  TREND_UP       : 상승 추세 (외국인 매수 + LONG bias 지속)
  TREND_DOWN     : 하락 추세 (외국인 매도 + SHORT bias 지속)
  VOLATILE       : 고변동 (방향 불명, volume_burst HIGH)
  CHOPPY         : 저변동 혼조 (방향 없음, range 좁음)
  SPIKE_REVERSAL : 급등락 후 반전 (오버슈팅 + 급반전)
  REVERSAL_UP    : 하락 후 반전 상승
  REVERSAL_DOWN  : 상승 후 반전 하락 (BULL_TRAP 포함)

운영 규칙:
  - 08:44~09:00: 스킵 (초반 15분 관망)
  - 09:00~15:45: 3분마다 실행
  - API 키 없거나 호출 실패 시: market_view = None 유지 (거래 계속)
  - 실패해도 state 오염 없음 (None 유지)
"""
from __future__ import annotations

import json
import logging
import os
import sqlite3
import threading
from collections import deque
from datetime import datetime, timedelta
from typing import Any, Deque, Dict, List, Optional

import requests

log = logging.getLogger("MMEAN.MarketAnalyst")

VALID_VIEWS = {
    "TREND_UP", "TREND_DOWN", "VOLATILE", "CHOPPY",
    "SPIKE_REVERSAL", "REVERSAL_UP", "REVERSAL_DOWN",
}
VALID_DIRECTIONS = {"LONG_BIAS", "SHORT_BIAS", "NEUTRAL"}
VALID_CONFIDENCE = {"HIGH", "MID", "LOW"}

_HAIKU_MODEL   = "claude-haiku-4-5-20251001"
_HAIKU_URL     = "https://api.anthropic.com/v1/messages"
_HAIKU_TIMEOUT = 10        # seconds
_MAX_TOKENS    = 150
_HISTORY_LEN   = 3        # 보관할 이전 view 수

_SYSTEM_PROMPT = """\
KOSPI200 선물 장중 시장 분석가.
최근 3분간 지표를 보고 현재 시장 상태를 분류한다.

반드시 아래 JSON 형식으로만 응답 (다른 텍스트 없이):
{"view": "...", "direction": "...", "confidence": "...", "reason": "..."}

view 선택지: TREND_UP, TREND_DOWN, VOLATILE, CHOPPY, SPIKE_REVERSAL, REVERSAL_UP, REVERSAL_DOWN
direction: LONG_BIAS, SHORT_BIAS, NEUTRAL
confidence: HIGH, MID, LOW
reason: 한 줄 핵심 근거 (한국어)"""


class MarketAnalyst:
    """
    장중 3분 인터벌 시장 상태 분석기.

    사용:
        analyst = MarketAnalyst(runtime, interval_sec=180)
        analyst.start()
        ...
        analyst.stop()
    """

    def __init__(self, runtime: Any, interval_sec: int = 180) -> None:
        self._runtime      = runtime
        self._interval     = interval_sec
        self._stop_event   = threading.Event()
        self._thread: Optional[threading.Thread] = None
        self._view_history: Deque[Dict] = deque(maxlen=_HISTORY_LEN)
        self._api_key      = os.getenv("ANTHROPIC_API_KEY", "").strip()

        if not self._api_key:
            log.warning("MarketAnalyst: ANTHROPIC_API_KEY 없음 — 비활성 상태로 시작")

    # ── 수명 주기 ──────────────────────────────────────────────────

    def start(self) -> None:
        if self._thread and self._thread.is_alive():
            return
        self._stop_event.clear()
        self._thread = threading.Thread(
            target=self._loop,
            daemon=True,
            name="MMEAN-MarketAnalyst",
        )
        self._thread.start()
        log.info("MarketAnalyst 시작 | interval=%ds | model=%s", self._interval, _HAIKU_MODEL)

    def stop(self) -> None:
        self._stop_event.set()
        if self._thread:
            self._thread.join(timeout=5)
        log.info("MarketAnalyst 종료")

    # ── 메인 루프 ──────────────────────────────────────────────────

    def _loop(self) -> None:
        while not self._stop_event.is_set():
            self._stop_event.wait(self._interval)
            if self._stop_event.is_set():
                break
            try:
                self._run_once()
            except Exception as e:
                log.warning("MarketAnalyst 루프 오류 (계속): %s", e)

    def _run_once(self) -> None:
        now_hhmm = datetime.now().strftime("%H:%M")

        # 초반 15분 및 장 외 시간 스킵
        if now_hhmm < "09:00" or now_hhmm > "15:45":
            return

        if not self._api_key:
            return

        ticks = self._fetch_recent_ticks(minutes=3)
        if len(ticks) < 5:
            log.debug("MarketAnalyst: 틱 부족 (%d개) — 스킵", len(ticks))
            return

        summary  = self._summarize(ticks)
        prev_view = self._view_history[-1] if self._view_history else None
        result   = self._call_haiku(summary, prev_view)

        if result is None:
            return

        ts = datetime.now().strftime("%H:%M:%S")
        entry = {**result, "ts": ts}
        self._view_history.append(entry)

        with self._runtime.state_obj.engine_lock:
            self._runtime.state["market_view"]         = result
            self._runtime.state["market_view_ts"]      = ts
            self._runtime.state["market_view_history"] = list(self._view_history)

        log.info(
            "MarketAnalyst | %s view=%-15s dir=%-11s conf=%s | %s",
            ts, result["view"], result["direction"],
            result["confidence"], result["reason"],
        )
        # ※ AdoptionViewAdapter 직접 호출 제거됨 (2026-03-21)
        # market_view는 미시 상태 감시 전용. config 자동 교체는 CheckpointAnalyst가 담당.

    # ── DB 조회 ────────────────────────────────────────────────────

    def _fetch_recent_ticks(self, minutes: int = 3) -> List[Any]:
        cutoff = (datetime.now() - timedelta(minutes=minutes)).strftime("%Y-%m-%d %H:%M:%S")
        today  = datetime.now().strftime("%Y-%m-%d")
        try:
            conn = sqlite3.connect(
                f"file:{self._runtime.db_path}?mode=ro",
                uri=True,
                timeout=5,
            )
            conn.row_factory = sqlite3.Row
            rows = conn.execute("""
                SELECT ts, futures_price, bias, long_score, short_score,
                       volume_burst, flow_score, foreign_buy
                FROM regime_ticks
                WHERE ts >= ?
                  AND substr(ts, 1, 10) = ?
                  AND futures_price > 100
                ORDER BY ts
            """, (cutoff, today)).fetchall()
            conn.close()
            return rows
        except Exception as e:
            log.warning("MarketAnalyst DB 조회 실패: %s", e)
            return []

    # ── 요약 통계 ──────────────────────────────────────────────────

    def _summarize(self, ticks: List[Any]) -> Dict:
        prices      = [float(r["futures_price"]) for r in ticks]
        biases      = [r["bias"] for r in ticks if r["bias"]]
        flow_scores = [float(r["flow_score"]) for r in ticks if r["flow_score"] is not None]
        vol_bursts  = [float(r["volume_burst"]) for r in ticks if r["volume_burst"] is not None]

        long_ratio  = biases.count("LONG")  / len(biases) if biases else 0.5
        short_ratio = biases.count("SHORT") / len(biases) if biases else 0.5

        avg_flow  = sum(flow_scores) / len(flow_scores) if flow_scores else 0.0
        avg_vol   = sum(vol_bursts)  / len(vol_bursts)  if vol_bursts  else 1.0

        # flow_score 추이: 전반부 vs 후반부
        if len(flow_scores) >= 4:
            mid = len(flow_scores) // 2
            flow_trend = (
                "상승" if sum(flow_scores[mid:]) / len(flow_scores[mid:])
                          > sum(flow_scores[:mid]) / len(flow_scores[:mid])
                else "하락"
            )
        else:
            flow_trend = "데이터부족"

        price_chg = round(prices[-1] - prices[0], 2) if len(prices) >= 2 else 0.0
        # 고점/저점 위치 (시작 기준 몇 %에서 발생했는가)
        if len(prices) >= 3:
            idx_high = prices.index(max(prices)) / (len(prices) - 1)
            idx_low  = prices.index(min(prices))  / (len(prices) - 1)
        else:
            idx_high = idx_low = 0.5

        return {
            "tick_count":   len(ticks),
            "price_change": price_chg,
            "long_ratio":   round(long_ratio, 2),
            "short_ratio":  round(short_ratio, 2),
            "avg_flow":     round(avg_flow, 3),
            "flow_trend":   flow_trend,
            "avg_vol":      round(avg_vol, 2),
            "vol_state":    "HIGH" if avg_vol >= 1.8 else ("LOW" if avg_vol <= 0.8 else "MID"),
            "high_pos_pct": round(idx_high * 100),   # 고점이 구간 몇%에 있나
            "low_pos_pct":  round(idx_low  * 100),   # 저점이 구간 몇%에 있나
        }

    # ── Haiku 호출 ─────────────────────────────────────────────────

    def _call_haiku(
        self,
        summary: Dict,
        prev_view: Optional[Dict],
    ) -> Optional[Dict]:

        prev_str = ""
        if prev_view:
            prev_str = (
                f"\n[직전 분석({prev_view.get('ts', '')})] "
                f"view={prev_view.get('view')} "
                f"direction={prev_view.get('direction')} "
                f"reason={prev_view.get('reason', '')}"
            )

        user_prompt = (
            f"최근 3분 지표:\n"
            f"- 틱수: {summary['tick_count']}\n"
            f"- 가격변동: {summary['price_change']:+.1f}pt\n"
            f"- LONG비율: {summary['long_ratio']:.0%} / SHORT비율: {summary['short_ratio']:.0%}\n"
            f"- flow_score 평균: {summary['avg_flow']:+.3f} (추이: {summary['flow_trend']})\n"
            f"- 변동성(volume_burst): {summary['avg_vol']:.2f} [{summary['vol_state']}]\n"
            f"- 고점 발생위치: 구간의 {summary['high_pos_pct']}% / "
            f"저점 발생위치: 구간의 {summary['low_pos_pct']}%"
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
            log.warning("MarketAnalyst Haiku HTTP 오류: %s", e)
            return None

        # JSON 추출 (```json ... ``` 감싸기 제거)
        if raw.startswith("```"):
            lines = raw.split("\n")
            raw = "\n".join(lines[1:-1] if lines[-1].strip() == "```" else lines[1:])

        try:
            parsed = json.loads(raw)
        except (json.JSONDecodeError, ValueError) as e:
            log.warning("MarketAnalyst JSON 파싱 실패: %s | raw=%s", e, raw[:200])
            return None

        # 스키마 검증
        view      = str(parsed.get("view", "")).strip().upper()
        direction = str(parsed.get("direction", "")).strip().upper()
        confidence = str(parsed.get("confidence", "")).strip().upper()
        reason    = str(parsed.get("reason", "")).strip()

        if view not in VALID_VIEWS:
            log.warning("MarketAnalyst 유효하지 않은 view=%s", view)
            return None
        if direction not in VALID_DIRECTIONS:
            direction = "NEUTRAL"
        if confidence not in VALID_CONFIDENCE:
            confidence = "LOW"

        return {
            "view":       view,
            "direction":  direction,
            "confidence": confidence,
            "reason":     reason,
        }
