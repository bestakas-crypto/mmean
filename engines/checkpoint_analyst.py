# engines/checkpoint_analyst.py
"""
CheckpointAnalyst — 장중 3개 체크포인트 구조 분석기

체크포인트:
  09:15 → opening_char      : 아침장 성격 판정 (추격 필터 / size 축소 근거)
  10:30 → provisional_regime: 잠정 데이 레짐  (Sonnet 진입 판단 반영)
  13:00 → afternoon_regime  : 오후장 재평가   (후반 운용 방향 수정)

역할 분리:
  - MarketAnalyst    : 3분 미시 상태 감시 (market_view)  ← 빠른 신호
  - CheckpointAnalyst: 구간별 구조 분석   (slow regime)   ← 느린 판단
  - session_pattern  : 장 종료 후 사후 확정 (RAG/sim 용)  ← 학습 재료

09:15 출력 (opening_char):
  char       : opening_strength | opening_fade | initial_chop |
               early_imbalance_up | early_imbalance_down
  size_filter: normal | reduce | skip
  reason     : 한 줄 근거

10:30 출력 (provisional_regime):
  regime    : trend_continuation | reversal | balance
  direction : LONG_BIAS | SHORT_BIAS | NEUTRAL
  confidence: HIGH | MID | LOW
  reason    : 한 줄 근거

13:00 출력 (afternoon_regime):
  regime  : trend_alive | trend_dead | reversal | box
  late_bias: LONG_BIAS | SHORT_BIAS | NEUTRAL
  risk_adj : normal | conservative | aggressive
  reason   : 한 줄 근거

운영 규칙:
  - 매일 1회씩만 발동 (날짜 + 이름으로 fired 추적)
  - 체크포인트 시각 이후 처음 30초 루프 순환에서 실행
  - Haiku 사용 (빠르고 저렴)
  - API 실패 시 state 그대로 유지 (거래 계속)
"""
from __future__ import annotations

import json
import logging
import os
import sqlite3
import threading
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

import requests

log = logging.getLogger("MMEAN.CheckpointAnalyst")

_HAIKU_MODEL   = "claude-haiku-4-5-20251001"
_HAIKU_URL     = "https://api.anthropic.com/v1/messages"
_HAIKU_TIMEOUT = 15
_MAX_TOKENS    = 200
_LOOP_SEC      = 30   # 체크포인트 체크 주기 (초)

# ── 체크포인트 정의 ────────────────────────────────────────────────────────
#   (name, trigger_hhmm, state_key, data_window_start, data_window_end)
_CHECKPOINTS = [
    ("opening",   "09:15", "opening_char",       "08:44", "09:15"),
    ("midday",    "10:30", "provisional_regime",  "09:00", "10:30"),
    ("afternoon", "13:00", "afternoon_regime",    "09:00", "13:00"),
]

# ── 시스템 프롬프트 ────────────────────────────────────────────────────────
_SYS_OPENING = """\
KOSPI200 선물 아침장(08:44~09:15) 분석가.
오전 개장 구간의 성격을 판단한다.

반드시 아래 JSON만 출력 (다른 텍스트 없이):
{"char": "...", "size_filter": "...", "reason": "..."}

char 선택지: opening_strength, opening_fade, initial_chop, early_imbalance_up, early_imbalance_down
size_filter 선택지: normal, reduce, skip
reason: 한 줄 핵심 근거 (한국어)"""

_SYS_MIDDAY = """\
KOSPI200 선물 오전장(09:00~10:30) 분석가.
오전 구간의 방향성과 잠정 데이 레짐을 판단한다.

반드시 아래 JSON만 출력 (다른 텍스트 없이):
{"regime": "...", "direction": "...", "confidence": "...", "reason": "..."}

regime 선택지: trend_continuation, reversal, balance
direction 선택지: LONG_BIAS, SHORT_BIAS, NEUTRAL
confidence 선택지: HIGH, MID, LOW
reason: 한 줄 핵심 근거 (한국어)"""

_SYS_AFTERNOON = """\
KOSPI200 선물 오후장 재평가 분석가.
오전~점심 구간의 추세 지속 여부와 오후 운용 방향을 판단한다.

반드시 아래 JSON만 출력 (다른 텍스트 없이):
{"regime": "...", "late_bias": "...", "risk_adj": "...", "reason": "..."}

regime 선택지: trend_alive, trend_dead, reversal, box
late_bias 선택지: LONG_BIAS, SHORT_BIAS, NEUTRAL
risk_adj 선택지: normal, conservative, aggressive
reason: 한 줄 핵심 근거 (한국어)"""

_SYS_MAP = {
    "opening":   _SYS_OPENING,
    "midday":    _SYS_MIDDAY,
    "afternoon": _SYS_AFTERNOON,
}


class CheckpointAnalyst:
    """
    장중 3개 시점 구조 분석기.

    사용:
        ca = CheckpointAnalyst(runtime)
        ca.start()
        ...
        ca.stop()
    """

    def __init__(self, runtime: Any) -> None:
        self._runtime    = runtime
        self._stop_event = threading.Event()
        self._thread: Optional[threading.Thread] = None
        self._fired: Dict[str, bool] = {}   # "YYYY-MM-DD:name" → True
        self._api_key = os.getenv("ANTHROPIC_API_KEY", "").strip()

        if not self._api_key:
            log.warning("CheckpointAnalyst: ANTHROPIC_API_KEY 없음 — 비활성 상태")

    # ── 수명 주기 ──────────────────────────────────────────────────────────

    def start(self) -> None:
        if self._thread and self._thread.is_alive():
            return
        self._stop_event.clear()
        self._thread = threading.Thread(
            target=self._loop,
            daemon=True,
            name="MMEAN-CheckpointAnalyst",
        )
        self._thread.start()
        log.info("CheckpointAnalyst 시작 | 체크포인트: 09:15 / 10:30 / 13:00")

    def stop(self) -> None:
        self._stop_event.set()
        if self._thread:
            self._thread.join(timeout=5)
        log.info("CheckpointAnalyst 종료")

    # ── 메인 루프 ──────────────────────────────────────────────────────────

    def _loop(self) -> None:
        while not self._stop_event.is_set():
            self._stop_event.wait(_LOOP_SEC)
            if self._stop_event.is_set():
                break
            try:
                self._check_all()
            except Exception as e:
                log.warning("CheckpointAnalyst 루프 오류 (계속): %s", e)

    def _check_all(self) -> None:
        now_hhmm = datetime.now().strftime("%H:%M")
        today    = datetime.now().strftime("%Y-%m-%d")

        if now_hhmm < "08:44" or now_hhmm > "15:45":
            return

        for name, trigger, state_key, win_start, win_end in _CHECKPOINTS:
            if now_hhmm < trigger:
                continue
            fired_key = f"{today}:{name}"
            if self._fired.get(fired_key):
                continue

            # 체크포인트 도달 → 분석 실행
            self._fired[fired_key] = True
            log.info("CheckpointAnalyst: [%s] 체크포인트 도달 → 분석 시작", name)
            try:
                self._run_checkpoint(name, state_key, win_start, win_end, today)
            except Exception as e:
                log.warning("CheckpointAnalyst [%s] 실패 (계속): %s", name, e)

    # ── 체크포인트 실행 ───────────────────────────────────────────────────

    def _run_checkpoint(
        self,
        name: str,
        state_key: str,
        win_start: str,
        win_end: str,
        today: str,
    ) -> None:
        if not self._api_key:
            return

        ticks = self._fetch_ticks(today, win_start, win_end)
        if len(ticks) < 5:
            log.info("CheckpointAnalyst [%s]: 틱 부족 (%d개) — 스킵", name, len(ticks))
            return

        summary  = self._summarize(ticks)
        prev_ctx = self._build_prev_context(name)
        result   = self._call_haiku(name, summary, prev_ctx)

        if result is None:
            return

        ts = datetime.now().strftime("%H:%M:%S")
        result["ts"] = ts

        with self._runtime.state_obj.engine_lock:
            self._runtime.state[state_key] = result

        log.info(
            "CheckpointAnalyst [%s] | %s %s",
            name, ts, json.dumps(result, ensure_ascii=False),
        )

    # ── DB 조회 ────────────────────────────────────────────────────────────

    def _fetch_ticks(
        self, today: str, win_start: str, win_end: str
    ) -> List[Any]:
        ts_start = f"{today} {win_start}:00"
        ts_end   = f"{today} {win_end}:59"
        try:
            conn = sqlite3.connect(
                f"file:{self._runtime.db_path}?mode=ro",
                uri=True, timeout=5,
            )
            conn.row_factory = sqlite3.Row
            rows = conn.execute("""
                SELECT ts, futures_price, bias, long_score, short_score,
                       volume_burst, flow_score, foreign_buy
                FROM regime_ticks
                WHERE ts BETWEEN ? AND ?
                  AND futures_price > 100
                ORDER BY ts
            """, (ts_start, ts_end)).fetchall()
            conn.close()
            return rows
        except Exception as e:
            log.warning("CheckpointAnalyst DB 조회 실패: %s", e)
            return []

    # ── 요약 통계 ──────────────────────────────────────────────────────────

    def _summarize(self, ticks: List[Any]) -> Dict:
        prices      = [float(r["futures_price"]) for r in ticks]
        biases      = [r["bias"] for r in ticks if r["bias"]]
        flow_scores = [float(r["flow_score"]) for r in ticks if r["flow_score"] is not None]
        vol_bursts  = [float(r["volume_burst"]) for r in ticks if r["volume_burst"] is not None]
        long_scores = [float(r["long_score"]) for r in ticks if r["long_score"] is not None]
        short_scores= [float(r["short_score"]) for r in ticks if r["short_score"] is not None]

        long_ratio  = biases.count("LONG") / len(biases) if biases else 0.5
        avg_flow    = sum(flow_scores) / len(flow_scores) if flow_scores else 0.0
        avg_vol     = sum(vol_bursts)  / len(vol_bursts)  if vol_bursts  else 1.0
        avg_long    = sum(long_scores) / len(long_scores) if long_scores else 0.5
        avg_short   = sum(short_scores)/ len(short_scores)if short_scores else 0.5

        # 가격 구조
        price_chg   = round(prices[-1] - prices[0], 2) if len(prices) >= 2 else 0.0
        price_range = round(max(prices) - min(prices), 2) if len(prices) >= 2 else 0.0
        # 고점/저점 발생 위치 (0.0~1.0)
        if len(prices) >= 3:
            idx_high = prices.index(max(prices)) / (len(prices) - 1)
            idx_low  = prices.index(min(prices))  / (len(prices) - 1)
        else:
            idx_high = idx_low = 0.5

        # flow 추이 (전반부 vs 후반부)
        flow_trend = "데이터부족"
        if len(flow_scores) >= 4:
            mid = len(flow_scores) // 2
            f_avg_early = sum(flow_scores[:mid]) / mid
            f_avg_late  = sum(flow_scores[mid:]) / (len(flow_scores) - mid)
            flow_trend  = "상승" if f_avg_late > f_avg_early else "하락"

        return {
            "tick_count":   len(ticks),
            "price_change": price_chg,
            "price_range":  price_range,
            "long_ratio":   round(long_ratio, 2),
            "avg_flow":     round(avg_flow, 3),
            "flow_trend":   flow_trend,
            "avg_vol":      round(avg_vol, 2),
            "avg_long":     round(avg_long, 3),
            "avg_short":    round(avg_short, 3),
            "high_pos_pct": round(idx_high * 100),
            "low_pos_pct":  round(idx_low  * 100),
        }

    # ── 이전 체크포인트 컨텍스트 ───────────────────────────────────────────

    def _build_prev_context(self, name: str) -> str:
        """이전 체크포인트 결과를 프롬프트 컨텍스트로 구성."""
        state = self._runtime.state
        parts = []

        if name in ("midday", "afternoon"):
            oc = state.get("opening_char")
            if oc:
                parts.append(
                    f"[09:15 아침장 판정] char={oc.get('char')} "
                    f"size_filter={oc.get('size_filter')} reason={oc.get('reason', '')}"
                )
        if name == "afternoon":
            pr = state.get("provisional_regime")
            if pr:
                parts.append(
                    f"[10:30 잠정 레짐] regime={pr.get('regime')} "
                    f"direction={pr.get('direction')} conf={pr.get('confidence')} "
                    f"reason={pr.get('reason', '')}"
                )
        mv = state.get("market_view")
        if mv:
            parts.append(
                f"[현재 3분 market_view] view={mv.get('view')} "
                f"dir={mv.get('direction')} conf={mv.get('confidence')}"
            )

        return "\n".join(parts) if parts else ""

    # ── Haiku 호출 ─────────────────────────────────────────────────────────

    def _call_haiku(
        self,
        name: str,
        summary: Dict,
        prev_ctx: str,
    ) -> Optional[Dict]:
        sys_prompt = _SYS_MAP[name]

        user_prompt = (
            f"분석 구간 지표 요약:\n"
            f"- 틱수: {summary['tick_count']}\n"
            f"- 가격변동: {summary['price_change']:+.1f}pt  "
            f"  가격범위: {summary['price_range']:.1f}pt\n"
            f"- LONG비율: {summary['long_ratio']:.0%}\n"
            f"- flow_score 평균: {summary['avg_flow']:+.3f} (추이: {summary['flow_trend']})\n"
            f"- 변동성(avg_vol): {summary['avg_vol']:.2f}\n"
            f"- long_score평균: {summary['avg_long']:.3f}  "
            f"  short_score평균: {summary['avg_short']:.3f}\n"
            f"- 고점위치: 구간의 {summary['high_pos_pct']}%  "
            f"  저점위치: 구간의 {summary['low_pos_pct']}%"
        )
        if prev_ctx:
            user_prompt += f"\n\n[이전 체크포인트 결과]\n{prev_ctx}"

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
                    "system":     sys_prompt,
                    "messages":   [{"role": "user", "content": user_prompt}],
                },
                timeout=_HAIKU_TIMEOUT,
            )
            resp.raise_for_status()
            raw = resp.json()["content"][0]["text"].strip()
        except Exception as e:
            log.warning("CheckpointAnalyst [%s] Haiku HTTP 오류: %s", name, e)
            return None

        # ```json ... ``` 래퍼 제거
        if raw.startswith("```"):
            lines = raw.split("\n")
            raw = "\n".join(lines[1:-1] if lines[-1].strip() == "```" else lines[1:])

        try:
            parsed = json.loads(raw)
        except (json.JSONDecodeError, ValueError) as e:
            log.warning(
                "CheckpointAnalyst [%s] JSON 파싱 실패: %s | raw=%s",
                name, e, raw[:200],
            )
            return None

        return parsed
