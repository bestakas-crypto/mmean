# engines/llm_caller.py
"""
LLM 호출 오케스트레이터 — 진입 전 1회 호출 + 6가지 실패 처리 + 중복 호출 차단

명세서 §7 안전장치:
  호출 실패 조건 (아래 6가지 전부, 하나도 누락하면 안 됨):
    1. timeout
    2. JSON parse fail
    3. schema mismatch
    4. rate limit
    5. provider fail
    6. confidence < 0.55

  모든 실패: weight_adjust = 0, confidence = 0
             → 엔진 기본 규칙만으로 진행 (거래 중단 아님)
  실패 시 반드시 로그에 "[FALLBACK weight_adjust=0]" 문구 기록

호출 시점:
  ✔ 진입 전 1회만 (LLMCallDedup으로 중복 차단)
  ✗ 포지션 보유 중 호출 금지 (포지션 흔들림)
  ✗ 청산 판단 개입 금지 (손절 지연 위험)

중복 호출 차단 (LLMCallDedup):
  Call Key = (분봉 문자열, entry_signal)
  예: ("2026-03-16 14:23", "LONG_READY")
  동일 분봉·신호에서 2회 이상 call_once() 호출 시 두 번째부터 즉시 캐시 반환
"""
from __future__ import annotations

import asyncio
import json
import logging
from dataclasses import dataclass
from typing import Any, Dict, Optional, Tuple

from logs.llm_decision_log import (
    LLMDecisionLog,
    ERR_TIMEOUT, ERR_JSON_PARSE_FAIL, ERR_SCHEMA_MISMATCH,
    ERR_RATE_LIMIT, ERR_PROVIDER_FAIL, ERR_CONFIDENCE_LOW,
)

log = logging.getLogger("MMEAN.LLMCaller")

# ── 파라미터 ──────────────────────────────────────────────────────
CONFIDENCE_MIN    = 0.55      # 이 미만은 LLM 출력 무시
WEIGHT_ADJ_MAX    =  0.20
WEIGHT_ADJ_MIN    = -0.20

# LLM JSON 출력 필수 키
REQUIRED_KEYS    = {"risk_view", "confidence", "reason_main", "reason_against", "weight_adjust"}
VALID_RISK_VIEWS = {"positive", "neutral", "negative"}

# ── FALLBACK 로그 태그 ─────────────────────────────────────────────
_FALLBACK_TAG = "[FALLBACK weight_adjust=0]"


@dataclass
class LLMCallResult:
    success:        bool
    weight_adjust:  float     # 실패 시 항상 0.0
    confidence:     float     # 실패 시 항상 0.0
    risk_view:      str       # 실패 시 "failure"
    reason_main:    str
    reason_against: str
    error_type:     Optional[str]   # None이면 성공
    log_id:         Optional[int]   # LLMDecisionLog row id
    from_cache:     bool = False     # True: 중복 차단 캐시 반환


# ── 실패 결과 팩토리 ─────────────────────────────────────────────
def _failure(error_type: str, log_id: Optional[int] = None) -> LLMCallResult:
    return LLMCallResult(
        success=False,
        weight_adjust=0.0,
        confidence=0.0,
        risk_view="failure",
        reason_main="",
        reason_against="",
        error_type=error_type,
        log_id=log_id,
        from_cache=False,
    )


# ── 중복 호출 차단 ────────────────────────────────────────────────
class LLMCallDedup:
    """
    동일 분봉·신호에서 중복 LLM 호출 차단.

    Call Key = (minute_str, signal)
      minute_str: "YYYY-MM-DD HH:MM" 형식 (분봉 단위)
      signal:     "LONG_READY" | "SHORT_READY" 등

    동작:
      - 새 key → 허용, key 저장
      - 동일 key 재호출 → 차단, 직전 결과 반환
      - consume_on_entry() 호출 또는 새 분봉 → key 초기화

    사용 예:
        dedup = LLMCallDedup()
        if dedup.check_and_mark(minute_str, signal):
            result = await caller.call_once(...)
            dedup.last_result = result
        else:
            result = dedup.last_result  # 캐시 반환
    """

    def __init__(self) -> None:
        self._last_key:    Optional[Tuple[str, str]] = None
        self.last_result:  Optional[LLMCallResult]   = None

    def check_and_mark(self, minute_str: str, signal: str) -> bool:
        """
        True  → 신규 호출 허용 (key 저장)
        False → 중복 차단 (last_result 재사용)
        """
        key = (minute_str, signal)
        if key == self._last_key:
            log.warning(
                "LLMCallDedup 차단 %s | key=(%s, %s) — 직전 결과 재사용",
                _FALLBACK_TAG, minute_str, signal,
            )
            return False
        self._last_key = key
        return True

    def reset(self) -> None:
        """진입 결정 소멸(TTL) 또는 외부 초기화 시 호출."""
        self._last_key   = None
        self.last_result = None
        log.debug("LLMCallDedup 초기화")


# ── 메인 호출 클래스 ──────────────────────────────────────────────
class LLMCaller:
    """
    LLM 호출 + 실패 처리 + 로그 기록 통합.

    사용 예:
        caller = LLMCaller(llm_chain=runtime.llm_chain, decision_log=LLMDecisionLog())
        result = await caller.call_once(
            system_prompt=system_prompt,
            user_prompt=user_prompt,
            input_snapshot={...},   # build_snapshot() 결과 — 원본 시계열 제외
        )
        weight_adjust = result.weight_adjust   # 실패 시 자동 0.0
    """

    def __init__(
        self,
        llm_chain:     Any,                  # LLMChain (llm_chain.py)
        decision_log:  LLMDecisionLog,
        timeout_sec:   float = 12.0,
    ) -> None:
        self._llm        = llm_chain
        self._log        = decision_log
        self._timeout    = timeout_sec
        self.dedup       = LLMCallDedup()

    async def call_once(
        self,
        system_prompt:  str,
        user_prompt:    str,
        input_snapshot: Dict[str, Any],
    ) -> LLMCallResult:
        """
        진입 전 1회 LLM 호출.
        모든 실패 케이스에서 weight_adjust=0.0 반환 보장.
        실패마다 [FALLBACK weight_adjust=0] 태그 로그 기록.
        """
        raw_response: Optional[str] = None

        # ── 1. LLM 호출 (timeout 포함) ───────────────────────────
        try:
            raw_response = await asyncio.wait_for(
                self._llm.ask(
                    system=system_prompt,
                    user=user_prompt,
                ),
                timeout=self._timeout,
            )
        except asyncio.TimeoutError:
            log.warning(
                "LLMCaller timeout (%.1fs) %s | error=%s",
                self._timeout, _FALLBACK_TAG, ERR_TIMEOUT,
            )
            self._log.record_failure(ERR_TIMEOUT, input_snapshot.get("flow_score", 0.0))
            return _failure(ERR_TIMEOUT)
        except Exception as e:
            err_str = str(e).lower()
            if "429" in err_str or "rate" in err_str:
                etype = ERR_RATE_LIMIT
            else:
                etype = ERR_PROVIDER_FAIL
            log.warning(
                "LLMCaller provider 오류 [%s] %s | detail=%s",
                etype, _FALLBACK_TAG, e,
            )
            self._log.record_failure(etype, input_snapshot.get("flow_score", 0.0), str(e))
            return _failure(etype)

        # ── 2. JSON 파싱 ─────────────────────────────────────────
        try:
            text = raw_response.strip()
            if text.startswith("```"):
                lines = text.split("\n")
                text  = "\n".join(lines[1:-1] if lines[-1].strip() == "```" else lines[1:])
            parsed: Dict[str, Any] = json.loads(text)
        except (json.JSONDecodeError, ValueError) as e:
            log.warning(
                "LLMCaller JSON 파싱 실패 %s | error=%s | raw=%s",
                _FALLBACK_TAG, e, raw_response[:200],
            )
            self._log.record_failure(
                ERR_JSON_PARSE_FAIL, input_snapshot.get("flow_score", 0.0),
                f"raw={raw_response[:200]}"
            )
            return _failure(ERR_JSON_PARSE_FAIL)

        # ── 3. 스키마 검증 ────────────────────────────────────────
        missing = REQUIRED_KEYS - set(parsed.keys())
        if missing:
            log.warning(
                "LLMCaller 스키마 불일치 %s | 누락 키=%s",
                _FALLBACK_TAG, missing,
            )
            self._log.record_failure(
                ERR_SCHEMA_MISMATCH, input_snapshot.get("flow_score", 0.0),
                f"missing={missing}"
            )
            return _failure(ERR_SCHEMA_MISMATCH)

        risk_view = str(parsed.get("risk_view", "")).strip().lower()
        if risk_view not in VALID_RISK_VIEWS:
            log.warning(
                "LLMCaller 스키마 불일치 %s | risk_view='%s'",
                _FALLBACK_TAG, risk_view,
            )
            self._log.record_failure(
                ERR_SCHEMA_MISMATCH, input_snapshot.get("flow_score", 0.0),
                f"invalid risk_view={risk_view}"
            )
            return _failure(ERR_SCHEMA_MISMATCH)

        # ── 4. 수치 추출 및 clamp ─────────────────────────────────
        try:
            confidence    = float(parsed["confidence"])
            weight_adjust = float(parsed["weight_adjust"])
        except (TypeError, ValueError) as e:
            log.warning(
                "LLMCaller 수치 파싱 실패 %s | error=%s",
                _FALLBACK_TAG, e,
            )
            self._log.record_failure(
                ERR_SCHEMA_MISMATCH, input_snapshot.get("flow_score", 0.0)
            )
            return _failure(ERR_SCHEMA_MISMATCH)

        confidence    = max(0.0, min(1.0, confidence))
        weight_adjust = max(WEIGHT_ADJ_MIN, min(WEIGHT_ADJ_MAX, weight_adjust))

        # ── 5. confidence 필터 ────────────────────────────────────
        if confidence < CONFIDENCE_MIN:
            log.info(
                "LLMCaller confidence=%.3f < %.2f %s | error=%s",
                confidence, CONFIDENCE_MIN, _FALLBACK_TAG, ERR_CONFIDENCE_LOW,
            )
            self._log.record_failure(
                ERR_CONFIDENCE_LOW, input_snapshot.get("flow_score", 0.0),
                f"confidence={confidence:.3f}"
            )
            return _failure(ERR_CONFIDENCE_LOW)

        # ── 6. 성공 로그 기록 ─────────────────────────────────────
        llm_output = {
            "risk_view":      risk_view,
            "confidence":     confidence,
            "reason_main":    str(parsed.get("reason_main", "")),
            "reason_against": str(parsed.get("reason_against", "")),
            "weight_adjust":  weight_adjust,
        }
        log_id = self._log.record(
            input_snapshot=input_snapshot,
            llm_output=llm_output,
        )

        log.info(
            "LLMCaller 성공 | risk=%s | conf=%.3f | wa=%+.4f | log_id=%d",
            risk_view, confidence, weight_adjust, log_id,
        )

        result = LLMCallResult(
            success=True,
            weight_adjust=weight_adjust,
            confidence=confidence,
            risk_view=risk_view,
            reason_main=llm_output["reason_main"],
            reason_against=llm_output["reason_against"],
            error_type=None,
            log_id=log_id,
            from_cache=False,
        )
        self.dedup.last_result = result
        return result
