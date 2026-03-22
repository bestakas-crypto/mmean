# engines/llm_ema_filter.py
"""
LLM EMA 필터 — weight_adjust 안정화

목적:
  직전 LLM 호출값을 바로 반영하면 주문 사이즈 진동 발생.
  span=3 지수이동평균으로 안정화 후 PositionEngine에 전달.

weight_adjust 범위: -0.20 ~ +0.20 (PositionEngine 최종 clamp와 독립)

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
expire_mode 옵션 (TTL 만료 시 동작)
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
  "decay" (기본):
    TTL 만료 후 매 틱 update(0.0)을 계속 호출.
    EMA가 0 방향으로 점진적으로 감쇠.
    span=3 기준 α=0.5이므로 약 4~5틱 후 실질적으로 0에 수렴.
    장점: 급격한 사이즈 변화 방지 (실험 초기 안전)
    단점: 만료 후에도 수 틱간 LLM 영향 잔존

  "reset":
    TTL 만료 이벤트(on_ttl_expire()) 발생 즉시 EMA를 None으로 초기화.
    다음 update() 호출 시 raw값으로 새로 시작.
    장점: 만료된 LLM 판단이 완전히 사라짐 (깔끔한 분리)
    단점: 다음 유효 LLM 결과 전까지 weight_adjust=0 점프 가능

선택 기준:
  실험 초기 → decay (기본)
  LLM 정확도 검증 후, 만료 후 잔존이 문제면 → reset으로 전환
  app_bootstrap에서 expire_mode="reset" 인자로 변경 가능
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
"""
from __future__ import annotations

import logging
from typing import Literal

log = logging.getLogger("MMEAN.LLMEmaFilter")

# ── EMA span ──────────────────────────────────────────────────────
DEFAULT_SPAN = 3        # 명세서 지정값
MAX_ADJUST   =  0.20    # weight_adjust 절대 한도
MIN_ADJUST   = -0.20

EXPIRE_MODE_DECAY = "decay"
EXPIRE_MODE_RESET = "reset"


def _ema_alpha(span: int) -> float:
    """span → EMA 감쇠 계수 α = 2 / (span + 1)"""
    return 2.0 / (span + 1)


class LLMEmaFilter:
    """
    weight_adjust 시계열에 span=N EMA를 적용하여 안정화된 값을 반환.

    사용 예:
        # 기본 (decay 모드)
        flt = LLMEmaFilter()
        filtered = flt.update(raw_weight_adjust)  # → PositionEngine에 전달

        # reset 모드 (TTL 만료 후 즉시 0)
        flt = LLMEmaFilter(expire_mode="reset")

        # TTL 만료 이벤트 (engine_runtime에서 호출)
        flt.on_ttl_expire()

    span 튜닝:
        span=3 (α≈0.50): 빠른 반응, 약 4틱 후 0 수렴 (기본)
        span=5 (α≈0.33): 중간
        span=7 (α≈0.25): 느린 반응, 더 오래 잔존
        app_bootstrap에서 span= 인자로 조절 가능
    """

    def __init__(
        self,
        span:        int = DEFAULT_SPAN,
        expire_mode: str = EXPIRE_MODE_DECAY,
    ) -> None:
        if expire_mode not in (EXPIRE_MODE_DECAY, EXPIRE_MODE_RESET):
            log.warning(
                "LLMEmaFilter: 알 수 없는 expire_mode='%s', 'decay'로 대체",
                expire_mode,
            )
            expire_mode = EXPIRE_MODE_DECAY

        self.span        = span
        self.alpha       = _ema_alpha(span)
        self.expire_mode = expire_mode
        self._ema: float | None = None

        log.debug(
            "LLMEmaFilter 초기화 | span=%d α=%.4f expire_mode=%s",
            span, self.alpha, expire_mode,
        )

    def update(self, raw: float) -> float:
        """
        raw weight_adjust를 받아 EMA 적용 후 반환.
        첫 호출 시 EMA를 raw로 초기화.
        TTL 만료 후 raw=0.0이면 EMA를 0 방향으로 감쇠 (decay 모드).
        reset 모드에서 EMA가 None이면 0.0 반환 (on_ttl_expire가 reset했으므로).
        """
        # reset 모드 + TTL 만료(on_ttl_expire 호출됨) → EMA=None → 0.0 반환
        if self._ema is None and raw == 0.0:
            return 0.0

        # 입력 clamp
        raw = max(MIN_ADJUST, min(MAX_ADJUST, raw))

        if self._ema is None:
            self._ema = raw
        else:
            self._ema = self.alpha * raw + (1.0 - self.alpha) * self._ema

        filtered = max(MIN_ADJUST, min(MAX_ADJUST, self._ema))

        log.debug(
            "LLMEmaFilter | raw=%.4f → ema=%.4f | span=%d α=%.4f mode=%s",
            raw, filtered, self.span, self.alpha, self.expire_mode,
        )
        return filtered

    def on_ttl_expire(self) -> None:
        """
        TTL 만료 이벤트 처리.
        engine_runtime에서 TTL 소멸 직후 호출.

        expire_mode="reset":
            EMA 즉시 None으로 초기화 → 다음 LLM 결과 전까지 weight_adjust=0
        expire_mode="decay":
            아무것도 하지 않음 — 이후 update(0.0) 호출로 점진 감쇠
        """
        if self.expire_mode == EXPIRE_MODE_RESET:
            log.info(
                "LLMEmaFilter TTL 만료 → hard reset | 기존 ema=%.4f",
                self._ema if self._ema is not None else 0.0,
            )
            self._ema = None
        else:
            log.debug(
                "LLMEmaFilter TTL 만료 → decay 유지 | 현재 ema=%.4f (이후 update(0.0)으로 감쇠)",
                self._ema if self._ema is not None else 0.0,
            )

    def reset(self) -> None:
        """외부 요청 또는 세션 초기화 시 EMA 강제 초기화."""
        old = self._ema
        self._ema = None
        log.debug("LLMEmaFilter 강제 reset | 기존 ema=%s", f"{old:.4f}" if old is not None else "None")

    def current(self) -> float:
        """현재 EMA 값 조회 (초기화 전이면 0.0). 부작용 없음."""
        return self._ema if self._ema is not None else 0.0

    def reconfigure(self, span: int | None = None, expire_mode: str | None = None) -> None:
        """
        런타임 파라미터 변경 (span 튜닝 또는 expire_mode 전환).
        변경 즉시 alpha 재계산. EMA 값은 유지.
        """
        if span is not None and span != self.span:
            self.span  = span
            self.alpha = _ema_alpha(span)
            log.info("LLMEmaFilter span 변경 → %d (α=%.4f)", span, self.alpha)
        if expire_mode is not None and expire_mode != self.expire_mode:
            if expire_mode not in (EXPIRE_MODE_DECAY, EXPIRE_MODE_RESET):
                log.warning("LLMEmaFilter reconfigure: 알 수 없는 expire_mode='%s' — 무시", expire_mode)
            else:
                log.info(
                    "LLMEmaFilter expire_mode 변경 '%s' → '%s'",
                    self.expire_mode, expire_mode,
                )
                self.expire_mode = expire_mode
