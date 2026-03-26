# engines/position_engine.py
"""
PositionEngine — 최종 주문 사이즈 계산

최종 공식:
  order_size = base_size
               × regime_weight          (FlowEngine 연속값)
               × (1 + weight_adjust_filtered)  (LLM 조정 ±20% 한도)

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
weight_adjust 사이즈 전용 원칙 (변경 금지)
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
  ✔ weight_adjust는 주문 사이즈(크기)만 조정한다.
  ✗ 방향(LONG/SHORT/direction) 결정에 관여 금지
  ✗ 진입 여부(주문 차단/허용) 결정에 관여 금지
  ✗ 청산 판단에 관여 금지

  근거:
    LLM이 레짐·방향을 뒤집는 엔진으로 변질되면
    BiasRegimeEngine의 구조적 판단이 단건 LLM 해석에 의해 무너진다.
    LLM은 진입 확신도를 사이즈로만 표현해야 한다.

  코드 보호:
    - direction 파라미터는 로깅·검증 전용, 사이즈 계산에 영향 없음
    - weight_adjust > 0 이어도 direction을 바꾸지 않음
    - weight_adjust = -1.0 (하한 초과)이어도 order_size는 0이 되지 않음
      (PositionEngine이 order_size=0으로 만들 수 없음 — 진입 차단은 상위 엔진이 담당)
    - MIN_ORDER_SIZE_RATIO 이하 시 경고만 기록, 차단하지 않음

코드 리뷰 기준 (라벨-실행 분리):
  PositionEngine 내부에 regime_label(STRONG_LONG 등) 기반 if문 추가 금지.
  regime_weight (float) 만 사용.
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
"""
from __future__ import annotations

import logging
from dataclasses import dataclass

log = logging.getLogger("MMEAN.PositionEngine")

# ── weight_adjust 한도 ────────────────────────────────────────────
MAX_WEIGHT_ADJUST  =  0.20
MIN_WEIGHT_ADJUST  = -0.20

# ── 경고 임계 (차단 아님) ─────────────────────────────────────────
MIN_ORDER_SIZE_RATIO = 0.05   # order_size / base_size < 5% 시 경고만


@dataclass
class OrderResult:
    base_size:              float
    regime_weight:          float
    weight_adjust_filtered: float   # 실제 적용된 weight_adjust (clamp 후)
    order_size:             float   # 최종 주문 사이즈
    direction:              int     # +1: LONG, -1: SHORT, 0: 없음 (로깅 전용)
    size_before_llm:        float   # base_size × regime_weight (LLM 개입 전)


class PositionEngine:
    """
    최종 주문 사이즈 결정.

    weight_adjust는 사이즈 조정 전용.
    direction은 이 클래스에서 결정하거나 변경하지 않음 (로깅 전용 입력).

    사용 예:
        engine = PositionEngine()
        result = engine.compute(
            base_size=1,
            regime_weight=0.72,           # RegimeEngine.long_weight
            weight_adjust_filtered=0.10,  # LLMEmaFilter.update() 출력
            direction=1,                  # +1: LONG (PositionEngine이 결정하지 않음)
        )
        print(result.order_size)  # 예: 0.792
    """

    def compute(
        self,
        base_size:              float,
        regime_weight:          float,
        weight_adjust_filtered: float,
        direction:              int,
    ) -> OrderResult:
        """
        최종 주문 사이즈 계산.

        Args:
            base_size:              기본 주문 단위 (예: 계약수 1)
            regime_weight:          FlowEngine/RegimeEngine의 연속 가중치
            weight_adjust_filtered: LLM EMA 필터 출력 (-0.20 ~ +0.20)
            direction:              +1 (LONG) or -1 (SHORT) — 로깅 전용, 사이즈 계산 무관

        PositionEngine이 direction을 바꾸거나 주문을 차단하지 않는다.
        """
        # weight_adjust 범위 강제 clamp
        wa = max(MIN_WEIGHT_ADJUST, min(MAX_WEIGHT_ADJUST, weight_adjust_filtered))

        # ── 사이즈 계산 (방향 무관) ───────────────────────────────
        size_before_llm = base_size * regime_weight
        order_size      = size_before_llm * (1.0 + wa)

        # ── 경고 (차단 아님 — 진입 여부는 상위 엔진이 결정) ────────
        if base_size > 0 and (order_size / base_size) < MIN_ORDER_SIZE_RATIO:
            log.warning(
                "PositionEngine | order_size/base_size=%.3f < %.2f (경고만, 차단 안 함)"
                " | regime_weight=%.4f wa=%+.4f",
                order_size / base_size, MIN_ORDER_SIZE_RATIO,
                regime_weight, wa,
            )

        result = OrderResult(
            base_size=base_size,
            regime_weight=regime_weight,
            weight_adjust_filtered=wa,
            order_size=order_size,
            direction=direction,
            size_before_llm=size_before_llm,
        )

        log.debug(
            "PositionEngine | dir=%+d(로깅전용) | base=%.2f × regime_w=%.4f"
            " = %.4f | LLM×(1%+.2f) → order_size=%.4f",
            direction, base_size, regime_weight,
            size_before_llm, wa, order_size,
        )
        return result

    # ------------------------------------------------------------------
    @staticmethod
    def zero_order(base_size: float = 0.0) -> OrderResult:
        """주문 없음 — 신호 없을 때. PositionEngine 외부에서만 호출."""
        return OrderResult(
            base_size=base_size,
            regime_weight=0.0,
            weight_adjust_filtered=0.0,
            order_size=0.0,
            direction=0,
            size_before_llm=0.0,
        )
