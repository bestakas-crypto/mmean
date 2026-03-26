# engines/regime_engine.py
"""
RegimeEngine — flow_score → 레짐 라벨 + 주문 가중치

개편 철학:
  - 레짐 라벨은 모니터링/로깅 전용 — 주문 가중치 계산에 사용 금지
  - 주문 가중치는 sigmoid 연속 함수로만 계산 (하드컷 금지)
  - long_weight / short_weight 둘 다 절대 0.0 금지 (최솟값 0.05 보장)
  - 경계값 진동 방지: 라벨 기반 분기 없음
"""
from __future__ import annotations

import logging
import math
from dataclasses import dataclass

log = logging.getLogger("MMEAN.RegimeEngine")

# ── 레짐 라벨 임계값 ───────────────────────────────────────────────
LABEL_STRONG_LONG_THR  =  1.2
LABEL_WEAK_LONG_THR    =  0.4
LABEL_WEAK_SHORT_THR   = -0.4
LABEL_STRONG_SHORT_THR = -1.2

# ── sigmoid 스케일 파라미터 ────────────────────────────────────────
DEFAULT_K       = 1.5       # 튜닝 파라미터 (초기값)
MIN_WEIGHT      = 0.05      # 가중치 최솟값 하한 (0.0 완전차단 금지)


def sigmoid(x: float) -> float:
    """표준 sigmoid: 1 / (1 + exp(-x))"""
    # 오버플로 방지
    if x >= 0:
        return 1.0 / (1.0 + math.exp(-x))
    else:
        exp_x = math.exp(x)
        return exp_x / (1.0 + exp_x)


def regime_label(flow_score: float) -> str:
    """
    레짐 라벨 반환 — 모니터링·로깅·알림 전용.
    ⚠ 이 라벨을 주문 가중치 계산에 사용하면 안 된다.
    """
    if flow_score >= LABEL_STRONG_LONG_THR:
        return "STRONG_LONG"
    elif flow_score >= LABEL_WEAK_LONG_THR:
        return "WEAK_LONG"
    elif flow_score >= LABEL_WEAK_SHORT_THR:
        return "NEUTRAL"
    elif flow_score >= LABEL_STRONG_SHORT_THR:
        return "WEAK_SHORT"
    else:
        return "STRONG_SHORT"


@dataclass
class RegimeResult:
    flow_score:   float
    label:        str       # 모니터링 전용
    long_weight:  float     # 주문 가중치 (>= MIN_WEIGHT)
    short_weight: float     # 주문 가중치 (>= MIN_WEIGHT)
    k:            float     # 사용된 sigmoid 스케일


class RegimeEngine:
    """
    flow_score를 받아 레짐 라벨과 주문 가중치를 반환.

    사용 예:
        engine = RegimeEngine()
        result = engine.compute(flow_score)
        # result.long_weight, result.short_weight → 연속값, 0.05 이상 보장
    """

    def __init__(self, k: float = DEFAULT_K) -> None:
        self.k = k

    def compute(self, flow_score: float) -> RegimeResult:
        """
        flow_score → RegimeResult.

        ⚠ 주문 가중치 계산은 sigmoid 연속 함수만 사용.
           레짐 라벨 기반 분기 없음.
        """
        label = regime_label(flow_score)

        # ── 연속 sigmoid 가중치 ──────────────────────────────────
        long_weight  = sigmoid( flow_score * self.k)
        short_weight = sigmoid(-flow_score * self.k)

        # ── 최솟값 하한: 0.0 완전차단 절대 금지 ──────────────────
        long_weight  = max(long_weight,  MIN_WEIGHT)
        short_weight = max(short_weight, MIN_WEIGHT)

        result = RegimeResult(
            flow_score=flow_score,
            label=label,
            long_weight=long_weight,
            short_weight=short_weight,
            k=self.k,
        )

        log.debug(
            "RegimeEngine | flow_score=%.4f | label=%s"
            " | long_w=%.4f | short_w=%.4f | k=%.2f",
            flow_score, label, long_weight, short_weight, self.k,
        )
        return result
