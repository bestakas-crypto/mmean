# engines/foreign_flow_engine.py
"""
FlowEngine — 외국인 순매수 시계열 → flow_score (-2.0 ~ +2.0)

개편 철학:
  - 절대치 바이너리 판단 금지 (net > 0 → LONG 등)
  - sign() 이산화 금지 — 강도 정보 보존
  - 연속 float 반환으로 모든 하위 레이어가 강도를 활용 가능하게 함

알고리즘 3요소:
  A. Delta      (가중 0.40) — 전일 대비 변화량 / 변동성 정규화
  B. EMA 모멘텀 (가중 0.35) — 현재값 vs EMA5 이격
  C. 다중 MA    (가중 0.25) — ma3·ma5·ma10·ma20 이격 가중합

초기 데이터 부족 구간:
  - ma20 미만 길이에서는 가용 길이 기준 단기 MA만 사용 (fallback)
  - fallback 발생 시 로그 기록 필수
  - fallback에서도 바이너리 판단으로 회귀 금지
"""
from __future__ import annotations

import logging
import math
from typing import List, Optional

log = logging.getLogger("MMEAN.FlowEngine")

# ── 상수 ───────────────────────────────────────────────────────────
MIN_SCALE       = 1_000.0       # 분모 0 방지 최솟값 (억 단위 입력 기준 1_000억)
FALLBACK_MIN    = 3             # 최소 데이터 길이 (3개 미만이면 flow_score=0.0 반환)
WEIGHT_DELTA    = 0.40
WEIGHT_EMA      = 0.35
WEIGHT_MA       = 0.25
FLOW_CLAMP      = 2.0           # 최종 결과 clamp 범위
COMP_CLAMP      = 1.0           # 각 요소 clamp 범위


def _clamp(value: float, lo: float = -COMP_CLAMP, hi: float = COMP_CLAMP) -> float:
    return max(lo, min(hi, value))


def _calc_ema(series: List[float], period: int) -> float:
    """단순 지수이동평균 — series 마지막 값이 최신."""
    if not series:
        return 0.0
    alpha = 2.0 / (period + 1)
    ema = series[0]
    for v in series[1:]:
        ema = alpha * v + (1 - alpha) * ema
    return ema


def _rolling_std(series: List[float], window: int = 20) -> float:
    """최근 window 개 표준편차. 데이터 부족 시 가용 길이 사용."""
    data = series[-window:] if len(series) >= window else series[:]
    if len(data) < 2:
        return 0.0
    n = len(data)
    mean = sum(data) / n
    variance = sum((x - mean) ** 2 for x in data) / (n - 1)
    return math.sqrt(variance)


def _safe_mean(series: List[float], n: int) -> Optional[float]:
    """최근 n개 평균. 데이터 부족 시 None."""
    if len(series) < n:
        return None
    return sum(series[-n:]) / n


class FlowEngine:
    """
    외국인 순매수 시계열을 연속 flow_score로 변환.

    사용 예:
        engine = FlowEngine()
        score  = engine.compute(net_series)   # net_series: 억 단위 list, 최신값이 마지막
    """

    def __init__(self, min_scale: float = MIN_SCALE) -> None:
        self.min_scale = min_scale

    # ------------------------------------------------------------------
    def compute(self, net_series: List[float]) -> float:
        """
        메인 진입점.
        net_series: 외국인 순매수 시계열 (최신값이 마지막).
        반환: flow_score float (-2.0 ~ +2.0)
        """
        n = len(net_series)

        if n < FALLBACK_MIN:
            log.warning(
                "FlowEngine fallback | 데이터 길이 부족 (%d개, 최소 %d개) | flow_score=0.0 반환",
                n, FALLBACK_MIN,
            )
            return 0.0

        is_fallback = n < 20
        if is_fallback:
            log.debug(
                "FlowEngine fallback | 데이터 길이 %d개 (ma20 미만) | 단기 MA만 사용",
                n,
            )

        today  = net_series[-1]
        std20  = _rolling_std(net_series, window=20)
        denom  = max(std20, self.min_scale)

        # ── A. Delta ──────────────────────────────────────────────
        if n >= 2:
            prev        = net_series[-2]
            delta       = today - prev
            delta_norm  = _clamp(delta / denom)
        else:
            delta_norm  = 0.0

        # ── B. EMA 모멘텀 ─────────────────────────────────────────
        ema_period = min(5, n)
        ema5       = _calc_ema(net_series[-ema_period:], period=ema_period)
        ema_val    = _clamp((today - ema5) / denom)

        # ── C. 다중 MA 이격 ───────────────────────────────────────
        ma_score = self._calc_ma_score(net_series, today, denom, is_fallback)

        # ── 최종 합산 ─────────────────────────────────────────────
        raw_score = (
            delta_norm * WEIGHT_DELTA
            + ema_val  * WEIGHT_EMA
            + ma_score * WEIGHT_MA
        )
        flow_score = _clamp(raw_score, -FLOW_CLAMP, FLOW_CLAMP)

        log.debug(
            "FlowEngine | n=%d | delta_norm=%.3f | ema_val=%.3f | ma_score=%.3f"
            " | flow_score=%.4f | fallback=%s",
            n, delta_norm, ema_val, ma_score, flow_score, is_fallback,
        )
        return flow_score

    # ------------------------------------------------------------------
    def _calc_ma_score(
        self,
        series: List[float],
        today: float,
        denom: float,
        is_fallback: bool,
    ) -> float:
        """
        다중 MA 이격 가중합.
        fallback 구간에서는 가용 길이에 맞는 단기 MA만 활성화.
        """
        configs = [
            (3,  0.4),
            (5,  0.3),
            (10, 0.2),
            (20, 0.1),
        ]

        total_score  = 0.0
        total_weight = 0.0

        for period, weight in configs:
            ma_val = _safe_mean(series, period)
            if ma_val is None:
                # 데이터 부족 — 이 MA는 건너뜀 (weight 재배분)
                continue
            diff  = _clamp((today - ma_val) / denom)
            total_score  += diff * weight
            total_weight += weight

        if total_weight == 0.0:
            return 0.0

        # 건너뛴 MA의 weight를 재배분 (비율 정규화)
        return total_score / total_weight
