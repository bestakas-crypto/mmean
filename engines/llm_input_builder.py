# engines/llm_input_builder.py
"""
LLM 입력 구성기 — 3레이어 입력 + 5원칙 시스템 프롬프트 + CoT 순서

레이어 구성:
  Layer 1: 외국인 순매수 시계열 원본 (최소 5구간) — LLM이 기울기를 읽도록
           ※ 원본 시계열은 Layer1에만 사용. 로그에는 압축 스냅샷만 기록.
  Layer 2: 계산된 흐름 지표 (delta추이, EMA5비교, flow_score, MA크로스)
  Layer 3: 시장 맥락 (OI, 베이시스, 기관수급, volatility_state ← 필수)

금지:
  - 단편 수치 단독 전달 금지
  - "오늘 매도세인데 어때?" 같은 유도 질문 금지
  - "반대 시나리오를 반드시 생성하라" 문구 금지 (억지 반론 유발)

5원칙은 모든 LLM 호출 시스템 프롬프트 최상단에 고정. 변경 금지.

CoT 추론 순서 강제:
  1. 현상 분석 — 데이터가 말하는 것
  2. 전제 검증 — 입력 해석이 데이터와 일치하는가
  3. 반대 가능성 — 틀릴 조건이 있는가. 없으면 "없음" 명시
  4. 최종 판단 — risk_view + weight_adjust

volatility_state 산출 (VolatilityClassifier):
  ATR 기반 (권장):
    atr_ratio = current_atr / baseline_atr (최근 ATR_PERIOD 평균)
    expanding:    atr_ratio >= 1.30
    contracting:  atr_ratio <= 0.70
    normal:       그 외

  가격 시계열 기반 (ATR 없을 때 대안):
    std_ratio = rolling_std(5) / rolling_std(20)
    expanding:    std_ratio >= 1.25
    contracting:  std_ratio <= 0.75
    normal:       그 외
"""
from __future__ import annotations

import logging
import math
from dataclasses import dataclass, field
from typing import Dict, List, Optional

log = logging.getLogger("MMEAN.LLMInputBuilder")

# ── 시스템 프롬프트 5원칙 (변경 금지) ─────────────────────────────
SYSTEM_PROMPT_5PRINCIPLES = """\
[판단 원칙 — 반드시 준수]
1. 사용자의 표현이나 암묵적 전제를 그대로 따르지 말고,
   입력 데이터 기준으로 검증하라.
2. 사용자의 해석이 데이터와 다를 경우,
   반드시 왜 다른지 이유와 사실을 명시하라.
3. 맞는 경우에는 맞다고 판단하되,
   확신의 근거를 함께 적시하라.
4. 입력 데이터가 불충분하면 단정하지 말고
   불확실성을 명시하라.
5. 출력은 방향성 동조가 아니라
   데이터 기반 판단이어야 한다.\
"""

# ── CoT 추론 순서 지시문 ──────────────────────────────────────────
COT_INSTRUCTION = """\
[추론 순서 — 반드시 아래 순서로]
1. 현상 분석    — 데이터가 말하는 것
2. 전제 검증    — 입력 해석이 데이터와 일치하는가
3. 반대 가능성  — 틀릴 조건이 있는가. 없으면 "없음" 명시
4. 최종 판단    — risk_view + weight_adjust\
"""

# ── 출력 형식 지시문 ──────────────────────────────────────────────
OUTPUT_FORMAT_INSTRUCTION = """\
[출력 형식 — JSON 외 출력 금지]
반드시 아래 JSON만 출력하라. 설명, 마크다운, 기타 텍스트 일체 금지.
{
  "risk_view":      "positive" | "neutral" | "negative",
  "confidence":     0.00 ~ 1.00,
  "reason_main":    "핵심 근거 1줄",
  "reason_against": "반대 조건 1줄 또는 없음",
  "weight_adjust":  -0.20 ~ +0.20
}
※ weight_adjust는 주문 사이즈 조정 전용이다.
   방향(LONG/SHORT) 또는 진입 여부 결정에 관여하지 않는다.\
"""


@dataclass
class MarketContext:
    """Layer 3 입력: 시장 맥락"""
    oi_trend:          str   # "증가" | "감소" | "횡보"
    basis_trend:       str   # "확대" | "축소" | "방향없음"
    institution_trend: str   # "외국인과 동행" | "외국인과 역행" | "중립"
    volatility_state:  str   # "expanding" | "contracting" | "normal"  ← 필수
    account_risk:      str   # "정상" | "주의" | "한도근접"

    def __post_init__(self) -> None:
        valid_vol = {"expanding", "contracting", "normal"}
        if self.volatility_state not in valid_vol:
            log.warning(
                "MarketContext: 알 수 없는 volatility_state='%s', 'normal'로 대체",
                self.volatility_state,
            )
            self.volatility_state = "normal"


@dataclass
class FlowMetrics:
    """Layer 2 입력: 계산된 흐름 지표"""
    delta_trend:   str    # 예: "매일 +500~700억 개선 중"
    ema5_relation: str    # 예: "현재값 EMA5 상회"
    flow_score:    float  # FlowEngine 출력
    ma_cross:      str    # 예: "단기 > 장기 진행 중"


# ── VolatilityClassifier ──────────────────────────────────────────
class VolatilityClassifier:
    """
    volatility_state 산출기.

    수식 1 (ATR 기반, 권장):
        atr_ratio = current_atr / baseline_atr
        baseline_atr = 최근 ATR_BASELINE_PERIOD개 ATR의 단순 평균
        expanding:    atr_ratio >= ATR_EXPAND_THR  (1.30)
        contracting:  atr_ratio <= ATR_CONTRACT_THR (0.70)
        normal:       나머지

    수식 2 (가격 시계열 직접 산출, ATR 없을 때 대안):
        std_ratio = rolling_std(short=5) / rolling_std(long=20)
        expanding:    std_ratio >= STD_EXPAND_THR  (1.25)
        contracting:  std_ratio <= STD_CONTRACT_THR (0.75)
        normal:       나머지

    임계값 근거:
        ATR 30% 초과 팽창 → 비정상 변동성 구간 (시장 충격 가능성)
        ATR 30% 초과 수축 → 과도한 정체 (조만간 급등락 선행 지표)
        표준편차 ±25% 기준은 ATR 기준과 경험적으로 유사 결과
    """

    # ── ATR 기반 임계값 ──────────────────────────────────────────
    ATR_EXPAND_THR:  float = 1.30
    ATR_CONTRACT_THR: float = 0.70

    # ── 표준편차 기반 임계값 ────────────────────────────────────
    STD_EXPAND_THR:  float = 1.25
    STD_CONTRACT_THR: float = 0.75
    STD_SHORT_WINDOW: int  = 5
    STD_LONG_WINDOW:  int  = 20

    @classmethod
    def from_atr(cls, current_atr: float, baseline_atr: float) -> str:
        """
        ATR 기반 volatility_state 산출.

        Args:
            current_atr:  최신 ATR (1봉)
            baseline_atr: 기준 ATR (최근 ATR_BASELINE_PERIOD 평균)

        Returns:
            "expanding" | "contracting" | "normal"
        """
        if baseline_atr <= 0 or current_atr < 0:
            log.warning(
                "VolatilityClassifier: ATR 비정상 (current=%.4f, baseline=%.4f) → normal",
                current_atr, baseline_atr,
            )
            return "normal"

        ratio = current_atr / baseline_atr
        if ratio >= cls.ATR_EXPAND_THR:
            log.debug(
                "VolatilityClassifier ATR: expanding | ratio=%.3f >= %.2f",
                ratio, cls.ATR_EXPAND_THR,
            )
            return "expanding"
        if ratio <= cls.ATR_CONTRACT_THR:
            log.debug(
                "VolatilityClassifier ATR: contracting | ratio=%.3f <= %.2f",
                ratio, cls.ATR_CONTRACT_THR,
            )
            return "contracting"
        return "normal"

    @classmethod
    def from_price_series(cls, price_series: List[float]) -> str:
        """
        가격 시계열에서 표준편차 비율로 volatility_state 산출.
        ATR 이력이 없을 때 대안으로 사용.

        Args:
            price_series: 가격 리스트 (최신값이 마지막), 최소 20개 권장

        Returns:
            "expanding" | "contracting" | "normal"
        """
        n = len(price_series)
        if n < cls.STD_LONG_WINDOW:
            log.debug(
                "VolatilityClassifier price_series: 데이터 부족 (%d개 < %d) → normal",
                n, cls.STD_LONG_WINDOW,
            )
            return "normal"

        std_short = _rolling_std(price_series, cls.STD_SHORT_WINDOW)
        std_long  = _rolling_std(price_series, cls.STD_LONG_WINDOW)

        if std_long <= 0:
            return "normal"

        ratio = std_short / std_long
        if ratio >= cls.STD_EXPAND_THR:
            log.debug(
                "VolatilityClassifier std: expanding | ratio=%.3f >= %.2f",
                ratio, cls.STD_EXPAND_THR,
            )
            return "expanding"
        if ratio <= cls.STD_CONTRACT_THR:
            log.debug(
                "VolatilityClassifier std: contracting | ratio=%.3f <= %.2f",
                ratio, cls.STD_CONTRACT_THR,
            )
            return "contracting"
        return "normal"


def _rolling_std(series: List[float], window: int) -> float:
    """최근 window개 표준편차 (모듈 내부 헬퍼)."""
    data = series[-window:] if len(series) >= window else series[:]
    if len(data) < 2:
        return 0.0
    n = len(data)
    mean = sum(data) / n
    variance = sum((x - mean) ** 2 for x in data) / (n - 1)
    return math.sqrt(variance)


# ── LLMInputBuilder ───────────────────────────────────────────────
class LLMInputBuilder:
    """
    3레이어 LLM 입력 페이로드를 구성.

    레이어 분리 원칙:
        Layer1 원본 시계열  → user_prompt에만 포함 (LLM에 전달)
        압축 스냅샷         → input_snapshot dict에만 포함 (로그에 기록)
        두 가지를 혼용하지 말 것 (단편 수치로 회귀 방지)

    사용 예:
        builder = LLMInputBuilder()
        system_prompt, user_prompt = builder.build(
            net_series=[-5200, -4800, -4100, -3600, -3000],
            metrics=FlowMetrics(...),
            context=MarketContext(...),
        )
        # 로그용 스냅샷 (user_prompt와 별도)
        snapshot = builder.build_snapshot(metrics, context)
    """

    def build(
        self,
        net_series: List[float],
        metrics: FlowMetrics,
        context: MarketContext,
    ) -> tuple[str, str]:
        """
        반환: (system_prompt, user_prompt)
        user_prompt에 Layer1 원본 시계열 포함.
        """
        system_prompt = self._build_system_prompt()
        user_prompt   = self._build_user_prompt(net_series, metrics, context)
        return system_prompt, user_prompt

    def build_snapshot(
        self,
        metrics: FlowMetrics,
        context: MarketContext,
    ) -> dict:
        """
        로그 기록용 압축 스냅샷.
        Layer1 원본 시계열은 포함하지 않음 (로그 용량 절약 + 분리 원칙).
        """
        return {
            "flow_score":        metrics.flow_score,
            "delta_trend":       metrics.delta_trend,
            "ema5_relation":     metrics.ema5_relation,
            "ma_cross":          metrics.ma_cross,
            "oi_trend":          context.oi_trend,
            "basis_trend":       context.basis_trend,
            "institution_trend": context.institution_trend,
            "volatility_state":  context.volatility_state,
            "account_risk":      context.account_risk,
        }

    # ------------------------------------------------------------------
    def _build_system_prompt(self) -> str:
        return "\n\n".join([
            SYSTEM_PROMPT_5PRINCIPLES,
            COT_INSTRUCTION,
            OUTPUT_FORMAT_INSTRUCTION,
        ])

    def _build_user_prompt(
        self,
        net_series: List[float],
        metrics: FlowMetrics,
        context: MarketContext,
    ) -> str:
        layer1 = self._layer1_series(net_series)
        layer2 = self._layer2_metrics(metrics)
        layer3 = self._layer3_context(context)

        return "\n\n".join([
            "=== Layer 1: 외국인 순매수 시계열 (최신순) ===",
            layer1,
            "=== Layer 2: 계산된 흐름 지표 ===",
            layer2,
            "=== Layer 3: 시장 맥락 ===",
            layer3,
            "위 데이터를 기반으로 판단하라.",
        ])

    def _layer1_series(self, net_series: List[float]) -> str:
        """
        최근 최대 10구간, 최소 5구간 시계열 나열.
        이 원본 시계열은 LLM user_prompt에만 포함.
        로그 스냅샷에는 포함하지 않음 (build_snapshot 참조).
        """
        series = net_series[-10:] if len(net_series) > 10 else net_series
        n = len(series)

        if n < 5:
            log.warning(
                "LLMInputBuilder Layer1: 시계열 %d개 (최소 5개 권장)", n
            )

        lines = []
        for i, val in enumerate(series):
            t_idx = n - 1 - i
            label = "T0" if t_idx == 0 else f"T-{t_idx}"
            lines.append(f"{label}: {val:+.0f}억")

        return "\n".join(lines)

    def _layer2_metrics(self, m: FlowMetrics) -> str:
        return (
            f"Delta 추이:   {m.delta_trend}\n"
            f"EMA5 대비:    {m.ema5_relation}\n"
            f"flow_score:   {m.flow_score:+.4f}\n"
            f"MA 크로스:    {m.ma_cross}"
        )

    def _layer3_context(self, c: MarketContext) -> str:
        return (
            f"선물 OI:           {c.oi_trend}\n"
            f"베이시스:          {c.basis_trend}\n"
            f"기관 수급:         {c.institution_trend}\n"
            f"volatility_state:  {c.volatility_state}\n"
            f"현재 계좌 리스크:  {c.account_risk}"
        )
