# engines/llm_gate.py
"""
LLM Gate — LLM 호출 여부를 결정하는 복합 게이트

현재 활성 조건:
  [1] flow_score — |flow_score| >= 0.8 이면 강한 추세 → LLM 스킵

추후 확장 예정 조건 (GateInputs에 이미 필드 준비, 메서드 stub 존재):
  [2] basis_pct     — 베이시스 이상 구간 감지
  [3] oi_delta_pct  — OI 변화율 애매 구간 감지
  [4] volatility_state — expanding/contracting 전환 시점

확장 원칙:
  - 각 조건은 독립적 _check_*() 메서드로 구현
  - GateInputs에 필드 추가 → _check_*() 구현 → check() 내 조건 리스트에 append
  - 최종 논리: should_call = any(c.should_call for c in conditions)
    → 어떤 한 조건이라도 "전환 구간"으로 판정하면 LLM 호출
  - check() 시그니처 변경 없음 — 하위 호환 유지
"""
from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import List, Optional

log = logging.getLogger("MMEAN.LLMGate")

# ── 게이트 임계값 ─────────────────────────────────────────────────
FLOW_SCORE_GATE_THR = 0.8   # |flow_score| >= 이 값이면 LLM 스킵


@dataclass
class GateInputs:
    """
    LLM Gate 복합 입력.

    현재 활성 필드: flow_score
    확장 예정 필드: basis_pct / oi_delta_pct / volatility_state
      → None 이면 해당 조건 비활성 (현재 기본값)
    """
    flow_score:       float
    # ── 추후 확장 필드 (현재 비활성 — None 이면 건너뜀) ─────────────
    basis_pct:        Optional[float] = None   # 베이시스 (%, 예: 0.12)
    oi_delta_pct:     Optional[float] = None   # OI 변화율 (%, 예: -2.3)
    volatility_state: Optional[str]   = None   # "expanding"|"contracting"|"normal"


@dataclass
class GateConditionResult:
    """단일 게이트 조건의 판정 결과"""
    condition_name: str
    should_call:    bool    # True → 이 조건만으로 LLM 호출 필요
    reason:         str


@dataclass
class GateResult:
    should_call:  bool
    flow_score:   float
    reason:       str     # 최종 요약 (로그용)
    conditions:   List[GateConditionResult] = field(default_factory=list)


class LLMGate:
    """
    LLM 호출 여부를 결정하는 복합 게이트.

    확장 방법:
        1. GateInputs에 새 필드 추가
        2. _check_xxx() 메서드 구현
        3. check() 내 conditions.append(self._check_xxx(...)) 주석 해제

    최종 로직: should_call = any(c.should_call for c in conditions)
    """

    def __init__(self, flow_threshold: float = FLOW_SCORE_GATE_THR) -> None:
        self.flow_threshold = flow_threshold

    def check(self, inputs: GateInputs) -> GateResult:
        """
        GateInputs를 받아 LLM 호출 여부 결정.
        각 조건 독립 평가 → OR 합산.
        """
        conditions: List[GateConditionResult] = []

        # ── 조건 1: flow_score [활성] ──────────────────────────────
        conditions.append(self._check_flow_score(inputs.flow_score))

        # ── 조건 2: basis_pct [비활성 — 확장 준비] ─────────────────
        # if inputs.basis_pct is not None:
        #     conditions.append(self._check_basis(inputs.basis_pct))

        # ── 조건 3: oi_delta_pct [비활성 — 확장 준비] ──────────────
        # if inputs.oi_delta_pct is not None:
        #     conditions.append(self._check_oi(inputs.oi_delta_pct))

        # ── 조건 4: volatility_state [비활성 — 확장 준비] ──────────
        # if inputs.volatility_state is not None:
        #     conditions.append(self._check_volatility(inputs.volatility_state))

        # ── 최종 결정 ────────────────────────────────────────────
        final_should_call = any(c.should_call for c in conditions)
        final_reason      = " | ".join(c.reason for c in conditions)

        log.debug(
            "LLMGate %s | flow=%.3f | %s",
            "통과(LLM호출)" if final_should_call else "차단(스킵)",
            inputs.flow_score,
            final_reason,
        )

        return GateResult(
            should_call=final_should_call,
            flow_score=inputs.flow_score,
            reason=final_reason,
            conditions=conditions,
        )

    # ------------------------------------------------------------------
    # 활성 조건
    # ------------------------------------------------------------------

    def _check_flow_score(self, flow_score: float) -> GateConditionResult:
        """
        조건 1: |flow_score| 기반.
        강한 추세 (>= threshold) → should_call=False
        전환 구간 (<  threshold) → should_call=True
        """
        abs_score = abs(flow_score)
        if abs_score >= self.flow_threshold:
            return GateConditionResult(
                condition_name="flow_score",
                should_call=False,
                reason=f"강한추세 |flow|={abs_score:.3f}≥{self.flow_threshold:.1f}→스킵",
            )
        return GateConditionResult(
            condition_name="flow_score",
            should_call=True,
            reason=f"전환구간 |flow|={abs_score:.3f}<{self.flow_threshold:.1f}→호출",
        )

    # ------------------------------------------------------------------
    # 비활성 조건 (stub — 활성화 시 위 conditions.append 주석 해제)
    # ------------------------------------------------------------------

    # def _check_basis(self, basis_pct: float) -> GateConditionResult:
    #     """
    #     조건 2: 베이시스.
    #     |basis_pct| < 0.05% → 중립 구간 → LLM 호출
    #     |basis_pct| >= 0.20% → 방향 명확 → LLM 스킵
    #     그 사이 → LLM 호출 (애매 구간)
    #     임계값은 실측 후 조정.
    #     """
    #     BASIS_CLEAR_THR = 0.20
    #     abs_basis = abs(basis_pct)
    #     if abs_basis >= BASIS_CLEAR_THR:
    #         return GateConditionResult(
    #             condition_name="basis",
    #             should_call=False,
    #             reason=f"베이시스 방향명확 |basis|={abs_basis:.3f}%≥{BASIS_CLEAR_THR}→스킵",
    #         )
    #     return GateConditionResult(
    #         condition_name="basis",
    #         should_call=True,
    #         reason=f"베이시스 애매구간 |basis|={abs_basis:.3f}%<{BASIS_CLEAR_THR}→호출",
    #     )

    # def _check_oi(self, oi_delta_pct: float) -> GateConditionResult:
    #     """
    #     조건 3: OI 변화율.
    #     |oi_delta_pct| > 3% → 큰 포지션 변동 → LLM 호출
    #     그 외 → 기본 스킵
    #     """
    #     OI_SIGNAL_THR = 3.0
    #     abs_oi = abs(oi_delta_pct)
    #     if abs_oi > OI_SIGNAL_THR:
    #         return GateConditionResult(
    #             condition_name="oi_delta",
    #             should_call=True,
    #             reason=f"OI 변동 큼 |oi_delta|={abs_oi:.1f}%>{OI_SIGNAL_THR}→호출",
    #         )
    #     return GateConditionResult(
    #         condition_name="oi_delta",
    #         should_call=False,
    #         reason=f"OI 변동 무시 |oi_delta|={abs_oi:.1f}%≤{OI_SIGNAL_THR}→스킵",
    #     )

    # def _check_volatility(self, volatility_state: str) -> GateConditionResult:
    #     """
    #     조건 4: volatility_state.
    #     expanding 또는 contracting → 전환 국면 → LLM 호출
    #     normal → 스킵
    #     """
    #     if volatility_state in ("expanding", "contracting"):
    #         return GateConditionResult(
    #             condition_name="volatility",
    #             should_call=True,
    #             reason=f"변동성 전환 state={volatility_state}→호출",
    #         )
    #     return GateConditionResult(
    #         condition_name="volatility",
    #         should_call=False,
    #         reason=f"변동성 정상 state={volatility_state}→스킵",
    #     )
