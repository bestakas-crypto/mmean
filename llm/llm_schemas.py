# [Directory] MMEAN
# [File] llm_schemas.py
from __future__ import annotations

from typing import Any, Dict, Optional


def _to_int(v: Any, default: int) -> int:
    try:
        return int(v)
    except Exception:
        return default


def _to_float(v: Any, default: float) -> float:
    try:
        return float(v)
    except Exception:
        return default


def _to_str(v: Any, default: str = "") -> str:
    try:
        return str(v)
    except Exception:
        return default


class StrategySchema:
    """
    Layer 1 — 개장 전 전략 조정 스키마
    """

    VALID_MODES = {"aggressive", "normal", "conservative"}

    @classmethod
    def default(cls) -> Dict[str, Any]:
        # v1.1: 실패 시 baseline 유지가 원칙이므로 delta는 0
        return {
            "mode": "normal",
            "max_trades_per_day": 20,
            "daily_loss_limit": -1000000,
            "long_weight": 1.0,
            "short_weight": 1.0,
            "enter_score_adj": 0.0,
            "tp_ticks_adj": 0,
            "sl_ticks_adj": 0,
            "reason": "default_fallback",
        }

    @classmethod
    def validate_and_clamp(cls, data: Dict[str, Any]) -> Dict[str, Any]:
        src = dict(cls.default())
        if isinstance(data, dict):
            src.update(data)

        mode = _to_str(src.get("mode", "normal"), "normal").strip().lower()
        if mode not in cls.VALID_MODES:
            mode = "normal"

        max_trades_per_day = max(5, min(50, _to_int(src.get("max_trades_per_day"), 20)))

        daily_loss_limit = _to_int(src.get("daily_loss_limit"), -1000000)
        if daily_loss_limit >= 0:
            daily_loss_limit = -abs(daily_loss_limit or 1000000)

        long_weight = max(0.5, min(2.0, _to_float(src.get("long_weight"), 1.0)))
        short_weight = max(0.5, min(2.0, _to_float(src.get("short_weight"), 1.0)))
        enter_score_adj = max(-1.0, min(1.0, _to_float(src.get("enter_score_adj"), 0.0)))
        tp_ticks_adj = max(-4, min(4, _to_int(src.get("tp_ticks_adj"), 0)))
        sl_ticks_adj = max(-4, min(4, _to_int(src.get("sl_ticks_adj"), 0)))
        reason = _to_str(src.get("reason", ""), "").strip()[:200]

        return {
            "mode": mode,
            "max_trades_per_day": max_trades_per_day,
            "daily_loss_limit": daily_loss_limit,
            "long_weight": long_weight,
            "short_weight": short_weight,
            "enter_score_adj": enter_score_adj,
            "tp_ticks_adj": tp_ticks_adj,
            "sl_ticks_adj": sl_ticks_adj,
            "reason": reason,
        }


class OpportunitySchema:
    """
    Layer 2 — 장중 기회 필터 스키마

    [출력 원칙 — §6 준수]
    LLM은 반드시 아래 필드를 반환해야 함:
      - source: 변경 채널 ('llm_filter' 고정)
      - reason_code: 시장 상태 근거 코드 (허용 목록 기반)
      - changed_fields: 실제 변경된 파라미터 키 목록
      - patch: 변경 값 dict (없으면 빈 dict)
    손익/거래횟수 기반 reason_code는 ParamStore에서 기각됨.
    """

    VALID_DIRS = {"LONG", "SHORT", "NEUTRAL"}

    # §6 허용 근거 코드 — 시장 상태 변수 기반만 허용
    VALID_REASON_CODES = {
        "ema_align_up", "ema_align_down",
        "ema_slope_expand", "ema_slope_contract",
        "vwap_above", "vwap_below",
        "atr_expand", "atr_contract",
        "foreign_flow_long", "foreign_flow_short",
        "oi_buildup_long", "oi_buildup_short",
        "regime_shift", "basis_expand", "basis_contract",
        "volume_burst", "adx_strength", "default_fallback",
    }

    @classmethod
    def default(cls) -> Dict[str, Any]:
        # fail-safe: 보수적 중립값, 파라미터 변경 없음
        return {
            "opportunity_score": 0.0,
            "direction_bias": "NEUTRAL",
            "tp_ticks": None,
            "sl_ticks": None,
            "valid_minutes": 1,
            "reason": "default_fallback",
            # 파라미터 조정 출력 필드 (§6)
            "source": "llm_filter",
            "reason_code": "default_fallback",
            "changed_fields": [],
            "patch": {},
            "expected_effect": "",
        }

    @classmethod
    def validate_and_clamp(cls, data: Dict[str, Any]) -> Dict[str, Any]:
        src = dict(cls.default())
        if isinstance(data, dict):
            src.update(data)

        opportunity_score = max(0.0, min(1.0, _to_float(src.get("opportunity_score"), 0.0)))

        direction_bias = _to_str(src.get("direction_bias", "NEUTRAL"), "NEUTRAL").strip().upper()
        if direction_bias not in cls.VALID_DIRS:
            direction_bias = "NEUTRAL"

        tp_raw = src.get("tp_ticks")
        if tp_raw in (None, "", "null"):
            tp_ticks: Optional[int] = None
        else:
            tp_ticks = max(4, min(30, _to_int(tp_raw, 0)))
            if tp_ticks <= 0:
                tp_ticks = None

        sl_raw = src.get("sl_ticks")
        if sl_raw in (None, "", "null"):
            sl_ticks: Optional[int] = None
        else:
            sl_ticks = max(4, min(30, _to_int(sl_raw, 0)))
            if sl_ticks <= 0:
                sl_ticks = None

        valid_minutes = max(1, min(30, _to_int(src.get("valid_minutes"), 5)))
        reason = _to_str(src.get("reason", ""), "").strip()[:150]

        # ── §6 reason_code 검증 ────────────────────────────────────
        reason_code = _to_str(src.get("reason_code", ""), "").strip().lower()
        if reason_code not in cls.VALID_REASON_CODES:
            reason_code = "default_fallback"

        # source는 llm_filter 고정 (외부 변경 무시)
        source = "llm_filter"

        # changed_fields: 리스트 타입 보장, 허용 키만 통과
        _allowed_patch_keys = {
            "trend_weight_long", "trend_weight_short",
            "trend_ema_align_score", "trend_ema_slope_score",
            "trend_ema_slope_threshold",
        }
        raw_cf = src.get("changed_fields", [])
        changed_fields = [
            k for k in (raw_cf if isinstance(raw_cf, list) else [])
            if k in _allowed_patch_keys
        ]

        # patch: dict 타입 보장, 허용 키만 통과
        raw_patch = src.get("patch", {})
        patch = {
            k: v for k, v in (raw_patch.items() if isinstance(raw_patch, dict) else [])
            if k in _allowed_patch_keys
        }

        # changed_fields와 patch 불일치 보정
        # patch에 있는 키는 changed_fields에도 포함
        for k in patch:
            if k not in changed_fields:
                changed_fields.append(k)

        expected_effect = _to_str(src.get("expected_effect", ""), "").strip()[:200]

        return {
            "opportunity_score": opportunity_score,
            "direction_bias": direction_bias,
            "tp_ticks": tp_ticks,
            "sl_ticks": sl_ticks,
            "valid_minutes": valid_minutes,
            "reason": reason,
            # §6 파라미터 조정 출력 필드
            "source": source,
            "reason_code": reason_code,
            "changed_fields": changed_fields,
            "patch": patch,
            "expected_effect": expected_effect,
        }