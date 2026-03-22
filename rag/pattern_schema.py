# MMEAN/pattern_schema.py
"""
패턴 문서화 Phase 1 — 공통 정의 모듈

내용:
  1. REGIME_TAGS       — regime_tags 어휘집 (태그명 → 설명)
  2. REGIME_TAG_RULES  — regime_ticks 컬럼값 → regime_tags 자동 분류 규칙
  3. PatternDocMeta    — 패턴 문서 공통 메타 스키마 (TypedDict)
  4. FAILURE_THRESHOLDS — failure_case 트리거 임계값
  5. CONFIDENCE_CRITERIA — confidence_level 기준 (sample 크기 기반)
  6. classify_regime_tags() — 틱 하나의 컬럼값 → regime_tags 리스트 계산

사용 예:
    from pattern_schema import classify_regime_tags, REGIME_TAGS
    tags = classify_regime_tags(row)  # row: dict-like (regime_ticks 1행)
"""
from __future__ import annotations

from typing import Any, Dict, List, Optional

# ────────────────────────────────────────────────────────────────────
# 1. REGIME_TAGS 어휘집
#
# 형식: {태그명: 설명}
# 태그명 규칙: 대문자 + 언더스코어, 계층은 "카테고리_속성"
# ────────────────────────────────────────────────────────────────────

REGIME_TAGS: Dict[str, str] = {

    # ── 방향성 태그 ────────────────────────────────────────────────
    "BULL_CLEAN":    "명확한 상승 레짐 — bias=LONG, long_score≥6, |long-short|≥3",
    "BULL_SOFT":     "약한 상승 레짐 — bias=LONG이나 score 차이 작음(1~2)",
    "BEAR_CLEAN":    "명확한 하락 레짐 — bias=SHORT, short_score≥6, |long-short|≥3",
    "BEAR_SOFT":     "약한 하락 레짐 — bias=SHORT이나 score 차이 작음(1~2)",
    "RANGE_TIGHT":   "횡보 레짐 — bias=NEUTRAL, score 차이 ≤1",
    "RANGE_NOISY":   "방향 혼재 레짐 — LONG/SHORT 교번 빈도 높음 또는 entry_signal 없음",

    # ── 변동성 태그 ────────────────────────────────────────────────
    "VOL_LOW":       "낮은 거래량 — volume_burst ≤ 0.7",
    "VOL_NORMAL":    "정상 거래량 — volume_burst 0.7~1.4",
    "VOL_HIGH":      "높은 거래량 — volume_burst 1.4~2.0",
    "VOL_SPIKE":     "거래량 급등 — volume_burst ≥ 2.0",

    # ── 베이시스 태그 ──────────────────────────────────────────────
    "BASIS_POSITIVE":    "베이시스 양수 — basis ≥ +0.10",
    "BASIS_NEGATIVE":    "베이시스 음수 — basis ≤ -0.10",
    "BASIS_FLAT":        "베이시스 중립 — |basis| < 0.10",
    "BASIS_EXPANDING":   "베이시스 확대 중 — basis_ema_delta > +0.02",
    "BASIS_COMPRESSING": "베이시스 수축 중 — basis_ema_delta < -0.02",

    # ── 외국인 수급 태그 ───────────────────────────────────────────
    "FOREIGN_BUY_PERSISTENT":   "외국인 지속 매수 — foreign_buy ≥ 50 (3틱 이상 유지 기준)",
    "FOREIGN_SELL_PERSISTENT":  "외국인 지속 매도 — foreign_buy ≤ -50 (3틱 이상 유지)",
    "FOREIGN_BUY_FADING":       "외국인 매수 약화 — foreign_buy 양수이나 감소 추세",
    "FOREIGN_SELL_FADING":      "외국인 매도 약화 — foreign_buy 음수이나 증가 추세",
    "FOREIGN_NEUTRAL":          "외국인 수급 중립 — |foreign_buy| < 20",

    # ── 플로우/신호 태그 ───────────────────────────────────────────
    "FLOW_LONG_BIAS":   "플로우 롱 편향 — flow_score ≥ 0.6 또는 flow_long_weight ≥ 0.6",
    "FLOW_SHORT_BIAS":  "플로우 숏 편향 — flow_score ≤ -0.6 또는 flow_short_weight ≥ 0.6",
    "FLOW_MIXED":       "플로우 혼재 — long/short weight 차이 < 0.2",

    # ── LLM 필터 태그 ─────────────────────────────────────────────
    "LLM_LONG_CONFIRMED":  "LLM 롱 확인 — llm_filter_valid=1, direction=LONG",
    "LLM_SHORT_CONFIRMED": "LLM 숏 확인 — llm_filter_valid=1, direction=SHORT",
    "LLM_NEUTRAL":         "LLM 중립/만료 — llm_filter_valid=0 또는 direction=NEUTRAL",

    # ── 신호 명확도 태그 ───────────────────────────────────────────
    "SIGNAL_CLEAR":  "신호 명확 — entry_signal in (LONG_READY, SHORT_READY), confidence≥0.6",
    "SIGNAL_MIXED":  "신호 혼재 — entry_signal 있으나 confidence 0.4~0.6",
    "SIGNAL_WEAK":   "신호 약함 — entry_signal 없거나 confidence < 0.4",

    # ── 세션 태그 ─────────────────────────────────────────────────
    "SESSION_OPENING": "장 초반 — 09:00~09:30",
    "SESSION_MID":     "장 중반 — 09:30~14:30",
    "SESSION_CLOSING": "장 마감 구간 — 14:30~15:20",
}


# ────────────────────────────────────────────────────────────────────
# 2. REGIME_TAG_RULES — 분류 임계값 상수
#    classify_regime_tags()에서 사용
# ────────────────────────────────────────────────────────────────────

_T = {
    # 방향성
    "score_strong":       6.0,   # BULL_CLEAN / BEAR_CLEAN 기준
    "score_gap_strong":   3.0,   # |long - short| ≥ 이 값이면 CLEAN
    "score_gap_soft":     1.0,   # |long - short| < 이 값이면 RANGE_TIGHT

    # 변동성
    "vol_low":    0.70,
    "vol_high":   1.40,
    "vol_spike":  2.00,

    # 베이시스
    "basis_pos":  0.10,
    "basis_neg": -0.10,
    "basis_expand":   0.02,   # basis_ema_delta ≥ 이 값이면 EXPANDING
    "basis_compress": -0.02,

    # 외국인
    "foreign_strong":  50.0,
    "foreign_neutral": 20.0,

    # 플로우
    "flow_bias":   0.60,
    "flow_gap":    0.20,

    # 신호 명확도
    "confidence_clear":  0.60,
    "confidence_mixed":  0.40,
}


# ────────────────────────────────────────────────────────────────────
# 3. PatternDocMeta — 패턴 문서 공통 메타 스키마
# ────────────────────────────────────────────────────────────────────

from typing import Literal

DocType = Literal[
    "pattern_case",        # 특정 시장 상황에서 효과적이었던 패턴
    "failure_case",        # 실패한 패턴 (재발 방지 교훈)
    "stable_config",       # 안정적으로 수렴한 레벨 설정 스냅샷
    "validation_summary",  # 특정 날짜/구간 검증 결과
    "stress_summary",      # 스트레스 테스트 결과
]

ConfidenceLevel = Literal["LOW", "MEDIUM", "HIGH"]


class PatternDocMeta:
    """
    패턴 문서 공통 메타 필드.
    JSON 직렬화 편의를 위해 TypedDict 대신 일반 클래스 + to_dict() 사용.

    필드:
        doc_type          문서 종류
        doc_id            고유 식별자 (자동 생성: "{doc_type}_{YYYYMMDD}_{level_band}")
        created_at        생성 시각 (ISO 8601)
        session_dates     근거 날짜 목록 (["2026-03-17", "2026-03-18"])
        regime_tags       적용 시장 태그 목록 (REGIME_TAGS 어휘집 기준)
        level_band_min    해당 문서 유효 레벨 최소값
        level_band_max    해당 문서 유효 레벨 최대값
        sample_size_dates 근거 날짜 수
        sample_size_trades 근거 거래 수
        confidence_level  신뢰도 (CONFIDENCE_CRITERIA 기준)
        summary_text      1층 자동 생성 요약 (규칙 기반)
        llm_summary       2층 LLM 보조 자연어 (선택, 비어 있을 수 있음)
        is_active         현재 유효 문서 여부 (False면 아카이브)
    """

    __slots__ = (
        "doc_type", "doc_id", "created_at",
        "session_dates", "regime_tags",
        "level_band_min", "level_band_max",
        "sample_size_dates", "sample_size_trades",
        "confidence_level",
        "summary_text", "llm_summary",
        "is_active",
    )

    def __init__(
        self,
        doc_type: DocType,
        doc_id: str,
        created_at: str,
        session_dates: List[str],
        regime_tags: List[str],
        level_band_min: int,
        level_band_max: int,
        sample_size_dates: int,
        sample_size_trades: int,
        summary_text: str,
        llm_summary: Optional[str] = None,
        is_active: bool = True,
    ) -> None:
        self.doc_type           = doc_type
        self.doc_id             = doc_id
        self.created_at         = created_at
        self.session_dates      = session_dates
        self.regime_tags        = regime_tags
        self.level_band_min     = level_band_min
        self.level_band_max     = level_band_max
        self.sample_size_dates  = sample_size_dates
        self.sample_size_trades = sample_size_trades
        self.summary_text       = summary_text
        self.llm_summary        = llm_summary
        self.is_active          = is_active
        self.confidence_level   = compute_confidence_level(
            sample_size_dates, sample_size_trades
        )

    def to_dict(self) -> Dict[str, Any]:
        return {
            "doc_type":           self.doc_type,
            "doc_id":             self.doc_id,
            "created_at":         self.created_at,
            "session_dates":      self.session_dates,
            "regime_tags":        self.regime_tags,
            "level_band_min":     self.level_band_min,
            "level_band_max":     self.level_band_max,
            "sample_size_dates":  self.sample_size_dates,
            "sample_size_trades": self.sample_size_trades,
            "confidence_level":   self.confidence_level,
            "summary_text":       self.summary_text,
            "llm_summary":        self.llm_summary,
            "is_active":          self.is_active,
        }

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> "PatternDocMeta":
        obj = cls.__new__(cls)
        for k in cls.__slots__:
            setattr(obj, k, d.get(k))
        return obj


# ────────────────────────────────────────────────────────────────────
# 4. FAILURE_THRESHOLDS — failure_case 트리거 임계값
#
# 하나라도 충족되면 failure_case 문서 생성 대상
# ────────────────────────────────────────────────────────────────────

FAILURE_THRESHOLDS: Dict[str, Any] = {
    # 스트레스 테스트 실패
    "stress_passed":            False,    # stress_passed = False이면 트리거

    # 수익성 붕괴
    "profit_factor_min":        1.0,      # profit_factor < 1.0
    "total_pnl_ticks_min":     -10.0,    # total_pnl_ticks < -10t

    # 최대 손실 허용치 (session-level 누적 peak-to-trough, 틱 단위)
    "max_drawdown_ticks_max":   100.0,    # max_drawdown > 100t (세션 누적 기준)

    # 목적함수 하한 (objective_score = f(profit_factor, sharpe, win_rate))
    "objective_score_min":      0.0,      # objective_score < 0 (음수이면 실패)

    # 동일 태그(regime_tags) 조합 반복 실패 기준
    "same_tag_failure_count":   2,        # 동일 태그 조합에서 2회 이상 실패 시

    # 표본 최소 요건 (샘플 너무 작으면 트리거 보류)
    "min_trades_to_trigger":    5,        # trade_count < 5이면 트리거 보류
}


# ────────────────────────────────────────────────────────────────────
# 5. CONFIDENCE_CRITERIA — confidence_level 기준
#
# sample_size_dates와 sample_size_trades 두 축 모두 충족해야 상위 등급
# 하나라도 미달이면 하위 등급 적용
# ────────────────────────────────────────────────────────────────────

CONFIDENCE_CRITERIA: Dict[str, Dict[str, int]] = {
    # HIGH: 데이터가 충분히 쌓인 경우
    "HIGH": {
        "min_dates":  10,   # 10거래일 이상
        "min_trades": 50,   # 50건 이상
    },
    # MEDIUM: 기본 통계 신뢰 가능 수준
    "MEDIUM": {
        "min_dates":  5,    # 5거래일 이상
        "min_trades": 20,   # 20건 이상
    },
    # LOW: 기준 미달 — 문서화는 하되 신뢰도 낮음
    "LOW": {
        "min_dates":  0,
        "min_trades": 0,
    },
}


def compute_confidence_level(
    sample_size_dates: int,
    sample_size_trades: int,
) -> ConfidenceLevel:
    """sample_size → confidence_level 계산."""
    d = sample_size_dates or 0
    t = sample_size_trades or 0
    hi = CONFIDENCE_CRITERIA["HIGH"]
    me = CONFIDENCE_CRITERIA["MEDIUM"]
    if d >= hi["min_dates"] and t >= hi["min_trades"]:
        return "HIGH"
    if d >= me["min_dates"] and t >= me["min_trades"]:
        return "MEDIUM"
    return "LOW"


# ────────────────────────────────────────────────────────────────────
# 6. classify_regime_tags() — 틱 컬럼값 → regime_tags 리스트
#
# 입력: dict-like 객체 (regime_ticks 1행 또는 집계 평균값 dict)
# 출력: List[str]  — REGIME_TAGS 어휘집에 정의된 태그들
# ────────────────────────────────────────────────────────────────────

def _f(v: Any, default: float = 0.0) -> float:
    """안전한 float 변환."""
    try:
        return float(v) if v is not None else default
    except (TypeError, ValueError):
        return default


def _s(v: Any, default: str = "") -> str:
    """안전한 str 변환."""
    return str(v) if v is not None else default


def classify_regime_tags(row: Dict[str, Any]) -> List[str]:
    """
    regime_ticks 1행 (또는 기간 평균 집계 dict)을 받아
    해당하는 REGIME_TAGS 태그 리스트를 반환.

    Parameters
    ----------
    row : dict
        키: regime_ticks 컬럼명 (bias, long_score, short_score,
            volume_burst, basis, basis_ema_delta, foreign_buy,
            flow_score, flow_long_weight, flow_short_weight,
            llm_filter_valid, llm_filter_direction,
            entry_signal, confidence, session_phase)

    Returns
    -------
    List[str] : REGIME_TAGS 어휘집 태그명 리스트 (중복 없음, 알파벳 정렬)
    """
    tags: List[str] = []
    t = _T

    bias        = _s(row.get("bias"), "NEUTRAL").upper()
    long_sc     = _f(row.get("long_score"))
    short_sc    = _f(row.get("short_score"))
    vol_burst   = _f(row.get("volume_burst"), 1.0)
    basis       = _f(row.get("basis"))
    basis_delta = _f(row.get("basis_ema_delta"))
    foreign_buy = _f(row.get("foreign_buy"))
    flow_score  = _f(row.get("flow_score"))
    flow_l      = _f(row.get("flow_long_weight"),  0.5)
    flow_s      = _f(row.get("flow_short_weight"), 0.5)
    llm_valid   = int(row.get("llm_filter_valid") or 0)
    llm_dir     = _s(row.get("llm_filter_direction"), "").upper()
    entry_sig   = _s(row.get("entry_signal"), "").upper()
    confidence  = _f(row.get("confidence"))
    phase       = _s(row.get("session_phase"), "").lower()

    score_gap = long_sc - short_sc

    # ── 방향성 태그 ──────────────────────────────────────────────
    if bias == "LONG":
        if long_sc >= t["score_strong"] and score_gap >= t["score_gap_strong"]:
            tags.append("BULL_CLEAN")
        else:
            tags.append("BULL_SOFT")
    elif bias == "SHORT":
        if short_sc >= t["score_strong"] and (-score_gap) >= t["score_gap_strong"]:
            tags.append("BEAR_CLEAN")
        else:
            tags.append("BEAR_SOFT")
    else:  # NEUTRAL
        if abs(score_gap) <= t["score_gap_soft"]:
            tags.append("RANGE_TIGHT")
        else:
            tags.append("RANGE_NOISY")

    # ── 변동성 태그 ──────────────────────────────────────────────
    if vol_burst >= t["vol_spike"]:
        tags.append("VOL_SPIKE")
    elif vol_burst >= t["vol_high"]:
        tags.append("VOL_HIGH")
    elif vol_burst <= t["vol_low"]:
        tags.append("VOL_LOW")
    else:
        tags.append("VOL_NORMAL")

    # ── 베이시스 태그 ────────────────────────────────────────────
    if basis >= t["basis_pos"]:
        tags.append("BASIS_POSITIVE")
    elif basis <= t["basis_neg"]:
        tags.append("BASIS_NEGATIVE")
    else:
        tags.append("BASIS_FLAT")

    if basis_delta >= t["basis_expand"]:
        tags.append("BASIS_EXPANDING")
    elif basis_delta <= t["basis_compress"]:
        tags.append("BASIS_COMPRESSING")

    # ── 외국인 수급 태그 ─────────────────────────────────────────
    if foreign_buy >= t["foreign_strong"]:
        tags.append("FOREIGN_BUY_PERSISTENT")
    elif foreign_buy <= -t["foreign_strong"]:
        tags.append("FOREIGN_SELL_PERSISTENT")
    elif abs(foreign_buy) < t["foreign_neutral"]:
        tags.append("FOREIGN_NEUTRAL")
    elif foreign_buy > 0:
        tags.append("FOREIGN_BUY_FADING")
    else:
        tags.append("FOREIGN_SELL_FADING")

    # ── 플로우 태그 ──────────────────────────────────────────────
    if flow_l - flow_s >= t["flow_gap"] or flow_score >= t["flow_bias"]:
        tags.append("FLOW_LONG_BIAS")
    elif flow_s - flow_l >= t["flow_gap"] or flow_score <= -t["flow_bias"]:
        tags.append("FLOW_SHORT_BIAS")
    else:
        tags.append("FLOW_MIXED")

    # ── LLM 필터 태그 ────────────────────────────────────────────
    if llm_valid == 1:
        if llm_dir == "LONG":
            tags.append("LLM_LONG_CONFIRMED")
        elif llm_dir == "SHORT":
            tags.append("LLM_SHORT_CONFIRMED")
        else:
            tags.append("LLM_NEUTRAL")
    else:
        tags.append("LLM_NEUTRAL")

    # ── 신호 명확도 태그 ─────────────────────────────────────────
    has_signal = entry_sig in ("LONG_READY", "SHORT_READY")
    if has_signal and confidence >= t["confidence_clear"]:
        tags.append("SIGNAL_CLEAR")
    elif has_signal and confidence >= t["confidence_mixed"]:
        tags.append("SIGNAL_MIXED")
    else:
        tags.append("SIGNAL_WEAK")

    # ── 세션 태그 ────────────────────────────────────────────────
    if phase == "opening":
        tags.append("SESSION_OPENING")
    elif phase == "closing":
        tags.append("SESSION_CLOSING")
    else:
        tags.append("SESSION_MID")

    return sorted(set(tags))


# ────────────────────────────────────────────────────────────────────
# 7. classify_session_tags() — 세션(하루) 단위 태그 집계
#
# 여러 틱의 태그를 집계해 해당 세션의 "대표 태그 셋" 산출
# ────────────────────────────────────────────────────────────────────

def classify_session_tags(
    rows: List[Dict[str, Any]],
    min_ratio: float = 0.40,
) -> List[str]:
    """
    여러 틱 rows의 태그 빈도를 집계해 대표 태그 셋 반환.

    Parameters
    ----------
    rows      : regime_ticks 행 리스트
    min_ratio : 전체 틱 대비 최소 출현 비율 (기본 40%)

    Returns
    -------
    List[str] : min_ratio 이상 출현한 태그 목록 (알파벳 정렬)
    """
    if not rows:
        return []

    from collections import Counter
    counter: Counter = Counter()
    for row in rows:
        for tag in classify_regime_tags(row):
            counter[tag] += 1

    total = len(rows)
    return sorted(
        tag for tag, cnt in counter.items()
        if cnt / total >= min_ratio
    )


# ────────────────────────────────────────────────────────────────────
# 8. check_failure_trigger() — failure_case 트리거 체크
# ────────────────────────────────────────────────────────────────────

def check_failure_trigger(
    profit_factor: Optional[float],
    total_pnl_ticks: Optional[float],
    max_drawdown_ticks: Optional[float],
    objective_score: Optional[float],
    trade_count: int,
    stress_passed: Optional[bool] = None,
) -> Dict[str, bool]:
    """
    FAILURE_THRESHOLDS 기준으로 각 트리거 조건 체크.

    Returns
    -------
    dict : {트리거명: True/False}  — True면 해당 조건 발동
    각 키: "triggered" (하나라도 발동), "stress", "profit_factor",
            "total_pnl", "drawdown", "objective"
    """
    thr = FAILURE_THRESHOLDS

    # 표본 부족이면 전체 보류
    if trade_count < thr["min_trades_to_trigger"]:
        return {k: False for k in
                ("triggered", "stress", "profit_factor", "total_pnl", "drawdown", "objective")}

    results = {
        "stress":        stress_passed is False,
        "profit_factor": profit_factor is not None and profit_factor < thr["profit_factor_min"],
        "total_pnl":     total_pnl_ticks is not None and total_pnl_ticks < thr["total_pnl_ticks_min"],
        "drawdown":      max_drawdown_ticks is not None and abs(max_drawdown_ticks) > thr["max_drawdown_ticks_max"],
        "objective":     objective_score is not None and objective_score < thr["objective_score_min"],
    }
    results["triggered"] = any(results.values())
    return results


# ────────────────────────────────────────────────────────────────────
# CLI — 어휘집 출력 및 테스트
# ────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    import sys

    if "--list-tags" in sys.argv:
        print(f"\n{'태그명':<30} 설명")
        print("─" * 80)
        for tag, desc in REGIME_TAGS.items():
            print(f"{tag:<30} {desc}")
        print(f"\n총 {len(REGIME_TAGS)}개 태그\n")

    elif "--test" in sys.argv:
        # 간단한 테스트 케이스
        test_row = {
            "bias": "LONG", "long_score": 7.0, "short_score": 3.0,
            "volume_burst": 1.6, "basis": 0.25, "basis_ema_delta": 0.03,
            "foreign_buy": 80.0,
            "flow_score": 0.7, "flow_long_weight": 0.7, "flow_short_weight": 0.3,
            "llm_filter_valid": 1, "llm_filter_direction": "LONG",
            "entry_signal": "LONG_READY", "confidence": 0.75,
            "session_phase": "mid",
        }
        tags = classify_regime_tags(test_row)
        print("\n테스트 틱 태그:")
        for t in tags:
            print(f"  {t:<30} {REGIME_TAGS[t]}")
        print()

        cl = compute_confidence_level(12, 65)
        print(f"confidence_level(12일, 65건) = {cl}")  # → HIGH
        cl = compute_confidence_level(3, 10)
        print(f"confidence_level( 3일, 10건) = {cl}")  # → LOW
        cl = compute_confidence_level(6, 25)
        print(f"confidence_level( 6일, 25건) = {cl}")  # → MEDIUM

        result = check_failure_trigger(
            profit_factor=0.8, total_pnl_ticks=-15.0,
            max_drawdown_ticks=20.0, objective_score=-0.5,
            trade_count=10
        )
        print(f"\nfailure_trigger 결과: {result}")

    else:
        print("사용법:")
        print("  python pattern_schema.py --list-tags   # 어휘집 출력")
        print("  python pattern_schema.py --test        # 테스트 실행")
