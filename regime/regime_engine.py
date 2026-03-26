# [Directory] MMEAN
# [File] regime_engine.py
from __future__ import annotations

from collections import deque
from dataclasses import asdict, dataclass
from typing import Deque, Dict, List


BIAS_LONG = "LONG_BIAS"
BIAS_SHORT = "SHORT_BIAS"
BIAS_NEUTRAL = "NEUTRAL"

ENTRY_LONG = "LONG_READY"
ENTRY_SHORT = "SHORT_READY"
ENTRY_WAIT = "WAIT"


@dataclass
class BiasInputs:
    foreign_buy: float
    oi_delta: float
    basis: float
    basis_ema: float
    basis_ema_delta: float
    basis_slope: float
    volume_burst: float
    trade_strength: float = 100.0   # 체결강도 (100=중립, >100=매수우위, <100=매도우위)
    # EMA 추세 점수 입력 (analyzer_app에서 계산 후 주입)
    ema_fast: float = 0.0           # 단기 EMA (예: EMA20)
    ema_slow: float = 0.0           # 장기 EMA (예: EMA60)
    ema_fast_slope: float = 0.0     # 단기 EMA slope (n틱 변화량)


@dataclass
class ThresholdConfig:
    foreign_strong: float
    foreign_mid: float
    foreign_weak: float
    oi_strong: float
    oi_mid: float
    oi_negative_strong: float
    oi_negative_mid: float
    basis_strong: float
    basis_mid: float
    basis_weak: float
    ema_delta_strong: float
    slope_long_ready: float
    slope_short_ready: float
    volume_burst_ready: float
    enter_score: float
    enter_gap: float
    enter_confirm_count: int
    hold_score: float
    hold_gap: float
    exit_fail_count: int
    entry_confirm_count: int
    trade_strength_strong: float  # 강한 매수/매도 기준
    trade_strength_mid: float
    # EMA 추세 점수 파라미터
    trend_ema_align_score: float = 1.5
    trend_ema_slope_score: float = 0.5
    trend_ema_slope_threshold: float = 0.02
    trend_weight_long: float = 0.8
    trend_weight_short: float = 0.6


@dataclass
class BiasSnapshot:
    mode: str
    bias: str
    entry_signal: str
    confidence: float
    long_score: float
    short_score: float
    enter_long_streak: int
    enter_short_streak: int
    neutral_streak: int
    entry_long_streak: int
    entry_short_streak: int
    foreign_buy: float
    oi_delta: float
    basis: float
    basis_ema: float
    basis_ema_delta: float
    basis_slope: float
    volume_burst: float
    trade_strength: float
    reason: str
    # EMA 추세 점수 (사후 분석용)
    trend_long_score: float = 0.0
    trend_short_score: float = 0.0
    trend_weight_long: float = 0.8
    trend_weight_short: float = 0.6
    ema_fast: float = 0.0
    ema_slow: float = 0.0
    ema_fast_slope: float = 0.0


def get_threshold_config(mode: str) -> ThresholdConfig:
    mode = (mode or "smooth").lower()

    if mode == "tight":
        return ThresholdConfig(
            foreign_strong=250.0,
            foreign_mid=120.0,
            foreign_weak=40.0,
            oi_strong=60.0,
            oi_mid=25.0,
            oi_negative_strong=-60.0,
            oi_negative_mid=-25.0,
            basis_strong=0.25,
            basis_mid=0.12,
            basis_weak=0.03,
            ema_delta_strong=0.010,
            slope_long_ready=0.008,
            slope_short_ready=-0.008,
            volume_burst_ready=1.35,
            enter_score=4.0,
            enter_gap=1.0,
            enter_confirm_count=2,
            hold_score=3.0,
            hold_gap=0.5,
            exit_fail_count=1,
            entry_confirm_count=2,
            trade_strength_strong=120.0,
            trade_strength_mid=110.0,
            trend_ema_align_score=1.5,
            trend_ema_slope_score=0.5,
            trend_ema_slope_threshold=0.02,
            trend_weight_long=0.8,
            trend_weight_short=0.6,
        )

    return ThresholdConfig(
        foreign_strong=350.0,
        foreign_mid=180.0,
        foreign_weak=50.0,
        oi_strong=80.0,
        oi_mid=35.0,
        oi_negative_strong=-80.0,
        oi_negative_mid=-35.0,
        basis_strong=0.35,
        basis_mid=0.18,
        basis_weak=0.05,
        ema_delta_strong=0.015,
        slope_long_ready=0.012,
        slope_short_ready=-0.012,
        volume_burst_ready=1.60,
        enter_score=5.0,
        enter_gap=1.5,
        enter_confirm_count=3,
        hold_score=4.0,
        hold_gap=1.0,
        exit_fail_count=2,
        entry_confirm_count=2,
        trade_strength_strong=125.0,
        trade_strength_mid=112.0,
        trend_ema_align_score=1.5,
        trend_ema_slope_score=0.5,
        trend_ema_slope_threshold=0.02,
        trend_weight_long=0.8,
        trend_weight_short=0.6,
    )


class BiasRegimeEngine:
    def __init__(self, mode: str = "smooth", history_limit: int = 300):
        self.mode = (mode or "smooth").lower()
        self.cfg = get_threshold_config(self.mode)
        self.history: Deque[Dict] = deque(maxlen=history_limit)
        self.reset_state(clear_history=False)

    def set_mode(self, mode: str) -> None:
        mode = (mode or "smooth").lower()
        if mode not in ("smooth", "tight") or mode == self.mode:
            return
        self.mode = mode
        self.cfg = get_threshold_config(mode)
        self.reset_state(clear_history=True)

    def reset_state(self, clear_history: bool = True) -> None:
        self.current_bias = BIAS_NEUTRAL
        self.enter_long_streak = 0
        self.enter_short_streak = 0
        self.neutral_streak = 0
        self.long_hold_fail_streak = 0
        self.short_hold_fail_streak = 0
        self.entry_long_streak = 0
        self.entry_short_streak = 0
        if clear_history:
            self.history.clear()

    def _score_foreign(self, foreign_buy: float) -> Dict[str, float]:
        c = self.cfg
        long_score = 0.0
        short_score = 0.0

        if foreign_buy >= c.foreign_strong:
            long_score += 3.0
        elif foreign_buy >= c.foreign_mid:
            long_score += 2.0
        elif foreign_buy >= c.foreign_weak:
            long_score += 1.0

        if foreign_buy <= -c.foreign_strong:
            short_score += 3.0
        elif foreign_buy <= -c.foreign_mid:
            short_score += 2.0
        elif foreign_buy <= -c.foreign_weak:
            short_score += 1.0

        return {"long": long_score, "short": short_score}

    def _score_basis(self, basis: float) -> Dict[str, float]:
        c = self.cfg
        long_score = 0.0
        short_score = 0.0

        if basis >= c.basis_strong:
            long_score += 3.0
        elif basis >= c.basis_mid:
            long_score += 2.0
        elif basis >= c.basis_weak:
            long_score += 1.0

        if basis <= -c.basis_strong:
            short_score += 3.0
        elif basis <= -c.basis_mid:
            short_score += 2.0
        elif basis <= -c.basis_weak:
            short_score += 1.0

        return {"long": long_score, "short": short_score}

    def _score_basis_ema_delta(self, basis_ema_delta: float) -> Dict[str, float]:
        c = self.cfg
        if basis_ema_delta > c.ema_delta_strong:
            return {"long": 1.5, "short": 0.0}
        if basis_ema_delta < -c.ema_delta_strong:
            return {"long": 0.0, "short": 1.5}
        return {"long": 0.0, "short": 0.0}

    def _score_oi_confirm(self, foreign_buy: float, oi_delta: float) -> Dict[str, float]:
        c = self.cfg
        long_score = 0.0
        short_score = 0.0

        if oi_delta >= c.oi_strong:
            if foreign_buy > 0:
                long_score += 1.5
            elif foreign_buy < 0:
                short_score += 1.5
        elif oi_delta >= c.oi_mid:
            if foreign_buy > 0:
                long_score += 1.0
            elif foreign_buy < 0:
                short_score += 1.0
        elif oi_delta <= c.oi_negative_strong:
            long_score -= 0.5
            short_score -= 0.5
        elif oi_delta <= c.oi_negative_mid:
            long_score -= 0.25
            short_score -= 0.25

        return {"long": long_score, "short": short_score}

    def _score_trade_strength(self, ts: float) -> Dict[str, float]:
        """체결강도: >strong → long+1, >mid → long+0.5, <100-strong → short+1"""
        c = self.cfg
        neutral = 100.0
        if ts >= neutral + (c.trade_strength_strong - neutral):
            return {"long": 1.0, "short": 0.0}
        if ts >= neutral + (c.trade_strength_mid - neutral):
            return {"long": 0.5, "short": 0.0}
        if ts <= neutral - (c.trade_strength_strong - neutral):
            return {"long": 0.0, "short": 1.0}
        if ts <= neutral - (c.trade_strength_mid - neutral):
            return {"long": 0.0, "short": 0.5}
        return {"long": 0.0, "short": 0.0}

    def _score_trend(self, inp: BiasInputs) -> Dict[str, float]:
        """
        EMA 정배열/역배열 + slope 기반 추세 원점수 계산.
        가중치(trend_weight_long/short) 적용 후 반환.
        ema_fast=0이면 추세 점수 비활성(0 반환).
        """
        c = self.cfg
        if inp.ema_fast == 0.0 and inp.ema_slow == 0.0:
            return {"long": 0.0, "short": 0.0,
                    "raw_long": 0.0, "raw_short": 0.0}

        raw_long = 0.0
        raw_short = 0.0

        # EMA 정배열 / 역배열
        if inp.ema_fast > inp.ema_slow:
            raw_long += c.trend_ema_align_score
        elif inp.ema_fast < inp.ema_slow:
            raw_short += c.trend_ema_align_score

        # EMA slope 보조 점수
        thr = c.trend_ema_slope_threshold
        if inp.ema_fast_slope > thr:
            raw_long += c.trend_ema_slope_score
        elif inp.ema_fast_slope < -thr:
            raw_short += c.trend_ema_slope_score

        # 가중치 적용
        trend_long  = raw_long  * c.trend_weight_long
        trend_short = raw_short * c.trend_weight_short

        return {
            "long":      round(trend_long,  3),
            "short":     round(trend_short, 3),
            "raw_long":  round(raw_long,    3),
            "raw_short": round(raw_short,   3),
        }

    def _compute_scores(self, inp: BiasInputs) -> Dict[str, float]:
        fs = self._score_foreign(inp.foreign_buy)
        bs = self._score_basis(inp.basis)
        es = self._score_basis_ema_delta(inp.basis_ema_delta)
        os = self._score_oi_confirm(inp.foreign_buy, inp.oi_delta)
        ts = self._score_trade_strength(inp.trade_strength)
        tr = self._score_trend(inp)

        long_score  = fs["long"]  + bs["long"]  + es["long"]  + os["long"]  + ts["long"]  + tr["long"]
        short_score = fs["short"] + bs["short"] + es["short"] + os["short"] + ts["short"] + tr["short"]

        return {
            "long_score":       round(long_score,  2),
            "short_score":      round(short_score, 2),
            "trend_long_score": tr["long"],
            "trend_short_score":tr["short"],
        }

    def _update_bias_streaks(self, long_score: float, short_score: float) -> None:
        c = self.cfg

        long_enter_ok = long_score >= c.enter_score and (long_score - short_score) >= c.enter_gap
        short_enter_ok = short_score >= c.enter_score and (short_score - long_score) >= c.enter_gap
        neutral_ok = abs(long_score - short_score) <= 1.0 or (
            long_score < c.hold_score and short_score < c.hold_score
        )

        self.enter_long_streak = self.enter_long_streak + 1 if long_enter_ok else 0
        self.enter_short_streak = self.enter_short_streak + 1 if short_enter_ok else 0
        self.neutral_streak = self.neutral_streak + 1 if neutral_ok else 0

    def _decide_bias(self, long_score: float, short_score: float) -> str:
        c = self.cfg
        prev_bias = self.current_bias
        self._update_bias_streaks(long_score, short_score)

        if prev_bias == BIAS_LONG:
            hold_ok = long_score >= c.hold_score and (long_score - short_score) >= c.hold_gap
            if hold_ok:
                self.long_hold_fail_streak = 0
                return BIAS_LONG

            self.long_hold_fail_streak += 1
            if self.enter_short_streak >= c.enter_confirm_count:
                self.long_hold_fail_streak = 0
                return BIAS_SHORT
            if self.long_hold_fail_streak >= c.exit_fail_count or self.neutral_streak >= c.exit_fail_count:
                self.long_hold_fail_streak = 0
                return BIAS_NEUTRAL
            return BIAS_LONG

        if prev_bias == BIAS_SHORT:
            hold_ok = short_score >= c.hold_score and (short_score - long_score) >= c.hold_gap
            if hold_ok:
                self.short_hold_fail_streak = 0
                return BIAS_SHORT

            self.short_hold_fail_streak += 1
            if self.enter_long_streak >= c.enter_confirm_count:
                self.short_hold_fail_streak = 0
                return BIAS_LONG
            if self.short_hold_fail_streak >= c.exit_fail_count or self.neutral_streak >= c.exit_fail_count:
                self.short_hold_fail_streak = 0
                return BIAS_NEUTRAL
            return BIAS_SHORT

        if self.enter_long_streak >= c.enter_confirm_count:
            return BIAS_LONG
        if self.enter_short_streak >= c.enter_confirm_count:
            return BIAS_SHORT
        return BIAS_NEUTRAL

    def _decide_entry(self, bias: str, inp: BiasInputs) -> str:
        c = self.cfg

        long_ready_now = (
            bias == BIAS_LONG
            and inp.basis_slope >= c.slope_long_ready
            and inp.basis_ema_delta > 0
            and inp.volume_burst >= c.volume_burst_ready
        )
        short_ready_now = (
            bias == BIAS_SHORT
            and inp.basis_slope <= c.slope_short_ready
            and inp.basis_ema_delta < 0
            and inp.volume_burst >= c.volume_burst_ready
        )

        self.entry_long_streak = self.entry_long_streak + 1 if long_ready_now else 0
        self.entry_short_streak = self.entry_short_streak + 1 if short_ready_now else 0

        if self.entry_long_streak >= c.entry_confirm_count:
            return ENTRY_LONG
        if self.entry_short_streak >= c.entry_confirm_count:
            return ENTRY_SHORT
        return ENTRY_WAIT

    def _calc_confidence(self, bias: str, long_score: float, short_score: float) -> float:
        if bias == BIAS_LONG:
            base = 50.0 + (long_score - short_score) * 8.0
        elif bias == BIAS_SHORT:
            base = 50.0 + (short_score - long_score) * 8.0
        else:
            base = 40.0 - abs(long_score - short_score) * 5.0
        return round(max(0.0, min(100.0, base)), 1)

    def _make_reason(
        self,
        inp: BiasInputs,
        long_score: float,
        short_score: float,
        bias: str,
        entry_signal: str,
    ) -> str:
        reasons: List[str] = []

        if inp.foreign_buy >= self.cfg.foreign_mid:
            reasons.append("외국인 순매수 우세")
        elif inp.foreign_buy <= -self.cfg.foreign_mid:
            reasons.append("외국인 순매도 우세")
        else:
            reasons.append("외국인 수급 중립권")

        if inp.oi_delta >= self.cfg.oi_mid:
            reasons.append("OI 증가(신규 유입 확인)")
        elif inp.oi_delta <= self.cfg.oi_negative_mid:
            reasons.append("OI 감소(청산/감쇠 가능성)")
        else:
            reasons.append("OI 변화 제한적")

        if inp.basis >= self.cfg.basis_mid:
            reasons.append("basis 양수 우세")
        elif inp.basis <= -self.cfg.basis_mid:
            reasons.append("basis 음수 우세")
        else:
            reasons.append("basis 중립권")

        if inp.basis_ema_delta > self.cfg.ema_delta_strong:
            reasons.append("EMA basis 개선")
        elif inp.basis_ema_delta < -self.cfg.ema_delta_strong:
            reasons.append("EMA basis 악화")
        else:
            reasons.append("EMA basis 변화 미약")

        if inp.volume_burst >= self.cfg.volume_burst_ready:
            reasons.append(f"거래량 폭발 {inp.volume_burst:.2f}x")
        else:
            reasons.append(f"거래량 보통 {inp.volume_burst:.2f}x")

        if entry_signal == ENTRY_LONG:
            reasons.append("진입시점: LONG_READY")
        elif entry_signal == ENTRY_SHORT:
            reasons.append("진입시점: SHORT_READY")
        else:
            reasons.append("진입시점: WAIT")

        reasons.append(f"점수 L:{long_score:.1f} / S:{short_score:.1f} → {bias}")
        return " | ".join(reasons)

    def update(self, inp: BiasInputs) -> BiasSnapshot:
        scores = self._compute_scores(inp)
        long_score  = scores["long_score"]
        short_score = scores["short_score"]

        bias = self._decide_bias(long_score, short_score)
        self.current_bias = bias

        entry_signal = self._decide_entry(bias, inp)
        confidence   = self._calc_confidence(bias, long_score, short_score)
        reason       = self._make_reason(inp, long_score, short_score, bias, entry_signal)

        snap = BiasSnapshot(
            mode=self.mode,
            bias=bias,
            entry_signal=entry_signal,
            confidence=confidence,
            long_score=long_score,
            short_score=short_score,
            enter_long_streak=self.enter_long_streak,
            enter_short_streak=self.enter_short_streak,
            neutral_streak=self.neutral_streak,
            entry_long_streak=self.entry_long_streak,
            entry_short_streak=self.entry_short_streak,
            foreign_buy=inp.foreign_buy,
            oi_delta=inp.oi_delta,
            basis=inp.basis,
            basis_ema=inp.basis_ema,
            basis_ema_delta=inp.basis_ema_delta,
            basis_slope=inp.basis_slope,
            volume_burst=inp.volume_burst,
            trade_strength=inp.trade_strength,
            reason=reason,
            trend_long_score=scores["trend_long_score"],
            trend_short_score=scores["trend_short_score"],
            trend_weight_long=self.cfg.trend_weight_long,
            trend_weight_short=self.cfg.trend_weight_short,
            ema_fast=inp.ema_fast,
            ema_slow=inp.ema_slow,
            ema_fast_slope=inp.ema_fast_slope,
        )

        self.history.append(asdict(snap))
        return snap

    def get_recent_history(self, limit: int = 60) -> List[Dict]:
        if limit <= 0:
            return []
        return list(self.history)[-limit:]