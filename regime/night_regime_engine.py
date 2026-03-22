# [Directory] MMEAN
# [File] night_regime_engine.py
from __future__ import annotations

from collections import deque
from dataclasses import asdict, dataclass
from typing import Deque, Dict, List, Optional


# -------------------------------------------------------------------
# 상수
# -------------------------------------------------------------------
REGIME_BULL    = "BULL"
REGIME_BEAR    = "BEAR"
REGIME_NEUTRAL = "NEUTRAL"

ENTRY_LONG  = "LONG_READY"
ENTRY_SHORT = "SHORT_READY"
ENTRY_WAIT  = "WAIT"


# -------------------------------------------------------------------
# 입력 / 출력 구조체
# -------------------------------------------------------------------
@dataclass
class NightInputs:
    futures_price: float       # 현재 체결가
    cum_volume: float          # 당일 야간 누적 거래량
    timestamp_sec: float       # 현재 시각 (time.time())
    session_start_sec: float   # 야간 세션 시작 시각 (time.time() 기준)


@dataclass
class NightSnapshot:
    regime: str
    entry_signal: str
    confidence: float
    ema20: float
    ema60: float
    ma_gap_pct: float          # (EMA20-EMA60)/EMA60*100
    night_vol_ratio: float     # 당일 야간거래량 / 5일 평균
    orb_high: float
    orb_low: float
    orb_active: bool           # ORB 확정 여부 (1시간 경과 후 True)
    ma_score: float
    vol_score: float
    orb_score: float
    raw_score: float
    reason: str


# -------------------------------------------------------------------
# 엔진
# -------------------------------------------------------------------
class NightRegimeEngine:
    """
    야간 선물 추세추종 신호 엔진.
    나스닥 실시간 없이 KRX WS 체결가 + 거래량만 사용.

    신호 가중치:
      MA 이격률 50%  |  야간 거래량 비율 30%  |  ORB 20%
    """

    # ORB: 야간 개장 후 첫 1시간
    ORB_WINDOW_SEC = 3600.0

    # MA 이격 임계값
    # 실측: 야간 이격 0.001~0.005% 수준 → 기존 0.05% 기준은 진입 불가
    MA_GAP_STRONG  = 0.008  # % (기존 0.05 → 야간 실측 기반)
    MA_GAP_WEAK    = 0.003  # % (기존 0.02)

    # 거래량 비율 임계값 (야간 절대량 낮음 → 기준 완화)
    VOL_HIGH = 1.1          # (기존 1.3)
    VOL_LOW  = 0.6          # (기존 0.7)

    # 최종 점수 임계값 (진입 문턱 완화)
    SCORE_BULL = +0.45      # (기존 +0.6)
    SCORE_BEAR = -0.45      # (기존 -0.6)

    # EMA 파라미터
    ALPHA20 = 2 / (20 + 1)
    ALPHA60 = 2 / (60 + 1)

    # 5일 평균 거래량 — 실운영 시 외부에서 주입 가능
    DEFAULT_5D_AVG_VOL = 5000.0

    def __init__(
        self,
        history_limit: int = 600,
        avg_night_vol_5d: float = DEFAULT_5D_AVG_VOL,
    ):
        self.avg_night_vol_5d = max(avg_night_vol_5d, 1.0)
        self.history: Deque[Dict] = deque(maxlen=history_limit)
        self._reset()

    # ------------------------------------------------------------------
    # 외부 API
    # ------------------------------------------------------------------
    def set_avg_night_vol(self, vol: float) -> None:
        """5일 평균 야간 거래량 갱신 (REST 저빈도 조회 결과로 주입)."""
        self.avg_night_vol_5d = max(vol, 1.0)

    def reset_session(self, session_start_sec: float) -> None:
        """야간 세션 시작 시 호출 — ORB / EMA 초기화."""
        self._reset()
        self._session_start = session_start_sec

    def update(self, inp: NightInputs) -> NightSnapshot:
        # ① EMA 갱신
        self._update_ema(inp.futures_price)

        # ② ORB 갱신
        elapsed = inp.timestamp_sec - self._session_start
        self._update_orb(inp.futures_price, elapsed)

        # ③ 개별 신호 점수
        ma_score  = self._score_ma()
        vol_score = self._score_volume(inp.cum_volume)
        orb_score = self._score_orb(inp.futures_price)

        # ④ 거래량은 방향성 없음 → MA 방향과 곱
        vol_dir   = 1.0 if ma_score >= 0 else -1.0
        raw_score = (ma_score * 0.5) + (vol_score * vol_dir * 0.3) + (orb_score * 0.2)
        raw_score = round(raw_score, 3)

        # ⑤ 레짐 판정
        if raw_score >= self.SCORE_BULL:
            regime = REGIME_BULL
        elif raw_score <= self.SCORE_BEAR:
            regime = REGIME_BEAR
        else:
            regime = REGIME_NEUTRAL

        # ⑥ 진입 신호 (레짐 확정 + ORB 방향 일치 조건)
        entry_signal = self._entry_signal(regime, orb_score)

        # ⑦ 신뢰도
        confidence = self._confidence(regime, raw_score)

        # ⑧ 사유
        ma_gap_pct = self._ma_gap_pct()
        night_vol_ratio = inp.cum_volume / self.avg_night_vol_5d
        reason = self._reason(
            ma_gap_pct, night_vol_ratio, orb_score,
            ma_score, vol_score, raw_score, regime, entry_signal,
        )

        snap = NightSnapshot(
            regime=regime,
            entry_signal=entry_signal,
            confidence=confidence,
            ema20=round(self._ema20, 4),
            ema60=round(self._ema60, 4),
            ma_gap_pct=round(ma_gap_pct, 4),
            night_vol_ratio=round(night_vol_ratio, 3),
            orb_high=round(self._orb_high, 4),
            orb_low=round(self._orb_low, 4),
            orb_active=self._orb_active,
            ma_score=ma_score,
            vol_score=vol_score,
            orb_score=orb_score,
            raw_score=raw_score,
            reason=reason,
        )
        self.history.append(asdict(snap))
        return snap

    def get_recent_history(self, limit: int = 60) -> List[Dict]:
        if limit <= 0:
            return []
        return list(self.history)[-limit:]

    # ------------------------------------------------------------------
    # 내부 로직
    # ------------------------------------------------------------------
    def _reset(self) -> None:
        self._ema20: float = 0.0
        self._ema60: float = 0.0
        self._orb_high: float = float("-inf")
        self._orb_low: float  = float("inf")
        self._orb_active: bool = False
        self._session_start: float = 0.0
        self._entry_long_streak:  int = 0
        self._entry_short_streak: int = 0

    def _update_ema(self, price: float) -> None:
        if price <= 0:
            return
        if self._ema20 == 0.0:
            self._ema20 = price
            self._ema60 = price
            return
        self._ema20 = self.ALPHA20 * price + (1 - self.ALPHA20) * self._ema20
        self._ema60 = self.ALPHA60 * price + (1 - self.ALPHA60) * self._ema60

    def _update_orb(self, price: float, elapsed: float) -> None:
        if price <= 0:
            return
        if elapsed < self.ORB_WINDOW_SEC:
            # ORB 형성 중
            self._orb_high = max(self._orb_high, price)
            self._orb_low  = min(self._orb_low,  price)
            self._orb_active = False
        else:
            # ORB 확정
            self._orb_active = True

    def _ma_gap_pct(self) -> float:
        if self._ema60 == 0.0:
            return 0.0
        return (self._ema20 - self._ema60) / self._ema60 * 100.0

    def _score_ma(self) -> float:
        gap = self._ma_gap_pct()
        if gap >= self.MA_GAP_STRONG:
            return +1.0
        if gap >= self.MA_GAP_WEAK:
            return +0.5
        if gap <= -self.MA_GAP_STRONG:
            return -1.0
        if gap <= -self.MA_GAP_WEAK:
            return -0.5
        return 0.0

    def _score_volume(self, cum_volume: float) -> float:
        ratio = cum_volume / self.avg_night_vol_5d
        if ratio >= self.VOL_HIGH:
            return +1.0
        if ratio <= self.VOL_LOW:
            return -1.0
        return 0.0

    def _score_orb(self, price: float) -> float:
        if not self._orb_active or price <= 0:
            return 0.0
        if self._orb_high == float("-inf") or self._orb_low == float("inf"):
            return 0.0
        if price > self._orb_high:
            return +1.0
        if price < self._orb_low:
            return -1.0
        return 0.0

    def _entry_signal(self, regime: str, orb_score: float) -> str:
        """
        진입 조건 (완화):
          ORB 확정 후  → BULL+ORB상향 / BEAR+ORB하향
          ORB 미확정   → 레짐만으로 진입 (MA+VOL 기반)
          streak = 1 (연속 1틱으로 완화, 노이즈 방지 최소 유지)
        """
        if self._orb_active:
            # ORB 확정: 레짐 + ORB 방향 일치
            long_ok  = (regime == REGIME_BULL and orb_score > 0)
            short_ok = (regime == REGIME_BEAR and orb_score < 0)
        else:
            # ORB 미확정: 레짐만으로 진입 허용
            long_ok  = (regime == REGIME_BULL)
            short_ok = (regime == REGIME_BEAR)

        if long_ok:
            self._entry_long_streak  += 1
            self._entry_short_streak  = 0
        elif short_ok:
            self._entry_short_streak += 1
            self._entry_long_streak   = 0
        else:
            self._entry_long_streak  = 0
            self._entry_short_streak = 0

        if self._entry_long_streak  >= 1:
            return ENTRY_LONG
        if self._entry_short_streak >= 1:
            return ENTRY_SHORT
        return ENTRY_WAIT

    def _confidence(self, regime: str, raw_score: float) -> float:
        if regime == REGIME_NEUTRAL:
            base = 40.0 - abs(raw_score) * 20.0
        else:
            base = 50.0 + abs(raw_score) * 40.0
        return round(max(0.0, min(100.0, base)), 1)

    def _reason(
        self,
        ma_gap_pct: float,
        night_vol_ratio: float,
        orb_score: float,
        ma_score: float,
        vol_score: float,
        raw_score: float,
        regime: str,
        entry_signal: str,
    ) -> str:
        parts = []

        # MA
        if ma_gap_pct >= self.MA_GAP_STRONG:
            parts.append(f"MA이격 상향 {ma_gap_pct:+.3f}%")
        elif ma_gap_pct <= -self.MA_GAP_STRONG:
            parts.append(f"MA이격 하향 {ma_gap_pct:+.3f}%")
        else:
            parts.append(f"MA이격 중립 {ma_gap_pct:+.3f}%")

        # 거래량
        if night_vol_ratio >= self.VOL_HIGH:
            parts.append(f"야간거래량 활발 {night_vol_ratio:.2f}x")
        elif night_vol_ratio <= self.VOL_LOW:
            parts.append(f"야간거래량 빈약 {night_vol_ratio:.2f}x")
        else:
            parts.append(f"야간거래량 보통 {night_vol_ratio:.2f}x")

        # ORB
        if not self._orb_active:
            parts.append("ORB 형성중")
        elif orb_score > 0:
            parts.append("ORB 상향돌파")
        elif orb_score < 0:
            parts.append("ORB 하향돌파")
        else:
            parts.append("ORB 내부")

        parts.append(
            f"점수 MA:{ma_score:+.1f} VOL:{vol_score:+.1f} ORB:{orb_score:+.1f} "
            f"→ raw:{raw_score:+.3f} → {regime} / {entry_signal}"
        )
        return " | ".join(parts)
