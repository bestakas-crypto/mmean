# engines/sideways_detector.py
"""
SidewaysDetector — 횡보 구간 감지 엔진

역할:
  - 가격이 VWAP ± ATR×0.5 이내에서 5분 이상 지속되면 횡보(RANGE) 선언
  - 횡보 중 진입은 하드 블록 (Hard Block) — 어떤 신호도 override 불가
  - 해제 조건: 레인지 이탈 + 2틱 연속 유지 + 수급/기술 조건 1개 이상

횡보 진입 조건:
  가격이 VWAP ± (ATR × RANGE_ATR_MULT) 이내에서 ENTER_TICKS 연속

횡보 해제 조건 (모두 충족):
  ① 가격이 레인지 밖으로 이탈
  ② 이탈 방향으로 EXIT_TICKS 연속 유지
  ③ 아래 중 1개 이상:
     · 해당 방향 점수 우위 (long_score vs short_score)
     · EMA 기울기 해당 방향 (ema_fast_slope)
     · 외국인 선물 delta 해당 방향

블록 계층 (확정):
  RANGE  = 하드 블록 (Hard Block) — 진입 전면 금지, override 불가
  CHOPPY = 소프트 블록 (Soft Block) — MarketAnalyst 판정, 별도 처리
  우선순위: RANGE > CHOPPY
"""
from __future__ import annotations

import logging
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime
from typing import Deque, Dict, Optional

log = logging.getLogger("MMEAN.SidewaysDetector")

# ── 상수 (확정값, 운영 중 변경 불가) ────────────────────────────────────────
RANGE_ATR_MULT  = 0.5    # 레인지 = VWAP ± ATR × 0.5
ENTER_TICKS     = 5      # 횡보 진입: N틱 연속 레인지 내
EXIT_TICKS      = 2      # 해제 후보: N틱 연속 레인지 이탈
EMA_SLOPE_MIN   = 0.01   # EMA 기울기 유의미 최솟값 (절대값)
LS_DIFF_MIN     = 0.05   # long_score - short_score 유의미 최솟값 (절대값)
FOREIGN_DELTA_MIN = 100  # 외국인 delta 유의미 최솟값 (계약수 또는 억원)


@dataclass
class SidewaysState:
    """외부에 노출되는 횡보 상태 스냅샷."""
    is_sideways:        bool   = False
    range_lower:        float  = 0.0      # 현재 레인지 하단
    range_upper:        float  = 0.0      # 현재 레인지 상단
    in_range_ticks:     int    = 0        # 레인지 내 연속 틱 수
    out_range_ticks:    int    = 0        # 레인지 이탈 연속 틱 수
    exit_direction:     int    = 0        # 이탈 방향: +1 상단 / -1 하단 / 0 없음
    confirm_score:      bool   = False    # 해제 조건③ 충족 여부
    confirm_ema:        bool   = False    # EMA 기울기 조건 충족 여부
    confirm_foreign:    bool   = False    # 외국인 delta 조건 충족 여부
    entered_at:         str    = ""       # 횡보 진입 시각 HH:MM:SS
    reason:             str    = ""       # 현재 상태 요약


class SidewaysDetector:
    """
    횡보 구간 감지기.

    사용:
        det = SidewaysDetector()
        det.update(state)        # 매 틱마다 호출
        status = det.get_state() # 현재 상태 조회
        if det.is_hard_block():  # 하드 블록 여부 (진입 금지)
            ...
    """

    def __init__(
        self,
        atr_mult:    float = RANGE_ATR_MULT,
        enter_ticks: int   = ENTER_TICKS,
        exit_ticks:  int   = EXIT_TICKS,
    ) -> None:
        self._atr_mult    = atr_mult
        self._enter_ticks = enter_ticks
        self._exit_ticks  = exit_ticks

        # 내부 상태
        self._is_sideways:     bool  = False
        self._in_range_ticks:  int   = 0
        self._out_ticks:       int   = 0
        self._exit_dir:        int   = 0     # +1 상단이탈 / -1 하단이탈
        self._entered_at:      str   = ""
        self._range_lower:     float = 0.0
        self._range_upper:     float = 0.0

        # 외국인 delta 계산용 직전값
        self._prev_foreign:    Optional[float] = None

        # 이력 (디버깅용, 최근 20틱)
        self._tick_log: Deque[Dict] = deque(maxlen=20)

    # ── 퍼블릭 API ─────────────────────────────────────────────────────────

    def update(self, state: Dict) -> SidewaysState:
        """
        매 틱마다 호출. state는 engine_runtime의 state 딕셔너리.
        반환: 현재 SidewaysState 스냅샷.
        """
        futures_price = float(state.get("futures_price", 0.0))
        vwap          = float(state.get("vwap",          0.0))
        atr           = float(state.get("atr",           0.0))
        ema_slope     = float(state.get("ema_fast_slope", 0.0))
        long_score    = float(state.get("long_score",    0.0))
        short_score   = float(state.get("short_score",   0.0))
        foreign_buy   = float(state.get("foreign_buy",   0.0))

        # 유효성 검사 — 가격·VWAP·ATR 없으면 상태 유지
        if futures_price <= 0 or vwap <= 0 or atr <= 0:
            log.debug("SidewaysDetector: 유효하지 않은 입력값 — 상태 유지")
            return self._build_state("입력값 부족 — 상태 유지")

        # ── 레인지 계산 ────────────────────────────────────────────────────
        half       = atr * self._atr_mult
        lower      = vwap - half
        upper      = vwap + half
        in_range   = lower <= futures_price <= upper
        self._range_lower = lower
        self._range_upper = upper

        # ── 외국인 delta ───────────────────────────────────────────────────
        if self._prev_foreign is None:
            foreign_delta = 0.0
        else:
            foreign_delta = foreign_buy - self._prev_foreign
        self._prev_foreign = foreign_buy

        # ── 상태 전이 ─────────────────────────────────────────────────────
        if not self._is_sideways:
            result = self._check_enter(in_range, lower, upper)
        else:
            result = self._check_exit(
                futures_price, lower, upper,
                long_score, short_score, ema_slope, foreign_delta,
            )

        # 틱 로그 기록
        self._tick_log.append({
            "ts":            datetime.now().strftime("%H:%M:%S"),
            "price":         futures_price,
            "vwap":          vwap,
            "lower":         lower,
            "upper":         upper,
            "in_range":      in_range,
            "is_sideways":   self._is_sideways,
            "foreign_delta": round(foreign_delta),
        })

        return result

    def is_hard_block(self) -> bool:
        """횡보 하드 블록 여부 — True이면 신규 진입 전면 금지."""
        return self._is_sideways

    def get_state(self) -> SidewaysState:
        return self._build_state()

    def reset(self) -> None:
        """장 시작 시 상태 초기화."""
        self._is_sideways    = False
        self._in_range_ticks = 0
        self._out_ticks      = 0
        self._exit_dir       = 0
        self._entered_at     = ""
        self._prev_foreign   = None
        self._tick_log.clear()
        log.info("SidewaysDetector 초기화")

    # ── 횡보 진입 체크 ──────────────────────────────────────────────────────

    def _check_enter(self, in_range: bool, lower: float, upper: float) -> SidewaysState:
        if in_range:
            self._in_range_ticks += 1
            self._out_ticks = 0
        else:
            self._in_range_ticks = 0
            self._out_ticks      = 0

        if self._in_range_ticks >= self._enter_ticks:
            self._is_sideways = True
            self._entered_at  = datetime.now().strftime("%H:%M:%S")
            log.info(
                "SidewaysDetector ▶ 횡보 진입 | 레인지 [%.2f ~ %.2f] | %d틱 연속",
                lower, upper, self._in_range_ticks,
            )
            return self._build_state(f"횡보 진입 ({self._in_range_ticks}틱 연속)")

        return self._build_state(
            f"정상 | 레인지 내 {self._in_range_ticks}/{self._enter_ticks}틱"
            if in_range else "정상 | 레인지 외"
        )

    # ── 횡보 해제 체크 ──────────────────────────────────────────────────────

    def _check_exit(
        self,
        price:        float,
        lower:        float,
        upper:        float,
        long_score:   float,
        short_score:  float,
        ema_slope:    float,
        foreign_delta: float,
    ) -> SidewaysState:

        in_range = lower <= price <= upper

        if in_range:
            # 레인지 안으로 다시 들어옴 → 이탈 카운터 리셋
            self._out_ticks = 0
            self._exit_dir  = 0
            return self._build_state("횡보 유지 | 레인지 내")

        # 이탈 방향 결정
        cur_dir = +1 if price > upper else -1

        if self._exit_dir != 0 and self._exit_dir != cur_dir:
            # 이탈 방향이 바뀜 → 카운터 리셋
            self._out_ticks = 1
            self._exit_dir  = cur_dir
        else:
            self._exit_dir = cur_dir
            self._out_ticks += 1

        # ─ 조건 ①②: 이탈 + 2틱 연속 ────────────────────────────────────
        if self._out_ticks < self._exit_ticks:
            return self._build_state(
                f"횡보 유지 | 이탈 {self._out_ticks}/{self._exit_ticks}틱 "
                f"({'상단' if cur_dir > 0 else '하단'})"
            )

        # ─ 조건 ③: 수급/기술 조건 1개 이상 ─────────────────────────────
        score_ok   = self._check_score(cur_dir, long_score, short_score)
        ema_ok     = self._check_ema(cur_dir, ema_slope)
        foreign_ok = self._check_foreign(cur_dir, foreign_delta)

        confirms = [score_ok, ema_ok, foreign_ok]
        confirm_any = any(confirms)

        if confirm_any:
            self._is_sideways    = False
            self._in_range_ticks = 0
            self._out_ticks      = 0
            dir_str              = "상단" if cur_dir > 0 else "하단"
            log.info(
                "SidewaysDetector ◀ 횡보 해제 | 방향=%s | score=%s ema=%s foreign=%s",
                dir_str, score_ok, ema_ok, foreign_ok,
            )
            return self._build_state(
                f"횡보 해제 | {dir_str} 이탈 확정 "
                f"[score={score_ok} ema={ema_ok} foreign={foreign_ok}]",
                confirm_score=score_ok,
                confirm_ema=ema_ok,
                confirm_foreign=foreign_ok,
            )

        # 조건③ 미충족 → 횡보 유지
        return self._build_state(
            f"횡보 유지 | 이탈 {self._out_ticks}틱이나 조건③ 미충족 "
            f"[score={score_ok} ema={ema_ok} foreign={foreign_ok}]",
            confirm_score=score_ok,
            confirm_ema=ema_ok,
            confirm_foreign=foreign_ok,
        )

    # ── 조건③ 개별 체크 ────────────────────────────────────────────────────

    def _check_score(self, direction: int, long_score: float, short_score: float) -> bool:
        """해당 방향 점수 우위 여부."""
        diff = long_score - short_score
        if direction > 0:
            return diff >= LS_DIFF_MIN     # 상단 이탈 → LONG 점수 우위
        else:
            return diff <= -LS_DIFF_MIN    # 하단 이탈 → SHORT 점수 우위

    def _check_ema(self, direction: int, ema_slope: float) -> bool:
        """EMA 기울기가 이탈 방향과 일치하는지."""
        if direction > 0:
            return ema_slope >= EMA_SLOPE_MIN
        else:
            return ema_slope <= -EMA_SLOPE_MIN

    def _check_foreign(self, direction: int, foreign_delta: float) -> bool:
        """외국인 delta가 이탈 방향과 일치하는지."""
        if direction > 0:
            return foreign_delta >= FOREIGN_DELTA_MIN
        else:
            return foreign_delta <= -FOREIGN_DELTA_MIN

    # ── 상태 빌더 ──────────────────────────────────────────────────────────

    def _build_state(
        self,
        reason:          str  = "",
        confirm_score:   bool = False,
        confirm_ema:     bool = False,
        confirm_foreign: bool = False,
    ) -> SidewaysState:
        return SidewaysState(
            is_sideways      = self._is_sideways,
            range_lower      = self._range_lower,
            range_upper      = self._range_upper,
            in_range_ticks   = self._in_range_ticks,
            out_range_ticks  = self._out_ticks,
            exit_direction   = self._exit_dir,
            confirm_score    = confirm_score,
            confirm_ema      = confirm_ema,
            confirm_foreign  = confirm_foreign,
            entered_at       = self._entered_at,
            reason           = reason,
        )
