# engines/pulse_validator.py
"""
PulseValidator — 30분 LLM 검증기

역할:
  - PulseEngine 방향과 LLM 판단이 일치하는지 30분마다 검증
  - 불일치 시 처방 적용 (half 모드, 진입 금지 등)

Fallback 정책 (확정값, 변경 불가):
  1회 실패 → 직전 결과 유지
  2회 연속 실패 → validator 비활성 + 진입 half 모드
  2회 연속 성공 → 정상 복귀 (1회만으로 즉시 복귀 불가)

불일치 처방 (확정값):
  일치              → 정상
  약한 불일치       → 경고 로그, 사이즈 50% 축소 권고
  (엔진=BULLISH/BEARISH ↔ LLM=NEUTRAL 또는 반대)
  강한 불일치       → 신규 진입 전면 금지, 손절선 타이트
  (엔진=BULLISH ↔ LLM=BEARISH 또는 반대)
  1회 강한 불일치 → 진입 금지
  2회 연속 강한 불일치 → 기존 포지션 청산 권고, 엔진 강제 NEUTRAL 알림
  복구: 2회 연속 일치 필요
"""
from __future__ import annotations

import json
import logging
import os
import threading
from datetime import datetime
from typing import Any, Dict, Optional

import requests

log = logging.getLogger("MMEAN.PulseValidator")

# ── 상수 (확정값, 운영 중 변경 불가) ──────────────────────────────────────
INTERVAL_SEC           = 1800   # 30분
WARMUP_SEC             = 60     # 첫 실행 대기
VALIDATOR_FAIL_FALLBACK = 1     # N회 실패 시 직전 결과 유지
VALIDATOR_FAIL_HALF     = 2     # N회 연속 실패 시 half 모드
VALIDATOR_RECOVER_COUNT = 2     # 정상 복귀 필요 연속 성공 횟수

_HAIKU_MODEL   = "claude-haiku-4-5-20251001"
_HAIKU_URL     = "https://api.anthropic.com/v1/messages"
_HAIKU_TIMEOUT = 15
_MAX_TOKENS    = 300

VALID_DIRECTIONS = {"BULLISH", "NEUTRAL", "BEARISH"}

_SYSTEM_PROMPT = """\
KOSPI200 선물 장중 전략 방향 분석가.
현재 지표를 보고 오늘 장의 전략 방향을 판단한다.

반드시 아래 JSON으로만 응답:
{"direction": "...", "confidence": 0.00, "reason": "..."}

direction: BULLISH | NEUTRAL | BEARISH
confidence: 0.0~1.0
reason: 판단 근거 한 문장 (한국어)"""


class ValidatorStatus:
    NORMAL      = "normal"
    WEAK_MISS   = "weak_mismatch"
    STRONG_MISS = "strong_mismatch"
    HALF        = "half_mode"
    INACTIVE    = "inactive"


class PulseValidator:
    """
    30분 LLM 검증기.

    사용:
        validator = PulseValidator(runtime, pulse_engine)
        validator.start()
        state = validator.get_state()
    """

    def __init__(self, runtime: Any, pulse_engine: Any,
                 interval_sec: int = INTERVAL_SEC) -> None:
        self._runtime      = runtime
        self._pulse        = pulse_engine
        self._interval     = interval_sec
        self._stop         = threading.Event()
        self._thread: Optional[threading.Thread] = None
        self._lock         = threading.Lock()
        self._api_key      = os.getenv("ANTHROPIC_API_KEY", "").strip()

        # 상태
        self._status        = ValidatorStatus.NORMAL
        self._fail_count    = 0       # 연속 LLM 호출 실패 횟수
        self._mismatch_strong_count = 0  # 연속 강한 불일치 횟수
        self._recover_count = 0       # 연속 일치 횟수 (복구용)
        self._last_llm_dir  = ""      # 마지막 LLM 판정 방향
        self._last_llm_conf = 0.0
        self._last_llm_reason = ""
        self._last_check_ts = ""
        self._last_result   = "none"  # match|weak_miss|strong_miss|fail

        # 처방 플래그
        self._entry_blocked = False    # 강한 불일치 → 진입 금지
        self._half_mode     = False    # 연속 실패 → half 모드
        self._force_neutral = False    # 2회 강한 불일치 → 강제 알림

    # ── 수명 주기 ───────────────────────────────────────────────────────────

    def start(self) -> None:
        if self._thread and self._thread.is_alive():
            return
        if not self._api_key:
            log.warning("PulseValidator: ANTHROPIC_API_KEY 없음 — 비활성")
            return
        self._stop.clear()
        self._thread = threading.Thread(
            target=self._loop, daemon=True, name="MMEAN-PulseValidator",
        )
        self._thread.start()
        log.info("PulseValidator 시작 | interval=%ds", self._interval)

    def stop(self) -> None:
        self._stop.set()
        if self._thread:
            self._thread.join(timeout=5)

    # ── 메인 루프 ───────────────────────────────────────────────────────────

    def _loop(self) -> None:
        self._stop.wait(WARMUP_SEC)
        if not self._stop.is_set():
            try:
                self._run_once()
            except Exception as e:
                log.warning("PulseValidator 초기 실행 오류: %s", e)

        while not self._stop.is_set():
            self._stop.wait(self._interval)
            if self._stop.is_set():
                break
            try:
                self._run_once()
            except Exception as e:
                log.warning("PulseValidator 루프 오류: %s", e)

    def _run_once(self) -> None:
        now_hhmm = datetime.now().strftime("%H:%M")
        if now_hhmm < "09:00" or now_hhmm > "15:45":
            return

        engine_dir = self._pulse.get_signal().direction

        # LLM 호출
        llm_result = self._call_llm()

        if llm_result is None:
            # ─ 호출 실패 fallback ────────────────────────────────────────
            with self._lock:
                self._fail_count += 1
                self._last_result = "fail"
                self._last_check_ts = datetime.now().strftime("%H:%M:%S")

                if self._fail_count >= VALIDATOR_FAIL_HALF:
                    self._half_mode = True
                    self._status    = ValidatorStatus.HALF
                    log.warning(
                        "PulseValidator: %d회 연속 실패 → half 모드 활성",
                        self._fail_count,
                    )
                else:
                    log.warning(
                        "PulseValidator: LLM 호출 실패 (%d/%d) → 직전 결과 유지",
                        self._fail_count, VALIDATOR_FAIL_HALF,
                    )
            return

        # 호출 성공 → fail_count 리셋
        with self._lock:
            self._fail_count = 0
            if self._half_mode:
                self._recover_count += 1
                if self._recover_count >= VALIDATOR_RECOVER_COUNT:
                    self._half_mode     = False
                    self._recover_count = 0
                    self._status        = ValidatorStatus.NORMAL
                    log.info("PulseValidator: half 모드 해제 (연속 %d회 성공)", VALIDATOR_RECOVER_COUNT)
            else:
                self._recover_count = 0

        llm_dir  = llm_result["direction"]
        llm_conf = llm_result["confidence"]
        llm_rsn  = llm_result["reason"]

        with self._lock:
            self._last_llm_dir    = llm_dir
            self._last_llm_conf   = llm_conf
            self._last_llm_reason = llm_rsn
            self._last_check_ts   = datetime.now().strftime("%H:%M:%S")

        # ─ 불일치 판정 ───────────────────────────────────────────────────
        mismatch = self._classify_mismatch(engine_dir, llm_dir)

        with self._lock:
            self._last_result = mismatch

            if mismatch == "match":
                self._mismatch_strong_count = 0
                self._recover_count        += 1
                if self._entry_blocked and self._recover_count >= VALIDATOR_RECOVER_COUNT:
                    self._entry_blocked  = False
                    self._force_neutral  = False
                    self._recover_count  = 0
                    self._status         = ValidatorStatus.NORMAL
                    log.info("PulseValidator: 진입 금지 해제 (연속 %d회 일치)", VALIDATOR_RECOVER_COUNT)
                else:
                    self._status = ValidatorStatus.NORMAL

            elif mismatch == "weak_miss":
                self._mismatch_strong_count = 0
                self._recover_count         = 0
                self._status                = ValidatorStatus.WEAK_MISS
                log.warning(
                    "PulseValidator 약한 불일치 | engine=%s llm=%s → 사이즈 50%%",
                    engine_dir, llm_dir,
                )

            elif mismatch == "strong_miss":
                self._recover_count          = 0
                self._mismatch_strong_count += 1
                self._entry_blocked          = True
                self._status                 = ValidatorStatus.STRONG_MISS
                log.warning(
                    "PulseValidator ★ 강한 불일치 (%d회) | engine=%s llm=%s → 진입 금지",
                    self._mismatch_strong_count, engine_dir, llm_dir,
                )
                if self._mismatch_strong_count >= 2:
                    self._force_neutral = True
                    log.warning(
                        "PulseValidator ★★ 2회 연속 강한 불일치 → 포지션 청산 검토 권고"
                    )

    # ── LLM 호출 ────────────────────────────────────────────────────────────

    def _call_llm(self) -> Optional[Dict]:
        state = self._runtime.state
        prompt = (
            f"현재 시각: {datetime.now().strftime('%H:%M')}\n"
            f"선물가: {state.get('futures_price', 0):.2f} | VWAP: {state.get('vwap', 0):.2f}\n"
            f"flow_score: {state.get('flow_score', 0):+.3f}\n"
            f"외국인 순매수: {state.get('foreign_buy', 0):+,.0f}\n"
            f"OI: {state.get('oi_value', 0):,.0f} (delta: {state.get('oi_delta', 0):+,.0f})\n"
            f"베이시스: {state.get('basis', 0):+.3f}\n"
            f"EMA기울기: {state.get('ema_fast_slope', 0):+.4f}\n"
            f"엔진 현재 판정: {self._pulse.get_signal().direction}"
        )
        try:
            resp = requests.post(
                _HAIKU_URL,
                headers={
                    "x-api-key":         self._api_key,
                    "anthropic-version": "2023-06-01",
                    "content-type":      "application/json",
                },
                json={
                    "model":      _HAIKU_MODEL,
                    "max_tokens": _MAX_TOKENS,
                    "system":     _SYSTEM_PROMPT,
                    "messages":   [{"role": "user", "content": prompt}],
                },
                timeout=_HAIKU_TIMEOUT,
            )
            resp.raise_for_status()
            raw = resp.json()["content"][0]["text"].strip()
            if raw.startswith("```"):
                lines = raw.split("\n")
                raw = "\n".join(lines[1:-1] if lines[-1].strip() == "```" else lines[1:])
            parsed    = json.loads(raw)
            direction = str(parsed.get("direction", "")).strip().upper()
            if direction not in VALID_DIRECTIONS:
                return None
            return {
                "direction":  direction,
                "confidence": float(parsed.get("confidence", 0.5)),
                "reason":     str(parsed.get("reason", "")),
            }
        except Exception as e:
            log.warning("PulseValidator LLM 호출 실패: %s", e)
            return None

    # ── 불일치 분류 ─────────────────────────────────────────────────────────

    def _classify_mismatch(self, engine: str, llm: str) -> str:
        if engine == llm:
            return "match"
        # 강한 불일치: 정반대 방향
        if {engine, llm} == {"BULLISH", "BEARISH"}:
            return "strong_miss"
        # 약한 불일치: 한쪽이 NEUTRAL
        return "weak_miss"

    # ── 퍼블릭 API ───────────────────────────────────────────────────────────

    def is_entry_blocked(self) -> bool:
        """강한 불일치로 진입 금지 여부."""
        with self._lock:
            return self._entry_blocked

    def is_half_mode(self) -> bool:
        """LLM 연속 실패로 half 모드 여부."""
        with self._lock:
            return self._half_mode

    def should_close_position(self) -> bool:
        """2회 연속 강한 불일치 → 포지션 청산 권고."""
        with self._lock:
            return self._force_neutral

    def get_size_factor(self) -> float:
        """권장 사이즈 팩터 (1.0=정상, 0.5=약한불일치, 0.0=진입금지)."""
        with self._lock:
            if self._entry_blocked:
                return 0.0
            if self._half_mode:
                return 0.5
            if self._status == ValidatorStatus.WEAK_MISS:
                return 0.5
            return 1.0

    def get_state(self) -> Dict:
        with self._lock:
            if self._entry_blocked:
                _sf = 0.0
            elif self._half_mode or self._status == ValidatorStatus.WEAK_MISS:
                _sf = 0.5
            else:
                _sf = 1.0
            return {
                "active":               bool(self._thread and self._thread.is_alive()),
                "status":               self._status,
                "last_llm_direction":   self._last_llm_dir,
                "last_llm_confidence":  round(self._last_llm_conf, 2),
                "last_llm_reason":      self._last_llm_reason,
                "last_check_ts":        self._last_check_ts,
                "last_result":          self._last_result,
                "fail_count":           self._fail_count,
                "strong_mismatch_count":self._mismatch_strong_count,
                "entry_blocked":        self._entry_blocked,
                "half_mode":            self._half_mode,
                "force_neutral":        self._force_neutral,
                "size_factor":          _sf,
            }
