# engines/llm_ttl_manager.py
"""
LLM TTL 관리자 — weight_adjust 유효시간 관리

TTL 소멸 조건 (AND가 아닌 OR — 먼저 만족하는 쪽):
  조건 A: 다음 진입 의사결정 1회 발생 시 즉시 소멸
  조건 B: LLM 호출 후 4분(DEFAULT_TTL_SEC) 경과 시 만료

만료 시: weight_adjust_filtered = 0으로 초기화

이유:
  13:01 해석을 13:09 진입에 그대로 쓰면 이미 시장 맥락이 바뀌어 있을 수 있다.
  TTL 없이 진입 전 1회 호출 구조를 유지하면 오래된 해석이 주문에 영향을 준다.

검증 요구사항:
  - 조건 A(진입 소멸)와 조건 B(시간 만료) 모두 로그에 expire_reason 기록
  - get_active_weight() 호출마다 잔여 TTL 로그 기록 (DEBUG 레벨)
  - 운영 로그 grep: "LLMTTLManager" 키워드로 전체 TTL 이벤트 확인 가능
"""
from __future__ import annotations

import logging
import time
from dataclasses import dataclass
from typing import Optional

log = logging.getLogger("MMEAN.LLMTTLManager")

# ── TTL 기본값 ────────────────────────────────────────────────────
DEFAULT_TTL_SEC = 240   # 4분 (명세서 3~5분 중간값)

# ── 소멸 사유 상수 ────────────────────────────────────────────────
EXPIRE_REASON_ENTRY   = "entry_decision"   # 조건 A
EXPIRE_REASON_TIMEOUT = "time_elapsed"     # 조건 B


@dataclass
class TTLState:
    """현재 TTL 상태 스냅샷."""
    active:          bool
    weight_adjust:   float
    elapsed_sec:     float
    ttl_sec:         float
    remaining_sec:   float       # 남은 유효시간 (만료 시 0.0)
    expire_reason:   str         # "" | "entry_decision" | "time_elapsed"


class LLMTTLManager:
    """
    LLM weight_adjust TTL 관리.

    사용 패턴:
        ttl = LLMTTLManager()

        # LLM 호출 후
        ttl.set(weight_adjust=0.14)

        # 매 틱
        wa = ttl.get_active_weight()  # TTL 만료 시 자동 0.0 반환

        # 진입 의사결정 발생 시
        ttl.consume_on_entry()        # 즉시 소멸 (조건 A)

    운영 검증 방법:
        grep "LLMTTLManager" 로그파일 | 소멸 사유 분포 확인
        entry_decision vs time_elapsed 비율로 "오래된 판단 재사용" 여부 점검
    """

    def __init__(self, ttl_sec: float = DEFAULT_TTL_SEC) -> None:
        self.ttl_sec          = ttl_sec
        self._weight_adjust:  float          = 0.0
        self._set_time:       Optional[float] = None
        self._active:         bool            = False
        self._expire_reason:  str             = ""

    # ------------------------------------------------------------------
    def set(self, weight_adjust: float) -> None:
        """LLM 호출 직후 새 weight_adjust 등록 및 TTL 시작."""
        self._weight_adjust  = weight_adjust
        self._set_time       = time.monotonic()
        self._active         = True
        self._expire_reason  = ""
        log.info(
            "LLMTTLManager 등록 | weight_adjust=%+.4f | ttl=%.0fs",
            weight_adjust, self.ttl_sec,
        )

    def get_active_weight(self) -> float:
        """
        현재 활성 weight_adjust 반환.
        TTL 만료(조건 B) 시 자동 소멸 후 0.0 반환.
        DEBUG 레벨로 잔여 TTL 기록.
        """
        if not self._active:
            return 0.0

        elapsed   = time.monotonic() - self._set_time
        remaining = self.ttl_sec - elapsed

        if remaining <= 0:
            log.info(
                "LLMTTLManager 만료 [%s] | elapsed=%.1fs >= ttl=%.0fs"
                " | weight_adjust=%+.4f → 0",
                EXPIRE_REASON_TIMEOUT, elapsed, self.ttl_sec, self._weight_adjust,
            )
            self._expire(EXPIRE_REASON_TIMEOUT)
            return 0.0

        log.debug(
            "LLMTTLManager 활성 | wa=%+.4f | elapsed=%.1fs | remaining=%.1fs",
            self._weight_adjust, elapsed, remaining,
        )
        return self._weight_adjust

    def consume_on_entry(self) -> None:
        """
        조건 A: 진입 의사결정 1회 발생 → 즉시 소멸.
        진입 체결 여부와 무관하게, 의사결정 자체가 발생하면 소멸.
        (오래된 LLM 판단이 다음 진입 기회에 재사용되지 않도록)
        """
        if self._active:
            elapsed = time.monotonic() - self._set_time
            log.info(
                "LLMTTLManager 만료 [%s] | elapsed=%.1fs | weight_adjust=%+.4f → 0",
                EXPIRE_REASON_ENTRY, elapsed, self._weight_adjust,
            )
            self._expire(EXPIRE_REASON_ENTRY)

    def state(self) -> TTLState:
        """현재 TTL 상태 스냅샷 반환 (모니터링·대시보드용)."""
        elapsed   = (time.monotonic() - self._set_time) if self._set_time else 0.0
        remaining = max(0.0, self.ttl_sec - elapsed) if self._active else 0.0
        return TTLState(
            active=self._active,
            weight_adjust=self._weight_adjust if self._active else 0.0,
            elapsed_sec=elapsed,
            ttl_sec=self.ttl_sec,
            remaining_sec=remaining,
            expire_reason=self._expire_reason,
        )

    # ------------------------------------------------------------------
    def _expire(self, reason: str) -> None:
        self._expire_reason  = reason
        self._active         = False
        self._weight_adjust  = 0.0
        self._set_time       = None
