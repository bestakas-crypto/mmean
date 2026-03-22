# engines/adoption_view_adapter.py
"""
AdoptionViewAdapter — market_view 변경 시 최적 adoption config 자동 교체

동작:
  1. CheckpointAnalyst(10:30~)가 provisional_regime 확정 후 on_view_change() 호출
  2. sim_adoptions에서 train_pattern = view 인 최적 adoption 조회
  3. SHADOW_MODE=True(기본): 로그만 남기고 실제 교체 없음
     SHADOW_MODE=False      : adoption의 config_json → ConfigManager.update() 적용

안전장치 (SHADOW_MODE=False 시 적용):
  - CONSECUTIVE_REQUIRED : 동일 view가 N회 연속으로 나올 때만 교체 (기본 2)
  - CONFIDENCE_REQUIRED  : confidence=HIGH 일 때만 교체
  - COOLDOWN_SEC         : 교체 후 최소 N초 대기 (기본 1800 = 30분)
  - MAX_SWAPS_PER_DAY    : 하루 최대 교체 횟수 (기본 3)
  - UNSTABLE_VIEWS       : config 교체 없이 size_multiplier 축소만

우선순위 (adoption 조회):
  1순위: status='adopted' AND stress_passed=1
  2순위: status='adopted'
  3순위: status='candidate'
  없으면: 현재 config 그대로 유지

장 종료 처리:
  reset_to_default() — 장 마감 시 DEFAULT_CONFIG 복원

state 키 (읽기 전용):
  adoption_view_active_id  : 현재 적용 중인 adoption id (없으면 None)
  adoption_view_active_pat : 현재 적용 중인 train_pattern
  adoption_view_fallback   : True = adoption 없어서 기본 config 사용 중
"""
from __future__ import annotations

import json
import logging
import sqlite3
import time
from datetime import datetime
from typing import Any, Dict, Optional

log = logging.getLogger("MMEAN.AdoptionViewAdapter")

# ── 안전장치 상수 ──────────────────────────────────────────────────────────
SHADOW_MODE         = True   # True=로그만 / False=실제 교체. adoption 충분 시 False로 전환.
CONSECUTIVE_REQUIRED = 2     # 동일 view 연속 N회 확인 후 교체
CONFIDENCE_REQUIRED  = True  # True=HIGH confidence 일 때만 교체
COOLDOWN_SEC         = 1800  # 교체 후 30분 쿨다운
MAX_SWAPS_PER_DAY    = 3     # 하루 최대 교체 횟수

# 불안정 view: config 교체 없이 size 축소만
UNSTABLE_VIEWS = {"VOLATILE", "SPIKE_REVERSAL"}

_MIN_VALID_TRADES = 3


class AdoptionViewAdapter:
    """
    market_view / provisional_regime → sim_adoptions → ConfigManager 자동 교체기.

    사용:
        adapter = AdoptionViewAdapter(runtime, sim_db_path)
        adapter.on_view_change("TREND_UP", confidence="HIGH")
        adapter.reset_to_default()  # 장 마감 시
    """

    def __init__(self, runtime: Any, sim_db_path: str) -> None:
        self._runtime        = runtime
        self._sim_db         = sim_db_path
        self._last_view: Optional[str] = None
        self._active_id: Optional[int] = None

        # 안전장치 상태
        self._consec_view: Optional[str] = None
        self._consec_count: int          = 0
        self._last_swap_ts: float        = 0.0
        self._today_swaps: int           = 0
        self._today_date: str            = ""

    # ── 공개 API ───────────────────────────────────────────────────────────

    def on_view_change(self, new_view: str, confidence: str = "LOW") -> None:
        """
        view 변경 시 호출.
        confidence: "HIGH" | "MID" | "LOW"
        """
        # 날짜 바뀌면 일일 카운터 초기화
        today = datetime.now().strftime("%Y-%m-%d")
        if today != self._today_date:
            self._today_date  = today
            self._today_swaps = 0

        if SHADOW_MODE:
            self._run_shadow(new_view, confidence)
            return

        # ── 실제 교체 로직 (SHADOW_MODE=False) ────────────────────────────
        if new_view in UNSTABLE_VIEWS:
            log.info(
                "AdoptionViewAdapter: view=%s 불안정 → config 교체 없음 (size 축소 권고)",
                new_view,
            )
            return

        # 연속 횟수 추적
        if new_view == self._consec_view:
            self._consec_count += 1
        else:
            self._consec_view  = new_view
            self._consec_count = 1

        if self._consec_count < CONSECUTIVE_REQUIRED:
            log.debug(
                "AdoptionViewAdapter: view=%s consec=%d/%d — 아직 대기",
                new_view, self._consec_count, CONSECUTIVE_REQUIRED,
            )
            return

        # confidence 검사
        if CONFIDENCE_REQUIRED and confidence != "HIGH":
            log.debug(
                "AdoptionViewAdapter: view=%s confidence=%s — HIGH 아니면 스킵",
                new_view, confidence,
            )
            return

        # 쿨다운 검사
        elapsed = time.time() - self._last_swap_ts
        if elapsed < COOLDOWN_SEC:
            log.debug(
                "AdoptionViewAdapter: 쿨다운 중 (%.0f/%.0f초 경과) — 스킵",
                elapsed, COOLDOWN_SEC,
            )
            return

        # 일일 한도 검사
        if self._today_swaps >= MAX_SWAPS_PER_DAY:
            log.info(
                "AdoptionViewAdapter: 일일 교체 한도(%d회) 초과 — 스킵",
                MAX_SWAPS_PER_DAY,
            )
            return

        # view 동일하면 스킵
        if new_view == self._last_view:
            return

        self._last_view = new_view
        self._do_swap(new_view)

    def reset_to_default(self) -> None:
        """장 마감 시 DEFAULT_CONFIG로 복원."""
        self._runtime.cfg_mgr.reset_to_default()
        self._last_view    = None
        self._active_id    = None
        self._consec_view  = None
        self._consec_count = 0
        self._update_state(None, None, fallback=True)
        log.info("AdoptionViewAdapter: DEFAULT_CONFIG 복원 완료")

    # ── 내부 로직 ──────────────────────────────────────────────────────────

    def _run_shadow(self, new_view: str, confidence: str) -> None:
        """SHADOW_MODE: 실제 교체 없이 로그만."""
        adoption = self._find_best_adoption(new_view)
        if adoption is None:
            log.info(
                "[SHADOW] view=%s — 매칭 adoption 없음 (train_pattern 불일치 또는 데이터 부족)",
                new_view,
            )
        else:
            log.info(
                "[SHADOW] view=%s conf=%s → adoption_id=%d (obj=%.4f trades=%d stress=%s) "
                "시뮬레이션 중 — 실제 교체 없음",
                new_view, confidence,
                adoption["id"],
                adoption["valid_obj"] or 0.0,
                adoption["valid_trades"] or 0,
                "OK" if adoption["stress_passed"] else "NO",
            )

    def _do_swap(self, new_view: str) -> None:
        """실제 adoption 조회 후 ConfigManager 교체."""
        adoption = self._find_best_adoption(new_view)

        if adoption is None:
            log.info(
                "AdoptionViewAdapter: view=%s 에 맞는 adoption 없음 — 현재 config 유지",
                new_view,
            )
            self._update_state(None, new_view, fallback=True)
            return

        try:
            patch: Dict[str, Any] = json.loads(adoption["config_json"])
        except (json.JSONDecodeError, TypeError) as e:
            log.warning(
                "AdoptionViewAdapter: config_json 파싱 실패 | id=%d | %s",
                adoption["id"], e,
            )
            return

        updated = self._runtime.cfg_mgr.update(patch)
        self._active_id    = adoption["id"]
        self._last_swap_ts = time.time()
        self._today_swaps += 1
        self._update_state(adoption["id"], new_view, fallback=False)

        log.info(
            "AdoptionViewAdapter: view=%s → adoption_id=%d 적용 (#%d회) | "
            "obj=%.4f trades=%d stress=%s | "
            "sim_tp=%s sim_sl=%s enter_score=%s",
            new_view, adoption["id"], self._today_swaps,
            adoption["valid_obj"] or 0.0,
            adoption["valid_trades"] or 0,
            "OK" if adoption["stress_passed"] else "NO",
            updated.get("sim_tp_ticks", "?"),
            updated.get("sim_sl_ticks", "?"),
            updated.get("enter_score", "?"),
        )

    # ── DB 조회 ────────────────────────────────────────────────────────────

    def _find_best_adoption(self, pattern: str) -> Optional[Any]:
        try:
            conn = sqlite3.connect(
                f"file:{self._sim_db}?mode=ro", uri=True, timeout=5
            )
            conn.row_factory = sqlite3.Row

            for query in (
                """SELECT id, config_json, valid_obj, valid_trades, stress_passed
                   FROM sim_adoptions
                   WHERE train_pattern = ? AND status = 'adopted' AND stress_passed = 1
                     AND (valid_trades IS NULL OR valid_trades >= ?)
                   ORDER BY valid_obj DESC LIMIT 1""",
                """SELECT id, config_json, valid_obj, valid_trades, stress_passed
                   FROM sim_adoptions
                   WHERE train_pattern = ? AND status = 'adopted'
                   ORDER BY valid_obj DESC LIMIT 1""",
                """SELECT id, config_json, valid_obj, valid_trades, stress_passed
                   FROM sim_adoptions
                   WHERE train_pattern = ? AND status = 'candidate'
                   ORDER BY valid_obj DESC LIMIT 1""",
            ):
                params = (pattern, _MIN_VALID_TRADES) if "stress_passed = 1" in query else (pattern,)
                row = conn.execute(query, params).fetchone()
                if row is not None:
                    conn.close()
                    return row

            conn.close()
            return None

        except Exception as e:
            log.warning("AdoptionViewAdapter DB 조회 실패: %s", e)
            return None

    # ── state 갱신 ─────────────────────────────────────────────────────────

    def _update_state(
        self,
        adoption_id: Optional[int],
        pattern: Optional[str],
        fallback: bool,
    ) -> None:
        try:
            with self._runtime.state_obj.engine_lock:
                self._runtime.state["adoption_view_active_id"]  = adoption_id
                self._runtime.state["adoption_view_active_pat"] = pattern
                self._runtime.state["adoption_view_fallback"]   = fallback
        except Exception as e:
            log.warning("AdoptionViewAdapter state 갱신 실패: %s", e)
