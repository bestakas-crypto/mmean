# [Directory] MMEAN
# [File] param_store.py
"""
LLM 파라미터 공유 저장소 — 운영 규칙서 v1 구현

[설계 원칙]
  - LLM: 비동기 write (ParamSnapshot atomic swap)
  - 실행 엔진: 동기 read (lock-free, 틱 시작 시 1회 고정)
  - baseline(Champion) / active(Challenger) 명시적 분리

[운영 규칙서 조항 매핑]
  §3  Champion vs Challenger
        get_champion() → baseline 고정값 (LLM 장전 기준)
        get_challenger() / get_safe() → active runtime 값
  §4  변경 제한
        MAX_PATCH_DELTA ±0.10/±0.20, MIN_DELTA 0.02, COOLDOWN_SEC 30분
  §5  Drift Control
        baseline 대비 30%/40% 초과 시 해당 키 기각, drift_report()
  §6  근거 코드 강제
        ALLOWED_REASON_CODES: 시장 상태 근거 + 시스템 이벤트만 허용
        FORBIDDEN_REASON_CODES: 손익 기반 코드 즉시 기각
        허용 목록에 없는 코드도 기각 (대체 없음)
  §7  Parameter TTL
        런타임 patch는 60분 후 baseline으로 자동 복귀
  §9  Guardrail
        1시간 내 4회 patch → param_update_locked, unlock_guardrail() 해제
  §10 Staleness
        10분 WARN + active 유지, 60분 baseline 복귀

[사용 패턴]
  from param_store import get_param_store
  store = get_param_store(db_path)

  # 장 시작 전 (llm_controller — 하루 1회, created_new=True 일 때만)
  store.set_baseline_only(params, source="llm_strategy", session_date="2026-03-15")
  store.reset_active_to_baseline()       # 신규 생성일 때만 호출할 것

  # 실행 엔진 — 틱 시작 시 1회 read (결정성 보장)
  params = store.get_safe()              # TTL/staleness guard 포함

  # 장중 LLM (llm_filter — 5분 주기)
  store.apply_patch(patch, source="llm_filter", reason_code="ema_align_up")

  # 운영 모니터링
  report = store.get_drift_report()      # drift_alert=True면 점검
  store.unlock_guardrail("manual")       # Guardrail 해제

[hash 설계]
  param_hash   : 순수 파라미터 값만 hash → 거래 AB 분석 fingerprint (핵심)
  config_hash  : param + source + version hash → 전체 상태 추적용
  source/reason_code 달라도 파라미터 값이 같으면 param_hash는 동일
"""
from __future__ import annotations

import hashlib
import json
import logging
import sqlite3
import threading
import time
from dataclasses import dataclass, replace
from typing import Any, Dict, List, Optional

log = logging.getLogger("MMEAN.ParamStore")

# ===================================================================
# §10 Staleness 임계값
# ===================================================================
STALE_WARN_SEC     = 600    # 10분: WARN + active 유지
STALE_FALLBACK_SEC = 3600   # 60분: ERROR + baseline 복귀

# ===================================================================
# §9 Guardrail
# ===================================================================
GUARDRAIL_PATCH_WINDOW_SEC = 3600   # 1시간 내
GUARDRAIL_PATCH_COUNT_MAX  = 4      # 4회 → locked

# ===================================================================
# §4 변경 주기 제한 (쿨다운)
# ===================================================================
COOLDOWN_SEC = 1800   # 30분

# ===================================================================
# §4 MIN_DELTA — 최소 변경 단위 (운영 규칙서 기준)
# ===================================================================
MIN_DELTA: Dict[str, float] = {
    "trend_weight_long":         0.02,
    "trend_weight_short":        0.02,
    "trend_ema_align_score":     0.05,
    "trend_ema_slope_score":     0.05,
    "trend_ema_slope_threshold": 0.005,
}

# ===================================================================
# §4 변경폭 제한 — 한 번에 허용되는 최대 변경폭
# ===================================================================
MAX_PATCH_DELTA: Dict[str, float] = {
    "trend_weight_long":         0.10,
    "trend_weight_short":        0.10,
    "trend_ema_align_score":     0.20,
    "trend_ema_slope_score":     0.20,
    "trend_ema_slope_threshold": 0.01,
}

# ===================================================================
# §5 Drift Control — baseline 대비 최대 허용 drift 비율
# ===================================================================
MAX_DRIFT_RATIO: Dict[str, float] = {
    "trend_weight_long":         0.30,
    "trend_weight_short":        0.30,
    "trend_ema_align_score":     0.40,
    "trend_ema_slope_score":     0.40,
    "trend_ema_slope_threshold": 0.50,
}

# ===================================================================
# §6 LLM 변경 허용/금지 근거 코드
#
# [분류 원칙]
#   source = 어디서 왔나 (채널) → ALLOWED_SOURCES 참고
#   reason_code = 왜 바꿨나 (근거) → 아래 목록만 허용
#
# [reason_code 분류]
#   A. 시장 상태 근거 (§6 핵심 허용)
#      → LLM이 파라미터를 조정할 때 반드시 이 중 하나여야 함
#      → 분석 시 "어떤 시장 근거가 성과에 유효했나" 추적 가능
#
#   B. 시스템 이벤트 (자동화 흐름 전용)
#      → TTL 복귀, baseline 설정, fallback 등 코드 내부에서만 사용
#      → LLM이 직접 사용하면 안 됨 (OpportunitySchema에서 걸림)
#
#   C. 하위 호환 코드 (기존 reason_code → 점진적으로 A 코드로 교체 권장)
#
# [금지 목록] FORBIDDEN_REASON_CODES: 손익/결과 기반 코드 → 즉시 기각
# ===================================================================
ALLOWED_REASON_CODES = frozenset({
    # ── A. 시장 상태 근거 ──────────────────────────────────────────
    # EMA
    "ema_align_up",        # EMA20 > EMA60 정배열 전환
    "ema_align_down",      # EMA20 < EMA60 역배열 전환
    "ema_slope_expand",    # EMA slope 확대 (추세 가속)
    "ema_slope_contract",  # EMA slope 수축 (추세 둔화)
    # VWAP
    "vwap_above",          # 현재가 > VWAP (매수 우위)
    "vwap_below",          # 현재가 < VWAP (매도 우위)
    # ATR
    "atr_expand",          # 변동성 확대
    "atr_contract",        # 변동성 수축
    # Foreign / OI
    "foreign_flow_long",   # 외국인 순매수 강화
    "foreign_flow_short",  # 외국인 순매도 강화
    "oi_buildup_long",     # OI 증가 + 롱 방향
    "oi_buildup_short",    # OI 증가 + 숏 방향
    # Basis / Volume / ADX
    "basis_expand",        # basis 확대
    "basis_contract",      # basis 수축
    "volume_burst",        # 거래량 급증
    "adx_strength",        # ADX 강도 변화
    "regime_shift",        # 시장 국면 전환 (복합 근거)

    # ── B. 시스템 이벤트 (코드 내부 자동화 전용) ───────────────────
    "baseline_set",        # 장전 baseline 신규 설정
    "baseline_override",   # baseline 강제 재설정 (운영자 명시)
    "ttl_expired_revert",  # TTL 만료 → baseline 자동 복귀
    "stale_fallback",      # stale 상태 → baseline fallback
    "default_fallback",    # schema default / 오류 상황

    # ── C. 하위 호환 (점진적으로 A 코드로 교체 권장) ───────────────
    "ema_align",           # → ema_align_up / ema_align_down 사용 권장
    "vwap_position",       # → vwap_above / vwap_below 사용 권장
    "atr_volatility",      # → atr_expand / atr_contract 사용 권장
    "foreign_flow",        # → foreign_flow_long / short 사용 권장
    "oi_change",           # → oi_buildup_long / short 사용 권장
})

FORBIDDEN_REASON_CODES = frozenset({
    "pnl_loss", "consecutive_loss", "recent_trade",
    "drawdown", "equity_curve",
})

# §4 허용 source 채널
ALLOWED_SOURCES = frozenset({
    "llm_strategy", "llm_filter",
    "manual", "system_reset", "api_config", "api_config_reset",
    "ttl_revert", "fallback", "baseline_sync", "baseline_set",
    "default",
})

# 연속 실패 알람 임계값
CONSECUTIVE_FAIL_WARN  = 3
CONSECUTIVE_FAIL_ERROR = 10

# TTL 만료 후 복귀 source 태그 (stale fallback 제외 대상)
_TTL_EXEMPT_SOURCES = frozenset({
    "default", "baseline_ttl", "manual",
    "api_config", "api_config_reset", "baseline_set",
})


# ===================================================================
# ParamSnapshot — frozen dataclass (immutable)
# ===================================================================
@dataclass(frozen=True)
class ParamSnapshot:
    """EMA 추세 파라미터 스냅샷. frozen=True → atomic swap 보장."""
    trend_weight_long:          float
    trend_weight_short:         float
    trend_ema_align_score:      float
    trend_ema_slope_score:      float
    trend_ema_slope_threshold:  float
    updated_at:                 float
    version:                    int
    config_hash:                str   # snapshot_hash: 전체 상태 fingerprint
    source:                     str
    param_hash:                 str = ""  # 순수 파라미터 값만 hash (거래 분석용)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "trend_weight_long":         self.trend_weight_long,
            "trend_weight_short":        self.trend_weight_short,
            "trend_ema_align_score":     self.trend_ema_align_score,
            "trend_ema_slope_score":     self.trend_ema_slope_score,
            "trend_ema_slope_threshold": self.trend_ema_slope_threshold,
            "updated_at":                self.updated_at,
            "version":                   self.version,
            "config_hash":               self.config_hash,
            "source":                    self.source,
            "param_hash":                self.param_hash,
        }

    def to_param_dict(self) -> Dict[str, float]:
        """순수 파라미터 값만 반환 — param_hash 생성 기준."""
        return {
            "trend_weight_long":         self.trend_weight_long,
            "trend_weight_short":        self.trend_weight_short,
            "trend_ema_align_score":     self.trend_ema_align_score,
            "trend_ema_slope_score":     self.trend_ema_slope_score,
            "trend_ema_slope_threshold": self.trend_ema_slope_threshold,
        }


def _default_snapshot() -> ParamSnapshot:
    params = {
        "trend_weight_long":         0.8,
        "trend_weight_short":        0.6,
        "trend_ema_align_score":     1.5,
        "trend_ema_slope_score":     0.5,
        "trend_ema_slope_threshold": 0.02,
    }
    return ParamSnapshot(
        **params,
        updated_at  = 0.0,
        version     = 0,
        config_hash = "default",
        source      = "default",
        param_hash  = _make_hash(params),
    )


def _make_param_hash(snap_or_dict) -> str:
    """순수 파라미터 값만으로 hash 생성 (거래 분석용 fingerprint)."""
    if isinstance(snap_or_dict, dict):
        data = snap_or_dict
    else:
        data = snap_or_dict.to_param_dict()
    return _make_hash(data)


def _make_hash(data: Dict[str, Any]) -> str:
    raw = json.dumps(data, sort_keys=True, ensure_ascii=False)
    return hashlib.sha1(raw.encode()).hexdigest()[:8]


# ===================================================================
# ParamStore
# ===================================================================
class ParamStore:
    """
    운영 규칙서 v1 기반 LLM 파라미터 공유 저장소.

    Champion(고정) / Challenger(동적) 이중 구조:
      - Champion  : baseline 값 고정, LLM 변경 없음  (§3)
      - Challenger: ParamStore 기반 runtime tuning   (§3)

    모든 변경은 §4~§9 규칙을 통과해야 적용됨.
    """

    def __init__(self, db_path: str) -> None:
        self._db_path = db_path
        self._lock    = threading.Lock()

        # §3 Champion / Challenger 스냅샷
        self._baseline_snapshot: ParamSnapshot = _default_snapshot()
        self._snapshot:          ParamSnapshot = _default_snapshot()
        self._baseline_session_date: str = ""  # session_date 기준 1회 제한용

        # §9 Guardrail 상태
        self._patch_timestamps: List[float] = []
        self._update_locked:    bool = False
        self._locked_reason:    str  = ""

        # 연속 실패 카운터
        self._consecutive_fails: int = 0

        # §4 쿨다운 (키별 마지막 변경 시각)
        self._last_patch_time: Dict[str, float] = {}

        self._conn = sqlite3.connect(db_path, timeout=2, check_same_thread=False)
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA synchronous=NORMAL")
        self._conn.execute("PRAGMA busy_timeout=2000")
        self._ensure_table()
        log.info("ParamStore 초기화 완료 | db=%s", db_path)

    # ------------------------------------------------------------------
    # §3 Champion / Challenger 읽기 (lock-free)
    # ------------------------------------------------------------------
    def get(self) -> ParamSnapshot:
        return self._snapshot

    def get_champion(self) -> ParamSnapshot:
        """Champion — baseline 고정값 반환 (LLM 변경 없음)."""
        return self._baseline_snapshot

    def get_challenger(self) -> ParamSnapshot:
        """Challenger — 동적 파라미터 현재값 반환."""
        return self._snapshot

    def get_safe(self) -> ParamSnapshot:
        """
        §7 TTL + §10 Staleness guard 적용 후 Challenger 스냅샷 반환.
        실행 엔진 틱 시작 시 1회 호출.

        TTL(60분): runtime patch 만료 → baseline 자동 복귀  (§7)
        1차 stale(10분): active 유지 + WARN 기록            (§10)
        """
        snap = self._snapshot
        now  = time.time()

        if snap.updated_at == 0.0:
            return snap

        age = now - snap.updated_at

        # §7 TTL: LLM이 변경한 값은 60분 후 baseline 복귀
        if snap.source not in _TTL_EXEMPT_SOURCES and age >= STALE_FALLBACK_SEC:
            baseline = self._baseline_snapshot
            ttl_snap = replace(
                baseline,
                updated_at  = now,
                version     = snap.version + 1,
                config_hash = _make_hash(baseline.to_dict()),
                source      = "baseline_ttl",
            )
            with self._lock:
                self._snapshot = ttl_snap
            self._record_event(
                event_type="stale_fallback",
                action_taken="baseline_ttl_restore",
                stale_age_sec=age,
                snap=ttl_snap,
            )
            log.warning(
                "ParamStore TTL 만료 → baseline 복귀 | age=%.0fs | v=%d",
                age, ttl_snap.version,
            )
            return ttl_snap

        if age >= STALE_WARN_SEC:
            self._record_event(
                event_type="stale_warn",
                action_taken="kept_active_snapshot",
                stale_age_sec=age,
                snap=snap,
            )
            log.warning(
                "ParamStore 1차 stale | age=%.0fs → active 유지 | v=%d",
                age, snap.version,
            )

        return snap

    # ------------------------------------------------------------------
    # §3 Baseline 설정 — baseline only / active reset 분리
    # ------------------------------------------------------------------

    def set_baseline_only(
        self,
        params: Dict[str, Any],
        source: str = "llm_strategy",
        session_date: str = "",
        allow_override: bool = False,
    ) -> ParamSnapshot:
        """
        당일 Champion 기준값(baseline)만 설정.
        active challenger는 건드리지 않음 — reset_active_to_baseline() 별도 호출 필요.

        [session_date 기준 1회 제한]
          - 같은 session_date에 이미 baseline이 설정돼 있으면 재설정 금지
          - allow_override=True일 때만 재설정 허용 (source 무관)
          - override 시 event_type="baseline_override"로 audit log 강제 기록
          - 강제 갱신이 필요한 경우: allow_override=True + source="manual" 권장
            (source는 코드가 강제하지 않지만, 분석 추적을 위해 명시 권장)

        [호출 정책]
          - llm_controller: 하루 1회, created_new=True일 때만 호출
          - _sync_trend_to_param_store (api_config): allow_override=True 없이 호출
            → 당일 baseline 미설정 상태에서만 반영됨 (의도적)
        """
        today = session_date or time.strftime("%Y-%m-%d")

        # session_date 기준 1회 제한
        existing_date = self._baseline_session_date
        if existing_date == today and not allow_override:
            log.info(
                "ParamStore baseline 재설정 차단 | session_date=%s 이미 존재 | "
                "override 필요 시 allow_override=True + source=manual",
                today,
            )
            return self._baseline_snapshot

        clamped = self._clamp(params)
        if not clamped:
            log.warning("ParamStore.set_baseline_only: 유효 파라미터 없음")
            return self._baseline_snapshot

        now = time.time()
        base = _default_snapshot()
        merged = {
            "trend_weight_long":         clamped.get("trend_weight_long",         base.trend_weight_long),
            "trend_weight_short":        clamped.get("trend_weight_short",        base.trend_weight_short),
            "trend_ema_align_score":     clamped.get("trend_ema_align_score",     base.trend_ema_align_score),
            "trend_ema_slope_score":     clamped.get("trend_ema_slope_score",     base.trend_ema_slope_score),
            "trend_ema_slope_threshold": clamped.get("trend_ema_slope_threshold", base.trend_ema_slope_threshold),
        }
        ph = _make_param_hash(merged)
        new_hash = _make_hash({**merged, "source": source, "version": self._baseline_snapshot.version + 1})
        new_baseline = ParamSnapshot(
            **merged,
            updated_at  = now,
            version     = self._baseline_snapshot.version + 1,
            config_hash = new_hash,
            source      = source,
            param_hash  = ph,
        )
        with self._lock:
            self._baseline_snapshot = new_baseline
            self._baseline_session_date = today

        event_type = "baseline_override" if allow_override else "baseline_set"
        self._record_event(
            event_type=event_type,
            action_taken="applied",
            snap=new_baseline,
            source=source,
            reason_code="baseline_override" if allow_override else "baseline_set",
        )
        log.info(
            "ParamStore baseline 설정 | v=%d | param_hash=%s | "
            "tw_long=%.2f tw_short=%.2f | override=%s",
            new_baseline.version, ph,
            new_baseline.trend_weight_long, new_baseline.trend_weight_short,
            allow_override,
        )
        return new_baseline

    def reset_active_to_baseline(
        self,
        source: str = "baseline_sync",
        reason_code: str = "baseline_set",
    ) -> ParamSnapshot:
        """
        active challenger를 baseline 기준으로 재설정.
        쿨다운/patch_history 초기화.
        반드시 명시적 호출만 허용 — set_baseline_only()는 자동 호출 안 함.
        """
        with self._lock:
            baseline = self._baseline_snapshot
            self._snapshot = baseline
            self._last_patch_time.clear()

        self._record_event(
            event_type="active_reset",
            action_taken="reset_to_baseline",
            snap=baseline,
            source=source,
            reason_code=reason_code,
        )
        log.info(
            "ParamStore active → baseline 재설정 | v=%d | param_hash=%s",
            baseline.version, baseline.param_hash,
        )
        return baseline

    # 하위 호환용 alias — 기존 set_baseline() 호출 코드가 있으면 경고 후 위임
    def set_baseline(
        self,
        params: Dict[str, Any],
        source: str = "llm_strategy",
        session_date: str = "",
    ) -> ParamSnapshot:
        """
        [Deprecated] set_baseline_only() 사용 권장.
        기존 호환성 유지 — baseline만 설정하고 active는 건드리지 않음.
        """
        log.debug("ParamStore.set_baseline() 호출 → set_baseline_only()로 위임")
        return self.set_baseline_only(params=params, source=source, session_date=session_date)

    # ------------------------------------------------------------------
    # §4 Patch 적용 (LLM 스레드에서 호출)
    # ------------------------------------------------------------------
    def apply_patch(
        self,
        patch: Dict[str, Any],
        source: str = "llm",
        provider: str = "unknown",
        raw_excerpt: Optional[str] = None,
        layer: str = "opportunity",
        reason_code: str = "regime_shift",
    ) -> bool:
        """
        LLM 파라미터 패치 적용.

        통과 조건 (순서대로):
          0. source 화이트리스트 확인 (ALLOWED_SOURCES)
          1. §9 Guardrail locked 확인
          2. §6 reason_code — 허용 목록 확인 + 금지 목록 확인
          3. 빈 패치 검사
          4. schema clamp + 절대 범위 제한
          5. §4 변경폭 제한 (MAX_PATCH_DELTA)
          6. §5 Drift 한계 초과 검사 (키별 기각)
          7. §4 쿨다운 30분 (키별 필터)
          8. MIN_DELTA 필터
          9. atomic swap + param_hash 갱신 + §9 Guardrail 카운터
        """
        # 0. source 화이트리스트
        if source not in ALLOWED_SOURCES:
            log.warning("ParamStore 허용되지 않은 source | source=%s", source)
            self._record_event("invalid_source", "skipped_update",
                               self._snapshot, provider=provider, layer=layer,
                               reason_code=reason_code)
            return False

        # 1. §9 Guardrail
        if self._update_locked:
            log.warning("ParamStore LOCKED | reason=%s | provider=%s",
                        self._locked_reason, provider)
            self._record_event("guardrail_blocked", "skipped_update",
                               self._snapshot, provider=provider, layer=layer)
            return False

        # 2. §6 reason_code — 금지 먼저, 그다음 허용 목록 강제
        if reason_code in FORBIDDEN_REASON_CODES:
            log.warning("ParamStore 금지 근거 | reason_code=%s", reason_code)
            self._record_event("forbidden_reason", "skipped_update",
                               self._snapshot, provider=provider, layer=layer,
                               blocked_reason=f"forbidden_reason_code:{reason_code}")
            return False
        if reason_code not in ALLOWED_REASON_CODES:
            # 대체하지 않고 기각 — 분석 품질 보호
            # 어떤 잘못된 reason이 왔는지 blocked_reason으로 기록
            log.warning(
                "ParamStore 허용되지 않은 reason_code 기각 | reason_code=%s "
                "(hint: ALLOWED_REASON_CODES 확인)",
                reason_code,
            )
            self._record_event("invalid_reason_code", "skipped_update",
                               self._snapshot, provider=provider, layer=layer,
                               blocked_reason=f"invalid_reason_code:{reason_code}")
            return False

        # 3. 빈 패치
        if not patch or not isinstance(patch, dict):
            self._handle_fail("empty_patch", provider, layer, raw_excerpt)
            return False

        # 4. clamp
        try:
            clamped = self._clamp(patch)
        except Exception as e:
            log.warning("ParamStore clamp 실패: %s", e)
            self._handle_fail("validation_fail", provider, layer, raw_excerpt)
            return False

        if not clamped:
            self._handle_fail("empty_patch", provider, layer, raw_excerpt)
            return False

        with self._lock:
            current  = self._snapshot
            baseline = self._baseline_snapshot
            now      = time.time()

            # 5. §4 변경폭 제한
            over_limit = self._check_patch_delta(clamped, current)
            if over_limit:
                log.warning("ParamStore 변경폭 초과 기각 | keys=%s", over_limit)
                self._record_event("patch_delta_exceeded", "skipped_update",
                                   current, provider=provider, layer=layer)
                return False

            # 6. §5 Drift — 초과 키만 제거
            drift_violations = self._check_drift(clamped, baseline)
            if drift_violations:
                log.warning("ParamStore Drift 초과 키 기각 | %s", drift_violations)
                for k in drift_violations:
                    clamped.pop(k, None)
                if not clamped:
                    self._record_event("drift_exceeded", "skipped_update",
                                       current, provider=provider, layer=layer)
                    return False

            # 7. §4 쿨다운 (키별 필터)
            cooldown_skipped = [
                k for k in list(clamped)
                if now - self._last_patch_time.get(k, 0.0) < COOLDOWN_SEC
            ]
            for k in cooldown_skipped:
                del clamped[k]
            if not clamped:
                self._record_event("cooldown_blocked", "skipped_update",
                                   current, provider=provider, layer=layer)
                return False

            # 8. MIN_DELTA
            if not self._has_significant_change(current, clamped):
                self._record_event("update_skip", "skipped_update",
                                   current, provider=provider, layer=layer)
                return False

            # 9. atomic swap
            # param_hash: 순수 파라미터 값만 (거래 분석용 fingerprint)
            # config_hash: 전체 상태 fingerprint (source/version 포함)
            new_params = {
                k: clamped.get(k, getattr(current, k))
                for k in (
                    "trend_weight_long", "trend_weight_short",
                    "trend_ema_align_score", "trend_ema_slope_score",
                    "trend_ema_slope_threshold",
                )
            }
            new_ph   = _make_param_hash(new_params)
            new_hash = _make_hash({**new_params, "source": source, "version": current.version + 1})
            new_snap = replace(
                current,
                **{k: clamped[k] for k in clamped},
                updated_at  = now,
                version     = current.version + 1,
                config_hash = new_hash,
                source      = source,
                param_hash  = new_ph,
            )
            self._snapshot          = new_snap
            self._consecutive_fails = 0
            for k in clamped:
                self._last_patch_time[k] = now

            # §9 Guardrail 카운터
            self._patch_timestamps.append(now)
            self._patch_timestamps = [
                t for t in self._patch_timestamps
                if now - t <= GUARDRAIL_PATCH_WINDOW_SEC
            ]
            if len(self._patch_timestamps) >= GUARDRAIL_PATCH_COUNT_MAX:
                self._update_locked = True
                self._locked_reason = (
                    f"{GUARDRAIL_PATCH_COUNT_MAX}회 연속 patch — "
                    f"unlock_guardrail() 호출 필요"
                )
                log.error(
                    "ParamStore GUARDRAIL LOCKED | %d회 / %ds",
                    len(self._patch_timestamps), GUARDRAIL_PATCH_WINDOW_SEC,
                )
                self._record_event("guardrail_locked", "locked",
                                   new_snap, provider=provider, layer=layer)

            self._record_event(
                event_type="update_success",
                action_taken="applied",
                snap=new_snap,
                config_hash_before=current.config_hash,
                provider=provider,
                layer=layer,
                reason_code=reason_code,
                before_param_hash=current.param_hash,
                after_param_hash=new_snap.param_hash,
                changed_fields=list(clamped.keys()),
            )
            log.info(
                "ParamStore 업데이트 | v=%d | param_hash=%s → %s | src=%s | "
                "tw_long=%.2f tw_short=%.2f",
                new_snap.version, current.param_hash, new_ph, source,
                new_snap.trend_weight_long, new_snap.trend_weight_short,
            )
            return True

    def record_parse_fail(
        self,
        provider: str = "unknown",
        layer: str = "opportunity",
        raw_excerpt: Optional[str] = None,
    ) -> None:
        self._handle_fail("parse_fail", provider, layer, raw_excerpt)

    # ------------------------------------------------------------------
    # §9 Guardrail 수동 해제
    # ------------------------------------------------------------------
    def unlock_guardrail(self, reason: str = "manual_unlock") -> None:
        """운영자가 수동으로 Guardrail을 해제할 때 호출."""
        with self._lock:
            self._update_locked = False
            self._locked_reason = ""
            self._patch_timestamps.clear()
        self._record_event("guardrail_unlocked", "unlocked",
                           self._snapshot, layer="manual", reason_code=reason)
        log.info("ParamStore Guardrail 해제 | reason=%s", reason)

    # ------------------------------------------------------------------
    # §5 Drift Report
    # ------------------------------------------------------------------
    def get_drift_report(self) -> Dict[str, Any]:
        """
        Challenger와 baseline(Champion) 간 drift 보고서.
        매일 운영자가 확인. drift_alert=True면 점검 필요.
        """
        snap     = self._snapshot
        baseline = self._baseline_snapshot
        now      = time.time()
        drift: Dict[str, Any] = {}
        max_drift_pct = 0.0

        for k in (
            "trend_weight_long", "trend_weight_short",
            "trend_ema_align_score", "trend_ema_slope_score",
            "trend_ema_slope_threshold",
        ):
            anchor  = getattr(baseline, k, None)
            current = getattr(snap, k, None)
            if anchor is None or current is None:
                continue
            delta    = current - anchor
            pct      = abs(delta / anchor * 100) if anchor != 0 else 0.0
            limit    = MAX_DRIFT_RATIO.get(k, 0.30) * 100
            max_drift_pct = max(max_drift_pct, pct)
            drift[k] = {
                "anchor":    round(anchor,  4),
                "current":   round(current, 4),
                "delta":     round(delta,   4),
                "drift_pct": round(pct,     1),
                "limit_pct": round(limit,   1),
                "exceeded":  pct > limit,
            }

        return {
            "snapshot_version":   snap.version,
            "baseline_version":   baseline.version,
            "snapshot_source":    snap.source,
            "updated_at":         snap.updated_at,
            "age_sec":            round(now - snap.updated_at, 1) if snap.updated_at else None,
            "max_drift_pct":      round(max_drift_pct, 1),
            "drift_alert":        max_drift_pct > 20.0,
            "update_locked":      self._update_locked,
            "locked_reason":      self._locked_reason,
            "consecutive_fails":  self._consecutive_fails,
            "recent_patch_count": len(self._patch_timestamps),
            "drift":              drift,
        }

    # ------------------------------------------------------------------
    # 상태 조회
    # ------------------------------------------------------------------
    def get_status(self) -> Dict[str, Any]:
        snap = self._snapshot
        now  = time.time()
        age  = (now - snap.updated_at) if snap.updated_at > 0 else None
        return {
            "version":           snap.version,
            "config_hash":       snap.config_hash,
            "source":            snap.source,
            "updated_at":        snap.updated_at,
            "stale_age_sec":     round(age, 1) if age is not None else None,
            "is_stale_warn":     (age >= STALE_WARN_SEC)     if age else False,
            "is_stale_fallback": (age >= STALE_FALLBACK_SEC) if age else False,
            "update_locked":     self._update_locked,
            "locked_reason":     self._locked_reason,
            "consecutive_fails": self._consecutive_fails,
            "params":            snap.to_dict(),
            "baseline_params":   self._baseline_snapshot.to_dict(),
        }

    def get_recent_events(self, limit: int = 20) -> list:
        try:
            cur = self._conn.execute(
                "SELECT ts, layer, event_type, action_taken, stale_age_sec, "
                "consecutive_fails, config_hash_before, config_hash_after "
                "FROM llm_param_events ORDER BY id DESC LIMIT ?",
                (limit,),
            )
            return [dict(zip([c[0] for c in cur.description], row))
                    for row in cur.fetchall()]
        except Exception as e:
            log.warning("ParamStore.get_recent_events 오류: %s", e)
            return []

    def close(self) -> None:
        try:
            self._conn.close()
        except Exception:
            pass

    # ------------------------------------------------------------------
    # 내부 헬퍼
    # ------------------------------------------------------------------
    def _handle_fail(
        self,
        event_type: str,
        provider: str,
        layer: str,
        raw_excerpt: Optional[str],
    ) -> None:
        with self._lock:
            self._consecutive_fails += 1
            fails = self._consecutive_fails
            snap  = self._snapshot
        excerpt = (raw_excerpt or "")[:500]
        self._record_event(
            event_type=event_type,
            action_taken="kept_active_snapshot",
            snap=snap,
            provider=provider,
            layer=layer,
            raw_excerpt=excerpt,
            consecutive_fails=fails,
        )
        if fails >= CONSECUTIVE_FAIL_ERROR:
            log.error("ParamStore 연속 실패 %d회 | type=%s | provider=%s",
                      fails, event_type, provider)
        elif fails >= CONSECUTIVE_FAIL_WARN:
            log.warning("ParamStore 연속 실패 %d회 | type=%s | provider=%s",
                        fails, event_type, provider)
        else:
            log.debug("ParamStore 단발 실패 | type=%s | provider=%s",
                      event_type, provider)

    def _clamp(self, patch: Dict[str, Any]) -> Dict[str, Any]:
        SCHEMA = {
            "trend_weight_long":         (float, 0.0,   2.0),
            "trend_weight_short":        (float, 0.0,   2.0),
            "trend_ema_align_score":     (float, 0.5,   3.0),
            "trend_ema_slope_score":     (float, 0.0,   1.5),
            "trend_ema_slope_threshold": (float, 0.005, 0.1),
        }
        out: Dict[str, Any] = {}
        for key, (typ, lo, hi) in SCHEMA.items():
            if key not in patch:
                continue
            try:
                val = typ(patch[key])
                out[key] = round(max(lo, min(hi, val)), 4)
            except (TypeError, ValueError):
                pass
        return out

    def _check_patch_delta(
        self, clamped: Dict[str, Any], current: ParamSnapshot
    ) -> List[str]:
        """§4 변경폭 제한 — 초과 키 목록. 부동소수점 오차 방지를 위해 round."""
        violations = []
        for k, val in clamped.items():
            cur_val = getattr(current, k, None)
            if cur_val is None:
                continue
            delta = round(abs(float(val) - float(cur_val)), 6)
            if delta > MAX_PATCH_DELTA.get(k, 0.10):
                violations.append(k)
        return violations

    def _check_drift(
        self, clamped: Dict[str, Any], baseline: ParamSnapshot
    ) -> List[str]:
        """§5 Drift — 적용 후 drift 한계 초과 키 목록."""
        violations = []
        for k, val in clamped.items():
            anchor = getattr(baseline, k, None)
            if anchor is None or anchor == 0:
                continue
            if abs(float(val) - float(anchor)) / abs(float(anchor)) > MAX_DRIFT_RATIO.get(k, 0.30):
                violations.append(k)
        return violations

    def _has_significant_change(
        self, current: ParamSnapshot, clamped: Dict[str, Any]
    ) -> bool:
        for k, val in clamped.items():
            cur_val = getattr(current, k, None)
            if cur_val is None:
                return True
            delta = round(abs(float(val) - float(cur_val)), 6)
            if delta >= MIN_DELTA.get(k, 0.0):
                return True
        return False

    def _record_event(
        self,
        event_type: str,
        action_taken: str,
        snap: ParamSnapshot,
        config_hash_before: Optional[str] = None,
        provider: str = "",
        layer: str = "",
        raw_excerpt: Optional[str] = None,
        stale_age_sec: float = 0.0,
        consecutive_fails: int = 0,
        source: str = "",
        reason_code: str = "",
        before_param_hash: str = "",
        after_param_hash: str = "",
        changed_fields: Optional[List[str]] = None,
        blocked_reason: str = "",
    ) -> None:
        try:
            self._conn.execute(
                """
                INSERT INTO llm_param_events (
                    ts, layer, event_type, provider, error_type,
                    attempt_count, config_hash_before, config_hash_after,
                    stale_age_sec, snapshot_version, raw_excerpt,
                    action_taken, consecutive_fails, session_date,
                    source, reason_code,
                    before_param_hash, after_param_hash,
                    changed_fields_json, blocked_reason
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    time.strftime("%Y-%m-%d %H:%M:%S"),
                    layer, event_type, provider,
                    event_type if "fail" in event_type else (blocked_reason or ""),
                    consecutive_fails,
                    config_hash_before or snap.config_hash,
                    snap.config_hash if event_type == "update_success" else None,
                    round(stale_age_sec, 1),
                    snap.version,
                    (raw_excerpt or "")[:500] or None,
                    action_taken,
                    consecutive_fails,
                    time.strftime("%Y-%m-%d"),
                    source or snap.source,
                    reason_code or "",
                    before_param_hash or "",
                    after_param_hash or snap.param_hash,
                    json.dumps(changed_fields or [], ensure_ascii=False),
                    blocked_reason or "",
                ),
            )
            self._conn.commit()
        except Exception as e:
            log.warning("ParamStore._record_event 오류: %s", e)

    def _ensure_table(self) -> None:
        try:
            self._conn.execute("""
                CREATE TABLE IF NOT EXISTS llm_param_events (
                    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
                    ts                  TEXT    NOT NULL,
                    layer               TEXT    NOT NULL,
                    event_type          TEXT    NOT NULL,
                    provider            TEXT,
                    error_type          TEXT,
                    attempt_count       INTEGER DEFAULT 0,
                    config_hash_before  TEXT,
                    config_hash_after   TEXT,
                    stale_age_sec       REAL    DEFAULT 0,
                    snapshot_version    INTEGER DEFAULT 0,
                    raw_excerpt         TEXT,
                    action_taken        TEXT,
                    consecutive_fails   INTEGER DEFAULT 0,
                    session_date        TEXT,
                    source              TEXT,
                    reason_code         TEXT,
                    before_param_hash   TEXT,
                    after_param_hash    TEXT,
                    changed_fields_json TEXT,
                    blocked_reason      TEXT
                )
            """)
            # 기존 DB 마이그레이션 (컬럼 없으면 추가)
            for col, dtype in [
                ("source",              "TEXT"),
                ("reason_code",         "TEXT"),
                ("before_param_hash",   "TEXT"),
                ("after_param_hash",    "TEXT"),
                ("changed_fields_json", "TEXT"),
                ("blocked_reason",      "TEXT"),
            ]:
                try:
                    self._conn.execute(f"ALTER TABLE llm_param_events ADD COLUMN {col} {dtype}")
                except Exception:
                    pass
            self._conn.commit()
        except Exception as e:
            log.warning("ParamStore._ensure_table 오류: %s", e)


# ===================================================================
# 싱글턴 접근자
# ===================================================================
_store_instance: Optional[ParamStore] = None
_store_lock = threading.Lock()


def get_param_store(db_path: str = "") -> ParamStore:
    global _store_instance
    if _store_instance is None:
        with _store_lock:
            if _store_instance is None:
                if not db_path:
                    raise RuntimeError("ParamStore 최초 생성 시 db_path 필요")
                _store_instance = ParamStore(db_path)
    return _store_instance