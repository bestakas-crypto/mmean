# MMEAN/core/webhook_sender.py
"""
웹훅 발신 모듈.

진입(ENTRY) / 청산(EXIT) / 강제 청산(FORCE_EXIT) 발생 시
.env.webhook 에 등록된 모든 클라이언트에 JSON POST 를 병렬로 전송한다.

페이로드 구조:
  {
    "event":  "ENTRY",
    "ts":     "2026-03-22T03:14:00.123456+00:00",   ← UTC ISO-8601
    "side":   "LONG",                                ← 진입 시에만 포함
    "state": { ... },                                ← MMEAN 판단 변수 전체
    "order": { ok, fill_qty, fill_price, ord_no, error, ... }  ← 체결 결과
  }

HMAC 서명 (CLIENT_N_SECRET 설정 시):
  X-MMEAN-Signature: sha256=<hex>
  — body(bytes) 전체에 대한 HMAC-SHA256. 수신 측에서 검증 권장.

POST 타임아웃: 5초. 실패 시 조용히 무시 (트레이딩 로직 영향 없음).
"""
from __future__ import annotations

import hashlib
import hmac as _hmac
import json
import threading
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

_WEBHOOK_TIMEOUT_SEC = 5

# 웹훅 페이로드에 포함할 state 키 목록
# 진입 판단에 사용된 모든 flow_* 변수와 컨텍스트 변수를 망라한다.
_STATE_KEYS = [
    # ── FlowEngine / FlowRegimeEngine ─────────────────────────────────────
    "flow_score",
    "flow_regime_label",
    "flow_long_weight",
    "flow_short_weight",
    # ── PatternMemory / LLM 게이트 ────────────────────────────────────────
    "flow_entry_gate",          # ALLOW / HALF / REJECT
    "flow_caution_level",       # danger / warning / caution / none
    "flow_fac_type",            # forbidden / failure / none
    "flow_fac_conf",            # HIGH / MED / LOW / NONE
    "flow_gate_reject_count",
    "flow_gate_half_count",
    # ── LLM 가중치 조정 ──────────────────────────────────────────────────
    "flow_llm_weight_adjust",   # 게이트 적용 후 최종 스케일드 값
    "flow_wa_raw",              # LLM 원본 (게이트 적용 전)
    "flow_llm_risk_view",       # positive / neutral / negative / failure
    "flow_llm_confidence",      # 0.0 ~ 1.0
    "flow_llm_reason_main",
    "flow_llm_reason_against",
    "flow_llm_error",
    # ── 포지션 크기 결정 ──────────────────────────────────────────────────
    "flow_order_size",
    "flow_size_before_llm",
    # ── 최종 게이트 사유 ──────────────────────────────────────────────────
    "flow_final_gate_reason",
    # ── 신호 / 바이어스 ──────────────────────────────────────────────────
    "entry_signal",             # LONG_READY / SHORT_READY / WAIT / NEUTRAL
    "bias",                     # LONG / SHORT / NEUTRAL
    "basis_zone",               # HIGH / MID / LOW
    "volume_zone",              # HIGH / MID / LOW
    "time_bucket",              # OPEN / MID / CLOSE / …
    # ── 시장 지표 ─────────────────────────────────────────────────────────
    "atr",
    "data_quality",
    "futures_price",
    "spot_price",
    "foreign_buy",
    "oi_value",
    "oi_delta",
    "basis",
    "basis_ema",
    "basis_slope",
    "volume_burst",
    "long_score",
    "short_score",
    "confidence",
    "ema_fast",
    "ema_slow",
    "ema_fast_slope",
    "trend_weight_long",
    "trend_weight_short",
    "trend_long_score",
    "trend_short_score",
    "vwap",
    "price_vs_vwap",
    # ── 사전 개장 편향 보정 ───────────────────────────────────────────────
    "premarket_bias_adj_long",
    "premarket_bias_adj_short",
    "premarket_size_adj",
    "premarket_manual_mode",
    # ── 세션 / 실행 컨텍스트 ─────────────────────────────────────────────
    "session_phase",            # opening / mid / closing
    "session_type",             # day / night
    "execution_mode",           # OFF / PAPER / LIVE
    "mode_type",                # expert / auto / …
    "timestamp",                # 마지막 틱 타임스탬프
    # ── 야간 세션 ─────────────────────────────────────────────────────────
    "night_regime",
    "night_entry",
    "night_confidence",
    "night_raw_score",
    # ── MarketAnalyst ─────────────────────────────────────────────────────
    "market_view",
    "provisional_regime",
    "opening_char",
    # ── LLM 메타 ─────────────────────────────────────────────────────────
    "llm_filter_ts",
    "llm_call_id",
    "active_judgment_event_id",
    "llm_last_model",
    "llm_intraday_influence",
    # ── 실행 결과 (최근 체결) ─────────────────────────────────────────────
    "live_last_side",
    "live_last_fill_price",
    "live_last_fill_qty",
    "live_last_ok",
]


def collect_state(runtime) -> Dict[str, Any]:
    """runtime.state 에서 진입 판단에 사용된 모든 변수를 수집."""
    s = runtime.state
    result: Dict[str, Any] = {}
    for k in _STATE_KEYS:
        v = s.get(k)
        if v is not None:
            result[k] = v
    return result


def build_payload(
    event: str,
    runtime,
    *,
    side: Optional[str] = None,
    result=None,
) -> Dict[str, Any]:
    """웹훅 전송 페이로드를 구성한다."""
    payload: Dict[str, Any] = {
        "event": event,        # ENTRY / EXIT / FORCE_EXIT
        "ts":    datetime.now(tz=timezone.utc).isoformat(),
        "state": collect_state(runtime),
    }

    if side is not None:
        payload["side"] = side

    if result is not None:
        payload["order"] = {
            "ok":          getattr(result, "ok",          None),
            "cancelled":   getattr(result, "cancelled",   None),
            "refused":     getattr(result, "refused",     None),
            "fill_qty":    getattr(result, "fill_qty",    None),
            "fill_price":  getattr(result, "fill_price",  None),
            "ord_no":      getattr(result, "ord_no",      None),
            "error":       getattr(result, "error",       None),
        }

    return payload


def _post(url: str, payload: Dict[str, Any], secret: str, client_id: int) -> None:
    """실제 HTTP POST — daemon 스레드 내부에서 호출된다."""
    try:
        import requests as _req  # noqa: PLC0415 (지연 import — 스레드 내부)

        body = json.dumps(payload, ensure_ascii=False, default=str).encode("utf-8")
        headers: Dict[str, str] = {
            "Content-Type":      "application/json; charset=utf-8",
            "X-MMEAN-Event":     payload.get("event", ""),
            "X-MMEAN-Client-ID": str(client_id),
        }

        if secret:
            sig = _hmac.new(
                secret.encode("utf-8"), body, hashlib.sha256
            ).hexdigest()
            headers["X-MMEAN-Signature"] = f"sha256={sig}"

        resp = _req.post(url, data=body, headers=headers, timeout=_WEBHOOK_TIMEOUT_SEC)
        resp.raise_for_status()

    except Exception:  # noqa: BLE001
        # 웹훅 전송 실패는 트레이딩 로직에 영향 없음 — 조용히 무시
        pass


def send(
    event: str,
    runtime,
    *,
    side: Optional[str] = None,
    result=None,
) -> None:
    """
    등록된 모든 클라이언트에 웹훅을 병렬(daemon 스레드)로 전송한다.

    .env.webhook 에 등록된 클라이언트가 없으면 즉시 반환.
    각 클라이언트에 대해 독립적인 daemon 스레드를 생성해 동시 전송한다.

    Args:
        event:   "ENTRY" / "EXIT" / "FORCE_EXIT"
        runtime: AppRuntime 인스턴스
        side:    진입 방향 ("LONG" / "SHORT"), 청산 시 생략
        result:  ExecuteResult 인스턴스, 없으면 생략
    """
    clients = getattr(runtime, "webhook_clients", [])
    if not clients:
        return

    payload = build_payload(event, runtime, side=side, result=result)

    for client in clients:
        if not client.url:
            continue
        threading.Thread(
            target=_post,
            args=(client.url, payload, client.secret, client.id),
            daemon=True,
            name=f"Webhook-{event}-C{client.id}",
        ).start()
