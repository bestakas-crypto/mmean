# MMEAN/routes/routes_webhook.py
"""
웹훅 수신 라우트.

클라이언트가 MMEAN에 현재 시장 상황을 요청할 때 사용한다.

엔드포인트:
  POST /api/webhook/state
    Header: X-MMEAN-Token: <client-token>
    Body:   없음 (선택적으로 JSON {"fields": [...]} 로 원하는 필드만 요청 가능)

응답:
  200 OK
    {
      "client_id":   1,
      "client_name": "MyBot",
      "ts":          "2026-03-22T03:14:00.123456+00:00",
      "ready":       true,
      "state": {
        "entry_signal":      "LONG_READY",
        "bias":              "LONG",
        "flow_score":        0.42,
        "flow_regime_label": "STRONG_LONG",
        ...
      }
    }

  401 Unauthorized          — 토큰 없음 또는 미등록 (감사 로그 WARNING)
  503 Service Unavailable   — 엔진 초기화 미완료
"""
from __future__ import annotations

from datetime import datetime, timezone

from flask import jsonify, request

from app_state import AppRuntime
from webhook_registry import find_by_token
from webhook_sender import collect_state


def register_webhook_routes(app, runtime: AppRuntime) -> None:
    log = runtime.log

    @app.route("/api/webhook/state", methods=["POST"])
    def webhook_state():
        remote = request.remote_addr or "unknown"
        token  = request.headers.get("X-MMEAN-Token", "").strip()
        clients = getattr(runtime, "webhook_clients", [])
        client = find_by_token(clients, token)

        # ── 인증 실패 — 감사 로그 ─────────────────────────────────────
        if client is None:
            log.warning(
                "[webhook/state] 인증 실패 | ip=%s | token_prefix=%s",
                remote,
                (token[:6] + "…") if len(token) >= 6 else ("(empty)" if not token else token),
            )
            return jsonify({"error": "unauthorized"}), 401

        # ── 데이터 준비 상태 체크 ───────────────────────────────────────
        if runtime.state.get("data_quality") == "initializing":
            log.info(
                "[webhook/state] 조회 요청 — 초기화 중 | client=#%d(%s) ip=%s",
                client.id, client.name, remote,
            )
            return jsonify({
                "client_id":   client.id,
                "client_name": client.name,
                "ts":          datetime.now(tz=timezone.utc).isoformat(),
                "ready":       False,
                "message":     "엔진 초기화 중 — 첫 번째 WebSocket 틱 대기",
            }), 503

        # ── 시장 상태 스냅샷 수집 ───────────────────────────────────────
        with runtime.state_obj.engine_lock:
            state_snap = collect_state(runtime)

        # ── 요청자가 특정 fields 만 원하는 경우 필터 ──────────────────────
        requested_fields = None
        try:
            body = request.get_json(silent=True) or {}
            requested_fields = body.get("fields")
            if isinstance(requested_fields, list) and requested_fields:
                state_snap = {
                    k: v for k, v in state_snap.items()
                    if k in requested_fields
                }
        except Exception:
            pass  # 파싱 실패 시 전체 반환

        # ── 성공 감사 로그 ─────────────────────────────────────────────
        log.info(
            "[webhook/state] 조회 성공 | client=#%d(%s) ip=%s fields=%s",
            client.id,
            client.name,
            remote,
            len(requested_fields) if isinstance(requested_fields, list) else "all",
        )

        return jsonify({
            "client_id":   client.id,
            "client_name": client.name,
            "ts":          datetime.now(tz=timezone.utc).isoformat(),
            "ready":       True,
            "state":       state_snap,
        })
