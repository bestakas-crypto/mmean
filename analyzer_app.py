# MMEAN/analyzer_app.py
from __future__ import annotations

import path_setup  # noqa: F401 — 서브디렉토리 sys.path 등록

import hmac
import threading

from flask import Flask

from app_bootstrap import build_runtime
from engine_runtime import start_runtime
from routes_analytics import register_analytics_routes
from routes_core import register_core_routes
from routes_webhook import register_webhook_routes

def _startup_live_gate(runtime) -> bool:
    """
    EXECUTION_ENABLED=true 이고 LIVE_AUTOSTART=false 이면
    콘솔 비밀번호 입력(30초 타임아웃). 불일치/타임아웃 → 시뮬레이션 강제 전환.

    반환값: True = 게이트 통과(실전 허용), False = 다운그레이드(시뮬레이션)
    """
    settings = runtime.settings

    def _downgrade(reason: str) -> bool:
        runtime.log.warning("[시작 게이트] %s — 시뮬레이션으로 다운그레이드", reason)
        settings["EXECUTION_ENABLED"] = False
        runtime.state["execution_mode"] = "OFF"  # 명시적 OFF 고정
        return False

    if not settings.get("EXECUTION_ENABLED"):
        return False  # 처음부터 비활성 — live 동작 없음

    if settings.get("LIVE_AUTOSTART"):
        runtime.log.info("[시작 게이트] LIVE_AUTOSTART=true — 자동 실전 시작")
        return True

    stored_pw = settings.get("LIVE_PASSWORD", "")
    if not stored_pw:
        return _downgrade("LIVE_PASSWORD 미설정")

    pw_holder: list = [None]
    def _ask() -> None:
        try:
            pw_holder[0] = input(
                "\n[MMEAN] 실전 매매 — 비밀번호 입력 (30초 내 미입력 시 시뮬레이션): "
            ).strip()
        except Exception:
            pw_holder[0] = ""

    t = threading.Thread(target=_ask, daemon=True)
    t.start()
    t.join(timeout=30)
    entered = pw_holder[0] or ""
    if entered and hmac.compare_digest(stored_pw.encode(), entered.encode()):
        runtime.log.info("[시작 게이트] 비밀번호 확인 — 실전 매매 활성화")
        return True
    reason = "타임아웃" if pw_holder[0] is None else "비밀번호 불일치"
    return _downgrade(reason)


def _post_gate_live_init(runtime) -> None:
    """
    시작 게이트 통과 후에만 실행하는 live 초기화.
    fill WS 시작 + 부팅 포지션 동기화(크래시 복구).
    """
    # fill WS 시작
    if runtime.fill_ws_client is not None:
        try:
            runtime.fill_ws_client.start()
            runtime.log.info("[live-init] 체결통보 WS 시작 완료")
        except Exception as e:
            runtime.log.warning("[live-init] 체결통보 WS 시작 실패: %s", e)

    # 부팅 포지션 동기화 + 크래시 복구
    if runtime.order_state is not None:
        try:
            from app_bootstrap import _boot_position_sync
            _boot_position_sync(runtime)
        except Exception as e:
            runtime.log.warning("[live-init] 부팅 포지션 동기화 실패: %s", e)


app = Flask(__name__)
runtime = build_runtime(app)

# 게이트 통과 후에만 live 초기화 (fill WS + 크래시 복구)
if _startup_live_gate(runtime):
    _post_gate_live_init(runtime)

register_core_routes(app, runtime)
register_analytics_routes(app=app, db_path=runtime.db_path, log=runtime.log)
register_webhook_routes(app, runtime)

@app.after_request
def _close_connection(resp):
    resp.headers["Connection"] = "close"
    return resp

if __name__ == "__main__":
    start_runtime(runtime)
    app.run(host="0.0.0.0", port=runtime.settings["PORT"], threaded=True)
