# order/__init__.py
"""
MMEAN 주문 실행 레이어

WS-first 설계:
  - 체결 확인   → H0IFCNI0/H0IFCNI9 WebSocket (REST 0회)
  - 포지션 확인 → OrderStateManager in-memory (REST 0회)
  - 주문 전송   → REST 1회
  - 취소        → 타임아웃(30s) 발생 시만 REST 1회
  - 잔고 동기화 → 시작 1회 + WS 재연결 후만

실거래 전환: .env.public KIS_ORDER_ENV=virtual → live 한 줄만 변경
"""
from order.kis_tr_catalog  import get_tr_id, get_url, TR_CATALOG
from order.kis_order_api   import (
    place_order, place_order_and_track,
    cancel_order, get_balance, get_fill_history_today,
)
from order.kis_fill_ws     import KISFillNoticeClient
from order.order_state     import OrderStateManager
# ── 엔진/라우트/서비스 계층 진입점 (place_order 직접 호출 대신 이것만 사용) ──
from order.order_executor  import execute_entry, execute_exit, ExecuteResult

__all__ = [
    "TR_CATALOG", "get_tr_id", "get_url",
    # order_executor 공개 API (외부 계층 전용)
    "execute_entry", "execute_exit", "ExecuteResult",
    # order 내부 전용 (외부 계층에서 직접 import 금지)
    "place_order", "place_order_and_track",
    "cancel_order", "get_balance", "get_fill_history_today",
    "KISFillNoticeClient",
    "OrderStateManager",
]
