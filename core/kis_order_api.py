# MMEAN/core/kis_order_api.py
"""
[DEPRECATED] 하위 호환 re-export 전용 — 직접 import 금지.

모든 KIS 주문·조회 함수는 order/ 패키지로 통합됨.

올바른 사용법
─────────────
  주문 실행     : order.order_executor   execute_entry / execute_exit
  주문·취소     : order.kis_order_api    place_order / cancel_order
  정정          : order.kis_order_api    modify_order
  잔고·가능     : order.kis_order_api    get_balance / get_orderable_qty
  체결내역      : order.kis_order_api    get_order_history / get_fill_history_today
  인증·헤더     : order.kis_auth_order   get_order_token / make_order_headers
  TR 카탈로그   : order.kis_tr_catalog   get_tr_id / get_url

잘못된 사용법
─────────────
  ❌ from core.kis_order_api import ...
  ❌ from kis_order_api import ...  (core/ 내 직접 참조)
"""

# engine_runtime.py 하위 호환 — 다음 리팩토링 시 삭제 예정
from order.kis_auth_order import get_order_token  # noqa: F401
