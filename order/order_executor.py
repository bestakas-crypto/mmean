# order/order_executor.py
"""
주문 실행 엔진 — 엔진/라우트/서비스 계층의 유일한 주문 진입점

┌──────────────────────────────────────────────────────────────────┐
│  코드 규칙  (위반 → WS 체결통보 미수신 → 포지션 오염 위험)        │
│                                                                  │
│  ❌  from order.kis_order_api import place_order                  │
│  ❌  place_order(runtime, ...)  # 직접 호출                       │
│                                                                  │
│  ✅  from order.order_executor import execute_entry, execute_exit │
│  ✅  execute_entry(runtime, side="LONG", qty=1)                   │
└──────────────────────────────────────────────────────────────────┘

실행 흐름:
  execute_entry / execute_exit
    → place_order_and_track()     ← add_pending 자동 포함
    → OrderStateManager.wait_fill_detail(timeout=30s)
      - 체결   → ExecuteResult(ok=True, fill_price, fill_qty)
      - 거부   → ExecuteResult(refused=True)
      - 취소   → ExecuteResult(cancelled=True)
      - 타임아웃 → cancel_order() + remove_pending()
                   → ExecuteResult(cancelled=True|error)
"""
from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Optional

from app_state import AppRuntime
from order.kis_order_api import place_order_and_track, cancel_order, get_balance

log = logging.getLogger("mmean.order_executor")


# ── 결과 데이터클래스 ─────────────────────────────────────────────────────────

@dataclass
class ExecuteResult:
    """주문 실행 1사이클 결과."""
    ok:          bool  = False   # True = 정상 체결 확인
    ord_no:      str   = ""      # KIS 주문번호
    side:        str   = ""      # 요청 side (LONG/SHORT/CLOSE_LONG/CLOSE_SHORT)
    req_qty:     int   = 0       # 요청 수량
    fill_price:  float = 0.0     # 체결단가 (0 = 미체결)
    fill_qty:    int   = 0       # 체결수량
    cancelled:   bool  = False   # 취소 처리됨 (타임아웃 또는 KIS 취소 확정)
    refused:     bool  = False   # KIS 거부
    error:       str   = ""      # 예외 메시지 (ok=False 이고 cancelled/refused 아닐 때)

    def is_terminal(self) -> bool:
        """ok / cancelled / refused / error 중 하나라도 확정 상태면 True."""
        return self.ok or self.cancelled or self.refused or bool(self.error)


# ── 내부 실행 함수 ────────────────────────────────────────────────────────────

def _execute(
    runtime: AppRuntime,
    side:    str,
    qty:     int,
    price:   float = 0.0,
    symbol:  Optional[str] = None,
    timeout: float = 30.0,
) -> ExecuteResult:
    """
    공통 주문 실행.
    place_order_and_track() → wait_fill_detail() → 타임아웃 시 cancel_order().
    외부에서 직접 호출하지 말 것 — execute_entry / execute_exit 사용.
    """
    result = ExecuteResult(side=side, req_qty=qty)

    # ── 종목코드 결정: 미니 선택 시 MINI_FUTURES_CODE 사용 ─────────────────
    # 가격 피드·분석은 항상 노멀(FUTURES_CODE) 기준 — 주문 종목만 분기.
    # 수량 환산 없음 (1포지션 = 1계약 고정).
    if symbol is None:
        instrument = runtime.settings.get("TRADE_INSTRUMENT", "normal").lower()
        if instrument == "mini":
            symbol = (
                runtime.settings.get("MINI_FUTURES_CODE", "")
                or runtime.settings.get("FUTURES_CODE", "")
            )

    # ── 주문 전송 + add_pending (원자) ─────────────────────────────────────
    try:
        ord_no = place_order_and_track(runtime, side, qty, price, symbol)
        result.ord_no = ord_no
    except Exception as e:
        result.error = str(e)
        log.error("주문 전송 실패 | side=%s qty=%d | %s", side, qty, e)
        return result

    # ── order_state 없으면 체결 확인 불가 ────────────────────────────────
    order_state = runtime.order_state
    if order_state is None:
        result.error = "order_state 없음 — 체결 확인 불가"
        log.error("order_state 없음 | ord_no=%s", ord_no)
        return result

    # ── 체결 대기 ─────────────────────────────────────────────────────────
    filled, fill_price, fill_qty, cancelled, refused = \
        order_state.wait_fill_detail(ord_no, timeout=timeout)

    if refused:
        result.refused = True
        result.error   = f"KIS 주문 거부 | ord_no={ord_no}"
        log.warning("주문 거부 | ord_no=%s side=%s", ord_no, side)
        return result

    if cancelled:
        result.cancelled = True
        log.info("주문 취소 확정 | ord_no=%s side=%s", ord_no, side)
        return result

    if filled:
        result.ok          = True
        result.fill_price  = fill_price
        result.fill_qty    = fill_qty
        log.info(
            "체결 완료 | ord_no=%s side=%s qty=%d price=%.2f",
            ord_no, side, fill_qty, fill_price,
        )
        return result

    # ── 타임아웃 → 취소 전송 ─────────────────────────────────────────────
    log.warning("체결 타임아웃(%.0fs) | ord_no=%s side=%s — 취소 전송", timeout, ord_no, side)
    try:
        cancel_ok        = cancel_order(runtime, ord_no)
        result.cancelled = cancel_ok
        if not cancel_ok:
            result.error = f"취소 실패 | ord_no={ord_no}"
    except Exception as ce:
        result.error = f"취소 예외 | {ce}"
        log.error("취소 예외 | ord_no=%s | %s", ord_no, ce)
    finally:
        order_state.remove_pending(ord_no)

    # ── 타임아웃 후 포지션 재동기화 ──────────────────────────────────────
    # WS 끊김 중 주문이 실제 체결됐을 수 있음 → REST 잔고로 in-memory 보정
    # 취소 성공 여부와 무관하게 항상 실행 (체결 후 취소 실패 케이스 방어)
    try:
        bal    = get_balance(runtime)
        symbol = runtime.settings.get("FUTURES_CODE", "")
        order_state.sync_from_balance(bal["positions"], symbol)
        log.info("타임아웃 후 포지션 재동기화 완료 | symbol=%s", symbol)
    except Exception as se:
        log.warning("타임아웃 후 포지션 재동기화 실패 (계속 진행): %s", se)

    return result


# ── 공개 API ──────────────────────────────────────────────────────────────────

def execute_entry(
    runtime: AppRuntime,
    side:    str,
    qty:     int   = 1,
    price:   float = 0.0,     # 0 = 시장가
    symbol:  Optional[str] = None,
    timeout: float = 30.0,
) -> ExecuteResult:
    """
    진입 주문 실행.

    side: "LONG" (매수 신규) | "SHORT" (매도 신규)
    price: 0 → 시장가 (NMPR_TYPE_CD=02), 지정 시 지정가 (01)
    timeout: 체결 대기 최대 초 (초과 시 자동 취소)
    """
    if side.upper() not in ("LONG", "SHORT"):
        return ExecuteResult(
            side=side, req_qty=qty,
            error=f"execute_entry: side는 LONG|SHORT 만 허용 (입력={side})",
        )
    return _execute(runtime, side.upper(), qty, price, symbol, timeout)


def execute_exit(
    runtime: AppRuntime,
    qty:     int   = 0,       # 0 = 현재 포지션 전량
    price:   float = 0.0,
    symbol:  Optional[str] = None,
    timeout: float = 30.0,
) -> ExecuteResult:
    """
    청산 주문 실행.

    현재 포지션 방향을 order_state 에서 자동 판별:
      direction=1  → CLOSE_LONG  (매도 청산)
      direction=-1 → CLOSE_SHORT (매수 청산)

    qty=0 이면 보유 수량 전량 청산.
    포지션 FLAT 이거나 order_state 없으면 ok=False 즉시 반환.
    """
    order_state = runtime.order_state
    if order_state is None:
        return ExecuteResult(error="order_state 없음")

    pos = order_state.get_position()
    if pos.is_flat():
        return ExecuteResult(error="포지션 없음 — 청산 불필요")

    close_side = "CLOSE_LONG" if pos.direction == 1 else "CLOSE_SHORT"
    close_qty  = qty if qty > 0 else pos.qty

    return _execute(runtime, close_side, close_qty, price, symbol, timeout)
