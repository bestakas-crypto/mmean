# order/order_state.py
"""
OrderStateManager — in-memory 주문·포지션 상태 관리

WS 체결통보 이벤트 → 포지션 자동 업데이트 (REST 조회 불필요)

사용 흐름
  1. ord_no = place_order(runtime, "LONG", 1)
  2. mgr.add_pending(ord_no, "LONG", 1, price)
  3. 별도 스레드: mgr.wait_fill(ord_no, timeout=30)
     - 체결 → True  → 완료
     - 타임아웃 → False → cancel_order(runtime, ord_no) 호출

포지션 PnL 계산 기준
  KOSPI 200 선물: 1포인트 = 250,000원 (계약 승수)
"""
from __future__ import annotations

import logging
import threading
from dataclasses import dataclass, field
from typing import Dict, Optional

log = logging.getLogger("mmean.order_state")

# KOSPI 200 선물 계약 승수 (1포인트 = 250,000원)
FUTURES_MULTIPLIER = 250_000

# 체결통보 필드명 (kis_fill_ws.FILL_COLUMNS 순서와 동일)
_F_ORD_NO    = "oder_no"
_F_CNTG_YN   = "cntg_yn"    # 2=체결
_F_ACPT_YN   = "acpt_yn"    # 3=취소
_F_RFUS_YN   = "rfus_yn"    # 1=거부
_F_SIDE      = "seln_byov_cls"  # 01=매도, 02=매수
_F_FILL_QTY  = "cntg_qty"
_F_FILL_PRC  = "cntg_unpr"


@dataclass
class PendingOrder:
    ord_no:     str
    side:       str           # LONG / SHORT / CLOSE_LONG / CLOSE_SHORT
    qty:        int
    price:      float
    event:      threading.Event = field(default_factory=threading.Event)
    filled:     bool  = False
    cancelled:  bool  = False
    refused:    bool  = False
    fill_price: float = 0.0
    fill_qty:   int   = 0


@dataclass
class PositionState:
    direction:   int   = 0    # 1=LONG, -1=SHORT, 0=FLAT
    qty:         int   = 0
    entry_price: float = 0.0
    symbol:      str   = ""

    def is_flat(self) -> bool:
        return self.direction == 0 or self.qty == 0


class OrderStateManager:
    """
    스레드 안전 주문·포지션 매니저.
    WS 체결통보(on_fill_notice) → 포지션 자동 업데이트 → REST 0회.
    """

    def __init__(self) -> None:
        self._lock     = threading.Lock()
        self.position  = PositionState()
        self._pending: Dict[str, PendingOrder] = {}
        self.daily_pnl:   float = 0.0
        self.trade_count: int   = 0

    # ── 주문 등록·해제 ────────────────────────────────────────────────────

    def add_pending(
        self, ord_no: str, side: str, qty: int, price: float
    ) -> None:
        with self._lock:
            self._pending[ord_no] = PendingOrder(
                ord_no=ord_no, side=side, qty=qty, price=price,
            )
        log.info(
            "주문 등록 | ord_no=%s side=%s qty=%d price=%.2f",
            ord_no, side, qty, price,
        )

    def remove_pending(self, ord_no: str) -> Optional[PendingOrder]:
        with self._lock:
            return self._pending.pop(ord_no, None)

    # ── WS 체결통보 처리 ──────────────────────────────────────────────────

    def on_fill_notice(self, fields: dict) -> None:
        """
        kis_fill_ws 가 파싱한 필드 dict 수신 → 포지션 업데이트.
        lock 외부에서 호출됨 (WS 스레드).
        """
        ord_no  = fields.get(_F_ORD_NO, "").strip().lstrip("0")
        cntg_yn = fields.get(_F_CNTG_YN, "")
        acpt_yn = fields.get(_F_ACPT_YN, "")
        rfus_yn = fields.get(_F_RFUS_YN, "0")

        with self._lock:
            pending = self._pending.get(ord_no)

        # ── 거부 ──────────────────────────────────────────────────────────
        if rfus_yn == "1":
            log.warning("주문 거부 | ord_no=%s", ord_no)
            if pending:
                pending.refused = True
                pending.event.set()
            return

        # ── 취소 확정 ─────────────────────────────────────────────────────
        if acpt_yn == "3":
            log.info("주문 취소 확정 | ord_no=%s", ord_no)
            if pending:
                pending.cancelled = True
                pending.event.set()
            return

        # ── 접수통보 등 무시 ──────────────────────────────────────────────
        if cntg_yn != "2":
            return

        # ── 체결 ──────────────────────────────────────────────────────────
        try:
            fill_qty       = int(fields.get(_F_FILL_QTY, "0").strip() or "0")
            fill_price_raw = fields.get(_F_FILL_PRC, "0").strip()
            # KIS는 소수점 없이 정수로 전달 (예: 36250 → 362.50)
            fill_price     = float(fill_price_raw) / 100.0
            side_code      = fields.get(_F_SIDE, "")
        except Exception as e:
            log.error("체결통보 파싱 오류: %s | fields=%s", e, fields)
            return

        log.info(
            "체결 확인 | ord_no=%s side_code=%s qty=%d price=%.2f",
            ord_no, side_code, fill_qty, fill_price,
        )

        with self._lock:
            self._update_position(side_code, fill_qty, fill_price)
            if pending:
                pending.filled     = True
                pending.fill_price = fill_price
                pending.fill_qty   = fill_qty
                self._pending.pop(ord_no, None)
                pending.event.set()
            self.trade_count += 1

    def _update_position(
        self, side_code: str, qty: int, price: float
    ) -> None:
        """매수(02)/매도(01) 체결 → 포지션 업데이트. _lock 내부에서만 호출."""
        pos = self.position

        if side_code == "02":   # 매수 체결
            if pos.direction == -1:             # SHORT 청산
                pnl = (pos.entry_price - price) * qty * FUTURES_MULTIPLIER
                self.daily_pnl += pnl
                pos.qty -= qty
                if pos.qty <= 0:
                    pos.direction   = 0
                    pos.qty         = 0
                    pos.entry_price = 0.0
                    log.info("SHORT 청산 | pnl=%.0f원", pnl)
            else:                               # LONG 신규 진입
                pos.direction   = 1
                pos.qty        += qty
                pos.entry_price = price
                log.info("LONG 진입 | qty=%d price=%.2f", qty, price)

        elif side_code == "01": # 매도 체결
            if pos.direction == 1:              # LONG 청산
                pnl = (price - pos.entry_price) * qty * FUTURES_MULTIPLIER
                self.daily_pnl += pnl
                pos.qty -= qty
                if pos.qty <= 0:
                    pos.direction   = 0
                    pos.qty         = 0
                    pos.entry_price = 0.0
                    log.info("LONG 청산 | pnl=%.0f원", pnl)
            else:                               # SHORT 신규 진입
                pos.direction   = -1
                pos.qty        += qty
                pos.entry_price = price
                log.info("SHORT 진입 | qty=%d price=%.2f", qty, price)

    # ── 체결 대기 ─────────────────────────────────────────────────────────

    def wait_fill(self, ord_no: str, timeout: float = 30.0) -> bool:
        """
        체결통보 대기. timeout 초 내 체결 확인 시 True.
        False(타임아웃) → 호출자가 cancel_order() 실행해야 함.
        상세 결과가 필요하면 wait_fill_detail() 사용.
        """
        filled, *_ = self.wait_fill_detail(ord_no, timeout)
        return filled

    def wait_fill_detail(
        self, ord_no: str, timeout: float = 30.0
    ) -> tuple:
        """
        체결 대기 후 상세 결과 반환.
        order_executor._execute() 에서 fill_price/fill_qty 획득에 사용.

        반환: (filled, fill_price, fill_qty, cancelled, refused)
          filled    True = 정상 체결
          fill_price / fill_qty: 체결단가·수량 (미체결 시 0)
          cancelled  True = 취소 확정 (acpt_yn=3)
          refused    True = KIS 거부 (rfus_yn=1)
        """
        with self._lock:
            pending = self._pending.get(ord_no)
        if not pending:
            # 이미 on_fill_notice 에서 처리됨 (체결 간주)
            return (True, 0.0, 0, False, False)

        triggered = pending.event.wait(timeout=timeout)
        if not triggered:
            log.warning("체결 타임아웃 | ord_no=%s (%.0fs 경과)", ord_no, timeout)
            return (False, 0.0, 0, False, False)

        return (
            pending.filled,
            pending.fill_price,
            pending.fill_qty,
            pending.cancelled,
            pending.refused,
        )

    # ── 상태 조회 ─────────────────────────────────────────────────────────

    def is_flat(self) -> bool:
        with self._lock:
            return self.position.is_flat()

    def has_pending(self) -> bool:
        with self._lock:
            return len(self._pending) > 0

    def get_position(self) -> PositionState:
        with self._lock:
            p = self.position
            return PositionState(
                direction=p.direction,
                qty=p.qty,
                entry_price=p.entry_price,
                symbol=p.symbol,
            )

    # ── REST 잔고 동기화 (시작 1회 + WS 재연결 후) ───────────────────────

    def sync_from_balance(
        self, positions: list, symbol: str
    ) -> None:
        """
        REST 잔고 조회 결과(output1 rows)로 포지션 초기화.
        호출 시점: 프로세스 시작 1회 + WS 재연결 후.
        """
        net_qty   = 0
        avg_price = 0.0

        for r in positions:
            pdno = str(r.get("shtn_pdno") or r.get("pdno") or "").strip()
            if pdno != symbol:
                continue
            qty = int(float(str(r.get("cblc_qty") or "0") or "0"))
            if qty == 0:
                continue
            raw_side = str(
                r.get("sll_buy_dvsn_name") or r.get("sll_buy_dvsn_cd") or ""
            ).upper()
            sign = 1 if any(k in raw_side for k in ("02", "BUY", "매수")) else -1
            net_qty  += sign * qty
            avg_price = float(
                str(r.get("ccld_avg_unpr1") or r.get("avg_price") or "0") or "0"
            )

        with self._lock:
            if net_qty > 0:
                self.position = PositionState(
                    direction=1, qty=net_qty,
                    entry_price=avg_price, symbol=symbol,
                )
            elif net_qty < 0:
                self.position = PositionState(
                    direction=-1, qty=abs(net_qty),
                    entry_price=avg_price, symbol=symbol,
                )
            else:
                self.position = PositionState(symbol=symbol)

        log.info(
            "포지션 동기화 | symbol=%s net_qty=%d avg=%.2f",
            symbol, net_qty, avg_price,
        )
