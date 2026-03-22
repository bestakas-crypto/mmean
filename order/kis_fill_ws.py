# order/kis_fill_ws.py
"""
선물옵션 실시간체결통보 WebSocket 클라이언트 (WS-2)

  실전: H0IFCNI0  ws://ops.koreainvestment.com:21000
  모의: H0IFCNI9  ws://ops.koreainvestment.com:31000

특이사항
  - tr_key = HTS ID (종목코드 아님! KIS 로그인 아이디)
  - 구독 성공 시 AES key/iv 수신 → 이후 메시지 AES-CBC 복호화 필요
  - 메시지 형식: encrypt_flag|TR_ID|cnt|data
      encrypt_flag 1 → AES 복호화, 0 → 평문
      data 필드 구분자: ^ (caret)
  - KIS PINGPONG 텍스트 핑퐁 에코 처리
  - 지수 백오프 재연결 (2s → 최대 60s)

MME kis_ws_krx.py 로직을 흡수하여 MMEAN runtime 스타일로 재작성.
"""
from __future__ import annotations

import asyncio
import json
import logging
import threading
import time
from typing import Optional

import websockets
from websockets.exceptions import ConnectionClosedError, ConnectionClosedOK

from app_state import AppRuntime
from order.kis_auth_order import get_order_approval_key, order_ws_url, aes_decrypt
from order.kis_tr_catalog import get_tr_id
from order.order_state import OrderStateManager

log = logging.getLogger("mmean.fill_ws")

# 체결통보 응답 필드 순서 (실시간-012 스펙)
FILL_COLUMNS = [
    "cust_id",         # 고객 ID
    "acnt_no",         # 계좌번호
    "oder_no",         # 주문번호
    "ooder_no",        # 원주문번호
    "seln_byov_cls",   # 매도매수구분 01=매도 02=매수
    "rctf_cls",        # 정정구분
    "oder_kind2",      # 주문종류2  L=주문접수통보 0=체결통보
    "stck_shrn_iscd",  # 단축종목코드
    "cntg_qty",        # 체결수량
    "cntg_unpr",       # 체결단가 (정수 × 100 = 실제가)
    "stck_cntg_hour",  # 체결시간
    "rfus_yn",         # 거부여부  1=거부
    "cntg_yn",         # 체결여부  1=주문/정정/취소/거부통보 2=체결
    "acpt_yn",         # 접수여부  1=접수 2=확인 3=취소
    "brnc_no",         # 지점번호
    "oder_qty",        # 주문수량
    "acnt_name",       # 계좌명
    "cntg_isnm",       # 체결종목명
    "oder_cond",       # 주문조건
    "ord_grp",         # 주문그룹ID
    "ord_grpseq",      # 주문그룹SEQ
    "order_prc",       # 주문가격
]


class KISFillNoticeClient:
    """
    주문 계좌 전용 WebSocket 체결통보 클라이언트.

    - 별도 daemon 스레드 + asyncio 루프
    - AES-CBC 복호화 → ^ 파싱 → OrderStateManager.on_fill_notice()
    - 지수 백오프 재연결 (stop.wait 방식 즉시 종료 지원)

    사용:
        client = KISFillNoticeClient(runtime, order_state_mgr)
        client.start()   # 비동기 시작 (즉시 반환)
        ...
        client.stop()    # 종료 대기 (최대 5s)
    """

    def __init__(
        self,
        runtime:     AppRuntime,
        order_state: OrderStateManager,
    ) -> None:
        self.runtime     = runtime
        self.order_state = order_state
        self._aes_key:  Optional[str] = None
        self._aes_iv:   Optional[str] = None
        self._stop      = threading.Event()
        self._thread:   Optional[threading.Thread] = None
        self._backoff_base = 2.0
        self._backoff_max  = 60.0

    def start(self) -> None:
        if self._thread and self._thread.is_alive():
            return
        self._stop.clear()
        self._thread = threading.Thread(
            target=self._run_thread,
            daemon=True,
            name="KISFillNoticeWS",
        )
        self._thread.start()
        log.info(
            "체결통보 WS 시작 | env=%s | url=%s",
            self.runtime.settings["ORDER_ENV"],
            order_ws_url(self.runtime),
        )

    def stop(self) -> None:
        self._stop.set()
        if self._thread:
            self._thread.join(timeout=5.0)
        log.info("체결통보 WS 종료")

    def _run_thread(self) -> None:
        asyncio.run(self._run_forever())

    async def _run_forever(self) -> None:
        backoff = self._backoff_base
        while not self._stop.is_set():
            try:
                await self._connect_and_consume()
                backoff = self._backoff_base
            except Exception as e:
                if self._stop.is_set():
                    break
                log.warning(
                    "체결통보 WS 끊김 — %.1fs 후 재연결 | %s", backoff, e,
                )
                # stop 이벤트 감지하며 대기
                waited = 0.0
                while waited < backoff and not self._stop.is_set():
                    await asyncio.sleep(0.3)
                    waited += 0.3
                backoff = min(self._backoff_max, backoff * 2.0)

    async def _connect_and_consume(self) -> None:
        if self._stop.is_set():
            return

        st     = self.runtime.settings
        env    = st["ORDER_ENV"]
        ws_url = order_ws_url(self.runtime)
        tr_id  = get_tr_id("FILL_NOTICE_DAY", env)
        hts_id = st.get("HTS_ID", "")

        if not hts_id:
            raise RuntimeError(
                "KIS_HTS_ID 미설정 — .env.secrets에 KIS_HTS_ID 입력 필요"
            )

        apv_key = get_order_approval_key(self.runtime)
        sub_msg = json.dumps({
            "header": {
                "approval_key": apv_key,
                "custtype":     "P",
                "tr_type":      "1",
                "content-type": "utf-8",
            },
            "body": {"input": {"tr_id": tr_id, "tr_key": hts_id}},
        })

        ws = None
        try:
            ws = await asyncio.wait_for(
                websockets.connect(
                    ws_url,
                    ping_interval=None,
                    close_timeout=5,
                    max_queue=512,
                ),
                timeout=10.0,
            )
            await ws.send(sub_msg)
            log.info(
                "체결통보 WS 구독 | tr_id=%s hts=...%s",
                tr_id, hts_id[-4:] if len(hts_id) >= 4 else hts_id,
            )

            while not self._stop.is_set():
                try:
                    raw = await asyncio.wait_for(ws.recv(), timeout=30.0)
                except asyncio.TimeoutError:
                    # 조용하면 ping으로 생존 확인
                    try:
                        await asyncio.wait_for(ws.ping(), timeout=5.0)
                        continue
                    except Exception as e:
                        raise RuntimeError(f"ping_failed: {e}") from e

                if not raw:
                    continue

                # KIS PINGPONG 에코
                if isinstance(raw, str) and "PINGPONG" in raw:
                    await ws.send(raw)
                    continue

                if isinstance(raw, (bytes, bytearray)):
                    raw = raw.decode("utf-8", errors="ignore")

                raw = raw.strip()
                if not raw:
                    continue

                # JSON → 구독 성공/실패 처리
                if raw.startswith("{"):
                    self._handle_json(raw)
                    continue

                # 실시간 데이터: encrypt_flag|TR_ID|cnt|data
                if "|" in raw:
                    self._handle_data(raw)

        except (ConnectionClosedError, ConnectionClosedOK) as e:
            raise RuntimeError(f"ws_closed: {e}") from e
        finally:
            if ws is not None:
                try:
                    await ws.close()
                except Exception:
                    pass

    def _handle_json(self, raw: str) -> None:
        """구독 성공/실패 JSON 처리 — AES key/iv 저장."""
        try:
            j    = json.loads(raw)
            body = j.get("body", {})
            if body.get("rt_cd") == "0":
                out          = body.get("output", {})
                self._aes_key = out.get("key")
                self._aes_iv  = out.get("iv")
                log.info(
                    "체결통보 구독 성공 | encrypted=%s",
                    "Y" if self._aes_key else "N",
                )
            else:
                log.error("체결통보 구독 실패: %s", body.get("msg1"))
        except Exception as e:
            log.warning("JSON 파싱 오류: %s | raw=%s", e, raw[:100])

    def _handle_data(self, raw: str) -> None:
        """실시간 데이터 처리 — AES 복호화 후 OrderStateManager 전달."""
        parts = raw.split("|", 3)
        if len(parts) < 4:
            return
        encrypt_flag, _tr_id, _cnt, data = parts

        try:
            if encrypt_flag == "1":
                if not self._aes_key or not self._aes_iv:
                    log.warning("AES key 미수신 — 체결통보 메시지 스킵")
                    return
                data = aes_decrypt(data, self._aes_key, self._aes_iv)

            fields_list = data.split("^")
            fields = {
                col: (fields_list[i] if i < len(fields_list) else "")
                for i, col in enumerate(FILL_COLUMNS)
            }
            self.order_state.on_fill_notice(fields)

        except Exception as e:
            log.error("체결통보 처리 오류: %s | raw=%.120s", e, raw)
