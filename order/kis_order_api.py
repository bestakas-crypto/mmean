# order/kis_order_api.py
"""
선물옵션 주문 REST API

함수 목록
  place_order()            주문 전송 → 주문번호 반환 (REST 1회)
  cancel_order()           취소 (WS 타임아웃 시에만 호출, REST 1회)
  get_balance()            잔고 조회 (시작 1회 + WS 재연결 후)
  get_fill_history_today() 오늘 체결내역 (예외·디버그 전용)

REST 호출 정책
  정상 거래: REST 2회 (주문 1 + 체결확인 WS → 청산 주문 1)
  타임아웃:  REST 최대 3회 (주문 1 + 취소 1)
  체결 확인: WS 체결통보 (REST 0회)
  포지션 확인: in-memory (REST 0회)
"""
from __future__ import annotations

import logging
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

import requests

from app_state import AppRuntime
from order.kis_auth_order import make_order_headers, order_base_url
from order.kis_tr_catalog import get_tr_id, get_url

log = logging.getLogger("mmean.order_api")


# ── 내부 헬퍼 ────────────────────────────────────────────────────────────────

def _session(runtime: AppRuntime) -> str:
    """현재 세션 → 'day' | 'night'"""
    return str(runtime.state.get("session_type", "day")).lower()


def _env(runtime: AppRuntime) -> str:
    return runtime.settings["ORDER_ENV"]  # 'live' | 'virtual'


def _cano(runtime: AppRuntime) -> Tuple[str, str]:
    """계좌번호 앞8자리(CANO), 뒤2자리(ACNT_PRDT_CD) 분리"""
    full = runtime.settings.get("ORDER_CANO", "")
    return full[:8], full[8:] or "03"


def _hashkey(runtime: AppRuntime, params: dict) -> str:
    """위변조 방지 hashkey 발급. 실패 시 빈 문자열 반환 (주문은 계속 진행)."""
    try:
        st = runtime.settings
        res = requests.post(
            order_base_url(runtime) + "/uapi/hashkey",
            headers={
                "content-type": "application/json",
                "appkey":       st["ORDER_KEY"],
                "appsecret":    st["ORDER_SECRET"],
            },
            json=params,
            timeout=10,
        )
        if res.status_code == 200:
            return res.json().get("HASH", "")
    except Exception as e:
        log.warning("hashkey 발급 실패 (계속 진행): %s", e)
    return ""


def _post(runtime: AppRuntime, url_path: str, tr_id: str, params: dict) -> dict:
    """POST 공통 (hashkey 포함)."""
    headers = make_order_headers(runtime, tr_id)
    hk = _hashkey(runtime, params)
    if hk:
        headers["hashkey"] = hk

    res = requests.post(
        order_base_url(runtime) + url_path,
        headers=headers,
        json=params,
        timeout=10,
    )
    res.raise_for_status()
    return res.json()


def _get(
    runtime:  AppRuntime,
    url_path: str,
    tr_id:    str,
    params:   dict,
    tr_cont:  str = "",
) -> dict:
    """GET 공통."""
    res = requests.get(
        order_base_url(runtime) + url_path,
        headers=make_order_headers(runtime, tr_id, tr_cont),
        params=params,
        timeout=10,
    )
    res.raise_for_status()
    return res.json()


# ── 주문 전송 ────────────────────────────────────────────────────────────────

def place_order(
    runtime: AppRuntime,
    side:    str,           # "LONG"|"SHORT" 진입 / "CLOSE_LONG"|"CLOSE_SHORT" 청산
    qty:     int,
    price:   float = 0.0,  # 0 = 시장가
    symbol:  Optional[str] = None,
) -> str:
    """
    선물옵션 주문 전송 — order/ 내부 전용 (저수준 함수).

    ⚠ 엔진/라우트/서비스 계층에서 직접 호출 금지.
      add_pending 누락 시 WS 체결통보를 받아도 포지션이 갱신되지 않음.
      외부 계층은 반드시 order_executor.execute_entry / execute_exit 사용.

    반환: 주문번호(ODNO)

    side 매핑
      LONG        → 매수(02) 신규
      SHORT       → 매도(01) 신규
      CLOSE_LONG  → 매도(01) 청산
      CLOSE_SHORT → 매수(02) 청산
    """
    env  = _env(runtime)
    sess = _session(runtime)

    if sess == "night" and env == "virtual":
        raise ValueError("모의투자는 야간 주문을 지원하지 않습니다")

    tr_name = "ORDER_NIGHT" if sess == "night" else "ORDER_DAY"
    tr_id   = get_tr_id(tr_name, env)
    url     = get_url(tr_name)
    cano, prdt = _cano(runtime)
    pdno = symbol or runtime.settings.get("FUTURES_CODE", "")

    # 매도매수구분: 01=매도, 02=매수
    sll_buy = {
        "LONG":        "02",
        "SHORT":       "01",
        "CLOSE_LONG":  "01",
        "CLOSE_SHORT": "02",
    }.get(side.upper())
    if not sll_buy:
        raise ValueError(f"알 수 없는 side: {side}")

    nmpr_type = "02" if price == 0.0 else "01"   # 02=시장가, 01=지정가
    price_str = "0" if price == 0.0 else f"{price:.2f}"

    params = {
        "ORD_PRCS_DVSN_CD":  "02",
        "CANO":              cano,
        "ACNT_PRDT_CD":      prdt,
        "SLL_BUY_DVSN_CD":   sll_buy,
        "SHTN_PDNO":         pdno,
        "ORD_QTY":           str(qty),
        "UNIT_PRICE":        price_str,
        "NMPR_TYPE_CD":      nmpr_type,
        "KRX_NMPR_CNDT_CD":  "0",
        "ORD_DVSN_CD":       "01",
        "FUOP_ITEM_DVSN_CD": "01" if sess == "night" else "",
        "CTAC_TLNO":         "",
    }

    data = _post(runtime, url, tr_id, params)

    if data.get("rt_cd") != "0":
        raise RuntimeError(
            f"주문 실패 | tr_id={tr_id} | msg={data.get('msg1')} | code={data.get('msg_cd')}"
        )

    ord_no = (data.get("output") or {}).get("ODNO", "").strip()
    if not ord_no:
        raise RuntimeError(f"주문번호(ODNO) 없음: {data}")

    log.info(
        "주문 전송 | env=%s sess=%s side=%s qty=%d price=%s ord_no=%s",
        env, sess, side, qty, price_str, ord_no,
    )
    return ord_no


# ── 주문 전송 + 즉시 pending 등록 (add_pending 누락 방지 래퍼) ──────────────

def place_order_and_track(
    runtime: AppRuntime,
    side:    str,
    qty:     int,
    price:   float = 0.0,
    symbol:  Optional[str] = None,
) -> str:
    """
    place_order() + OrderStateManager.add_pending() 원자적 실행.

    엔진은 place_order() 대신 반드시 이 함수를 사용해야 한다.
    add_pending 누락 시 WS 체결통보가 수신되어도 포지션 업데이트가 일어나지 않음.

    반환: 주문번호(ODNO)
    """
    ord_no = place_order(runtime, side, qty, price, symbol)

    if runtime.order_state is not None:
        runtime.order_state.add_pending(ord_no, side, qty, price or 0.0)
    else:
        log.warning(
            "order_state 미설정 — add_pending 건너뜀 | ord_no=%s side=%s qty=%d",
            ord_no, side, qty,
        )
    return ord_no


# ── 취소 주문 ────────────────────────────────────────────────────────────────

def cancel_order(
    runtime:    AppRuntime,
    org_ord_no: str,
    qty:        int = 0,    # 0 = 전량 취소
) -> bool:
    """
    주문 취소. 성공 시 True.
    정상 운영에서는 WS 체결통보 타임아웃(30s) 발생 시에만 호출.
    """
    env  = _env(runtime)
    sess = _session(runtime)

    if sess == "night" and env == "virtual":
        log.warning("모의투자 야간 취소 미지원 — 건너뜀")
        return False

    tr_name = "ORDER_CANCEL_NIGHT" if sess == "night" else "ORDER_CANCEL_DAY"
    tr_id   = get_tr_id(tr_name, env)
    url     = get_url(tr_name)
    cano, prdt = _cano(runtime)

    params = {
        "ORD_PRCS_DVSN_CD":  "02",
        "CANO":              cano,
        "ACNT_PRDT_CD":      prdt,
        "RVSE_CNCL_DVSN_CD": "02",       # 02=취소
        "ORGN_ODNO":         org_ord_no,
        "ORD_QTY":           str(qty),    # 0=전량
        "UNIT_PRICE":        "0",
        "NMPR_TYPE_CD":      "01",
        "KRX_NMPR_CNDT_CD":  "0",
        "RMN_QTY_YN":        "Y" if qty == 0 else "N",
        "ORD_DVSN_CD":       "01",
        "FUOP_ITEM_DVSN_CD": "01" if sess == "night" else "",
    }

    data = _post(runtime, url, tr_id, params)
    ok   = data.get("rt_cd") == "0"

    if not ok:
        log.error(
            "취소 실패 | org_no=%s | msg=%s | code=%s",
            org_ord_no, data.get("msg1"), data.get("msg_cd"),
        )
    else:
        log.info("취소 완료 | org_no=%s", org_ord_no)
    return ok


# ── 잔고 조회 ────────────────────────────────────────────────────────────────

def get_balance(runtime: AppRuntime) -> Dict[str, Any]:
    """
    선물옵션 잔고현황 조회. 연속조회 자동 처리.
    호출 시점: 프로세스 시작 1회 + WS 재연결 후 재동기화.
    반환: {"positions": [output1 rows], "summary": output2 dict}
    """
    env  = _env(runtime)
    tr_id = get_tr_id("BALANCE", env)
    url   = get_url("BALANCE")
    cano, prdt = _cano(runtime)

    all_pos: List[Dict] = []
    summary: Dict       = {}
    fk200 = nk200 = tr_cont = ""

    for page in range(10):
        data = _get(runtime, url, tr_id, {
            "CANO":           cano,
            "ACNT_PRDT_CD":   prdt,
            "MGNA_DVSN":      "01",
            "EXCC_STAT_CD":   "1",
            "CTX_AREA_FK200": fk200,
            "CTX_AREA_NK200": nk200,
        }, tr_cont=tr_cont)

        if data.get("rt_cd") != "0":
            log.error("잔고 조회 실패 (page=%d): %s", page, data.get("msg1"))
            break

        out1 = data.get("output1") or []
        out2 = data.get("output2") or {}
        if isinstance(out1, list):
            all_pos.extend(out1)
        if isinstance(out2, dict):
            summary = out2

        if data.get("tr_cont") in ("M", "F"):
            fk200   = data.get("ctx_area_fk200", "")
            nk200   = data.get("ctx_area_nk200", "")
            tr_cont = "N"
        else:
            break

    log.info("잔고 조회 완료 | pos=%d | env=%s", len(all_pos), env)
    return {"positions": all_pos, "summary": summary}


# ── 체결내역 조회 (예외·디버그 전용) ─────────────────────────────────────────

def get_fill_history_today(runtime: AppRuntime) -> List[Dict]:
    """
    오늘 체결내역 조회.
    정상 운영 시 WS 체결통보로 대체.
    예외 상황·디버그·시작 시 포지션 복원에만 사용.
    """
    env   = _env(runtime)
    tr_id = get_tr_id("FILL_HISTORY", env)
    url   = get_url("FILL_HISTORY")
    cano, prdt = _cano(runtime)
    today = datetime.now().strftime("%Y%m%d")

    data = _get(runtime, url, tr_id, {
        "CANO":            cano,
        "ACNT_PRDT_CD":    prdt,
        "STRT_ORD_DT":     today,
        "END_ORD_DT":      today,
        "SLL_BUY_DVSN_CD": "00",  # 전체
        "CCLD_NCCS_DVSN":  "01",  # 체결만
        "SORT_SQN":        "DS",
        "CTX_AREA_FK200":  "",
        "CTX_AREA_NK200":  "",
    })

    if data.get("rt_cd") != "0":
        log.error("체결내역 조회 실패: %s", data.get("msg1"))
        return []

    rows = data.get("output1") or []
    log.info("체결내역 조회 | count=%d | env=%s", len(rows), env)
    return rows
