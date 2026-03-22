# MMEAN/core/kis_order_api.py
"""
KIS 주문 계좌 전용 모듈 (모의/실전 공용).

환경 전환
─────────
  .env.public  KIS_ORDER_ENV=virtual  ← 개발 중 (모의계좌)
  .env.public  KIS_ORDER_ENV=live     ← 실거래 전환 시 한 줄만 변경

역할 분담
─────────
- 주문 토큰 발급·캐시
- 선물 신규 주문 / 정정·취소 / 잔고 조회 / 주문가능 조회 / 체결내역 조회

규칙
────
- HTTP I/O + TR 선택 담당. 포지션 관리·state는 엔진 담당.
- 야간 선물 주문: 모의 미지원 → ORDER_ENV 무관 항상 실전 TR 사용.
- 신규 주문 API: TR 코드를 kis_tr_catalog 에 먼저 등록.
"""
from __future__ import annotations

import time
from datetime import datetime
from typing import Dict, List, Literal, Optional

import requests

from app_state import AppRuntime
from kis_tr_catalog import (
    KIS_BASE_LIVE,
    KIS_BASE_VIRTUAL,
    OAUTH_TOKEN_PATH,
    TR_FUTURES_ORDER_LIVE, TR_FUTURES_ORDER_NIGHT, TR_FUTURES_ORDER_VIRTUAL,
    TR_FUTURES_CANCEL_LIVE, TR_FUTURES_CANCEL_NIGHT, TR_FUTURES_CANCEL_VIRTUAL,
    TR_FUTURES_BALANCE_LIVE, TR_FUTURES_BALANCE_VIRTUAL,
    TR_FUTURES_ORDERABLE_LIVE, TR_FUTURES_ORDERABLE_VIRTUAL,
    TR_FUTURES_CCLD_LIVE, TR_FUTURES_CCLD_VIRTUAL,
    EP_FUTURES_ORDER, EP_FUTURES_CANCEL,
    EP_FUTURES_BALANCE, EP_FUTURES_ORDERABLE, EP_FUTURES_CCLD,
    order_tr,
)


# ── BASE URL ──────────────────────────────────────────────────────────────────
def get_order_base_url(runtime: AppRuntime) -> str:
    """주문 계좌 BASE URL — KIS_ORDER_ENV 기준 (virtual=모의 / live=실거래)."""
    return KIS_BASE_LIVE if runtime.settings["ORDER_ENV"] == "live" else KIS_BASE_VIRTUAL


# ── 주문 계좌 토큰 ────────────────────────────────────────────────────────────
def get_order_token(runtime: AppRuntime) -> str:
    """주문 계좌 access_token 반환. 만료 5분 전 자동 갱신."""
    st    = runtime.settings
    cache = runtime.state_obj.order_token_cache
    lock  = runtime.state_obj.order_token_lock

    with lock:
        if (
            cache["access_token"]
            and (time.time() - float(cache["issued_at"])) < (int(cache["expires_in"]) - 300)
        ):
            return str(cache["access_token"])

    if not st["ORDER_KEY"] or not st["ORDER_SECRET"]:
        raise RuntimeError("KIS_ORDER_KEY / KIS_ORDER_SECRET 미설정 — .env.secrets 확인")

    res = requests.post(
        get_order_base_url(runtime) + OAUTH_TOKEN_PATH,
        json={"grant_type": "client_credentials", "appkey": st["ORDER_KEY"], "appsecret": st["ORDER_SECRET"]},
        timeout=10,
    )
    res.raise_for_status()
    data = res.json()
    token      = data.get("access_token", "").strip()
    expires_in = int(data.get("expires_in", 86400))
    if not token:
        raise RuntimeError(f"[ORDER] access_token 발급 실패: {data}")

    with lock:
        cache.update({"access_token": token, "issued_at": time.time(), "expires_in": expires_in})
    runtime.log.info("주문 토큰 발급 | env=%s | key=...%s", st["ORDER_ENV"], st["ORDER_KEY"][-6:])
    return token


# ── 공통 주문 헤더 ────────────────────────────────────────────────────────────
def _order_headers(runtime: AppRuntime, tr_id: str) -> Dict[str, str]:
    token = get_order_token(runtime)
    st    = runtime.settings
    return {
        "content-type":  "application/json; charset=utf-8",
        "authorization": f"Bearer {token}",
        "appkey":        st["ORDER_KEY"],
        "appsecret":     st["ORDER_SECRET"],
        "tr_id":         tr_id,
        "custtype":      "P",
    }


def _get_cano(runtime: AppRuntime, cano: str = "") -> tuple[str, str]:
    """계좌번호 앞8자리·뒤2자리 반환. cano 미입력 시 .env KIS_ORDER_CANO 참조."""
    raw = cano or runtime.settings.get("ORDER_CANO", "")
    if not raw:
        raise RuntimeError("계좌번호(KIS_ORDER_CANO) 미설정 — .env.secrets 확인")
    return raw[:8], raw[8:] if len(raw) > 8 else "03"


# ═══════════════════════════════════════════════════════════════════════════════
# 선물옵션 신규 주문 [v1_국내선물-001]
# ═══════════════════════════════════════════════════════════════════════════════
def place_futures_order(
    runtime:    AppRuntime,
    side:       Literal["LONG", "SHORT"],   # LONG=매수진입/숏청산, SHORT=매도진입/롱청산
    qty:        int,
    price:      float,
    pdno:       str = "",                   # 종목코드 6자리 (미입력 시 settings["FUTURES_CODE"])
    order_type: Literal["LIMIT", "MARKET", "IOC_LIMIT", "FOK_LIMIT",
                        "IOC_MARKET", "FOK_MARKET"] = "LIMIT",
    cano:       str = "",
    session:    Literal["day", "night"] = "day",
) -> dict:
    """선물 신규 주문 → KIS 응답 dict 반환.

    Args:
        side      : LONG(매수) / SHORT(매도)
        qty       : 계약 수
        price     : 지정가 호가 (MARKET 계열 = 0)
        pdno      : 종목코드 6자리 (예: 'A01606')
        order_type: 주문유형
        cano      : 계좌번호 10자리 (미입력 시 KIS_ORDER_CANO)
        session   : 'day'(주간) / 'night'(야간 — 모의 미지원, 항상 실전 TR)
    Returns:
        output dict: ODNO(주문번호), ORD_TMD(주문시각), ITEM_NAME, ACNT_NAME
    """
    st    = runtime.settings
    _pdno = pdno or st.get("FUTURES_CODE", "")
    if not _pdno:
        raise RuntimeError("종목코드(FUTURES_CODE 또는 pdno) 미설정")
    cno8, cno2 = _get_cano(runtime, cano)

    # TR 선택
    if session == "night":
        tr_id = TR_FUTURES_ORDER_NIGHT          # 야간=모의 미지원, 항상 실전 TR
    else:
        tr_id = order_tr(TR_FUTURES_ORDER_LIVE, TR_FUTURES_ORDER_VIRTUAL, st["ORDER_ENV"])

    # 매도매수구분: 01=매도, 02=매수
    sll_buy = "02" if side == "LONG" else "01"

    # 주문구분코드
    ord_dvsn_map = {
        "LIMIT":      "01",
        "MARKET":     "02",
        "IOC_LIMIT":  "10",
        "FOK_LIMIT":  "11",
        "IOC_MARKET": "12",
        "FOK_MARKET": "13",
    }
    ord_dvsn = ord_dvsn_map.get(order_type, "01")
    unit_price = "0" if order_type != "LIMIT" else str(price)

    body = {
        "ORD_PRCS_DVSN_CD": "02",
        "CANO":              cno8,
        "ACNT_PRDT_CD":      cno2,
        "SLL_BUY_DVSN_CD":  sll_buy,
        "SHTN_PDNO":         _pdno,
        "ORD_QTY":           str(qty),
        "UNIT_PRICE":        unit_price,
        "NMPR_TYPE_CD":      "",
        "KRX_NMPR_CNDT_CD":  "",
        "CTAC_TLNO":         "",
        "FUOP_ITEM_DVSN_CD": "",
        "ORD_DVSN_CD":       ord_dvsn,
    }

    url = get_order_base_url(runtime) + EP_FUTURES_ORDER
    res = requests.post(url, headers=_order_headers(runtime, tr_id), json=body, timeout=10)
    res.raise_for_status()
    data = res.json()

    if data.get("rt_cd") != "0":
        raise RuntimeError(
            f"선물 주문 실패 | side={side} qty={qty} price={price} | "
            f"rt_cd={data.get('rt_cd')} msg={data.get('msg1','')}"
        )
    out = data.get("output", {})
    runtime.log.info(
        "선물 주문 접수 | env=%s side=%s qty=%d price=%s odno=%s tmd=%s",
        st["ORDER_ENV"], side, qty, unit_price,
        out.get("ODNO", ""), out.get("ORD_TMD", ""),
    )
    return out


# ═══════════════════════════════════════════════════════════════════════════════
# 선물옵션 정정·취소 [v1_국내선물-002]
# ═══════════════════════════════════════════════════════════════════════════════
def cancel_futures_order(
    runtime:   AppRuntime,
    orgn_odno: str,
    qty:       int   = 0,
    cano:      str   = "",
    session:   Literal["day", "night"] = "day",
) -> dict:
    """선물 주문 취소.

    Args:
        orgn_odno: 원주문번호 (place_futures_order 응답 ODNO)
        qty      : 취소 수량 (실전 전량=0, 모의는 수량 입력 필수)
        session  : 'day' / 'night'
    """
    st = runtime.settings
    cno8, cno2 = _get_cano(runtime, cano)

    if session == "night":
        tr_id = TR_FUTURES_CANCEL_NIGHT
    else:
        tr_id = order_tr(TR_FUTURES_CANCEL_LIVE, TR_FUTURES_CANCEL_VIRTUAL, st["ORDER_ENV"])

    # 모의계좌는 수량 반드시 입력 (0 불가)
    _qty = qty if (st["ORDER_ENV"] == "virtual" and qty == 0) else qty

    body = {
        "ORD_PRCS_DVSN_CD":  "02",
        "CANO":               cno8,
        "ACNT_PRDT_CD":       cno2,
        "RVSE_CNCL_DVSN_CD": "02",   # 02=취소
        "ORGN_ODNO":          orgn_odno,
        "ORD_QTY":            str(_qty),
        "UNIT_PRICE":         "0",
        "NMPR_TYPE_CD":       "01",
        "KRX_NMPR_CNDT_CD":  "0",
        "RMN_QTY_YN":         "Y",   # 전량 취소
        "FUOP_ITEM_DVSN_CD":  "",
        "ORD_DVSN_CD":        "01",
    }

    url = get_order_base_url(runtime) + EP_FUTURES_CANCEL
    res = requests.post(url, headers=_order_headers(runtime, tr_id), json=body, timeout=10)
    res.raise_for_status()
    data = res.json()

    if data.get("rt_cd") != "0":
        raise RuntimeError(
            f"선물 취소 실패 | odno={orgn_odno} | "
            f"rt_cd={data.get('rt_cd')} msg={data.get('msg1','')}"
        )
    out = data.get("output", {})
    runtime.log.info("선물 취소 완료 | odno=%s orgn=%s", out.get("ODNO",""), orgn_odno)
    return out


def modify_futures_order(
    runtime:    AppRuntime,
    orgn_odno:  str,
    new_price:  float,
    qty:        int,
    order_type: Literal["LIMIT", "MARKET"] = "LIMIT",
    cano:       str = "",
) -> dict:
    """선물 주문 정정 (가격·수량 변경).

    Args:
        orgn_odno : 원주문번호
        new_price : 변경할 가격 (시장가=0)
        qty       : 변경할 수량
    """
    st = runtime.settings
    cno8, cno2 = _get_cano(runtime, cano)
    tr_id = order_tr(TR_FUTURES_CANCEL_LIVE, TR_FUTURES_CANCEL_VIRTUAL, st["ORDER_ENV"])

    ord_dvsn   = "01" if order_type == "LIMIT" else "02"
    unit_price = str(new_price) if order_type == "LIMIT" else "0"

    body = {
        "ORD_PRCS_DVSN_CD":  "02",
        "CANO":               cno8,
        "ACNT_PRDT_CD":       cno2,
        "RVSE_CNCL_DVSN_CD": "01",   # 01=정정
        "ORGN_ODNO":          orgn_odno,
        "ORD_QTY":            str(qty),
        "UNIT_PRICE":         unit_price,
        "NMPR_TYPE_CD":       ord_dvsn,
        "KRX_NMPR_CNDT_CD":  "0",
        "RMN_QTY_YN":         "N",
        "FUOP_ITEM_DVSN_CD":  "",
        "ORD_DVSN_CD":        ord_dvsn,
    }

    url = get_order_base_url(runtime) + EP_FUTURES_CANCEL
    res = requests.post(url, headers=_order_headers(runtime, tr_id), json=body, timeout=10)
    res.raise_for_status()
    data = res.json()

    if data.get("rt_cd") != "0":
        raise RuntimeError(
            f"선물 정정 실패 | odno={orgn_odno} | "
            f"rt_cd={data.get('rt_cd')} msg={data.get('msg1','')}"
        )
    out = data.get("output", {})
    runtime.log.info("선물 정정 완료 | odno=%s orgn=%s price=%s", out.get("ODNO",""), orgn_odno, unit_price)
    return out


# ═══════════════════════════════════════════════════════════════════════════════
# 선물옵션 잔고현황 [v1_국내선물-004]
# ═══════════════════════════════════════════════════════════════════════════════
def fetch_futures_balance(
    runtime:       AppRuntime,
    cano:          str = "",
    mgna_dvsn:     Literal["01", "02"] = "01",
    excc_stat_cd:  Literal["1", "2"]   = "1",
) -> dict:
    """선물 계좌 잔고 조회 (output1: 종목별 + output2: 계좌 요약).

    Args:
        mgna_dvsn   : 증거금 구분 01=개시 / 02=유지
        excc_stat_cd: 1=정산가 / 2=본정산(매입가)
    Returns:
        {
          "positions": list of dict (output1 — 종목별),
          "summary":   dict (output2 — 계좌 요약)
        }
        positions 항목 주요 키:
          shtn_pdno, prdt_name, sll_buy_dvsn_name, cblc_qty(잔고수량),
          ccld_avg_unpr1(체결평균단가), evlu_pfls_amt(평가손익), lqd_psbl_qty(청산가능수량)
        summary 주요 키:
          ord_psbl_cash(주문가능현금), ord_psbl_tota(주문가능총액),
          prsm_dpast(추정예탁자산), wdrw_psbl_tot_amt(인출가능총금액),
          futr_trad_pfls_amt(선물실현손익), futr_evlu_pfls_amt(선물평가손익),
          add_mgna_cash(추가증거금)
    """
    cno8, cno2 = _get_cano(runtime, cano)
    tr_id       = order_tr(TR_FUTURES_BALANCE_LIVE, TR_FUTURES_BALANCE_VIRTUAL,
                           runtime.settings["ORDER_ENV"])
    url = get_order_base_url(runtime) + EP_FUTURES_BALANCE
    token = get_order_token(runtime)
    st    = runtime.settings
    headers = {
        "content-type":  "application/json; charset=utf-8",
        "authorization": f"Bearer {token}",
        "appkey":        st["ORDER_KEY"],
        "appsecret":     st["ORDER_SECRET"],
        "tr_id":         tr_id,
        "custtype":      "P",
    }
    params = {
        "CANO":              cno8,
        "ACNT_PRDT_CD":      cno2,
        "MGNA_DVSN":         mgna_dvsn,
        "EXCC_STAT_CD":      excc_stat_cd,
        "CTX_AREA_FK200":    "",
        "CTX_AREA_NK200":    "",
    }

    res = requests.get(url, headers=headers, params=params, timeout=10)
    res.raise_for_status()
    data = res.json()

    if data.get("rt_cd") != "0":
        raise RuntimeError(
            f"잔고 조회 실패 | rt_cd={data.get('rt_cd')} msg={data.get('msg1','')}"
        )
    return {
        "positions": data.get("output1", []) or [],
        "summary":   data.get("output2", {}) or {},
    }


# ═══════════════════════════════════════════════════════════════════════════════
# 선물옵션 주문가능 [v1_국내선물-005]
# ═══════════════════════════════════════════════════════════════════════════════
def fetch_orderable_qty(
    runtime:    AppRuntime,
    pdno:       str = "",
    side:       Literal["LONG", "SHORT"] = "LONG",
    price:      float = 0.0,
    order_type: Literal["LIMIT", "MARKET"] = "LIMIT",
    cano:       str = "",
) -> dict:
    """주문가능 수량 조회.

    Returns:
        dict 주요 키:
          tot_psbl_qty  : 총가능수량
          lqd_psbl_qty1 : 청산가능수량
          ord_psbl_qty  : 주문가능수량
    """
    st    = runtime.settings
    _pdno = pdno or st.get("FUTURES_CODE", "")
    cno8, cno2 = _get_cano(runtime, cano)
    tr_id  = order_tr(TR_FUTURES_ORDERABLE_LIVE, TR_FUTURES_ORDERABLE_VIRTUAL, st["ORDER_ENV"])
    sll_buy = "02" if side == "LONG" else "01"
    ord_dvsn = "01" if order_type == "LIMIT" else "02"

    token = get_order_token(runtime)
    headers = {
        "content-type":  "application/json; charset=utf-8",
        "authorization": f"Bearer {token}",
        "appkey":        st["ORDER_KEY"],
        "appsecret":     st["ORDER_SECRET"],
        "tr_id":         tr_id,
        "custtype":      "P",
    }
    params = {
        "CANO":             cno8,
        "ACNT_PRDT_CD":     cno2,
        "PDNO":             _pdno,
        "SLL_BUY_DVSN_CD":  sll_buy,
        "UNIT_PRICE":       str(price),
        "ORD_DVSN_CD":      ord_dvsn,
    }

    url = get_order_base_url(runtime) + EP_FUTURES_ORDERABLE
    res = requests.get(url, headers=headers, params=params, timeout=10)
    res.raise_for_status()
    data = res.json()

    if data.get("rt_cd") != "0":
        raise RuntimeError(
            f"주문가능 조회 실패 | rt_cd={data.get('rt_cd')} msg={data.get('msg1','')}"
        )
    return data.get("output", {}) or {}


# ═══════════════════════════════════════════════════════════════════════════════
# 선물옵션 주문체결내역 조회 [v1_국내선물-003]
# ═══════════════════════════════════════════════════════════════════════════════
def fetch_order_history(
    runtime:     AppRuntime,
    start_dt:    str = "",
    end_dt:      str = "",
    side:        Literal["ALL", "LONG", "SHORT"] = "ALL",
    ccld_dvsn:   Literal["ALL", "FILLED", "UNFILLED"] = "ALL",
    pdno:        str = "",
    sort:        Literal["AS", "DS"] = "DS",
    cano:        str = "",
) -> List[dict]:
    """선물 주문체결내역 조회 (당일 기준, 연속조회로 전체 반환).

    Args:
        start_dt : 시작일 YYYYMMDD (미입력 시 당일)
        end_dt   : 종료일 YYYYMMDD (미입력 시 당일)
        side     : ALL / LONG(매수) / SHORT(매도)
        ccld_dvsn: ALL / FILLED(체결) / UNFILLED(미체결)
        pdno     : 종목코드 (공란=전체)
        sort     : AS=정순 / DS=역순(기본)
    Returns:
        list of dict (output1 항목들)
        주요 키: odno, ord_dt, sll_buy_dvsn_cd, pdno, prdt_name,
                 ord_qty, tot_ccld_qty, avg_idx, tot_ccld_amt, ord_tmd, qty(잔량)
    """
    st    = runtime.settings
    today = datetime.now().strftime("%Y%m%d")
    _s    = start_dt or today
    _e    = end_dt   or today
    cno8, cno2 = _get_cano(runtime, cano)
    tr_id = order_tr(TR_FUTURES_CCLD_LIVE, TR_FUTURES_CCLD_VIRTUAL, st["ORDER_ENV"])

    sll_buy_map = {"ALL": "00", "LONG": "02", "SHORT": "01"}
    ccld_map    = {"ALL": "00", "FILLED": "01", "UNFILLED": "02"}
    sll_buy = sll_buy_map.get(side, "00")
    ccld    = ccld_map.get(ccld_dvsn, "00")

    token = get_order_token(runtime)
    headers = {
        "content-type":  "application/json; charset=utf-8",
        "authorization": f"Bearer {token}",
        "appkey":        st["ORDER_KEY"],
        "appsecret":     st["ORDER_SECRET"],
        "tr_id":         tr_id,
        "custtype":      "P",
        "tr_cont":       "",
    }

    url      = get_order_base_url(runtime) + EP_FUTURES_CCLD
    all_rows: List[dict] = []
    ctx_fk   = ""
    ctx_nk   = ""

    while True:
        params = {
            "CANO":           cno8,
            "ACNT_PRDT_CD":   cno2,
            "STRT_ORD_DT":    _s,
            "END_ORD_DT":     _e,
            "SLL_BUY_DVSN_CD": sll_buy,
            "CCLD_NCCS_DVSN": ccld,
            "SORT_SQN":       sort,
            "STRT_ODNO":      "",
            "PDNO":           pdno,
            "MKET_ID_CD":     "",
            "CTX_AREA_FK200": ctx_fk,
            "CTX_AREA_NK200": ctx_nk,
        }
        res = requests.get(url, headers=headers, params=params, timeout=10)
        res.raise_for_status()
        data = res.json()

        if data.get("rt_cd") != "0":
            raise RuntimeError(
                f"체결내역 조회 실패 | rt_cd={data.get('rt_cd')} msg={data.get('msg1','')}"
            )
        rows = data.get("output1", []) or []
        all_rows.extend(rows)

        # 연속조회
        tr_cont = res.headers.get("tr_cont", "")
        if tr_cont not in ("F", "M"):
            break
        ctx_fk = data.get("ctx_area_fk200", "")
        ctx_nk = data.get("ctx_area_nk200", "")
        headers["tr_cont"] = "N"

    return all_rows
