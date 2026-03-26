# MMEAN/core/kis_data_api.py
"""
KIS 실계좌 데이터 조회 전용 모듈.

규칙
────
- HTTP I/O 만 담당. state 변경은 호출자(engine_runtime)가 수행.
- 모의계좌 미지원 API 전용 (실계좌 KIS_APP_KEY 사용).
- 신규 시세·지표 REST 추가 시 TR 코드를 kis_tr_catalog 에 먼저 등록.
"""
from __future__ import annotations

import time
from typing import Any, Dict, List

import requests

from app_state import AppRuntime
from kis_tr_catalog import (
    KIS_BASE_LIVE,
    KIS_BASE_VIRTUAL,
    OAUTH_TOKEN_PATH,
    OAUTH_APPROVAL_PATH,
    TR_FUTURES_PRICE,
    EP_FUTURES_PRICE,
    TR_INVESTOR_TIME_BY_MARKET,
    EP_INVESTOR_TIME_BY_MARKET,
    TR_FOREIGN_FLOW_TREND,
    EP_FOREIGN_FLOW_TREND,
    TR_INVESTOR_ESTIMATE,
    EP_INVESTOR_ESTIMATE,
    FUTURES_MRKT_DIV_DAY,
    FUTURES_MRKT_DIV_NIGHT,
)


# ── BASE URL ──────────────────────────────────────────────────────────────────
def get_data_base_url(runtime: AppRuntime) -> str:
    """데이터 계좌(실계좌) BASE URL — KIS_DATA_ENV(=ENV) 기준."""
    return KIS_BASE_LIVE if runtime.settings["ENV"] == "live" else KIS_BASE_VIRTUAL


# ── 데이터 계좌 토큰 ──────────────────────────────────────────────────────────
def get_data_token(runtime: AppRuntime) -> str:
    """실계좌 access_token 반환. 만료 5분 전 자동 갱신.

    캐시: runtime.state_obj.token_cache
    """
    st    = runtime.settings
    cache = runtime.state_obj.token_cache
    lock  = runtime.state_obj.token_lock

    with lock:
        if (
            cache["access_token"]
            and (time.time() - float(cache["issued_at"])) < (int(cache["expires_in"]) - 300)
        ):
            return str(cache["access_token"])

    if not st["APP_KEY"] or not st["APP_SECRET"]:
        raise RuntimeError("KIS_APP_KEY / KIS_APP_SECRET 미설정 — .env.secrets 확인")

    res = requests.post(
        get_data_base_url(runtime) + OAUTH_TOKEN_PATH,
        json={"grant_type": "client_credentials", "appkey": st["APP_KEY"], "appsecret": st["APP_SECRET"]},
        timeout=10,
    )
    res.raise_for_status()
    data       = res.json()
    token      = data.get("access_token", "").strip()
    expires_in = int(data.get("expires_in", 86400))
    if not token:
        raise RuntimeError(f"[DATA] access_token 발급 실패: {data}")

    with lock:
        cache.update({"access_token": token, "issued_at": time.time(), "expires_in": expires_in})
    runtime.log.info("데이터 토큰 발급 | env=%s | key=...%s", st["ENV"], st["APP_KEY"][-6:])
    return token


# ── 공통 데이터 헤더 ──────────────────────────────────────────────────────────
def _data_headers(runtime: AppRuntime, tr_id: str) -> Dict[str, str]:
    token = get_data_token(runtime)
    st    = runtime.settings
    return {
        "content-type":  "application/json; charset=utf-8",
        "authorization": f"Bearer {token}",
        "appkey":        st["APP_KEY"],
        "appsecret":     st["APP_SECRET"],
        "tr_id":         tr_id,
        "custtype":      "P",
    }


# ── WebSocket approval_key ────────────────────────────────────────────────────
def issue_approval_key(runtime: AppRuntime) -> str:
    """WebSocket 실시간 구독용 approval_key — 데이터 계좌(실계좌) 전용."""
    st = runtime.settings
    res = requests.post(
        get_data_base_url(runtime) + OAUTH_APPROVAL_PATH,
        headers={"content-type": "application/json"},
        json={"grant_type": "client_credentials", "appkey": st["APP_KEY"], "secretkey": st["APP_SECRET"]},
        timeout=10,
    )
    res.raise_for_status()
    data         = res.json()
    approval_key = data.get("approval_key", "").strip()
    if not approval_key:
        raise RuntimeError(f"approval_key 발급 실패: {data}")
    return approval_key


# ── 선물 현재가 시세 [v1_국내선물-006] ───────────────────────────────────────
def fetch_futures_price(runtime: AppRuntime, pdno: str, session: str = "day") -> Dict[str, Any]:
    """선물 현재가·베이시스·미결제 등 정적 시세 1회 조회.

    Args:
        pdno   : 종목코드 (예: 'A01606', '101S03')
        session: 'day' (기본) 또는 'night'
    Returns:
        dict — 주요 키:
          futs_prpr     : 현재가 (str)
          mrkt_basis    : 시장 베이시스 (str)
          hts_otst_stpl_qty : 미결제 약정 수량 (str)
          acml_vol      : 누적 거래량 (str)
          futs_prdy_ctrt: 전일 대비율 (str)
          futs_hgpr     : 최고가 (str)
          futs_lwpr     : 최저가 (str)
          hist_vltl     : 역사적 변동성 (str, 옵션)
    """
    mrkt_div = FUTURES_MRKT_DIV_NIGHT if session == "night" else FUTURES_MRKT_DIV_DAY
    url = get_data_base_url(runtime) + EP_FUTURES_PRICE
    res = requests.get(
        url,
        headers=_data_headers(runtime, TR_FUTURES_PRICE),
        params={"FID_COND_MRKT_DIV_CODE": mrkt_div, "FID_INPUT_ISCD": pdno},
        timeout=10,
    )
    res.raise_for_status()
    body = res.json()
    if body.get("rt_cd") != "0":
        raise RuntimeError(
            f"선물 시세 조회 실패 | pdno={pdno} rt_cd={body.get('rt_cd')} msg={body.get('msg1','')}"
        )
    return body.get("output", {}) or {}


# ── 투자자 매매 단건 조회 [v1_국내주식-074] ────────────────────────────────
def fetch_investor_once(runtime: AppRuntime) -> Dict[str, Any]:
    """시장별 투자자 매매동향 1회 조회 → raw output dict 반환.

    호출자(engine_runtime)가 state 업데이트 담당.
    TR: FHPTJ04030000 — 실계좌 전용
    """
    st  = runtime.settings
    url = get_data_base_url(runtime) + EP_INVESTOR_TIME_BY_MARKET

    res = requests.get(
        url,
        headers=_data_headers(runtime, TR_INVESTOR_TIME_BY_MARKET),
        params={
            "fid_input_iscd":   st["INVESTOR_MARKET_CODE"],
            "fid_input_iscd_2": st["INVESTOR_SUB_CODE"],
        },
        timeout=10,
    )
    res.raise_for_status()
    body = res.json()
    if body.get("rt_cd") != "0":
        raise RuntimeError(
            f"투자자 API 오류 rt_cd={body.get('rt_cd')} msg={body.get('msg1','')}"
        )
    raw = body.get("output", {})
    return raw[0] if isinstance(raw, list) else (raw or {})


def fetch_investor_by_market(
    runtime: AppRuntime,
    market_code: str,
    sub_code: str,
) -> Dict[str, Any]:
    """시장코드·세부코드 지정 투자자 매매동향 단건 조회.

    fetch_investor_once()의 범용 버전 — 선물(K2I/F001)·현물(KSP/2001) 등
    모든 시장에 재사용 가능.
    """
    url = get_data_base_url(runtime) + EP_INVESTOR_TIME_BY_MARKET
    res = requests.get(
        url,
        headers=_data_headers(runtime, TR_INVESTOR_TIME_BY_MARKET),
        params={
            "fid_input_iscd":   market_code,
            "fid_input_iscd_2": sub_code,
        },
        timeout=10,
    )
    res.raise_for_status()
    body = res.json()
    if body.get("rt_cd") != "0":
        raise RuntimeError(
            f"투자자 API 오류 market={market_code} "
            f"rt_cd={body.get('rt_cd')} msg={body.get('msg1','')}"
        )
    raw = body.get("output", {})
    return raw[0] if isinstance(raw, list) else (raw or {})


# ── 외국계 순매수 추이 [국내주식-164] ────────────────────────────────────────
def fetch_foreign_flow_trend(
    runtime: AppRuntime,
    iscd: str,
    iscd2: str = "99999",
    mrkt_div: str = "J",
) -> List[Dict[str, Any]]:
    """종목별 외국계 시간대별 순매수 추이 조회.

    Args:
        iscd    : 종목코드 (예: 코스피200 ETF '069500', 주식 '005930')
        iscd2   : 외국계 구분 (99999=전체)
        mrkt_div: 시장구분 (J=KRX, KRX만 지원)
    Returns:
        list of dict — 각 항목:
          bsop_hour           : 영업시간 (HHMMSS)
          stck_prpr           : 주식 현재가
          glob_ntby_qty       : 외국계 순매수 수량
          frgn_ntby_qty_icdc  : 외국인 순매수 증감
          acml_vol            : 누적 거래량
    """
    url = get_data_base_url(runtime) + EP_FOREIGN_FLOW_TREND
    res = requests.get(
        url,
        headers=_data_headers(runtime, TR_FOREIGN_FLOW_TREND),
        params={
            "FID_INPUT_ISCD":           iscd,
            "FID_INPUT_ISCD_2":         iscd2,
            "FID_COND_MRKT_DIV_CODE":   mrkt_div,
        },
        timeout=10,
    )
    res.raise_for_status()
    body = res.json()
    if body.get("rt_cd") != "0":
        raise RuntimeError(
            f"외국계 추이 오류 rt_cd={body.get('rt_cd')} msg={body.get('msg1','')}"
        )
    return body.get("output", []) or []


# ── 외인기관 추정가집계 [v1_국내주식-046] ─────────────────────────────────
def fetch_investor_estimate(runtime: AppRuntime, iscd: str) -> List[Dict[str, Any]]:
    """장중 외인/기관 추정 순매수 가집계 조회.

    Args:
        iscd: 종목코드 (예: '000660')
    Returns:
        list of dict (output2) — 각 항목:
          bsop_hour_gb      : 구분 (1=09:30 / 2=10:00 / 3=11:20 / 4=13:20 / 5=14:30)
          frgn_fake_ntby_qty: 외국인 가집계 순매수 수량
          orgn_fake_ntby_qty: 기관 가집계 순매수 수량
          sum_fake_ntby_qty : 합산 가집계 순매수 수량
    """
    url = get_data_base_url(runtime) + EP_INVESTOR_ESTIMATE
    res = requests.get(
        url,
        headers=_data_headers(runtime, TR_INVESTOR_ESTIMATE),
        params={"MKSC_SHRN_ISCD": iscd},
        timeout=10,
    )
    res.raise_for_status()
    body = res.json()
    if body.get("rt_cd") != "0":
        raise RuntimeError(
            f"외인기관 추정 오류 rt_cd={body.get('rt_cd')} msg={body.get('msg1','')}"
        )
    return body.get("output2", []) or []
