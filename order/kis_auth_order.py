# order/kis_auth_order.py
"""
주문 계좌 전용 인증 모듈

데이터 계좌(실계좌) 인증: core/engine_runtime.py get_valid_token() 사용
주문 계좌(모의/실계좌) 인증: 이 모듈 전용

함수 목록
  order_base_url()        주문 REST base URL (ORDER_ENV 기준)
  order_ws_url()          주문 WebSocket URL (ORDER_ENV 기준)
  get_order_token()       액세스 토큰 (캐싱, 만료 5분 전 자동갱신)
  get_order_approval_key() WS-2 구독용 approval_key
  make_order_headers()    REST 공통 헤더 dict 생성
  aes_decrypt()           체결통보 AES-CBC 복호화
"""
from __future__ import annotations

import base64
import time
from typing import Optional

import requests

from app_state import AppRuntime


# ── URL 헬퍼 ────────────────────────────────────────────────────────────────

def order_base_url(runtime: AppRuntime) -> str:
    """주문 계좌 REST base URL. ORDER_ENV=live → 실거래, virtual → 모의."""
    return (
        "https://openapi.koreainvestment.com:9443"
        if runtime.settings["ORDER_ENV"] == "live"
        else "https://openapivts.koreainvestment.com:29443"
    )


def order_ws_url(runtime: AppRuntime) -> str:
    """주문 계좌 WebSocket URL. ORDER_ENV=live → :21000, virtual → :31000."""
    return (
        "ws://ops.koreainvestment.com:21000"
        if runtime.settings["ORDER_ENV"] == "live"
        else "ws://ops.koreainvestment.com:31000"
    )


# ── 토큰 ────────────────────────────────────────────────────────────────────

def get_order_token(runtime: AppRuntime) -> str:
    """
    주문 계좌 액세스 토큰 반환. 만료 5분 전 자동 갱신.
    ORDER_ENV=virtual(모의) / live(실거래)
    실거래 전환: .env.public KIS_ORDER_ENV=virtual → live 한 줄만 변경.
    """
    st    = runtime.settings
    cache = runtime.state_obj.order_token_cache
    lock  = runtime.state_obj.order_token_lock

    with lock:
        if (
            cache["access_token"]
            and (time.time() - float(cache["issued_at"]))
            < (int(cache["expires_in"]) - 300)
        ):
            return str(cache["access_token"])

    if not st["ORDER_KEY"] or not st["ORDER_SECRET"]:
        raise RuntimeError(
            "KIS_ORDER_KEY / KIS_ORDER_SECRET 미설정 — .env.secrets 확인"
        )

    res = requests.post(
        order_base_url(runtime) + "/oauth2/tokenP",
        json={
            "grant_type": "client_credentials",
            "appkey":     st["ORDER_KEY"],
            "appsecret":  st["ORDER_SECRET"],
        },
        timeout=10,
    )
    res.raise_for_status()
    data       = res.json()
    token      = data.get("access_token", "").strip()
    expires_in = int(data.get("expires_in", 86400))

    if not token:
        raise RuntimeError(f"[ORDER] access_token 발급 실패: {data}")

    with lock:
        cache.update({
            "access_token": token,
            "issued_at":    time.time(),
            "expires_in":   expires_in,
        })

    runtime.log.info(
        "주문 토큰 발급 | env=%s | key=...%s",
        st["ORDER_ENV"], st["ORDER_KEY"][-6:],
    )
    return token


def get_order_approval_key(runtime: AppRuntime) -> str:
    """
    주문 계좌 WebSocket approval_key 발급.
    WS-2 구독 메시지 헤더에 사용. 재연결마다 새로 발급.
    """
    st = runtime.settings
    res = requests.post(
        order_base_url(runtime) + "/oauth2/Approval",
        headers={"content-type": "application/json"},
        json={
            "grant_type": "client_credentials",
            "appkey":     st["ORDER_KEY"],
            "secretkey":  st["ORDER_SECRET"],
        },
        timeout=10,
    )
    res.raise_for_status()
    data = res.json()
    key  = data.get("approval_key", "").strip()
    if not key:
        raise RuntimeError(f"[ORDER] approval_key 발급 실패: {data}")
    return key


# ── REST 헤더 ────────────────────────────────────────────────────────────────

def make_order_headers(
    runtime: AppRuntime,
    tr_id:   str,
    tr_cont: str = "",
) -> dict:
    """주문 REST API 공통 헤더 dict 생성."""
    token = get_order_token(runtime)
    st    = runtime.settings
    return {
        "content-type":  "application/json; charset=utf-8",
        "authorization": f"Bearer {token}",
        "appkey":        st["ORDER_KEY"],
        "appsecret":     st["ORDER_SECRET"],
        "tr_id":         tr_id,
        "custtype":      "P",
        "tr_cont":       tr_cont,
    }


# ── AES 복호화 ───────────────────────────────────────────────────────────────

def aes_decrypt(cipher_b64: str, key: str, iv: str) -> str:
    """
    KIS 체결통보 WebSocket AES-CBC 복호화.

    key, iv: 구독 성공 응답 output.key / output.iv (각 32/16자 UTF-8 문자열)
    cipher_b64: 수신 메시지에서 '|' 구분 후 4번째 필드 (base64 인코딩)

    pycryptodome 필요: pip install pycryptodome
    """
    try:
        from Crypto.Cipher import AES
        from Crypto.Util.Padding import unpad
    except ImportError as e:
        raise RuntimeError(
            "pycryptodome 미설치 — pip install pycryptodome"
        ) from e

    cipher    = AES.new(key.encode("utf-8"), AES.MODE_CBC, iv.encode("utf-8"))
    decrypted = unpad(
        cipher.decrypt(base64.b64decode(cipher_b64)),
        AES.block_size,
    )
    return decrypted.decode("utf-8")
