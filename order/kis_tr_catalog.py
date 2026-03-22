# order/kis_tr_catalog.py
"""
KIS Open API TR 코드 전체 레지스트리

규칙:
  - TR ID / REST URL / WS 여부는 오직 이 파일에서만 관리
  - 엔진·API 파일에 TR ID 문자열 직접 기입 금지
  - 모의(virtual) 미지원 항목은 None 으로 명시
"""
from __future__ import annotations

TR_CATALOG: dict = {

    # ══════════════════════════════════════════════════════════════════
    # WebSocket — 데이터 계좌 (실계좌, :21000)
    # ══════════════════════════════════════════════════════════════════
    "IDX_FUT_TICK": {
        "real":    "H0IFCNT0",
        "virtual": "H0IFCNT0",   # 시세 WS는 실전/모의 동일 TR, URL만 다름
        "ws":      True,
        "desc":    "지수선물 실시간체결가 [실시간-010]",
    },
    "IDX_FUT_QUOTE": {
        "real":    "H0IFASP0",
        "virtual": "H0IFASP0",
        "ws":      True,
        "desc":    "지수선물 실시간호가 [실시간-011]",
    },

    # ══════════════════════════════════════════════════════════════════
    # WebSocket — 주문 계좌 (WS-2, :31000 모의 / :21000 실전)
    # tr_key = HTS ID  |  메시지 AES-CBC 암호화
    # ══════════════════════════════════════════════════════════════════
    "FILL_NOTICE_DAY": {
        "real":    "H0IFCNI0",
        "virtual": "H0IFCNI9",
        "ws":      True,
        "encrypted": True,
        "desc":    "선물옵션 실시간체결통보 [실시간-012]  tr_key=HTS_ID",
    },
    "KRX_NIGHT_TICK": {
        "real":    "H0MFCNT0",
        "virtual": None,          # 모의 야간 미지원
        "ws":      True,
        "desc":    "KRX야간선물 실시간종목체결 [실시간-064]",
    },
    "KRX_NIGHT_QUOTE": {
        "real":    "H0MFASP0",
        "virtual": None,
        "ws":      True,
        "desc":    "KRX야간선물 실시간호가 [실시간-065]",
    },
    "FILL_NOTICE_NIGHT": {
        "real":    "H0MFCNI0",
        "virtual": None,          # 모의 야간 미지원
        "ws":      True,
        "encrypted": True,
        "desc":    "KRX야간선물 실시간체결통보 [실시간-066]  tr_key=HTS_ID",
    },

    # ══════════════════════════════════════════════════════════════════
    # REST — 주문 (주문 계좌)
    # ══════════════════════════════════════════════════════════════════
    "ORDER_DAY": {
        "real":    "TTTO1101U",
        "virtual": "VTTO1101U",
        "url":     "/uapi/domestic-futureoption/v1/trading/order",
        "method":  "POST",
        "desc":    "선물옵션 주문 [v1_국내선물-001] 주간",
    },
    "ORDER_NIGHT": {
        "real":    "STTN1101U",
        "virtual": None,          # 모의 야간 주문 미지원
        "url":     "/uapi/domestic-futureoption/v1/trading/order",
        "method":  "POST",
        "desc":    "선물옵션 주문 [v1_국내선물-001] 야간",
    },
    "ORDER_CANCEL_DAY": {
        "real":    "TTTO1103U",
        "virtual": "VTTO1103U",
        "url":     "/uapi/domestic-futureoption/v1/trading/order-rvsecncl",
        "method":  "POST",
        "desc":    "선물옵션 정정취소주문 [v1_국내선물-002] 주간",
    },
    "ORDER_CANCEL_NIGHT": {
        "real":    "STTN1103U",
        "virtual": None,
        "url":     "/uapi/domestic-futureoption/v1/trading/order-rvsecncl",
        "method":  "POST",
        "desc":    "선물옵션 정정취소주문 [v1_국내선물-002] 야간 (신)",
    },

    # ══════════════════════════════════════════════════════════════════
    # REST — 조회 (주문 계좌)
    # ══════════════════════════════════════════════════════════════════
    "FILL_HISTORY": {
        "real":    "TTTO5201R",
        "virtual": "VTTO5201R",
        "url":     "/uapi/domestic-futureoption/v1/trading/inquire-ccnl",
        "method":  "GET",
        "desc":    "선물옵션 주문체결내역조회 [v1_국내선물-003]  예외·디버그용",
    },
    "BALANCE": {
        "real":    "CTFO6118R",
        "virtual": "VTFO6118R",
        "url":     "/uapi/domestic-futureoption/v1/trading/inquire-balance",
        "method":  "GET",
        "desc":    "선물옵션 잔고현황 [v1_국내선물-004]  시작1회+WS재연결후만",
    },
    "ORDERABLE": {
        "real":    "TTTO5105R",
        "virtual": "VTTO5105R",
        "url":     "/uapi/domestic-futureoption/v1/trading/inquire-psbl-order",
        "method":  "GET",
        "desc":    "선물옵션 주문가능 [v1_국내선물-005]",
    },

    # ══════════════════════════════════════════════════════════════════
    # REST — 시세 조회 (데이터 계좌, 실계좌 전용)
    # ══════════════════════════════════════════════════════════════════
    "FUTURES_PRICE": {
        "real":    "FHKIF03010100",
        "virtual": None,          # 실계좌 전용
        "url":     "/uapi/domestic-futureoption/v1/quotations/inquire-price",
        "method":  "GET",
        "desc":    "선물옵션 시세 [v1_국내선물-006]  실계좌 전용",
    },
    "INVESTOR_TIME": {
        "real":    "FHPTJ04030000",
        "virtual": None,          # 실계좌 전용
        "url":     "/uapi/domestic-stock/v1/quotations/inquire-investor-time-by-market",
        "method":  "GET",
        "desc":    "시장별 투자자매매동향(시세) [v1_국내주식-074]  실계좌 전용",
    },
    "FOREIGN_INSTITUTION": {
        "real":    "FHPTJ04030000",
        "virtual": None,
        "url":     "/uapi/domestic-stock/v1/quotations/inquire-investor-time-by-market",
        "method":  "GET",
        "desc":    "종목별 외인기관 추정가집계 [v1_국내주식-046]  실계좌 전용",
    },
}


def get_tr_id(name: str, env: str) -> str:
    """
    TR 이름 + 환경 → TR ID 반환
    env: 'live' | 'virtual'
    """
    entry = TR_CATALOG.get(name)
    if not entry:
        raise KeyError(f"TR 미등록: '{name}' — kis_tr_catalog.py 확인")
    key = "real" if env == "live" else "virtual"
    tr_id = entry.get(key)
    if not tr_id:
        raise ValueError(f"TR [{name}] 은 {env} 환경을 지원하지 않습니다")
    return tr_id


def get_url(name: str) -> str:
    """TR 이름 → REST URL 경로 반환 (WebSocket 전용이면 ValueError)"""
    entry = TR_CATALOG.get(name)
    if not entry:
        raise KeyError(f"TR 미등록: '{name}'")
    if entry.get("ws"):
        raise ValueError(f"TR [{name}] 은 WebSocket 전용 — REST URL 없음")
    url = entry.get("url")
    if not url:
        raise ValueError(f"TR [{name}] 에 URL 미정의")
    return url


def is_encrypted(name: str) -> bool:
    """체결통보 등 AES 암호화 여부"""
    return bool((TR_CATALOG.get(name) or {}).get("encrypted"))
