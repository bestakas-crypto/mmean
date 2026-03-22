# MMEAN/core/kis_tr_catalog.py
"""
KIS API TR 코드 카탈로그 — 모든 TR ID / 엔드포인트 / 필드 인덱스를 단일 파일에 등록.

출처: <프로젝트루트>/TRcode/ xlsx 문서 전수 분석 (2026-03-20)

규칙
────
- 신규 TR 추가 시 이 파일에 먼저 등록 후 kis_data_api / kis_order_api 에서 참조.
- TR ID는 상수(str)로 정의 — 코드 전체에서 문자열 하드코딩 금지.
- CATEGORY: DATA(시세·지표 REST), ORDER(주문 REST), WS(WebSocket 실시간)
"""
from __future__ import annotations

# ═══════════════════════════════════════════════════════════════════════════════
# BASE URL
# ═══════════════════════════════════════════════════════════════════════════════
KIS_BASE_LIVE    = "https://openapi.koreainvestment.com:9443"
KIS_BASE_VIRTUAL = "https://openapivts.koreainvestment.com:29443"
KIS_WS_DAY   = "ws://ops.koreainvestment.com:21000"    # 주간 WebSocket
KIS_WS_NIGHT = "ws://ops.koreainvestment.com:21000"    # 야간 WebSocket (동일 호스트)

# ── OAuth ─────────────────────────────────────────────────────────────────────
OAUTH_TOKEN_PATH    = "/oauth2/tokenP"      # POST → access_token
OAUTH_APPROVAL_PATH = "/oauth2/Approval"    # POST → WebSocket approval_key


# ═══════════════════════════════════════════════════════════════════════════════
# DATA TR — 실계좌 전용 (시세·지표 REST, 모의 미지원)
# ═══════════════════════════════════════════════════════════════════════════════

# ── 선물 현재가 시세 [v1_국내선물-006] ───────────────────────────────────────
TR_FUTURES_PRICE       = "FHMIF10000000"          # 실전/모의 동일 TR
EP_FUTURES_PRICE       = "/uapi/domestic-futureoption/v1/quotations/inquire-price"
# params: FID_COND_MRKT_DIV_CODE (F:지수선물/CM:야간선물 등), FID_INPUT_ISCD (종목코드)
# FID_COND_MRKT_DIV_CODE 값:
#   F=지수선물 O=지수옵션 JF=주식선물 JO=주식옵션
#   CF=상품·금리·통화선물 CM=야간선물 EU=야간옵션
FUTURES_MRKT_DIV_DAY   = "F"   # 주간 지수선물
FUTURES_MRKT_DIV_NIGHT = "CM"  # 야간 지수선물

# ── 시장별 투자자매매동향 [v1_국내주식-074] ────────────────────────────────
TR_INVESTOR_TIME_BY_MARKET = "FHPTJ04030000"   # 모의 미지원
EP_INVESTOR_TIME_BY_MARKET = (
    "/uapi/domestic-stock/v1/quotations/inquire-investor-time-by-market"
)
# params: fid_input_iscd (시장코드), fid_input_iscd_2 (세부코드)
#   시장코드: K2I(선물/콜/풋) KSP(코스피) KSQ(코스닥) MKI(미니선물) ...
#   세부코드 (K2I 기준): F001(선물) OC01(콜) OP01(풋)

# ── 종목별 외국계 순매수추이 [국내주식-164] ────────────────────────────────
TR_FOREIGN_FLOW_TREND = "FHKST644400C0"         # 모의 미지원
EP_FOREIGN_FLOW_TREND = "/uapi/domestic-stock/v1/quotations/frgnmem-pchs-trend"
# params: FID_INPUT_ISCD(종목코드), FID_INPUT_ISCD_2(99999=전체), FID_COND_MRKT_DIV_CODE(J)
# output(array): bsop_hour, stck_prpr, glob_ntby_qty, frgn_ntby_qty_icdc ...

# ── 종목별 외인기관 추정가집계 [v1_국내주식-046] ─────────────────────────
TR_INVESTOR_ESTIMATE  = "HHPTJ04160200"         # 모의 미지원
EP_INVESTOR_ESTIMATE  = "/uapi/domestic-stock/v1/quotations/investor-trend-estimate"
# params: MKSC_SHRN_ISCD(종목코드)
# output2(array): bsop_hour_gb(1~5), frgn_fake_ntby_qty, orgn_fake_ntby_qty, sum_fake_ntby_qty
# 장중 업데이트: 외국인 09:30/11:20/13:20/14:30, 기관 10:00/11:20/13:20/14:30


# ═══════════════════════════════════════════════════════════════════════════════
# ORDER TR — 주간 선물옵션 주문 (실계좌/모의 구분)
# ═══════════════════════════════════════════════════════════════════════════════

# ── 선물옵션 주문 [v1_국내선물-001] ──────────────────────────────────────────
TR_FUTURES_ORDER_LIVE    = "TTTO1101U"   # 실전 주간
TR_FUTURES_ORDER_NIGHT   = "STTN1101U"  # 실전 야간 (신 TR)
TR_FUTURES_ORDER_VIRTUAL = "VTTO1101U"  # 모의 주간 (야간 미제공)
EP_FUTURES_ORDER = "/uapi/domestic-futureoption/v1/trading/order"
# body 필수: ORD_PRCS_DVSN_CD=02, CANO(8), ACNT_PRDT_CD(2),
#            SLL_BUY_DVSN_CD(01:매도/02:매수), SHTN_PDNO(종목코드6자리),
#            ORD_QTY, UNIT_PRICE(시장가=0), ORD_DVSN_CD
# ORD_DVSN_CD: 01:지정가 02:시장가 03:조건부 04:최유리
#              10:지정가(IOC) 11:지정가(FOK) 12:시장가(IOC) 13:시장가(FOK)
# output: ODNO(주문번호), ORD_TMD(주문시각), ACNT_NAME, ITEM_NAME

# ── 선물옵션 정정취소주문 [v1_국내선물-002] ────────────────────────────────
TR_FUTURES_CANCEL_LIVE    = "TTTO1103U"
TR_FUTURES_CANCEL_NIGHT   = "STTN1103U"
TR_FUTURES_CANCEL_VIRTUAL = "VTTO1103U"
EP_FUTURES_CANCEL = "/uapi/domestic-futureoption/v1/trading/order-rvsecncl"
# body 필수: ORD_PRCS_DVSN_CD=02, CANO, ACNT_PRDT_CD,
#            RVSE_CNCL_DVSN_CD(01:정정/02:취소),
#            ORGN_ODNO(원주문번호), ORD_QTY(전량=0 실전/모의는 수량입력),
#            UNIT_PRICE(취소=0), NMPR_TYPE_CD, KRX_NMPR_CNDT_CD(취소=0),
#            RMN_QTY_YN(Y:전량/N:일부), ORD_DVSN_CD(취소=01)

# ── 선물옵션 잔고현황 [v1_국내선물-004] ────────────────────────────────────
TR_FUTURES_BALANCE_LIVE    = "CTFO6118R"
TR_FUTURES_BALANCE_VIRTUAL = "VTFO6118R"
EP_FUTURES_BALANCE = "/uapi/domestic-futureoption/v1/trading/inquire-balance"
# params: CANO, ACNT_PRDT_CD, MGNA_DVSN(01:개시/02:유지), EXCC_STAT_CD(1:정산/2:본정산),
#         CTX_AREA_FK200, CTX_AREA_NK200 (페이징, 최초="")
# output1(array): pdno, shtn_pdno, prdt_name, sll_buy_dvsn_name, cblc_qty,
#                 ccld_avg_unpr1, evlu_pfls_amt, trad_pfls_amt, lqd_psbl_qty
# output2(object): ord_psbl_cash, ord_psbl_tota, prsm_dpast, wdrw_psbl_tot_amt,
#                  futr_trad_pfls_amt, futr_evlu_pfls_amt, add_mgna_cash

# ── 선물옵션 주문가능 [v1_국내선물-005] ────────────────────────────────────
TR_FUTURES_ORDERABLE_LIVE    = "TTTO5105R"
TR_FUTURES_ORDERABLE_VIRTUAL = "VTTO5105R"
EP_FUTURES_ORDERABLE = "/uapi/domestic-futureoption/v1/trading/inquire-psbl-order"
# params: CANO, ACNT_PRDT_CD, PDNO(종목코드), SLL_BUY_DVSN_CD, UNIT_PRICE, ORD_DVSN_CD
# output: tot_psbl_qty(총가능수량), lqd_psbl_qty1(청산가능수량), ord_psbl_qty(주문가능수량)

# ── 선물옵션 주문체결내역조회 [v1_국내선물-003] ────────────────────────────
TR_FUTURES_CCLD_LIVE    = "TTTO5201R"
TR_FUTURES_CCLD_VIRTUAL = "VTTO5201R"
EP_FUTURES_CCLD = "/uapi/domestic-futureoption/v1/trading/inquire-ccnl"
# params: CANO, ACNT_PRDT_CD, STRT_ORD_DT(YYYYMMDD), END_ORD_DT,
#         SLL_BUY_DVSN_CD(00:전체/01:매도/02:매수),
#         CCLD_NCCS_DVSN(00:전체/01:체결/02:미체결), SORT_SQN(AS:정순/DS:역순),
#         STRT_ODNO, PDNO, MKET_ID_CD(공란), CTX_AREA_FK200, CTX_AREA_NK200
# output1(array): odno, ord_dt, sll_buy_dvsn_cd, pdno, prdt_name, ord_qty,
#                 tot_ccld_qty, avg_idx, tot_ccld_amt, ord_tmd, qty(잔량)
# output2(object): tot_ord_qty, tot_ccld_amt_smtl, fee_smtl


# ═══════════════════════════════════════════════════════════════════════════════
# WebSocket TR — 실시간 (모의 미지원)
# ═══════════════════════════════════════════════════════════════════════════════

# ── 지수선물 실시간 체결 [실시간-010/064] ──────────────────────────────────
WS_FUTURES_TICK_DAY   = "H0IFCNT0"   # 주간 지수선물 체결틱
WS_FUTURES_TICK_NIGHT = "H0MFCNT0"   # 야간 KRX 선물 체결틱
# tr_key: 종목코드 (예: A01606, 101S03)
# response(^구분): 인덱스별 주요 필드
WS_TICK_IDX = {
    "FUTS_SHRN_ISCD":   0,   # 단축 종목코드
    "BSOP_HOUR":        1,   # 영업 시간
    "FUTS_PRPR":        5,   # 선물 현재가
    "FUTS_OPRC":        6,   # 시가
    "FUTS_HGPR":        7,   # 최고가
    "FUTS_LWPR":        8,   # 최저가
    "LAST_CNQN":        9,   # 최종 체결량 (trade_qty)
    "ACML_VOL":        10,   # 누적 거래량 (cum_volume)
    "MRKT_BASIS":      13,   # 시장 베이시스 (basis)
    "HTS_OTST_STPL_QTY": 18, # 미결제 약정 (oi_value)
    "CTTR":            30,   # 체결강도 (trade_strength)
    "FUTS_ASKP1":      34,   # 매도호가1 (best_ask)
    "FUTS_BIDP1":      35,   # 매수호가1 (best_bid)
}

# ── 지수선물 실시간 호가 [실시간-011/065] ──────────────────────────────────
WS_FUTURES_ASK_DAY   = "H0IFASP0"   # 주간 호가
WS_FUTURES_ASK_NIGHT = "H0MFASP0"   # 야간 호가
# tr_key: 종목코드 / 0.2초 필터링 적용
# response(^구분): 호가 5단계 + 잔량
WS_ASK_IDX = {
    "FUTS_SHRN_ISCD":        0,
    "BSOP_HOUR":             1,
    "FUTS_ASKP1":            2,   # 매도호가1 (best_ask)
    "FUTS_BIDP1":            7,   # 매수호가1 (best_bid)
    "TOTAL_ASKP_RSQN":      22,
    "TOTAL_BIDP_RSQN":      23,
}

# ── 야간선물 실시간 체결통보 [실시간-066] ─────────────────────────────────
WS_NIGHT_EXEC_NOTICE = "H0MFCNI0"
# ⚠️ tr_key = HTS ID (종목코드 아님!) — 본인 주문 체결 알림 전용
# response: CUST_ID, ACNT_NO, ODER_NO, CNTG_QTY, CNTG_UNPR, RFUS_YN, CNTG_YN

# ── 세션별 TR 묶음 ─────────────────────────────────────────────────────────
WS_TR_BY_SESSION: dict[str, dict[str, str]] = {
    "day": {
        "tick": WS_FUTURES_TICK_DAY,
        "ask":  WS_FUTURES_ASK_DAY,
    },
    "night": {
        "tick": WS_FUTURES_TICK_NIGHT,
        "ask":  WS_FUTURES_ASK_NIGHT,
    },
}


# ═══════════════════════════════════════════════════════════════════════════════
# 헬퍼
# ═══════════════════════════════════════════════════════════════════════════════
def order_tr(live_tr: str, virtual_tr: str, order_env: str) -> str:
    """ORDER_ENV('live'/'virtual') 에 따라 올바른 TR ID 반환."""
    return live_tr if order_env == "live" else virtual_tr


def night_order_tr(order_env: str) -> str:
    """야간 선물 주문 TR — 모의 미지원이므로 야간=항상 실전 TR."""
    return TR_FUTURES_ORDER_NIGHT  # STTN1101U


def night_cancel_tr(order_env: str) -> str:
    """야간 선물 취소 TR."""
    return TR_FUTURES_CANCEL_NIGHT  # STTN1103U
