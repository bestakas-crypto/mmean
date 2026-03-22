# idx/contract_spec.py
"""
KOSPI200 선물·미니선물 상품 명세 — 틱 상수 및 거래승수 정의.

출처:
  표준 선물: 최소가격변동폭 0.05pt × 거래승수 250,000원/pt = 12,500원/tick
  미니 선물: 최소가격변동폭 0.02pt × 거래승수  50,000원/pt =  1,000원/tick

설계 원칙:
  - 가격 피드(WS), 분석·판단, TP/SL 계산 → 항상 노멀 틱 기준
  - 실제 주문 체결만 노멀/미니 구분 (종목코드 변경, 수량 환산 없음)
"""
from __future__ import annotations

# ── 표준 KOSPI200 선물 (A01xxx) ───────────────────────────────────────────────
NORMAL_TICK_PT    = 0.05        # 최소가격변동폭 (포인트)
NORMAL_MULTIPLIER = 250_000     # 거래승수 (원/포인트)
NORMAL_TICK_VALUE = 12_500      # 1틱 가치 (원) = NORMAL_TICK_PT × NORMAL_MULTIPLIER
NORMAL_PREFIX     = "A01"       # 단축코드 prefix

# ── 미니 KOSPI200 선물 (A05xxx) ───────────────────────────────────────────────
MINI_TICK_PT    = 0.02          # 최소가격변동폭 (포인트)
MINI_MULTIPLIER = 50_000        # 거래승수 (원/포인트)
MINI_TICK_VALUE = 1_000         # 1틱 가치 (원) = MINI_TICK_PT × MINI_MULTIPLIER
MINI_PREFIX     = "A05"         # 단축코드 prefix


def is_mini(futures_code: str) -> bool:
    """미니선물 종목코드 여부."""
    return futures_code.startswith(MINI_PREFIX)


def tick_pt(futures_code: str) -> float:
    """종목코드로 틱 크기(포인트) 반환. 미판별 시 노멀 기본값."""
    return MINI_TICK_PT if is_mini(futures_code) else NORMAL_TICK_PT


def tick_value(futures_code: str) -> int:
    """종목코드로 1틱 가치(원) 반환. 미판별 시 노멀 기본값."""
    return MINI_TICK_VALUE if is_mini(futures_code) else NORMAL_TICK_VALUE


def multiplier(futures_code: str) -> int:
    """종목코드로 거래승수(원/pt) 반환."""
    return MINI_MULTIPLIER if is_mini(futures_code) else NORMAL_MULTIPLIER
