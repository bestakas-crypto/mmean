# MMEAN/engine_runtime.py
from __future__ import annotations

import asyncio
import json
import os
import threading
import time
from datetime import datetime, timedelta
from typing import Any, Dict, List, Tuple

import requests
import websockets

from app_state import AppRuntime
from regime_engine import BiasInputs, BIAS_LONG, BIAS_SHORT

# ── KIS API 계층 ─────────────────────────────────────────────────────────────
# 데이터(실계좌): 토큰·approval_key·투자자 REST
from kis_data_api import get_data_token, issue_approval_key, fetch_investor_once
# 주문(모의/실전): 토큰 — core 계층
from kis_order_api import get_order_token  # noqa: F401
# 잔고 조회 — order 레이어 (get_balance는 order/kis_order_api.py에 있음)
try:
    from order.kis_order_api import get_balance  # noqa: F401
    _GET_BALANCE_AVAILABLE = True
except ImportError:
    _GET_BALANCE_AVAILABLE = False
    def get_balance(runtime):  # type: ignore[misc]
        raise ImportError("order 레이어 미설치 — get_balance 사용 불가")
# 실주문 실행 엔진 (엔진 계층 유일 진입점)
try:
    from order.order_executor import execute_entry, execute_exit, ExecuteResult
    _ORDER_EXECUTOR_AVAILABLE = True
except ImportError:
    execute_entry = execute_exit = ExecuteResult = None  # type: ignore[assignment]
    _ORDER_EXECUTOR_AVAILABLE = False
# TR 코드 상수
from kis_tr_catalog import WS_TR_BY_SESSION

ERROR_REPEAT_WARN_COUNT  = 3    # 반복 경고 시작
ERROR_CIRCUIT_OPEN_COUNT = 3    # 동일 에러 N회 연속 → 서킷 오픈 (pause, 60s half-open)
ERROR_HARD_STOP_COUNT    = 5    # 동일 에러 N회 연속 → 하드스톱 (수동 resume 필요)
CIRCUIT_HALF_OPEN_SEC    = 60   # 서킷 오픈 후 N초 뒤 자동 반개방(half-open) 재시도
INVESTOR_FOREIGN_BUY_CANDIDATES = ["stot_frgn_ntby_qty", "frgn_ntby_qty", "frgn_ntby_tr_pbmn"]
INVESTOR_OI_CANDIDATES = ["opint_qty", "hts_otst_stpl_qty", "unsettled_qty", "open_interest"]
INVESTOR_STRENGTH_CANDIDATES = ["cntg_isrt", "cttr", "trade_strength"]
TREND_PARAM_KEYS = {
    "trend_weight_long",
    "trend_weight_short",
    "trend_ema_align_score",
    "trend_ema_slope_score",
    "trend_ema_slope_threshold",
}


class ErrorTracker:
    def __init__(self, runtime: AppRuntime) -> None:
        self.runtime = runtime
        # ── 카테고리별 독립 상태 ──────────────────────────────────────────
        # 스레드마다 자기 카테고리만 초기화하므로 다른 카테고리 누적에 영향 없음
        self._cat_count:    Dict[str, int]  = {}   # 연속 에러 횟수
        self._cat_warned:   Dict[str, bool] = {}   # 반복 경고 발송 여부
        self._cat_last_msg: Dict[str, str]  = {}   # 로깅용 마지막 원본 메시지
        # ── 전역 서킷 브레이커 / 하드스톱 (모든 카테고리 최대 count 기준) ──
        self._circuit_open      = False
        self._circuit_open_time = 0.0
        self._hard_stopped      = False
        self._lock = threading.Lock()

    # ------------------------------------------------------------------
    # 에러 카테고리 정규화
    # ------------------------------------------------------------------
    @staticmethod
    def _categorize(error: str) -> str:
        """
        에러 문자열 → 정규화된 카테고리 키.
        raw 메시지가 조금씩 달라도 같은 종류의 에러를 연속으로 카운트하기 위함.
        """
        e = error.lower()
        if "timeout" in e or "timed out" in e:
            return "timeout"
        if "429" in e or "rate limit" in e or "rate_limit" in e:
            return "rate_limit"
        if any(c in e for c in ("503", "502", "500", "http 5")):
            return "http_5xx"
        if "ws " in e or "websocket" in e or "ws_" in e or "ws오류" in e:
            return "ws_error"
        if "investor" in e:
            return "investor_error"
        if "시장 데이터" in e or "data_gate" in e or "stale" in e:
            return "data_gate"
        if "engine_error" in e:
            return "engine_error"
        if "connection" in e or "connect" in e:
            return "connection"
        return "general"

    def _max_count(self) -> int:
        """모든 카테고리 중 최대 연속 에러 횟수 (lock 내부에서만 호출)."""
        return max(self._cat_count.values(), default=0)

    # ------------------------------------------------------------------
    # 상태 조회
    # ------------------------------------------------------------------
    def is_circuit_open(self) -> bool:
        """
        True → 엔진 틱 스킵 (5s 대기).
        half-open: CIRCUIT_HALF_OPEN_SEC 경과 후 틱 1회 통과 허용.
        하드스톱 중이면 항상 False (is_hard_stopped 가 먼저 처리).
        """
        with self._lock:
            if self._hard_stopped or not self._circuit_open:
                return False
            if time.time() - self._circuit_open_time >= CIRCUIT_HALF_OPEN_SEC:
                return False  # half-open
            return True

    def is_hard_stopped(self) -> bool:
        """True → 수동 resume 전까지 모든 틱 차단."""
        with self._lock:
            return self._hard_stopped

    def reset_hard_stop(self) -> None:
        """대시보드 /api/engine/resume 에서 호출."""
        log = self.runtime.log
        state = self.runtime.state
        with self._lock:
            if not self._hard_stopped:
                return
            self._hard_stopped = False
            self._circuit_open = False
            self._cat_count.clear()
            self._cat_warned.clear()
            self._cat_last_msg.clear()
            state["engine_halted"] = False
            state["circuit_open"]  = False
            state["last_error"]    = ""
            log.warning("하드스톱 해제 (수동 resume)")

    # ------------------------------------------------------------------
    # 에러 기록
    # ------------------------------------------------------------------
    def record(self, error: str, category: str = "") -> None:
        """
        error    : 에러 메시지. 빈 문자열이면 해당 카테고리 복구 처리.
        category : 카테고리 키. 빈 문자열이면 error 문자열에서 자동 감지.
                   error="" 시 지정 카테고리만 리셋 → 다른 카테고리 누적 유지.
        """
        log   = self.runtime.log
        state = self.runtime.state
        cat   = category or (self._categorize(error) if error else "")

        with self._lock:
            if error:
                # ── 에러 누적 ────────────────────────────────────────────
                state["last_error"] = error
                prev = self._cat_count.get(cat, 0)
                self._cat_count[cat]    = prev + 1
                self._cat_last_msg[cat] = error
                count = self._cat_count[cat]

                if prev == 0:
                    log.error("신규 에러 [%s]: %s", cat, error)

                # ① 반복 경고 (3회)
                if count >= ERROR_REPEAT_WARN_COUNT and not self._cat_warned.get(cat):
                    self._cat_warned[cat] = True
                    log.warning("에러 %d회 반복 [%s]: %s", count, cat, error)

                # ② 서킷 오픈 (3회 — pause, 60s half-open 자동복구)
                if count >= ERROR_CIRCUIT_OPEN_COUNT and not self._circuit_open and not self._hard_stopped:
                    self._circuit_open      = True
                    self._circuit_open_time = time.time()
                    state["circuit_open"]   = True
                    log.critical(
                        "서킷 브레이커 오픈 | [%s] %d회 연속 | %ds 후 half-open",
                        cat, count, CIRCUIT_HALF_OPEN_SEC,
                    )

                # ③ 하드스톱 (5회 — 수동 resume 필요)
                if count >= ERROR_HARD_STOP_COUNT and not self._hard_stopped:
                    self._hard_stopped    = True
                    state["engine_halted"] = True
                    log.critical(
                        "하드스톱 발동 | [%s] %d회 연속 | /api/engine/resume 으로 수동 해제 필요",
                        cat, count,
                    )

            else:
                # ── 복구: 지정 카테고리만 리셋 ──────────────────────────
                if cat and self._cat_count.get(cat, 0) > 0:
                    log.info(
                        "에러 해소 [%s] (%d회 → 0 | %s)",
                        cat, self._cat_count[cat], self._cat_last_msg.get(cat, ""),
                    )
                    self._cat_count[cat]    = 0
                    self._cat_warned[cat]   = False
                    self._cat_last_msg[cat] = ""

                # 서킷 재평가: 모든 카테고리가 임계값 미만이면 닫기
                if self._circuit_open and self._max_count() < ERROR_CIRCUIT_OPEN_COUNT:
                    self._circuit_open    = False
                    state["circuit_open"] = False
                    log.info("서킷 브레이커 해제 ([%s] 복구 후 전체 최대=%d)", cat, self._max_count())

                # state["last_error"] = 현재 가장 많이 누적된 카테고리의 메시지
                worst = max(self._cat_count, key=self._cat_count.get, default="") if self._cat_count else ""
                state["last_error"] = self._cat_last_msg.get(worst, "")


class PriceGuard:
    """WebSocket 수신 가격 신뢰도 방어층 — 재연결 직후 쓰레기 틱 차단.

    Layer 1 — 절대 범위 : price ≤ 0 또는 범위 외 즉시 거부
    Layer 2 — 틱간 스파이크: _last 기준 5% 초과 변동 거부
                ※ _last=None(최초 기동)이면 L2 생략 → L1만 적용
    Layer 3 — 연속 거부 REJECT_LIMIT 틱 이상 → is_blocked=True → data_gate 차단

    reset 전략
    ──────────
    soft_reset()  WS 재연결 시 사용.
                  _streak만 초기화, _last 보존.
                  → 재연결 직후 첫 쓰레기 틱을 직전 유효가 기준 L2로 차단.
                  예) 재연결 전 _last=812, 첫 틱=350
                      → |350-812|/812=57% > 5% → 차단 ✓

    seed(price)   프로세스 기동 직후 최초로 신뢰 가능한 가격을 주입.
                  복원 포지션 entry_price 등을 이용해 L2 앵커를 세팅.
                  → 기동 직후 쓰레기 첫 틱도 L2에서 차단 가능.

    hard_reset()  _last + _streak 모두 초기화.
                  장기 블로킹 후 재앵커가 필요할 때 내부에서 자동 호출.
    """

    MIN_PRICE           = 200.0    # KOSPI 200 선물 절대 하한 (역사적 최저 근방)
    MAX_PRICE           = 1500.0   # 절대 상한
    MAX_TICK_CHANGE_PCT = 0.05     # 단일 틱 최대 허용 변동률 5% (서킷브레이커 ≤8%)
    REJECT_LIMIT        = 3        # 연속 거부 N틱 → is_blocked
    REANCHOR_LIMIT      = 30       # 연속 거부 N틱 이상 → 강제 재앵커 (장기 시장 이동 허용)

    def __init__(self) -> None:
        self._last: "float | None" = None
        self._streak: int = 0

    # ------------------------------------------------------------------
    # 공개 API
    # ------------------------------------------------------------------
    def validate(self, price: float) -> "tuple[bool, str]":
        """(is_valid, reject_reason) 반환.
        거부 시 reject_reason에 원인 문자열, is_valid=False.

        특수 사유 "waiting_first_tick":
            _last=None(최초 기동 또는 시드 미설정) 이고 price가 유효 범위 밖일 때.
            아직 첫 체결가를 받지 못한 정상적인 WS 초기화 상태이므로
            streak 을 누적하지 않는다. (재연결 후 0.00 유입 등)
        """
        # 장기 블로킹 → 강제 재앵커 (실제 시장 이동 허용, 쓰레기 스트림 아님)
        if self._streak >= self.REANCHOR_LIMIT:
            self.hard_reset()

        # L1: 절대 범위
        if price <= 0 or not (self.MIN_PRICE <= price <= self.MAX_PRICE):
            # 아직 앵커가 없으면 streak 미누적 — 첫 유효 틱 대기 중인 정상 상태
            if self._last is None:
                return False, "waiting_first_tick"
            self._streak += 1
            return False, f"out_of_range({price:.2f})"

        # L2: 틱간 스파이크 (_last 있을 때만)
        if self._last is not None:
            pct = abs(price - self._last) / self._last
            if pct > self.MAX_TICK_CHANGE_PCT:
                self._streak += 1
                return False, f"spike({pct:.1%}|{self._last:.2f}→{price:.2f})"

        # 유효 가격
        self._last = price
        self._streak = 0
        return True, ""

    def soft_reset(self) -> None:
        """WS 재연결 시 호출 — _streak만 초기화, _last(직전 유효가) 보존.
        보존된 _last로 재연결 직후 첫 쓰레기 틱을 L2에서 차단한다.
        """
        self._streak = 0

    def seed(self, price: float) -> None:
        """프로세스 기동 시 신뢰 가능한 초기 가격 주입 (L2 앵커 세팅).
        복원 포지션 entry_price 등을 이용해 기동 직후 쓰레기 틱도 L2에서 차단.
        price가 L1 범위 밖이면 무시.
        """
        if self.MIN_PRICE <= price <= self.MAX_PRICE:
            self._last = price

    def hard_reset(self) -> None:
        """_last + _streak 모두 초기화. 장기 블로킹 후 재앵커 시 내부 자동 호출."""
        self._last = None
        self._streak = 0

    @property
    def is_blocked(self) -> bool:
        """연속 REJECT_LIMIT 틱 이상 거부 → data_gate 차단 권고"""
        return self._streak >= self.REJECT_LIMIT


class KISRealtimeClient:
    def __init__(self, runtime: AppRuntime, tr_key: str):
        self.runtime = runtime
        self.tr_key = tr_key
        self._lock = threading.Lock()
        self.latest = {
            "futures_price": 0.0,
            "spot_price": 0.0,
            "basis": 0.0,
            "best_ask": 0.0,
            "best_bid": 0.0,
            "updated_at": 0.0,
            "cum_volume": 0.0,
            "trade_qty": 0.0,
            "trade_strength": 0.0,
            "oi_value": 0.0,
        }

    def get_snapshot(self) -> Dict[str, float]:
        with self._lock:
            return dict(self.latest)

    async def _run(self) -> None:
        runtime = self.runtime
        state = runtime.state
        st = runtime.settings
        while True:
            try:
                runtime.log.info("WS 구독 시작 | tr_key=%s", self.tr_key)
                approval_key = issue_approval_key(runtime)
                _first_tick_logged = False  # 연결마다 리셋 — 첫 체결 틱 1회 진단용
                async with websockets.connect(st["WS_URL"], ping_interval=20) as ws:
                    state["ws_connected"] = True
                    runtime.error_tracker.record("", "ws_error")
                    _session = st["MMEAN_SESSION"]
                    _ws_tr   = WS_TR_BY_SESSION.get(_session, WS_TR_BY_SESSION["day"])
                    await ws.send(json.dumps({
                        "header": {
                            "approval_key": approval_key,
                            "custtype": "P",
                            "tr_type": "1",
                            "content-type": "utf-8",
                        },
                        "body": {"input": {"tr_id": _ws_tr["tick"], "tr_key": self.tr_key}},
                    }))
                    await ws.send(json.dumps({
                        "header": {
                            "approval_key": approval_key,
                            "custtype": "P",
                            "tr_type": "1",
                            "content-type": "utf-8",
                        },
                        "body": {"input": {"tr_id": _ws_tr["ask"], "tr_key": self.tr_key}},
                    }))
                    async for msg in ws:
                        if msg.startswith("0|") or msg.startswith("1|"):
                            parts = msg.split("|")
                            tid, row = parts[1], parts[3].split("^")
                            with self._lock:
                                if tid == _ws_tr["tick"]:
                                    f, b = to_float(row[5]), to_float(row[13])
                                    # ── 첫 체결 틱 1회 진단 로그 (파싱·종목코드 검증용) ──
                                    if not _first_tick_logged:
                                        _first_tick_logged = True
                                        runtime.log.info(
                                            "WS 첫 체결 틱 | tid=%s | price=%.2f"
                                            " | basis=%.4f | row_len=%d",
                                            tid, f, b, len(row),
                                        )
                                    self.latest.update({
                                        "futures_price": f,
                                        "basis": b,
                                        "spot_price": round(f - b, 4),
                                        "trade_qty": to_float(row[9]),
                                        "cum_volume": to_float(row[10]),
                                        "trade_strength": to_float(row[30]),
                                        "oi_value": to_float(row[18]),
                                        "best_ask": to_float(row[34]),
                                        "best_bid": to_float(row[35]),
                                        "updated_at": time.time(),
                                    })
                                else:
                                    self.latest.update({
                                        "best_ask": to_float(row[2]) or self.latest["best_ask"],
                                        "best_bid": to_float(row[7]) or self.latest["best_bid"],
                                        "updated_at": time.time(),
                                    })
            except Exception as e:
                state["ws_connected"] = False
                runtime.error_tracker.record(f"WS 오류: {e}")
                # WS 재연결마다 포지션 재동기화 (REST 1회 — 항목 4 검증)
                _sync_position_from_rest(runtime)
                await asyncio.sleep(st["WS_RECONNECT_SEC"])


def to_float(v: Any, d: float = 0.0) -> float:
    try:
        return float(str(v).replace(",", "")) if v else d
    except Exception:
        return d


def is_live_ready(runtime: AppRuntime) -> bool:
    return bool(runtime.settings["APP_KEY"] and runtime.settings["APP_SECRET"])


def _sync_position_from_rest(runtime: AppRuntime) -> None:
    """
    get_balance() → sync_from_balance() 원스텝 위임.

    호출 시점:
      1. start_runtime() — 프로세스 부팅 1회
      2. KISRealtimeClient._run() 예외 핸들러 — WS 재연결마다

    ORDER_CANO 미설정·order_state None·ORDER_KEY 미설정 시 조용히 스킵.
    """
    if runtime.order_state is None:
        return
    if not runtime.settings.get("ORDER_CANO") or not runtime.settings.get("ORDER_KEY"):
        return
    try:
        bal    = get_balance(runtime)
        symbol = runtime.settings.get("FUTURES_CODE", "")
        runtime.order_state.sync_from_balance(bal["positions"], symbol)
        runtime.log.info(
            "포지션 동기화 완료 | symbol=%s rows=%d",
            symbol, len(bal["positions"]),
        )
    except Exception as e:
        runtime.log.warning("포지션 동기화 실패 (계속 진행): %s", e)


# ── get_valid_token: 하위 호환 alias (내부 코드에서 기존 이름으로 호출하는 곳 대비) ──
def get_valid_token(runtime: AppRuntime) -> str:
    """kis_data_api.get_data_token() 의 alias — 기존 호출부 호환용."""
    return get_data_token(runtime)


class LiveOrderExecutor:
    """
    engine_loop 전용 실주문 실행 관리자.

    설계 원칙:
      - EXECUTION_ENABLED=false(기본) → 로그만, 주문 미전송 (dry-run)
      - EXECUTION_ENABLED=true        → execute_entry / execute_exit 실행
      - 동시 주문 방지: threading.Lock (_active 플래그)
      - 30s 블로킹 격리: 별도 daemon 스레드에서 실행
      - 모든 결과는 state["live_last_*"] 에 기록 (대시보드 2단계용)

    사용:
      _exec = LiveOrderExecutor(runtime)
      _exec.try_entry("LONG", qty=1)   # 비동기, 즉시 반환
      _exec.try_exit()
    """

    _TICK_SIZE = 0.05   # KOSPI200 선물 최소 틱 (0.05pt)

    def __init__(self, runtime: AppRuntime) -> None:
        self.runtime           = runtime
        self._lock             = threading.Lock()
        self._active           = False   # True = 현재 주문 스레드 실행 중
        self._trail_peak_ticks = 0.0     # Trailing SL 고점 (틱 단위)

    @property
    def is_active(self) -> bool:
        with self._lock:
            return self._active

    # ── 진입 ──────────────────────────────────────────────────────────────

    def try_entry(self, side: str, qty: int, price: float = 0.0) -> bool:
        """
        진입 주문 시도. 이미 주문 진행 중이면 즉시 False 반환.
        EXECUTION_ENABLED=false 이면 dry-run 로그만 출력.
        """
        with self._lock:
            if self._active:
                return False
            self._active = True

        env       = self.runtime.settings.get("ORDER_ENV", "virtual")
        exec_mode = str(self.runtime.state.get("execution_mode", "OFF")).upper()

        # VIRTUAL = 내부 시뮬레이션 전용 — KIS API 호출 없음
        if exec_mode == "VIRTUAL":
            self.runtime.log.debug(
                "[EXEC-VIRTUAL] entry | side=%s qty=%d → 내부 시뮬레이션 모드, KIS 주문 미전송",
                side, qty,
            )
            with self._lock:
                self._active = False
            return False

        # LIVE 모드는 EXECUTION_ENABLED=true 이중 게이트 (실계좌 안전장치)
        if exec_mode == "LIVE" and not bool(self.runtime.settings.get("EXECUTION_ENABLED", False)):
            self.runtime.log.warning(
                "[EXEC-DRY] entry | side=%s qty=%d price=%.2f env=%s"
                " → LIVE 모드이지만 EXECUTION_ENABLED=false (주문 미전송)",
                side, qty, price, env,
            )
            with self._lock:
                self._active = False
            return False

        threading.Thread(
            target=self._run_entry,
            args=(side, qty, price),
            daemon=True,
            name=f"LiveOrder-{side}",
        ).start()
        return True

    # ── 청산 ──────────────────────────────────────────────────────────────

    def try_exit(self, qty: int = 0, price: float = 0.0) -> bool:
        """
        청산 주문 시도. 이미 주문 진행 중이면 즉시 False 반환.
        qty=0 → 현재 포지션 전량 청산 (execute_exit 위임).
        """
        with self._lock:
            if self._active:
                return False
            self._active = True

        env       = self.runtime.settings.get("ORDER_ENV", "virtual")
        exec_mode = str(self.runtime.state.get("execution_mode", "OFF")).upper()

        # VIRTUAL = 내부 시뮬레이션 전용 — KIS API 호출 없음
        if exec_mode == "VIRTUAL":
            self.runtime.log.debug(
                "[EXEC-VIRTUAL] exit | qty=%d → 내부 시뮬레이션 모드, KIS 주문 미전송", qty,
            )
            with self._lock:
                self._active = False
            return False

        # LIVE 모드는 EXECUTION_ENABLED=true 이중 게이트 (실계좌 안전장치)
        if exec_mode == "LIVE" and not bool(self.runtime.settings.get("EXECUTION_ENABLED", False)):
            self.runtime.log.warning(
                "[EXEC-DRY] exit | qty=%d price=%.2f env=%s"
                " → LIVE 모드이지만 EXECUTION_ENABLED=false (주문 미전송)",
                qty, price, env,
            )
            with self._lock:
                self._active = False
            return False

        threading.Thread(
            target=self._run_exit,
            args=(qty, price),
            daemon=True,
            name="LiveOrder-EXIT",
        ).start()
        return True

    # ── 내부 실행 (daemon 스레드) ──────────────────────────────────────────

    def _run_entry(self, side: str, qty: int, price: float) -> None:
        try:
            if not _ORDER_EXECUTOR_AVAILABLE:
                self.runtime.log.error("[EXEC] order_executor import 실패 — 진입 불가")
                return
            result = execute_entry(self.runtime, side=side, qty=qty, price=price)
            self._update_state(result)
            if result.ok:
                self.runtime.log.info(
                    "[EXEC] 진입 체결 ✓ | side=%s qty=%d fill=%.2f ord=%s env=%s",
                    side, result.fill_qty, result.fill_price, result.ord_no,
                    self.runtime.settings.get("ORDER_ENV"),
                )
            elif result.cancelled:
                self.runtime.log.warning(
                    "[EXEC] 진입 취소 | ord=%s side=%s", result.ord_no, side,
                )
            elif result.refused:
                self.runtime.log.warning(
                    "[EXEC] 진입 거부 | side=%s | %s", side, result.error,
                )
            else:
                self.runtime.log.error(
                    "[EXEC] 진입 실패 | side=%s | %s", side, result.error,
                )
        except Exception as e:
            self.runtime.log.error("[EXEC] 진입 예외 | side=%s | %s", side, e)
        finally:
            self.reset_trail()   # 신규 진입 완료 → trail 고점 초기화
            with self._lock:
                self._active = False

    def _run_exit(self, qty: int, price: float) -> None:
        try:
            if not _ORDER_EXECUTOR_AVAILABLE:
                self.runtime.log.error("[EXEC] order_executor import 실패 — 청산 불가")
                return
            result = execute_exit(self.runtime, qty=qty, price=price)
            self._update_state(result)
            if result.ok:
                self.runtime.log.info(
                    "[EXEC] 청산 체결 ✓ | qty=%d fill=%.2f ord=%s env=%s",
                    result.fill_qty, result.fill_price, result.ord_no,
                    self.runtime.settings.get("ORDER_ENV"),
                )
            elif result.cancelled:
                self.runtime.log.warning(
                    "[EXEC] 청산 취소 | ord=%s", result.ord_no,
                )
            elif result.refused:
                self.runtime.log.warning(
                    "[EXEC] 청산 거부 | %s", result.error,
                )
            else:
                self.runtime.log.error(
                    "[EXEC] 청산 실패 | %s", result.error,
                )
        except Exception as e:
            self.runtime.log.error("[EXEC] 청산 예외 | %s", e)
        finally:
            self.reset_trail()   # 청산 완료(성공/실패 무관) → trail 고점 초기화
            with self._lock:
                self._active = False

    def reset_trail(self) -> None:
        """Trailing 고점 초기화 — 진입/청산 완료 시 호출."""
        with self._lock:
            self._trail_peak_ticks = 0.0

    # ── TP/SL 모니터링 (engine_loop 매 틱 호출) ───────────────────────────

    def check_tp_sl(self, current_price: float) -> None:
        """
        실계좌 포지션 TP/SL/Trailing 체크. engine_loop에서 매 틱 호출.

        설정 (0 = 비활성):
          LIVE_TP_TICKS          — 목표 수익 틱
          LIVE_SL_TICKS          — 손절 틱
          LIVE_TRAILING_TICKS    — 고점 대비 되돌림 틱 (trailing stop)
          LIVE_TRAILING_ACTIVATE — trailing 활성화 최소 수익 틱

        발동 우선순위: SL → Trailing SL → TP
        """
        if self.is_active:
            return
        if runtime := self.runtime:
            if str(runtime.state.get("execution_mode", "OFF")).upper() not in ("PAPER", "LIVE"):
                return
            if runtime.order_state is None:
                return

            pos = runtime.order_state.get_position()
            if pos.is_flat() or pos.entry_price == 0.0:
                self.reset_trail()
                return

            st          = runtime.settings
            tp_ticks    = int(st.get("LIVE_TP_TICKS",          16))
            sl_ticks    = int(st.get("LIVE_SL_TICKS",          10))
            trail_ticks = int(st.get("LIVE_TRAILING_TICKS",     6))
            trail_act   = int(st.get("LIVE_TRAILING_ACTIVATE",  8))

            # 방향 기준 수익 틱 (양수=이익, 음수=손실)
            price_diff = (current_price - pos.entry_price) * pos.direction
            pnl_ticks  = price_diff / self._TICK_SIZE

            # ── SL ──────────────────────────────────────────────────────
            if sl_ticks > 0 and pnl_ticks <= -sl_ticks:
                runtime.log.warning(
                    "[TP/SL] SL 발동 | pnl=%.1f틱 ≤ -%.0f | price=%.2f entry=%.2f dir=%+d",
                    pnl_ticks, sl_ticks, current_price, pos.entry_price, pos.direction,
                )
                self.try_exit()
                return

            # ── Trailing SL ─────────────────────────────────────────────
            if trail_act > 0 and pnl_ticks >= trail_act:
                with self._lock:
                    if pnl_ticks > self._trail_peak_ticks:
                        self._trail_peak_ticks = pnl_ticks
                    drawdown = self._trail_peak_ticks - pnl_ticks
                if trail_ticks > 0 and drawdown >= trail_ticks:
                    runtime.log.info(
                        "[TP/SL] Trailing SL 발동 | peak=%.1f틱 drawdown=%.1f ≥ %.0f",
                        self._trail_peak_ticks, drawdown, trail_ticks,
                    )
                    self.try_exit()
                    return

            # ── TP ───────────────────────────────────────────────────────
            if tp_ticks > 0 and pnl_ticks >= tp_ticks:
                runtime.log.info(
                    "[TP/SL] TP 발동 | pnl=%.1f틱 ≥ %.0f | price=%.2f entry=%.2f dir=%+d",
                    pnl_ticks, tp_ticks, current_price, pos.entry_price, pos.direction,
                )
                self.try_exit()

    def _update_state(self, result: "ExecuteResult") -> None:
        """ExecuteResult → state 반영 (2단계 대시보드 표시용 준비)."""
        if result is None:
            return
        try:
            self.runtime.state.update({
                "live_last_ord_no":     result.ord_no,
                "live_last_side":       result.side,
                "live_last_ok":         result.ok,
                "live_last_fill_price": result.fill_price,
                "live_last_fill_qty":   result.fill_qty,
                "live_last_cancelled":  result.cancelled,
                "live_last_refused":    result.refused,
                "live_last_error":      result.error,
            })
        except Exception:
            pass


def _first_nonzero(output: dict, candidates: List[str]) -> float:
    for key in candidates:
        val = to_float(output.get(key))
        if val != 0.0:
            return val
    return 0.0


def _pick_foreign_buy_qty(output: dict) -> float:
    for key in INVESTOR_FOREIGN_BUY_CANDIDATES:
        if key in output:
            return to_float(output[key])
    return 0.0


def fetch_investor_data(runtime: AppRuntime) -> None:
    """투자자 매매 폴링 루프 — kis_data_api.fetch_investor_once() 로 HTTP 위임.
    state 업데이트·에러 추적·슬립은 엔진 책임.
    """
    st = runtime.settings
    if not st["INVESTOR_ENABLED"] or not is_live_ready(runtime):
        return

    runtime.log.info(
        "investor 폴링 시작 | market=%s sub=%s",
        st["INVESTOR_MARKET_CODE"],
        st["INVESTOR_SUB_CODE"],
    )
    while True:
        try:
            out = fetch_investor_once(runtime)   # kis_data_api — HTTP I/O 전담
            runtime.state_obj.investor_cache.update({
                "foreign_buy":    _pick_foreign_buy_qty(out),
                "oi_value":       _first_nonzero(out, INVESTOR_OI_CANDIDATES),
                "trade_strength": _first_nonzero(out, INVESTOR_STRENGTH_CANDIDATES),
                "updated_at":     time.time(),
            })
            # FlowEngine 시계열 누적 — 폴링마다 외국인 순매수 추가
            runtime.state_obj.foreign_net_history.append(
                runtime.state_obj.investor_cache["foreign_buy"]
            )
            runtime.error_tracker.record("", "investor_error")
        except Exception as e:
            runtime.error_tracker.record(f"investor 오류: {e}")
        time.sleep(st["INVESTOR_POLL_SEC"])


def update_ema(runtime: AppRuntime, prev: float, value: float) -> float:
    alpha = runtime.settings["EMA_ALPHA"]
    return (alpha * value) + ((1 - alpha) * prev) if prev else value


def compute_slope(points: List[Tuple[float, float]]) -> float:
    return (points[-1][1] - points[0][1]) / (points[-1][0] - points[0][0]) if len(points) > 1 else 0.0


def compute_volume_burst(runtime: AppRuntime, value: float) -> float:
    runtime.state_obj.volume_delta_history.append(value)
    avg = sum(runtime.state_obj.volume_delta_history) / len(runtime.state_obj.volume_delta_history)
    return value / avg if avg > 0 else 0.0


def compute_foreign_signal(runtime: AppRuntime, cum: float) -> "Tuple[float, float]":
    """composite(누적+틱 합성) 와 delta_only(틱 변화량) 두 값을 함께 반환."""
    st = runtime.settings
    delta = cum - runtime.state_obj.foreign_prev_cum
    runtime.state_obj.foreign_prev_cum = cum
    scaled_cum = cum / st["FOREIGN_SCALE_DIV"]
    composite  = round(scaled_cum * st["FOREIGN_CUM_WEIGHT"] + delta * st["FOREIGN_DELTA_WEIGHT"], 2)
    delta_only = round(delta, 2)
    return composite, delta_only


def _ema_alpha(period: int) -> float:
    return 2.0 / (period + 1)


def update_trend_emas(runtime: AppRuntime, price: float, now_ts: float) -> Tuple[float, float, float]:
    st = runtime.settings
    state_obj = runtime.state_obj
    a_fast = _ema_alpha(st["TREND_EMA_FAST"])
    a_slow = _ema_alpha(st["TREND_EMA_SLOW"])
    if state_obj.ema_fast_val == 0.0:
        state_obj.ema_fast_val = price
        state_obj.ema_slow_val = price
    else:
        state_obj.ema_fast_val = a_fast * price + (1 - a_fast) * state_obj.ema_fast_val
        state_obj.ema_slow_val = a_slow * price + (1 - a_slow) * state_obj.ema_slow_val
    state_obj.ema_fast_history.append((now_ts, state_obj.ema_fast_val))
    if len(state_obj.ema_fast_history) >= st["TREND_SLOPE_WIN"]:
        slope = state_obj.ema_fast_val - state_obj.ema_fast_history[-st["TREND_SLOPE_WIN"]][1]
    else:
        slope = 0.0
    return round(state_obj.ema_fast_val, 4), round(state_obj.ema_slow_val, 4), round(slope, 5)


def update_vwap(runtime: AppRuntime, price: float, volume: float) -> float:
    state_obj = runtime.state_obj
    state_obj.vwap_cum_pv += price * volume
    state_obj.vwap_cum_vol += volume
    if state_obj.vwap_cum_vol <= 0:
        return price
    return round(state_obj.vwap_cum_pv / state_obj.vwap_cum_vol, 4)


def update_atr(runtime: AppRuntime, high: float, low: float, close: float) -> float:
    state_obj = runtime.state_obj
    if state_obj.prev_close == 0.0:
        state_obj.prev_close = close
        return 0.0
    tr = max(high - low, abs(high - state_obj.prev_close), abs(low - state_obj.prev_close))
    state_obj.atr_history.append(tr)
    state_obj.prev_close = close
    if len(state_obj.atr_history) < 2:
        return round(tr, 4)
    return round(sum(state_obj.atr_history) / len(state_obj.atr_history), 4)


def reset_intraday_indicators(runtime: AppRuntime) -> None:
    runtime.state_obj.vwap_cum_pv = 0.0
    runtime.state_obj.vwap_cum_vol = 0.0
    runtime.log.info("당일 지표 초기화 (VWAP 리셋)")


def sync_trend_to_param_store(runtime: AppRuntime, cfg: Dict[str, Any], source: str = "manual") -> None:
    trend_patch = {k: cfg[k] for k in TREND_PARAM_KEYS if k in cfg}
    if not trend_patch:
        return
    try:
        runtime.param_store.set_baseline_only(params=trend_patch, source=source, allow_override=True)
        runtime.param_store.reset_active_to_baseline(source=source, reason_code="baseline_override")
        runtime.log.info(
            "ParamStore 수동 동기화 완료 (baseline+active 리셋) | source=%s | patch=%s",
            source,
            trend_patch,
        )
    except Exception as e:
        runtime.log.warning("ParamStore 동기화 실패 (무시): %s", e)


def simulated_data_tick(runtime: AppRuntime, t: float) -> Dict[str, float]:
    import math
    f = 350.0 + 0.3 * math.sin(t / 4.0)
    b = 0.20 + 0.05 * math.sin(t / 6.0)
    s = f - b
    v = 10.0
    return {
        "futures_price": round(f, 4),
        "spot_price": round(s, 4),
        "basis": round(b, 4),
        "foreign_buy": 0.0,
        "oi_value": 1000.0,
        "oi_delta": 0.0,
        "trade_qty": 1.0,
        "cum_volume": 1000.0 + (t % 300),
        "volume_delta": v,
        "volume_burst": compute_volume_burst(runtime, v),
        "last_trade_strength": 100.0,
        "trade_strength": 100.0,
        "best_ask": round(f + 0.1, 4),
        "best_bid": round(f - 0.1, 4),
        "updated_at": time.time(),
    }


def refresh_today_stats_loop(runtime: AppRuntime) -> None:
    import sqlite3
    while True:
        try:
            today = datetime.now().strftime("%Y-%m-%d")
            conn = sqlite3.connect(runtime.db_path, check_same_thread=False)
            conn.row_factory = sqlite3.Row
            row = conn.execute(
                "SELECT COUNT(*) AS cnt, SUM(COALESCE(pnl_ticks,0)*25000) AS pnl FROM trades WHERE substr(open_ts,1,10)=?",
                (today,),
            ).fetchone()
            cnt = int(row["cnt"] or 0)
            pnl = float(row["pnl"] or 0.0)
            daily_limit = float(runtime.settings["LLM_DAILY_LOSS_LIMIT"])
            left = daily_limit - min(0.0, pnl)
            ticks = conn.execute(
                "SELECT ts, futures_price, basis, long_score, short_score, volume_burst, bias FROM regime_ticks ORDER BY ts DESC LIMIT 10"
            ).fetchall()
            conn.close()
            recent = [dict(t) for t in reversed(list(ticks))]
            with runtime.state_obj.today_stats_lock:
                runtime.state_obj.today_stats["today_trade_count"] = cnt
                runtime.state_obj.today_stats["today_pnl_won"] = pnl
                runtime.state_obj.today_stats["daily_loss_limit_left"] = left
                runtime.state_obj.today_stats["recent_ticks"] = recent
        except Exception as e:
            runtime.log.warning("refresh_today_stats_loop 오류: %s", e)
        time.sleep(60)



# ──────────────────────────────────────────────────────────────────────────
# 장전 수동 옵션 보정 헬퍼
# ──────────────────────────────────────────────────────────────────────────
# 허용값 → (long_bias_adj, short_bias_adj, size_adj)
# 방향 강제 금지. weight/size 미세 보정만.
_PREMARKET_ADJ_MAP: Dict[str, Tuple[float, float, float]] = {
    "MANIA_2": ( 0.10, -0.10, 1.05),   # 위험선호: long 완화, short 억제
    "MANIA_1": ( 0.05, -0.05, 1.02),
    "NORMAL":  ( 0.00,  0.00, 1.00),   # 기본 — 보정 없음
    "FEAR_1":  (-0.05,  0.05, 0.95),   # 위험회피: long 억제, short 일부 완화
    "FEAR_2":  (-0.10,  0.10, 0.90),
}


def _get_premarket_adj(mode: str) -> Tuple[float, float, float]:
    """premarket_manual_mode → (long_adj, short_adj, size_adj).
    알 수 없는 값은 NORMAL(0, 0, 1)로 처리."""
    return _PREMARKET_ADJ_MAP.get(str(mode).upper(), (0.0, 0.0, 1.0))


# ──────────────────────────────────────────────────────────────────────────
# 장중 LLM 영향도 스케일링 헬퍼
# ──────────────────────────────────────────────────────────────────────────
_LLM_INFLUENCE_SCALE: Dict[str, float] = {
    "LOW":  0.5,   # weight_adjust 를 절반만 반영
    "MID":  1.0,   # 그대로 반영 (기본)
    "HIGH": 1.5,   # 1.5배 증폭 (clamp 적용)
}
# LLM 권한 비대칭 클램프 — "위험 조정기" 원칙
# SIZE_DOWN(-): 손실 회피 방향이므로 -0.20까지 허용
# SIZE_UP(+):   진입 확대는 더 보수적으로 +0.10까지만 허용
_WA_MAX_DOWN = 0.20   # SIZE_DOWN 최대 절대값
_WA_MAX_UP   = 0.10   # SIZE_UP 최대값 (SIZE_DOWN 의 절반)


def _scale_wa_by_influence(wa: float, influence: str) -> float:
    """EMA 필터 출력값을 영향도 레벨로 스케일링.
    양(+)과 음(-) 방향의 상한선이 다름 — LLM SIZE_UP 권한 제한."""
    scale  = _LLM_INFLUENCE_SCALE.get(str(influence).upper(), 1.0)
    scaled = wa * scale
    if scaled >= 0:
        return min(_WA_MAX_UP, scaled)
    else:
        return max(-_WA_MAX_DOWN, scaled)


# ──────────────────────────────────────────────────────────────────────────
# 이상징후 차단기 (Anomaly Circuit Breakers)
# ──────────────────────────────────────────────────────────────────────────
# CB1: WS 끊김 → 진입 차단
# CB2: flow_score 결측 (flow_engine 있는데 데이터 부족) → 전략 차단
# CB3: 세션 불일치 (env 세션 vs 실제 시각) → 진입 차단
# CB4: 동일 신호 반복 발사 쿨다운

_CB_MIN_FLOW_GATE: int = int(os.getenv("MIN_FLOW_GATE", "5"))
_CB_SIGNAL_COOLDOWN_SEC: float = float(os.getenv("ENTRY_SIGNAL_COOLDOWN_SEC", "30"))

# 세션별 유효 시간 범위 (KST HH:MM)
_CB_SESSION_WINDOWS: Dict[str, list] = {
    "day":   [("08:44", "15:36")],
    "night": [("18:00", "23:59"), ("00:00", "06:01")],
}


def _cb_in_session(session_type: str, hhmm: str) -> bool:
    """현재 시각이 선언된 세션 범위 안에 있는지 확인."""
    for s, e in _CB_SESSION_WINDOWS.get(session_type.lower(), []):
        if s <= hhmm <= e:
            return True
    return False


def _check_anomaly_gates(
    state:           Dict,
    runtime:         "AppRuntime",
    now_ts:          float,
    fnet_len:        int,
    last_signal:     str,
    last_signal_ts:  float,
    entry_signal:    str,
) -> str:
    """
    이상징후 차단기 — 이상 시 차단 이유 문자열 반환, 정상 시 빈 문자열 반환.

    live 모드(is_live_ready=True) 에서만 CB1~CB3 활성.
    CB4(쿨다운)는 모든 모드에서 활성.
    """
    live = is_live_ready(runtime)

    if live:
        # CB1: WS 끊김
        if not state.get("ws_connected", False):
            return "CB1_WS_DISCONNECTED"

        # CB2: flow_engine 존재하지만 데이터 부족
        if runtime.flow_engine is not None and fnet_len < _CB_MIN_FLOW_GATE:
            return f"CB2_FLOW_INSUFFICIENT(n={fnet_len}<{_CB_MIN_FLOW_GATE})"

        # CB3: 세션 불일치
        from datetime import datetime as _dt
        _hhmm = _dt.now().strftime("%H:%M")
        _sess = str(state.get("session_type", "day"))
        if not _cb_in_session(_sess, _hhmm):
            return f"CB3_SESSION_MISMATCH(declared={_sess},now={_hhmm})"

    # CB4: 동일 신호 반복 쿨다운 (전 모드 공통)
    if (entry_signal == last_signal
            and (now_ts - last_signal_ts) < _CB_SIGNAL_COOLDOWN_SEC):
        remaining = _CB_SIGNAL_COOLDOWN_SEC - (now_ts - last_signal_ts)
        return f"CB4_COOLDOWN({entry_signal},{remaining:.0f}s 남음)"

    return ""


def _map_llm_action(direction: int, risk_view: str) -> str:
    """LLM direction + risk_view → 사용자 표시용 액션 레이블."""
    if risk_view == "failure":
        return "오류"
    if risk_view == "neutral":
        return "nothing"
    if risk_view == "positive":
        return "매수" if direction == 1 else "매도"
    if risk_view == "negative":
        return "매수청산" if direction == 1 else "매도청산"
    return "nothing"


def _llm_call_in_bg(
    runtime: AppRuntime,
    net_series: List[float],
    flow_score: float,
    direction: int,
    state_snap: Dict[str, Any],
    call_key: str = "",
) -> None:
    """
    백그라운드 스레드: LLM 호출 → weight_adjust → TTL 등록 → state 갱신.
    호출 실패 시 weight_adjust=0 (TTL 갱신 안 함 → 기존 상태 유지).
    완료 후 runtime.state에 flow_llm_* 필드를 직접 갱신 (대시보드 가시성).
    """
    try:
        from engines.llm_input_builder import LLMInputBuilder, FlowMetrics, MarketContext

        builder = LLMInputBuilder()
        n = len(net_series)
        delta_desc = (
            f"{'개선' if net_series[-1] > net_series[-2] else '악화'} "
            f"({net_series[-1] - net_series[-2]:+.0f}억)"
            if n >= 2 else "데이터 부족"
        )
        metrics = FlowMetrics(
            delta_trend=delta_desc,
            ema5_relation=f"flow_score {flow_score:+.4f}",
            flow_score=flow_score,
            ma_cross="단기 MA 확인 중",
        )
        oi_delta = float(state_snap.get("oi_delta", 0.0))
        basis_v  = float(state_snap.get("basis", 0.0))
        atr_v    = float(state_snap.get("atr", 0.0))
        pnl_left = float(state_snap.get("daily_loss_limit_left", 0.0))
        context = MarketContext(
            oi_trend="증가" if oi_delta > 0 else ("감소" if oi_delta < 0 else "횡보"),
            basis_trend="확대" if basis_v > 0.2 else ("축소" if basis_v < -0.2 else "방향없음"),
            institution_trend="중립",
            volatility_state=(
                "expanding" if atr_v > 0.3 else ("contracting" if atr_v < 0.1 else "normal")
            ),
            account_risk=(
                "한도근접" if pnl_left < 100_000 else ("주의" if pnl_left < 300_000 else "정상")
            ),
        )
        system_prompt, user_prompt = builder.build(
            net_series=net_series,
            metrics=metrics,
            context=context,
        )

        # ── MarketAnalyst view 컨텍스트 추가 ──────────────────────────
        # 3분 인터벌 Haiku 분석 결과를 EntryAdvisor(Sonnet)에 전달
        # 없으면 무시 (거래 계속)
        _mv = state_snap.get("market_view")
        _mv_ts = state_snap.get("market_view_ts", "")
        if _mv:
            _mv_history = state_snap.get("market_view_history", [])
            _hist_str = ""
            if len(_mv_history) >= 2:
                _prev_views = [
                    f"{v.get('ts','')} {v.get('view')}({v.get('direction')})"
                    for v in _mv_history[:-1]
                ]
                _hist_str = f"\n  이전: {' → '.join(_prev_views)}"
            user_prompt += (
                f"\n\n[장중 시장 분석 ({_mv_ts})]"
                f"\n  현재: view={_mv.get('view')} direction={_mv.get('direction')}"
                f" confidence={_mv.get('confidence')}"
                f"\n  근거: {_mv.get('reason', '')}"
                f"{_hist_str}"
            )

        # ── CheckpointAnalyst 결과 컨텍스트 추가 ─────────────────────────
        # 09:15 / 10:30 / 13:00 체크포인트 결과를 Sonnet에 전달
        # 없는 항목은 자동 생략
        _cp_lines = []
        _oc = state_snap.get("opening_char")
        if _oc:
            _cp_lines.append(
                f"  09:15 아침장: char={_oc.get('char')} "
                f"size_filter={_oc.get('size_filter')} | {_oc.get('reason', '')}"
            )
        _pr = state_snap.get("provisional_regime")
        if _pr:
            _cp_lines.append(
                f"  10:30 잠정레짐: regime={_pr.get('regime')} "
                f"direction={_pr.get('direction')} conf={_pr.get('confidence')} "
                f"| {_pr.get('reason', '')}"
            )
        _ar = state_snap.get("afternoon_regime")
        if _ar:
            _cp_lines.append(
                f"  13:00 오후재평가: regime={_ar.get('regime')} "
                f"late_bias={_ar.get('late_bias')} risk_adj={_ar.get('risk_adj')} "
                f"| {_ar.get('reason', '')}"
            )
        if _cp_lines:
            user_prompt += "\n\n[장중 체크포인트 분석]\n" + "\n".join(_cp_lines)

        # ── 실패 회피 패턴 경고 주입 ─────────────────────────────────────
        # failure_patterns 테이블에서 현재 체크포인트 조합과 일치하는
        # forbidden/failure 패턴 조회 → 존재 시 user_prompt에 추가.
        # confidence=LOW 패턴은 주입 안 함 (과보수화 방지).
        try:
            from rag.rag_retriever import get_failure_avoidance_context as _get_fac
            _fac = _get_fac(
                opening_char     = (_oc or {}).get("char") if _oc else None,
                prov_regime      = (_pr or {}).get("regime") if _pr else None,
                afternoon_regime = (_ar or {}).get("regime") if _ar else None,
                mmean_db         = runtime.db_path,
            )
            if _fac:
                user_prompt += f"\n\n{_fac}"
        except Exception as _fac_err:
            runtime.log.debug("failure_avoidance_context 조회 실패 (무시): %s", _fac_err)

        # build_snapshot()으로 압축 스냅샷 구성 (원본 시계열 제외 — 로그 분리 원칙)
        input_snapshot = builder.build_snapshot(metrics, context)

        result = asyncio.run(
            runtime.llm_caller.call_once(
                system_prompt=system_prompt,
                user_prompt=user_prompt,
                input_snapshot=input_snapshot,
            )
        )
        import time as _time_mod
        _ts_str = _time_mod.strftime("%H:%M")
        _model_name = getattr(runtime.llm_chain, "last_provider", "") or ""

        if result.success:
            runtime.llm_ttl_manager.set(result.weight_adjust)
            runtime.log.info(
                "LLM bg 완료 | dir=%+d | wa=%+.4f | conf=%.3f | risk=%s | key=%s",
                direction, result.weight_adjust, result.confidence, result.risk_view, call_key,
            )
            _action = _map_llm_action(direction, result.risk_view)

            # JudgmentLog: LLM 결과 업데이트
            _j_id_snap = state_snap.get("active_judgment_event_id")
            if _j_id_snap and getattr(runtime, "judgment_log", None):
                try:
                    runtime.judgment_log.update_llm(
                        event_id=_j_id_snap,
                        action=_action,
                        risk_view=result.risk_view,
                        confidence=result.confidence,
                        weight_adj=result.weight_adjust,
                        call_key=call_key,
                    )
                except Exception as _je:
                    log.warning("JudgmentLog update_llm 실패 (계속): %s", _je)

            # ── 대시보드·로그 state 갱신 (engine_lock으로 레이스 방지) ──
            with runtime.state_obj.engine_lock:
                runtime.state.update({
                    "flow_llm_risk_view":       result.risk_view,
                    "flow_llm_confidence":      result.confidence,
                    "flow_llm_weight_adjust":   result.weight_adjust,
                    "flow_llm_error":           "",
                    "flow_llm_log_id":          result.log_id or 0,
                    "llm_last_model":           _model_name,
                    "flow_llm_reason_main":     result.reason_main,
                    "flow_llm_reason_against":  result.reason_against,
                })
                _hist = list(runtime.state.get("llm_signal_history", []))
                _hist.insert(0, {
                    "action":     _action,
                    "model":      _model_name,
                    "ts":         _ts_str,
                    "risk_view":  result.risk_view,
                    "confidence": round(result.confidence, 2),
                })
                runtime.state["llm_signal_history"] = _hist[:5]
        else:
            runtime.log.info(
                "LLM bg 실패 [FALLBACK wa=0] | error=%s | key=%s",
                result.error_type, call_key,
            )
            _action = _map_llm_action(direction, "failure")
            # ── 실패도 state에 기록 (에러 추적) ──────────────────────
            with runtime.state_obj.engine_lock:
                runtime.state.update({
                    "flow_llm_risk_view":     "failure",
                    "flow_llm_confidence":    0.0,
                    "flow_llm_weight_adjust": 0.0,
                    "flow_llm_error":         result.error_type or "unknown",
                })
                _hist = list(runtime.state.get("llm_signal_history", []))
                _hist.insert(0, {
                    "action":     _action,
                    "model":      _model_name or "?",
                    "ts":         _ts_str,
                    "risk_view":  "failure",
                    "confidence": 0.0,
                })
                runtime.state["llm_signal_history"] = _hist[:5]
    except Exception as e:
        runtime.log.warning("_llm_call_in_bg 예외: %s", e)
        with runtime.state_obj.engine_lock:
            runtime.state.update({
                "flow_llm_risk_view":    "failure",
                "flow_llm_confidence":   0.0,
                "flow_llm_weight_adjust": 0.0,
                "flow_llm_error":        f"bg_exception:{type(e).__name__}",
            })


def engine_loop(runtime: AppRuntime) -> None:
    state = runtime.state
    st = runtime.settings
    start_ts = time.time()
    _stale_since: float = 0.0                                             # stale 시작 시각 (0=정상)
    _DATA_STALE_HALT_SEC: int = int(os.getenv("DATA_STALE_HALT_SEC", "60"))  # N초 stale → 에스컬레이션
    _last_stale_warn: float   = 0.0
    _engine_err_cat: str      = ""   # 마지막 except Exception 카테고리 (성공 시 clear용)
    _ws_was_connected: bool   = False  # WS 재연결 감지용 (PriceGuard reset 트리거)
    # ── [FLOW] 진단 로그 타이머 ────────────────────────────────────────────
    # Flow/Regime 성숙도를 주기적으로 출력 (외국인 수급 누적 상태 모니터링)
    _last_flow_diag: float     = 0.0
    _FLOW_DIAG_INTERVAL: float = float(os.getenv("FLOW_DIAG_INTERVAL_SEC", "10"))
    # ── [CB4] 동일 신호 반복 쿨다운 추적 ───────────────────────────────────
    _cb4_last_signal:    str   = ""
    _cb4_last_signal_ts: float = 0.0
    price_guard   = PriceGuard()         # 가격 신뢰도 방어층
    _live_executor = LiveOrderExecutor(runtime)  # 실주문 실행 관리자
    # 프로세스 재시작 시 복원된 포지션 진입가로 L2 앵커 초기화 (쓰레기 틱 비교 기준 확보)
    if runtime.sim_engine and getattr(runtime.sim_engine, "_pos", None):
        price_guard.seed(runtime.sim_engine._pos.entry_price)
        runtime.log.info(
            "PriceGuard seed | 복원 포지션 진입가 %.2f",
            runtime.sim_engine._pos.entry_price,
        )
    while True:
        # 하드스톱: 수동 /api/engine/resume 전까지 모든 틱 차단
        if runtime.error_tracker and runtime.error_tracker.is_hard_stopped():
            time.sleep(10)
            continue
        # 서킷 브레이커: 3회 연속 에러 → pause, 60s half-open 자동 재시도
        if runtime.error_tracker and runtime.error_tracker.is_circuit_open():
            time.sleep(5)
            continue
        try:
            now_ts = time.time()
            data_quality = "live"
            if not is_live_ready(runtime):
                raw = simulated_data_tick(runtime, now_ts - start_ts)
                data_quality = "simulated"
            else:
                snap_rt = runtime.rt_client.get_snapshot() if runtime.rt_client else {}
                if not snap_rt or snap_rt.get("updated_at", 0.0) <= 0:
                    raw = simulated_data_tick(runtime, now_ts - start_ts)
                    data_quality = "simulated"
                elif now_ts - float(snap_rt["updated_at"]) > 10.0:
                    raw = simulated_data_tick(runtime, now_ts - start_ts)
                    data_quality = "stale"
                else:
                    vd = max(0.0, float(snap_rt["cum_volume"]) - float(state["cum_volume"])) if float(state["cum_volume"]) > 0 else 0.0
                    raw = {
                        **snap_rt,
                        "foreign_buy": runtime.state_obj.investor_cache["foreign_buy"],
                        "oi_delta": float(snap_rt["oi_value"]) - float(state["oi_value"]),
                        "volume_delta": vd,
                        "volume_burst": compute_volume_burst(runtime, vd),
                    }

            # ─── PriceGuard: 가격 신뢰도 방어층 ──────────────────────────
            # WS 재연결 감지 → last_valid 초기화 (L2 스파이크 기준 리셋)
            ws_now = state.get("ws_connected", False)
            if ws_now and not _ws_was_connected:
                price_guard.soft_reset()
                runtime.log.info(
                    "PriceGuard soft_reset | WS 재연결 | last=%.2f",
                    price_guard._last or 0.0,
                )
            _ws_was_connected = ws_now
            # 실시간 데이터일 때만 가격 검증 (simulated/stale은 이미 gate 차단)
            if data_quality == "live":
                _pg_ok, _pg_reason = price_guard.validate(float(raw["futures_price"]))
                if not _pg_ok:
                    if _pg_reason == "waiting_first_tick":
                        # 첫 유효 틱 아직 미수신 — 정상 초기화 상태. streak 미누적.
                        # WARNING 이 아니라 DEBUG 로 처리 (로그 오염 방지).
                        data_quality = "waiting_first_tick"
                        runtime.log.debug("PriceGuard | 첫 유효 틱 대기 중 | price=%.2f", float(raw["futures_price"]))
                    else:
                        runtime.log.warning(
                            "PriceGuard 거부 [%s] | streak=%d → data_gate 차단",
                            _pg_reason, price_guard._streak,
                        )
                        data_quality = "price_blocked"
                else:
                    # 유효 가격 수신 — 첫 번째 수신이면 state에 기록
                    if not state.get("ws_first_valid_tick_received", False):
                        state["ws_first_valid_tick_received"] = True
                        runtime.log.info(
                            "PriceGuard 첫 유효 틱 앵커 설정 | price=%.2f",
                            float(raw["futures_price"]),
                        )

            # ─── 데이터 게이트 ─────────────────────────────────────────────
            # dev mode(is_live_ready==False) → simulated 허용 (gate 통과)
            # prod mode(is_live_ready==True) → "live" 아니면 진입/기록 차단
            data_gate_ok = not is_live_ready(runtime) or data_quality == "live"
            if data_quality == "live":
                _stale_since = 0.0          # 정상 복구 → stale 타이머 리셋
            elif is_live_ready(runtime) and _stale_since == 0.0 and data_quality != "waiting_first_tick":
                # waiting_first_tick 은 정상 초기화 상태 — stale 에스컬레이션 타이머 시작 안 함
                _stale_since = now_ts       # stale/simulated/price_blocked 시작 시각 최초 기록

            if not data_gate_ok:
                stale_duration = (now_ts - _stale_since) if _stale_since > 0 else 0.0
                if now_ts - _last_stale_warn >= 10.0:
                    _last_stale_warn = now_ts
                    if data_quality == "waiting_first_tick":
                        # 첫 유효 틱 대기 — DEBUG 수준으로 조용히 처리 (에스컬레이션 없음)
                        runtime.log.debug("데이터 게이트 | 첫 유효 틱 대기 중 (WS 초기화)")
                    else:
                        runtime.log.warning(
                            "데이터 게이트 차단 | quality=%s | stale=%.0fs | 진입/기록 차단",
                            data_quality, stale_duration,
                        )
                if stale_duration >= _DATA_STALE_HALT_SEC and data_quality != "waiting_first_tick":
                    runtime.error_tracker.record(
                        f"시장 데이터 {data_quality} {stale_duration:.0f}초 지속"
                    )

            with runtime.state_obj.engine_lock:
                ema = update_ema(runtime, float(state["basis_ema"]), float(raw["basis"]))
                runtime.state_obj.ema_basis_history.append((now_ts, ema))
                slope = compute_slope(list(runtime.state_obj.ema_basis_history)[-st["SLOPE_WINDOW"]:])
                basis_ema_delta = round(ema - float(state["basis_ema"]), 4)
                fsg_composite, fsg_delta = compute_foreign_signal(runtime, float(raw["foreign_buy"]))
                _fsg_mode = str(state.get("foreign_signal_mode",
                                           st.get("FOREIGN_SIGNAL_MODE", "composite")))
                foreign_signal = fsg_delta if _fsg_mode == "delta_only" else fsg_composite
                _alt_signal    = fsg_composite if _fsg_mode == "delta_only" else fsg_delta

                # ── FlowEngine: 외국인 순매수 시계열 → flow_score ──────────
                _fnet = list(runtime.state_obj.foreign_net_history)
                if runtime.flow_engine is not None and len(_fnet) >= 3:
                    _flow_score = runtime.flow_engine.compute(_fnet)
                    _flow_res   = runtime.flow_regime_engine.compute(_flow_score)
                else:
                    _flow_score = 0.0
                    _flow_res   = None

                ema_fast, ema_slow, ema_fast_slope = update_trend_emas(runtime, float(raw["futures_price"]), now_ts)
                vwap = update_vwap(runtime, float(raw["futures_price"]), float(raw["trade_qty"]))
                price_vs_vwap = round(float(raw["futures_price"]) - vwap, 4)
                price = float(raw["futures_price"])
                spread = abs(float(raw["best_ask"]) - float(raw["best_bid"]))
                atr = update_atr(runtime, price + spread * 0.5, price - spread * 0.5, price)

                challenger_params = runtime.param_store.get_safe()
                tc = runtime.regime_engine.cfg
                tc.trend_weight_long = challenger_params.trend_weight_long
                tc.trend_weight_short = challenger_params.trend_weight_short
                tc.trend_ema_align_score = challenger_params.trend_ema_align_score
                tc.trend_ema_slope_score = challenger_params.trend_ema_slope_score
                tc.trend_ema_slope_threshold = challenger_params.trend_ema_slope_threshold

                _bias_kwargs = dict(
                    oi_delta=float(raw["oi_delta"]),
                    basis=float(raw["basis"]),
                    basis_ema=ema,
                    basis_ema_delta=basis_ema_delta,
                    basis_slope=slope,
                    volume_burst=float(raw["volume_burst"]),
                    trade_strength=float(raw.get("trade_strength", 100.0)),
                    ema_fast=ema_fast,
                    ema_slow=ema_slow,
                    ema_fast_slope=ema_fast_slope,
                )
                bias_inputs = BiasInputs(foreign_buy=foreign_signal, **_bias_kwargs)
                bias_inputs_alt = BiasInputs(foreign_buy=_alt_signal, **_bias_kwargs)
                res = runtime.regime_engine.update(bias_inputs)
                state.update({
                    "bias": res.bias,
                    "entry_signal": res.entry_signal,
                    "confidence": res.confidence,
                    "futures_price": raw["futures_price"],
                    "spot_price": raw["spot_price"],
                    "foreign_buy": raw["foreign_buy"],
                    "oi_value": raw["oi_value"],
                    "oi_delta": raw["oi_delta"],
                    "basis": raw["basis"],
                    "basis_ema": round(ema, 4),
                    "basis_ema_delta": basis_ema_delta,
                    "basis_slope": round(slope, 5),
                    "trade_qty": raw["trade_qty"],
                    "cum_volume": raw["cum_volume"],
                    "volume_delta": raw["volume_delta"],
                    "volume_burst": raw["volume_burst"],
                    "last_trade_strength": raw.get("trade_strength", 100.0),
                    "best_ask": raw["best_ask"],
                    "best_bid": raw["best_bid"],
                    "long_score": res.long_score,
                    "short_score": res.short_score,
                    "reason": res.reason,
                    "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3],
                    "config_hash": runtime.cfg_mgr.get_hash(),
                    "data_quality": data_quality,
                    "ema_fast": ema_fast,
                    "ema_slow": ema_slow,
                    "ema_fast_slope": ema_fast_slope,
                    "trend_long_score": res.trend_long_score,
                    "trend_short_score": res.trend_short_score,
                    "trend_weight_long": res.trend_weight_long,
                    "trend_weight_short": res.trend_weight_short,
                    "param_hash": challenger_params.param_hash,
                    "param_store_version": challenger_params.version,
                    "param_store_hash": challenger_params.config_hash,
                    "vwap": vwap,
                    "atr": atr,
                    "price_vs_vwap": price_vs_vwap,
                    "foreign_signal_mode": _fsg_mode,
                    # FlowEngine 파이프라인 출력
                    "flow_score":        _flow_score,
                    "flow_regime_label": _flow_res.label       if _flow_res else "NEUTRAL",
                    "flow_long_weight":  _flow_res.long_weight  if _flow_res else 0.5,
                    "flow_short_weight": _flow_res.short_weight if _flow_res else 0.5,
                })

                # ── [FLOW] 진단 로그: Flow/Regime 성숙도 주기 모니터링 ─────────
                # 왜 regime_weight가 0.5인지 한 줄에 전부 보인다.
                # 확인 항목: 외국인 수급 시계열 길이, foreign_buy, flow_score,
                #            regime 레이블, long/short weight, bias, entry, data_quality
                if now_ts - _last_flow_diag >= _FLOW_DIAG_INTERVAL:
                    _last_flow_diag = now_ts
                    runtime.log.info(
                        "[FLOW] fq_len=%d foreign_buy=%.0f"
                        " flow_score=%+.4f regime=%s"
                        " long_w=%.4f short_w=%.4f"
                        " bias=%s entry=%s dq=%s",
                        len(runtime.state_obj.foreign_net_history),
                        float(raw.get("foreign_buy", 0.0)),
                        _flow_score,
                        state.get("flow_regime_label", "NEUTRAL"),
                        state.get("flow_long_weight",  0.5),
                        state.get("flow_short_weight", 0.5),
                        state.get("bias",         "NEUTRAL"),
                        state.get("entry_signal", "WAIT"),
                        data_quality,
                    )

                if data_gate_ok:
                    runtime.recorder.update_regime_event(state["timestamp"], res, state)
                    runtime.recorder.insert_entry_event(state["timestamp"], res, state)

                with runtime.state_obj.today_stats_lock:
                    ts_cache = dict(runtime.state_obj.today_stats)
                state.update({
                    "today_trade_count": ts_cache["today_trade_count"],
                    "today_pnl_won": ts_cache["today_pnl_won"],
                    "daily_loss_limit_left": ts_cache["daily_loss_limit_left"],
                    "recent_ticks": ts_cache["recent_ticks"],
                    "sim_state": {
                        "has_position": bool(state.get("sim_has_pos", False)),
                        "direction": int(state.get("sim_direction", 0)),
                        "entry_price": float(state.get("sim_entry_price", 0.0)),
                        "trailing_active": bool(state.get("sim_trailing_active", False)),
                    },
                })

                if runtime.llm_filter is not None:
                    opp = runtime.llm_filter.get_latest_valid() or {}
                    state.update({
                        "llm_filter_score": float(opp.get("opportunity_score", -1.0)),
                        "llm_filter_direction": str(opp.get("direction_bias", "UNKNOWN")),
                        "llm_filter_valid": bool(opp),
                    })

                # ── LLM Gate + TTL + EMA filter + PositionEngine ─────────
                # ① TTL 현재 값을 먼저 읽는다 (consume 전 — 이번 진입에 실제 적용될 값)
                _raw_wa = (
                    runtime.llm_ttl_manager.get_active_weight()
                    if runtime.llm_ttl_manager is not None else 0.0
                )

                _ttl_just_expired = False   # 이번 틱에 TTL이 소멸됐는지 추적

                # ── 이상징후 차단기 (CB1~CB4) ─────────────────────────────────
                # 진입 시도 전 이상 상태 감지 → entry_signal을 WAIT으로 강제
                _cb_reason = ""
                if res.entry_signal in ("LONG_READY", "SHORT_READY") and data_gate_ok:
                    _cb_reason = _check_anomaly_gates(
                        state         = state,
                        runtime       = runtime,
                        now_ts        = now_ts,
                        fnet_len      = len(_fnet),
                        last_signal   = _cb4_last_signal,
                        last_signal_ts= _cb4_last_signal_ts,
                        entry_signal  = res.entry_signal,
                    )
                    if _cb_reason:
                        log.warning("이상징후 차단 | reason=%s | signal=%s → WAIT",
                                    _cb_reason, res.entry_signal)
                    else:
                        # CB4 타이머 갱신 (차단 없이 통과했을 때만)
                        _cb4_last_signal    = res.entry_signal
                        _cb4_last_signal_ts = now_ts

                if runtime.llm_gate is not None and runtime.llm_ttl_manager is not None:
                    if (res.entry_signal in ("LONG_READY", "SHORT_READY")
                            and data_gate_ok
                            and not _cb_reason):
                        # ② 이번 진입에 _raw_wa를 읽은 뒤 TTL 소멸 (조건 A)
                        runtime.llm_ttl_manager.consume_on_entry()
                        _ttl_just_expired = True

                        # JudgmentLog: 판단 사건 열기
                        if getattr(runtime, "judgment_log", None):
                            try:
                                _j_id = runtime.judgment_log.open_event(
                                    state_snap=dict(state),
                                    entry_signal=res.entry_signal,
                                    exec_mode=str(state.get("execution_mode", "OFF")),
                                )
                                state["active_judgment_event_id"] = _j_id
                            except Exception as _je:
                                log.warning("JudgmentLog open_event 실패 (계속): %s", _je)

                        # ③ 분봉 Call Key 생성 (중복 LLM 호출 차단)
                        #    현재: minute+signal 단위
                        #    향후 확장: minute+signal+flow_bucket (같은 분 내 큰 flow 변화 재평가)
                        #    → LLMCallDedup.check_and_mark() 시그니처 확장 후 적용
                        _minute_str = str(state.get("timestamp", ""))[:16]   # "YYYY-MM-DD HH:MM"
                        _signal_str = res.entry_signal
                        _call_key   = f"{_minute_str}|{_signal_str}"

                        # ④ LLM 게이트 + Dedup 체크 → bg 호출 (다음 진입용 TTL 생성)
                        from engines.llm_gate import GateInputs as _GateInputs
                        _g = runtime.llm_gate.check(
                            _GateInputs(flow_score=float(state.get("flow_score", 0.0)))
                        )
                        _dedup_ok = (
                            runtime.llm_caller.dedup.check_and_mark(_minute_str, _signal_str)
                            if runtime.llm_caller is not None else False
                        )
                        if (
                            _g.should_call
                            and _dedup_ok
                            and runtime.llm_caller is not None
                            and len(_fnet) >= 5
                        ):
                            _dir_llm = 1 if res.entry_signal == "LONG_READY" else -1
                            threading.Thread(
                                target=_llm_call_in_bg,
                                args=(runtime, _fnet, float(state.get("flow_score", 0.0)),
                                      _dir_llm, dict(state), _call_key),
                                daemon=True,
                                name="MMEAN-LLMCall",
                            ).start()
                            state["flow_llm_call_key"] = _call_key

                    # 시간 경과로 TTL 만료됐는지 감지 (get_active_weight가 이미 0.0 반환했을 때)
                    elif _raw_wa == 0.0 and not runtime.llm_ttl_manager._active:
                        _ttl_just_expired = True   # 조건 B 만료 — EMA expire 처리 필요

                # ⑤ EMA 필터
                #    TTL 소멸(조건A/B) 시 on_ttl_expire() 호출 → expire_mode에 따라 reset 또는 decay
                #    TTL 활성: update(raw) 정상 적용
                if runtime.llm_ema_filter is not None:
                    if _ttl_just_expired:
                        runtime.llm_ema_filter.on_ttl_expire()   # reset 또는 noop(decay)
                    _wa_filtered = runtime.llm_ema_filter.update(_raw_wa)
                else:
                    _wa_filtered = _raw_wa

                # ⑥ TTL 상태 state 갱신 (대시보드)
                if runtime.llm_ttl_manager is not None:
                    _ttl_st = runtime.llm_ttl_manager.state()
                    state.update({
                        "flow_ttl_active":    _ttl_st.active,
                        "flow_ttl_remaining": _ttl_st.remaining_sec,
                    })

                # ⑦ 장중 LLM 영향도 스케일링
                #    LOW=0.5× / MID=1.0× / HIGH=1.5× (±0.20 clamp 유지)
                _influence = str(state.get("llm_intraday_influence", "MID"))
                _wa_scaled  = _scale_wa_by_influence(_wa_filtered, _influence)

                # ── Warning Signal Entry Gate ──────────────────────────────
                # Caution(미시, pattern_memory) + Ambiguous(거시, failure_patterns)
                # 엔진이 직접 계산 → entry_gate로 변환.
                # entry_gate 가 LLM weight_adjust 보다 우선한다.
                #   REJECT : 진입 차단  + _wa_scaled 0으로 무효화
                #   HALF   : 최대 SIZE_DOWN 강제 (_wa_scaled = -_WA_MAX_DOWN)
                #   ALLOW  : _wa_scaled 그대로 사용
                # Warning Gate 상태 초기값 (try 실패 시 대비)
                _pw_level = "none"
                _fac_type = "none"
                _fac_conf = "NONE"
                _entry_gate = "ALLOW"
                try:
                    from rag.rag_prep       import get_pattern_warning_level as _get_pwl
                    from rag.rag_retriever  import get_failure_pattern_meta   as _get_fpm

                    _bz = str(state.get("basis_zone",  "MID"))
                    _vz = str(state.get("volume_zone", "MID"))
                    _tb = str(state.get("time_bucket", "MID"))
                    _pw_level = _get_pwl(runtime.db_path, _bz, _vz, _tb)

                    if _pw_level == "danger":
                        _entry_gate = "REJECT"
                    elif _pw_level in ("warning", "caution"):
                        _entry_gate = "HALF"

                    # Ambiguous proxy v1: forbidden(MED/HIGH) → REJECT 격상
                    _oc_char = (state.get("opening_char")       or {}).get("char")
                    _pr_reg  = (state.get("provisional_regime") or {}).get("regime")
                    _ar_reg  = (state.get("afternoon_regime")   or {}).get("regime")
                    _fac_type, _fac_conf = _get_fpm(
                        opening_char     = _oc_char,
                        prov_regime      = _pr_reg,
                        afternoon_regime = _ar_reg,
                        mmean_db         = runtime.db_path,
                    )
                    if _fac_type == "forbidden" and _fac_conf in {"MED", "HIGH"}:
                        _entry_gate = "REJECT"

                    if _entry_gate != "ALLOW":
                        log.info("entry_gate=%s | caution=%s | fac=%s/%s",
                                 _entry_gate, _pw_level, _fac_type, _fac_conf)
                except Exception as _eg_err:
                    log.debug("entry_gate 계산 실패 (ALLOW 유지): %s", _eg_err)

                # entry_gate 우선순위 적용 — LLM weight_adjust 덮어씀
                if _entry_gate == "REJECT":
                    _wa_scaled = 0.0
                elif _entry_gate == "HALF":
                    _wa_scaled = -_WA_MAX_DOWN
                # ──────────────────────────────────────────────────────────

                state["flow_llm_weight_adjust"] = _wa_scaled
                state["flow_entry_gate"]        = _entry_gate   # 대시보드/로그용
                state["flow_caution_level"]     = _pw_level          # danger/warning/caution/none
                state["flow_fac_type"]          = _fac_type           # forbidden/failure/none
                state["flow_fac_conf"]          = _fac_conf           # HIGH/MED/LOW/NONE
                state["flow_wa_raw"]            = float(_raw_wa)      # LLM 원본 (gate 적용 전)
                # entry_gate 누적 카운터 + 인메모리 이력 + DB 기록
                # ── 상태 전이 감지: 이전 gate 와 달라진 경우에만 카운터/이력/DB 기록 ──
                _prev_gate = str(state.get("flow_entry_gate_prev") or "ALLOW")
                _gate_transitioned = (_entry_gate != _prev_gate)
                state["flow_entry_gate_prev"] = _entry_gate  # 다음 루프 비교용

                if _entry_gate in ("REJECT", "HALF") and _gate_transitioned:
                    # ① 누적 카운터 (전이 시에만 증가)
                    if _entry_gate == "REJECT":
                        state["flow_gate_reject_count"] = int(state.get("flow_gate_reject_count", 0)) + 1
                    else:
                        state["flow_gate_half_count"]   = int(state.get("flow_gate_half_count",   0)) + 1

                    # ② 인메모리 이력 (최근 10건, 대시보드 실시간 표시용)
                    try:
                        from datetime import datetime as _dt
                        _gate_ts    = _dt.now().strftime("%H:%M:%S")
                        _gate_event = {
                            "ts":      _gate_ts,
                            "gate":    _entry_gate,
                            "caution": _pw_level,
                            "fac":     _fac_type,
                            "conf":    _fac_conf,
                        }
                        _gate_log = list(state.get("flow_gate_log") or [])
                        _gate_log.insert(0, _gate_event)
                        state["flow_gate_log"] = _gate_log[:10]  # 최근 10건만 보관
                    except Exception as _gl_err:
                        log.debug("gate_log 인메모리 기록 실패: %s", _gl_err)

                    # ③ DB 기록 (entry_gate_log 테이블) — 상태 전이 시에만
                    # 연결을 runtime에 캐싱해 매 전이마다 open/close 반복 방지
                    try:
                        from datetime import datetime as _dt2
                        import sqlite3 as _sqlite3
                        _now_iso = _dt2.now().strftime("%Y-%m-%dT%H:%M:%S")
                        _sess_dt = _now_iso[:10]
                        # lazy init: runtime._gate_log_conn 에 연결 캐싱
                        if not getattr(runtime, "_gate_log_conn", None):
                            _gc = _sqlite3.connect(runtime.db_path, check_same_thread=False)
                            _gc.execute("PRAGMA journal_mode=WAL")
                            _gc.execute("PRAGMA synchronous=NORMAL")
                            runtime._gate_log_conn = _gc
                        runtime._gate_log_conn.execute("""
                            INSERT INTO entry_gate_log
                                (ts, session_date, gate, caution_level, fac_type, fac_conf,
                                 basis_zone, volume_zone, time_bucket,
                                 opening_char, prov_regime, afternoon_regime)
                            VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
                        """, (
                            _now_iso, _sess_dt, _entry_gate,
                            _pw_level, _fac_type, _fac_conf,
                            str(state.get("basis_zone",  "") or ""),
                            str(state.get("volume_zone", "") or ""),
                            str(state.get("time_bucket", "") or ""),
                            str(_oc_char or ""),
                            str(_pr_reg  or ""),
                            str(_ar_reg  or ""),
                        ))
                        runtime._gate_log_conn.commit()
                        log.info("entry_gate_log 기록: %s ← %s", _entry_gate, _prev_gate)
                    except Exception as _db_err:
                        # 연결 오류 시 캐시 초기화해 다음 시도에서 재연결
                        runtime._gate_log_conn = None
                        log.debug("entry_gate_log DB 기록 실패: %s", _db_err)

                # ⑧ 장전 수동 옵션 보정값 로드
                #    MANIA_2/1/NORMAL/FEAR_1/2 → regime weight·size 미세 조정
                _pm_mode = str(state.get("premarket_manual_mode", "NORMAL"))
                _pm_long_adj, _pm_short_adj, _pm_size_adj = _get_premarket_adj(_pm_mode)
                # state 보정 결과 동기화 (대시보드 표시용)
                state["premarket_bias_adj_long"]  = _pm_long_adj
                state["premarket_bias_adj_short"] = _pm_short_adj
                state["premarket_size_adj"]       = _pm_size_adj

                # ── CheckpointAnalyst opening_char size_filter 게이트 ───────
                # opening_char.size_filter: normal=통과 / reduce=0.5× / skip=진입차단
                _oc_state  = state.get("opening_char") or {}
                _oc_sf     = _oc_state.get("size_filter", "normal")
                _oc_size_adj = 1.0 if _oc_sf == "normal" else (0.5 if _oc_sf == "reduce" else 0.0)
                if _oc_sf == "reduce":
                    log.debug("opening_char size_filter=reduce → base_size×0.5")
                elif _oc_sf == "skip":
                    log.info("opening_char size_filter=skip → 진입 차단")

                # ── final_gate_reason: 진입 차단 원인 단일 요약 필드 ─────────
                # 운영자가 왜 WAIT이 됐는지 한 눈에 확인 가능하도록 압축.
                # 우선순위: data_gate > opening_skip > cb_block > entry_gate_reject > entry_gate_half > allowed
                if not data_gate_ok:
                    _final_gate_reason = "data_gate"
                elif _oc_sf == "skip":
                    _final_gate_reason = "opening_skip"
                elif _cb_reason:
                    _final_gate_reason = "cb_block:" + str(_cb_reason)[:30]
                elif _entry_gate == "REJECT":
                    _final_gate_reason = "entry_gate_reject"
                elif _entry_gate == "HALF":
                    _final_gate_reason = "entry_gate_half"
                else:
                    _final_gate_reason = "allowed"
                state["flow_final_gate_reason"] = _final_gate_reason

                # PositionEngine: 최종 주문 사이즈 계산
                # ※ BIAS_LONG="LONG_BIAS", BIAS_SHORT="SHORT_BIAS" (regime_engine 상수)
                if runtime.position_engine is not None:
                    _bias_dir = 1 if res.bias == BIAS_LONG else (-1 if res.bias == BIAS_SHORT else 0)
                    if _bias_dir != 0:
                        # ── 장전 보정: regime weight 에 bias_adj 가산 ──────────
                        #    방향 확정 금지 — 보정 후 가중치가 0 이하로 내려가도 최소 0.05 보장
                        if _bias_dir == 1:
                            _regime_w = max(0.05,
                                float(state.get("flow_long_weight", 0.5)) + _pm_long_adj)
                        else:
                            _regime_w = max(0.05,
                                float(state.get("flow_short_weight", 0.5)) + _pm_short_adj)
                        _pos_res = runtime.position_engine.compute(
                            base_size=_pm_size_adj * _oc_size_adj,  # opening size_filter 반영
                            regime_weight=_regime_w,
                            weight_adjust_filtered=_wa_scaled,  # 영향도 스케일 적용
                            direction=_bias_dir,
                        )
                        state["flow_order_size"]      = _pos_res.order_size
                        state["flow_size_before_llm"] = _pos_res.size_before_llm
                    else:
                        state["flow_order_size"]      = 0.0
                        state["flow_size_before_llm"] = 0.0

                # ── 실주문 실행 배선 ───────────────────────────────────────────
                # execution_mode="OFF"     → 건너뜀 (데이터 수집 전용)
                # execution_mode="VIRTUAL" → 내부 sim_engine만, KIS API 없음
                # execution_mode="PAPER"   → KIS 모의계좌 주문 (ORDER_ENV=virtual)
                # execution_mode="LIVE"    → KIS 실계좌 주문 (EXECUTION_ENABLED=true 필요)
                _exec_mode = str(state.get("execution_mode", "OFF")).upper()
                if (data_gate_ok
                        and runtime.order_state is not None
                        and not _live_executor.is_active
                        and _exec_mode in ("PAPER", "LIVE")):
                    # TP/SL 체크 — 신호 판단보다 먼저 실행 (가격 기반 청산 우선)
                    _live_executor.check_tp_sl(float(raw["futures_price"]))

                    # opening_char size_filter=skip / 이상징후 차단기 / entry_gate=REJECT → 진입 차단
                    _gated_signal = (
                        "WAIT"
                        if (_oc_sf == "skip" or _cb_reason or _entry_gate == "REJECT")
                        else res.entry_signal
                    )
                    # flow_order_size: PositionEngine 출력 → 최소 1계약 보정
                    _raw_size  = float(state.get("flow_order_size", 0.0))
                    _order_qty = max(1, round(_raw_size)) if _raw_size >= 0.5 else 0
                    _live_pos  = runtime.order_state.get_position()

                    if _live_pos.is_flat():
                        # ── 포지션 없음 → 진입 조건 확인 ─────────────────────
                        if _gated_signal == "LONG_READY" and _order_qty > 0:
                            _live_executor.try_entry("LONG",  _order_qty)
                        elif _gated_signal == "SHORT_READY" and _order_qty > 0:
                            _live_executor.try_entry("SHORT", _order_qty)
                    else:
                        # ── 포지션 있음 → 청산 조건 확인 ─────────────────────
                        # 신호 반전 또는 WAIT → 청산
                        _should_exit = (
                            _gated_signal == "WAIT"
                            or (_live_pos.direction ==  1 and _gated_signal == "SHORT_READY")
                            or (_live_pos.direction == -1 and _gated_signal == "LONG_READY")
                        )
                        if _should_exit:
                            _live_executor.try_exit()

                # ── 판단 근거 완전 기록: 모든 state 갱신 완료 후 insert_tick ──
                if data_gate_ok:
                    # session_phase: ts에서 계산
                    _hhmm = state["timestamp"][11:16]
                    if   _hhmm < "09:30": state["session_phase"] = "opening"
                    elif _hhmm < "14:30": state["session_phase"] = "mid"
                    else:                 state["session_phase"] = "closing"
                    # mode_type: simulation_mode 그대로
                    state["mode_type"] = str(state.get("simulation_mode", "expert")).lower()
                    # LLM filter 메타 (call_id, 판단 생성 시각)
                    if runtime.llm_filter is not None:
                        _llm_meta = runtime.llm_filter.get_latest_meta()
                        state["llm_filter_ts"]  = _llm_meta.get("created_at")
                        state["llm_call_id"]    = _llm_meta.get("llm_call_id")
                    runtime.recorder.insert_tick(state["timestamp"], res, state)
                    runtime.recorder.flush_tick_batch()

                if runtime.sim_engine:
                    # close_event 감지용: on_tick 전 포지션 상태 저장
                    _prev_sim_has_pos    = bool(state.get("sim_has_pos", False))
                    _prev_sim_direction  = int(state.get("sim_direction", 0))
                    _prev_sim_entry_px   = float(state.get("sim_entry_price", 0.0))
                    _prev_sim_extreme_px = float(state.get("sim_extreme_price", 0.0))

                    # data_gate_ok / 이상징후 차단기 / entry_gate=REJECT 모두 적용
                    gated_entry = (
                        res.entry_signal
                        if (data_gate_ok and not _cb_reason and _entry_gate != "REJECT")
                        else "WAIT"
                    )
                    runtime.sim_engine.on_tick(
                        state["timestamp"],
                        float(raw["futures_price"]),
                        res.bias,
                        gated_entry,
                        res.long_score,
                        res.short_score,
                        snap=dict(state),
                        data_gate_ok=data_gate_ok,
                    )
                    sim_st = runtime.sim_engine.get_state()
                    state.update({
                        "sim_has_pos": sim_st["has_position"],
                        "sim_direction": sim_st["direction"],
                        "sim_entry_price": sim_st["entry_price"],
                        "sim_tp_price": sim_st["tp_price"],
                        "sim_sl_price": sim_st["sl_price"],
                        "sim_trailing_active": sim_st["trailing_active"],
                        "sim_extreme_price": sim_st["extreme_price"],
                        "sim_cum_equity": sim_st["cum_equity"],
                    })

                    # JudgmentLog: sim 거래 종료 감지 → close_event
                    if (_prev_sim_has_pos
                            and not sim_st["has_position"]
                            and _prev_sim_direction != 0
                            and _prev_sim_entry_px > 0):
                        _j_id_close = state.get("active_judgment_event_id")
                        if _j_id_close and getattr(runtime, "judgment_log", None):
                            try:
                                _cur_price = float(raw.get("futures_price", 0.0))
                                _pnl_pt    = (_cur_price - _prev_sim_entry_px) * _prev_sim_direction
                                # MFE: 극가격 기준 최대 유리 추정
                                _mfe = abs(_prev_sim_extreme_px - _prev_sim_entry_px) if _prev_sim_extreme_px > 0 else 0.0
                                # exit_reason: TP/SL/SIGNAL 추정
                                _tp = float(state.get("sim_tp_price", 0.0))
                                _sl = float(state.get("sim_sl_price", 0.0))
                                _exit_r = "UNKNOWN"
                                if _tp > 0 and abs(_cur_price - _tp) < 0.3:
                                    _exit_r = "TP"
                                elif _sl > 0 and abs(_cur_price - _sl) < 0.3:
                                    _exit_r = "SL"
                                elif gated_entry in ("LONG_READY", "SHORT_READY"):
                                    _exit_r = "SIGNAL"
                                runtime.judgment_log.close_event(
                                    event_id=_j_id_close,
                                    entry_price=_prev_sim_entry_px,
                                    exit_price=_cur_price,
                                    direction=_prev_sim_direction,
                                    exit_reason=_exit_r,
                                    mfe_pt=round(_mfe, 4),
                                )
                                state["active_judgment_event_id"] = None
                            except Exception as _je:
                                log.warning("JudgmentLog close_event 실패 (계속): %s", _je)

            if data_gate_ok:
                # data_gate 카테고리 복구
                runtime.error_tracker.record("", "data_gate")
                # 이전 틱에서 발생했던 일반 엔진 예외도 함께 복구
                if _engine_err_cat:
                    runtime.error_tracker.record("", _engine_err_cat)
                    _engine_err_cat = ""
        except Exception as e:
            runtime.log.exception("엔진 오류: %s", e)
            _engine_err_cat = "engine_error"
            runtime.error_tracker.record(str(e), "engine_error")
        time.sleep(st["LOOP_INTERVAL_SEC"])


# ─── 세션 자동 종료 시간표 ───────────────────────────────────────────────────
# (HHMM 정수 비교 — session_detect._hhmm 과 동일 방식)
_SESSION_SHUTDOWN = {
    "day":   1545,  # 15:45 데이장 종료
    # "night": 600,  # 야간장 비활성화
}
# 종료 N분 전 실계좌 포지션 강제 청산 시각
_SESSION_FORCE_CLOSE = {
    "day":   1540,  # 15:40 — 5분 버퍼 (KRX 선물 최종 주문 가능 시각 확보)
    # "night": 555,
}
_FORCE_CLOSE_TIMEOUT_SEC = 35  # 강제 청산 체결 대기 최대 초 (30s 주문 + 5s 여유)
_SHUTDOWN_GRACE_SEC      = 10  # 종료 직전 대기
_WATCHDOG_INTERVAL       = 30  # 감시 주기 (초)


def _force_close_live_position(runtime: AppRuntime) -> None:
    """
    세션 종료 전 실계좌 포지션 강제 청산.

    - execute_exit() 직접 블로킹 호출 (최대 _FORCE_CLOSE_TIMEOUT_SEC)
    - EXECUTION_ENABLED=false 이면 경고 로그만 (실주문 없음)
    - order_state 없거나 포지션 FLAT 이면 즉시 반환
    """
    if not _ORDER_EXECUTOR_AVAILABLE:
        return
    if runtime.order_state is None:
        return

    pos = runtime.order_state.get_position()
    if pos.is_flat():
        runtime.log.info("SessionWatchdog: 실계좌 포지션 없음 — 강제 청산 불필요")
        return

    if not runtime.settings.get("EXECUTION_ENABLED", False):
        runtime.log.warning(
            "SessionWatchdog: ⚠ 포지션 미결 (dir=%+d qty=%d entry=%.2f)"
            " — EXECUTION_ENABLED=false, 강제 청산 미실행 (수동 처리 필요)",
            pos.direction, pos.qty, pos.entry_price,
        )
        return

    runtime.log.warning(
        "SessionWatchdog: 강제 청산 시작 | dir=%+d qty=%d entry=%.2f env=%s",
        pos.direction, pos.qty, pos.entry_price,
        runtime.settings.get("ORDER_ENV", "?"),
    )

    try:
        result = execute_exit(runtime, qty=0, timeout=_FORCE_CLOSE_TIMEOUT_SEC)

        if result.ok:
            runtime.log.info(
                "SessionWatchdog: 강제 청산 체결 ✓ | fill=%.2f qty=%d ord=%s",
                result.fill_price, result.fill_qty, result.ord_no,
            )
        elif result.cancelled:
            runtime.log.error(
                "SessionWatchdog: 강제 청산 취소됨 | ord=%s"
                " — 포지션 미결 상태로 프로세스 종료 ⚠",
                result.ord_no,
            )
        elif result.refused:
            runtime.log.error(
                "SessionWatchdog: 강제 청산 KIS 거부 | %s"
                " — 포지션 미결 상태로 프로세스 종료 ⚠",
                result.error,
            )
        else:
            runtime.log.error(
                "SessionWatchdog: 강제 청산 실패 | %s"
                " — 포지션 미결 상태로 프로세스 종료 ⚠",
                result.error,
            )
    except Exception as e:
        runtime.log.error("SessionWatchdog: 강제 청산 예외 | %s ⚠", e)


def session_watchdog_loop(runtime: AppRuntime) -> None:
    """
    세션 종료를 감시해 실계좌 강제 청산 후 프로세스를 종료한다.

    흐름:
      _SESSION_FORCE_CLOSE 시각 도달
        → _force_close_live_position() (블로킹, 최대 35s)
      _SESSION_SHUTDOWN 시각 도달
        → _SHUTDOWN_GRACE_SEC 대기 → os._exit(0)

    주의:
      - hm >= 비교: 30s 감시 주기로 정각을 놓쳐도 다음 틱에서 처리
      - 강제 청산은 _force_closed 플래그로 1회만 실행
    """
    session = runtime.settings.get("MMEAN_SESSION", "day")
    shutdown_hhmm    = _SESSION_SHUTDOWN.get(session)
    force_close_hhmm = _SESSION_FORCE_CLOSE.get(session)

    if shutdown_hhmm is None:
        runtime.log.warning("SessionWatchdog: 알 수 없는 세션 '%s' — 자동종료 비활성화", session)
        return

    shutdown_str = f"{shutdown_hhmm // 100:02d}:{shutdown_hhmm % 100:02d}"
    fc_str = (
        f"{force_close_hhmm // 100:02d}:{force_close_hhmm % 100:02d}"
        if force_close_hhmm else "없음"
    )
    runtime.log.info(
        "SessionWatchdog 시작 | 세션=%s | 강제청산=%s | 자동종료=%s | 감시주기=%ds",
        session.upper(), fc_str, shutdown_str, _WATCHDOG_INTERVAL,
    )

    _force_closed = False   # 강제 청산 1회 보장 플래그

    while True:
        time.sleep(_WATCHDOG_INTERVAL)
        now = datetime.now()
        hm  = now.hour * 100 + now.minute

        # ── 강제 청산 시각 도달 ─────────────────────────────────────────────
        if (force_close_hhmm is not None
                and hm >= force_close_hhmm
                and not _force_closed):
            _force_closed = True
            runtime.log.warning(
                "SessionWatchdog: 강제 청산 시각 도달 (%s) — 실계좌 포지션 청산 시도",
                now.strftime("%H:%M"),
            )
            _force_close_live_position(runtime)   # 블로킹 (최대 35s)

        # ── 종료 시각 도달 ──────────────────────────────────────────────────
        if hm >= shutdown_hhmm:
            runtime.log.warning(
                "SessionWatchdog: %s 세션 종료 시각 도달 (%s) — %d초 후 프로세스 종료",
                session.upper(), now.strftime("%H:%M"), _SHUTDOWN_GRACE_SEC,
            )
            time.sleep(_SHUTDOWN_GRACE_SEC)
            # AdoptionViewAdapter config 복원 (다음 장 오염 방지)
            _adapter = getattr(runtime, "adoption_view_adapter", None)
            if _adapter is not None:
                try:
                    _adapter.reset_to_default()
                except Exception:
                    pass
            runtime.log.info("SessionWatchdog: 프로세스 종료 (os._exit)")
            os._exit(0)


def start_runtime(runtime: AppRuntime) -> None:
    runtime.error_tracker = ErrorTracker(runtime)
    runtime.rt_client = KISRealtimeClient(runtime, runtime.settings["WS_TR_KEY"])
    runtime.log.info("MMEAN 가동")

    # ── 항목 1: 부팅 시 포지션 동기화 (get_balance → sync_from_balance) ──
    # ORDER_CANO·ORDER_KEY 설정 + order_state 있을 때만 실행
    if is_live_ready(runtime) and runtime.order_state is not None:
        _sync_position_from_rest(runtime)
    if is_live_ready(runtime) and runtime.settings["DATA_SOURCE"] in ("live", "live_ws"):
        threading.Thread(target=lambda: asyncio.run(runtime.rt_client._run()), daemon=True, name="MMEAN-WS").start()
    if runtime.settings["INVESTOR_ENABLED"] and is_live_ready(runtime):
        threading.Thread(target=fetch_investor_data, args=(runtime,), daemon=True, name="MMEAN-Investor").start()
    threading.Thread(target=refresh_today_stats_loop, args=(runtime,), daemon=True, name="MMEAN-TodayStats").start()
    if runtime.llm_filter is not None:
        runtime.llm_filter.start()
    threading.Thread(target=engine_loop, args=(runtime,), daemon=True, name="MMEAN-EngineLoop").start()
    threading.Thread(target=session_watchdog_loop, args=(runtime,), daemon=True, name="MMEAN-Watchdog").start()
