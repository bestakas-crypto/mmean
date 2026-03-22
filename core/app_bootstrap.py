# MMEAN/app_bootstrap.py
from __future__ import annotations

import atexit
import logging
import logging.handlers
import os
import sqlite3
import subprocess
import sys
import threading
import time
from typing import Any, Dict

from dotenv import load_dotenv
from flask import Flask

from app_state import AppRuntime, create_app_state
from config_manager import ConfigManager, DEFAULT_CONFIG
from db_setup import setup_all as db_setup_all
from night_regime_engine import NightRegimeEngine
from param_store import get_param_store
from prompt_manager import PromptManager, VALID_NAMES as PROMPT_VALID_NAMES
from regime_engine import BiasRegimeEngine
from regime_recorder import RegimeRecorder
from sim_engine import SimEngine

try:
    from llm_chain import LLMChain
    from llm_filter import LLMOpportunityFilter
    LLM_AVAILABLE = True
except ImportError:
    LLMChain = LLMOpportunityFilter = None
    LLM_AVAILABLE = False

# ── FlowEngine 파이프라인 ─────────────────────────────────────────────
from engines.foreign_flow_engine import FlowEngine as ForeignFlowEngine
from engines.regime_engine import RegimeEngine as FlowRegimeEngine
from engines.llm_gate import LLMGate
from engines.llm_ema_filter import LLMEmaFilter
from engines.llm_ttl_manager import LLMTTLManager
from engines.position_engine import PositionEngine
from logs.llm_decision_log import LLMDecisionLog

try:
    from engines.llm_caller import LLMCaller
    LLM_CALLER_AVAILABLE = True
except ImportError:
    LLMCaller = None
    LLM_CALLER_AVAILABLE = False

from engines.sim_profile_resolver import SimProfileResolver
from engines.market_analyst import MarketAnalyst
from engines.checkpoint_analyst import CheckpointAnalyst
from engines.adoption_view_adapter import AdoptionViewAdapter
from logs.judgment_log import JudgmentLog


def _terminate_old_process(pid: int, log: logging.Logger) -> None:
    """기존 프로세스를 종료 요청 → 10s 대기 → 강제 kill 순서로 처리."""
    try:
        if sys.platform == "win32":
            # 살아있는지 확인
            chk = subprocess.run(
                ["tasklist", "/FI", f"PID eq {pid}", "/NH"],
                capture_output=True, text=True,
            )
            if str(pid) not in chk.stdout:
                log.info("PID 락 | 구 프로세스 PID=%d 이미 종료됨", pid)
                return
            log.warning("PID 락 | 구 프로세스 PID=%d 종료 요청 (SIGTERM)", pid)
            subprocess.run(["taskkill", "/PID", str(pid), "/T"], capture_output=True)
            for _ in range(20):          # 최대 10s 대기 (0.5s × 20)
                time.sleep(0.5)
                r = subprocess.run(
                    ["tasklist", "/FI", f"PID eq {pid}", "/NH"],
                    capture_output=True, text=True,
                )
                if str(pid) not in r.stdout:
                    log.info("PID 락 | 구 프로세스 PID=%d 정상 종료 확인", pid)
                    return
            # 여전히 살아있으면 강제 종료
            subprocess.run(["taskkill", "/PID", str(pid), "/T", "/F"], capture_output=True)
            log.warning("PID 락 | 구 프로세스 PID=%d 강제 종료 (FORCE)", pid)
        else:
            import signal as _signal
            try:
                os.kill(pid, 0)  # 살아있는지 확인 (OSError면 없는 것)
            except OSError:
                log.info("PID 락 | 구 프로세스 PID=%d 이미 종료됨", pid)
                return
            log.warning("PID 락 | 구 프로세스 PID=%d SIGTERM 전송", pid)
            os.kill(pid, _signal.SIGTERM)
            for _ in range(20):
                time.sleep(0.5)
                try:
                    os.kill(pid, 0)
                except OSError:
                    log.info("PID 락 | 구 프로세스 PID=%d 정상 종료 확인", pid)
                    return
            os.kill(pid, _signal.SIGKILL)
            log.warning("PID 락 | 구 프로세스 PID=%d SIGKILL 전송", pid)
    except Exception as e:
        log.info("PID 락 | 구 프로세스 PID=%d 처리 중 예외 (무시): %s", pid, e)


def _acquire_pid_lock(base_dir: str, log: logging.Logger) -> None:
    """단일 인스턴스 보장.
    - PID 파일 존재 시 구 프로세스를 종료하고 현재 PID로 갱신.
    - 프로세스 종료 시 atexit으로 PID 파일 자동 삭제.
    """
    pid_path = os.path.join(base_dir, "storage", "mmean.pid")
    my_pid = os.getpid()

    if os.path.exists(pid_path):
        try:
            old_pid = int(open(pid_path).read().strip())
        except (ValueError, OSError):
            old_pid = None
        if old_pid and old_pid != my_pid:
            _terminate_old_process(old_pid, log)

    # 현재 PID 기록
    try:
        with open(pid_path, "w") as f:
            f.write(str(my_pid))
        log.info("PID 락 획득 | pid=%d | path=%s", my_pid, pid_path)
    except OSError as e:
        log.warning("PID 락 파일 쓰기 실패 (계속 진행): %s", e)
        return

    # 정상/비정상 종료 시 PID 파일 삭제
    def _release() -> None:
        try:
            if os.path.exists(pid_path) and int(open(pid_path).read().strip()) == my_pid:
                os.remove(pid_path)
                log.info("PID 락 해제 | pid=%d", my_pid)
        except Exception:
            pass
    atexit.register(_release)


def load_settings(base_dir: str) -> Dict[str, Any]:
    env_public_path = os.path.join(base_dir, ".env.public")
    env_secrets_path = os.path.join(base_dir, ".env.secrets")
    if os.path.exists(env_public_path):
        load_dotenv(env_public_path, override=False)
    if os.path.exists(env_secrets_path):
        load_dotenv(env_secrets_path, override=True)

    env = os.getenv("KIS_ENV", "live").lower()
    settings = {
        "ENV": env,
        "APP_KEY": os.getenv("KIS_APP_KEY", "").strip(),
        "APP_SECRET": os.getenv("KIS_APP_SECRET", "").strip(),
        "PORT": int(os.getenv("MMEAN_PORT", "5001")),
        "DATA_SOURCE": os.getenv("MMEAN_DATA_SOURCE", "live_ws").lower(),
        "LOOP_INTERVAL_SEC": float(os.getenv("MMEAN_LOOP_INTERVAL_SEC", "0.5")),
        "EMA_ALPHA": float(os.getenv("MMEAN_EMA_ALPHA", "0.18")),
        "SLOPE_WINDOW": int(os.getenv("MMEAN_SLOPE_WINDOW", "14")),
        "VOLUME_WINDOW": int(os.getenv("MMEAN_VOLUME_WINDOW", "20")),
        "LOG_LEVEL": os.getenv("MMEAN_LOG_LEVEL", "INFO").upper(),
        "TREND_EMA_FAST": int(os.getenv("MMEAN_TREND_EMA_FAST", "20")),
        "TREND_EMA_SLOW": int(os.getenv("MMEAN_TREND_EMA_SLOW", "60")),
        "TREND_SLOPE_WIN": int(os.getenv("MMEAN_TREND_SLOPE_WIN", "5")),
        "ADX_PERIOD": int(os.getenv("MMEAN_ADX_PERIOD", "14")),
        "ATR_PERIOD": int(os.getenv("MMEAN_ATR_PERIOD", "14")),
        "FUTURES_CODE": os.getenv("MMEAN_FUTURES_CODE", "A01606"),
        "WS_TR_KEY": os.getenv("MMEAN_WS_TR_KEY", os.getenv("MMEAN_FUTURES_CODE", "A01606")),
        "SPOT_CODE": os.getenv("MMEAN_SPOT_CODE", "2001"),
        "MMEAN_SESSION": __import__("session_detect").get_session(
            env_override=os.getenv("MMEAN_SESSION"),
            strict=False,
        ),
        "WS_URL": os.getenv(
            "MMEAN_WS_URL",
            "ws://ops.koreainvestment.com:21000" if env == "live" else "ws://ops.koreainvestment.com:31000",
        ),
        "WS_RECONNECT_SEC": float(os.getenv("MMEAN_WS_RECONNECT_SEC", "3.0")),
        "INVESTOR_ENABLED": os.getenv("MMEAN_USE_INVESTOR", "0") == "1",
        "INVESTOR_POLL_SEC": float(os.getenv("MMEAN_INVESTOR_POLL_SEC", "60")),
        "INVESTOR_MARKET_CODE": os.getenv("MMEAN_INVESTOR_MARKET_CODE", "K2I"),
        "INVESTOR_SUB_CODE": os.getenv("MMEAN_INVESTOR_SUB_CODE", "F001"),
        "FOREIGN_SCALE_DIV": float(os.getenv("MMEAN_FOREIGN_SCALE_DIV", "10.0")),
        "FOREIGN_CUM_WEIGHT": float(os.getenv("MMEAN_FOREIGN_CUM_WEIGHT", "0.4")),
        "FOREIGN_DELTA_WEIGHT": float(os.getenv("MMEAN_FOREIGN_DELTA_WEIGHT", "0.6")),
        "FOREIGN_SIGNAL_MODE": os.getenv("MMEAN_FOREIGN_SIGNAL_MODE", "composite").strip().lower(),
        "LLM_DAILY_LOSS_LIMIT": float(os.getenv("LLM_DAILY_LOSS_LIMIT", "-1000000")),
        "SIM_ENABLED": os.getenv("MMEAN_SIM_ENABLED", "1") == "1",
        # ── 실주문 실행 게이트 ────────────────────────────────────────────
        "EXECUTION_ENABLED": os.getenv("MMEAN_EXECUTION_ENABLED", "false").lower() == "true",
        # ── 듀얼 인증 — 주문 계좌 ────────────────────────────────────────
        "ORDER_ENV":    os.getenv("KIS_ORDER_ENV", "virtual").lower(),
        "ORDER_KEY":    os.getenv("KIS_ORDER_KEY",    os.getenv("KIS_APP_KEY", "")).strip(),
        "ORDER_SECRET": os.getenv("KIS_ORDER_SECRET", os.getenv("KIS_APP_SECRET", "")).strip(),
        "ORDER_CANO":   os.getenv("KIS_ORDER_CANO",   "").strip(),
        "HTS_ID":       os.getenv("KIS_HTS_ID",       "").strip(),
        "LIVE_PASSWORD": os.getenv("MMEAN_LIVE_PASSWORD", "").strip(),
        # ── 크래시 복구 / 무인 서버 ──────────────────────────────────────
        "CRASH_RECOVERY": os.getenv("MMEAN_CRASH_RECOVERY", "close").lower(),
        "LIVE_AUTOSTART": os.getenv("MMEAN_LIVE_AUTOSTART", "false").lower() == "true",
        # ── 실주문 TP/SL ─────────────────────────────────────────────────
        "LIVE_TP_TICKS":          int(os.getenv("MMEAN_LIVE_TP_TICKS",          "16")),
        "LIVE_SL_TICKS":          int(os.getenv("MMEAN_LIVE_SL_TICKS",          "10")),
        "LIVE_TRAILING_TICKS":    int(os.getenv("MMEAN_LIVE_TRAILING_TICKS",    "6")),
        "LIVE_TRAILING_ACTIVATE": int(os.getenv("MMEAN_LIVE_TRAILING_ACTIVATE", "8")),
    }

    sc = DEFAULT_CONFIG
    settings.update({
        "SIM_TP_TICKS": int(os.getenv("MMEAN_SIM_TP_TICKS", str(sc["sim_tp_ticks"]))),
        "SIM_SL_TICKS": int(os.getenv("MMEAN_SIM_SL_TICKS", str(sc["sim_sl_ticks"]))),
        "SIM_TRAILING_TICKS": int(os.getenv("MMEAN_SIM_TRAILING_TICKS", str(sc["sim_trailing_ticks"]))),
        "SIM_TRAILING_ACTIVATE": int(os.getenv("MMEAN_SIM_TRAILING_ACTIVATE", str(sc["sim_trailing_activate"]))),
        "SIM_NEUTRAL_EXIT_TICKS": int(os.getenv("MMEAN_SIM_NEUTRAL_EXIT_TICKS", str(sc["sim_neutral_exit_ticks"]))),
        "SIM_PROFIT_PROTECT_TICKS": int(os.getenv("MMEAN_SIM_PROFIT_PROTECT", str(sc["sim_profit_protect"]))),
        "SIM_SLIPPAGE_TICKS": int(os.getenv("MMEAN_SIM_SLIPPAGE_TICKS", str(sc["sim_slippage_ticks"]))),
        "SIM_FORCE_EXIT_HOUR": int(os.getenv("MMEAN_SIM_FORCE_EXIT_HOUR", "14")),
        "SIM_FORCE_EXIT_MIN": int(os.getenv("MMEAN_SIM_FORCE_EXIT_MIN", "45")),
    })
    return settings


def setup_logging(base_dir: str, log_level: str) -> logging.Logger:
    fmt_str = "%(asctime)s [%(levelname)s] %(name)s - %(message)s"
    level = getattr(logging, log_level, logging.INFO)
    logging.basicConfig(level=level, format=fmt_str)
    # werkzeug HTTP 접근 로그(200 OK)를 억제 — /api/status 폴링이 매초 찍히는 노이즈 방지
    logging.getLogger("werkzeug").setLevel(logging.WARNING)
    logger = logging.getLogger("MMEAN")
    log_dir = os.path.join(base_dir, "logs")
    os.makedirs(log_dir, exist_ok=True)
    log_file = os.path.join(log_dir, "mmean.log")
    handler = logging.handlers.TimedRotatingFileHandler(
        log_file,
        when="midnight",
        interval=1,
        backupCount=14,
        encoding="utf-8",
    )
    handler.setFormatter(logging.Formatter(fmt_str))
    if not any(isinstance(h, logging.handlers.TimedRotatingFileHandler) for h in logger.handlers):
        logger.addHandler(handler)
    return logger


def apply_runtime_config(runtime: AppRuntime) -> None:
    c = runtime.cfg_mgr.get()
    state = runtime.state

    runtime.night_engine.MA_GAP_STRONG = c["night_ma_gap_strong"]
    runtime.night_engine.MA_GAP_WEAK = c["night_ma_gap_weak"]
    runtime.night_engine.VOL_HIGH = c["night_vol_high"]
    runtime.night_engine.VOL_LOW = c["night_vol_low"]
    runtime.night_engine.SCORE_BULL = c["night_score_bull"]
    runtime.night_engine.SCORE_BEAR = c["night_score_bear"]

    tc = runtime.regime_engine.cfg
    tc.foreign_strong = c["foreign_strong"]
    tc.foreign_mid = c["foreign_mid"]
    tc.foreign_weak = c["foreign_weak"]
    tc.oi_strong = c["oi_strong"]
    tc.oi_mid = c["oi_mid"]
    tc.oi_negative_strong = c["oi_negative_strong"]
    tc.oi_negative_mid = c["oi_negative_mid"]
    tc.basis_strong = c["basis_strong"]
    tc.basis_mid = c["basis_mid"]
    tc.basis_weak = c["basis_weak"]
    tc.ema_delta_strong = c["ema_delta_strong"]
    tc.slope_long_ready = c["slope_long_ready"]
    tc.slope_short_ready = c["slope_short_ready"]
    tc.volume_burst_ready = c["volume_burst_ready"]
    tc.enter_score = c["enter_score"]
    tc.enter_gap = c["enter_gap"]
    tc.enter_confirm_count = int(c["enter_confirm_count"])
    tc.hold_score = c["hold_score"]
    tc.hold_gap = c["hold_gap"]
    tc.exit_fail_count = int(c["exit_fail_count"])
    tc.entry_confirm_count = int(c["entry_confirm_count"])
    tc.trade_strength_strong = c["trade_strength_strong"]
    tc.trade_strength_mid = c["trade_strength_mid"]
    tc.trend_ema_align_score = c["trend_ema_align_score"]
    tc.trend_ema_slope_score = c["trend_ema_slope_score"]
    tc.trend_ema_slope_threshold = c["trend_ema_slope_threshold"]
    tc.trend_weight_long = c["trend_weight_long"]
    tc.trend_weight_short = c["trend_weight_short"]

    if runtime.sim_engine:
        runtime.sim_engine.tp_ticks = int(c["sim_tp_ticks"])
        runtime.sim_engine.sl_ticks = int(c["sim_sl_ticks"])
        runtime.sim_engine.trailing_ticks = int(c["sim_trailing_ticks"])
        runtime.sim_engine.trailing_activate_ticks = int(c["sim_trailing_activate"])
        runtime.sim_engine.neutral_exit_ticks = int(c["sim_neutral_exit_ticks"])
        runtime.sim_engine.profit_protect_ticks = int(c["sim_profit_protect"])
        runtime.sim_engine.slippage_pt = int(c["sim_slippage_ticks"]) * 0.05

    # ── Easy Mode: SimProfileResolver → 메모리 반영 (cfg_mgr 기록 없음) ──
    # 1순위: cfg_mgr.update() 호출 금지 — Easy 설정은 런타임 메모리에만 적용
    # 2순위: resolve 기준 = DEFAULT_CONFIG + fixed + level_json  (수동 config 혼입 없음)
    # 4순위: resolved_config 기준으로 night_engine / regime_engine / sim_engine 모두 재반영
    if runtime.sim_profile_resolver is not None:
        _sim_mode = str(state.get("simulation_mode", "expert")).lower()
        _sel_lvl  = state.get("selected_level")
        if _sim_mode == "easy":
            _result = runtime.sim_profile_resolver.resolve(
                mode=_sim_mode,
                selected_level=_sel_lvl,
                current_config=c,   # Easy mode는 resolver 내부에서 무시; Expert mode용
            )
            _rc = _result["resolved_config"]     # DEFAULT_CONFIG + fixed + level (수동 config 미혼입)

            def _v(key):
                """resolved_config 우선, 없으면 현재 cfg_mgr 값으로 방어 폴백."""
                return _rc.get(key, c[key])

            # night_engine
            runtime.night_engine.MA_GAP_STRONG = _v("night_ma_gap_strong")
            runtime.night_engine.MA_GAP_WEAK   = _v("night_ma_gap_weak")
            runtime.night_engine.VOL_HIGH       = _v("night_vol_high")
            runtime.night_engine.VOL_LOW        = _v("night_vol_low")
            runtime.night_engine.SCORE_BULL     = _v("night_score_bull")
            runtime.night_engine.SCORE_BEAR     = _v("night_score_bear")

            # regime_engine
            _tc = runtime.regime_engine.cfg
            _tc.foreign_strong            = _v("foreign_strong")
            _tc.foreign_mid               = _v("foreign_mid")
            _tc.foreign_weak              = _v("foreign_weak")
            _tc.oi_strong                 = _v("oi_strong")
            _tc.oi_mid                    = _v("oi_mid")
            _tc.oi_negative_strong        = _v("oi_negative_strong")
            _tc.oi_negative_mid           = _v("oi_negative_mid")
            _tc.basis_strong              = _v("basis_strong")
            _tc.basis_mid                 = _v("basis_mid")
            _tc.basis_weak                = _v("basis_weak")
            _tc.ema_delta_strong          = _v("ema_delta_strong")
            _tc.slope_long_ready          = _v("slope_long_ready")
            _tc.slope_short_ready         = _v("slope_short_ready")
            _tc.volume_burst_ready        = _v("volume_burst_ready")
            _tc.enter_score               = _v("enter_score")
            _tc.enter_gap                 = _v("enter_gap")
            _tc.enter_confirm_count       = int(_v("enter_confirm_count"))
            _tc.hold_score                = _v("hold_score")
            _tc.hold_gap                  = _v("hold_gap")
            _tc.exit_fail_count           = int(_v("exit_fail_count"))
            _tc.entry_confirm_count       = int(_v("entry_confirm_count"))
            _tc.trade_strength_strong     = _v("trade_strength_strong")
            _tc.trade_strength_mid        = _v("trade_strength_mid")
            _tc.trend_ema_align_score     = _v("trend_ema_align_score")
            _tc.trend_ema_slope_score     = _v("trend_ema_slope_score")
            _tc.trend_ema_slope_threshold = _v("trend_ema_slope_threshold")
            _tc.trend_weight_long         = _v("trend_weight_long")
            _tc.trend_weight_short        = _v("trend_weight_short")

            # sim_engine
            if runtime.sim_engine:
                runtime.sim_engine.tp_ticks                = int(_v("sim_tp_ticks"))
                runtime.sim_engine.sl_ticks                = int(_v("sim_sl_ticks"))
                runtime.sim_engine.trailing_ticks          = int(_v("sim_trailing_ticks"))
                runtime.sim_engine.trailing_activate_ticks = int(_v("sim_trailing_activate"))
                runtime.sim_engine.neutral_exit_ticks      = int(_v("sim_neutral_exit_ticks"))
                runtime.sim_engine.profit_protect_ticks    = int(_v("sim_profit_protect"))
                runtime.sim_engine.slippage_pt             = int(_v("sim_slippage_ticks")) * 0.05

    # ── 장전 수동 옵션 / 장중 LLM 영향도 → state 동기화 ────────────────
    # ConfigManager 에 저장된 값을 state 에도 반영하여 엔진 루프가 즉시 읽을 수 있게 함.
    _pm_mode = str(c.get("premarket_manual_mode", "NORMAL")).upper()
    if _pm_mode not in ("MANIA_2", "MANIA_1", "NORMAL", "FEAR_1", "FEAR_2"):
        _pm_mode = "NORMAL"
    _llm_inf = str(c.get("llm_intraday_influence", "MID")).upper()
    if _llm_inf not in ("LOW", "MID", "HIGH"):
        _llm_inf = "MID"
    state["premarket_manual_mode"]  = _pm_mode
    state["llm_intraday_influence"] = _llm_inf

    state["config_hash"] = runtime.cfg_mgr.get_hash()


def _boot_position_sync(runtime) -> None:
    """부팅 시 REST 잔고 → 포지션 동기화 → 크래시 복구 정책 적용."""
    log      = runtime.log
    settings = runtime.settings
    symbol   = settings.get("FUTURES_CODE", "")
    try:
        from order.kis_order_api  import get_balance
        from order.order_executor import execute_exit
    except ImportError as e:
        log.warning("order 레이어 없음 — 부팅 포지션 동기화 건너뜀: %s", e)
        return
    try:
        bal = get_balance(runtime)
        runtime.order_state.sync_from_balance(bal["positions"], symbol)
        log.info("[부팅] 포지션 동기화 완료 | symbol=%s", symbol)
    except Exception as e:
        log.warning("[부팅] 포지션 동기화 실패 (계속 진행): %s", e)
        return
    pos = runtime.order_state.get_position()
    if pos.is_flat():
        return
    policy    = settings.get("CRASH_RECOVERY", "close")
    dir_label = "LONG" if pos.direction == 1 else "SHORT"
    log.warning("[부팅] 크래시 복구: %s qty=%d entry=%.2f | 정책=%s",
                dir_label, pos.qty, pos.entry_price, policy)
    if policy == "close":
        try:
            result = execute_exit(runtime, timeout=30.0)
            if result.ok:
                log.info("[크래시 복구] 청산 완료 | fill_price=%.2f qty=%d",
                         result.fill_price, result.fill_qty)
            else:
                log.error("[크래시 복구] 청산 실패 | %s", result.error)
        except Exception as e:
            log.error("[크래시 복구] 청산 예외 | %s", e)
    else:
        log.info("[크래시 복구] 포지션 복원 — %s qty=%d entry=%.2f",
                 dir_label, pos.qty, pos.entry_price)


def build_runtime(app: Flask) -> AppRuntime:
    base_dir = os.path.dirname(os.path.abspath(__file__))
    settings = load_settings(base_dir)
    log = setup_logging(base_dir, settings["LOG_LEVEL"])

    # ─── 단일 인스턴스 보장: 기존 프로세스 종료 후 PID 락 획득 ──────────
    _acquire_pid_lock(base_dir, log)

    db_dir = os.path.join(base_dir, "storage")
    os.makedirs(db_dir, exist_ok=True)
    db_path = os.path.join(db_dir, "mmean.db")
    db_setup_all(db_path)

    state_obj = create_app_state(settings)
    recorder = RegimeRecorder(db_path)
    cfg_mgr = ConfigManager(db_path=db_path)
    param_store = get_param_store(db_path)
    regime_engine = BiasRegimeEngine(mode="smooth", history_limit=600)
    night_engine = NightRegimeEngine(
        history_limit=600,
        avg_night_vol_5d=float(os.getenv("MMEAN_NIGHT_AVG_VOL_5D", "5000")),
    )

    sim_engine = None
    if settings["SIM_ENABLED"]:
        sim_engine = SimEngine(
            db_path=db_path,
            tp_ticks=settings["SIM_TP_TICKS"],
            sl_ticks=settings["SIM_SL_TICKS"],
            trailing_ticks=settings["SIM_TRAILING_TICKS"],
            trailing_activate_ticks=settings["SIM_TRAILING_ACTIVATE"],
            neutral_exit_ticks=settings["SIM_NEUTRAL_EXIT_TICKS"],
            profit_protect_ticks=settings["SIM_PROFIT_PROTECT_TICKS"],
            slippage_ticks=settings["SIM_SLIPPAGE_TICKS"],
            force_exit_hour=settings["SIM_FORCE_EXIT_HOUR"],
            force_exit_min=settings["SIM_FORCE_EXIT_MIN"],
            recorder=recorder,
            cfg_mgr=cfg_mgr,
        )

    prompt_mgr = PromptManager(db_path=db_path)
    llm_chain = llm_filter = None
    if LLM_AVAILABLE:
        try:
            llm_chain = LLMChain(
                timeout_sec=float(os.getenv("LLM_TIMEOUT_SEC", "2")),
                connect_timeout_sec=float(os.getenv("LLM_CONNECT_TIMEOUT_SEC", "1")),
            )
            llm_filter = LLMOpportunityFilter(
                db_path=db_path,
                state_getter=lambda: dict(state_obj.state),
                llm_chain=llm_chain,
                prompt_mgr=prompt_mgr,
            )
            log.info(
                "LLM 모듈 초기화 완료 | apply=%s | filter=%s",
                os.getenv("LLM_APPLY_ENABLED", "0"),
                os.getenv("LLM_FILTER_ENABLED", "0"),
            )
        except Exception as llm_err:
            log.warning("LLM 모듈 초기화 실패: %s", llm_err)
            llm_chain = llm_filter = None

    if sim_engine and llm_filter:
        sim_engine._llm_filter = llm_filter

    # ── SimProfileResolver (Easy / Expert 2모드) ─────────────────────
    # default_config 주입: Easy mode 병합 기반을 resolver 내부에서 강제 고정
    sim_profile_resolver = SimProfileResolver(default_config=DEFAULT_CONFIG)

    # ── FlowEngine 파이프라인 인스턴스 ────────────────────────────────
    flow_engine         = ForeignFlowEngine()
    flow_regime_engine  = FlowRegimeEngine()
    llm_gate_obj        = LLMGate()
    llm_ema_filter_obj  = LLMEmaFilter()
    llm_ttl_manager_obj = LLMTTLManager()
    position_engine_obj = PositionEngine()
    llm_decision_log_obj = LLMDecisionLog(db_path)
    llm_caller_obj = None
    if LLM_CALLER_AVAILABLE and llm_chain is not None:
        llm_caller_obj = LLMCaller(
            llm_chain=llm_chain,
            decision_log=llm_decision_log_obj,
        )
    log.info(
        "FlowEngine 파이프라인 초기화 완료 | llm_caller=%s",
        "활성" if llm_caller_obj is not None else "비활성(llm_chain 없음)",
    )

    runtime = AppRuntime(
        app=app,
        base_dir=base_dir,
        db_path=db_path,
        log=log,
        settings=settings,
        state_obj=state_obj,
        regime_engine=regime_engine,
        night_engine=night_engine,
        recorder=recorder,
        cfg_mgr=cfg_mgr,
        sim_engine=sim_engine,
        param_store=param_store,
        prompt_mgr=prompt_mgr,
        prompt_valid_names=PROMPT_VALID_NAMES,
        llm_chain=llm_chain,
        llm_filter=llm_filter,
        flow_engine=flow_engine,
        flow_regime_engine=flow_regime_engine,
        llm_gate=llm_gate_obj,
        llm_ema_filter=llm_ema_filter_obj,
        llm_ttl_manager=llm_ttl_manager_obj,
        position_engine=position_engine_obj,
        llm_decision_log=llm_decision_log_obj,
        llm_caller=llm_caller_obj,
        sim_profile_resolver=sim_profile_resolver,
    )

    # ── JudgmentLog 초기화 ───────────────────────────────────────────
    judgment_log = JudgmentLog(db_path=db_path)
    runtime.judgment_log = judgment_log
    log.info("JudgmentLog 초기화 완료 | db=%s", db_path)

    # ── AdoptionViewAdapter 초기화 ────────────────────────────────────
    sim_db_path = os.path.join(db_dir, "sim.db")
    adoption_view_adapter = AdoptionViewAdapter(runtime, sim_db_path)
    runtime.adoption_view_adapter = adoption_view_adapter
    log.info("AdoptionViewAdapter 초기화 완료 | sim_db=%s", sim_db_path)

    # ── MarketAnalyst 초기화 및 시작 ─────────────────────────────────
    market_analyst = MarketAnalyst(runtime, interval_sec=180)
    market_analyst.start()
    runtime.market_analyst = market_analyst
    log.info("MarketAnalyst 초기화 완료 | interval=180s")

    # ── CheckpointAnalyst 초기화 및 시작 ─────────────────────────────
    checkpoint_analyst = CheckpointAnalyst(runtime)
    checkpoint_analyst.start()
    runtime.checkpoint_analyst = checkpoint_analyst
    log.info("CheckpointAnalyst 초기화 완료 | 체크포인트: 09:15 / 10:30 / 13:00")

    # ── 주문 실행 레이어 초기화 ──────────────────────────────────────────
    try:
        from order.order_state import OrderStateManager
        from order.kis_fill_ws import KISFillNoticeClient
        order_state_obj = OrderStateManager()
        runtime.order_state = order_state_obj
        log.info("OrderStateManager 초기화 완료")
        if settings.get("HTS_ID"):
            try:
                fill_ws = KISFillNoticeClient(runtime, order_state_obj)
                runtime.fill_ws_client = fill_ws
                # ※ fill_ws.start() 는 여기서 호출하지 않음
                # ※ 시작 게이트(_startup_live_gate) 통과 후 analyzer_app.py 에서 호출
                log.info("체결통보 WS 클라이언트 생성 완료 (미시작) | env=%s", settings["ORDER_ENV"])
            except Exception as _fe:
                log.warning("체결통보 WS 생성 실패 (계속 진행): %s", _fe)
        else:
            log.warning("KIS_HTS_ID 미설정 — 체결통보 WS 비활성")
    except ImportError as _oe:
        log.warning("order 레이어 임포트 실패 (주문 비활성): %s", _oe)

    # ── _boot_position_sync 는 여기서 호출하지 않음 ───────────────────────
    # ※ 시작 게이트 통과 후 analyzer_app.py 에서 호출 (비밀번호 검증 선행 보장)

    apply_runtime_config(runtime)

    # ── RAG 사전작업 백그라운드 자동 실행 ────────────────────────────
    # pattern_memory / failure_patterns 가 비어있거나 6시간 이상 오래됐으면
    # rag_prep.py step2+4 를 백그라운드 스레드로 실행.
    # 시작 지연 없이 진행, 완료/오류 로그만 남김.
    _rag_thread = threading.Thread(
        target=_run_rag_prep_if_stale,
        args=(db_path, log, runtime),
        daemon=True,
        name="rag-prep-bg",
    )
    _rag_thread.start()
    log.info("RAG 사전작업 백그라운드 스레드 시작")

    return runtime


def _run_rag_prep_if_stale(db_path: str, log: logging.Logger, runtime=None) -> None:
    """
    pattern_memory / failure_patterns 신선도 체크 후 stale 이면 rag_prep.py 실행.

    staleness 판단 기준:
      - pattern_memory 행이 0개이거나
      - pattern_memory 의 max(last_updated) 가 6시간 이상 경과한 경우

    runtime 이 전달되면 state 에 헬스 정보를 기록한다:
      flow_rag_prep_status       : idle / running / success / error
      flow_rag_prep_last_run_ts  : 마지막 실행 시작 시각
      flow_rag_prep_last_success_ts : 마지막 성공 완료 시각
      flow_rag_prep_error        : 마지막 오류 메시지 (성공 시 "")
      flow_rag_prep_pm_count     : pattern_memory 행 수 (마지막 확인 기준)
      flow_rag_prep_fp_count     : failure_patterns 행 수
    """
    from datetime import datetime as _dt

    _STALE_HOURS = 6
    _STARTUP_DELAY_SEC = 10  # 엔진 초기화 완료 대기

    def _set_state(**kwargs):
        """runtime.state 가 있으면 업데이트, 없으면 무시."""
        if runtime is not None:
            try:
                runtime.state.update(kwargs)
            except Exception:
                pass

    time.sleep(_STARTUP_DELAY_SEC)

    # 시작 시 idle 상태 기록 (pattern_memory 현황 포함)
    try:
        conn = sqlite3.connect(db_path, check_same_thread=False)
        try:
            pm_cnt  = conn.execute("SELECT COUNT(*) FROM pattern_memory").fetchone()[0]
            fp_cnt  = conn.execute("SELECT COUNT(*) FROM failure_patterns").fetchone()[0]
            mf_cnt  = conn.execute("SELECT COUNT(*) FROM market_features").fetchone()[0]
            rt_cnt  = conn.execute("SELECT COUNT(*) FROM regime_ticks").fetchone()[0]
            _set_state(
                flow_rag_prep_status    = "idle",
                flow_rag_prep_pm_count  = pm_cnt,
                flow_rag_prep_fp_count  = fp_cnt,
                flow_rag_prep_error     = "",
            )

            # 스킵 조건: pattern_memory 가 있고 + market_features 도 있고 + 최근 N시간 내 갱신
            if pm_cnt > 0 and mf_cnt > 0:
                last_upd = conn.execute(
                    "SELECT MAX(last_updated) FROM pattern_memory"
                ).fetchone()[0] or ""
                if last_upd:
                    try:
                        last_dt   = _dt.fromisoformat(last_upd[:19])
                        elapsed_h = (_dt.now() - last_dt).total_seconds() / 3600
                        if elapsed_h < _STALE_HOURS:
                            log.info(
                                "RAG prep 스킵 — pm=%d mf=%d rt=%d 최근 갱신: %.1fh 전",
                                pm_cnt, mf_cnt, rt_cnt, elapsed_h,
                            )
                            _set_state(
                                flow_rag_prep_status           = "success",
                                flow_rag_prep_last_success_ts  = last_upd[:16],
                            )
                            return
                    except Exception:
                        pass
            log.info(
                "RAG prep 실행 — pm=%d mf=%d rt=%d fp=%d",
                pm_cnt, mf_cnt, rt_cnt, fp_cnt,
            )
        finally:
            conn.close()

    except Exception as e:
        log.warning("RAG prep 신선도 체크 실패: %s", e)
        _set_state(flow_rag_prep_status="error", flow_rag_prep_error=str(e)[:120])
        return

    # rag_prep.py 경로 계산 (app_bootstrap.py 기준 ../rag/rag_prep.py)
    _this_dir   = os.path.dirname(os.path.abspath(__file__))
    _proj_dir   = os.path.dirname(_this_dir)
    _rag_script = os.path.join(_proj_dir, "rag", "rag_prep.py")
    if not os.path.exists(_rag_script):
        msg = f"rag_prep.py 없음: {_rag_script}"
        log.warning(msg + " — RAG prep 건너뜀")
        _set_state(flow_rag_prep_status="error", flow_rag_prep_error=msg)
        return

    _now_str = _dt.now().strftime("%Y-%m-%dT%H:%M:%S")
    _set_state(
        flow_rag_prep_status       = "running",
        flow_rag_prep_last_run_ts  = _now_str,
        flow_rag_prep_error        = "",
    )

    # step1: regime_ticks → market_features (step2의 전제)
    # step2: market_features + trades → pattern_memory
    # step4: judgment_events → failure_patterns
    _last_err = ""
    for step in (1, 2, 4):
        try:
            result = subprocess.run(
                [sys.executable, _rag_script, "--step", str(step), "--db", db_path],
                capture_output=True, text=True, timeout=120,
                cwd=_proj_dir,
            )
            if result.returncode == 0:
                log.info("RAG prep step%d 완료", step)
            else:
                _last_err = f"step{step} rc={result.returncode}: {result.stderr[-200:]}"
                log.warning("RAG prep step%d 실패 (rc=%d):\n%s",
                            step, result.returncode, result.stderr[-500:])
        except Exception as _se:
            _last_err = f"step{step} exception: {_se}"
            log.warning("RAG prep step%d 예외: %s", step, _se)

    # 완료 후 최종 카운트 갱신
    try:
        conn = sqlite3.connect(db_path, check_same_thread=False)
        pm_cnt = conn.execute("SELECT COUNT(*) FROM pattern_memory").fetchone()[0]
        fp_cnt = conn.execute("SELECT COUNT(*) FROM failure_patterns").fetchone()[0]
        conn.close()
    except Exception:
        pm_cnt = fp_cnt = -1

    _success_ts = _dt.now().strftime("%Y-%m-%dT%H:%M:%S")
    if _last_err:
        _set_state(
            flow_rag_prep_status    = "error",
            flow_rag_prep_error     = _last_err[:200],
            flow_rag_prep_pm_count  = pm_cnt,
            flow_rag_prep_fp_count  = fp_cnt,
        )
    else:
        _set_state(
            flow_rag_prep_status           = "success",
            flow_rag_prep_last_success_ts  = _success_ts,
            flow_rag_prep_error            = "",
            flow_rag_prep_pm_count         = pm_cnt,
            flow_rag_prep_fp_count         = fp_cnt,
        )
        log.info("RAG prep 완료 | pattern_memory=%d, failure_patterns=%d", pm_cnt, fp_cnt)

