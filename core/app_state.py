# MMEAN/app_state.py
from __future__ import annotations

import logging
import threading
from collections import deque
from dataclasses import dataclass, field
from typing import Any, Deque, Dict, Optional, Tuple


@dataclass
class AppState:
    state: Dict[str, object]
    engine_lock: threading.RLock = field(default_factory=threading.RLock)
    today_stats: Dict[str, object] = field(default_factory=dict)
    today_stats_lock: threading.Lock = field(default_factory=threading.Lock)
    token_cache: Dict[str, object] = field(default_factory=dict)
    token_lock: threading.Lock = field(default_factory=threading.Lock)
    order_token_cache: Dict[str, object] = field(default_factory=dict)
    order_token_lock: threading.Lock = field(default_factory=threading.Lock)
    ema_basis_history: Deque[Tuple[float, float]] = field(default_factory=lambda: deque(maxlen=600))
    volume_delta_history: Deque[float] = field(default_factory=deque)
    investor_cache: Dict[str, float] = field(default_factory=dict)
    ema_fast_val: float = 0.0
    ema_slow_val: float = 0.0
    ema_fast_history: Deque[Tuple[float, float]] = field(default_factory=deque)
    vwap_cum_pv: float = 0.0
    vwap_cum_vol: float = 0.0
    atr_history: Deque[float] = field(default_factory=deque)
    prev_close: float = 0.0
    foreign_prev_cum: float = 0.0
    night_last_session_date: str = ""
    # FlowEngine 시계열 — investor 폴링마다 누적값 추가 (maxlen=30: ma20+버퍼)
    foreign_net_history: Deque[float] = field(
        default_factory=lambda: deque(maxlen=30)
    )


@dataclass
class AppRuntime:
    app: Any
    base_dir: str
    db_path: str
    log: logging.Logger
    settings: Dict[str, Any]
    state_obj: AppState
    regime_engine: Any
    night_engine: Any
    recorder: Any
    cfg_mgr: Any
    sim_engine: Any
    param_store: Any
    prompt_mgr: Any
    prompt_valid_names: Any
    llm_chain: Any = None
    llm_filter: Any = None
    rt_client: Optional[Any] = None
    error_tracker: Optional[Any] = None
    # ── FlowEngine 파이프라인 ─────────────────────────────────────────
    flow_engine: Optional[Any] = None         # ForeignFlowEngine
    flow_regime_engine: Optional[Any] = None  # RegimeEngine (engines/)
    llm_gate: Optional[Any] = None            # LLMGate
    llm_ema_filter: Optional[Any] = None      # LLMEmaFilter
    llm_ttl_manager: Optional[Any] = None     # LLMTTLManager
    position_engine: Optional[Any] = None     # PositionEngine
    llm_decision_log: Optional[Any] = None    # LLMDecisionLog
    llm_caller: Optional[Any] = None          # LLMCaller
    sim_profile_resolver: Optional[Any] = None  # SimProfileResolver
    judgment_log: Optional[Any] = None          # JudgmentLog (판단 사건 단위 기록)
    market_analyst: Optional[Any] = None        # MarketAnalyst (3분 인터벌 시장 분석)
    checkpoint_analyst: Optional[Any] = None    # CheckpointAnalyst (09:15/10:30/13:00)
    adoption_view_adapter: Optional[Any] = None  # AdoptionViewAdapter (view→config 자동 교체)
    # ── 주문 실행 레이어 ─────────────────────────────────────────────────
    order_state: Optional[Any] = None         # OrderStateManager
    fill_ws_client: Optional[Any] = None      # KISFillNoticeClient
    # ── 웹훅 클라이언트 레지스트리 ───────────────────────────────────────
    webhook_clients: list = field(default_factory=list)  # List[WebhookClient]
    llm_pulse: Optional[Any] = None              # LLMPulse (5분 장세 평가, 병행 운영)
    sideways_detector: Optional[Any] = None      # SidewaysDetector (횡보 감지)
    pulse_engine: Optional[Any] = None           # PulseEngine (규칙 기반 전략 방향)
    pulse_scorer: Optional[Any] = None           # PulseScorer (자동 채점)
    pulse_validator: Optional[Any] = None        # PulseValidator (30분 LLM 검증)
    pulse_analyzer: Optional[Any] = None         # PulseAnalyzer (누적 분석)

    @property
    def state(self) -> Dict[str, object]:
        return self.state_obj.state


def create_app_state(settings: Dict[str, Any]) -> AppState:
    night_defaults = {
        "night_regime": "NEUTRAL",
        "night_entry": "WAIT",
        "night_confidence": 0.0,
        "night_ema20": 0.0,
        "night_ema60": 0.0,
        "night_ma_gap_pct": 0.0,
        "night_vol_ratio": 0.0,
        "night_orb_high": 0.0,
        "night_orb_low": 0.0,
        "night_orb_active": False,
        "night_raw_score": 0.0,
        "night_reason": "",
    }

    state = {
        "mode": "smooth",
        "data_source": settings["DATA_SOURCE"],
        "env": settings["ENV"],
        "ws_connected": False,
        "ws_first_valid_tick_received": False,  # WS 최초 유효 가격 수신 여부 (PriceGuard 앵커 설정)
        "session_type": settings["MMEAN_SESSION"],
        "bias": "NEUTRAL",
        "entry_signal": "WAIT",
        "confidence": 0.0,
        "futures_price": 0.0,
        "spot_price": 0.0,
        "foreign_buy": 0.0,
        "oi_value": 0.0,
        "oi_delta": 0.0,
        # 선물 3주체 절대값 + delta
        "fut_fgn_buy":    0.0,
        "fut_inst_buy":   0.0,
        "fut_indiv_buy":  0.0,
        "fut_fgn_delta":  0.0,
        "fut_inst_delta": 0.0,
        "fut_indiv_delta":0.0,
        # 현물 3주체 절대값 + delta
        "spot_fgn_buy":    0.0,
        "spot_inst_buy":   0.0,
        "spot_indiv_buy":  0.0,
        "spot_fgn_delta":  0.0,
        "spot_inst_delta": 0.0,
        "spot_indiv_delta":0.0,
        # 당일 레인지 추적
        "session_date":      "",
        "session_high":      0.0,
        "session_low":       999999.0,
        "price_range_pct":   0.5,
        "basis": 0.0,
        "basis_ema": 0.0,
        "basis_ema_delta": 0.0,
        "basis_slope": 0.0,
        "cum_volume": 0.0,
        "trade_qty": 0.0,
        "volume_delta": 0.0,
        "volume_burst": 0.0,
        "last_trade_strength": 0.0,
        "best_ask": 0.0,
        "best_bid": 0.0,
        "long_score": 0.0,
        "short_score": 0.0,
        "reason": "",
        "timestamp": "",
        "last_error": "",
        "config_hash": "",
        "data_quality": "initializing",
        "ema_fast": 0.0,
        "ema_slow": 0.0,
        "ema_fast_slope": 0.0,
        "trend_long_score": 0.0,
        "trend_short_score": 0.0,
        "trend_weight_long": 0.8,
        "trend_weight_short": 0.6,
        "param_store_version": 0,
        "param_store_hash": "default",
        "param_hash": "default",
        "vwap": 0.0,
        "atr": 0.0,
        "price_vs_vwap": 0.0,
        "sim_has_pos": False,
        "sim_direction": 0,
        "sim_entry_price": 0.0,
        "sim_tp_price": 0.0,
        "sim_sl_price": 0.0,
        "sim_trailing_active": False,
        "sim_extreme_price": 0.0,
        "sim_cum_equity": 0.0,
        **night_defaults,
        "today_trade_count": 0,
        "today_pnl_won": 0.0,
        "daily_loss_limit_left": float(settings["LLM_DAILY_LOSS_LIMIT"]),
        "recent_ticks": [],
        "llm_filter_score": -1.0,
        "llm_filter_direction": "UNKNOWN",
        "llm_filter_valid": False,
        # ── FlowEngine 파이프라인 출력 (대시보드·로그에서 전부 보여야 함) ──
        # FlowEngine
        "flow_score":           0.0,       # -2.0 ~ +2.0
        "flow_regime_label":    "NEUTRAL", # 모니터링 전용 — 실행 if문 사용 금지
        "flow_long_weight":     0.5,       # sigmoid 연속값 (≥0.05)
        "flow_short_weight":    0.5,       # sigmoid 연속값 (≥0.05)
        # PositionEngine
        "flow_order_size":      0.0,       # 최종 주문 사이즈
        "flow_size_before_llm": 0.0,       # LLM 개입 전 사이즈 (base × regime_weight)
        # LLM 판단 (실패 시 0/empty)
        "flow_llm_risk_view":   "",        # "positive"|"neutral"|"negative"|"failure"|""
        "flow_llm_confidence":  0.0,       # 0.0 ~ 1.0
        "flow_llm_weight_adjust": 0.0,     # EMA 필터 적용 후 최종값
        "flow_llm_call_key":    "",        # "YYYY-MM-DD HH:MM|SIGNAL" — 중복 호출 추적
        "flow_llm_error":       "",        # 마지막 LLM 오류 유형 (성공 시 "")
        "flow_llm_log_id":      0,         # LLMDecisionLog row id (사후 hit 업데이트용)
        # TTL 상태
        "flow_ttl_active":      False,     # weight_adjust TTL 활성 여부
        "flow_ttl_remaining":   0.0,       # 잔여 TTL 초
        # ── 장전 수동 옵션 ──────────────────────────────────────────────
        # MANIA_2 / MANIA_1 / NORMAL / FEAR_1 / FEAR_2
        # 사용자가 대시보드에서 직접 선택. 특수상황 리스크 보정 전용.
        "premarket_manual_mode":   "NORMAL",
        "premarket_bias_adj_long": 0.0,    # long weight 보정 계수 (계산 결과)
        "premarket_bias_adj_short":0.0,    # short weight 보정 계수 (계산 결과)
        "premarket_size_adj":      1.0,    # size 승수 보정 (0.9 ~ 1.1)
        "premarket_last_updated":  "",     # 마지막 변경 시각 (ISO str)
        # ── 장중 LLM 영향도 ────────────────────────────────────────────
        # LOW=0.5× / MID=1.0× / HIGH=1.5× — weight_adjust 반영 계수만 조정
        "llm_intraday_influence":  "MID",
        # ── 외국인 신호 모드 ──────────────────────────────────────────
        # composite: 누적(40%) + 틱 변화량(60%) 합성  /  delta_only: 틱 변화량 단독
        "foreign_signal_mode":     "composite",
        # ── LLM 신호 이력 ─────────────────────────────────────────────
        # 최근 5회 LLM 호출 결과: [{action, model, ts, risk_view, confidence}]
        "llm_signal_history":      [],
        "llm_last_model":          "",   # 마지막 성공 공급자 이름 (Claude/GPT/Gemini)
        # ── JudgmentLog (판단 사건 단위 기록) ─────────────────────────────
        "active_judgment_event_id": None,   # 현재 열린 판단 사건 id (청산 시 None)
        # ── SidewaysDetector ───────────────────────────────────────────
        "sideways_active":  False,  # 횡보 하드 블록 활성 여부
        "sideways_reason":  "",     # 현재 횡보 상태 요약
        "sideways_lower":   0.0,    # 레인지 하단
        "sideways_upper":   0.0,    # 레인지 상단
        # ── MarketAnalyst (3분 인터벌 시장 분석) ──────────────────────────
        # view: TREND_UP|TREND_DOWN|VOLATILE|CHOPPY|SPIKE_REVERSAL|REVERSAL_UP|REVERSAL_DOWN
        "market_view":             None,   # dict | None
        "market_view_ts":          "",     # 마지막 갱신 시각 HH:MM:SS
        "market_view_history":     [],     # 최근 3개 view 이력
        # ── CheckpointAnalyst (09:15 / 10:30 / 13:00 구조 분석) ───────────
        # opening_char: {char, size_filter, reason, ts}
        #   char: opening_strength|opening_fade|initial_chop|
        #         early_imbalance_up|early_imbalance_down
        #   size_filter: normal|reduce|skip
        "opening_char":            None,
        # provisional_regime: {regime, direction, confidence, reason, ts}
        #   regime: trend_continuation|reversal|balance
        #   direction: LONG_BIAS|SHORT_BIAS|NEUTRAL
        #   confidence: HIGH|MID|LOW
        "provisional_regime":      None,
        # afternoon_regime: {regime, late_bias, risk_adj, reason, ts}
        #   regime: trend_alive|trend_dead|reversal|box
        #   late_bias: LONG_BIAS|SHORT_BIAS|NEUTRAL
        #   risk_adj: normal|conservative|aggressive
        "afternoon_regime":        None,
        # ── AdoptionViewAdapter (view→config 자동 교체) ───────────────────
        "adoption_view_active_id":  None,  # 현재 적용 중인 adoption id
        "adoption_view_active_pat": None,  # 현재 적용 중인 train_pattern
        "adoption_view_fallback":   True,  # True=adoption 없어서 기본 config 사용
        # ── 시뮬레이션 모드 (Easy / Expert) ────────────────────────────
        # simulation_mode: "easy" | "expert"
        #   easy   → selected_level(1~20) 기반으로 level JSON + fixed 병합 실행
        #            개별 옵션 패널 입력 무시
        #   expert → 기존 ConfigManager 개별 옵션 그대로 사용
        # selected_level: 이지모드 선택 레벨 번호 (1~20); expert 시 None
        "simulation_mode":         "expert",
        "selected_level":          None,
        # ── 실행 모드 ────────────────────────────────────────────────────
        # ── 실행 모드 플래그 (AND 구조 — 독립 토글, 복수 동시 활성 가능) ──────
        # exec_data  : 데이터 수집 (항상 활성, UI 표시 전용)
        # exec_sim   : 내부 시뮬레이션 (sim_engine 가상 주문 기록)
        # exec_paper : KIS 모의계좌 주문 (ORDER_ENV=virtual)
        # exec_live  : KIS 실계좌 주문 (EXECUTION_ENABLED=true + 비밀번호 필요)
        "exec_data":               True,
        "exec_sim":                True,
        "exec_paper":              False,
        "exec_live":               False,
        # ── 실주문 최근 체결 정보 ─────────────────────────────────────────
        "live_last_ord_no":        "",
        "live_last_side":          "",
        "live_last_fill_price":    0.0,
        "live_last_fill_qty":      0,
        "live_last_ok":            False,
        "live_last_error":         "",
    }

    return AppState(
        state=state,
        today_stats={
            "today_trade_count": 0,
            "today_pnl_won": 0.0,
            "daily_loss_limit_left": float(settings["LLM_DAILY_LOSS_LIMIT"]),
            "recent_ticks": [],
        },
        token_cache={
            "access_token": "",
            "issued_at": 0.0,
            "expires_in": 86400,
        },
        volume_delta_history=deque(maxlen=int(settings["VOLUME_WINDOW"])),
        investor_cache={
            "foreign_buy": 0.0,
            "oi_value": 0.0,
            "trade_strength": 0.0,
            "updated_at": 0.0,
            # 선물 3주체
            "fut_fgn_buy":    0.0,
            "fut_inst_buy":   0.0,
            "fut_indiv_buy":  0.0,
            "fut_fgn_delta":  0.0,
            "fut_inst_delta": 0.0,
            "fut_indiv_delta":0.0,
            # 현물 3주체
            "spot_fgn_buy":    0.0,
            "spot_inst_buy":   0.0,
            "spot_indiv_buy":  0.0,
            "spot_fgn_delta":  0.0,
            "spot_inst_delta": 0.0,
            "spot_indiv_delta":0.0,
        },
        ema_fast_history=deque(maxlen=int(settings["TREND_EMA_SLOW"]) + 20),
        atr_history=deque(maxlen=int(settings["ATR_PERIOD"]) + 1),
    )
