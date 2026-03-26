# MMEAN/db_setup.py
"""
MMEAN DB 스키마 통합 관리

역할:
  - 모든 테이블 CREATE / ALTER (마이그레이션)
  - 인덱스 생성
  - RAG 사전 테이블 생성 (llm_calls, market_features, pattern_memory)
  - llm_controller / llm_filter 각자 _setup_tables() 대체

사용:
  from db_setup import setup_all
  setup_all(db_path)
"""
from __future__ import annotations

import logging
import sqlite3
from typing import List, Tuple

log = logging.getLogger("MMEAN.DBSetup")


# -------------------------------------------------------------------
# 마이그레이션 컬럼 목록 (테이블, 컬럼명, 타입)
# 역할: 기존 DB에 없는 컬럼을 안전하게 추가 (IF NOT EXISTS 효과)
# 신규 DB에서는 CREATE TABLE이 이미 포함하므로 ALTER는 무시됨 (except로 처리)
# 순서: 의존성 없도록 배열
# -------------------------------------------------------------------
_MIGRATIONS: List[Tuple[str, str, str]] = [
    # ── trades: 기존 DB 호환 (신규 CREATE에 포함됨) ─────────────────
    ("trades", "max_favorable_pt",        "REAL    DEFAULT 0"),
    ("trades", "hold_ticks",              "INTEGER DEFAULT 0"),
    ("trades", "cum_equity",              "REAL    DEFAULT 0"),
    ("trades", "max_adverse_excursion",   "REAL    DEFAULT 0"),
    ("trades", "entry_llm_call_id",       "INTEGER DEFAULT 0"),
    ("trades", "entry_llm_score",         "REAL    DEFAULT -1"),
    ("trades", "entry_llm_direction",     "TEXT    DEFAULT 'UNKNOWN'"),
    ("trades", "entry_trend_weight_long", "REAL    DEFAULT 0"),
    ("trades", "entry_trend_weight_short","REAL    DEFAULT 0"),
    ("trades", "entry_trend_long_score",  "REAL    DEFAULT 0"),
    ("trades", "entry_trend_short_score", "REAL    DEFAULT 0"),
    # hash 컬럼 (신규 CREATE에 포함됨)
    # config_hash      : 일반 파라미터 hash (ConfigManager)
    # param_store_hash : trend snapshot_hash (ParamStore 전체 상태)
    # param_hash       : 순수 파라미터 fingerprint (AB 비교 핵심)
    ("trades", "param_store_hash",        "TEXT    DEFAULT ''"),
    ("trades", "param_store_version",     "INTEGER DEFAULT 0"),
    ("trades", "param_hash",              "TEXT    DEFAULT ''"),
    ("trades", "foreign_signal_mode",     "TEXT    DEFAULT 'composite'"),
    ("trades", "level_no",               "INTEGER NULL"),
    ("trades", "simulation_mode",        "TEXT    DEFAULT 'expert'"),

    # ── llm_calls ────────────────────────────────────────────────────
    ("llm_calls", "opportunity_score",  "REAL"),
    ("llm_calls", "direction",          "TEXT"),
    ("llm_calls", "expires_at",         "TEXT"),
    ("llm_calls", "prompt_name",        "TEXT"),
    ("llm_calls", "prompt_hash",        "TEXT"),
    ("llm_calls", "prompt_source",      "TEXT"),

    # ── regime_ticks: 기존 DB 호환 (신규 CREATE에 포함됨) ───────────
    ("regime_ticks", "trend_long_score",  "REAL    DEFAULT 0"),
    ("regime_ticks", "trend_short_score", "REAL    DEFAULT 0"),
    ("regime_ticks", "trend_weight_long", "REAL    DEFAULT 0"),
    ("regime_ticks", "trend_weight_short","REAL    DEFAULT 0"),
    ("regime_ticks", "ema_fast",          "REAL    DEFAULT 0"),
    ("regime_ticks", "ema_slow",          "REAL    DEFAULT 0"),
    ("regime_ticks", "ema_fast_slope",    "REAL    DEFAULT 0"),
    ("regime_ticks", "param_hash",        "TEXT    DEFAULT ''"),
    ("regime_ticks", "feature_json",      "TEXT"),
    # ── regime_ticks: 판단 근거 보강 (재생 시뮬레이션 & 분석용) ────
    ("regime_ticks", "vwap",                  "REAL"),
    ("regime_ticks", "atr",                   "REAL"),
    ("regime_ticks", "price_vs_vwap",         "REAL"),
    ("regime_ticks", "foreign_signal_mode",   "TEXT"),
    ("regime_ticks", "flow_score",            "REAL"),
    ("regime_ticks", "flow_long_weight",      "REAL"),
    ("regime_ticks", "flow_short_weight",     "REAL"),
    ("regime_ticks", "flow_llm_weight_adjust","REAL"),
    ("regime_ticks", "flow_order_size",       "REAL"),
    ("regime_ticks", "llm_filter_score",      "REAL"),
    ("regime_ticks", "llm_filter_direction",  "TEXT"),
    ("regime_ticks", "llm_filter_valid",      "INTEGER DEFAULT 0"),
    ("regime_ticks", "llm_filter_ts",         "TEXT"),
    ("regime_ticks", "llm_call_id",           "INTEGER"),
    ("regime_ticks", "level_no",              "INTEGER"),
    ("regime_ticks", "mode_type",             "TEXT"),
    ("regime_ticks", "session_phase",         "TEXT"),
    ("regime_ticks", "night_regime",          "TEXT"),
    ("regime_ticks", "night_entry",           "TEXT"),
    ("regime_ticks", "night_confidence",      "REAL"),
    ("regime_ticks", "night_raw_score",       "REAL"),
    ("regime_ticks", "today_trade_count",     "INTEGER"),
    ("regime_ticks", "daily_loss_limit_left", "REAL"),

    # ── llm_param_events: 분석용 확장 컬럼 ──────────────────────────
    ("llm_param_events", "source",              "TEXT"),
    ("llm_param_events", "reason_code",         "TEXT"),
    ("llm_param_events", "before_param_hash",   "TEXT"),
    ("llm_param_events", "after_param_hash",    "TEXT"),
    ("llm_param_events", "changed_fields_json", "TEXT"),
    ("llm_param_events", "blocked_reason",      "TEXT"),

    # ── market_features: regime_tags 보강 (2026-03) ──────────────────
    ("market_features", "regime_tags", "TEXT"),  # JSON 배열 문자열

    # ── regime_ticks: 6주체 수급 delta + 당일 레인지 (2026-03-23) ────
    ("regime_ticks", "fut_fgn_delta",    "REAL DEFAULT 0"),
    ("regime_ticks", "fut_inst_delta",   "REAL DEFAULT 0"),
    ("regime_ticks", "fut_indiv_delta",  "REAL DEFAULT 0"),
    ("regime_ticks", "spot_fgn_delta",   "REAL DEFAULT 0"),
    ("regime_ticks", "spot_inst_delta",  "REAL DEFAULT 0"),
    ("regime_ticks", "spot_indiv_delta", "REAL DEFAULT 0"),
    ("regime_ticks", "session_high",     "REAL DEFAULT 0"),
    ("regime_ticks", "session_low",      "REAL DEFAULT 0"),
    ("regime_ticks", "price_range_pct",  "REAL DEFAULT 0.5"),
]


def setup_all(db_path: str) -> None:
    """전체 DB 셋업 — 앱 시작 시 1회 호출."""
    conn = sqlite3.connect(db_path, check_same_thread=False)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    try:
        _create_core_tables(conn)
        _create_llm_tables(conn)
        _create_prompt_tables(conn)
        _create_rag_tables(conn)
        _create_failure_pattern_table(conn)
        _create_entry_gate_log_table(conn)
        _create_replay_tables(conn)
        _create_pattern_tables(conn)
        _create_checkpoint_today_table(conn)
        _run_migrations(conn)
        _create_indexes(conn)
        conn.commit()
        log.info("DB setup 완료 | path=%s", db_path)
    finally:
        conn.close()


def setup_judgment_db(db_path: str) -> None:
    """judgment.db 전용 초기화 — judgment_events 테이블 + 인덱스."""
    conn = sqlite3.connect(db_path, check_same_thread=False)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    try:
        _create_judgment_table(conn)
        for name, sql in [
            ("idx_judgment_ts",     "CREATE INDEX IF NOT EXISTS idx_judgment_ts ON judgment_events(ts)"),
            ("idx_judgment_signal", "CREATE INDEX IF NOT EXISTS idx_judgment_signal ON judgment_events(entry_signal, final_signal)"),
            ("idx_judgment_prov",   "CREATE INDEX IF NOT EXISTS idx_judgment_prov ON judgment_events(prov_regime, afternoon_regime)"),
            ("idx_judgment_llm",    "CREATE INDEX IF NOT EXISTS idx_judgment_llm ON judgment_events(llm_was_right, llm_risk_view)"),
        ]:
            try:
                conn.execute(sql)
            except Exception:
                pass
        conn.commit()
        log.info("DBSetup judgment.db 완료 | path=%s", db_path)
    finally:
        conn.close()


# -------------------------------------------------------------------
# 코어 테이블
# -------------------------------------------------------------------
def _create_core_tables(conn: sqlite3.Connection) -> None:
    conn.executescript("""
    CREATE TABLE IF NOT EXISTS regime_ticks (
        id              INTEGER PRIMARY KEY AUTOINCREMENT,
        ts              TEXT    NOT NULL,
        bias            TEXT    NOT NULL,
        entry_signal    TEXT    NOT NULL,
        confidence      REAL    NOT NULL,
        futures_price   REAL    NOT NULL,
        spot_price      REAL    NOT NULL,
        basis           REAL    NOT NULL,
        basis_ema       REAL    NOT NULL,
        basis_ema_delta REAL    NOT NULL,
        basis_slope     REAL    NOT NULL,
        volume_burst    REAL    NOT NULL,
        foreign_buy     REAL    NOT NULL,
        oi_value        REAL    NOT NULL,
        oi_delta        REAL    NOT NULL,
        trade_qty       REAL    NOT NULL,
        cum_volume      REAL    NOT NULL,
        trade_strength  REAL    NOT NULL,
        best_ask        REAL    NOT NULL,
        best_bid        REAL    NOT NULL,
        long_score      REAL    NOT NULL,
        short_score     REAL    NOT NULL,
        reason          TEXT,
        -- EMA 추세 점수 (틱 단위 기록)
        trend_long_score    REAL    DEFAULT 0,
        trend_short_score   REAL    DEFAULT 0,
        trend_weight_long   REAL    DEFAULT 0,
        trend_weight_short  REAL    DEFAULT 0,
        ema_fast            REAL    DEFAULT 0,
        ema_slow            REAL    DEFAULT 0,
        ema_fast_slope      REAL    DEFAULT 0,
        -- ParamStore fingerprint (틱 당시 파라미터 상태 역추적용)
        param_hash          TEXT    DEFAULT '',
        feature_json        TEXT,
        -- ── 판단 근거 보강: 재생 시뮬레이션 & 분석용 (2026-03) ──────
        vwap                    REAL,
        atr                     REAL,
        price_vs_vwap           REAL,
        foreign_signal_mode     TEXT,
        flow_score              REAL,
        flow_long_weight        REAL,
        flow_short_weight       REAL,
        flow_llm_weight_adjust  REAL,
        flow_order_size         REAL,
        llm_filter_score        REAL,
        llm_filter_direction    TEXT,
        llm_filter_valid        INTEGER DEFAULT 0,
        llm_filter_ts           TEXT,
        llm_call_id             INTEGER,
        level_no                INTEGER,
        mode_type               TEXT,
        session_phase           TEXT,
        night_regime            TEXT,
        night_entry             TEXT,
        night_confidence        REAL,
        night_raw_score         REAL,
        today_trade_count       INTEGER,
        daily_loss_limit_left   REAL,
        -- 6주체 수급 delta (2026-03-23)
        fut_fgn_delta    REAL DEFAULT 0,
        fut_inst_delta   REAL DEFAULT 0,
        fut_indiv_delta  REAL DEFAULT 0,
        spot_fgn_delta   REAL DEFAULT 0,
        spot_inst_delta  REAL DEFAULT 0,
        spot_indiv_delta REAL DEFAULT 0,
        -- 당일 레인지 내 현재가 위치
        session_high     REAL DEFAULT 0,
        session_low      REAL DEFAULT 0,
        price_range_pct  REAL DEFAULT 0.5
    );

    CREATE TABLE IF NOT EXISTS regime_events (
        id              INTEGER PRIMARY KEY AUTOINCREMENT,
        bias            TEXT    NOT NULL,
        start_ts        TEXT    NOT NULL,
        end_ts          TEXT,
        duration_sec    REAL,
        start_price     REAL    NOT NULL,
        end_price       REAL,
        max_confidence  REAL    DEFAULT 0,
        min_basis       REAL    DEFAULT 0,
        max_basis       REAL    DEFAULT 0,
        tick_count      INTEGER DEFAULT 0,
        status          TEXT    NOT NULL DEFAULT 'OPEN'
    );

    CREATE TABLE IF NOT EXISTS entry_events (
        id              INTEGER PRIMARY KEY AUTOINCREMENT,
        ts              TEXT    NOT NULL,
        bias            TEXT    NOT NULL,
        entry_signal    TEXT    NOT NULL,
        futures_price   REAL    NOT NULL,
        basis           REAL    NOT NULL,
        basis_slope     REAL    NOT NULL,
        basis_ema_delta REAL    NOT NULL,
        volume_burst    REAL    NOT NULL,
        confidence      REAL    NOT NULL,
        long_score      REAL    NOT NULL,
        short_score     REAL    NOT NULL,
        reason          TEXT
    );

    CREATE TABLE IF NOT EXISTS trades (
        id                      INTEGER PRIMARY KEY AUTOINCREMENT,
        session_id              TEXT    NOT NULL,
        open_ts                 TEXT    NOT NULL,
        close_ts                TEXT,
        direction               INTEGER NOT NULL,
        entry_price             REAL    NOT NULL,
        exit_price              REAL,
        pnl_ticks               REAL,
        exit_reason             TEXT,
        entry_bias              TEXT,
        entry_long_score        REAL,
        entry_short_score       REAL,
        entry_confidence        REAL,
        entry_basis             REAL,
        entry_slope             REAL,
        entry_vol_burst         REAL,
        entry_params            TEXT,
        entry_reason            TEXT,
        exit_bias               TEXT,
        exit_long_score         REAL,
        exit_short_score        REAL,
        exit_confidence         REAL,
        exit_params             TEXT,
        exit_reason_detail      TEXT,
        config_hash             TEXT,
        max_favorable_pt        REAL    DEFAULT 0,
        max_adverse_excursion   REAL    DEFAULT 0,
        hold_ticks              INTEGER DEFAULT 0,
        cum_equity              REAL    DEFAULT 0,
        entry_llm_call_id       INTEGER DEFAULT 0,
        entry_llm_score         REAL    DEFAULT -1,
        entry_llm_direction     TEXT    DEFAULT 'UNKNOWN',
        entry_trend_weight_long  REAL    DEFAULT 0,
        entry_trend_weight_short REAL    DEFAULT 0,
        entry_trend_long_score   REAL    DEFAULT 0,
        entry_trend_short_score  REAL    DEFAULT 0,
        param_store_hash         TEXT    DEFAULT '',
        param_store_version      INTEGER DEFAULT 0,
        param_hash               TEXT    DEFAULT '',
        foreign_signal_mode      TEXT    DEFAULT 'composite',
        level_no                 INTEGER NULL,
        simulation_mode          TEXT    DEFAULT 'expert'
    );

    CREATE TABLE IF NOT EXISTS config_snapshots (
        id      INTEGER PRIMARY KEY AUTOINCREMENT,
        ts      TEXT    NOT NULL,
        hash    TEXT    NOT NULL,
        action  TEXT    NOT NULL,
        changed TEXT,
        config  TEXT    NOT NULL
    );

    CREATE TABLE IF NOT EXISTS champion_vs_challenger (
        id                      INTEGER PRIMARY KEY AUTOINCREMENT,
        ts                      TEXT    NOT NULL,
        session_date            TEXT    NOT NULL,

        champion_param_hash     TEXT    DEFAULT '',
        champion_bias           TEXT    NOT NULL,
        champion_entry_signal   TEXT    NOT NULL,
        champion_long_score     REAL    DEFAULT 0,
        champion_short_score    REAL    DEFAULT 0,
        champion_confidence     REAL    DEFAULT 0,
        champion_trend_long     REAL    DEFAULT 0,
        champion_trend_short    REAL    DEFAULT 0,

        challenger_param_hash   TEXT    DEFAULT '',
        challenger_bias         TEXT    NOT NULL,
        challenger_entry_signal TEXT    NOT NULL,
        challenger_long_score   REAL    DEFAULT 0,
        challenger_short_score  REAL    DEFAULT 0,
        challenger_confidence   REAL    DEFAULT 0,
        challenger_trend_long   REAL    DEFAULT 0,
        challenger_trend_short  REAL    DEFAULT 0,

        same_bias               INTEGER DEFAULT 0,
        same_signal             INTEGER DEFAULT 0,
        diverged                INTEGER DEFAULT 0,
        param_hash_same         INTEGER DEFAULT 0,
        score_delta_long        REAL    DEFAULT 0,
        score_delta_short       REAL    DEFAULT 0
    );
    """)


# -------------------------------------------------------------------
# LLM 테이블
# -------------------------------------------------------------------
def _create_llm_tables(conn: sqlite3.Connection) -> None:
    conn.executescript("""
    CREATE TABLE IF NOT EXISTS llm_calls (
        id                INTEGER PRIMARY KEY AUTOINCREMENT,
        ts                TEXT    NOT NULL,
        layer             TEXT    NOT NULL,
        provider          TEXT    NOT NULL,
        prompt_json       TEXT,
        response_json     TEXT    NOT NULL,
        latency_ms        INTEGER,
        applied           INTEGER DEFAULT 0,
        validation_ok     INTEGER DEFAULT 1,
        session_date      TEXT,
        baseline_hash     TEXT,
        apply_mode        TEXT,
        error_type        TEXT,
        fallback_depth    INTEGER DEFAULT 0,
        attempts_json     TEXT,
        expires_at        TEXT,
        opportunity_score REAL,
        direction         TEXT,
        prompt_name       TEXT,
        prompt_hash       TEXT,
        prompt_source     TEXT
    );

    CREATE TABLE IF NOT EXISTS llm_daily_baseline (
        session_date    TEXT    PRIMARY KEY,
        created_ts      TEXT    NOT NULL,
        baseline_hash   TEXT    NOT NULL,
        baseline_json   TEXT    NOT NULL
    );

    CREATE TABLE IF NOT EXISTS llm_param_events (
        id                  INTEGER PRIMARY KEY AUTOINCREMENT,
        ts                  TEXT    NOT NULL,
        layer               TEXT    NOT NULL,
        event_type          TEXT    NOT NULL,
        provider            TEXT,
        error_type          TEXT,
        attempt_count       INTEGER DEFAULT 0,
        config_hash_before  TEXT,
        config_hash_after   TEXT,
        stale_age_sec       REAL    DEFAULT 0,
        snapshot_version    INTEGER DEFAULT 0,
        raw_excerpt         TEXT,
        action_taken        TEXT,
        consecutive_fails   INTEGER DEFAULT 0,
        session_date        TEXT,
        source              TEXT,
        reason_code         TEXT,
        before_param_hash   TEXT,
        after_param_hash    TEXT,
        changed_fields_json TEXT,
        blocked_reason      TEXT
    );
    """)


# -------------------------------------------------------------------
# Prompt / RAG 테이블
# -------------------------------------------------------------------
def _create_prompt_tables(conn: sqlite3.Connection) -> None:
    conn.executescript("""
    CREATE TABLE IF NOT EXISTS prompt_snapshots (
        id          INTEGER PRIMARY KEY AUTOINCREMENT,
        ts          TEXT    NOT NULL,
        prompt_name TEXT    NOT NULL,
        prompt_hash TEXT    NOT NULL,
        source_type TEXT    NOT NULL,
        content     TEXT    NOT NULL,
        note        TEXT
    );
    """)


def _create_rag_tables(conn: sqlite3.Connection) -> None:
    conn.executescript("""
    CREATE TABLE IF NOT EXISTS market_features (
        id              INTEGER PRIMARY KEY AUTOINCREMENT,
        tick_id         INTEGER,
        ts              TEXT    NOT NULL,
        session_date    TEXT    NOT NULL,
        time_bucket     TEXT,
        basis_zone      TEXT,
        volume_zone     TEXT,
        bias_streak     INTEGER DEFAULT 0,
        daily_pnl_zone  TEXT,
        feature_vector  TEXT,
        regime_tags     TEXT    -- JSON 배열 ["BULL_CLEAN", "VOL_HIGH", ...]
    );

    CREATE TABLE IF NOT EXISTS pattern_memory (
        id                  INTEGER PRIMARY KEY AUTOINCREMENT,
        cluster_key         TEXT    NOT NULL UNIQUE,
        sample_count        INTEGER DEFAULT 0,
        win_count           INTEGER DEFAULT 0,
        loss_count          INTEGER DEFAULT 0,
        win_rate            REAL    DEFAULT 0.0,
        avg_pnl_ticks       REAL    DEFAULT 0.0,
        avg_mae             REAL    DEFAULT 0.0,
        avg_mfe             REAL    DEFAULT 0.0,
        long_win_rate       REAL    DEFAULT 0.0,
        short_win_rate      REAL    DEFAULT 0.0,
        last_updated        TEXT,
        last_trade_ts       TEXT,
        warning_level       TEXT    DEFAULT 'none'
    );

    CREATE TABLE IF NOT EXISTS llm_evaluations (
        id                  INTEGER PRIMARY KEY AUTOINCREMENT,
        llm_call_id         INTEGER NOT NULL,
        trade_id            INTEGER,
        evaluated_at        TEXT    NOT NULL,
        provider            TEXT,
        layer               TEXT,
        llm_score           REAL,
        llm_direction       TEXT,
        actual_direction    INTEGER,
        actual_pnl_ticks    REAL,
        actual_exit_reason  TEXT,
        score_vs_pnl        TEXT,
        direction_match     INTEGER DEFAULT 0,
        session_date        TEXT
    );
    """)


# -------------------------------------------------------------------
# 패턴 문서 테이블  (pattern_schema.py Phase 1)
# -------------------------------------------------------------------
def _create_pattern_tables(conn: sqlite3.Connection) -> None:
    conn.executescript("""
    CREATE TABLE IF NOT EXISTS pattern_docs (
        id                  INTEGER PRIMARY KEY AUTOINCREMENT,
        doc_id              TEXT    NOT NULL UNIQUE,
        doc_type            TEXT    NOT NULL,
        -- doc_type 허용값:
        --   'pattern_case'       특정 시장 상황에서 효과적이었던 패턴
        --   'failure_case'       실패 패턴 (재발 방지 교훈)
        --   'stable_config'      안정 수렴한 레벨 설정 스냅샷
        --   'validation_summary' 검증 결과 요약
        --   'stress_summary'     스트레스 테스트 결과
        created_at          TEXT    NOT NULL,
        session_dates_json  TEXT,               -- JSON 배열 ["2026-03-17", ...]
        regime_tags_json    TEXT,               -- JSON 배열 ["BULL_CLEAN", ...]
        level_band_min      INTEGER,
        level_band_max      INTEGER,
        sample_size_dates   INTEGER DEFAULT 0,
        sample_size_trades  INTEGER DEFAULT 0,
        confidence_level    TEXT    DEFAULT 'LOW',   -- LOW/MEDIUM/HIGH
        summary_text        TEXT,               -- 1층: 규칙 기반 자동 생성
        llm_summary         TEXT,               -- 2층: LLM 보조 자연어 (선택)
        is_active           INTEGER DEFAULT 1,  -- 0이면 아카이브
        detail_json         TEXT                -- 레벨별 성적, 스트레스 결과 등 상세
    );
    """)


# -------------------------------------------------------------------
# 재생 시뮬레이션 테이블
# -------------------------------------------------------------------
def _create_judgment_table(conn: sqlite3.Connection) -> None:
    conn.executescript("""
    CREATE TABLE IF NOT EXISTS judgment_events (
        id                    INTEGER PRIMARY KEY AUTOINCREMENT,
        -- 타이밍
        ts                    TEXT NOT NULL,
        exec_mode             TEXT DEFAULT 'VIRTUAL',
        -- 진입 신호
        entry_signal          TEXT,          -- LONG_READY / SHORT_READY
        final_signal          TEXT,          -- 게이트 통과 후 실제 신호 (WAIT 포함)
        -- 레짐 스냅샷 (진입 신호 당시)
        flow_score            REAL,
        bias                  TEXT,
        long_score            REAL,
        short_score           REAL,
        volume_burst          REAL,
        regime_type           TEXT,
        -- 체크포인트 요약 (자주 쿼리할 필드는 컬럼으로 분리)
        opening_char          TEXT,          -- char 값만 (ex: opening_strength)
        opening_char_filter   TEXT,          -- size_filter 값 (normal/reduce/skip)
        prov_regime           TEXT,          -- regime 값만 (ex: trend_continuation)
        afternoon_regime      TEXT,          -- regime 값만 (ex: trend_alive)
        market_view           TEXT,          -- view 값만 (ex: TREND_UP)
        -- 체크포인트 전체 JSON (원본 보존)
        opening_char_json     TEXT,
        prov_regime_json      TEXT,
        afternoon_regime_json TEXT,
        market_view_json      TEXT,
        -- LLM 판단 (update_llm 후 채워짐)
        llm_action            TEXT,          -- BUY_LONG / SELL_SHORT / HOLD 등
        llm_risk_view         TEXT,          -- positive / neutral / negative / failure
        llm_confidence        REAL,
        llm_weight_adj        REAL,
        llm_call_key          TEXT,
        -- 거래 결과 (close_event 후 채워짐)
        entry_price           REAL,
        exit_price            REAL,
        direction             INTEGER,       -- 1=LONG, -1=SHORT
        pnl_pt                REAL,          -- 손익 포인트
        mfe_pt                REAL,          -- 최대 유리 (Max Favorable Excursion)
        mae_pt                REAL,          -- 최대 불리 (Max Adverse Excursion)
        exit_reason           TEXT,          -- TP / SL / SIGNAL / TIMEOUT / FORCE
        hold_sec              INTEGER,       -- 보유 시간(초)
        -- 분석
        llm_was_right         INTEGER        -- 1=맞음, 0=틀림, NULL=판정불가
    );
    """)


def _create_failure_pattern_table(conn: sqlite3.Connection) -> None:
    """
    failure_patterns — 체크포인트 조합별 실패/금지 패턴 메모리.

    rag_prep.py step4 가 judgment_events 를 집계해서 UPSERT.
    engine_runtime._llm_call_in_bg 에서 LLM 프롬프트에 주입.

    pattern_type:
        forbidden — WR < 30%, n >= 5  (진입 강경 경고)
        failure   — 30% <= WR < 45%, avg_pnl < 0  (진입 주의 경고)
        success   — WR >= 50%, avg_pnl > 0  (미래 참고용 — LLM 주입 대상 아님)

    combo_type (어떤 체크포인트 조합인지):
        oc_prov   — opening_char + prov_regime
        prov_ar   — prov_regime + afternoon_regime
        oc_ar     — opening_char + afternoon_regime
        single_oc — opening_char 단독
    """
    conn.executescript("""
    CREATE TABLE IF NOT EXISTS failure_patterns (
        id                   INTEGER PRIMARY KEY AUTOINCREMENT,
        combo_key            TEXT    NOT NULL,   -- "oc_val|prov_val" 형태
        combo_type           TEXT    NOT NULL,   -- oc_prov | prov_ar | oc_ar | single_oc
        pattern_type         TEXT    NOT NULL,   -- forbidden | failure | success
        n                    INTEGER NOT NULL,   -- 집계 샘플 수
        win_count            INTEGER DEFAULT 0,
        loss_count           INTEGER DEFAULT 0,
        win_rate             REAL,               -- 0.0~1.0
        avg_pnl              REAL,               -- 평균 손익 (pt)
        worst_pnl            REAL,               -- 최악 손익 (pt)
        sample_window_days   INTEGER DEFAULT 90, -- 집계 기간 (일)
        confidence           TEXT    DEFAULT 'LOW', -- LOW(<10) | MED(10-19) | HIGH(>=20)
        summary_text         TEXT,              -- 자동 생성 요약
        updated_at           TEXT    NOT NULL,

        UNIQUE(combo_key, combo_type)
    );
    """)


def _create_checkpoint_today_table(conn: sqlite3.Connection) -> None:
    """
    checkpoint_today — CheckpointAnalyst 당일 실행 결과 영속화.

    재시작 시 이 테이블에서 오늘 결과를 복원해 중복 LLM 호출을 방지한다.
    date + name이 PK이므로 하루에 체크포인트당 1건만 유지된다.
    """
    conn.executescript("""
    CREATE TABLE IF NOT EXISTS checkpoint_today (
        date   TEXT NOT NULL,
        name   TEXT NOT NULL,
        result TEXT NOT NULL,
        ts     TEXT NOT NULL,
        PRIMARY KEY (date, name)
    );
    """)


def _create_entry_gate_log_table(conn: sqlite3.Connection) -> None:
    """
    entry_gate_log — REJECT/HALF 발동 이력 기록.

    engine_runtime.py 에서 entry_gate != ALLOW 일 때 INSERT.
    일별 통계, RAG 피드백 루프에 활용 예정.
    """
    conn.executescript("""
    CREATE TABLE IF NOT EXISTS entry_gate_log (
        id              INTEGER PRIMARY KEY AUTOINCREMENT,
        ts              TEXT    NOT NULL,           -- 발동 시각 (ISO)
        session_date    TEXT    NOT NULL,           -- 날짜 (YYYY-MM-DD)
        gate            TEXT    NOT NULL,           -- REJECT | HALF
        caution_level   TEXT    NOT NULL,           -- danger | warning | caution | none
        fac_type        TEXT    NOT NULL,           -- forbidden | failure | none
        fac_conf        TEXT    NOT NULL,           -- HIGH | MED | LOW | NONE
        basis_zone      TEXT,
        volume_zone     TEXT,
        time_bucket     TEXT,
        opening_char    TEXT,                       -- prov_regime opening_char 값
        prov_regime     TEXT,
        afternoon_regime TEXT
    );
    """)


def _create_replay_tables(conn: sqlite3.Connection) -> None:
    conn.executescript("""
    CREATE TABLE IF NOT EXISTS replay_runs (
        id              INTEGER PRIMARY KEY AUTOINCREMENT,
        session_date    TEXT    NOT NULL,
        level_no        INTEGER,
        config_hash     TEXT,
        config_json     TEXT,
        run_started_at  TEXT,
        run_finished_at TEXT,
        tick_count      INTEGER,
        warmup_ticks    INTEGER DEFAULT 200,
        trade_count     INTEGER DEFAULT 0,
        status          TEXT    DEFAULT 'running',
        notes           TEXT
    );

    CREATE TABLE IF NOT EXISTS replay_trades (
        id                      INTEGER PRIMARY KEY AUTOINCREMENT,
        run_id                  INTEGER NOT NULL,
        session_date            TEXT    NOT NULL,
        level_no                INTEGER,
        open_ts                 TEXT,
        close_ts                TEXT,
        direction               TEXT,
        entry_price             REAL,
        exit_price              REAL,
        pnl_ticks               REAL,
        exit_reason             TEXT,
        hold_ticks              INTEGER,
        max_favorable_pt        REAL    DEFAULT 0,
        max_adverse_excursion   REAL    DEFAULT 0,
        entry_long_score        REAL,
        entry_short_score       REAL,
        entry_confidence        REAL,
        entry_llm_score         REAL,
        entry_llm_valid         INTEGER DEFAULT 0,
        entry_session_phase     TEXT
    );
    """)


# -------------------------------------------------------------------
# 마이그레이션
# -------------------------------------------------------------------
def _run_migrations(conn: sqlite3.Connection) -> None:
    for table, col, dtype in _MIGRATIONS:
        try:
            conn.execute(f"ALTER TABLE {table} ADD COLUMN {col} {dtype}")
            log.info("마이그레이션 | %s.%s 추가", table, col)
        except Exception:
            pass


# -------------------------------------------------------------------
# 인덱스
# -------------------------------------------------------------------
def _create_indexes(conn: sqlite3.Connection) -> None:
    indexes = [
        ("idx_regime_ticks_ts",         "CREATE INDEX IF NOT EXISTS idx_regime_ticks_ts ON regime_ticks(ts)"),
        ("idx_trades_open_ts",          "CREATE INDEX IF NOT EXISTS idx_trades_open_ts ON trades(open_ts)"),
        ("idx_trades_llm_call_id",      "CREATE INDEX IF NOT EXISTS idx_trades_llm_call_id ON trades(entry_llm_call_id)"),
        ("idx_llm_calls_session_layer", "CREATE INDEX IF NOT EXISTS idx_llm_calls_session_layer ON llm_calls(session_date, layer)"),
        ("idx_llm_calls_layer_score",   "CREATE INDEX IF NOT EXISTS idx_llm_calls_layer_score ON llm_calls(layer, opportunity_score)"),
        ("idx_market_features_ts",      "CREATE INDEX IF NOT EXISTS idx_market_features_ts ON market_features(ts)"),
        ("idx_market_features_cluster", "CREATE INDEX IF NOT EXISTS idx_market_features_cluster ON market_features(basis_zone, volume_zone, time_bucket)"),
        ("idx_pattern_memory_cluster",  "CREATE INDEX IF NOT EXISTS idx_pattern_memory_cluster ON pattern_memory(cluster_key)"),
        ("idx_llm_eval_call",           "CREATE INDEX IF NOT EXISTS idx_llm_eval_call ON llm_evaluations(llm_call_id)"),
        ("idx_llm_eval_trade",          "CREATE INDEX IF NOT EXISTS idx_llm_eval_trade ON llm_evaluations(trade_id)"),
        ("idx_prompt_snapshots_name",   "CREATE INDEX IF NOT EXISTS idx_prompt_snapshots_name ON prompt_snapshots(prompt_name)"),
        ("idx_llm_calls_prompt_hash",   "CREATE INDEX IF NOT EXISTS idx_llm_calls_prompt_hash ON llm_calls(prompt_hash)"),
        ("idx_llm_param_events_layer",  "CREATE INDEX IF NOT EXISTS idx_llm_param_events_layer ON llm_param_events(layer, ts)"),
        ("idx_llm_param_events_type",   "CREATE INDEX IF NOT EXISTS idx_llm_param_events_type ON llm_param_events(event_type, ts)"),
        ("idx_cvc_ts",                  "CREATE INDEX IF NOT EXISTS idx_cvc_ts ON champion_vs_challenger(ts)"),
        ("idx_cvc_session",             "CREATE INDEX IF NOT EXISTS idx_cvc_session ON champion_vs_challenger(session_date)"),
        ("idx_cvc_diverged",            "CREATE INDEX IF NOT EXISTS idx_cvc_diverged ON champion_vs_challenger(session_date, diverged)"),
        ("idx_cvc_param_hash",          "CREATE INDEX IF NOT EXISTS idx_cvc_param_hash ON champion_vs_challenger(champion_param_hash, challenger_param_hash)"),
        # ── regime_ticks 보강 인덱스 ────────────────────────────────
        ("idx_regime_ticks_date",       "CREATE INDEX IF NOT EXISTS idx_regime_ticks_date ON regime_ticks(ts)"),
        ("idx_regime_ticks_llm",        "CREATE INDEX IF NOT EXISTS idx_regime_ticks_llm ON regime_ticks(llm_call_id)"),
        ("idx_regime_ticks_level",      "CREATE INDEX IF NOT EXISTS idx_regime_ticks_level ON regime_ticks(level_no, ts)"),
        # ── 재생 시뮬레이션 인덱스 ──────────────────────────────────
        ("idx_replay_runs_date",        "CREATE INDEX IF NOT EXISTS idx_replay_runs_date ON replay_runs(session_date)"),
        ("idx_replay_runs_level",       "CREATE INDEX IF NOT EXISTS idx_replay_runs_level ON replay_runs(session_date, level_no)"),
        ("idx_replay_trades_run",       "CREATE INDEX IF NOT EXISTS idx_replay_trades_run ON replay_trades(run_id)"),
        ("idx_replay_trades_date_lvl",  "CREATE INDEX IF NOT EXISTS idx_replay_trades_date_lvl ON replay_trades(session_date, level_no)"),
        # ── 패턴 문서 인덱스 ────────────────────────────────────────
        ("idx_pattern_docs_type",       "CREATE INDEX IF NOT EXISTS idx_pattern_docs_type ON pattern_docs(doc_type, is_active)"),
        ("idx_pattern_docs_level_band", "CREATE INDEX IF NOT EXISTS idx_pattern_docs_level_band ON pattern_docs(level_band_min, level_band_max)"),
        ("idx_pattern_docs_confidence", "CREATE INDEX IF NOT EXISTS idx_pattern_docs_confidence ON pattern_docs(confidence_level, is_active)"),
        # ── market_features regime_tags 인덱스 ──────────────────────
        ("idx_market_features_tags",    "CREATE INDEX IF NOT EXISTS idx_market_features_tags ON market_features(session_date, regime_tags)"),
        # ── entry_gate_log 인덱스 ────────────────────────────────────
        ("idx_gate_log_ts",             "CREATE INDEX IF NOT EXISTS idx_gate_log_ts ON entry_gate_log(ts)"),
        ("idx_gate_log_date_gate",      "CREATE INDEX IF NOT EXISTS idx_gate_log_date_gate ON entry_gate_log(session_date, gate)"),
    ]
    for name, sql in indexes:
        try:
            conn.execute(sql)
        except Exception as e:
            log.warning("인덱스 생성 실패 | %s: %s", name, e)