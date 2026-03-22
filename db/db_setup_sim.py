# MMEAN/db_setup_sim.py
"""
SIM DB 스키마 관리 — 시뮬레이션 실험 전용 (sim.db)

원칙:
    source_db (mmean.db) : regime_ticks 읽기 전용  ← 운영 DB 오염 방지
    sim_db    (sim.db)   : 실험 결과 쓰기 전용

테이블:
    sim_runs         — 실행 메타데이터 (run_type / session_mode 기반)
    sim_trades       — 개별 거래 기록
    sim_run_summary  — 집계 지표 + composite objective score
    sim_observations — BO 관측치 (BO 도입 시 채움)
    sim_run_sessions — 멀티세션 날짜별 요약
    sim_adoptions    — 채택 후보 (validation YES → stress 통과 → 운영 후보)

사용:
    from db_setup_sim import setup_sim_db
    setup_sim_db()  # 기본값: <프로젝트루트>/storage/sim.db
"""
from __future__ import annotations

import logging
import sqlite3
from pathlib import Path

log = logging.getLogger("MMEAN.DBSetupSim")

_DEFAULT_SIM_DB = str(Path(__file__).resolve().parent.parent / "storage" / "sim.db")


# ────────────────────────────────────────────────────────────────────
# 테이블 생성
# ────────────────────────────────────────────────────────────────────

def _create_tables(conn: sqlite3.Connection) -> None:

    # ── sim_runs ─────────────────────────────────────────────────────
    conn.execute("""
        CREATE TABLE IF NOT EXISTS sim_runs (
            id               INTEGER PRIMARY KEY AUTOINCREMENT,

            -- 실행 분류
            run_type         TEXT NOT NULL DEFAULT 'profile',
                             -- profile | random | bayes_opt | validation | stress
            session_mode     TEXT NOT NULL DEFAULT 'day',
                             -- day | night

            -- 평가 대상 날짜
            source_dates     TEXT NOT NULL,  -- JSON array: ["2026-03-17", ...]
            date_count       INTEGER DEFAULT 1,

            -- 프로파일/BO 구분
            profile_no       INTEGER,        -- run_type=profile 시 사용
            trial_no         INTEGER,        -- run_type=bayes_opt 시 사용
            study_name       TEXT,           -- run_type=bayes_opt 시 사용

            -- 설정
            config_hash      TEXT,
            config_json      TEXT NOT NULL,

            -- 실행 정보
            run_started_at   TEXT,
            run_finished_at  TEXT,
            tick_count_total INTEGER DEFAULT 0,
            warmup_ticks     INTEGER DEFAULT 100,
            trade_count      INTEGER DEFAULT 0,

            -- 결과
            objective_score  REAL,           -- sim_run_summary 계산 후 업데이트

            status           TEXT DEFAULT 'running',
                             -- running | done | error
            error_message    TEXT,  -- 실패 시 예외 메시지
            notes            TEXT
        )
    """)

    # ── sim_trades ───────────────────────────────────────────────────
    conn.execute("""
        CREATE TABLE IF NOT EXISTS sim_trades (
            id               INTEGER PRIMARY KEY AUTOINCREMENT,
            run_id           INTEGER NOT NULL,
            session_date     TEXT    NOT NULL,
            session_mode     TEXT    NOT NULL DEFAULT 'day',

            -- 체결 정보
            open_ts          TEXT,
            close_ts         TEXT,
            direction        TEXT,
            entry_price      REAL,
            exit_price       REAL,
            pnl_ticks        REAL,
            exit_reason      TEXT,
            hold_ticks       INTEGER,
            max_favorable_pt          REAL DEFAULT 0,
            max_adverse_excursion     REAL DEFAULT 0,

            -- 진입 근거 (day: long/short_score, night: confidence/raw_score)
            entry_score_a    REAL,   -- day=long_score,      night=night_confidence
            entry_score_b    REAL,   -- day=short_score,     night=night_raw_score
            entry_confidence REAL,
            entry_llm_score  REAL,   -- night=-1 (LLM 없음)
            entry_llm_valid  INTEGER DEFAULT 0,
            entry_session_phase TEXT,

            FOREIGN KEY (run_id) REFERENCES sim_runs(id)
        )
    """)

    # ── sim_run_summary ──────────────────────────────────────────────
    conn.execute("""
        CREATE TABLE IF NOT EXISTS sim_run_summary (
            id               INTEGER PRIMARY KEY AUTOINCREMENT,
            run_id           INTEGER NOT NULL UNIQUE,

            -- 기본 성과
            total_pnl        REAL,
            win_rate         REAL,   -- 0.0 ~ 1.0
            profit_factor    REAL,
            avg_pnl          REAL,
            trade_count      INTEGER,
            avg_hold_ticks   REAL,
            worst_trade      REAL,

            -- 리스크 지표
            max_drawdown              REAL,  -- 최대 낙폭 (누적 PnL 기준, 음수)
            max_consecutive_loss      INTEGER,  -- 연속 손실 최대 횟수

            -- 강건성 지표 (멀티 세션 기준)
            session_count             INTEGER,  -- 평가에 사용된 세션 수
            session_positive_ratio    REAL,     -- 수익 세션 비율 (0.0~1.0)
            pnl_std                   REAL,     -- 세션 간 PnL 표준편차

            -- 패널티
            low_trade_penalty         REAL DEFAULT 0,
                -- 거래 수 부족 패널티 (min_trade_count 미만 시)
            slippage_sensitivity      REAL DEFAULT 0,
                -- 슬리피지 ±1틱 변화 시 PnL 변동폭 (낮을수록 좋음)

            -- 최종 composite objective score
            objective_score  REAL,
            computed_at      TEXT,

            FOREIGN KEY (run_id) REFERENCES sim_runs(id)
        )
    """)

    # ── sim_observations ─────────────────────────────────────────────
    # BO 관측치 저장 (BO 도입 시 사용)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS sim_observations (
            id               INTEGER PRIMARY KEY AUTOINCREMENT,
            study_name       TEXT NOT NULL,
            trial_no         INTEGER NOT NULL,
            run_id           INTEGER,

            -- 날짜/세션 단위 집계를 위한 메타 (study_name 무관 cross-study 검색 핵심)
            session_date     TEXT,               -- 탐색에 사용된 train 기준 날짜 (YYYY-MM-DD)
            session_mode     TEXT DEFAULT 'day', -- day | night
            batch_no         INTEGER DEFAULT 1,  -- 같은 날짜/세션 내 탐색 배치 번호

            config_json      TEXT NOT NULL,
            objective_score  REAL,
            created_at       TEXT,

            FOREIGN KEY (run_id) REFERENCES sim_runs(id)
        )
    """)

    # ── sim_adoptions ────────────────────────────────────────────────
    # validation YES 후보 자동 채택 기록.
    # 흐름: random/bayes_opt → validation → adopt_best → run_slippage_stress
    conn.execute("""
        CREATE TABLE IF NOT EXISTS sim_adoptions (
            id               INTEGER PRIMARY KEY AUTOINCREMENT,
            study_name       TEXT    NOT NULL,
            config_hash      TEXT    NOT NULL,
            config_json      TEXT    NOT NULL,
            adopted_at       TEXT    NOT NULL,

            -- 훈련 성과 (sim_observations 기준)
            train_run_id     INTEGER,
            train_obj        REAL,
            train_trades     INTEGER,
            train_total_pnl  REAL,

            -- 검증 성과 (run_type='validation' 기준)
            valid_run_id     INTEGER,
            valid_dates      TEXT,           -- JSON array
            valid_obj        REAL,
            valid_trades     INTEGER,
            valid_total_pnl  REAL,
            valid_max_dd     REAL,
            valid_pos_ratio  REAL,           -- 수익 세션 비율 (0.0~1.0)
            robustness       REAL,           -- valid_obj / train_obj

            -- 슬리피지 스트레스 결과 (run_slippage_stress 실행 후 업데이트)
            stress_1t_obj    REAL,           -- slippage=1 objective
            stress_2t_obj    REAL,           -- slippage=2 objective
            stress_passed    INTEGER DEFAULT 0,  -- 1=통과, 0=미실행/실패

            -- 레짐 패턴 (session_patterns 기준)
            train_pattern    TEXT,           -- 훈련 날짜의 장세 패턴 (TREND_UP 등)
            valid_patterns   TEXT,           -- JSON: {날짜: 패턴} 검증일별 패턴

            -- 상태
            status           TEXT    DEFAULT 'candidate',
                             -- 'candidate' | 'active' | 'retired'
            cluster_id       INTEGER,        -- 군집화 후 설정 (cluster_top_configs)
            notes            TEXT
        )
    """)

    # ── shadow_trades ────────────────────────────────────────────────
    # paper_shadow 채택 후보의 세션별 가상 거래 결과
    # (JudgmentLog 이벤트 기반 — 실제 체결 아님)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS shadow_trades (
            id               INTEGER PRIMARY KEY AUTOINCREMENT,
            adoption_id      INTEGER NOT NULL,     -- sim_adoptions.id FK
            session_date     TEXT    NOT NULL,      -- YYYY-MM-DD

            -- 원본 판단 이벤트 참조
            judgment_event_id INTEGER,             -- judgment_events.id FK (mmean.db)

            -- 진입 방향
            direction        TEXT,                 -- LONG / SHORT

            -- 가격 / 손익 (실제 시스템 체결 기준)
            entry_price      REAL,
            exit_price       REAL,
            pnl_pt           REAL,                 -- 손익 포인트

            -- 실제 시스템 대비 비교
            baseline_pnl_pt  REAL,                 -- 실제 시스템 손익 (이벤트 동일)

            -- shadow가 진입했는지 여부
            shadow_entered   INTEGER DEFAULT 1,    -- 1=진입, 0=필터로 스킵

            -- 진입 근거 (필터링에 사용된 실제 값)
            long_score       REAL,
            short_score      REAL,
            flow_score       REAL,
            volume_burst     REAL,

            -- 장세 컨텍스트 (판단 근거 분석용)
            pattern_label    TEXT,                 -- session_patterns.pattern_type

            -- 메타
            evaluated_at     TEXT,

            FOREIGN KEY (adoption_id) REFERENCES sim_adoptions(id)
        )
    """)

    # ── sim_run_sessions ──────────────────────────────────────────────
    # 멀티 세션 실행 시 날짜별 상세 결과 (run_profile_multi용)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS sim_run_sessions (
            id               INTEGER PRIMARY KEY AUTOINCREMENT,
            run_id           INTEGER NOT NULL,   -- sim_runs.id FK
            session_date     TEXT    NOT NULL,

            -- 세션 기본 수치
            tick_count       INTEGER DEFAULT 0,
            trade_count      INTEGER DEFAULT 0,
            total_pnl        REAL    DEFAULT 0,
            win_rate         REAL    DEFAULT 0,  -- 0.0 ~ 1.0
            profit_factor    REAL    DEFAULT 0,
            max_drawdown     REAL    DEFAULT 0,  -- 음수
            worst_trade      REAL    DEFAULT 0,

            FOREIGN KEY (run_id) REFERENCES sim_runs(id)
        )
    """)

    conn.commit()
    log.info("sim.db 테이블 생성 완료")


# ────────────────────────────────────────────────────────────────────
# 인덱스 생성
# ────────────────────────────────────────────────────────────────────

def _create_indexes(conn: sqlite3.Connection) -> None:
    indexes = [
        # sim_runs
        ("idx_sim_runs_type_mode",
         "CREATE INDEX IF NOT EXISTS idx_sim_runs_type_mode "
         "ON sim_runs(run_type, session_mode, status)"),
        ("idx_sim_runs_study",
         "CREATE INDEX IF NOT EXISTS idx_sim_runs_study "
         "ON sim_runs(study_name)"),
        ("idx_sim_runs_dates",
         "CREATE INDEX IF NOT EXISTS idx_sim_runs_dates "
         "ON sim_runs(source_dates)"),

        # sim_trades
        ("idx_sim_trades_run",
         "CREATE INDEX IF NOT EXISTS idx_sim_trades_run "
         "ON sim_trades(run_id)"),
        ("idx_sim_trades_date_mode",
         "CREATE INDEX IF NOT EXISTS idx_sim_trades_date_mode "
         "ON sim_trades(session_date, session_mode)"),

        # sim_run_summary
        ("idx_sim_summary_run",
         "CREATE INDEX IF NOT EXISTS idx_sim_summary_run "
         "ON sim_run_summary(run_id)"),
        ("idx_sim_summary_score",
         "CREATE INDEX IF NOT EXISTS idx_sim_summary_score "
         "ON sim_run_summary(objective_score DESC)"),

        # sim_observations
        ("idx_sim_obs_study",
         "CREATE INDEX IF NOT EXISTS idx_sim_obs_study "
         "ON sim_observations(study_name, trial_no)"),
        ("idx_sim_obs_date_mode",
         "CREATE INDEX IF NOT EXISTS idx_sim_obs_date_mode "
         "ON sim_observations(session_date, session_mode, objective_score DESC)"),

        # sim_run_sessions
        ("idx_sim_sessions_run",
         "CREATE INDEX IF NOT EXISTS idx_sim_sessions_run "
         "ON sim_run_sessions(run_id)"),
        ("idx_sim_sessions_date",
         "CREATE INDEX IF NOT EXISTS idx_sim_sessions_date "
         "ON sim_run_sessions(session_date)"),

        # sim_adoptions
        ("idx_sim_adoptions_study",
         "CREATE INDEX IF NOT EXISTS idx_sim_adoptions_study "
         "ON sim_adoptions(study_name, status)"),
        ("idx_sim_adoptions_hash",
         "CREATE INDEX IF NOT EXISTS idx_sim_adoptions_hash "
         "ON sim_adoptions(config_hash)"),

        # shadow_trades
        ("idx_shadow_trades_adoption",
         "CREATE INDEX IF NOT EXISTS idx_shadow_trades_adoption "
         "ON shadow_trades(adoption_id, session_date)"),
        ("idx_shadow_trades_date",
         "CREATE INDEX IF NOT EXISTS idx_shadow_trades_date "
         "ON shadow_trades(session_date)"),
    ]
    for name, sql in indexes:
        try:
            conn.execute(sql)
        except sqlite3.OperationalError as e:
            log.debug("인덱스 건너뜀 (%s): %s", name, e)
    conn.commit()
    log.info("sim.db 인덱스 생성 완료")


# ────────────────────────────────────────────────────────────────────
# 마이그레이션 (컬럼 추가)
# ────────────────────────────────────────────────────────────────────

_MIGRATIONS = [
    ("sim_runs",         "error_message",  "TEXT"),
    # stress run 이 validation run 을 역참조하기 위한 FK
    ("sim_runs",         "parent_run_id",  "INTEGER"),
    # RAG 레짐 분류 (regime mismatch 방지)
    ("rag_documents",    "regime_type",    "TEXT DEFAULT 'unknown'"),
    # sim_observations 날짜/세션 단위 집계를 위한 메타 (cross-study validation 핵심)
    ("sim_observations", "session_date",   "TEXT"),
    ("sim_observations", "session_mode",   "TEXT DEFAULT 'day'"),
    ("sim_observations", "batch_no",       "INTEGER DEFAULT 1"),
    # 채택 파라미터에 레짐 패턴 태깅 (RAG 레짐 매칭용)
    ("sim_adoptions",    "train_pattern",  "TEXT"),
    ("sim_adoptions",    "valid_patterns", "TEXT"),
    # RAG 문서에 실제 session_pattern 전파 (proxy regime_type 보완)
    ("rag_documents",    "session_pattern", "TEXT"),
    # ── 리포팅 전용 지표 (objective_score 미포함) ─────────────────────────
    ("sim_run_summary",  "force_exit_ratio",      "REAL DEFAULT 0"),
    ("sim_run_summary",  "mfe_realization_rate",  "REAL DEFAULT 0"),
    # ── paper_shadow 단계 컬럼 ────────────────────────────────────────
    # status 확장: candidate → validated → paper_shadow → adopted
    # (status 컬럼은 TEXT이므로 별도 마이그레이션 불필요; 값만 추가)
    ("sim_adoptions",    "shadow_trade_count",   "INTEGER DEFAULT 0"),
    ("sim_adoptions",    "shadow_total_pnl",     "REAL DEFAULT 0"),
    ("sim_adoptions",    "shadow_win_rate",       "REAL DEFAULT 0"),
    ("sim_adoptions",    "shadow_obj",            "REAL DEFAULT 0"),
    ("sim_adoptions",    "shadow_started_at",     "TEXT"),
    ("sim_adoptions",    "shadow_evaluated_at",   "TEXT"),
]


def _apply_migrations(conn: sqlite3.Connection) -> None:
    added = []
    skipped = []
    for table, column, col_type in _MIGRATIONS:
        try:
            conn.execute(f"ALTER TABLE {table} ADD COLUMN {column} {col_type}")
            conn.commit()
            log.info("마이그레이션 추가: table=%s  column=%s  type=%s", table, column, col_type)
            added.append(f"{table}.{column}")
        except sqlite3.OperationalError:
            skipped.append(f"{table}.{column}")

    if added:
        log.info("마이그레이션 완료 — 추가된 컬럼 %d개: %s", len(added), ", ".join(added))
    if skipped:
        log.debug("마이그레이션 건너뜀 (이미 존재) %d개: %s", len(skipped), ", ".join(skipped))


def _backfill_obs_session_date(conn: sqlite3.Connection) -> None:
    """
    sim_observations.session_date / session_mode 백필.

    코드 변경 이전에 INSERT된 행은 session_date=NULL, session_mode='day'(기본값) 상태.
    sim_runs.source_dates (JSON 배열) 의 첫 번째 날짜와
    sim_runs.session_mode 를 역참조해서 일괄 업데이트한다.

    idempotent — 이미 채워진 행은 건드리지 않음.
    """
    # 백필 대상 수 확인
    null_count = conn.execute(
        "SELECT COUNT(*) FROM sim_observations WHERE session_date IS NULL AND run_id IS NOT NULL"
    ).fetchone()[0]

    if null_count == 0:
        log.debug("백필 불필요 — sim_observations.session_date 전부 채워져 있음")
        return

    log.info("백필 시작 — sim_observations.session_date NULL 행: %d건", null_count)

    # SQLite json_extract 로 source_dates 배열의 첫 번째 날짜 추출
    try:
        conn.execute("""
            UPDATE sim_observations
            SET
                session_date = (
                    SELECT json_extract(r.source_dates, '$[0]')
                    FROM sim_runs r
                    WHERE r.id = sim_observations.run_id
                ),
                session_mode = (
                    SELECT r.session_mode
                    FROM sim_runs r
                    WHERE r.id = sim_observations.run_id
                )
            WHERE session_date IS NULL
              AND run_id IS NOT NULL
        """)
        updated = conn.execute("SELECT changes()").fetchone()[0]
        conn.commit()
    except sqlite3.OperationalError as e:
        log.warning("백필 건너뜀 (DB 잠금 또는 오류: %s) — 다음 시작 시 재시도됩니다.", e)
        return

    # 백필 후 상태 검증
    still_null = conn.execute(
        "SELECT COUNT(*) FROM sim_observations WHERE session_date IS NULL"
    ).fetchone()[0]

    log.info("백필 완료 — 업데이트: %d행  남은NULL: %d행", updated, still_null)
    if still_null > 0:
        log.warning("백필 후 NULL 잔존 %d건 — run_id=NULL 또는 source_dates=NULL인 행",
                    still_null)


# ────────────────────────────────────────────────────────────────────
# 진입점
# ────────────────────────────────────────────────────────────────────

def setup_sim_db(db_path: str = _DEFAULT_SIM_DB) -> None:
    """sim.db 초기화 (테이블 + 인덱스 + 마이그레이션)."""
    import os
    os.makedirs(os.path.dirname(os.path.abspath(db_path)), exist_ok=True)

    conn = sqlite3.connect(db_path, timeout=30)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    try:
        _create_tables(conn)
        _create_indexes(conn)
        _apply_migrations(conn)
        _backfill_obs_session_date(conn)
        log.info("sim.db 준비 완료: %s", db_path)
    finally:
        conn.close()


# ────────────────────────────────────────────────────────────────────
# CLI
# ────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    import sys
    import logging as _logging
    _logging.basicConfig(level=_logging.INFO,
                         format="%(asctime)s [%(levelname)s] %(message)s")
    path = sys.argv[1] if len(sys.argv) > 1 else _DEFAULT_SIM_DB
    setup_sim_db(path)
    print(f"sim.db 준비 완료: {path}")
