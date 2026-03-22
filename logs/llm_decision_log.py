# logs/llm_decision_log.py
"""
LLM 판단 로그 — 압축 스냅샷 + 사후 hit 기록

로그 구조:
  {
    "timestamp":      "2026-03-16 15:27",
    "input_snapshot": {
      "foreign_trend":    "improving",
      "flow_score":       0.85,
      "basis_trend":      "improving",
      "oi_trend":         "increasing",
      "volatility_state": "expanding"
    },
    "llm_output": {
      "risk_view":     "positive",
      "confidence":    0.72,
      "weight_adjust": 0.14
    },
    "actual_result":  "LONG" | "SHORT" | "NEUTRAL",  // 사후 기록
    "hit":            1 | 0                            // determine_hit() 결과
  }

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
hit 판정 기준 (고정 — 주간 재조정 루프 원천)
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
  actual_result 기준:
    진입 후 30분 뒤 포지션 방향 수익 → LONG 또는 SHORT
    breakeven ±2pt 이내 → NEUTRAL (보수적)

  매핑 테이블:
    positive + LONG    → hit=True    (LLM 낙관, 실제 상승)
    positive + SHORT   → hit=False
    positive + NEUTRAL → hit=False   (애매하면 miss로 보수적 처리)
    negative + SHORT   → hit=True    (LLM 비관, 실제 하락)
    negative + LONG    → hit=False
    negative + NEUTRAL → hit=False
    neutral  + NEUTRAL → hit=True    (LLM 중립, 실제 횡보)
    neutral  + LONG    → hit=False
    neutral  + SHORT   → hit=False
    failure  + *       → hit=False   (항상)

  보수적 처리 이유:
    NEUTRAL(breakeven)을 hit으로 인정하면 confidence 임계값이 낮아지는 방향으로
    재조정될 위험이 있다. 명확한 방향 예측에서만 hit으로 인정한다.
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

오류 유형 6가지 전부 기록:
  timeout / json_parse_fail / schema_mismatch /
  rate_limit / provider_fail / confidence_below_threshold
"""
from __future__ import annotations

import json
import logging
import os
import sqlite3
import threading
from datetime import datetime
from typing import Any, Dict, Optional

log = logging.getLogger("MMEAN.LLMDecisionLog")

# ── 오류 유형 상수 ────────────────────────────────────────────────
ERR_TIMEOUT              = "timeout"
ERR_JSON_PARSE_FAIL      = "json_parse_fail"
ERR_SCHEMA_MISMATCH      = "schema_mismatch"
ERR_RATE_LIMIT           = "rate_limit"
ERR_PROVIDER_FAIL        = "provider_fail"
ERR_CONFIDENCE_LOW       = "confidence_below_threshold"

ALL_ERROR_TYPES = {
    ERR_TIMEOUT, ERR_JSON_PARSE_FAIL, ERR_SCHEMA_MISMATCH,
    ERR_RATE_LIMIT, ERR_PROVIDER_FAIL, ERR_CONFIDENCE_LOW,
}

# ── hit 판정 테이블 (고정 — 변경 금지) ───────────────────────────
_HIT_TABLE: Dict[tuple, bool] = {
    ("positive", "LONG"):    True,
    ("positive", "SHORT"):   False,
    ("positive", "NEUTRAL"): False,
    ("negative", "SHORT"):   True,
    ("negative", "LONG"):    False,
    ("negative", "NEUTRAL"): False,
    ("neutral",  "NEUTRAL"): True,
    ("neutral",  "LONG"):    False,
    ("neutral",  "SHORT"):   False,
    # failure는 테이블에 없음 → get() 기본값 False
}

# ── 기본 저장 경로 ────────────────────────────────────────────────
DEFAULT_DB_PATH = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "storage", "mmean.db"
)


class LLMDecisionLog:
    """
    LLM 판단 로그 기록 및 사후 hit 업데이트.

    사용 예:
        log_db = LLMDecisionLog(db_path)

        # LLM 호출 후 (input_snapshot은 압축 스냅샷 — 원본 시계열 제외)
        entry_id = log_db.record(
            input_snapshot={...},
            llm_output={...},
            call_minute="2026-03-16 14:23",   # CallKey 추적용
        )

        # 결과 확인 후 (30분 뒤 등)
        actual = "LONG"
        hit    = LLMDecisionLog.determine_hit("positive", actual)
        log_db.update_hit(entry_id, actual_result=actual, hit=hit)

        # LLM 실패 시
        log_db.record_failure(error_type=ERR_TIMEOUT, flow_score=0.45)
    """

    def __init__(self, db_path: str = DEFAULT_DB_PATH) -> None:
        self.db_path = db_path
        self._lock   = threading.Lock()
        self._setup_table()

    # ------------------------------------------------------------------
    # hit 판정 (정적 메서드 — 외부에서도 직접 사용 가능)
    # ------------------------------------------------------------------

    @staticmethod
    def determine_hit(risk_view: str, actual_direction: str) -> bool:
        """
        hit 판정 기준 적용.

        Args:
            risk_view:         LLM 출력 "positive" | "neutral" | "negative" | "failure"
            actual_direction:  실제 결과 "LONG" | "SHORT" | "NEUTRAL"
                               (진입 후 30분 기준, breakeven ±2pt 이내 = "NEUTRAL")

        Returns:
            True(hit) | False(miss)

        규칙은 모듈 독스트링 hit 판정 기준 섹션에 고정.
        임의 변경 금지.
        """
        key = (risk_view.lower(), actual_direction.upper())
        result = _HIT_TABLE.get(key, False)
        log.debug(
            "determine_hit | risk_view=%s actual=%s → hit=%s",
            risk_view, actual_direction, result,
        )
        return result

    # ------------------------------------------------------------------
    # DB 기록
    # ------------------------------------------------------------------

    def record(
        self,
        input_snapshot: Dict[str, Any],
        llm_output:     Dict[str, Any],
        call_minute:    str = "",   # "YYYY-MM-DD HH:MM" — CallKey 추적용
    ) -> int:
        """
        LLM 성공 판단 기록.
        input_snapshot은 압축 스냅샷 (LLMInputBuilder.build_snapshot() 결과).
        반환: 레코드 id (사후 hit 업데이트에 사용)
        """
        ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        with self._lock:
            conn = self._conn()
            try:
                cur = conn.execute(
                    """
                    INSERT INTO llm_decision_log
                        (timestamp, input_snapshot_json, llm_output_json,
                         flow_score, risk_view, confidence, weight_adjust,
                         error_type, call_minute)
                    VALUES (?,?,?,?,?,?,?,NULL,?)
                    """,
                    (
                        ts,
                        json.dumps(input_snapshot, ensure_ascii=False),
                        json.dumps(llm_output,     ensure_ascii=False),
                        float(input_snapshot.get("flow_score", 0.0)),
                        str(llm_output.get("risk_view", "")),
                        float(llm_output.get("confidence", 0.0)),
                        float(llm_output.get("weight_adjust", 0.0)),
                        call_minute,
                    ),
                )
                conn.commit()
                row_id = cur.lastrowid
                log.debug("LLMDecisionLog 기록 | id=%d | ts=%s | min=%s", row_id, ts, call_minute)
                return row_id
            finally:
                conn.close()

    def record_failure(
        self,
        error_type: str,
        flow_score: float = 0.0,
        detail:     str   = "",
        call_minute: str  = "",
    ) -> None:
        """
        LLM 실패 기록 — 6가지 오류 유형 전부 처리.
        weight_adjust = 0, confidence = 0 으로 고정.
        """
        if error_type not in ALL_ERROR_TYPES:
            log.warning(
                "LLMDecisionLog: 알 수 없는 오류 유형 '%s' (기록은 계속)", error_type
            )

        ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        with self._lock:
            conn = self._conn()
            try:
                conn.execute(
                    """
                    INSERT INTO llm_decision_log
                        (timestamp, input_snapshot_json, llm_output_json,
                         flow_score, risk_view, confidence, weight_adjust,
                         error_type, call_minute)
                    VALUES (?,?,?,?,?,?,?,?,?)
                    """,
                    (
                        ts,
                        json.dumps({"flow_score": flow_score, "detail": detail},
                                   ensure_ascii=False),
                        json.dumps({"weight_adjust": 0, "confidence": 0},
                                   ensure_ascii=False),
                        flow_score,
                        "failure",
                        0.0,
                        0.0,
                        error_type,
                        call_minute,
                    ),
                )
                conn.commit()
                log.info(
                    "LLMDecisionLog 실패 기록 | error=%s | flow_score=%.4f | min=%s",
                    error_type, flow_score, call_minute,
                )
            finally:
                conn.close()

    def update_hit(
        self,
        record_id:     int,
        actual_result: str,
        hit:           Optional[bool] = None,
    ) -> None:
        """
        사후 실제 방향 + hit 여부 업데이트.

        Args:
            record_id:     record() 반환값
            actual_result: "LONG" | "SHORT" | "NEUTRAL"
            hit:           명시적으로 전달 시 그대로 기록,
                           None 이면 determine_hit()으로 자동 계산

        진입 후 30분 기준:
            LONG  = 포지션 수익 방향이 상승
            SHORT = 포지션 수익 방향이 하락
            NEUTRAL = breakeven ±2pt 이내
        """
        # risk_view를 DB에서 조회하여 hit 자동 계산
        if hit is None:
            with self._lock:
                conn = self._conn()
                try:
                    row = conn.execute(
                        "SELECT risk_view FROM llm_decision_log WHERE id=?",
                        (record_id,),
                    ).fetchone()
                finally:
                    conn.close()
            if row:
                hit = LLMDecisionLog.determine_hit(row["risk_view"] or "", actual_result)
            else:
                hit = False

        with self._lock:
            conn = self._conn()
            try:
                conn.execute(
                    """
                    UPDATE llm_decision_log
                       SET actual_result = ?, hit = ?
                     WHERE id = ?
                    """,
                    (actual_result, 1 if hit else 0, record_id),
                )
                conn.commit()
                log.debug(
                    "LLMDecisionLog hit 업데이트 | id=%d | actual=%s | hit=%s",
                    record_id, actual_result, hit,
                )
            finally:
                conn.close()

    def weekly_summary(self, session_date_prefix: str = "") -> Dict[str, Any]:
        """
        주간 재조정용 집계.
        session_date_prefix: 예 "2026-03-" (빈 문자열이면 전체)
        반환: hit_rate, avg_confidence, count, error_breakdown
        """
        like = f"{session_date_prefix}%" if session_date_prefix else "%"
        with self._lock:
            conn = self._conn()
            try:
                rows = conn.execute(
                    """
                    SELECT
                        COUNT(*)                                    AS total,
                        SUM(CASE WHEN hit=1 THEN 1 ELSE 0 END)     AS hits,
                        AVG(confidence)                             AS avg_conf,
                        SUM(CASE WHEN error_type IS NOT NULL
                                 THEN 1 ELSE 0 END)                AS errors
                    FROM llm_decision_log
                    WHERE timestamp LIKE ?
                    """,
                    (like,),
                ).fetchone()

                err_rows = conn.execute(
                    """
                    SELECT error_type, COUNT(*) AS cnt
                    FROM llm_decision_log
                    WHERE error_type IS NOT NULL AND timestamp LIKE ?
                    GROUP BY error_type
                    """,
                    (like,),
                ).fetchall()

                total = rows["total"] or 0
                hits  = rows["hits"]  or 0
                return {
                    "total":           total,
                    "hit_count":       hits,
                    "hit_rate":        hits / total if total > 0 else 0.0,
                    "avg_confidence":  rows["avg_conf"] or 0.0,
                    "error_count":     rows["errors"] or 0,
                    "error_breakdown": {r["error_type"]: r["cnt"] for r in err_rows},
                }
            finally:
                conn.close()

    # ------------------------------------------------------------------
    def _conn(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path, timeout=30, check_same_thread=False)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA busy_timeout=30000")
        return conn

    def _setup_table(self) -> None:
        with self._lock:
            conn = self._conn()
            try:
                conn.execute("""
                    CREATE TABLE IF NOT EXISTS llm_decision_log (
                        id                   INTEGER PRIMARY KEY AUTOINCREMENT,
                        timestamp            TEXT    NOT NULL,
                        input_snapshot_json  TEXT,
                        llm_output_json      TEXT,
                        flow_score           REAL    DEFAULT 0,
                        risk_view            TEXT,
                        confidence           REAL    DEFAULT 0,
                        weight_adjust        REAL    DEFAULT 0,
                        error_type           TEXT,
                        actual_result        TEXT,
                        hit                  INTEGER,
                        call_minute          TEXT    DEFAULT ''
                    )
                """)
                # call_minute 컬럼이 없는 구 DB에 대한 마이그레이션
                try:
                    conn.execute(
                        "ALTER TABLE llm_decision_log ADD COLUMN call_minute TEXT DEFAULT ''"
                    )
                    log.info("LLMDecisionLog: call_minute 컬럼 추가 완료 (마이그레이션)")
                except sqlite3.OperationalError:
                    pass  # 이미 존재

                conn.execute(
                    "CREATE INDEX IF NOT EXISTS idx_llm_dl_ts"
                    " ON llm_decision_log(timestamp)"
                )
                conn.execute(
                    "CREATE INDEX IF NOT EXISTS idx_llm_dl_hit"
                    " ON llm_decision_log(hit, confidence)"
                )
                conn.execute(
                    "CREATE INDEX IF NOT EXISTS idx_llm_dl_callmin"
                    " ON llm_decision_log(call_minute)"
                )
                conn.commit()
                log.debug("LLMDecisionLog DB 준비 완료 | %s", self.db_path)
            finally:
                conn.close()
