# [Directory] MMEAN
# [File] experiment_runner.py
"""
Champion vs Challenger 동시 실행 실험 구조

[역할]
  동일한 BiasInputs(틱 입력)를 두 엔진에 동시에 전달하여
  파라미터 변화가 실제 신호 판단에 미치는 영향을 틱 단위로 기록한다.

[Champion]
  ParamStore.get_champion() → baseline 고정 파라미터 (LLM 변경 없음)
  별도 BiasRegimeEngine 인스턴스에 주입

[Challenger]
  ParamStore.get_safe() → active 동적 파라미터 (LLM 실시간 조정)
  메인 엔진(analyzer_app의 regime_engine)이 이미 실행한 결과를 재사용

[데이터 흐름]
  매 틱:
    1. analyzer_app.engine_loop → BiasInputs 생성
    2. Challenger: 메인 regime_engine.update(bias_inputs) → challenger_res
    3. ExperimentRunner.on_tick(ts, bias_inputs, challenger_snap, challenger_res)
         → Champion 파라미터 주입 → champion_engine.update(bias_inputs)
         → champion_vs_challenger 테이블에 비교 결과 기록

[성과 분석 쿼리 예시]
  -- 파라미터 조합별 신호 일치율
  SELECT champion_param_hash, challenger_param_hash,
         COUNT(*) AS ticks,
         SUM(diverged) AS diverged_ticks,
         ROUND(AVG(score_delta_long), 4) AS avg_long_delta
  FROM champion_vs_challenger
  WHERE session_date = '2026-03-15'
  GROUP BY champion_param_hash, challenger_param_hash;

  -- Diverge된 틱에서 거래 결과 조회 (champion_vs_challenger JOIN trades)
  SELECT c.ts, c.champion_bias, c.challenger_bias,
         t.direction, t.pnl_ticks, t.param_hash
  FROM champion_vs_challenger c
  JOIN trades t ON t.open_ts BETWEEN c.ts AND datetime(c.ts, '+2 seconds')
  WHERE c.diverged = 1 AND c.session_date = '2026-03-15';

[설계 원칙]
  - Champion 엔진은 메인 엔진과 완전히 독립 (state 공유 없음)
  - on_tick()은 동기 호출 — 메인 루프 안에서 짧게 처리
  - DB write는 commit() 없이 INSERT만 수행, flush_batch() 별도 호출
    → analyzer_app의 flush_tick_batch() 이후 함께 커밋
  - param_hash_same=1이면 두 엔진 결과가 항상 동일 → 기록은 하되 분석 시 필터링
"""
from __future__ import annotations

import json
import logging
import sqlite3
import threading
import time
from typing import Any, Optional

log = logging.getLogger("MMEAN.ExperimentRunner")

# 기록 배치 크기 (N틱마다 commit)
_BATCH_COMMIT_SIZE = 50


class ExperimentRunner:
    """
    Champion(baseline) vs Challenger(active) 동시 실행 실험 러너.

    사용 패턴:
        runner = ExperimentRunner(db_path, param_store)

        # 매 틱 (engine_loop 안에서)
        runner.on_tick(
            ts_text       = state["timestamp"],
            bias_inputs   = _bias_inputs,      # BiasInputs 로컬 변수
            challenger_snap = _trend_params,   # get_safe() 결과
            challenger_res  = res,             # 메인 엔진 실행 결과
        )
    """

    def __init__(self, db_path: str, param_store: Any) -> None:
        # 지연 임포트 — analyzer_app 순환 참조 방지
        from regime_engine import BiasRegimeEngine

        self._store = param_store
        self._db_path = db_path
        self._lock = threading.Lock()
        self._tick_counter = 0

        # Champion 전용 엔진 (메인 엔진과 완전히 독립)
        self._champion_engine = BiasRegimeEngine(mode="smooth", history_limit=600)

        # DB 연결 (WAL 모드)
        self._conn = sqlite3.connect(db_path, timeout=30, check_same_thread=False)
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA synchronous=NORMAL")
        self._conn.execute("PRAGMA busy_timeout=30000")

        log.info("ExperimentRunner 초기화 완료 | db=%s", db_path)

    # ------------------------------------------------------------------
    # 메인 인터페이스 — 매 틱 호출
    # ------------------------------------------------------------------
    def on_tick(
        self,
        ts_text: str,
        bias_inputs: Any,           # BiasInputs (regime_engine 모듈 타입)
        challenger_snap: Any,       # ParamSnapshot (get_safe() 결과)
        challenger_res: Any,        # BiasSnapshot (메인 엔진 결과)
    ) -> None:
        """
        Champion 엔진 실행 후 두 결과를 champion_vs_challenger 테이블에 기록.
        동기 호출 — 메인 루프 블로킹 최소화 (DB write 지연 없도록 배치 커밋).
        """
        try:
            champion_snap = self._store.get_champion()

            # Champion 엔진에 baseline 파라미터 주입
            self._apply_snap_to_engine(self._champion_engine, champion_snap)

            # Champion 엔진 실행 (동일한 BiasInputs)
            champion_res = self._champion_engine.update(bias_inputs)

            # 비교 지표 계산
            same_bias    = int(champion_res.bias         == challenger_res.bias)
            same_signal  = int(champion_res.entry_signal == challenger_res.entry_signal)
            diverged     = int(not same_bias or not same_signal)
            param_same   = int(champion_snap.param_hash  == challenger_snap.param_hash)
            delta_long   = round(
                float(challenger_res.trend_long_score) - float(champion_res.trend_long_score), 4
            )
            delta_short  = round(
                float(challenger_res.trend_short_score) - float(champion_res.trend_short_score), 4
            )

            session_date = ts_text[:10] if ts_text else time.strftime("%Y-%m-%d")

            self._conn.execute(
                """
                INSERT INTO champion_vs_challenger (
                    ts, session_date,
                    champion_param_hash,  champion_bias,  champion_entry_signal,
                    champion_long_score,  champion_short_score, champion_confidence,
                    champion_trend_long,  champion_trend_short,
                    challenger_param_hash, challenger_bias, challenger_entry_signal,
                    challenger_long_score, challenger_short_score, challenger_confidence,
                    challenger_trend_long, challenger_trend_short,
                    same_bias, same_signal, diverged, param_hash_same,
                    score_delta_long, score_delta_short
                ) VALUES (
                    ?,?,
                    ?,?,?,  ?,?,?,  ?,?,
                    ?,?,?,  ?,?,?,  ?,?,
                    ?,?,?,?,  ?,?
                )
                """,
                (
                    ts_text, session_date,
                    # Champion
                    champion_snap.param_hash,
                    champion_res.bias,
                    champion_res.entry_signal,
                    float(champion_res.long_score),
                    float(champion_res.short_score),
                    float(champion_res.confidence),
                    float(getattr(champion_res, "trend_long_score",  0.0)),
                    float(getattr(champion_res, "trend_short_score", 0.0)),
                    # Challenger
                    challenger_snap.param_hash,
                    challenger_res.bias,
                    challenger_res.entry_signal,
                    float(challenger_res.long_score),
                    float(challenger_res.short_score),
                    float(challenger_res.confidence),
                    float(getattr(challenger_res, "trend_long_score",  0.0)),
                    float(getattr(challenger_res, "trend_short_score", 0.0)),
                    # 비교
                    same_bias, same_signal, diverged, param_same,
                    delta_long, delta_short,
                ),
            )

            # 배치 커밋 (N틱마다 1회)
            self._tick_counter += 1
            if self._tick_counter % _BATCH_COMMIT_SIZE == 0:
                self._conn.commit()

            if diverged:
                log.debug(
                    "CvC diverged | ts=%s | champ=%s/%s | chall=%s/%s | "
                    "param_same=%d",
                    ts_text,
                    champion_res.bias, champion_res.entry_signal,
                    challenger_res.bias, challenger_res.entry_signal,
                    param_same,
                )

        except Exception as e:
            log.warning("ExperimentRunner.on_tick 오류: %s", e)

    def flush_batch(self) -> None:
        """미커밋 레코드 즉시 commit — 앱 종료 또는 명시적 flush 시 호출."""
        try:
            self._conn.commit()
        except Exception as e:
            log.warning("ExperimentRunner.flush_batch 오류: %s", e)

    def get_session_summary(self, session_date: Optional[str] = None) -> dict:
        """
        당일 Champion vs Challenger 요약 통계.
        분석 API(/api/experiment_summary)에서 활용.
        """
        date = session_date or time.strftime("%Y-%m-%d")
        try:
            cur = self._conn.execute(
                """
                SELECT
                    COUNT(*)                        AS total_ticks,
                    SUM(diverged)                   AS diverged_ticks,
                    SUM(CASE WHEN param_hash_same=0 THEN 1 ELSE 0 END) AS param_diff_ticks,
                    ROUND(AVG(score_delta_long), 4)  AS avg_delta_long,
                    ROUND(AVG(score_delta_short), 4) AS avg_delta_short,
                    COUNT(DISTINCT champion_param_hash)   AS champion_configs,
                    COUNT(DISTINCT challenger_param_hash) AS challenger_configs
                FROM champion_vs_challenger
                WHERE session_date = ?
                """,
                (date,),
            )
            row = cur.fetchone()
            if not row or row[0] == 0:
                return {"session_date": date, "total_ticks": 0}
            cols = [d[0] for d in cur.description]
            result = dict(zip(cols, row))
            result["session_date"] = date
            result["diverge_rate"] = (
                round(result["diverged_ticks"] / result["total_ticks"] * 100, 1)
                if result["total_ticks"] > 0 else 0.0
            )
            return result
        except Exception as e:
            log.warning("ExperimentRunner.get_session_summary 오류: %s", e)
            return {"session_date": date, "error": str(e)}

    def close(self) -> None:
        try:
            self._conn.commit()
            self._conn.close()
        except Exception:
            pass

    # ------------------------------------------------------------------
    # 내부 헬퍼
    # ------------------------------------------------------------------
    def _apply_snap_to_engine(self, engine: Any, snap: Any) -> None:
        """ParamSnapshot의 trend 파라미터를 엔진 ThresholdConfig에 주입."""
        tc = engine.cfg
        tc.trend_weight_long         = snap.trend_weight_long
        tc.trend_weight_short        = snap.trend_weight_short
        tc.trend_ema_align_score     = snap.trend_ema_align_score
        tc.trend_ema_slope_score     = snap.trend_ema_slope_score
        tc.trend_ema_slope_threshold = snap.trend_ema_slope_threshold


# ===================================================================
# 싱글턴 접근자
# ===================================================================
_runner_instance: Optional[ExperimentRunner] = None
_runner_lock = threading.Lock()


def get_experiment_runner(
    db_path: str = "",
    param_store: Any = None,
) -> ExperimentRunner:
    """
    ExperimentRunner 싱글턴 반환.
    최초 호출 시 db_path와 param_store 필수.
    """
    global _runner_instance
    if _runner_instance is None:
        with _runner_lock:
            if _runner_instance is None:
                if not db_path or param_store is None:
                    raise RuntimeError(
                        "ExperimentRunner 최초 생성 시 db_path와 param_store 필수"
                    )
                _runner_instance = ExperimentRunner(db_path, param_store)
    return _runner_instance
