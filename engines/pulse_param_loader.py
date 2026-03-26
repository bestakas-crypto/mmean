# engines/pulse_param_loader.py
"""
PulseParamLoader — pulse_sim.db 상위 결과를 PulseEngine에 주입

방식:
  - method='ensemble' : 상위 top_n 결과의 파라미터를 가중 평균 (objective 비례)
  - method='best'     : 1위 파라미터 그대로 사용

사용:
  loader = PulseParamLoader(pulse_sim_db_path, top_n=5, method='ensemble')
  params = loader.load()           # dict 반환 (없으면 None)
  pulse_engine.update_params(params)
"""
from __future__ import annotations

import json
import logging
import sqlite3
from typing import Dict, List, Optional

log = logging.getLogger("MMEAN.PulseParamLoader")

# 최소 objective 기준 — 이 점수 이하는 쓰레기 결과로 제외
MIN_OBJECTIVE = 5.0
# 최근 며칠 데이터를 대상으로 할지 (NULL이면 전체)
LOOKBACK_DAYS = 30


class PulseParamLoader:
    """
    pulse_sim.db → PulseEngine 파라미터 로더.

    Args:
        db_path    : pulse_sim.db 경로
        top_n      : 상위 몇 개를 사용할지 (default 5)
        method     : 'ensemble' (가중평균) | 'best' (1위)
        min_obj    : 최소 objective 점수 (default 5.0)
        lookback_days: 최근 N일 데이터만 사용 (0=전체)
    """

    def __init__(
        self,
        db_path: str,
        top_n: int = 5,
        method: str = "ensemble",
        min_obj: float = MIN_OBJECTIVE,
        lookback_days: int = LOOKBACK_DAYS,
    ) -> None:
        self._db_path      = db_path
        self._top_n        = top_n
        self._method       = method
        self._min_obj      = min_obj
        self._lookback_days = lookback_days

    def load(self) -> Optional[Dict]:
        """
        pulse_sim.db에서 상위 파라미터를 로드해 반환.
        데이터 없거나 오류 시 None 반환 (호출자는 기본값 유지).
        """
        try:
            rows = self._fetch_top_rows()
        except Exception as e:
            log.warning("PulseParamLoader DB 조회 실패: %s", e)
            return None

        if not rows:
            log.info("PulseParamLoader 유효 결과 없음 (min_obj=%.1f) — 기본값 유지", self._min_obj)
            return None

        if self._method == "best":
            params = self._pick_best(rows)
        else:
            params = self._ensemble(rows)

        log.info(
            "PulseParamLoader 로드 완료 | method=%s top_n=%d/%d | "
            "a_bull=%.1f fgn=%.0f flow=%.2f/%.2f ls=%.3f ema=%.3f",
            self._method, len(rows), self._top_n,
            params.get("a_bullish_thresh", 4.0),
            params.get("fgn_delta_strong", 1000.0),
            params.get("flow_upper", 0.5),
            params.get("flow_lower", 0.1),
            params.get("ls_diff_thresh", 0.2),
            params.get("ema_slope_thresh", 0.05),
        )
        return params

    # ── 내부 메서드 ──────────────────────────────────────────────────────────

    def _fetch_top_rows(self) -> List[Dict]:
        """pulse_sim.db에서 상위 결과 조회."""
        conn = sqlite3.connect(self._db_path, timeout=10)
        conn.row_factory = sqlite3.Row
        try:
            if self._lookback_days > 0:
                sql = """
                    SELECT config_json, objective
                    FROM pulse_runs
                    WHERE objective >= ?
                      AND created_at >= datetime('now', ?, 'localtime')
                    ORDER BY objective DESC
                    LIMIT ?
                """
                rows = conn.execute(
                    sql,
                    (self._min_obj, f"-{self._lookback_days} days", self._top_n),
                ).fetchall()
            else:
                sql = """
                    SELECT config_json, objective
                    FROM pulse_runs
                    WHERE objective >= ?
                    ORDER BY objective DESC
                    LIMIT ?
                """
                rows = conn.execute(sql, (self._min_obj, self._top_n)).fetchall()
        finally:
            conn.close()

        result = []
        for r in rows:
            try:
                cfg = json.loads(r["config_json"])
                cfg["_objective"] = float(r["objective"])
                result.append(cfg)
            except Exception:
                continue
        return result

    def _pick_best(self, rows: List[Dict]) -> Dict:
        """1위 파라미터 반환."""
        best = rows[0]
        return {k: v for k, v in best.items() if not k.startswith("_")}

    def _ensemble(self, rows: List[Dict]) -> Dict:
        """objective 비례 가중 평균."""
        objectives = [r["_objective"] for r in rows]
        total_w = sum(objectives)
        if total_w <= 0:
            return self._pick_best(rows)

        # 파라미터 키 목록 (내부 메타 키 제외)
        param_keys = [k for k in rows[0] if not k.startswith("_")]

        averaged: Dict = {}
        for k in param_keys:
            weighted_sum = sum(r.get(k, 0) * r["_objective"] for r in rows)
            val = weighted_sum / total_w
            # int 파라미터는 반올림
            if isinstance(rows[0].get(k), int):
                val = int(round(val))
            else:
                val = round(val, 6)
            averaged[k] = val

        # 제약조건 보정
        if averaged.get("flow_lower", 0) >= averaged.get("flow_upper", 1):
            averaged["flow_lower"] = round(averaged["flow_upper"] * 0.3, 4)
        if averaged.get("confidence_min_entry", 0) >= averaged.get("confidence_min_confirm", 1):
            averaged["confidence_min_entry"] = round(averaged["confidence_min_confirm"] * 0.7, 4)

        return averaged
