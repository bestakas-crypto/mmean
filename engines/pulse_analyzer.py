# engines/pulse_analyzer.py
"""
PulseAnalyzer — 누적 로그 분석기 (STEP 6)

역할:
  - pulse_signals 누적 데이터 분석
  - 지표별 기여도, 실패 유형, 정확도, flip 횟수, 횡보 오판율 산출
  - PulseCalibrator (추후)의 데이터 소스

주요 분석:
  1. 방향 정확도  (5분/10분/15분)
  2. 실패 유형 분포 (overreact/slow/sideways_err)
  3. confirmed vs weak 정확도 비교
  4. 지표 스코어 vs 결과 상관 (단순 방향별 평균)
  5. 시간대별 성공률
  6. 추세 전환 감지 속도
"""
from __future__ import annotations

import logging
import sqlite3
from collections import defaultdict
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

log = logging.getLogger("MMEAN.PulseAnalyzer")


class PulseAnalyzer:

    def __init__(self, db_path: str) -> None:
        self._db_path = db_path

    def analyze(self, days: int = 30) -> Dict:
        """
        최근 N일 분석 결과 반환.
        /api/pulse/analysis?days=N 에서 호출.
        """
        since = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d")
        try:
            conn = sqlite3.connect(self._db_path, timeout=10)
            conn.row_factory = sqlite3.Row
            rows = conn.execute(
                """SELECT date, ts, direction, confidence, status,
                          consecutive, trend_confirmed,
                          score_direction, score_confidence,
                          score_fgn_delta, score_flow, score_oi, score_ema, score_basis,
                          price_at_signal, price_5m_later, price_10m_later, price_15m_later,
                          result_5m, result_10m, result_15m, fail_type
                   FROM pulse_signals
                   WHERE date >= ?
                   ORDER BY datetime_full""",
                (since,),
            ).fetchall()
            conn.close()
        except Exception as e:
            log.error("PulseAnalyzer DB 조회 실패: %s", e)
            return {"ok": False, "error": str(e)}

        if not rows:
            return {"ok": True, "days": days, "since": since, "total": 0}

        return {
            "ok":        True,
            "days":      days,
            "since":     since,
            "total":     len(rows),
            "accuracy":  self._accuracy(rows),
            "fail_types":self._fail_types(rows),
            "by_status": self._by_status(rows),
            "by_hour":   self._by_hour(rows),
            "score_dist":self._score_dist(rows),
            "flip_rate": self._flip_rate(rows),
        }

    # ── 분석 메서드 ──────────────────────────────────────────────────────────

    def _accuracy(self, rows) -> Dict:
        """5분/10분/15분 성공률."""
        result = {}
        for offset, col in [(5, "result_5m"), (10, "result_10m"), (15, "result_15m")]:
            scored = [r for r in rows if r[col] in ("success", "fail")]
            if not scored:
                result[f"{offset}m"] = None
                continue
            success = sum(1 for r in scored if r[col] == "success")
            result[f"{offset}m"] = {
                "total":   len(scored),
                "success": success,
                "fail":    len(scored) - success,
                "rate":    round(success / len(scored) * 100, 1),
            }
        return result

    def _fail_types(self, rows) -> Dict:
        """실패 유형 분포."""
        counts = defaultdict(int)
        for r in rows:
            ft = r["fail_type"]
            if ft:
                counts[ft] += 1
        total = sum(counts.values())
        return {
            "total":       total,
            "overreact":   counts.get("overreact", 0),
            "slow":        counts.get("slow", 0),
            "sideways_err":counts.get("sideways_err", 0),
        }

    def _by_status(self, rows) -> Dict:
        """confirmed vs weak 정확도 비교."""
        result = {}
        for status in ("confirmed", "weak", "transition"):
            sub = [r for r in rows if r["status"] == status]
            if not sub:
                continue
            scored = [r for r in sub if r["result_5m"] in ("success", "fail")]
            if not scored:
                result[status] = {"total": len(sub), "scored": 0}
                continue
            success = sum(1 for r in scored if r["result_5m"] == "success")
            result[status] = {
                "total":   len(sub),
                "scored":  len(scored),
                "success": success,
                "rate_5m": round(success / len(scored) * 100, 1),
            }
        return result

    def _by_hour(self, rows) -> List[Dict]:
        """시간대별 성공률."""
        hour_map: Dict[int, Dict] = {}
        for r in rows:
            try:
                h = int(r["ts"][:2])
            except Exception:
                continue
            if h not in hour_map:
                hour_map[h] = {"total": 0, "success": 0, "scored": 0}
            hour_map[h]["total"] += 1
            if r["result_5m"] in ("success", "fail"):
                hour_map[h]["scored"] += 1
                if r["result_5m"] == "success":
                    hour_map[h]["success"] += 1
        result = []
        for h in sorted(hour_map):
            d = hour_map[h]
            rate = round(d["success"] / d["scored"] * 100, 1) if d["scored"] else None
            result.append({"hour": h, **d, "rate_5m": rate})
        return result

    def _score_dist(self, rows) -> Dict:
        """방향 스코어 vs 성공률 — 스코어 구간별."""
        buckets: Dict[str, Dict] = {}
        for r in rows:
            sc = r["score_direction"]
            if sc is None:
                continue
            if sc >= 6:    key = "strong_bull"
            elif sc >= 4:  key = "bull"
            elif sc >= 0:  key = "mild_bull"
            elif sc >= -4: key = "mild_bear"
            elif sc >= -6: key = "bear"
            else:          key = "strong_bear"

            if key not in buckets:
                buckets[key] = {"total": 0, "success": 0, "scored": 0}
            buckets[key]["total"] += 1
            if r["result_5m"] in ("success", "fail"):
                buckets[key]["scored"] += 1
                if r["result_5m"] == "success":
                    buckets[key]["success"] += 1

        for v in buckets.values():
            v["rate"] = round(v["success"] / v["scored"] * 100, 1) if v["scored"] else None
        return buckets

    def _flip_rate(self, rows) -> Dict:
        """방향 전환 빈도 분석."""
        if len(rows) < 2:
            return {"flips": 0, "total": len(rows), "rate": 0.0}
        flips = sum(
            1 for i in range(1, len(rows))
            if rows[i]["direction"] != rows[i-1]["direction"]
        )
        return {
            "flips": flips,
            "total": len(rows),
            "rate":  round(flips / (len(rows) - 1) * 100, 1),
        }
