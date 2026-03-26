# engines/pulse_logger.py
"""
PulseLogger + PulseScorer — 판정 기록 및 사후 자동 채점

역할:
  - PulseEngine 판정 시점의 전체 상태를 DB에 기록
  - 판정 후 5분/10분/15분 뒤 실제 시장 결과를 자동 채점
  - 실패 유형 분류 (overreact / slow / sideways_err)
  - PulseAnalyzer의 데이터 소스

채점 기준:
  방향=BULLISH  → 5분후 가격 상승이면 success, 하락이면 fail
  방향=BEARISH  → 5분후 가격 하락이면 success, 상승이면 fail
  방향=NEUTRAL  → 항상 neutral (채점 대상 아님)
  성공 판단 임계: 가격 변동 > 0.3pt (ATR 기반으로 추후 보정 예정)

실패 유형:
  overreact   : BULLISH/BEARISH 판정 후 반대 방향으로 1pt 이상 움직임
  slow        : 5분 채점 fail인데 10분 채점 success (늦게 맞음)
  sideways_err: NEUTRAL 판정 후 실제로 1.5pt 이상 움직임 (횡보 오판)
"""
from __future__ import annotations

import logging
import sqlite3
import threading
import time
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

log = logging.getLogger("MMEAN.PulseLogger")

# 채점 임계값
SCORE_MOVE_MIN  = 0.3    # 방향성 판단 최소 가격 변동 (pt)
SCORE_MOVE_SIDE = 1.5    # NEUTRAL 오판 판단 임계 (pt)
SCORE_FAST_FAIL = 1.0    # overreact 판단 반대 변동 임계 (pt)


class PulseScorer:
    """
    5분/10분/15분 자동 채점기.
    PulseEngine과 독립적으로 동작하는 백그라운드 스레드.
    """

    def __init__(self, db_path: str, interval_sec: int = 60) -> None:
        self._db_path = db_path
        self._interval = interval_sec
        self._stop = threading.Event()
        self._thread: Optional[threading.Thread] = None

    def start(self) -> None:
        if self._thread and self._thread.is_alive():
            return
        self._stop.clear()
        self._thread = threading.Thread(
            target=self._loop, daemon=True, name="MMEAN-PulseScorer",
        )
        self._thread.start()
        log.info("PulseScorer 시작 | check_interval=%ds", self._interval)

    def stop(self) -> None:
        self._stop.set()
        if self._thread:
            self._thread.join(timeout=5)

    def _loop(self) -> None:
        while not self._stop.is_set():
            try:
                self._run_scoring()
            except Exception as e:
                log.warning("PulseScorer 루프 오류: %s", e)
            self._stop.wait(self._interval)

    def _run_scoring(self) -> None:
        """미채점 신호들에 대해 5/10/15분 후 가격 조회 후 채점."""
        now = datetime.now()
        try:
            conn = sqlite3.connect(self._db_path, timeout=1)
            conn.row_factory = sqlite3.Row

            # 채점 대상: price_at_signal이 있고 result_15m이 아직 없는 오늘 신호
            today = now.strftime("%Y-%m-%d")
            pending = conn.execute(
                """SELECT id, datetime_full, direction, price_at_signal,
                          price_5m_later, price_10m_later, price_15m_later,
                          result_5m, result_10m, result_15m
                   FROM pulse_signals
                   WHERE date = ?
                     AND price_at_signal IS NOT NULL
                     AND price_at_signal > 0
                     AND result_15m IS NULL
                   ORDER BY id""",
                (today,),
            ).fetchall()

            if not pending:
                conn.close()
                return

            # regime_ticks에서 해당 시점 이후 가격 조회
            for row in pending:
                sig_dt = datetime.strptime(row["datetime_full"], "%Y-%m-%d %H:%M:%S")
                updates = {}

                for offset_min, col_price, col_result in [
                    (5,  "price_5m_later",  "result_5m"),
                    (10, "price_10m_later", "result_10m"),
                    (15, "price_15m_later", "result_15m"),
                ]:
                    # 이미 채점된 항목 스킵
                    if row[col_result] is not None:
                        continue

                    target_dt = sig_dt + timedelta(minutes=offset_min)
                    if target_dt > now:
                        continue  # 아직 시간이 안 됨

                    # regime_ticks에서 가장 가까운 가격 조회
                    target_str = target_dt.strftime("%Y-%m-%d %H:%M:%S")
                    price_row = conn.execute(
                        """SELECT futures_price FROM regime_ticks
                           WHERE ts >= ? AND substr(ts,1,10) = ?
                             AND futures_price > 100
                           ORDER BY ts LIMIT 1""",
                        (target_str, today),
                    ).fetchone()

                    if price_row is None:
                        continue

                    later_price = float(price_row["futures_price"])
                    at_price    = float(row["price_at_signal"])
                    direction   = row["direction"]
                    move        = later_price - at_price

                    result = self._judge(direction, move, offset_min)
                    updates[col_price]  = later_price
                    updates[col_result] = result

                if updates:
                    # 실패 유형 분류
                    fail_type = None
                    if updates.get("result_5m") == "fail":
                        if abs(updates.get("price_5m_later", at_price) - at_price) > SCORE_FAST_FAIL:
                            fail_type = "overreact"
                        else:
                            fail_type = "slow"
                    if updates.get("result_15m") == "success" and fail_type == "slow":
                        fail_type = "slow"  # 늦게 맞음
                    if direction == "NEUTRAL":
                        p5 = updates.get("price_5m_later")
                        if p5 and abs(p5 - at_price) > SCORE_MOVE_SIDE:
                            fail_type = "sideways_err"

                    if fail_type:
                        updates["fail_type"] = fail_type

                    set_clause = ", ".join(f"{k}=?" for k in updates)
                    vals       = list(updates.values()) + [row["id"]]
                    conn.execute(
                        f"UPDATE pulse_signals SET {set_clause} WHERE id=?", vals
                    )
                    conn.commit()
                    log.debug(
                        "PulseScorer 채점 | id=%d dir=%s %s",
                        row["id"], direction, updates,
                    )

            conn.close()

        except Exception as e:
            log.warning("PulseScorer 채점 오류: %s", e)

    def _judge(self, direction: str, move: float, offset_min: int) -> str:
        if direction == "NEUTRAL":
            return "neutral"
        is_bullish = direction == "BULLISH"
        if is_bullish and move >= SCORE_MOVE_MIN:
            return "success"
        elif not is_bullish and move <= -SCORE_MOVE_MIN:
            return "success"
        elif abs(move) < SCORE_MOVE_MIN:
            return "neutral"
        return "fail"
