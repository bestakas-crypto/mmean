
# sim_opt/pulse_sim.py
"""
PulseEngine 내부 가중치/임계치 최적화 — Monte Carlo + Bayesian (Optuna)

목적:
  - PulseEngine의 A/B 그룹 임계값과 스코어 분기점을 최적화
  - 목적함수: 신호 품질 (PnL이 아닌 방향 정확도 + 신뢰도 적중률)
  - 데이터: mmean.db 의 regime_ticks (읽기 전용)
  - 결과: storage/pulse_sim.db 에 저장

목적함수 구성:
  - direction_accuracy: entry_allowed 신호의 5분 후 방향 적중률
  - confirmed_ratio:    confirmed 신호 비율 (너무 많아도 너무 적어도 패널티)
  - entry_rate:         전체 5분 틱 중 entry_allowed 비율 (1~10% 선호)
  - NEUTRAL 과다 패널티: 실제 방향이 있는 틱에서 NEUTRAL 판정 비율

사용:
  python sim_opt/pulse_sim.py --dates 2026-03-23
  python sim_opt/pulse_sim.py --dates 2026-03-18,2026-03-19,2026-03-20 --random 500 --bayes 200
  python sim_opt/pulse_sim.py --top 10  # 결과 조회
"""

from __future__ import annotations

import argparse
import hashlib
import json
import logging
import os
import random
import sqlite3
import sys
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple

log = logging.getLogger("MMEAN.PulseSim")

# ── 경로 ─────────────────────────────────────────────────────────────────
_ROOT    = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
_STORAGE = os.path.join(_ROOT, "storage")
_SOURCE_DB = os.path.join(_STORAGE, "mmean.db")
_SIM_DB    = os.path.join(_STORAGE, "pulse_sim.db")

# ── 정규장 시간 ───────────────────────────────────────────────────────────
_DAY_START = "09:00:00"
_DAY_END   = "15:35:00"

# ── 5분 집계 윈도우 ───────────────────────────────────────────────────────
_INTERVAL_MIN = 5

# ── DB DDL ────────────────────────────────────────────────────────────────
_DDL = """
CREATE TABLE IF NOT EXISTS pulse_runs (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    study_name      TEXT    NOT NULL,
    run_type        TEXT    NOT NULL,   -- 'random' | 'bayes_opt'
    trial_no        INTEGER NOT NULL,
    status          TEXT    NOT NULL DEFAULT 'done',
    dates           TEXT    NOT NULL,
    config_hash     TEXT    NOT NULL,
    config_json     TEXT    NOT NULL,
    -- 결과 지표
    objective       REAL,
    direction_acc   REAL,
    confirmed_ratio REAL,
    entry_rate      REAL,
    neutral_penalty REAL,
    signal_count    INTEGER,
    entry_count     INTEGER,
    -- 메타
    created_at      TEXT    DEFAULT (datetime('now','localtime'))
);
CREATE INDEX IF NOT EXISTS idx_pulse_runs_study  ON pulse_runs(study_name);
CREATE INDEX IF NOT EXISTS idx_pulse_runs_obj    ON pulse_runs(objective DESC);
CREATE INDEX IF NOT EXISTS idx_pulse_runs_hash   ON pulse_runs(config_hash);

CREATE TABLE IF NOT EXISTS pulse_adoptions (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id      INTEGER NOT NULL REFERENCES pulse_runs(id),
    config_json TEXT    NOT NULL,
    objective   REAL    NOT NULL,
    adopted_at  TEXT    DEFAULT (datetime('now','localtime'))
);
"""

# ── 탐색 공간 ─────────────────────────────────────────────────────────────
# PulseEngine 상수와 1:1 대응
_SEARCH_SPACE: Dict[str, tuple] = {
    # A-group 임계값
    "a_bullish_thresh":     ("float", 2.0, 8.0),    # 기본 +4
    # a_bearish_thresh = -a_bullish_thresh (대칭)

    # A1: 외국인 delta 강도 기준
    "fgn_delta_strong":     ("float", 200.0, 3000.0),  # 기본 1000

    # A3: flow_score 분기점 (upper/lower 두 개, upper > lower)
    "flow_upper":           ("float", 0.15, 0.8),   # 기본 0.5
    "flow_lower":           ("float", 0.02, 0.3),   # 기본 0.1

    # A4: long-short 차이 임계값
    "ls_diff_thresh":       ("float", 0.05, 0.5),   # 기본 0.2

    # B2: EMA 기울기 임계값
    "ema_slope_thresh":     ("float", 0.01, 0.2),   # 기본 0.05

    # B3: 베이시스 고/저 임계값
    "basis_high":           ("float", 0.05, 1.0),   # 기본 0.3
    # basis_low = -basis_high (대칭)

    # B-group 확정 임계값
    "b_confirm_min":        ("int",   0,   3),       # 기본 1

    # 추세 확정 연속 횟수
    "trend_confirm_count":  ("int",   2,   6),       # 기본 3

    # 신뢰도 임계값
    "confidence_min_confirm": ("float", 0.2, 0.8),  # 기본 0.5
    "confidence_min_entry":   ("float", 0.1, 0.6),  # 기본 0.3
}


# ── PulseEngine 로직 (독립 구현, runtime 불필요) ──────────────────────────

def _score_a1(fgn_delta: float, fgn_delta_strong: float) -> float:
    """외국인 선물 delta (최대 ±4)."""
    if fgn_delta > fgn_delta_strong:    return +4.0
    elif fgn_delta > 0:                 return +2.0
    elif fgn_delta == 0:                return  0.0
    elif fgn_delta >= -fgn_delta_strong: return -2.0
    else:                               return -4.0


def _score_a2(fgn_abs: float, fgn_delta: float) -> float:
    """외국인 절대+delta 조합 (최대 ±2)."""
    if fgn_abs > 0 and fgn_delta > 0:   return +2.0
    elif fgn_abs > 0 and fgn_delta <= 0: return +1.0
    elif fgn_abs < 0 and fgn_delta >= 0: return -1.0
    elif fgn_abs < 0 and fgn_delta < 0:  return -2.0
    return 0.0


def _score_a3(flow: float, upper: float, lower: float) -> float:
    """flow_score (최대 ±2)."""
    if flow > upper:            return +2.0
    elif flow > lower:          return +1.0
    elif flow >= -lower:        return  0.0
    elif flow >= -upper:        return -1.0
    else:                       return -2.0


def _score_a4(long_s: float, short_s: float, ls_thresh: float) -> float:
    """long-short 차이 (최대 ±1)."""
    diff = long_s - short_s
    if diff >= ls_thresh:    return +1.0
    elif diff <= -ls_thresh: return -1.0
    return 0.0


def _score_b1(oi_delta: float, price: float, vwap: float) -> float:
    """OI delta × 가격 방향 조합 (최대 ±2)."""
    if oi_delta == 0: return 0.0
    price_up = price >= vwap
    if oi_delta > 0 and price_up:      return +2.0
    elif oi_delta > 0 and not price_up: return -2.0
    elif oi_delta < 0 and price_up:     return +1.0
    else:                               return -1.0


def _score_b2(ema_slope: float, thresh: float) -> float:
    """EMA 기울기 (최대 ±1)."""
    if ema_slope >= thresh:          return +1.0
    elif ema_slope >= thresh * 0.5:  return +0.5
    elif ema_slope <= -thresh:       return -1.0
    elif ema_slope <= -thresh * 0.5: return -0.5
    return 0.0


def _score_b3(basis: float, basis_high: float) -> float:
    """베이시스 (최대 ±1)."""
    if basis > basis_high:   return -1.0
    elif basis < -basis_high: return +1.0
    return 0.0


def _data_quality(futures_price: float, flow_score: float, fgn_abs: float,
                  oi_value: float, ema_slope: float, basis: float) -> Tuple[float, int, int]:
    """신뢰도 초기값 계산."""
    conf = 1.0
    mc = 0
    ms = 0
    if futures_price <= 0: conf -= 0.3; mc += 1
    if flow_score == 0.0:  conf -= 0.3; mc += 1
    if fgn_abs == 0.0:     conf -= 0.3; mc += 1
    if oi_value == 0.0:    conf -= 0.1; ms += 1
    if ema_slope == 0.0:   conf -= 0.1; ms += 1
    if basis == 0.0:       conf -= 0.1; ms += 1
    return max(0.0, conf), mc, ms


def simulate_pulse(ticks_5m: List[Dict], params: Dict) -> Dict:
    """
    주어진 파라미터로 PulseEngine 로직을 재현하여 신호 품질 측정.

    Returns:
        objective, direction_acc, confirmed_ratio, entry_rate,
        neutral_penalty, signal_count, entry_count
    """
    p = params
    a_bull   = p["a_bullish_thresh"]
    a_bear   = -a_bull
    fds      = p["fgn_delta_strong"]
    fu       = p["flow_upper"]
    fl       = p["flow_lower"]
    ls_th    = p["ls_diff_thresh"]
    ema_th   = p["ema_slope_thresh"]
    b_high   = p["basis_high"]
    b_cmin   = p["b_confirm_min"]
    tcc      = p["trend_confirm_count"]
    cmc      = p["confidence_min_confirm"]
    cme      = p["confidence_min_entry"]

    # 추세 상태
    consecutive   = 0
    trend_dir     = "NEUTRAL"
    trend_conf    = False
    opposite_cnt  = 0
    prev_fgn_abs  = None

    signals: List[Dict] = []

    for i, tick in enumerate(ticks_5m):
        fp    = float(tick.get("futures_price") or 0.0)
        flow  = float(tick.get("flow_score")    or 0.0)
        long_s= float(tick.get("long_score")    or 0.0)
        short_s= float(tick.get("short_score")  or 0.0)
        ema   = float(tick.get("ema_fast_slope")or 0.0)
        oi_v  = float(tick.get("oi_value")      or 0.0)
        oi_d  = float(tick.get("oi_delta")      or 0.0)
        basis = float(tick.get("basis")         or 0.0)
        vwap  = float(tick.get("vwap")          or fp)
        fgn_abs = float(tick.get("foreign_buy") or 0.0)

        fgn_delta = 0.0 if prev_fgn_abs is None else fgn_abs - prev_fgn_abs
        prev_fgn_abs = fgn_abs

        # 데이터 품질
        conf, mc, ms = _data_quality(fp, flow, fgn_abs, oi_v, ema, basis)

        # A-group
        a1 = _score_a1(fgn_delta, fds)
        a2 = _score_a2(fgn_abs, fgn_delta)
        a3 = _score_a3(flow, fu, fl)
        a4 = _score_a4(long_s, short_s, ls_th)
        a_total = a1 + a2 + a3 + a4
        if mc > 0:
            a_total *= (1.0 - mc * 0.3)

        # 방향 후보
        if a_total >= a_bull:   candidate = "BULLISH"
        elif a_total <= a_bear: candidate = "BEARISH"
        else:                   candidate = "NEUTRAL"

        # B-group
        sign = +1 if candidate == "BULLISH" else (-1 if candidate == "BEARISH" else 0)
        b1 = _score_b1(oi_d, fp, vwap) * sign
        b2 = _score_b2(ema, ema_th)    * sign
        b3 = _score_b3(basis, b_high)  * sign
        b_total = b1 + b2 + b3
        if ms > 0:
            b_total = max(-4.0, b_total - ms * 0.5)

        # 상태 결정
        if candidate == "NEUTRAL":
            direction = "NEUTRAL"
            status    = "weak"
        elif b_total >= b_cmin:
            direction = candidate
            status    = "confirmed"
        elif b_total == 0:
            direction = candidate
            status    = "weak"
        else:
            direction = "NEUTRAL"
            status    = "transition"

        # confidence 보정
        conf = max(0.0, min(1.0, conf))
        if status != "confirmed":
            conf *= 0.7
        if conf < cmc:
            status    = "weak"

        # 추세 갱신
        if status == "confirmed":
            if direction == trend_dir:
                consecutive  += 1
                opposite_cnt  = 0
            else:
                if trend_conf:
                    opposite_cnt += 1
                    if opposite_cnt >= tcc:
                        trend_dir    = direction
                        trend_conf   = True
                        consecutive  = tcc
                        opposite_cnt = 0
                else:
                    trend_dir    = direction
                    consecutive  = 1
                    opposite_cnt = 0

            if consecutive >= tcc and not trend_conf:
                trend_conf = True

        # 진입 허용
        entry_allowed = (
            trend_conf
            and status == "confirmed"
            and conf >= cme
        )

        # 5분 후 / 10분 후 가격
        fp_5m  = float(ticks_5m[i+1]["futures_price"]) if i+1 < len(ticks_5m) else None
        fp_10m = float(ticks_5m[i+2]["futures_price"]) if i+2 < len(ticks_5m) else None

        # 방향 적중 (5분 후 기준)
        correct_5m = None
        if fp_5m is not None and fp > 0:
            if direction == "BULLISH":
                correct_5m = fp_5m > fp
            elif direction == "BEARISH":
                correct_5m = fp_5m < fp

        signals.append({
            "ts":           tick.get("ts", ""),
            "direction":    direction,
            "status":       status,
            "candidate":    candidate,
            "a_total":      a_total,
            "b_total":      b_total,
            "confidence":   round(conf, 3),
            "trend_conf":   trend_conf,
            "consecutive":  consecutive,
            "entry_allowed": entry_allowed,
            "fp":           fp,
            "fp_5m":        fp_5m,
            "correct_5m":   correct_5m,
        })

    # ── 목적함수 계산 ────────────────────────────────────────────────────
    total_sigs    = len(signals)
    entry_sigs    = [s for s in signals if s["entry_allowed"]]
    entry_count   = len(entry_sigs)
    confirmed_all = [s for s in signals if s["status"] == "confirmed"]

    if total_sigs == 0:
        return {
            "objective": -200.0, "direction_acc": 0.0, "confirmed_ratio": 0.0,
            "entry_rate": 0.0, "neutral_penalty": 0.0,
            "signal_count": 0, "entry_count": 0,
        }

    # 방향 정확도 (entry_allowed 신호)
    evalauble = [s for s in entry_sigs if s["correct_5m"] is not None]
    direction_acc = (
        sum(1 for s in evalauble if s["correct_5m"]) / len(evalauble)
        if evalauble else 0.0
    )

    # confirmed 비율
    confirmed_ratio = len(confirmed_all) / total_sigs

    # entry_rate (전체 대비 진입 허용 비율)
    entry_rate = entry_count / total_sigs

    # NEUTRAL 과다 패널티:
    # 실제로 방향이 있는데(A스코어 강한데) NEUTRAL로 판정한 비율
    strong_a = [s for s in signals if abs(s["a_total"]) >= a_bull * 0.7]
    neutral_of_strong = sum(1 for s in strong_a if s["direction"] == "NEUTRAL")
    neutral_penalty = (neutral_of_strong / len(strong_a)) if strong_a else 0.0

    # entry_count 패널티 (너무 적거나 없으면)
    entry_bonus = 0.0
    if entry_count == 0:
        entry_bonus = -50.0
    elif entry_count < 3:
        entry_bonus = -10.0 * (3 - entry_count)
    elif entry_count > total_sigs * 0.3:   # 30% 초과면 너무 많음
        entry_bonus = -5.0

    # entry_rate 선호: 3~15% 범위
    rate_bonus = 0.0
    if 0.03 <= entry_rate <= 0.15:
        rate_bonus = 10.0
    elif entry_rate > 0.25:
        rate_bonus = -10.0

    # 목적함수 합산
    objective = (
        direction_acc   * 100.0   # 방향 정확도 (0~100)
        + confirmed_ratio * 20.0  # confirmed 비율 (0~20)
        - neutral_penalty * 30.0  # NEUTRAL 오판 패널티 (0~-30)
        + entry_bonus             # entry count 보정
        + rate_bonus              # entry rate 보정
    )

    return {
        "objective":       round(objective, 4),
        "direction_acc":   round(direction_acc, 4),
        "confirmed_ratio": round(confirmed_ratio, 4),
        "entry_rate":      round(entry_rate, 4),
        "neutral_penalty": round(neutral_penalty, 4),
        "signal_count":    total_sigs,
        "entry_count":     entry_count,
    }


# ── 데이터 로드 ───────────────────────────────────────────────────────────

def load_ticks_5m(source_db: str, dates: List[str]) -> Dict[str, List[Dict]]:
    """
    regime_ticks에서 날짜별 5분 간격 집계 데이터 로드.
    각 5분 윈도우의 마지막 tick 사용.
    """
    conn = sqlite3.connect(source_db, timeout=10)
    conn.row_factory = sqlite3.Row
    result: Dict[str, List[Dict]] = {}

    for date in dates:
        rows = conn.execute(
            """
            SELECT ts, futures_price, spot_price, basis, foreign_buy,
                   oi_value, oi_delta, flow_score, long_score, short_score,
                   ema_fast_slope, vwap
            FROM regime_ticks
            WHERE substr(ts,1,10)=?
              AND time(substr(ts,12,8)) BETWEEN ? AND ?
            ORDER BY ts
            """,
            (date, _DAY_START, _DAY_END),
        ).fetchall()

        if not rows:
            log.warning("[LOAD] %s — 데이터 없음", date)
            continue

        # 5분 버킷으로 집계 (버킷 = HH:MM 에서 MM을 5분 단위로 내림)
        buckets: Dict[str, Dict] = {}
        for r in rows:
            ts_str = r["ts"]
            try:
                dt = datetime.strptime(ts_str[:19], "%Y-%m-%d %H:%M:%S")
            except ValueError:
                continue
            bucket_min = (dt.minute // _INTERVAL_MIN) * _INTERVAL_MIN
            bucket_key = dt.strftime(f"%Y-%m-%d %H:{bucket_min:02d}:00")
            buckets[bucket_key] = dict(r)  # 마지막 tick으로 덮어씌움

        # 시간순 정렬
        ticks_sorted = [v for _, v in sorted(buckets.items())]
        result[date] = ticks_sorted
        log.info("[LOAD] %s — %d ticks → %d 5m-buckets", date, len(rows), len(ticks_sorted))

    conn.close()
    return result


# ── DB 유틸 ───────────────────────────────────────────────────────────────

def _init_db(sim_db: str) -> None:
    conn = sqlite3.connect(sim_db, timeout=30)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.executescript(_DDL)
    conn.commit()
    conn.close()
    log.info("[DB] 초기화 완료 | %s", sim_db)


def _connect(sim_db: str) -> sqlite3.Connection:
    conn = sqlite3.connect(sim_db, timeout=30)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.row_factory = sqlite3.Row
    return conn


def _save_run(conn: sqlite3.Connection, study_name: str, run_type: str,
              trial_no: int, dates: List[str], config: Dict, result: Dict) -> int:
    cfg_json = json.dumps(config, ensure_ascii=False, sort_keys=True)
    cfg_hash = hashlib.md5(cfg_json.encode()).hexdigest()[:12]
    dates_str = ",".join(sorted(dates))
    cur = conn.execute(
        """INSERT INTO pulse_runs
           (study_name, run_type, trial_no, dates,
            config_hash, config_json,
            objective, direction_acc, confirmed_ratio,
            entry_rate, neutral_penalty, signal_count, entry_count)
           VALUES (?,?,?,?, ?,?, ?,?,?, ?,?,?,?)""",
        (
            study_name, run_type, trial_no, dates_str,
            cfg_hash, cfg_json,
            result["objective"], result["direction_acc"], result["confirmed_ratio"],
            result["entry_rate"], result["neutral_penalty"],
            result["signal_count"], result["entry_count"],
        ),
    )
    conn.commit()
    return cur.lastrowid


def _all_ticks_merged(sessions: Dict[str, List[Dict]]) -> List[Dict]:
    """날짜별 ticks 를 날짜 순으로 병합 (멀티데이 평가)."""
    merged: List[Dict] = []
    for date in sorted(sessions.keys()):
        merged.extend(sessions[date])
    return merged


# ── 랜덤 샘플링 ──────────────────────────────────────────────────────────

def _random_config(seed: Optional[int] = None) -> Dict:
    rng = random.Random(seed)
    cfg: Dict = {}

    for key, spec in _SEARCH_SPACE.items():
        kind = spec[0]
        if kind == "float":
            cfg[key] = round(rng.uniform(spec[1], spec[2]), 4)
        elif kind == "int":
            cfg[key] = rng.randint(int(spec[1]), int(spec[2]))
        elif kind == "bool":
            cfg[key] = rng.choice([True, False])

    # 제약: flow_upper > flow_lower
    if cfg["flow_upper"] <= cfg["flow_lower"]:
        cfg["flow_upper"] = round(cfg["flow_lower"] + 0.1, 4)

    # 제약: confidence_min_entry < confidence_min_confirm
    if cfg["confidence_min_entry"] >= cfg["confidence_min_confirm"]:
        cfg["confidence_min_entry"] = round(cfg["confidence_min_confirm"] - 0.05, 4)

    return cfg


# ── 메인 실행 ─────────────────────────────────────────────────────────────

class PulseSim:

    def __init__(self, source_db: str = _SOURCE_DB, sim_db: str = _SIM_DB):
        self.source_db = source_db
        self.sim_db    = sim_db
        _init_db(sim_db)

    def run_random(self, dates: List[str], n_trials: int = 200,
                   study_name: Optional[str] = None, seed: Optional[int] = None) -> List[Dict]:
        sessions = load_ticks_5m(self.source_db, dates)
        if not sessions:
            log.error("[RANDOM] 데이터 없음 — 중단")
            return []

        ticks = _all_ticks_merged(sessions)
        log.info("[RANDOM] %d개 날짜, %d개 5m-틱 | n=%d", len(sessions), len(ticks), n_trials)

        if study_name is None:
            h = hashlib.md5(",".join(sorted(dates)).encode()).hexdigest()[:6]
            study_name = f"pulse_random_{h}_{datetime.now().strftime('%Y%m%d_%H%M%S')}"

        conn = _connect(self.sim_db)
        results: List[Dict] = []
        rng = random.Random(seed)

        for trial_no in range(n_trials):
            cfg = _random_config(rng.randint(0, 2**31))
            res = simulate_pulse(ticks, cfg)
            run_id = _save_run(conn, study_name, "random", trial_no, dates, cfg, res)
            results.append({"run_id": run_id, "trial_no": trial_no, **res})

            if (trial_no + 1) % 50 == 0:
                log.info("[RANDOM] %d/%d | best_obj=%.2f",
                         trial_no + 1, n_trials,
                         max(r["objective"] for r in results))

        conn.close()
        log.info("[RANDOM] 완료 | study=%s | best_obj=%.2f",
                 study_name, max(r["objective"] for r in results))
        return results

    def run_bayes(self, dates: List[str], n_trials: int = 100,
                  study_name: Optional[str] = None, seed: Optional[int] = None,
                  n_startup: int = 20) -> List[Dict]:
        try:
            import optuna
        except ImportError as e:
            raise RuntimeError("optuna 가 필요합니다. pip install optuna") from e

        sessions = load_ticks_5m(self.source_db, dates)
        if not sessions:
            log.error("[BAYES] 데이터 없음 — 중단")
            return []

        ticks = _all_ticks_merged(sessions)
        log.info("[BAYES] %d개 날짜, %d개 5m-틱 | n=%d", len(sessions), len(ticks), n_trials)

        if study_name is None:
            h = hashlib.md5(",".join(sorted(dates)).encode()).hexdigest()[:6]
            study_name = f"pulse_bo_{h}_{datetime.now().strftime('%Y%m%d_%H%M%S')}"

        conn = _connect(self.sim_db)
        results: List[Dict] = []

        optuna.logging.set_verbosity(optuna.logging.WARNING)
        sampler = optuna.samplers.TPESampler(seed=seed, n_startup_trials=n_startup)
        study   = optuna.create_study(direction="maximize", sampler=sampler)

        def _objective(trial) -> float:
            cfg = {
                "a_bullish_thresh":       trial.suggest_float("a_bullish_thresh", 2.0, 8.0),
                "fgn_delta_strong":       trial.suggest_float("fgn_delta_strong", 200.0, 3000.0),
                "flow_upper":             trial.suggest_float("flow_upper", 0.15, 0.8),
                "flow_lower":             trial.suggest_float("flow_lower", 0.02, 0.3),
                "ls_diff_thresh":         trial.suggest_float("ls_diff_thresh", 0.05, 0.5),
                "ema_slope_thresh":       trial.suggest_float("ema_slope_thresh", 0.01, 0.2),
                "basis_high":             trial.suggest_float("basis_high", 0.05, 1.0),
                "b_confirm_min":          trial.suggest_int("b_confirm_min", 0, 3),
                "trend_confirm_count":    trial.suggest_int("trend_confirm_count", 2, 6),
                "confidence_min_confirm": trial.suggest_float("confidence_min_confirm", 0.2, 0.8),
                "confidence_min_entry":   trial.suggest_float("confidence_min_entry", 0.1, 0.6),
            }

            # 제약
            if cfg["flow_upper"] <= cfg["flow_lower"]:
                cfg["flow_upper"] = cfg["flow_lower"] + 0.1
            if cfg["confidence_min_entry"] >= cfg["confidence_min_confirm"]:
                cfg["confidence_min_entry"] = cfg["confidence_min_confirm"] - 0.05

            res = simulate_pulse(ticks, cfg)
            trial_no = len(results)
            run_id = _save_run(conn, study_name, "bayes_opt", trial_no, dates, cfg, res)
            results.append({"run_id": run_id, "trial_no": trial_no, **res})

            if (trial_no + 1) % 20 == 0:
                log.info("[BAYES] %d/%d | best=%.2f", trial_no + 1, n_trials, study.best_value)

            return res["objective"]

        study.optimize(_objective, n_trials=n_trials, show_progress_bar=False)
        conn.close()

        best = study.best_params
        log.info("[BAYES] 완료 | study=%s | best_obj=%.2f | params=%s",
                 study_name, study.best_value, best)
        return results

    # ── 자동 루프 ─────────────────────────────────────────────────────────

    def _detect_session_dates(self, n: int = 10) -> List[str]:
        """mmean.db regime_ticks 에서 정규장 데이터가 있는 최근 n일 반환."""
        try:
            conn = sqlite3.connect(
                f"file:{self.source_db}?mode=ro", uri=True, timeout=10
            )
            rows = conn.execute(
                """SELECT DISTINCT date(ts) FROM regime_ticks
                   WHERE time(ts) >= '09:00:00' AND time(ts) < '15:35:00'
                   ORDER BY date(ts) DESC LIMIT ?""",
                (n,),
            ).fetchall()
            conn.close()
            return sorted(r[0] for r in rows if r[0])
        except Exception as e:
            log.error("[AUTO] 날짜 탐지 실패: %s", e)
            return []

    def _last_study_dates(self) -> List[str]:
        """pulse_sim.db 의 마지막 bayes_opt study 가 사용한 날짜 목록 반환."""
        try:
            conn = _connect(self.sim_db)
            row = conn.execute(
                """SELECT dates FROM pulse_runs
                   WHERE run_type='bayes_opt'
                   ORDER BY id DESC LIMIT 1"""
            ).fetchone()
            conn.close()
            if row:
                return sorted(row["dates"].split(","))
            return []
        except Exception:
            return []

    def run_auto(self, n_days: int = 10, loops: int = 0,
                 random_trials: int = 300, bayes_trials: int = 150,
                 seed: Optional[int] = None, sleep_sec: int = 300) -> None:
        """
        자동 루프: 새 날짜가 탐지되면 랜덤+베이지안 자동 실행.

        loops=0 → 무한 반복
        loops=N → N회 실행 후 종료
        """
        loop_no   = 0
        done_loops = 0

        log.info("[AUTO] 시작 | n_days=%d loops=%s random=%d bayes=%d",
                 n_days, "∞" if loops == 0 else loops, random_trials, bayes_trials)

        while True:
            loop_no += 1
            now = datetime.now().strftime("%H:%M:%S")

            # 날짜 탐지
            all_dates  = self._detect_session_dates(n_days)
            last_dates = self._last_study_dates()
            new_dates  = [d for d in all_dates if d not in last_dates]

            if not new_dates:
                log.info("[AUTO] loop=%d @ %s — 새 날짜 없음, %ds 대기",
                         loop_no, now, sleep_sec)
                if loops > 0 and done_loops >= loops:
                    break
                import time; time.sleep(sleep_sec)
                continue

            log.info("[AUTO] loop=%d @ %s — 새 날짜 %s 탐지, 전체 %s 탐색 시작",
                     loop_no, now, new_dates, all_dates)

            h = hashlib.md5(",".join(all_dates).encode()).hexdigest()[:6]
            study = f"pulse_auto_{h}_{datetime.now().strftime('%Y%m%d_%H%M%S')}"

            self.run_random(all_dates, n_trials=random_trials,
                            study_name=study, seed=seed)
            self.run_bayes(all_dates, n_trials=bayes_trials,
                           study_name=study, seed=seed)
            self.show_top(10)

            done_loops += 1
            if loops > 0 and done_loops >= loops:
                log.info("[AUTO] %d회 완료 — 종료", done_loops)
                break

            import time; time.sleep(sleep_sec)

    def show_top(self, n: int = 10, sim_db: Optional[str] = None) -> None:
        db = sim_db or self.sim_db
        conn = _connect(db)
        rows = conn.execute(
            """SELECT study_name, run_type, trial_no, objective,
                      direction_acc, confirmed_ratio, entry_rate,
                      neutral_penalty, signal_count, entry_count,
                      config_json
               FROM pulse_runs
               WHERE objective IS NOT NULL
               ORDER BY objective DESC
               LIMIT ?""",
            (n,),
        ).fetchall()
        conn.close()

        if not rows:
            print("[TOP] 결과 없음")
            return

        print(f"\n{'='*80}")
        print(f"{'#':>3}  {'obj':>7}  {'dir_acc':>7}  {'conf_r':>6}  "
              f"{'e_rate':>6}  {'n_pen':>6}  {'ent':>4}  study / run_type")
        print(f"{'-'*80}")
        for i, r in enumerate(rows, 1):
            cfg = json.loads(r["config_json"])
            print(f"{i:>3}  {r['objective']:>7.2f}  {r['direction_acc']:>7.3f}  "
                  f"{r['confirmed_ratio']:>6.3f}  {r['entry_rate']:>6.3f}  "
                  f"{r['neutral_penalty']:>6.3f}  {r['entry_count']:>4}  "
                  f"{r['study_name'][:30]} / {r['run_type']}")
            # 주요 파라미터
            print(f"     a_bull={cfg.get('a_bullish_thresh','?'):.1f}  "
                  f"fgn_strong={cfg.get('fgn_delta_strong','?'):.0f}  "
                  f"flow={cfg.get('flow_lower','?'):.2f}~{cfg.get('flow_upper','?'):.2f}  "
                  f"b_cmin={cfg.get('b_confirm_min','?')}  "
                  f"tcc={cfg.get('trend_confirm_count','?')}  "
                  f"conf_entry={cfg.get('confidence_min_entry','?'):.2f}")
        print(f"{'='*80}\n")


# ── CLI ──────────────────────────────────────────────────────────────────

def _setup_logging(verbose: bool = False) -> None:
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
        datefmt="%H:%M:%S",
    )


def main() -> None:
    parser = argparse.ArgumentParser(
        description="PulseEngine 내부 가중치/임계치 Monte Carlo + Bayesian 최적화"
    )
    parser.add_argument("--dates",  type=str, default="",
                        help="쉼표 구분 날짜 (예: 2026-03-23 또는 2026-03-18,2026-03-19)")
    parser.add_argument("--random", type=int, default=300,
                        help="랜덤 탐색 trial 수 (기본 300)")
    parser.add_argument("--bayes",  type=int, default=150,
                        help="Bayesian 탐색 trial 수 (기본 150)")
    parser.add_argument("--top",    type=int, default=0,
                        help="결과 조회 상위 N건 (0=실행 안 함)")
    parser.add_argument("--seed",   type=int, default=None)
    # 자동 루프
    parser.add_argument("--auto",   action="store_true",
                        help="자동 루프: 새 날짜 탐지 시 랜덤+베이지안 자동 실행")
    parser.add_argument("--loops",  type=int, default=0,
                        help="자동 루프 반복 횟수 (0=무한, 기본 0)")
    parser.add_argument("--n-days", type=int, default=10,
                        help="자동 모드에서 탐지할 최근 정규장 날짜 수 (기본 10)")
    parser.add_argument("--sleep",  type=int, default=300,
                        help="자동 모드에서 새 날짜 없을 때 대기 초 (기본 300)")
    parser.add_argument("--source-db", type=str, default=_SOURCE_DB)
    parser.add_argument("--sim-db",    type=str, default=_SIM_DB)
    parser.add_argument("-v", "--verbose", action="store_true")
    args = parser.parse_args()

    _setup_logging(args.verbose)
    sim = PulseSim(source_db=args.source_db, sim_db=args.sim_db)

    # 결과 조회만
    if args.top > 0:
        sim.show_top(args.top)
        return

    # 자동 루프 모드
    if args.auto:
        sim.run_auto(
            n_days        = args.n_days,
            loops         = args.loops,
            random_trials = args.random,
            bayes_trials  = args.bayes,
            seed          = args.seed,
            sleep_sec     = args.sleep,
        )
        return

    if not args.dates:
        parser.error("--dates 또는 --auto 를 지정하세요")

    dates = [d.strip() for d in args.dates.split(",") if d.strip()]
    log.info("PulseSim 시작 | dates=%s | random=%d | bayes=%d",
             dates, args.random, args.bayes)

    if args.random > 0:
        sim.run_random(dates, n_trials=args.random, seed=args.seed)

    if args.bayes > 0:
        sim.run_bayes(dates, n_trials=args.bayes, seed=args.seed)

    sim.show_top(10)


if __name__ == "__main__":
    main()
