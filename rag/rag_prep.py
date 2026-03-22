# [Directory] MMEAN
# [File] rag_prep.py
"""
MMEAN RAG 사전작업 배치 스크립트

역할:
  1. regime_ticks → market_features 구간화 (feature 계산)
  2. trades + market_features → pattern_memory 집계
  3. llm_calls + trades → llm_evaluations 사후 평가
  4. pattern_memory.warning_level 갱신

실행 방법:
  python rag_prep.py               # 전체 실행
  python rag_prep.py --step 1      # feature 계산만
  python rag_prep.py --step 2      # 패턴 집계만
  python rag_prep.py --step 3      # LLM 평가만
  python rag_prep.py --days 10     # 최근 N일만 처리

스케줄:
  장 마감 후 1회 실행 권장 (16:30 또는 06:30 야간 종료 후)
"""
from __future__ import annotations

import argparse
import json
import logging
import os
import sqlite3
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s"
)
log = logging.getLogger("MMEAN.RAGPrep")

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH  = os.path.join(BASE_DIR, "storage", "mmean.db")

# pattern_schema 임포트 (선택적 — 없어도 step1 기본 동작은 유지)
try:
    from pattern_schema import classify_regime_tags as _classify_tags
    _HAVE_SCHEMA = True
except ImportError:
    _HAVE_SCHEMA = False
    log.warning("pattern_schema 없음 — regime_tags 컬럼은 NULL로 저장됩니다.")


# -------------------------------------------------------------------
# Step 1: regime_ticks → market_features 구간화
# -------------------------------------------------------------------
def _time_bucket(ts: str) -> str:
    """시각 → 시간대 버킷."""
    try:
        h = int(ts[11:13])
        if 9 <= h < 11:   return "morning"
        if 11 <= h < 14:  return "midday"
        if 14 <= h < 16:  return "late"
        if 18 <= h or h < 1: return "night_early"
        return "night_late"
    except Exception:
        return "unknown"


def _basis_zone(basis: float) -> str:
    if basis >=  0.30: return "strong_pos"
    if basis >=  0.10: return "mid_pos"
    if basis <= -0.30: return "strong_neg"
    if basis <= -0.10: return "mid_neg"
    return "neutral"


def _volume_zone(volume_burst: float) -> str:
    if volume_burst >= 1.6: return "burst"
    if volume_burst <= 0.6: return "quiet"
    return "normal"


def _daily_pnl_zone(daily_pnl: float) -> str:
    if daily_pnl > 100_000:   return "profit"
    if daily_pnl < -100_000:  return "drawdown"
    return "breakeven"


def step1_compute_features(conn: sqlite3.Connection, days: int = 30) -> int:
    """
    regime_ticks에서 feature 구간화 → market_features 삽입.
    이미 처리된 tick_id는 건너뜀.
    """
    since = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d")

    rows = conn.execute("""
        SELECT rt.id, rt.ts, rt.basis, rt.volume_burst,
               rt.long_score, rt.short_score, rt.bias,
               rt.basis_ema_delta, rt.foreign_buy,
               rt.flow_score, rt.flow_long_weight, rt.flow_short_weight,
               rt.llm_filter_valid, rt.llm_filter_direction,
               rt.entry_signal, rt.confidence, rt.session_phase
        FROM regime_ticks rt
        LEFT JOIN market_features mf ON mf.tick_id = rt.id
        WHERE rt.ts >= ? AND mf.id IS NULL
        ORDER BY rt.ts
    """, (since,)).fetchall()

    if not rows:
        log.info("Step1: 처리할 신규 틱 없음")
        return 0

    # 날짜별 누적 PnL (당일 trades 기준)
    daily_pnl_cache: Dict[str, float] = {}

    inserted = 0
    batch = []
    for row in rows:
        (tick_id, ts, basis, vol_burst, l_score, s_score, bias,
         basis_delta, foreign_buy,
         flow_score, flow_l, flow_s,
         llm_valid, llm_dir,
         entry_signal, confidence, session_phase) = row
        session_date = ts[:10]

        if session_date not in daily_pnl_cache:
            r = conn.execute("""
                SELECT COALESCE(SUM(COALESCE(pnl_ticks,0)*12500), 0)
                FROM trades
                WHERE substr(open_ts,1,10) = ?
            """, (session_date,)).fetchone()
            daily_pnl_cache[session_date] = float(r[0]) if r else 0.0

        tb   = _time_bucket(ts)
        bz   = _basis_zone(float(basis or 0.0))
        vz   = _volume_zone(float(vol_burst or 0.0))
        dpz  = _daily_pnl_zone(daily_pnl_cache[session_date])

        # regime_tags 계산 (pattern_schema 사용 가능 시)
        tags_json: Optional[str] = None
        if _HAVE_SCHEMA:
            tick_dict = {
                "bias":                  bias,
                "long_score":            l_score,
                "short_score":           s_score,
                "volume_burst":          vol_burst,
                "basis":                 basis,
                "basis_ema_delta":       basis_delta,
                "foreign_buy":           foreign_buy,
                "flow_score":            flow_score,
                "flow_long_weight":      flow_l,
                "flow_short_weight":     flow_s,
                "llm_filter_valid":      llm_valid,
                "llm_filter_direction":  llm_dir,
                "entry_signal":          entry_signal,
                "confidence":            confidence,
                "session_phase":         session_phase,
            }
            tags = _classify_tags(tick_dict)
            tags_json = json.dumps(tags, ensure_ascii=False) if tags else None

        # bias_streak: 간단 추정 (배치에서는 연속 카운트 생략, 0으로 채움)
        # 실시간 streak는 analyzer_app이 직접 기록하는 게 정확함
        fv = json.dumps({
            "time_bucket":    tb,
            "basis_zone":     bz,
            "volume_zone":    vz,
            "daily_pnl_zone": dpz,
            "long_score":     round(float(l_score or 0.0), 2),
            "short_score":    round(float(s_score or 0.0), 2),
            "bias":           bias or "NEUTRAL",
        }, ensure_ascii=False)

        batch.append((tick_id, ts, session_date, tb, bz, vz, 0, dpz, fv, tags_json))

        if len(batch) >= 1000:
            conn.executemany("""
                INSERT OR IGNORE INTO market_features
                (tick_id, ts, session_date, time_bucket, basis_zone,
                 volume_zone, bias_streak, daily_pnl_zone, feature_vector, regime_tags)
                VALUES (?,?,?,?,?,?,?,?,?,?)
            """, batch)
            conn.commit()
            inserted += len(batch)
            batch = []

    if batch:
        conn.executemany("""
            INSERT OR IGNORE INTO market_features
            (tick_id, ts, session_date, time_bucket, basis_zone,
             volume_zone, bias_streak, daily_pnl_zone, feature_vector, regime_tags)
            VALUES (?,?,?,?,?,?,?,?,?,?)
        """, batch)
        conn.commit()
        inserted += len(batch)

    log.info("Step1 완료: %d 건 market_features 삽입 (regime_tags=%s)",
             inserted, "ON" if _HAVE_SCHEMA else "OFF")
    return inserted


# -------------------------------------------------------------------
# Step 2: trades → pattern_memory 집계
# -------------------------------------------------------------------
def _cluster_key(basis_zone: str, volume_zone: str, time_bucket: str) -> str:
    return f"{basis_zone}|{volume_zone}|{time_bucket}"


def _warning_level(win_rate: float, sample_count: int, avg_pnl: float) -> str:
    if sample_count < 5:
        return "none"  # 샘플 부족
    if win_rate < 0.25 and avg_pnl < -0.5:
        return "danger"
    if win_rate < 0.35 and avg_pnl < -0.2:
        return "warning"
    if win_rate < 0.45:
        return "caution"
    return "none"


def step2_aggregate_patterns(conn: sqlite3.Connection, days: int = 20) -> int:
    """
    trades + market_features → pattern_memory 집계.
    최근 N일 기준으로 전체 재계산 (UPSERT).
    """
    since = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d")

    # 진입 시점의 market_feature와 trades 조인
    rows = conn.execute("""
        SELECT
            mf.basis_zone,
            mf.volume_zone,
            mf.time_bucket,
            t.direction,
            COALESCE(t.pnl_ticks, 0) AS pnl_ticks,
            COALESCE(t.max_adverse_excursion, 0) AS mae,
            COALESCE(t.max_favorable_pt, 0) AS mfe,
            t.open_ts
        FROM trades t
        JOIN market_features mf ON mf.ts <= t.open_ts
        WHERE t.open_ts >= ?
          AND mf.session_date = substr(t.open_ts, 1, 10)
          AND mf.basis_zone IS NOT NULL
        ORDER BY t.open_ts
    """, (since,)).fetchall()

    if not rows:
        log.info("Step2: 집계할 거래 없음")
        return 0

    # 클러스터별 집계
    clusters: Dict[str, Dict[str, Any]] = {}
    for bz, vz, tb, direction, pnl, mae, mfe, open_ts in rows:
        key = _cluster_key(bz or "neutral", vz or "normal", tb or "unknown")
        if key not in clusters:
            clusters[key] = {
                "total": 0, "wins": 0, "losses": 0,
                "pnl_sum": 0.0, "mae_sum": 0.0, "mfe_sum": 0.0,
                "long_total": 0, "long_wins": 0,
                "short_total": 0, "short_wins": 0,
                "last_ts": open_ts,
            }
        c = clusters[key]
        c["total"] += 1
        c["pnl_sum"] += pnl
        c["mae_sum"] += mae
        c["mfe_sum"] += mfe
        c["last_ts"] = open_ts
        if pnl > 0:
            c["wins"] += 1
        else:
            c["losses"] += 1
        if direction == 1:
            c["long_total"] += 1
            if pnl > 0: c["long_wins"] += 1
        else:
            c["short_total"] += 1
            if pnl > 0: c["short_wins"] += 1

    upserted = 0
    now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    for key, c in clusters.items():
        total = c["total"]
        if total == 0:
            continue
        wr   = c["wins"] / total
        apnl = c["pnl_sum"] / total
        amae = c["mae_sum"] / total
        amfe = c["mfe_sum"] / total
        lwr  = (c["long_wins"] / c["long_total"]) if c["long_total"] else 0.0
        swr  = (c["short_wins"] / c["short_total"]) if c["short_total"] else 0.0
        wlv  = _warning_level(wr, total, apnl)

        conn.execute("""
            INSERT INTO pattern_memory
            (cluster_key, sample_count, win_count, loss_count,
             win_rate, avg_pnl_ticks, avg_mae, avg_mfe,
             long_win_rate, short_win_rate,
             last_updated, last_trade_ts, warning_level)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)
            ON CONFLICT(cluster_key) DO UPDATE SET
                sample_count  = excluded.sample_count,
                win_count     = excluded.win_count,
                loss_count    = excluded.loss_count,
                win_rate      = excluded.win_rate,
                avg_pnl_ticks = excluded.avg_pnl_ticks,
                avg_mae       = excluded.avg_mae,
                avg_mfe       = excluded.avg_mfe,
                long_win_rate = excluded.long_win_rate,
                short_win_rate= excluded.short_win_rate,
                last_updated  = excluded.last_updated,
                last_trade_ts = excluded.last_trade_ts,
                warning_level = excluded.warning_level
        """, (key, total, c["wins"], c["losses"],
              round(wr, 4), round(apnl, 4), round(amae, 4), round(amfe, 4),
              round(lwr, 4), round(swr, 4),
              now_str, c["last_ts"], wlv))
        upserted += 1

    conn.commit()
    log.info("Step2 완료: %d 클러스터 pattern_memory 갱신", upserted)
    return upserted


# -------------------------------------------------------------------
# Step 3: llm_calls + trades → llm_evaluations 사후 평가
# -------------------------------------------------------------------
def _score_vs_pnl_label(score: float, pnl: float) -> str:
    high = score >= 0.6 if score >= 0 else False
    win  = pnl > 0
    if high and win:   return "high_score_win"
    if high and not win: return "high_score_loss"
    if not high and win: return "low_score_win"
    return "low_score_loss"


def step3_evaluate_llm(conn: sqlite3.Connection, days: int = 30) -> int:
    """
    opportunity 레이어 llm_calls → 해당 시간대 진입 거래와 매핑 → llm_evaluations 삽입.
    이미 평가된 call은 건너뜀.
    """
    since = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d")

    # opportunity 레이어 호출 중 아직 평가되지 않은 것
    calls = conn.execute("""
        SELECT lc.id, lc.ts, lc.provider, lc.layer,
               lc.opportunity_score, lc.direction, lc.expires_at, lc.session_date
        FROM llm_calls lc
        LEFT JOIN llm_evaluations le ON le.llm_call_id = lc.id
        WHERE lc.layer = 'opportunity'
          AND lc.session_date >= ?
          AND le.id IS NULL
        ORDER BY lc.ts
    """, (since,)).fetchall()

    if not calls:
        log.info("Step3: 평가할 LLM 호출 없음")
        return 0

    evaluated = 0
    now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    for call_id, call_ts, provider, layer, score, direction, expires_at, session_date in calls:
        # 유효 시간 내에 발생한 거래 찾기
        expire = expires_at or ""
        if expire:
            trades_in_window = conn.execute("""
                SELECT id, direction, pnl_ticks, exit_reason
                FROM trades
                WHERE open_ts >= ? AND open_ts <= ?
                ORDER BY open_ts LIMIT 5
            """, (call_ts, expire)).fetchall()
        else:
            # expires_at 없으면 call_ts 이후 5분 이내
            trades_in_window = conn.execute("""
                SELECT id, direction, pnl_ticks, exit_reason
                FROM trades
                WHERE open_ts >= ? AND open_ts <= datetime(?, '+5 minutes')
                ORDER BY open_ts LIMIT 5
            """, (call_ts, call_ts)).fetchall()

        if trades_in_window:
            for trade_id, actual_dir, pnl, exit_reason in trades_in_window:
                pnl_f   = float(pnl or 0.0)
                score_f = float(score or -1.0)
                dir_match = (
                    1 if (direction == "LONG" and actual_dir == 1) or
                          (direction == "SHORT" and actual_dir == -1) or
                          direction == "NEUTRAL"
                    else 0
                )
                conn.execute("""
                    INSERT OR IGNORE INTO llm_evaluations
                    (llm_call_id, trade_id, evaluated_at, provider, layer,
                     llm_score, llm_direction, actual_direction,
                     actual_pnl_ticks, actual_exit_reason,
                     score_vs_pnl, direction_match, session_date)
                    VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)
                """, (call_id, trade_id, now_str, provider, layer,
                      score_f, direction, actual_dir,
                      pnl_f, exit_reason,
                      _score_vs_pnl_label(score_f, pnl_f),
                      dir_match, session_date))
                evaluated += 1
        else:
            # 해당 구간 진입 없음 — 미진입 기록
            conn.execute("""
                INSERT OR IGNORE INTO llm_evaluations
                (llm_call_id, trade_id, evaluated_at, provider, layer,
                 llm_score, llm_direction, actual_direction,
                 actual_pnl_ticks, actual_exit_reason,
                 score_vs_pnl, direction_match, session_date)
                VALUES (?,NULL,?,?,?,?,?,NULL,NULL,NULL,'no_entry',0,?)
            """, (call_id, now_str, provider, layer,
                  float(score or -1.0), direction, session_date))
            evaluated += 1

    conn.commit()
    log.info("Step3 완료: %d 건 llm_evaluations 삽입", evaluated)
    return evaluated


# -------------------------------------------------------------------
# Step 4: judgment_events → failure_patterns 집계
# -------------------------------------------------------------------

# 실패 패턴 분류 임계값
_FORBIDDEN_WR   = 0.30   # WR < 30% → forbidden
_FAILURE_WR     = 0.45   # WR < 45% AND avg_pnl < 0 → failure
_SUCCESS_WR     = 0.50   # WR >= 50% AND avg_pnl > 0 → success
_MIN_SAMPLES    = 5      # 최소 샘플 수 (미만 시 건너뜀)
_SHADOW_DAYS    = 90     # 집계 기간 (일) — 구 데이터 과적합 방지


def _confidence_label(n: int) -> str:
    if n >= 20: return "HIGH"
    if n >= 10: return "MED"
    return "LOW"


def _pattern_type(win_rate: float, avg_pnl: float) -> Optional[str]:
    """분류 기준 반환. None 이면 패턴 등록 대상 아님."""
    if win_rate < _FORBIDDEN_WR:
        return "forbidden"
    if win_rate < _FAILURE_WR and avg_pnl < 0:
        return "failure"
    if win_rate >= _SUCCESS_WR and avg_pnl > 0:
        return "success"
    return None   # 중간 영역 — 등록 안 함


def _make_summary(combo_key: str, combo_type: str, pattern_type: str,
                  n: int, win_rate: float, avg_pnl: float, worst_pnl: float,
                  window_days: int) -> str:
    parts = combo_key.split("|")
    type_label = {
        "oc_prov":   "개장특성+잠정레짐",
        "prov_ar":   "잠정레짐+오후레짐",
        "oc_ar":     "개장특성+오후레짐",
        "single_oc": "개장특성",
    }.get(combo_type, combo_type)
    pt_label = {"forbidden": "⛔금지", "failure": "⚠️주의", "success": "✅성공"}.get(
        pattern_type, pattern_type
    )
    return (
        f"{pt_label} [{type_label}] {combo_key} | "
        f"최근{window_days}일 {n}건 | "
        f"승률{win_rate*100:.0f}% | 평균{avg_pnl:+.2f}pt | 최악{worst_pnl:+.2f}pt"
    )


def _aggregate_combo(rows: list, key_fn) -> Dict[str, Dict]:
    """rows(judgment_events)를 key_fn(row→str) 로 그룹화 → 통계 dict."""
    groups: Dict[str, Dict] = {}
    for r in rows:
        k = key_fn(r)
        if k is None or "|None" in k or "None|" in k or k.startswith("None"):
            continue   # NULL 체크포인트 포함된 조합 건너뜀
        if k not in groups:
            groups[k] = {"n": 0, "wins": 0, "pnl_sum": 0.0, "worst": 0.0}
        g = groups[k]
        pnl = float(r[2] or 0.0)   # r[2] = pnl_pt
        g["n"] += 1
        g["pnl_sum"] += pnl
        if pnl > 0:
            g["wins"] += 1
        if pnl < g["worst"]:
            g["worst"] = pnl
    return groups


def step4_build_failure_patterns(conn: sqlite3.Connection,
                                  days: int = _SHADOW_DAYS) -> int:
    """
    judgment_events → failure_patterns UPSERT.

    집계 기간: 최근 N일 (기본 90일) — 구 패턴 과적합 방지.
    최소 샘플: 5건 미만 건너뜀.
    대상 패턴: forbidden(WR<30%) / failure(30-45%, pnl<0) / success(WR>=50%, pnl>0).
    """
    since = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d")

    rows = conn.execute("""
        SELECT opening_char, prov_regime, afternoon_regime, pnl_pt
        FROM judgment_events
        WHERE date(ts) >= ?
          AND pnl_pt IS NOT NULL
          AND entry_signal IN ('LONG_READY','SHORT_READY')
        ORDER BY ts
    """, (since,)).fetchall()

    if not rows:
        log.info("Step4: judgment_events 없음 (since=%s)", since)
        return 0

    # ── 4가지 콤보 집계 ────────────────────────────────────────────
    combo_specs = [
        ("oc_prov",   lambda r: f"{r[0]}|{r[1]}"),
        ("prov_ar",   lambda r: f"{r[1]}|{r[2]}"),
        ("oc_ar",     lambda r: f"{r[0]}|{r[2]}"),
        ("single_oc", lambda r: f"{r[0]}"),
    ]

    now_str  = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    upserted = 0

    for combo_type, key_fn in combo_specs:
        groups = _aggregate_combo(rows, key_fn)

        for combo_key, g in groups.items():
            n = g["n"]
            if n < _MIN_SAMPLES:
                continue
            win_rate = g["wins"] / n
            avg_pnl  = g["pnl_sum"] / n
            worst    = g["worst"]
            pt = _pattern_type(win_rate, avg_pnl)
            if pt is None:
                continue

            conf    = _confidence_label(n)
            summary = _make_summary(combo_key, combo_type, pt,
                                    n, win_rate, avg_pnl, worst, days)

            conn.execute("""
                INSERT INTO failure_patterns
                    (combo_key, combo_type, pattern_type,
                     n, win_count, loss_count, win_rate, avg_pnl, worst_pnl,
                     sample_window_days, confidence, summary_text, updated_at)
                VALUES (?,?,?, ?,?,?,?,?,?, ?,?,?,?)
                ON CONFLICT(combo_key, combo_type) DO UPDATE SET
                    pattern_type       = excluded.pattern_type,
                    n                  = excluded.n,
                    win_count          = excluded.win_count,
                    loss_count         = excluded.loss_count,
                    win_rate           = excluded.win_rate,
                    avg_pnl            = excluded.avg_pnl,
                    worst_pnl          = excluded.worst_pnl,
                    sample_window_days = excluded.sample_window_days,
                    confidence         = excluded.confidence,
                    summary_text       = excluded.summary_text,
                    updated_at         = excluded.updated_at
            """, (
                combo_key, combo_type, pt,
                n, g["wins"], n - g["wins"],
                round(win_rate, 4), round(avg_pnl, 4), round(worst, 4),
                days, conf, summary, now_str,
            ))
            upserted += 1

    conn.commit()

    fp_cnt = conn.execute(
        "SELECT COUNT(*) FROM failure_patterns WHERE pattern_type='forbidden'"
    ).fetchone()[0]
    fa_cnt = conn.execute(
        "SELECT COUNT(*) FROM failure_patterns WHERE pattern_type='failure'"
    ).fetchone()[0]
    log.info(
        "Step4 완료: %d 패턴 UPSERT (⛔금지=%d ⚠️주의=%d)",
        upserted, fp_cnt, fa_cnt,
    )
    return upserted


# -------------------------------------------------------------------
# 메모리 요약 조회 (llm_filter에서 프롬프트 주입용)
# -------------------------------------------------------------------
def get_pattern_warning(
    db_path: str,
    basis_zone: str,
    volume_zone: str,
    time_bucket: str,
    min_samples: int = 5,
) -> Optional[str]:
    """
    현재 시장 상태와 유사한 과거 패턴의 경고 문자열 반환.
    warning_level='none'이면 None 반환.

    llm_filter._build_prompt()에서 주입 예:
        memory_ctx = get_pattern_warning(DB_PATH, bz, vz, tb)
        if memory_ctx:
            lines.append(memory_ctx)
    """
    key = _cluster_key(basis_zone, volume_zone, time_bucket)
    try:
        conn = sqlite3.connect(db_path, check_same_thread=False)
        row = conn.execute("""
            SELECT sample_count, win_rate, avg_pnl_ticks, avg_mae,
                   long_win_rate, short_win_rate, warning_level, last_trade_ts
            FROM pattern_memory
            WHERE cluster_key = ?
        """, (key,)).fetchone()
        conn.close()

        if not row:
            return None
        cnt, wr, apnl, amae, lwr, swr, wlv, last_ts = row

        if int(cnt or 0) < min_samples or wlv == "none":
            return None

        return (
            f"[과거 유사 패턴 경고 | {wlv.upper()}]\n"
            f"클러스터: {key}\n"
            f"최근 {int(cnt)}건 | 승률 {wr*100:.0f}% | 평균틱 {apnl:+.2f}pt | 평균MAE {amae:.2f}pt\n"
            f"LONG승률 {lwr*100:.0f}% | SHORT승률 {swr*100:.0f}%\n"
            f"마지막거래: {last_ts or '?'}"
        )
    except Exception as e:
        log.warning("get_pattern_warning 오류: %s", e)
        return None


def get_pattern_warning_level(
    db_path: str,
    basis_zone: str,
    volume_zone: str,
    time_bucket: str,
    min_samples: int = 5,
) -> str:
    """
    엔진 entry_gate 계산용 — raw warning_level 반환.
    반환값: 'danger' | 'warning' | 'caution' | 'none'

    get_pattern_warning()과 달리 포맷 텍스트 없이 등급만 반환한다.
    엔진이 직접 REJECT/HALF/ALLOW로 매핑하기 위한 전용 함수.
    """
    key = _cluster_key(basis_zone, volume_zone, time_bucket)
    try:
        conn = sqlite3.connect(db_path, check_same_thread=False)
        row = conn.execute(
            "SELECT sample_count, warning_level FROM pattern_memory WHERE cluster_key = ?",
            (key,),
        ).fetchone()
        conn.close()
        if not row or int(row[0] or 0) < min_samples:
            return "none"
        return str(row[1] or "none")
    except Exception as e:
        log.warning("get_pattern_warning_level 오류: %s", e)
        return "none"


def get_llm_score_stats(db_path: str, days: int = 14) -> Dict[str, Any]:
    """
    LLM 점수 구간별 승률 통계 — Analytics 대시보드 또는 보고서용.
    """
    since = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d")
    try:
        conn = sqlite3.connect(db_path, check_same_thread=False)
        rows = conn.execute("""
            SELECT
                CASE
                    WHEN llm_score < 0    THEN 'no_llm'
                    WHEN llm_score < 0.4  THEN '0.0-0.4'
                    WHEN llm_score < 0.6  THEN '0.4-0.6'
                    WHEN llm_score < 0.8  THEN '0.6-0.8'
                    ELSE '0.8-1.0'
                END AS score_bucket,
                COUNT(*) AS cnt,
                SUM(CASE WHEN actual_pnl_ticks > 0 THEN 1 ELSE 0 END) AS wins,
                AVG(COALESCE(actual_pnl_ticks, 0)) AS avg_pnl,
                SUM(CASE WHEN direction_match = 1 THEN 1 ELSE 0 END) AS dir_match
            FROM llm_evaluations
            WHERE session_date >= ?
              AND trade_id IS NOT NULL
            GROUP BY score_bucket
            ORDER BY score_bucket
        """, (since,)).fetchall()
        conn.close()
        return {
            "days": days,
            "since": since,
            "buckets": [
                {
                    "bucket": r[0], "cnt": r[1], "wins": r[2],
                    "win_rate": round(r[2]/r[1], 4) if r[1] else 0.0,
                    "avg_pnl": round(float(r[3] or 0.0), 4),
                    "dir_match_rate": round(r[4]/r[1], 4) if r[1] else 0.0,
                }
                for r in rows
            ]
        }
    except Exception as e:
        log.warning("get_llm_score_stats 오류: %s", e)
        return {}


# -------------------------------------------------------------------
# 진입점
# -------------------------------------------------------------------
def main() -> None:
    parser = argparse.ArgumentParser(description="MMEAN RAG 사전작업 배치")
    parser.add_argument("--step", type=int, default=0,
                        help="실행 스텝 (0=전체, 1=feature, 2=pattern, 3=eval, 4=failure)")
    parser.add_argument("--days", type=int, default=30,
                        help="처리 기간 (일)")
    parser.add_argument("--db", type=str, default=DB_PATH,
                        help="DB 경로")
    args = parser.parse_args()

    conn = sqlite3.connect(args.db, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")

    try:
        if args.step in (0, 1):
            step1_compute_features(conn, days=args.days)
        if args.step in (0, 2):
            step2_aggregate_patterns(conn, days=args.days)
        if args.step in (0, 3):
            step3_evaluate_llm(conn, days=args.days)
        if args.step in (0, 4):
            step4_build_failure_patterns(conn, days=90)  # 항상 90일 고정

        # 요약 출력
        pm_cnt  = conn.execute("SELECT COUNT(*) FROM pattern_memory").fetchone()[0]
        eval_cnt= conn.execute("SELECT COUNT(*) FROM llm_evaluations").fetchone()[0]
        warn_cnt= conn.execute(
            "SELECT COUNT(*) FROM pattern_memory WHERE warning_level != 'none'"
        ).fetchone()[0]
        fp_cnt  = conn.execute(
            "SELECT COUNT(*) FROM failure_patterns WHERE pattern_type != 'success'"
        ).fetchone()[0]
        log.info("=== RAG 사전작업 완료 ===")
        log.info("pattern_memory: %d 클러스터 (경고: %d)", pm_cnt, warn_cnt)
        log.info("llm_evaluations: %d 건", eval_cnt)
        log.info("failure_patterns: %d 건 (금지+주의)", fp_cnt)

        if pm_cnt > 0:
            stats = get_llm_score_stats(args.db, days=args.days)
            log.info("LLM 점수 통계 (최근 %d일):", stats.get("days", 0))
            for b in stats.get("buckets", []):
                log.info("  score %s | %d건 | 승률%.0f%% | 평균틱%+.2f | 방향일치%.0f%%",
                         b["bucket"], b["cnt"], b["win_rate"]*100,
                         b["avg_pnl"], b["dir_match_rate"]*100)
    finally:
        conn.close()


if __name__ == "__main__":
    main()
