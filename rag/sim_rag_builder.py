# MMEAN/sim_rag_builder.py
"""
RAG 문서 빌더

Phase 1 (기존): sim.db → rag_documents (sim.db 내부)
Phase 2A (신규): sim.db + mmean.db → pattern_docs (mmean.db)

Phase 1 사용:
    python sim_rag_builder.py              # rag_documents 신규 추가
    python sim_rag_builder.py --rebuild    # rag_documents 전체 재생성

Phase 2A 사용:
    python sim_rag_builder.py --pattern-docs              # pattern_docs 신규/갱신
    python sim_rag_builder.py --pattern-docs --rebuild    # 전체 재생성
    python sim_rag_builder.py --pattern-docs --verify     # 검증 SQL만 출력
    python sim_rag_builder.py --all                       # Phase 1 + Phase 2A 동시
"""
from __future__ import annotations

import hashlib
import json
import logging
import sqlite3
import sys
from collections import Counter
from datetime import datetime
from typing import Dict, List, Optional, Tuple

from pathlib import Path

log = logging.getLogger("MMEAN.RAGBuilder")

_STORAGE = Path(__file__).resolve().parent.parent / "storage"
_DEFAULT_SIM_DB = str(_STORAGE / "sim.db")


# ─────────────────────────────────────────────────────────────────────
# rag_documents / rag_embeddings 테이블 초기화
# ─────────────────────────────────────────────────────────────────────

def _setup_rag_tables(conn: sqlite3.Connection) -> None:
    conn.execute("""
        CREATE TABLE IF NOT EXISTS rag_documents (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            doc_id          TEXT NOT NULL UNIQUE,

            -- 분류
            doc_type        TEXT NOT NULL,
                            -- candidate | failure | run_summary
            session_mode    TEXT NOT NULL DEFAULT 'day',
                            -- day | night

            -- 출처 메타
            study_name      TEXT,
            run_id          INTEGER,
            config_hash     TEXT,
            date_from       TEXT,
            date_to         TEXT,

            -- 핵심 지표
            trade_count     INTEGER,
            objective_score REAL,
            profit_factor   REAL,
            max_drawdown    REAL,
            stress_passed   INTEGER DEFAULT 0,
            adoption_status TEXT,

            -- 레짐 분류 (regime mismatch 방지용)
            regime_type     TEXT DEFAULT 'unknown',
                            -- trend | range | mixed | unknown

            -- 실제 session_pattern (sim_adoptions.train_pattern에서 전파)
            session_pattern TEXT,
                            -- TREND_UP | TREND_DOWN | CHOPPY | RANGE_TIGHT | RANGE_WIDE | etc.

            -- 문서 본문
            title           TEXT NOT NULL,
            summary_text    TEXT NOT NULL,
            key_facts       TEXT,        -- JSON
            reasoning_hint  TEXT,

            created_at      TEXT NOT NULL,
            embedded        INTEGER DEFAULT 0  -- 1=임베딩 완료
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS rag_embeddings (
            doc_id      TEXT NOT NULL UNIQUE,
            embedding   BLOB NOT NULL,   -- numpy float32 bytes
            model       TEXT NOT NULL,
            created_at  TEXT NOT NULL
        )
    """)
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_rag_docs_type_mode "
        "ON rag_documents(doc_type, session_mode)"
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_rag_docs_embedded "
        "ON rag_documents(embedded)"
    )
    conn.commit()


# ─────────────────────────────────────────────────────────────────────
# 헬퍼
# ─────────────────────────────────────────────────────────────────────

def _j(text: Optional[str]) -> list:
    """JSON 문자열 → list. 파싱 실패 시 []."""
    if not text:
        return []
    try:
        return json.loads(text)
    except Exception:
        return []


def _infer_regime_type(
    win_rate:    Optional[float],
    profit_factor: Optional[float],
    pos_ratio:   Optional[float],
) -> str:
    """
    성과 지표에서 레짐 타입 추론 (proxy 분류).

    trend : 높은 승률 + 높은 PF → 추세 장에서 잘 동작
    range : 낮은 승률인데 PF 양호 → 역추세/박스 설정
    mixed : 중간
    unknown : 정보 없음
    """
    if win_rate is None or profit_factor is None:
        return "unknown"
    if win_rate >= 0.55 and profit_factor >= 1.5:
        return "trend"
    if win_rate < 0.45 and profit_factor >= 1.2:
        return "range"
    if pos_ratio is not None and pos_ratio >= 0.65:
        return "trend"
    return "mixed"


def _config_to_hint(cfg: dict) -> str:
    """파라미터 설정에서 전략 특징 한 줄 요약."""
    hints = []
    sl = cfg.get("sl_ticks", 0)
    if sl and sl <= 15:
        hints.append(f"SL타이트({sl}틱)")
    elif sl and sl >= 40:
        hints.append(f"SL넓음({sl}틱)")

    trail = cfg.get("trail_start_ticks", 0)
    if trail and trail <= 5:
        hints.append("트레일조기활성")

    mc = cfg.get("min_confidence", 0)
    if mc and mc >= 0.7:
        hints.append(f"고신뢰도기준(≥{mc:.1f})")

    return " / ".join(hints) if hints else "일반 설정"


# ─────────────────────────────────────────────────────────────────────
# candidate 문서 (sim_adoptions — status = candidate / active)
# ─────────────────────────────────────────────────────────────────────

def _build_candidate_docs(conn: sqlite3.Connection) -> List[Dict]:
    rows = conn.execute("""
        SELECT a.id, a.study_name, a.config_hash, a.config_json,
               a.adopted_at,
               a.train_run_id, a.train_obj, a.train_trades, a.train_total_pnl,
               a.valid_run_id, a.valid_dates, a.valid_obj,
               a.valid_trades, a.valid_total_pnl, a.valid_max_dd,
               a.valid_pos_ratio, a.robustness,
               a.stress_1t_obj, a.stress_2t_obj, a.stress_passed,
               a.status, a.cluster_id,
               a.train_pattern,
               r.session_mode, r.source_dates,
               vs.win_rate       AS valid_win_rate,
               vs.profit_factor  AS valid_profit_factor
        FROM   sim_adoptions a
        LEFT JOIN sim_runs        r  ON r.id  = a.train_run_id
        LEFT JOIN sim_run_summary vs ON vs.run_id = a.valid_run_id
        WHERE  a.status IN ('candidate', 'active')
    """).fetchall()

    docs = []
    for row in rows:
        (aid, study, cfg_hash, cfg_json, adopted_at,
         train_run_id, train_obj, train_trades, train_pnl,
         valid_run_id, valid_dates_json, valid_obj,
         valid_trades, valid_pnl, valid_dd,
         valid_pos, robustness,
         s1t, s2t, stress_passed,
         status, cluster_id,
         train_pattern,
         session_mode, src_dates_json,
         valid_win_rate, valid_profit_factor) = row

        session_mode = session_mode or "day"
        cfg         = json.loads(cfg_json) if cfg_json else {}
        valid_dates = _j(valid_dates_json)
        src_dates   = _j(src_dates_json)

        date_from = src_dates[0]   if src_dates   else ""
        date_to   = src_dates[-1]  if src_dates   else ""
        vd_from   = valid_dates[0] if valid_dates  else ""
        vd_to     = valid_dates[-1] if valid_dates else ""

        stress_str = (
            f"슬리피지 스트레스 통과 (1틱 obj={s1t:.3f}, 2틱 obj={s2t:.3f})"
            if stress_passed
            else "슬리피지 스트레스 미실행 또는 실패"
        )

        title = (
            f"[{session_mode.upper()} CANDIDATE] {study} | "
            f"valid_obj={valid_obj:.3f}  rob={robustness:.2f}  "
            f"train:{len(src_dates)}일  valid:{len(valid_dates)}일"
        )
        summary = (
            f"세션: {session_mode} | 스터디: {study} | 클러스터: {cluster_id}\n"
            f"훈련: {date_from}~{date_to} ({len(src_dates)}일) "
            f"→ trade={train_trades}, obj={train_obj:.3f}, pnl={train_pnl:.0f}원\n"
            f"검증: {vd_from}~{vd_to} ({len(valid_dates)}일) "
            f"→ trade={valid_trades}, obj={valid_obj:.3f}, "
            f"pnl={valid_pnl:.0f}원, dd={valid_dd:.3f}, pos_ratio={valid_pos:.2f}\n"
            f"강건성(valid/train): {robustness:.2f}\n"
            f"{stress_str}"
        )
        key_facts = {
            "train_obj":       train_obj,
            "valid_obj":       valid_obj,
            "robustness":      robustness,
            "valid_trades":    valid_trades,
            "valid_pos_ratio": valid_pos,
            "valid_max_dd":    valid_dd,
            "stress_passed":   bool(stress_passed),
            "config_hash":     cfg_hash,
        }

        # valid_run_id → sim_run_summary JOIN 으로 실제 win_rate / profit_factor 사용
        regime_type = _infer_regime_type(valid_win_rate, valid_profit_factor, valid_pos)

        doc_id = f"candidate_{cfg_hash[:12]}_{study}"
        docs.append({
            "doc_id":          doc_id,
            "doc_type":        "candidate",
            "session_mode":    session_mode,
            "study_name":      study,
            "run_id":          train_run_id,
            "config_hash":     cfg_hash,
            "date_from":       date_from,
            "date_to":         date_to,
            "trade_count":     valid_trades,
            "objective_score": valid_obj,
            "profit_factor":   valid_profit_factor,   # sim_run_summary JOIN 값
            "max_drawdown":    valid_dd,
            "stress_passed":   int(bool(stress_passed)),
            "adoption_status": status,
            "regime_type":     regime_type,
            "session_pattern": train_pattern,
            "title":           title,
            "summary_text":    summary,
            "key_facts":       json.dumps(key_facts, ensure_ascii=False),
            "reasoning_hint":  _config_to_hint(cfg),
        })
    return docs


# ─────────────────────────────────────────────────────────────────────
# failure 문서
#   1) stress 실패 (validation 통과했지만 슬리피지에 취약)
#   2) 거래 극소 (validation run에서 trade ≤ 3)
# ─────────────────────────────────────────────────────────────────────

def _build_failure_docs(conn: sqlite3.Connection) -> List[Dict]:
    docs: List[Dict] = []

    # ── 1. stress 실패 ────────────────────────────────────────────
    rows = conn.execute("""
        SELECT a.study_name, a.config_hash, a.valid_obj, a.valid_trades,
               a.stress_1t_obj, a.stress_2t_obj, a.robustness,
               a.valid_dates, a.valid_max_dd,
               a.train_pattern,
               r.session_mode, r.source_dates
        FROM   sim_adoptions a
        LEFT JOIN sim_runs r ON r.id = a.train_run_id
        WHERE  a.stress_passed = 0
          AND  a.stress_1t_obj IS NOT NULL
    """).fetchall()

    for row in rows:
        (study, cfg_hash, valid_obj, valid_trades,
         s1t, s2t, rob, valid_dates_json, valid_dd,
         train_pattern,
         session_mode, src_dates_json) = row

        session_mode = session_mode or "day"
        valid_dates  = _j(valid_dates_json)
        src_dates    = _j(src_dates_json)
        pct_drop     = ((valid_obj - s1t) / abs(valid_obj) * 100) if valid_obj else 0

        title = (
            f"[{session_mode.upper()} FAILURE:STRESS] {study} | "
            f"1틱 후 obj {pct_drop:.0f}% 하락"
        )
        summary = (
            f"세션: {session_mode} | 스터디: {study}\n"
            f"검증 obj={valid_obj:.3f} → 1틱슬리피지 obj={s1t:.3f} "
            f"({pct_drop:.0f}% 하락) → 스트레스 실패\n"
            f"검증 trade={valid_trades}, dd={valid_dd:.3f}, rob={rob:.2f}\n"
            f"문제: 슬리피지에 극도로 민감. 실환경 수익 유지 불가."
        )
        doc_id = f"failure_stress_{cfg_hash[:12]}_{study}"
        docs.append({
            "doc_id":          doc_id,
            "doc_type":        "failure",
            "session_mode":    session_mode,
            "study_name":      study,
            "run_id":          None,
            "config_hash":     cfg_hash,
            "date_from":       src_dates[0] if src_dates else "",
            "date_to":         src_dates[-1] if src_dates else "",
            "trade_count":     valid_trades,
            "objective_score": valid_obj,
            "profit_factor":   None,
            "max_drawdown":    valid_dd,
            "stress_passed":   0,
            "adoption_status": "stress_fail",
            "regime_type":     "unknown",
            "session_pattern": train_pattern,
            "title":           title,
            "summary_text":    summary,
            "key_facts":       json.dumps(
                {"valid_obj": valid_obj, "stress_1t_obj": s1t, "pct_drop": pct_drop},
                ensure_ascii=False,
            ),
            "reasoning_hint":  "슬리피지 민감도 높음. 타이트 SL 또는 과소 필터링 의심.",
        })

    # ── 2. 거래 극소 (validation run, trade ≤ 3) ─────────────────
    rows2 = conn.execute("""
        SELECT r.id, r.study_name, r.session_mode, r.source_dates,
               s.trade_count, s.objective_score, s.win_rate,
               s.total_pnl, s.max_drawdown
        FROM   sim_runs r
        JOIN   sim_run_summary s ON s.run_id = r.id
        WHERE  r.run_type = 'validation'
          AND  r.status   = 'done'
          AND  s.trade_count <= 3
        ORDER  BY r.id DESC
        LIMIT  50
    """).fetchall()

    for row in rows2:
        (run_id, study, session_mode, src_json,
         tc, obj, wr, pnl, dd) = row

        session_mode = session_mode or "day"
        src          = _j(src_json)
        obj_str      = f"{obj:.3f}" if obj is not None else "N/A"

        title = (
            f"[{session_mode.upper()} FAILURE:LOW_TRADE] {study or 'run#'+str(run_id)} | "
            f"거래 {tc}건 (통계 부족)"
        )
        summary = (
            f"세션: {session_mode} | 스터디: {study or '-'} | run_id={run_id}\n"
            f"검증 거래 수={tc}건 — 통계적으로 의미 없음\n"
            f"obj={obj_str}, win={wr:.0%}, pnl={pnl:.0f}원\n"
            f"문제: 진입 기준 과잉 엄격, 또는 해당 기간 기회 자체 희소."
        )
        doc_id = f"failure_lowtrade_run{run_id}"
        docs.append({
            "doc_id":          doc_id,
            "doc_type":        "failure",
            "session_mode":    session_mode,
            "study_name":      study,
            "run_id":          run_id,
            "config_hash":     None,
            "date_from":       src[0] if src else "",
            "date_to":         src[-1] if src else "",
            "trade_count":     tc,
            "objective_score": obj,
            "profit_factor":   None,
            "max_drawdown":    dd,
            "stress_passed":   0,
            "adoption_status": "low_trade",
            "regime_type":     "unknown",
            "session_pattern": None,
            "title":           title,
            "summary_text":    summary,
            "key_facts":       json.dumps(
                {"trade_count": tc, "objective_score": obj},
                ensure_ascii=False,
            ),
            "reasoning_hint":  "진입 필터 과잉 또는 기회 희소. 빈도 낮아 신뢰도 없음.",
        })

    return docs


# ─────────────────────────────────────────────────────────────────────
# run_summary 문서 (완료된 validation/random/bayes run 요약)
# ─────────────────────────────────────────────────────────────────────

def _build_run_summary_docs(conn: sqlite3.Connection) -> List[Dict]:
    rows = conn.execute("""
        SELECT r.id, r.run_type, r.session_mode, r.study_name,
               r.source_dates, r.date_count,
               s.total_pnl, s.win_rate, s.profit_factor,
               s.trade_count, s.max_drawdown, s.session_positive_ratio,
               s.objective_score, s.slippage_sensitivity
        FROM   sim_runs r
        JOIN   sim_run_summary s ON s.run_id = r.id
        WHERE  r.run_type IN ('validation', 'random', 'bayes_opt')
          AND  r.status    = 'done'
          AND  s.trade_count >= 4
        ORDER  BY s.objective_score DESC
        LIMIT  100
    """).fetchall()

    docs = []
    type_label = {"validation": "VAL", "random": "RND", "bayes_opt": "BO"}

    for row in rows:
        (run_id, run_type, session_mode, study,
         src_json, date_count,
         pnl, wr, pf, tc, dd, pos_ratio,
         obj, slip_sens) = row

        session_mode = session_mode or "day"
        src          = _j(src_json)
        lbl          = type_label.get(run_type, run_type.upper())

        title = (
            f"[{session_mode.upper()} {lbl}] run#{run_id} {study or ''} | "
            f"obj={obj:.3f}  pf={pf:.2f}  trade={tc}"
        )
        summary = (
            f"세션: {session_mode} | 타입: {run_type} | 스터디: {study or '-'}\n"
            f"날짜: {src[0] if src else '?'}~{src[-1] if src else '?'} ({date_count}일)\n"
            f"거래={tc}, 승률={wr:.0%}, PF={pf:.2f}, "
            f"총pnl={pnl:.0f}원, dd={dd:.3f}\n"
            f"세션 수익비율={pos_ratio:.0%}, obj={obj:.3f}, "
            f"슬리피지민감도={slip_sens:.3f}"
        )
        regime_type = _infer_regime_type(wr, pf, pos_ratio)

        doc_id = f"run_summary_{run_id}_{session_mode}"
        docs.append({
            "doc_id":          doc_id,
            "doc_type":        "run_summary",
            "session_mode":    session_mode,
            "study_name":      study,
            "run_id":          run_id,
            "config_hash":     None,
            "date_from":       src[0] if src else "",
            "date_to":         src[-1] if src else "",
            "trade_count":     tc,
            "objective_score": obj,
            "profit_factor":   pf,
            "max_drawdown":    dd,
            "stress_passed":   0,
            "adoption_status": None,
            "regime_type":     regime_type,
            "title":           title,
            "summary_text":    summary,
            "key_facts":       json.dumps(
                {"obj": obj, "pf": pf, "tc": tc,
                 "wr": wr, "slip_sens": slip_sens},
                ensure_ascii=False,
            ),
            "reasoning_hint":  None,
        })
    return docs


# ─────────────────────────────────────────────────────────────────────
# 메인 빌드 함수
# ─────────────────────────────────────────────────────────────────────

def build_rag_documents(
    sim_db:  str  = _DEFAULT_SIM_DB,
    rebuild: bool = False,
) -> int:
    """
    sim.db → rag_documents 테이블 업데이트.
    rebuild=True 면 기존 문서 전체 삭제 후 재생성.
    반환: 추가된 문서 수.
    """
    conn = sqlite3.connect(sim_db, timeout=30)
    conn.row_factory = sqlite3.Row
    try:
        _setup_rag_tables(conn)

        if rebuild:
            conn.execute("DELETE FROM rag_documents")
            conn.execute("DELETE FROM rag_embeddings")
            conn.commit()
            log.info("rag_documents 전체 삭제 (rebuild 모드)")

        existing = {
            r[0]
            for r in conn.execute("SELECT doc_id FROM rag_documents").fetchall()
        }

        all_docs: List[Dict] = []
        all_docs += _build_candidate_docs(conn)
        all_docs += _build_failure_docs(conn)
        all_docs += _build_run_summary_docs(conn)

        now   = datetime.now().isoformat(timespec="seconds")
        added = 0
        for doc in all_docs:
            if doc["doc_id"] in existing:
                continue
            conn.execute("""
                INSERT OR IGNORE INTO rag_documents
                (doc_id, doc_type, session_mode, study_name, run_id,
                 config_hash, date_from, date_to, trade_count,
                 objective_score, profit_factor, max_drawdown,
                 stress_passed, adoption_status, regime_type, session_pattern,
                 title, summary_text, key_facts, reasoning_hint, created_at)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """, (
                doc["doc_id"], doc["doc_type"], doc["session_mode"],
                doc["study_name"], doc["run_id"], doc["config_hash"],
                doc["date_from"], doc["date_to"], doc["trade_count"],
                doc["objective_score"], doc["profit_factor"], doc["max_drawdown"],
                doc["stress_passed"], doc["adoption_status"],
                doc.get("regime_type", "unknown"),
                doc.get("session_pattern"),
                doc["title"], doc["summary_text"],
                doc["key_facts"], doc["reasoning_hint"], now,
            ))
            added += 1

        conn.commit()
        log.info(
            "RAG 문서 빌드 완료: 추가=%d / 전체=%d",
            added, len(all_docs),
        )
        return added

    finally:
        conn.close()


# ═════════════════════════════════════════════════════════════════════
# Phase 2A: pattern_docs 빌더
# sim.db (source) + mmean.db (market_features, pattern_docs target)
# ═════════════════════════════════════════════════════════════════════

_DEFAULT_MMEAN_DB = str(_STORAGE / "mmean.db")

# 스터디당 pattern_case 대상 validation 상위 N개
_TOP_N_CONFIGS = 5

# 문서당 최대 대표 regime_tags 수
_MAX_TAGS = 8

# 태그 우선순위 (방향성 → 변동성 → 수급 → 베이시스 → 신호 → LLM → 세션)
_TAG_PRIORITY = [
    "BULL_CLEAN", "BULL_SOFT", "BEAR_CLEAN", "BEAR_SOFT",
    "RANGE_TIGHT", "RANGE_NOISY",
    "VOL_SPIKE", "VOL_HIGH", "VOL_NORMAL", "VOL_LOW",
    "FOREIGN_BUY_PERSISTENT", "FOREIGN_SELL_PERSISTENT",
    "FOREIGN_BUY_FADING", "FOREIGN_SELL_FADING", "FOREIGN_NEUTRAL",
    "BASIS_POSITIVE", "BASIS_NEGATIVE", "BASIS_FLAT",
    "BASIS_EXPANDING", "BASIS_COMPRESSING",
    "SIGNAL_CLEAR", "SIGNAL_MIXED", "SIGNAL_WEAK",
    "FLOW_LONG_BIAS", "FLOW_SHORT_BIAS", "FLOW_MIXED",
    "LLM_LONG_CONFIRMED", "LLM_SHORT_CONFIRMED", "LLM_NEUTRAL",
    "SESSION_OPENING", "SESSION_MID", "SESSION_CLOSING",
]

# pattern_schema 임포트 (check_failure_trigger, compute_confidence_level)
try:
    from pattern_schema import (
        check_failure_trigger as _check_failure,
        compute_confidence_level as _confidence_level,
        FAILURE_THRESHOLDS as _FAILURE_THR,
    )
    _HAVE_SCHEMA = True
except ImportError:
    _HAVE_SCHEMA = False
    log.warning("pattern_schema 없음 — failure trigger 기능 제한")


# ─────────────────────────────────────────────────────────────────────
# Phase 2A 헬퍼
# ─────────────────────────────────────────────────────────────────────

def _dates_hash(dates: List[str]) -> str:
    """날짜 리스트 -> 8자 결정론적 SHA1 해시."""
    key = "|".join(sorted(str(d) for d in dates))
    return hashlib.sha1(key.encode()).hexdigest()[:8]


def _extract_train_date(study_name: Optional[str]) -> str:
    """
    study_name에서 train_date 추출.
    예: "day_2026-03-18" -> "2026-03-18"
         None -> "unknown"
    """
    if not study_name:
        return "unknown"
    parts = study_name.split("_", 1)
    return parts[1] if len(parts) == 2 else study_name


def _load_regime_tags_for_dates(
    mmean_conn: sqlite3.Connection,
    dates: List[str],
    min_ratio: float = 0.35,
) -> List[str]:
    """
    mmean.db market_features에서 dates 기간의 대표 regime_tags 계산.
    min_ratio 이상 출현한 태그 중 우선순위 순 _MAX_TAGS개 반환.
    """
    if not dates:
        return []

    placeholders = ",".join("?" for _ in dates)
    rows = mmean_conn.execute(
        f"SELECT regime_tags FROM market_features "
        f"WHERE session_date IN ({placeholders}) AND regime_tags IS NOT NULL",
        dates,
    ).fetchall()

    if not rows:
        return []

    counter: Counter = Counter()
    total = len(rows)
    for (tags_json,) in rows:
        try:
            for tag in json.loads(tags_json):
                counter[tag] += 1
        except Exception:
            pass

    eligible = {tag for tag, cnt in counter.items() if cnt / total >= min_ratio}
    result = [t for t in _TAG_PRIORITY if t in eligible][:_MAX_TAGS]
    return result


def _cfg_enter_score(cfg: dict, default: float = 5.0) -> float:
    """day/night config 모두에서 진입 임계값 추출."""
    for key in ("enter_score", "night_raw_score_min", "day_raw_score_min"):
        v = cfg.get(key)
        if v is not None:
            try:
                return float(v)
            except (TypeError, ValueError):
                pass
    return default


def _cfg_conf_min(cfg: dict) -> str:
    """day/night config 모두에서 신뢰도 최솟값 추출 (표시용 문자열)."""
    for key in ("CONFIDENCE_MIN", "min_confidence",
                "night_confidence_min", "day_confidence_min"):
        v = cfg.get(key)
        if v is not None:
            try:
                return f"{float(v):.3f}"
            except (TypeError, ValueError):
                pass
    return "?"


def _infer_level_band(cfg: dict) -> Tuple[int, int]:
    """
    config 파라미터에서 레벨 밴드(L1~L20) 추정.
    enter_score + SL+TP 조합 기반 근사값 (정확한 레벨 매칭 아님).
    """
    try:
        es = _cfg_enter_score(cfg, default=5.0)
    except (TypeError, ValueError):
        es = 5.0
    try:
        sl = float(cfg.get("sim_sl_ticks") or 10.0)
        tp = float(cfg.get("sim_tp_ticks") or 10.0)
    except (TypeError, ValueError):
        sl, tp = 10.0, 10.0

    risk = sl + tp

    if es < 4.0:
        base = 1
    elif es < 5.0:
        base = 4
    elif es < 6.0:
        base = 7
    elif es < 7.5:
        base = 11
    else:
        base = 15

    if risk < 20:
        base = max(1, base - 1)
    elif risk > 60:
        base = min(18, base + 2)
    elif risk > 40:
        base = min(18, base + 1)

    lo = max(1, base)
    hi = min(20, base + 3)
    return lo, hi


def _market_desc_from_tags(tags: List[str]) -> str:
    """regime_tags에서 시장 상황 한 줄 요약."""
    if "BULL_CLEAN" in tags:
        return "강세 추세 장세"
    if "BULL_SOFT" in tags:
        return "약한 상승 장세"
    if "BEAR_CLEAN" in tags:
        return "강세 하락 장세"
    if "BEAR_SOFT" in tags:
        return "약한 하락 장세"
    if "RANGE_TIGHT" in tags:
        return "박스권 횡보 장세"
    if "RANGE_NOISY" in tags:
        return "혼조 노이즈 장세"
    return "방향 미정 장세"


def _confidence_note(level: str) -> str:
    return {
        "HIGH":   "충분한 표본, 신뢰도 높음",
        "MEDIUM": "표본 보통 추가 검증 권장",
        "LOW":    "표본 부족 결과 단정 금지 추가 날짜 검증 필요",
    }.get(level, "신뢰도 미상")


# ─────────────────────────────────────────────────────────────────────
# Phase 2A: summary_text 1층 (규칙 기반 템플릿)
# ─────────────────────────────────────────────────────────────────────

def _text_validation_summary(
    study_name: str,
    session_mode: str,
    source_dates: List[str],
    run_count: int,
    avg_obj: float,
    max_obj: float,
    avg_pf: float,
    avg_dd: float,
    avg_wr: float,
    avg_tc: float,
    fail_count: int,
    regime_tags: List[str],
    confidence_level: str,
) -> str:
    dates_str = ", ".join(source_dates)
    tags_str  = " / ".join(regime_tags) if regime_tags else "태그 없음"
    mkt_desc  = _market_desc_from_tags(regime_tags)
    conf_note = _confidence_note(confidence_level)
    fail_note = f"실패 트리거 {fail_count}건 포함." if fail_count > 0 else "실패 트리거 없음."

    return (
        f"[Validation Summary] {session_mode.upper()} / {study_name}\n"
        f"날짜: {dates_str} | 시장: {mkt_desc}\n"
        f"대표 태그: {tags_str}\n"
        f"검증 런 {run_count}건 완료\n"
        f"  avg obj={avg_obj:.1f} | max obj={max_obj:.1f}\n"
        f"  avg PF={avg_pf:.2f} | avg DD={avg_dd:.0f}t | avg WR={avg_wr:.0%} | avg TC={avg_tc:.1f}\n"
        f"{fail_note} 신뢰도: {confidence_level} ({conf_note})\n"
        f"[주의] 단일 날짜 데이터 기반 결과 단정 금지. 추가 날짜 검증 필요."
    )


def _text_pattern_case(
    session_mode: str,
    source_dates: List[str],
    obj: float,
    pf: float,
    dd: float,
    wr: float,
    tc: int,
    cfg: dict,
    regime_tags: List[str],
    level_band: Tuple[int, int],
    confidence_level: str,
) -> str:
    dates_str = ", ".join(source_dates)
    tags_str  = " / ".join(regime_tags) if regime_tags else "없음"
    mkt_desc  = _market_desc_from_tags(regime_tags)
    lo, hi    = level_band
    conf_note = _confidence_note(confidence_level)

    es = _cfg_enter_score(cfg, default=0.0)
    try:
        sl = float(cfg.get("sim_sl_ticks") or 0)
        tp = float(cfg.get("sim_tp_ticks") or 0)
    except (TypeError, ValueError):
        sl = tp = 0.0
    trail     = cfg.get("sim_trailing_ticks", 0)
    trail_act = cfg.get("sim_trailing_activate", 0)
    llm_on    = cfg.get("LLM_APPLY_ENABLED", 0)
    conf_min  = _cfg_conf_min(cfg)

    llm_str   = "LLM 활성" if llm_on else "LLM 비의존"
    trail_str = (
        f"trailing {float(trail):.0f}t (활성>{float(trail_act):.0f}t)"
        if trail and float(trail) > 0
        else "trailing 없음"
    )

    return (
        f"[Pattern Case] {session_mode.upper()} | 날짜: {dates_str}\n"
        f"장면: {mkt_desc} | 대표 태그: {tags_str}\n"
        f"설정: enter_score={es:.2f}, SL={sl:.0f}t, TP={tp:.0f}t, {trail_str}, {llm_str}, CONF_MIN={conf_min}\n"
        f"레벨 밴드(추정): L{lo:02d}~L{hi:02d}  [근사값 정확한 레벨 매칭 아님]\n"
        f"결과: obj={obj:.1f} | PF={pf:.2f} | DD={dd:.0f}t | WR={wr:.0%} | TC={tc}건\n"
        f"언제 쓸지: {mkt_desc}에서 enter_score={es:.1f}, SL={sl:.0f}t 구조 유효 관측.\n"
        f"언제 피할지: 혼조 저변동성 외국인 역방향 수급 구간에서 신호 왜곡 가능.\n"
        f"신뢰도: {confidence_level} ({conf_note})."
    )


def _text_failure_case(
    session_mode: str,
    source_dates: List[str],
    obj: float,
    pf: Optional[float],
    dd: Optional[float],
    wr: Optional[float],
    tc: int,
    cfg: dict,
    regime_tags: List[str],
    level_band: Tuple[int, int],
    triggers: dict,
) -> str:
    dates_str = ", ".join(source_dates)
    tags_str  = " / ".join(regime_tags) if regime_tags else "없음"
    mkt_desc  = _market_desc_from_tags(regime_tags)
    lo, hi    = level_band

    es = _cfg_enter_score(cfg, default=0.0)
    try:
        sl = float(cfg.get("sim_sl_ticks") or 0)
        tp = float(cfg.get("sim_tp_ticks") or 0)
    except (TypeError, ValueError):
        sl = tp = 0.0
    llm_on   = cfg.get("LLM_APPLY_ENABLED", 0)
    conf_min = _cfg_conf_min(cfg)
    llm_str  = "LLM 활성" if llm_on else "LLM 비의존"

    if triggers.get("profit_factor"):
        fail_reason = f"수익성 붕괴 (PF={pf:.2f} < 1.0)"
    elif triggers.get("drawdown"):
        thr_dd = _FAILURE_THR.get("max_drawdown_ticks_max", 15)
        fail_reason = f"MDD 과다 (DD={dd:.0f}t > {thr_dd:.0f}t 한계)"
    elif triggers.get("objective"):
        fail_reason = f"목적함수 음수 (obj={obj:.1f})"
    elif triggers.get("total_pnl"):
        fail_reason = "누적 손실 과다"
    elif triggers.get("stress"):
        fail_reason = "스트레스 테스트 실패"
    else:
        fail_reason = "복합 실패"

    pf_str = f"{pf:.2f}" if pf is not None else "N/A"
    dd_str = f"{dd:.0f}t" if dd is not None else "N/A"
    wr_str = f"{wr:.0%}" if wr is not None else "N/A"

    return (
        f"[Failure Case] {session_mode.upper()} | 날짜: {dates_str}\n"
        f"장면: {mkt_desc} | 대표 태그: {tags_str}\n"
        f"설정: enter_score={es:.2f}, SL={sl:.0f}t, TP={tp:.0f}t, {llm_str}, CONF_MIN={conf_min}\n"
        f"레벨 밴드(추정): L{lo:02d}~L{hi:02d}\n"
        f"실패 결과: obj={obj:.1f} | PF={pf_str} | DD={dd_str} | WR={wr_str} | TC={tc}건\n"
        f"실패 원인: {fail_reason}\n"
        f"재발 방지: 동일 태그 장면에서 이 레벨 밴드 진입 시 추가 필터 또는 SL 확대 검토.\n"
        f"[주의] 표본 제한으로 인과 관계 단정 금지. 추가 날짜 검증 필요."
    )


# ─────────────────────────────────────────────────────────────────────
# Phase 2A: upsert
# ─────────────────────────────────────────────────────────────────────

def _upsert_pattern_doc(mmean_conn: sqlite3.Connection, doc: dict) -> None:
    """pattern_docs에 문서 UPSERT (INSERT OR REPLACE)."""
    mmean_conn.execute("""
        INSERT OR REPLACE INTO pattern_docs
        (doc_id, doc_type, created_at,
         session_dates_json, regime_tags_json,
         level_band_min, level_band_max,
         sample_size_dates, sample_size_trades,
         confidence_level, summary_text, llm_summary,
         is_active, detail_json)
        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)
    """, (
        doc["doc_id"],
        doc["doc_type"],
        doc.get("created_at", datetime.now().isoformat(timespec="seconds")),
        json.dumps(doc.get("session_dates", []), ensure_ascii=False),
        json.dumps(doc.get("regime_tags", []), ensure_ascii=False),
        doc.get("level_band_min"),
        doc.get("level_band_max"),
        doc.get("sample_size_dates", 0),
        doc.get("sample_size_trades", 0),
        doc.get("confidence_level", "LOW"),
        doc.get("summary_text", ""),
        doc.get("llm_summary"),
        int(doc.get("is_active", True)),
        json.dumps(doc.get("detail_json", {}), ensure_ascii=False),
    ))


# ─────────────────────────────────────────────────────────────────────
# Phase 2A: validation_summary 생성
# ─────────────────────────────────────────────────────────────────────

def build_validation_summaries(
    sim_conn:   sqlite3.Connection,
    mmean_conn: sqlite3.Connection,
) -> int:
    """
    (study_name, session_mode, source_dates) 단위로 validation_summary 생성.
    doc_id = vs_{session_mode}_{train_date}_{valid_dates_hash}
    """
    rows = sim_conn.execute("""
        SELECT r.study_name, r.session_mode, r.source_dates,
               COUNT(r.id)              AS run_count,
               AVG(r.objective_score)   AS avg_obj,
               MAX(r.objective_score)   AS max_obj,
               MIN(r.objective_score)   AS min_obj,
               AVG(r.trade_count)       AS avg_tc,
               AVG(s.profit_factor)     AS avg_pf,
               AVG(ABS(s.max_drawdown)) AS avg_dd,
               AVG(s.win_rate)          AS avg_wr,
               AVG(s.total_pnl)         AS avg_pnl
        FROM   sim_runs r
        LEFT JOIN sim_run_summary s ON s.run_id = r.id
        WHERE  r.run_type = 'validation' AND r.status = 'done'
        GROUP  BY r.study_name, r.session_mode, r.source_dates
        ORDER  BY r.session_mode, r.study_name
    """).fetchall()

    if not rows:
        log.info("build_validation_summaries: validation 런 없음")
        return 0

    now     = datetime.now().isoformat(timespec="seconds")
    created = 0

    for row in rows:
        (study_name, session_mode, src_dates_json,
         run_count, avg_obj, max_obj, min_obj, avg_tc,
         avg_pf, avg_dd, avg_wr, avg_pnl) = row

        session_mode = session_mode or "day"
        src_dates    = _j(src_dates_json)
        train_date   = _extract_train_date(study_name)
        dates_h      = _dates_hash(src_dates)
        doc_id       = f"vs_{session_mode}_{train_date}_{dates_h}"

        # 실패 트리거 발동 건수
        fail_count = 0
        if _HAVE_SCHEMA:
            detail_rows = sim_conn.execute("""
                SELECT r.objective_score, r.trade_count,
                       s.profit_factor, s.max_drawdown, s.total_pnl
                FROM   sim_runs r
                LEFT JOIN sim_run_summary s ON s.run_id = r.id
                WHERE  r.run_type = 'validation' AND r.status = 'done'
                  AND  r.study_name = ? AND r.session_mode = ? AND r.source_dates = ?
            """, (study_name, session_mode, src_dates_json)).fetchall()

            for (d_obj, d_tc, d_pf, d_dd, d_pnl) in detail_rows:
                tr = _check_failure(
                    profit_factor      = d_pf,
                    total_pnl_ticks    = float(d_pnl) if d_pnl is not None else None,
                    max_drawdown_ticks = abs(d_dd) if d_dd else None,
                    objective_score    = d_obj,
                    trade_count        = int(d_tc or 0),
                )
                if tr.get("triggered"):
                    fail_count += 1

        regime_tags = _load_regime_tags_for_dates(mmean_conn, src_dates)
        n_dates     = len(src_dates)
        n_trades    = int(float(avg_tc or 0) * run_count)
        conf_lvl    = _confidence_level(n_dates, n_trades) if _HAVE_SCHEMA else "LOW"

        top_configs = sim_conn.execute("""
            SELECT config_hash, objective_score, trade_count
            FROM   sim_runs
            WHERE  run_type = 'validation' AND status = 'done'
              AND  study_name = ? AND session_mode = ? AND source_dates = ?
              AND  config_hash IS NOT NULL
            ORDER  BY objective_score DESC LIMIT ?
        """, (study_name, session_mode, src_dates_json, _TOP_N_CONFIGS)).fetchall()
        top_cfg_list = [
            {"config_hash": r[0], "obj": round(float(r[1] or 0), 3), "tc": r[2]}
            for r in top_configs
        ]

        summary_text = _text_validation_summary(
            study_name   = study_name or "unknown",
            session_mode = session_mode,
            source_dates = src_dates,
            run_count    = run_count,
            avg_obj      = float(avg_obj or 0),
            max_obj      = float(max_obj or 0),
            avg_pf       = float(avg_pf or 0),
            avg_dd       = float(avg_dd or 0),
            avg_wr       = float(avg_wr or 0),
            avg_tc       = float(avg_tc or 0),
            fail_count   = fail_count,
            regime_tags  = regime_tags,
            confidence_level = conf_lvl,
        )

        detail = {
            "source_dates":   src_dates,
            "run_count":      run_count,
            "avg_obj":        round(float(avg_obj or 0), 3),
            "max_obj":        round(float(max_obj or 0), 3),
            "min_obj":        round(float(min_obj or 0), 3),
            "avg_pf":         round(float(avg_pf or 0), 3),
            "avg_dd":         round(float(avg_dd or 0), 1),
            "avg_wr":         round(float(avg_wr or 0), 4),
            "avg_tc":         round(float(avg_tc or 0), 1),
            "avg_pnl_won":    round(float(avg_pnl or 0), 0),
            "fail_count":     fail_count,
            "top_configs":    top_cfg_list,
            "regime_tags":    regime_tags,
            "confidence_basis": {"sample_size_dates": n_dates, "sample_size_trades": n_trades},
        }

        _upsert_pattern_doc(mmean_conn, {
            "doc_id":           doc_id,
            "doc_type":         "validation_summary",
            "created_at":       now,
            "session_dates":    src_dates,
            "regime_tags":      regime_tags,
            "level_band_min":   None,
            "level_band_max":   None,
            "sample_size_dates":  n_dates,
            "sample_size_trades": n_trades,
            "confidence_level": conf_lvl,
            "summary_text":     summary_text,
            "llm_summary":      None,
            "is_active":        True,
            "detail_json":      detail,
        })
        created += 1
        log.info("validation_summary 생성: %s", doc_id)

    mmean_conn.commit()
    log.info("build_validation_summaries 완료: %d건", created)
    return created


# ─────────────────────────────────────────────────────────────────────
# Phase 2A: pattern_case 생성
# ─────────────────────────────────────────────────────────────────────

def build_pattern_cases(
    sim_conn:   sqlite3.Connection,
    mmean_conn: sqlite3.Connection,
) -> int:
    """
    스터디별 validation 상위 _TOP_N_CONFIGS config에 대해 pattern_case 생성.
    failure trigger 미충족 & trade_count >= min_trades 조건.
    doc_id = pc_{session_mode}_{train_date}_{config_hash[:12]}
    """
    study_rows = sim_conn.execute("""
        SELECT DISTINCT study_name, session_mode, source_dates
        FROM   sim_runs
        WHERE  run_type = 'validation' AND status = 'done'
    """).fetchall()

    now     = datetime.now().isoformat(timespec="seconds")
    created = 0
    min_tc  = _FAILURE_THR.get("min_trades_to_trigger", 5) if _HAVE_SCHEMA else 5

    for (study_name, session_mode, src_dates_json) in study_rows:
        session_mode = session_mode or "day"
        src_dates    = _j(src_dates_json)
        train_date   = _extract_train_date(study_name)
        regime_tags  = _load_regime_tags_for_dates(mmean_conn, src_dates)

        top_rows = sim_conn.execute("""
            SELECT r.id, r.config_hash, r.config_json,
                   r.objective_score, r.trade_count,
                   s.profit_factor, s.max_drawdown, s.win_rate, s.total_pnl
            FROM   sim_runs r
            LEFT JOIN sim_run_summary s ON s.run_id = r.id
            WHERE  r.run_type = 'validation' AND r.status = 'done'
              AND  r.study_name = ? AND r.session_mode = ? AND r.source_dates = ?
              AND  r.trade_count >= ? AND r.config_hash IS NOT NULL
            ORDER  BY r.objective_score DESC LIMIT ?
        """, (study_name, session_mode, src_dates_json, min_tc, _TOP_N_CONFIGS)).fetchall()

        for (run_id, cfg_hash, cfg_json, obj, tc, pf, dd, wr, total_pnl) in top_rows:
            cfg = json.loads(cfg_json) if cfg_json else {}

            if _HAVE_SCHEMA:
                triggers = _check_failure(
                    profit_factor      = pf,
                    total_pnl_ticks    = float(total_pnl) if total_pnl is not None else None,
                    max_drawdown_ticks = abs(dd) if dd else None,
                    objective_score    = obj,
                    trade_count        = int(tc or 0),
                )
                if triggers.get("triggered"):
                    log.debug("pattern_case 제외 (failure trigger): %s", cfg_hash)
                    continue
            else:
                triggers = {}

            level_band = _infer_level_band(cfg)
            lo, hi     = level_band
            conf_lvl   = _confidence_level(len(src_dates), int(tc or 0)) if _HAVE_SCHEMA else "LOW"
            doc_id     = f"pc_{session_mode}_{train_date}_{(cfg_hash or '')[:12]}"

            summary_text = _text_pattern_case(
                session_mode  = session_mode,
                source_dates  = src_dates,
                obj           = float(obj or 0),
                pf            = float(pf or 0),
                dd            = float(dd or 0),
                wr            = float(wr or 0),
                tc            = int(tc or 0),
                cfg           = cfg,
                regime_tags   = regime_tags,
                level_band    = level_band,
                confidence_level = conf_lvl,
            )

            detail = {
                "run_id":          run_id,
                "config_hash":     cfg_hash,
                "source_dates":    src_dates,
                "train_date":      train_date,
                "study_name":      study_name,
                "objective_score": round(float(obj or 0), 3),
                "profit_factor":   round(float(pf or 0), 3) if pf else None,
                "max_drawdown_t":  round(abs(float(dd or 0)), 1) if dd else None,
                "win_rate":        round(float(wr or 0), 4) if wr else None,
                "trade_count":     int(tc or 0),
                "level_band":      {"min": lo, "max": hi, "method": "enter_score+risk"},
                "regime_tags":     regime_tags,
                "failure_trigger": triggers,
                "confidence_basis": {
                    "sample_size_dates":  len(src_dates),
                    "sample_size_trades": int(tc or 0),
                },
                "key_config_params": {
                    k: cfg.get(k) for k in (
                        "enter_score", "enter_gap", "sim_tp_ticks", "sim_sl_ticks",
                        "sim_trailing_ticks", "sim_trailing_activate",
                        "CONFIDENCE_MIN", "LLM_APPLY_ENABLED", "sim_neutral_exit_ticks",
                    )
                },
            }

            _upsert_pattern_doc(mmean_conn, {
                "doc_id":           doc_id,
                "doc_type":         "pattern_case",
                "created_at":       now,
                "session_dates":    src_dates,
                "regime_tags":      regime_tags,
                "level_band_min":   lo,
                "level_band_max":   hi,
                "sample_size_dates":  len(src_dates),
                "sample_size_trades": int(tc or 0),
                "confidence_level": conf_lvl,
                "summary_text":     summary_text,
                "llm_summary":      None,
                "is_active":        True,
                "detail_json":      detail,
            })
            created += 1
            log.info("pattern_case 생성: %s", doc_id)

    mmean_conn.commit()
    log.info("build_pattern_cases 완료: %d건", created)
    return created


# ─────────────────────────────────────────────────────────────────────
# Phase 2A: failure_case 생성
# ─────────────────────────────────────────────────────────────────────

def build_failure_cases(
    sim_conn:   sqlite3.Connection,
    mmean_conn: sqlite3.Connection,
) -> int:
    """
    validation 런 중 failure trigger 충족 config에 대해 failure_case 생성.
    doc_id = fc_{session_mode}_{train_date}_{config_hash[:12]}
    """
    if not _HAVE_SCHEMA:
        log.warning("pattern_schema 없음 failure_case 생성 불가")
        return 0

    study_rows = sim_conn.execute("""
        SELECT DISTINCT study_name, session_mode, source_dates
        FROM   sim_runs
        WHERE  run_type = 'validation' AND status = 'done'
    """).fetchall()

    now     = datetime.now().isoformat(timespec="seconds")
    created = 0
    min_tc  = _FAILURE_THR.get("min_trades_to_trigger", 5)

    for (study_name, session_mode, src_dates_json) in study_rows:
        session_mode = session_mode or "day"
        src_dates    = _j(src_dates_json)
        train_date   = _extract_train_date(study_name)
        regime_tags  = _load_regime_tags_for_dates(mmean_conn, src_dates)

        bad_rows = sim_conn.execute("""
            SELECT r.id, r.config_hash, r.config_json,
                   r.objective_score, r.trade_count,
                   s.profit_factor, s.max_drawdown, s.win_rate, s.total_pnl
            FROM   sim_runs r
            LEFT JOIN sim_run_summary s ON s.run_id = r.id
            WHERE  r.run_type = 'validation' AND r.status = 'done'
              AND  r.study_name = ? AND r.session_mode = ? AND r.source_dates = ?
              AND  r.trade_count >= ? AND r.config_hash IS NOT NULL
            ORDER  BY r.objective_score ASC LIMIT 50
        """, (study_name, session_mode, src_dates_json, min_tc)).fetchall()

        for (run_id, cfg_hash, cfg_json, obj, tc, pf, dd, wr, total_pnl) in bad_rows:
            cfg      = json.loads(cfg_json) if cfg_json else {}
            triggers = _check_failure(
                profit_factor      = pf,
                total_pnl_ticks    = float(total_pnl) if total_pnl is not None else None,
                max_drawdown_ticks = abs(dd) if dd else None,
                objective_score    = obj,
                trade_count        = int(tc or 0),
            )
            if not triggers.get("triggered"):
                continue

            level_band = _infer_level_band(cfg)
            lo, hi     = level_band
            conf_lvl   = _confidence_level(len(src_dates), int(tc or 0))
            doc_id     = f"fc_{session_mode}_{train_date}_{(cfg_hash or '')[:12]}"

            summary_text = _text_failure_case(
                session_mode = session_mode,
                source_dates = src_dates,
                obj          = float(obj or 0),
                pf           = pf,
                dd           = dd,
                wr           = wr,
                tc           = int(tc or 0),
                cfg          = cfg,
                regime_tags  = regime_tags,
                level_band   = level_band,
                triggers     = triggers,
            )

            detail = {
                "run_id":          run_id,
                "config_hash":     cfg_hash,
                "source_dates":    src_dates,
                "train_date":      train_date,
                "study_name":      study_name,
                "objective_score": round(float(obj or 0), 3),
                "profit_factor":   round(float(pf or 0), 3) if pf else None,
                "max_drawdown_t":  round(abs(float(dd or 0)), 1) if dd else None,
                "win_rate":        round(float(wr or 0), 4) if wr else None,
                "trade_count":     int(tc or 0),
                "level_band":      {"min": lo, "max": hi},
                "regime_tags":     regime_tags,
                "failure_trigger": triggers,
                "key_config_params": {
                    k: cfg.get(k) for k in (
                        "enter_score", "enter_gap", "sim_tp_ticks", "sim_sl_ticks",
                        "CONFIDENCE_MIN", "LLM_APPLY_ENABLED",
                    )
                },
            }

            _upsert_pattern_doc(mmean_conn, {
                "doc_id":           doc_id,
                "doc_type":         "failure_case",
                "created_at":       now,
                "session_dates":    src_dates,
                "regime_tags":      regime_tags,
                "level_band_min":   lo,
                "level_band_max":   hi,
                "sample_size_dates":  len(src_dates),
                "sample_size_trades": int(tc or 0),
                "confidence_level": conf_lvl,
                "summary_text":     summary_text,
                "llm_summary":      None,
                "is_active":        True,
                "detail_json":      detail,
            })
            created += 1
            log.info("failure_case 생성: %s", doc_id)

    mmean_conn.commit()
    log.info("build_failure_cases 완료: %d건", created)
    return created


# ─────────────────────────────────────────────────────────────────────
# Phase 2A: 메인 빌드 + 검증
# ─────────────────────────────────────────────────────────────────────

def build_pattern_docs(
    sim_db:   str  = _DEFAULT_SIM_DB,
    mmean_db: str  = _DEFAULT_MMEAN_DB,
    rebuild:  bool = False,
) -> Dict[str, int]:
    """
    Phase 2A 메인 엔트리.
    sim.db -> mmean.db pattern_docs 생성 (validation_summary -> pattern_case -> failure_case).
    stable_config, stress_summary는 Phase 2B로 보류.
    """
    sim_conn   = sqlite3.connect(sim_db,   timeout=30)
    mmean_conn = sqlite3.connect(mmean_db, timeout=30)

    try:
        mmean_conn.execute("PRAGMA journal_mode=WAL")
        mmean_conn.execute("PRAGMA synchronous=NORMAL")

        if rebuild:
            mmean_conn.execute(
                "DELETE FROM pattern_docs WHERE doc_type IN "
                "('validation_summary','pattern_case','failure_case')"
            )
            mmean_conn.commit()
            log.info("pattern_docs 초기화 (rebuild stable_config/stress_summary 유지)")

        counts: Dict[str, int] = {}
        counts["validation_summary"] = build_validation_summaries(sim_conn, mmean_conn)
        counts["pattern_case"]       = build_pattern_cases(sim_conn, mmean_conn)
        counts["failure_case"]       = build_failure_cases(sim_conn, mmean_conn)
        counts["total"]              = sum(v for k, v in counts.items() if k != "total")

        log.info(
            "build_pattern_docs 완료 | VS=%d PC=%d FC=%d 합계=%d",
            counts["validation_summary"],
            counts["pattern_case"],
            counts["failure_case"],
            counts["total"],
        )
        return counts

    finally:
        sim_conn.close()
        mmean_conn.close()


def verify_pattern_docs(mmean_db: str = _DEFAULT_MMEAN_DB) -> None:
    """pattern_docs 검증용 SQL 실행 + 결과 출력 (Phase 2A 완료 기준 체크리스트)."""
    conn = sqlite3.connect(mmean_db)
    conn.row_factory = sqlite3.Row
    sep  = "-" * 60

    print(f"\n{'='*60}")
    print(f"  Pattern Docs 검증 리포트")
    print(f"{'='*60}")

    total = conn.execute("SELECT COUNT(*) FROM pattern_docs").fetchone()[0]
    print(f"\n[1] 총 문서 수: {total}건")

    print(f"\n[2] doc_type별:")
    for r in conn.execute(
        "SELECT doc_type, COUNT(*) AS cnt FROM pattern_docs "
        "GROUP BY doc_type ORDER BY doc_type"
    ).fetchall():
        print(f"    {r['doc_type']:<25} {r['cnt']:>4}건")

    print(f"\n[3] confidence_level별:")
    for r in conn.execute(
        "SELECT confidence_level, COUNT(*) AS cnt FROM pattern_docs "
        "GROUP BY confidence_level ORDER BY confidence_level"
    ).fetchall():
        print(f"    {r['confidence_level']:<10} {r['cnt']:>4}건")

    active_cnt = conn.execute("SELECT COUNT(*) FROM pattern_docs WHERE is_active=1").fetchone()[0]
    print(f"\n[4] 활성 문서: {active_cnt}건 / {total}건")

    low_cnt = conn.execute(
        "SELECT COUNT(*) FROM pattern_docs WHERE confidence_level='LOW'"
    ).fetchone()[0]
    low_pct = low_cnt / total * 100 if total else 0
    print(f"\n[5] LOW confidence 비율: {low_cnt}건 ({low_pct:.0f}%)")

    print(f"\n[6] 최근 생성 문서 (5건):")
    for r in conn.execute(
        "SELECT doc_id, doc_type, confidence_level, created_at "
        "FROM pattern_docs ORDER BY created_at DESC LIMIT 5"
    ).fetchall():
        print(f"    [{r['doc_type'][:2]}] {r['doc_id'][:52]:<52} {r['confidence_level']} | {r['created_at'][:19]}")

    print(f"\n[7] regime_tags 검색 테스트:")
    for tag in ("BEAR_CLEAN", "BULL_CLEAN", "RANGE_NOISY",
                "FOREIGN_SELL_PERSISTENT", "VOL_HIGH"):
        cnt = conn.execute(
            "SELECT COUNT(*) FROM pattern_docs WHERE regime_tags_json LIKE ?",
            (f'%{tag}%',)
        ).fetchone()[0]
        print(f"    {tag:<30} -> {cnt}건")

    sample_hash = conn.execute(
        "SELECT json_extract(detail_json,'$.config_hash') FROM pattern_docs "
        "WHERE doc_type='pattern_case' LIMIT 1"
    ).fetchone()
    if sample_hash and sample_hash[0]:
        h = sample_hash[0][:12]
        cnt = conn.execute(
            "SELECT COUNT(*) FROM pattern_docs WHERE detail_json LIKE ?",
            (f'%{h}%',)
        ).fetchone()[0]
        print(f"\n[8] config_hash 검색 ('{h[:8]}...'): {cnt}건")
    else:
        print(f"\n[8] config_hash 검색: pattern_case 없음")

    print(f"\n[9] 날짜 연결성:")
    vs_dates: set = set()
    pc_dates: set = set()
    for r in conn.execute(
        "SELECT session_dates_json FROM pattern_docs WHERE doc_type='validation_summary'"
    ).fetchall():
        vs_dates.update(json.loads(r[0] or "[]"))
    for r in conn.execute(
        "SELECT session_dates_json FROM pattern_docs WHERE doc_type='pattern_case'"
    ).fetchall():
        pc_dates.update(json.loads(r[0] or "[]"))
    overlap = vs_dates & pc_dates
    print(f"    VS 날짜: {sorted(vs_dates)}")
    print(f"    PC 날짜: {sorted(pc_dates)}")
    print(f"    공통 날짜: {sorted(overlap)}")

    print(f"\n[10] validation_summary 본문 샘플:")
    sample = conn.execute(
        "SELECT doc_id, summary_text FROM pattern_docs WHERE doc_type='validation_summary' LIMIT 1"
    ).fetchone()
    if sample:
        print(f"  doc_id: {sample['doc_id']}")
        print(sep)
        print(sample["summary_text"])
        print(sep)

    if total > 0:
        print(f"\n[11] pattern_case level_band 분포:")
        for r in conn.execute(
            "SELECT level_band_min, level_band_max, COUNT(*) AS cnt "
            "FROM pattern_docs WHERE doc_type='pattern_case' "
            "GROUP BY level_band_min, level_band_max ORDER BY level_band_min"
        ).fetchall():
            print(f"    L{r['level_band_min']:02d}~L{r['level_band_max']:02d}  {r['cnt']}건")

    print(f"\n{'='*60}\n")
    conn.close()


# ─────────────────────────────────────────────────────────────────────
# CLI
# ─────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    import argparse

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
    )

    parser = argparse.ArgumentParser(description="MMEAN RAG Builder")
    parser.add_argument("--pattern-docs", action="store_true",
                        help="Phase 2A: mmean.db pattern_docs 생성/갱신")
    parser.add_argument("--all", action="store_true",
                        help="Phase 1 rag_documents + Phase 2A pattern_docs 동시 실행")
    parser.add_argument("--rebuild", action="store_true",
                        help="기존 문서 삭제 후 전체 재생성")
    parser.add_argument("--verify", action="store_true",
                        help="pattern_docs 검증 리포트만 출력 (생성 없음)")
    parser.add_argument("--sim-db",   default=_DEFAULT_SIM_DB,
                        help=f"sim.db 경로 (기본: {_DEFAULT_SIM_DB})")
    parser.add_argument("--mmean-db", default=_DEFAULT_MMEAN_DB,
                        help=f"mmean.db 경로 (기본: {_DEFAULT_MMEAN_DB})")
    args = parser.parse_args()

    if args.verify:
        verify_pattern_docs(args.mmean_db)
    elif args.pattern_docs or args.all:
        if args.all:
            n1 = build_rag_documents(sim_db=args.sim_db, rebuild=args.rebuild)
            print(f"[Phase 1] rag_documents 추가: {n1}개")
        counts = build_pattern_docs(
            sim_db=args.sim_db, mmean_db=args.mmean_db, rebuild=args.rebuild
        )
        print(
            f"[Phase 2A] pattern_docs 생성: "
            f"VS={counts['validation_summary']} "
            f"PC={counts['pattern_case']} "
            f"FC={counts['failure_case']} "
            f"합계={counts['total']}"
        )
        verify_pattern_docs(args.mmean_db)
    else:
        # 기본 동작: Phase 1 rag_documents
        n = build_rag_documents(sim_db=args.sim_db, rebuild=args.rebuild)
        print(f"[Phase 1] rag_documents 추가: {n}개")
