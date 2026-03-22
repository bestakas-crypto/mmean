# MMEAN/rag_retriever.py
"""
RAG 검색기 — 현재 장중 상태 → 유사 사례 검색 → LLM 프롬프트용 context 반환

흐름:
    현재 상태 dict
        → build_query_text()  : 검색 질의 텍스트
        → sim_vector_store.search() : 유사 candidate/failure 문서 top-k
        → _format_context()   : LLM 프롬프트 삽입용 텍스트 블록
        → retrieve_context()  : 최종 결과 dict 반환

사용 예:
    from rag_retriever import retrieve_context

    ctx = retrieve_context({
        "session_mode": "day",
        "regime":       "NEUTRAL",
        "entry_signal": "LONG_READY",
        "confidence":   0.72,
        "flow_score":   0.15,
        "bias":         "LONG_BIAS",
        "vwap_position": "above",
        "foreign_buy":  8000,
    })

    # LLM 프롬프트에 ctx["context_text"] 삽입
    # 원본 리스트: ctx["success"], ctx["failure"]
"""
from __future__ import annotations

import logging
from pathlib import Path
from typing import Dict, List, Optional

log = logging.getLogger("MMEAN.RAGRetriever")

_STORAGE = Path(__file__).resolve().parent.parent / "storage"
_DEFAULT_SIM_DB = str(_STORAGE / "sim.db")


# ─────────────────────────────────────────────────────────────────────
# 현재 상태 → 검색 질의 텍스트
# ─────────────────────────────────────────────────────────────────────

def build_query_text(state: Dict) -> str:
    """
    현재 장중 상태 dict → 벡터 검색 질의 텍스트 (compact).

    임베딩 품질을 위해 핵심 키-값만 남기고 설명문 제거.

    state 필드:
        session_mode   : 'day' | 'night'
        regime         : 'NEUTRAL' | 'LONG' | 'SHORT' | ...
        entry_signal   : 'LONG_READY' | 'SHORT_READY' | 'NONE' | ...
        confidence     : float
        flow_score     : float
        bias           : 'LONG_BIAS' | 'SHORT_BIAS' | 'NEUTRAL'
        vwap_position  : 'above' | 'below' | 'near'   (선택)
        foreign_buy    : int                           (선택)
        session_phase   : 'open' | 'mid' | 'close'     (선택)
        session_pattern : 'TREND_UP' | 'CHOPPY' | ... (선택)
    """
    sm    = state.get("session_mode", "day")
    reg   = state.get("regime", "NEUTRAL")
    sig   = state.get("entry_signal", "")
    conf  = state.get("confidence", 0.0)
    flow  = state.get("flow_score", 0.0)
    bias  = state.get("bias", "NEUTRAL")
    vwap  = state.get("vwap_position", "")
    fb    = state.get("foreign_buy", 0)
    phase = state.get("session_phase", "")
    sp    = state.get("session_pattern", "")

    tokens = [sm, reg, sig, f"conf={conf:.2f}", f"flow={flow:.2f}", bias]
    if sp:
        tokens.append(f"pattern={sp}")
    if vwap:
        tokens.append(f"vwap={vwap}")
    if fb:
        tokens.append(f"fb={fb}")
    if phase:
        tokens.append(phase)

    return " ".join(tokens)


# ─────────────────────────────────────────────────────────────────────
# context 포맷 (LLM 프롬프트 삽입용)
# ─────────────────────────────────────────────────────────────────────

def _regime_mismatch(doc: Dict, current_regime: str,
                     current_session_pattern: Optional[str] = None) -> bool:
    """
    문서의 패턴/레짐과 현재 상태가 명백히 다른지 판단.

    우선순위:
      1) session_pattern 필드가 모두 존재 → 직접 비교 (exact match)
      2) session_pattern 없음 → regime_type proxy 비교 (기존 로직)
    """
    doc_sp = doc.get("session_pattern")

    # ── 1) session_pattern 직접 비교 ──────────────────────────────
    if doc_sp and current_session_pattern:
        return doc_sp != current_session_pattern

    # ── 2) proxy regime_type 비교 (fallback) ──────────────────────
    doc_rt = doc.get("regime_type", "unknown")
    if doc_rt == "unknown":
        return False
    cur_is_trend = current_regime in ("LONG", "SHORT")
    if cur_is_trend and doc_rt == "range":
        return True
    if not cur_is_trend and doc_rt == "trend":
        return True
    return False


def _format_context(
    success_docs:            List[Dict],
    failure_docs:            List[Dict],
    current_regime:          str           = "NEUTRAL",
    current_session_pattern: Optional[str] = None,
) -> str:
    lines: List[str] = []

    if success_docs:
        lines.append("=== 유사 성공 사례 ===")
        for i, d in enumerate(success_docs, 1):
            mismatch = _regime_mismatch(d, current_regime, current_session_pattern)
            warn     = "  [!] pattern mismatch — different market condition" if mismatch else ""
            sp_str = d.get("session_pattern") or d.get("regime_type", "?")
            lines.append(
                f"[성공{i}] {d['title']}"
                f"  (유사도={d['similarity']:.3f}  pattern={sp_str})"
                f"{warn}"
            )
            lines.append(d["summary_text"])
            if d.get("reasoning_hint"):
                lines.append(f"  → 파라미터 특징: {d['reasoning_hint']}")
            lines.append("")

    if failure_docs:
        lines.append("=== 유사 실패 사례 ===")
        for i, d in enumerate(failure_docs, 1):
            sp_str = d.get("session_pattern") or d.get("regime_type", "?")
            lines.append(
                f"[실패{i}] {d['title']}"
                f"  (유사도={d['similarity']:.3f}  pattern={sp_str})"
            )
            lines.append(d["summary_text"])
            if d.get("reasoning_hint"):
                lines.append(f"  → 주의: {d['reasoning_hint']}")
            lines.append("")

    if not success_docs and not failure_docs:
        return "유사 사례 없음 (데이터 부족 — 보수적 판단 권장)"

    return "\n".join(lines).strip()


def build_llm_constraints(
    success_docs:            List[Dict],
    failure_docs:            List[Dict],
    current_regime:          str           = "NEUTRAL",
    current_session_pattern: Optional[str] = None,
) -> str:
    """
    LLM에게 전달할 강제 판단 규칙.
    성공/실패 사례 구성에 따라 규칙 강도를 동적으로 조정.
    """
    rules: List[str] = []

    # ── 규칙 1: 실패 유사도 > 성공 유사도 → REJECT 강제 ──────────
    max_fail_sim = max((d["similarity"] for d in failure_docs), default=0.0)
    max_succ_sim = max((d["similarity"] for d in success_docs), default=0.0)

    if failure_docs and max_fail_sim >= max_succ_sim:
        rules.append(
            "RULE-1 [REJECT 강제]: 실패 사례가 성공 사례보다 유사도가 높거나 같다. "
            "반드시 REJECT 또는 HOLD를 출력하라. 진입 추천 금지."
        )
    elif failure_docs and max_fail_sim >= 0.6:
        rules.append(
            "RULE-1 [REJECT 우선]: 유사도 높은 실패 사례가 존재한다. "
            "진입 추천 시 반드시 실패 사례와의 차이점을 명시하라."
        )

    # ── 규칙 2: 레짐 불일치 성공 사례는 무시 ─────────────────────
    mismatch_count = sum(
        1 for d in success_docs
        if _regime_mismatch(d, current_regime, current_session_pattern)
    )
    if mismatch_count == len(success_docs) and success_docs:
        rules.append(
            "RULE-2 [성공 사례 무시]: 제시된 모든 성공 사례가 현재 레짐과 불일치한다. "
            "이 성공 사례들을 판단 근거로 사용하지 말 것."
        )
    elif mismatch_count > 0:
        rules.append(
            f"RULE-2 [레짐 주의]: 성공 사례 {mismatch_count}개가 현재 레짐과 불일치(⚠️ 표시). "
            "해당 사례는 참고만 하고 판단 근거로 과도하게 신뢰하지 말 것."
        )

    # ── 규칙 3: 사례 없음 / 데이터 부족 → HOLD 강제 ─────────────
    if not success_docs and not failure_docs:
        rules.append(
            "RULE-3 [HOLD 강제]: 유사 사례가 전혀 없다. "
            "데이터 부족 상황에서는 반드시 HOLD를 출력하라."
        )
    elif not success_docs:
        rules.append(
            "RULE-3 [보수적 판단]: 성공 사례가 없고 실패 사례만 존재한다. "
            "확신이 없으면 HOLD를 출력하라."
        )

    # ── 공통 규칙 (항상 포함) ─────────────────────────────────────
    rules.append(
        "COMMON: 엔진의 기본 조건(SL/TP/진입 기준)을 뒤집지 말 것. "
        "LLM은 필터 역할이며 최종 결정권은 엔진에 있다. "
        "근거가 부족하면 weight_adjust=0, direction=HOLD를 출력하라."
    )

    return "\n".join(f"  {r}" for r in rules)


# ─────────────────────────────────────────────────────────────────────
# 메인 검색 함수
# ─────────────────────────────────────────────────────────────────────

def retrieve_context(
    state:           Dict,
    sim_db:          str           = _DEFAULT_SIM_DB,
    top_k_success:   int           = 2,
    top_k_failure:   int           = 2,
    min_similarity:  float         = 0.30,
    session_pattern: Optional[str] = None,
) -> Dict:
    """
    현재 상태에서 유사 성공/실패 사례 검색.

    session_pattern : 오늘 날짜의 session_pattern (TREND_UP 등).
                      제공 시 같은 패턴 문서를 우선 검색하고,
                      결과가 부족하면 전체 풀로 fallback.

    반환:
        {
            "success":          [...],   # candidate 문서 리스트
            "failure":          [...],   # failure  문서 리스트
            "context_text":     "...",   # LLM 프롬프트 삽입용 텍스트
            "has_data":         bool,    # 유사 사례 존재 여부
            "pattern_filtered": bool,    # 패턴 필터 검색이 사용되었는지
        }
    """
    from sim_vector_store import search

    session_mode = state.get("session_mode", "day")
    # session_pattern은 state에서도 읽을 수 있도록 (파라미터 우선)
    sp = session_pattern or state.get("session_pattern")
    query = build_query_text(state)

    def _search_docs(doc_type: str, top_k: int,
                     pattern_filter: Optional[str]) -> List[Dict]:
        return search(
            query,
            sim_db           = sim_db,
            session_mode     = session_mode,
            doc_types        = [doc_type],
            top_k            = top_k,
            session_pattern  = pattern_filter,
        )

    try:
        # ── 1차: 같은 패턴 문서만 검색 ──────────────────────────
        pattern_filtered = False
        if sp:
            success_docs = _search_docs("candidate", top_k_success, sp)
            failure_docs = _search_docs("failure",   top_k_failure,  sp)
            # 유사도 하한 적용
            success_docs = [d for d in success_docs if d["similarity"] >= min_similarity]
            failure_docs = [d for d in failure_docs if d["similarity"] >= min_similarity]

            if success_docs or failure_docs:
                pattern_filtered = True
                log.info(
                    "RAG 패턴 필터 검색 | session=%s pattern=%s | 성공=%d 실패=%d",
                    session_mode, sp, len(success_docs), len(failure_docs),
                )
            else:
                log.info(
                    "RAG 패턴 필터 결과 없음 — 전체 풀로 fallback (pattern=%s)", sp
                )

        # ── 2차: 전체 풀 검색 (패턴 없거나 1차 결과 없을 때) ────
        if not sp or not pattern_filtered:
            success_docs = _search_docs("candidate", top_k_success, None)
            failure_docs = _search_docs("failure",   top_k_failure,  None)
            success_docs = [d for d in success_docs if d["similarity"] >= min_similarity]
            failure_docs = [d for d in failure_docs if d["similarity"] >= min_similarity]

    except Exception as exc:
        log.warning("RAG 검색 실패 (무시하고 진행): %s", exc)
        return {
            "success":          [],
            "failure":          [],
            "context_text":     f"RAG 검색 오류 (무시됨): {exc}",
            "has_data":         False,
            "pattern_filtered": False,
        }

    current_regime = state.get("regime", "NEUTRAL")
    context_text   = _format_context(
        success_docs, failure_docs, current_regime, sp
    )
    constraints = build_llm_constraints(
        success_docs, failure_docs, current_regime, sp
    )
    has_data = bool(success_docs or failure_docs)

    # 강제 규칙을 context 맨 아래에 항상 포함
    context_text = context_text + "\n\n=== LLM 판단 강제 규칙 ===\n" + constraints

    if has_data:
        log.info(
            "RAG 검색 완료 | session=%s | 성공=%d 실패=%d | pattern_filtered=%s",
            session_mode, len(success_docs), len(failure_docs), pattern_filtered,
        )
    else:
        log.info("RAG 검색: 유사 사례 없음 (session=%s)", session_mode)

    return {
        "success":          success_docs,
        "failure":          failure_docs,
        "context_text":     context_text,
        "constraints":      constraints,
        "has_data":         has_data,
        "pattern_filtered": pattern_filtered,
    }


# ─────────────────────────────────────────────────────────────────────
# 실패 회피 메모리 — 체크포인트 조합 기반 패턴 경고
# ─────────────────────────────────────────────────────────────────────

_DEFAULT_MMEAN_DB = str(Path(__file__).resolve().parent.parent / "storage" / "mmean.db")

# 주입 대상 confidence 최소 기준 (LOW는 너무 적은 샘플 — 과보수 방지)
_MIN_INJECT_CONFIDENCE = {"MED", "HIGH"}


def get_failure_avoidance_context(
    opening_char:     Optional[str],
    prov_regime:      Optional[str],
    afternoon_regime: Optional[str],
    mmean_db:         str = _DEFAULT_MMEAN_DB,
) -> Optional[str]:
    """
    현재 체크포인트 조합 → failure_patterns 조회 → LLM 주입용 경고 텍스트 반환.

    반환 None: 해당 조합의 패턴 없거나 confidence=LOW (과보수 방지).

    주입 대상: forbidden(⛔) + failure(⚠️) 패턴만.
    success 패턴은 LLM에 주입하지 않음 (진입 확증 편향 방지).
    """
    # 현재 상태로 조합 가능한 combo_key 목록 생성
    combos: List[tuple] = []    # (combo_type, combo_key)
    oc = opening_char   or None
    pr = prov_regime    or None
    ar = afternoon_regime or None

    if oc and pr:
        combos.append(("oc_prov",   f"{oc}|{pr}"))
    if pr and ar:
        combos.append(("prov_ar",   f"{pr}|{ar}"))
    if oc and ar:
        combos.append(("oc_ar",     f"{oc}|{ar}"))
    if oc:
        combos.append(("single_oc", f"{oc}"))

    if not combos:
        return None

    try:
        conn = sqlite3.connect(
            f"file:{mmean_db}?mode=ro", uri=True, timeout=5
        )
        conn.row_factory = sqlite3.Row

        findings: List[str] = []
        for combo_type, combo_key in combos:
            row = conn.execute("""
                SELECT pattern_type, n, win_rate, avg_pnl,
                       worst_pnl, confidence, summary_text
                FROM failure_patterns
                WHERE combo_key  = ?
                  AND combo_type = ?
                  AND pattern_type IN ('forbidden','failure')
                ORDER BY
                    CASE pattern_type WHEN 'forbidden' THEN 0 ELSE 1 END,
                    n DESC
                LIMIT 1
            """, (combo_key, combo_type)).fetchone()

            if not row:
                continue
            if row["confidence"] not in _MIN_INJECT_CONFIDENCE:
                continue   # LOW confidence — 과보수 방지로 건너뜀

            findings.append(row["summary_text"])

        conn.close()
    except Exception as e:
        log.warning("get_failure_avoidance_context 오류 (무시됨): %s", e)
        return None

    if not findings:
        return None

    lines = ["[진입 회피 패턴 — 과거 반복 실패 조합]"]
    for f in findings:
        lines.append(f"  {f}")
    lines.append(
        "  ※ 위 패턴과 현재 조건이 일치할 경우 진입에 신중하게 판단하라."
        " (단, 다른 강력한 진입 근거가 있으면 override 가능)"
    )
    return "\n".join(lines)


def get_failure_pattern_meta(
    opening_char:     Optional[str],
    prov_regime:      Optional[str],
    afternoon_regime: Optional[str],
    mmean_db:         str = _DEFAULT_MMEAN_DB,
) -> tuple:
    """
    엔진 entry_gate 계산용 — (pattern_type, confidence) 반환.

    get_failure_avoidance_context()와 달리 포맷 텍스트 없이
    가장 심각한 패턴의 (pattern_type, confidence)만 반환한다.
    forbidden > failure 우선. 해당 없으면 ('none', 'NONE').
    """
    combos: List[tuple] = []
    oc = opening_char    or None
    pr = prov_regime     or None
    ar = afternoon_regime or None

    if oc and pr:
        combos.append(("oc_prov",   f"{oc}|{pr}"))
    if pr and ar:
        combos.append(("prov_ar",   f"{pr}|{ar}"))
    if oc and ar:
        combos.append(("oc_ar",     f"{oc}|{ar}"))
    if oc:
        combos.append(("single_oc", f"{oc}"))

    if not combos:
        return ("none", "NONE")

    try:
        conn = sqlite3.connect(
            f"file:{mmean_db}?mode=ro", uri=True, timeout=5
        )
        conn.row_factory = sqlite3.Row
        for combo_type, combo_key in combos:
            row = conn.execute("""
                SELECT pattern_type, confidence FROM failure_patterns
                WHERE combo_key  = ?
                  AND combo_type = ?
                  AND pattern_type IN ('forbidden', 'failure')
                ORDER BY
                    CASE pattern_type WHEN 'forbidden' THEN 0 ELSE 1 END,
                    n DESC
                LIMIT 1
            """, (combo_key, combo_type)).fetchone()
            if row:
                conn.close()
                return (str(row["pattern_type"]), str(row["confidence"]))
        conn.close()
    except Exception as e:
        log.warning("get_failure_pattern_meta 오류: %s", e)

    return ("none", "NONE")


# ─────────────────────────────────────────────────────────────────────
# 빠른 빌드 + 임베딩 + 검색 (원스톱)
# ─────────────────────────────────────────────────────────────────────

def build_and_embed(sim_db: str = _DEFAULT_SIM_DB) -> None:
    """
    sim_rag_builder + sim_vector_store 순서대로 실행.
    새로 생긴 sim_adoptions / sim_run_summary 반영 후 검색 가능하게 만든다.
    """
    from sim_rag_builder  import build_rag_documents
    from sim_vector_store import embed_documents

    n_docs = build_rag_documents(sim_db=sim_db)
    n_emb  = embed_documents(sim_db=sim_db, only_new=True)
    log.info("build_and_embed 완료 | 문서 추가=%d 임베딩=%d", n_docs, n_emb)


# ─────────────────────────────────────────────────────────────────────
# CLI (빠른 테스트)
# ─────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    import logging as _logging
    _logging.basicConfig(
        level=_logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
    )

    # 테스트 상태
    test_state = {
        "session_mode":  "day",
        "regime":        "NEUTRAL",
        "entry_signal":  "LONG_READY",
        "confidence":    0.72,
        "flow_score":    0.15,
        "bias":          "LONG_BIAS",
        "vwap_position": "above",
        "foreign_buy":   8000,
    }

    print("=== 검색 질의 ===")
    print(build_query_text(test_state))
    print()

    result = retrieve_context(test_state)
    print("=== RAG Context ===")
    print(result["context_text"])
    print(f"\nhas_data={result['has_data']}")
