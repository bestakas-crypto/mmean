# MMEAN/sim_vector_store.py
"""
벡터 저장소 — rag_documents 임베딩 생성 및 유사도 검색

임베딩: OpenAI text-embedding-3-small
저장:   rag_embeddings 테이블 (float32 BLOB, sim.db 내부)
검색:   numpy cosine similarity (별도 벡터 DB 불필요)

의존: openai, numpy
    pip install openai numpy

사용:
    # 신규 문서만 임베딩
    python sim_vector_store.py

    # 전체 재임베딩
    python sim_vector_store.py --rebuild

    # 검색 테스트
    python sim_vector_store.py --search "LONG 신호 NEUTRAL 레짐 야간장"
"""
from __future__ import annotations

import json
import logging
import os
import sqlite3
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional

import numpy as np

log = logging.getLogger("MMEAN.VectorStore")

_STORAGE        = Path(__file__).resolve().parent.parent / "storage"
_DEFAULT_SIM_DB = str(_STORAGE / "sim.db")
_DEFAULT_MODEL      = os.getenv("LLM_EMBEDDING_MODEL", "text-embedding-3-small")
_EMBED_BATCH_SIZE   = 100


# ─────────────────────────────────────────────────────────────────────
# OpenAI 클라이언트
# ─────────────────────────────────────────────────────────────────────

def _get_client():
    import openai
    key = (
        os.getenv("LLM_API_KEY_OPENAI")
        or os.getenv("OPENAI_API_KEY")
        or ""
    )
    if not key:
        raise RuntimeError(
            "OPENAI_API_KEY 또는 LLM_API_KEY_OPENAI 환경변수가 없습니다."
        )
    return openai.OpenAI(api_key=key)


# ─────────────────────────────────────────────────────────────────────
# 임베딩 생성
# ─────────────────────────────────────────────────────────────────────

def _embed_texts(texts: List[str], model: str) -> np.ndarray:
    """
    texts → (N, D) float32 numpy array (L2 정규화 완료).
    cosine similarity = 내적으로 계산 가능.
    """
    client   = _get_client()
    all_vecs: List[List[float]] = []

    for i in range(0, len(texts), _EMBED_BATCH_SIZE):
        batch = texts[i : i + _EMBED_BATCH_SIZE]
        resp  = client.embeddings.create(input=batch, model=model)
        # index 기준 정렬 (API 응답 순서 보장)
        vecs  = [r.embedding for r in sorted(resp.data, key=lambda x: x.index)]
        all_vecs.extend(vecs)

    arr   = np.array(all_vecs, dtype=np.float32)
    norms = np.linalg.norm(arr, axis=1, keepdims=True)
    arr   = arr / np.maximum(norms, 1e-9)
    return arr


# ─────────────────────────────────────────────────────────────────────
# BLOB 변환 헬퍼
# ─────────────────────────────────────────────────────────────────────

def _to_blob(vec: np.ndarray) -> bytes:
    return vec.astype(np.float32).tobytes()


def _from_blob(blob: bytes) -> np.ndarray:
    return np.frombuffer(blob, dtype=np.float32).copy()


# ─────────────────────────────────────────────────────────────────────
# 임베딩 생성 & 저장
# ─────────────────────────────────────────────────────────────────────

def embed_documents(
    sim_db:    str  = _DEFAULT_SIM_DB,
    model:     str  = _DEFAULT_MODEL,
    only_new:  bool = True,
) -> int:
    """
    rag_documents 중 embedded=0 인 문서를 임베딩 → rag_embeddings 저장.
    only_new=False 면 전체 재임베딩 (rebuild 용).
    반환: 처리된 문서 수.
    """
    conn = sqlite3.connect(sim_db, timeout=30)
    conn.row_factory = sqlite3.Row
    try:
        query = (
            "SELECT doc_id, title, summary_text, key_facts "
            "FROM rag_documents WHERE embedded = 0"
            if only_new
            else
            "SELECT doc_id, title, summary_text, key_facts FROM rag_documents"
        )
        rows = conn.execute(query).fetchall()

        if not rows:
            log.info("임베딩할 문서 없음")
            return 0

        doc_ids = [r["doc_id"] for r in rows]
        texts   = [
            f"{r['title']}\n{r['summary_text']}\n{r['key_facts'] or ''}"
            for r in rows
        ]

        log.info("임베딩 생성: %d개 문서 | model=%s", len(texts), model)
        vecs = _embed_texts(texts, model)

        now = datetime.now().isoformat(timespec="seconds")
        for doc_id, vec in zip(doc_ids, vecs):
            conn.execute("""
                INSERT OR REPLACE INTO rag_embeddings
                (doc_id, embedding, model, created_at)
                VALUES (?, ?, ?, ?)
            """, (doc_id, _to_blob(vec), model, now))

        # embedded 플래그 갱신
        conn.execute(
            "UPDATE rag_documents SET embedded = 1 "
            "WHERE doc_id IN ({})".format(",".join("?" * len(doc_ids))),
            doc_ids,
        )
        conn.commit()
        log.info("임베딩 완료: %d개", len(doc_ids))
        return len(doc_ids)

    finally:
        conn.close()


# ─────────────────────────────────────────────────────────────────────
# 유사도 검색
# ─────────────────────────────────────────────────────────────────────

def search(
    query:           str,
    sim_db:          str            = _DEFAULT_SIM_DB,
    session_mode:    Optional[str]  = None,
    doc_types:       Optional[List[str]] = None,
    top_k:           int            = 5,
    model:           str            = _DEFAULT_MODEL,
    session_pattern: Optional[str]  = None,
) -> List[Dict]:
    """
    query 텍스트와 유사한 RAG 문서 top_k 반환.

    session_mode    : 'day' | 'night' | None (모두)
    doc_types       : ['candidate', 'failure', 'run_summary'] | None (모두)
    session_pattern : 'TREND_UP' | 'CHOPPY' | ... | None (모두) — 같은 패턴만 필터
    """
    # 쿼리 임베딩
    client = _get_client()
    resp   = client.embeddings.create(input=[query], model=model)
    q_vec  = np.array(resp.data[0].embedding, dtype=np.float32)
    q_vec  = q_vec / np.maximum(np.linalg.norm(q_vec), 1e-9)

    conn = sqlite3.connect(sim_db, timeout=30)
    conn.row_factory = sqlite3.Row
    try:
        where = ["e.doc_id = d.doc_id", "d.embedded = 1"]
        params: list = []

        if session_mode:
            where.append("d.session_mode = ?")
            params.append(session_mode)
        if doc_types:
            ph = ",".join("?" * len(doc_types))
            where.append(f"d.doc_type IN ({ph})")
            params.extend(doc_types)
        if session_pattern:
            where.append("d.session_pattern = ?")
            params.append(session_pattern)

        where_sql = " AND ".join(where)
        rows = conn.execute(f"""
            SELECT d.doc_id, d.doc_type, d.session_mode,
                   d.title, d.summary_text, d.key_facts, d.reasoning_hint,
                   d.objective_score, d.profit_factor, d.trade_count, d.stress_passed,
                   d.regime_type, d.session_pattern,
                   e.embedding
            FROM   rag_documents d, rag_embeddings e
            WHERE  {where_sql}
        """, params).fetchall()

        if not rows:
            return []

        # cosine similarity (내적 = L2 정규화 벡터 간 cosine)
        scored = []
        for row in rows:
            vec   = _from_blob(row["embedding"])
            score = float(np.dot(q_vec, vec))
            scored.append((score, row))

        scored.sort(key=lambda x: x[0], reverse=True)

        results = []
        for score, row in scored[:top_k]:
            results.append({
                "doc_id":          row["doc_id"],
                "doc_type":        row["doc_type"],
                "session_mode":    row["session_mode"],
                "title":           row["title"],
                "summary_text":    row["summary_text"],
                "key_facts":       json.loads(row["key_facts"])
                                   if row["key_facts"] else {},
                "reasoning_hint":  row["reasoning_hint"],
                "objective_score": row["objective_score"],
                "profit_factor":   row["profit_factor"],
                "trade_count":     row["trade_count"],
                "stress_passed":   bool(row["stress_passed"]),
                "regime_type":     row["regime_type"] or "unknown",
                "session_pattern": row["session_pattern"],
                "similarity":      round(score, 4),
            })
        return results

    finally:
        conn.close()


# ─────────────────────────────────────────────────────────────────────
# CLI
# ─────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    import sys

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
    )

    args = sys.argv[1:]

    # 검색 테스트
    if "--search" in args:
        idx   = args.index("--search")
        query = args[idx + 1] if idx + 1 < len(args) else "LONG 신호 야간장"
        print(f"\n검색: {query}\n")
        results = search(query, top_k=5)
        for r in results:
            print(f"  [{r['doc_type']}] sim={r['similarity']:.4f}  {r['title']}")
            print(f"    {r['summary_text'][:120]}...")
            print()
        sys.exit(0)

    rebuild   = "--rebuild" in args
    only_new  = not rebuild
    n = embed_documents(only_new=only_new)
    print(f"임베딩 처리: {n}개")
