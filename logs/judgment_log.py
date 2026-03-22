# logs/judgment_log.py
"""
JudgmentLog — 장중 판단 사건 단위 로그

매 진입 후보마다 한 레코드:
  1. open_event()  : 진입 신호 발생 시 — 레짐/체크포인트/신호 스냅샷 저장
  2. update_llm()  : LLM 응답 후     — LLM 판단 결과 업데이트
  3. close_event() : 거래 청산 시    — 실제 결과 back-fill + llm_was_right 계산

목적:
  나중에 "LLM이 왜 틀렸는지", "어떤 체크포인트 조합에서 승률이 높은지",
  "어떤 거절이 오히려 손실 회피였는지" 를 분석할 수 있게 한다.

분석 쿼리 예시:
  -- opening_char별 LLM 정확도
  SELECT opening_char, AVG(llm_was_right), COUNT(*)
  FROM judgment_events GROUP BY opening_char;

  -- size_filter=skip이 실제로 손실을 막았나
  SELECT final_signal, AVG(pnl_pt) FROM judgment_events
  WHERE opening_char_filter = 'skip';

  -- 체크포인트 조합별 승률
  SELECT prov_regime, afternoon_regime, AVG(llm_was_right)
  FROM judgment_events GROUP BY prov_regime, afternoon_regime;

state 연동:
  state["active_judgment_event_id"] : 현재 열린 판단 사건 id (청산 시 None으로 초기화)
"""
from __future__ import annotations

import json
import logging
import sqlite3
import threading
from datetime import datetime
from typing import Any, Dict, Optional

log = logging.getLogger("MMEAN.JudgmentLog")

try:
    from contract_spec import NORMAL_TICK_PT as _TICK_SIZE   # 0.05pt
except ImportError:
    _TICK_SIZE = 0.05   # fallback


class JudgmentLog:
    """
    진입 후보 판단 사건 기록기.

    사용:
        jl = JudgmentLog(db_path)
        event_id = jl.open_event(state_snap, entry_signal, exec_mode)
        jl.update_llm(event_id, action, risk_view, confidence, weight_adj, call_key)
        jl.close_event(event_id, entry_price, exit_price, direction, exit_reason,
                       mfe_pt=0.0, mae_pt=0.0)
    """

    def __init__(self, db_path: str) -> None:
        self.db_path = db_path
        self._lock   = threading.Lock()
        self._setup_table()

    # ── 공개 API ──────────────────────────────────────────────────────────

    def open_event(
        self,
        state_snap:   Dict[str, Any],
        entry_signal: str,
        exec_mode:    str = "VIRTUAL",
    ) -> int:
        """
        진입 신호 발생 시 판단 사건 열기.
        반환: event_id (이후 update_llm / close_event에 사용)
        """
        ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        # 체크포인트 스냅샷 JSON
        oc   = state_snap.get("opening_char")
        pr   = state_snap.get("provisional_regime")
        ar   = state_snap.get("afternoon_regime")
        mv   = state_snap.get("market_view")

        # opening_char.size_filter 별도 컬럼
        oc_filter = (oc or {}).get("size_filter", "normal")
        oc_char   = (oc or {}).get("char", "")
        pr_regime = (pr or {}).get("regime", "")
        ar_regime = (ar or {}).get("regime", "")
        mv_view   = (mv or {}).get("view", "")

        row = (
            ts,
            exec_mode,
            entry_signal,
            entry_signal,                                           # final_signal (기본 = entry_signal, 게이트 후 update 가능)
            float(state_snap.get("flow_score",    0.0)),
            str(state_snap.get("bias",            "")),
            float(state_snap.get("long_score",    0.0)),
            float(state_snap.get("short_score",   0.0)),
            float(state_snap.get("volume_burst",  0.0)),
            str(state_snap.get("flow_regime_label", "")),
            oc_char,
            oc_filter,
            pr_regime,
            ar_regime,
            mv_view,
            _j(oc),
            _j(pr),
            _j(ar),
            _j(mv),
        )

        with self._lock:
            conn = self._conn()
            try:
                cur = conn.execute("""
                    INSERT INTO judgment_events (
                        ts, exec_mode,
                        entry_signal, final_signal,
                        flow_score, bias, long_score, short_score, volume_burst, regime_type,
                        opening_char, opening_char_filter,
                        prov_regime, afternoon_regime, market_view,
                        opening_char_json, prov_regime_json, afternoon_regime_json, market_view_json
                    ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                """, row)
                conn.commit()
                eid = cur.lastrowid
            finally:
                conn.close()

        log.info(
            "JudgmentLog OPEN | id=%d | %s signal=%s mode=%s | oc=%s prov=%s mv=%s",
            eid, ts, entry_signal, exec_mode, oc_char or "-", pr_regime or "-", mv_view or "-",
        )
        return eid

    def update_final_signal(self, event_id: int, final_signal: str) -> None:
        """게이트 통과 후 실제 신호 업데이트 (size_filter=skip → WAIT 등)."""
        self._update(event_id, {"final_signal": final_signal})

    def update_llm(
        self,
        event_id:     int,
        action:       str,
        risk_view:    str,
        confidence:   float,
        weight_adj:   float,
        call_key:     str = "",
    ) -> None:
        """LLM 응답 후 판단 결과 업데이트."""
        self._update(event_id, {
            "llm_action":     action,
            "llm_risk_view":  risk_view,
            "llm_confidence": confidence,
            "llm_weight_adj": weight_adj,
            "llm_call_key":   call_key,
        })
        log.debug(
            "JudgmentLog LLM | id=%d | action=%s risk=%s conf=%.3f wa=%+.4f",
            event_id, action, risk_view, confidence, weight_adj,
        )

    def close_event(
        self,
        event_id:    int,
        entry_price: float,
        exit_price:  float,
        direction:   int,       # 1=LONG, -1=SHORT
        exit_reason: str = "UNKNOWN",
        mfe_pt:      float = 0.0,
        mae_pt:      float = 0.0,
        hold_sec:    int   = 0,
    ) -> None:
        """거래 청산 시 결과 back-fill."""
        pnl_pt      = round((exit_price - entry_price) * direction, 4)
        llm_was_right = self._calc_was_right(event_id, pnl_pt)

        self._update(event_id, {
            "entry_price":    entry_price,
            "exit_price":     exit_price,
            "direction":      direction,
            "pnl_pt":         pnl_pt,
            "mfe_pt":         mfe_pt,
            "mae_pt":         mae_pt,
            "exit_reason":    exit_reason,
            "hold_sec":       hold_sec,
            "llm_was_right":  llm_was_right,
        })
        log.info(
            "JudgmentLog CLOSE | id=%d | dir=%+d entry=%.2f exit=%.2f "
            "pnl=%.2fpt reason=%s llm_right=%s",
            event_id, direction, entry_price, exit_price,
            pnl_pt, exit_reason,
            "Y" if llm_was_right else "N" if llm_was_right is not None else "?",
        )

    # ── 내부 헬퍼 ─────────────────────────────────────────────────────────

    def _calc_was_right(self, event_id: int, pnl_pt: float) -> Optional[int]:
        """
        LLM risk_view가 실제 PnL 방향과 일치했는지 판정.
        positive + pnl>0 → right / negative + pnl<0 → right / else → wrong
        """
        try:
            with self._lock:
                conn = self._conn()
                row  = conn.execute(
                    "SELECT llm_risk_view FROM judgment_events WHERE id=?", (event_id,)
                ).fetchone()
                conn.close()
            if not row or not row["llm_risk_view"]:
                return None
            rv = row["llm_risk_view"].lower()
            if rv == "positive":
                return 1 if pnl_pt > 0 else 0
            elif rv == "negative":
                return 1 if pnl_pt < 0 else 0
            elif rv == "neutral":
                return 1 if abs(pnl_pt) <= 2.0 else 0
            else:
                return None
        except Exception:
            return None

    def _update(self, event_id: int, fields: Dict[str, Any]) -> None:
        sets   = ", ".join(f"{k}=?" for k in fields)
        values = list(fields.values()) + [event_id]
        with self._lock:
            conn = self._conn()
            try:
                conn.execute(
                    f"UPDATE judgment_events SET {sets} WHERE id=?", values
                )
                conn.commit()
            finally:
                conn.close()

    def _conn(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path, timeout=30, check_same_thread=False)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA busy_timeout=30000")
        return conn

    def _setup_table(self) -> None:
        """테이블 생성은 db_setup.py에 위임. 여기선 연결 확인만."""
        try:
            conn = self._conn()
            conn.execute("SELECT COUNT(*) FROM judgment_events")
            conn.close()
        except sqlite3.OperationalError:
            log.warning(
                "JudgmentLog: judgment_events 테이블 없음 — db_setup.py 실행 필요"
            )


def _j(obj: Any) -> Optional[str]:
    """None-safe JSON 직렬화."""
    if obj is None:
        return None
    try:
        return json.dumps(obj, ensure_ascii=False)
    except Exception:
        return None
