# [Directory] MME
# [File] llm_filter.py
from __future__ import annotations

import asyncio
import json
import logging
import os
import sqlite3
import threading
import time
from datetime import datetime, timedelta
from typing import Any, Callable, Dict, List, Optional

from llm_chain import LLMChain
from llm_schemas import OpportunitySchema
from prompt_manager import PromptManager

try:
    from param_store import get_param_store
    _PARAM_STORE_AVAILABLE = True
except ImportError:
    get_param_store = None
    _PARAM_STORE_AVAILABLE = False

log = logging.getLogger("MMEAN.LLMFilter")


class LLMOpportunityFilter:
    """
    Layer 2 — 장중 기회 필터 (운영 규칙서 v1 §6 준수)

    역할:
      - 시장 상태 변수(EMA, VWAP, ATR, ADX, foreign flow, OI, basis)만을
        근거로 진입 기회의 질을 평가
      - opportunity_score / direction_bias / tp/sl override 추천
      - EMA 추세 파라미터(trend_weight 등)를 ParamStore에 runtime patch로 반영
      - 최신 유효 응답만 캐시 / stale TTL 지난 값은 무효화
      - gatekeeper only 원칙 유지

    [§6 입력 원칙]
    포함: 시장 상태 변수만 (EMA, VWAP, ATR, ADX, foreign flow, OI, bias)
    제외: 손익(today_pnl_won), 손실한도(daily_loss_limit_left),
          거래횟수(today_trade_count), 운영 결과 일체
    이유: 장중 tuning은 시장 구조에만 반응해야 하며,
          운영 결과 변수는 간접 손익 적응 통로가 됨 (§6 위반)

    [출력 원칙]
    LLM은 reason_code(ALLOWED_REASON_CODES 기준), source, patch를 반환해야 함.
    허용되지 않은 reason_code는 ParamStore에서 기각됨.
    """

    def __init__(
        self,
        db_path: str,
        state_getter: Callable[[], Dict[str, Any]],
        llm_chain: Optional[LLMChain] = None,
        interval_sec: Optional[int] = None,
        fail_mode: Optional[str] = None,
        enabled: Optional[bool] = None,
        prompt_mgr: Optional[PromptManager] = None,
    ) -> None:
        self.db_path = db_path
        self.state_getter = state_getter
        self.llm_chain = llm_chain or LLMChain(
            timeout_sec=float(os.getenv("LLM_TIMEOUT_SEC", "3")),
            connect_timeout_sec=float(os.getenv("LLM_CONNECT_TIMEOUT_SEC", "1")),
        )
        self.prompt_mgr = prompt_mgr or PromptManager(db_path=self.db_path)

        self.enabled = (
            enabled if enabled is not None
            else os.getenv("LLM_FILTER_ENABLED", "0") == "1"
        )
        self.interval_sec = int(interval_sec or os.getenv("LLM_FILTER_INTERVAL_SEC", "300"))
        self.fail_mode = (fail_mode or os.getenv("LLM_FILTER_FAIL_MODE", "closed")).strip().lower()
        self.opportunity_threshold = float(os.getenv("LLM_OPPORTUNITY_THRESHOLD", "0.60"))

        self._lock = threading.Lock()
        self._stop_event = threading.Event()
        self._thread: Optional[threading.Thread] = None

        self._latest: Optional[Dict[str, Any]] = None
        self._latest_meta: Dict[str, Any] = {}
        self._last_run_ts: float = 0.0

        self._conn = sqlite3.connect(self.db_path, timeout=30, check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA busy_timeout=30000")
        self._setup_tables()

        # ParamStore 연결 (EMA 추세 파라미터 동적 업데이트)
        self._param_store = None
        if _PARAM_STORE_AVAILABLE:
            try:
                self._param_store = get_param_store(self.db_path)
            except Exception as _e:
                log.warning("LLMFilter: ParamStore 연결 실패 (무시): %s", _e)

    # ------------------------------------------------------------------
    # public
    # ------------------------------------------------------------------
    def start(self) -> None:
        if not self.enabled:
            log.info("LLMOpportunityFilter 비활성 상태입니다.")
            return

        if self._thread and self._thread.is_alive():
            return

        self._stop_event.clear()
        self._thread = threading.Thread(
            target=self._run_loop,
            name="MMEAN-LLMFilter",
            daemon=True,
        )
        self._thread.start()
        log.info(
            "LLMOpportunityFilter 시작 | interval=%ss | fail_mode=%s",
            self.interval_sec,
            self.fail_mode,
        )

    def stop(self) -> None:
        self._stop_event.set()
        if self._thread and self._thread.is_alive():
            self._thread.join(timeout=2.0)
        log.info("LLMOpportunityFilter 중지")

    def run_once_now(self) -> Dict[str, Any]:
        try:
            return asyncio.run(self._run_once())
        except RuntimeError:
            loop = asyncio.new_event_loop()
            try:
                return loop.run_until_complete(self._run_once())
            finally:
                loop.close()

    def get_latest(self) -> Optional[Dict[str, Any]]:
        with self._lock:
            return dict(self._latest) if self._latest else None

    def get_latest_meta(self) -> Dict[str, Any]:
        with self._lock:
            return dict(self._latest_meta)

    def get_latest_valid(self) -> Optional[Dict[str, Any]]:
        with self._lock:
            if not self._latest:
                return None if self.fail_mode == "open" else self._closed_fallback()

            expires_at = self._latest_meta.get("expires_at_ts", 0.0)
            now_ts = time.time()
            if expires_at and now_ts > expires_at:
                self._latest_meta["stale"] = True
                return None if self.fail_mode == "open" else self._closed_fallback()

            return dict(self._latest)

    def should_allow_entry(self, bias: str, entry_signal: str) -> tuple[bool, Dict[str, Any]]:
        opp = self.get_latest_valid()
        if opp is None:
            if self.fail_mode == "open":
                return True, {}
            return False, self._closed_fallback()

        score = float(opp.get("opportunity_score", 0.0))
        dir_bias = str(opp.get("direction_bias", "NEUTRAL")).upper()

        if score < self.opportunity_threshold:
            return False, opp

        if entry_signal == "LONG_READY" and bias == "LONG_BIAS":
            return dir_bias in ("LONG", "NEUTRAL"), opp

        if entry_signal == "SHORT_READY" and bias == "SHORT_BIAS":
            return dir_bias in ("SHORT", "NEUTRAL"), opp

        return False, opp

    def close(self) -> None:
        self.stop()
        try:
            self._conn.close()
        except Exception:
            pass
        try:
            self.llm_chain.close()
        except Exception:
            pass
        try:
            self.prompt_mgr.close()
        except Exception:
            pass

    # ------------------------------------------------------------------
    # main loop
    # ------------------------------------------------------------------
    def _run_loop(self) -> None:
        while not self._stop_event.is_set():
            try:
                self.run_once_now()
            except Exception as e:
                log.exception("LLMOpportunityFilter loop 오류: %s", e)
            self._stop_event.wait(self.interval_sec)

    async def _run_once(self) -> Dict[str, Any]:
        state = self._safe_state_snapshot()
        prompt = self._build_prompt(state)
        system_prompt = self.prompt_mgr.get_combined_prompt("opportunity")
        prompt_meta = self.prompt_mgr.get_combined_meta("opportunity")

        started = datetime.now()
        session_date = started.strftime("%Y-%m-%d")

        # LLM 호출
        result = await self.llm_chain.query(
            prompt=prompt,
            system_prompt=system_prompt,
            schema_class=OpportunitySchema,
            layer="opportunity",
        )

        meta     = result.pop("_meta",     {}) if isinstance(result, dict) else {}
        attempts = result.pop("_attempts", []) if isinstance(result, dict) else []

        # 파싱/응답 실패 처리
        if not isinstance(result, dict) or meta.get("status") == "error":
            raw_text = str(result)[:500] if result else ""
            if self._param_store:
                self._param_store.record_parse_fail(
                    provider=str(meta.get("provider", "unknown")),
                    layer="opportunity",
                    raw_excerpt=raw_text,
                )
            log.warning(
                "LLMFilter 응답 파싱 실패 | provider=%s | excerpt=%s",
                meta.get("provider"), raw_text[:100],
            )
            return {"ok": False, "error": "parse_fail", "meta": meta}

        validated = OpportunitySchema.validate_and_clamp(result)

        created_at = datetime.now()
        expires_at = created_at + __import__("datetime").timedelta(
            minutes=int(validated.get("valid_minutes", 5))
        )

        with self._lock:
            self._latest = dict(validated)
            self._latest_meta = {
                "provider":       meta.get("provider", "unknown"),
                "status":         meta.get("status", "unknown"),
                "latency_ms":     int(meta.get("latency_ms", 0) or 0),
                "created_at":     created_at.strftime("%Y-%m-%d %H:%M:%S"),
                "expires_at":     expires_at.strftime("%Y-%m-%d %H:%M:%S"),
                "created_at_ts":  created_at.timestamp(),
                "expires_at_ts":  expires_at.timestamp(),
                "stale":          False,
                "fallback_depth": int(meta.get("fallback_depth", 0) or 0),
                "attempt_count":  int(meta.get("attempt_count", 0) or 0),
                "prompt_name":    "opportunity",
                "prompt_hash":    str(prompt_meta.get("hash", "")),
                "prompt_source":  str(prompt_meta.get("source", "")),
            }
            self._last_run_ts = time.time()

        # ParamStore — EMA 추세 파라미터 패치 적용
        # 우선순위: validated["patch"] (LLM이 명시한 값) > validated에서 직접 추출
        # source="llm_filter": 장중 기회 필터 경로
        # reason_code: OpportunitySchema.VALID_REASON_CODES 통과한 값만 사용
        if self._param_store:
            _allowed_patch_keys = {
                "trend_weight_long", "trend_weight_short",
                "trend_ema_align_score", "trend_ema_slope_score",
                "trend_ema_slope_threshold",
            }
            # 1) validated["patch"]에 명시된 값 우선
            schema_patch = {
                k: v for k, v in validated.get("patch", {}).items()
                if k in _allowed_patch_keys
            }
            # 2) patch가 비어있으면 validated에서 직접 키 추출 (하위 호환)
            if not schema_patch:
                schema_patch = {
                    k: validated[k]
                    for k in _allowed_patch_keys
                    if k in validated
                }
            if schema_patch:
                reason_code = str(validated.get("reason_code", "regime_shift")).strip() or "regime_shift"
                self._param_store.apply_patch(
                    patch=schema_patch,
                    source="llm_filter",
                    provider=str(meta.get("provider", "unknown")),
                    raw_excerpt=str(validated)[:500],
                    layer="opportunity",
                    reason_code=reason_code,
                )

        _call_id = self._insert_llm_call(
            ts=started.strftime("%Y-%m-%d %H:%M:%S"),
            layer="opportunity",
            provider=str(meta.get("provider", "unknown")),
            prompt_json=json.dumps(
                {
                    "prompt_name":  "opportunity",
                    "prompt_hash":  prompt_meta.get("hash", ""),
                    "prompt_meta":  prompt_meta,
                    "state":        state,
                    "prompt":       prompt,
                    "system_prompt":system_prompt,
                },
                ensure_ascii=False,
            ),
            response_json=json.dumps(validated, ensure_ascii=False),
            latency_ms=int(meta.get("latency_ms", 0) or 0),
            applied=1,
            validation_ok=1,
            session_date=session_date,
            baseline_hash="",
            apply_mode="live" if self.enabled else "disabled",
            error_type="" if meta.get("status") == "success" else "fallback",
            fallback_depth=int(meta.get("fallback_depth", 0) or 0),
            attempts_json=json.dumps(attempts, ensure_ascii=False),
            expires_at=expires_at.strftime("%Y-%m-%d %H:%M:%S"),
            opportunity_score=float(validated.get("opportunity_score", -1.0)),
            direction=str(validated.get("direction_bias", "") or ""),
            prompt_name="opportunity",
            prompt_hash=str(prompt_meta.get("hash", "")),
            prompt_source=str(prompt_meta.get("source", "")),
        )

        with self._lock:
            self._latest_meta["llm_call_id"] = _call_id

        log.info(
            "LLM 필터 갱신 | score=%.2f | dir=%s | valid=%dm | provider=%s | prompt_hash=%s",
            float(validated.get("opportunity_score", 0.0)),
            validated.get("direction_bias", "NEUTRAL"),
            int(validated.get("valid_minutes", 0)),
            meta.get("provider", "unknown"),
            prompt_meta.get("hash", ""),
        )

        return {
            "ok":           True,
            "validated":    validated,
            "meta":         dict(self._latest_meta),
            "attempts":     attempts,
            "prompt_meta":  prompt_meta,
        }

    # ------------------------------------------------------------------
    # prompt body
    # ------------------------------------------------------------------
    def _build_prompt(self, state: Dict[str, Any]) -> str:
        """
        장중 파라미터 튜닝 프롬프트.

        [입력 원칙 — §6 준수]
        포함: 시장 상태 변수만 (EMA, VWAP, ATR, ADX, foreign flow, OI, bias)
        제외: 손익(today_pnl_won), 손실한도(daily_loss_limit_left),
              거래횟수(today_trade_count), 운영 결과 일체
        이유: 장중 tuning은 시장 구조에만 반응해야 하며,
              운영 결과 변수는 간접 손익 적응 통로가 된다.
        """
        ticks = state.get("recent_ticks", []) or []
        lines: List[str] = []

        lines.append("=== 장중 스냅샷 (최근 10틱) ===")
        lines.append("시각 | 가격 | Basis | L점수 | S점수 | VolBurst | Bias")
        for row in ticks[:10]:
            lines.append(
                f"{row.get('ts','-')} | "
                f"{float(row.get('futures_price', 0.0)):.2f} | "
                f"{float(row.get('basis', 0.0)):+.4f} | "
                f"{float(row.get('long_score', 0.0)):.2f} | "
                f"{float(row.get('short_score', 0.0)):.2f} | "
                f"{float(row.get('volume_burst', 0.0)):.2f}x | "
                f"{row.get('bias', 'NEUTRAL')}"
            )

        lines.append("")
        lines.append("=== 현재 시장 상태 ===")
        lines.append(
            f"현재 Bias: {state.get('bias','NEUTRAL')} | "
            f"Entry Signal: {state.get('entry_signal','WAIT')} | "
            f"신뢰도: {float(state.get('confidence', 0.0)):.1f}%"
        )
        lines.append(
            f"[핵심] Foreign Buy: {float(state.get('foreign_buy', 0.0)):+.1f} | "
            f"OI Delta: {float(state.get('oi_delta', 0.0)):+.1f} | "
            f"OI Value: {float(state.get('oi_value', 0.0)):+.1f}"
        )
        lines.append(
            f"Basis: {float(state.get('basis', 0.0)):+.4f} | "
            f"Basis EMA: {float(state.get('basis_ema', 0.0)):+.4f} | "
            f"Slope: {float(state.get('basis_slope', 0.0)):+.5f}"
        )
        lines.append(
            f"Volume Burst: {float(state.get('volume_burst', 0.0)):.2f}x | "
            f"Trade Strength: {float(state.get('trade_strength', 0.0)):.1f}"
        )

        # EMA 추세 정보
        ema_fast  = float(state.get("ema_fast", 0.0))
        ema_slow  = float(state.get("ema_slow", 0.0))
        ema_slope = float(state.get("ema_fast_slope", 0.0))
        ema_align = (
            "정배열(상승)" if ema_fast > ema_slow
            else ("역배열(하락)" if ema_fast < ema_slow else "중립")
        )
        lines.append(
            f"EMA20: {ema_fast:.2f} | EMA60: {ema_slow:.2f} | "
            f"정렬: {ema_align} | EMA slope: {ema_slope:+.4f}"
        )
        lines.append(
            f"추세점수 L:{float(state.get('trend_long_score', 0.0)):.2f} "
            f"S:{float(state.get('trend_short_score', 0.0)):.2f} | "
            f"TW long={float(state.get('trend_weight_long', 0.8)):.2f} "
            f"short={float(state.get('trend_weight_short', 0.6)):.2f}"
        )

        # VWAP / ATR 맥락
        vwap          = float(state.get("vwap", 0.0))
        price_vs_vwap = float(state.get("price_vs_vwap", 0.0))
        atr           = float(state.get("atr", 0.0))
        vwap_pos = "VWAP 위(매수우위)" if price_vs_vwap > 0 else "VWAP 아래(매도우위)"
        lines.append(
            f"VWAP: {vwap:.2f} | 현재가 vs VWAP: {price_vs_vwap:+.2f} ({vwap_pos}) | "
            f"ATR: {atr:.4f}"
        )

        # ── 손익/거래횟수/운영결과는 의도적으로 제외 ──────────────────
        # today_pnl_won, daily_loss_limit_left, today_trade_count 제거.
        # 이 값들은 risk layer / 분석 리포트 전용이며,
        # 장중 LLM tuning 입력에 포함하면 §6(손익 기반 변경 금지) 위반임.

        lines.append("")
        lines.append("=== 요청 ===")
        lines.append(
            "현재 시장 구조(EMA 정렬·slope, VWAP 위치, ATR 변동성, "
            "foreign flow 방향, OI 변화, basis 흐름, volume burst)만을 근거로 "
            "진입 기회의 질을 평가하고 파라미터 조정 여부를 판단하라. "
            "손익·거래횟수·운영 결과는 판단 근거로 사용하지 않는다. "
            "수급·추세·변동성이 같은 방향으로 동조할수록 점수를 높이고, "
            "방향이 충돌하거나 근거가 불명확하면 현재 파라미터를 유지하라."
        )

        return "\n".join(lines)

    # ------------------------------------------------------------------
    # fallback
    # ------------------------------------------------------------------
    def _closed_fallback(self) -> Dict[str, Any]:
        return OpportunitySchema.validate_and_clamp(OpportunitySchema.default())

    # ------------------------------------------------------------------
    # state
    # ------------------------------------------------------------------
    def _safe_state_snapshot(self) -> Dict[str, Any]:
        try:
            st = self.state_getter() or {}
            if not isinstance(st, dict):
                return {}
            return dict(st)
        except Exception as e:
            log.warning("state_getter 실패: %s", e)
            return {}

    # ------------------------------------------------------------------
    # db
    # ------------------------------------------------------------------
    def _setup_tables(self) -> None:
        for col, dtype in [
            ("opportunity_score", "REAL"),
            ("direction", "TEXT"),
            ("expires_at", "TEXT"),
            ("prompt_name", "TEXT"),
            ("prompt_hash", "TEXT"),
            ("prompt_source", "TEXT"),
        ]:
            try:
                self._conn.execute(f"ALTER TABLE llm_calls ADD COLUMN {col} {dtype}")
            except Exception:
                pass
        self._conn.commit()

    def _insert_llm_call(
        self,
        ts: str,
        layer: str,
        provider: str,
        prompt_json: str,
        response_json: str,
        latency_ms: int,
        applied: int,
        validation_ok: int,
        session_date: str,
        baseline_hash: str = "",
        apply_mode: str = "live",
        error_type: str = "",
        fallback_depth: int = 0,
        attempts_json: str = "",
        expires_at: str = "",
        opportunity_score: float = -1.0,
        direction: str = "",
        prompt_name: str = "",
        prompt_hash: str = "",
        prompt_source: str = "",
    ) -> int:
        cur = self._conn.execute(
            """
            INSERT INTO llm_calls (
                ts, layer, provider, prompt_json, response_json, latency_ms,
                applied, validation_ok, session_date, baseline_hash,
                apply_mode, error_type, fallback_depth, attempts_json,
                expires_at, opportunity_score, direction,
                prompt_name, prompt_hash, prompt_source
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                ts,
                layer,
                provider,
                prompt_json,
                response_json,
                latency_ms,
                applied,
                validation_ok,
                session_date,
                baseline_hash,
                apply_mode,
                error_type,
                fallback_depth,
                attempts_json,
                expires_at,
                opportunity_score if opportunity_score >= 0 else None,
                direction or None,
                prompt_name or None,
                prompt_hash or None,
                prompt_source or None,
            ),
        )
        self._conn.commit()
        return cur.lastrowid or 0