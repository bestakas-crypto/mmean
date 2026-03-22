# MMEAN/regime_recorder.py
from __future__ import annotations

import json
import sqlite3
import threading
from datetime import datetime
from typing import Dict


class RegimeRecorder:
    def __init__(self, db_path: str) -> None:
        self.conn = sqlite3.connect(db_path, timeout=30, check_same_thread=False)
        self.conn.execute("PRAGMA journal_mode=WAL")
        self.conn.execute("PRAGMA synchronous=NORMAL")
        self.conn.execute("PRAGMA busy_timeout=30000")
        self.lock = threading.Lock()
        self.active_regime_id = None
        self.active_regime_bias = None
        self._restore_open_regime()

    def _restore_open_regime(self) -> None:
        with self.lock:
            cur = self.conn.cursor()
            cur.execute("SELECT id, bias FROM regime_events WHERE status = 'OPEN' ORDER BY id DESC LIMIT 1")
            row = cur.fetchone()
            if row:
                self.active_regime_id = row[0]
                self.active_regime_bias = row[1]

    @staticmethod
    def _to_epoch(ts_text: str) -> float:
        try:
            fmt = "%Y-%m-%d %H:%M:%S.%f" if "." in ts_text else "%Y-%m-%d %H:%M:%S"
            return datetime.strptime(ts_text, fmt).timestamp()
        except Exception:
            return 0.0

    @staticmethod
    def _f(v, default=None):
        """float 변환 — None/missing 허용."""
        try:
            return float(v) if v is not None else default
        except (TypeError, ValueError):
            return default

    @staticmethod
    def _i(v, default=None):
        """int 변환 — None/missing 허용."""
        try:
            return int(v) if v is not None else default
        except (TypeError, ValueError):
            return default

    def insert_tick(self, ts_text: str, snap, state_dict: Dict[str, object]) -> None:
        s = state_dict
        f = self._f
        i = self._i
        with self.lock:
            cur = self.conn.cursor()
            cur.execute("""
                INSERT INTO regime_ticks (
                    ts, bias, entry_signal, confidence,
                    futures_price, spot_price, basis, basis_ema, basis_ema_delta, basis_slope,
                    volume_burst, foreign_buy, oi_value, oi_delta,
                    trade_qty, cum_volume, trade_strength, best_ask, best_bid,
                    long_score, short_score, reason,
                    trend_long_score, trend_short_score, trend_weight_long, trend_weight_short,
                    ema_fast, ema_slow, ema_fast_slope,
                    param_hash,
                    vwap, atr, price_vs_vwap,
                    foreign_signal_mode,
                    flow_score, flow_long_weight, flow_short_weight,
                    flow_llm_weight_adjust, flow_order_size,
                    llm_filter_score, llm_filter_direction, llm_filter_valid,
                    llm_filter_ts, llm_call_id,
                    level_no, mode_type, session_phase,
                    night_regime, night_entry, night_confidence, night_raw_score,
                    today_trade_count, daily_loss_limit_left
                ) VALUES (
                    ?,?,?,?,  ?,?,?,?,?,?,  ?,?,?,?,  ?,?,?,?,?,  ?,?,?,
                    ?,?,?,?,  ?,?,?,  ?,
                    ?,?,?,  ?,  ?,?,?,  ?,?,  ?,?,?,  ?,?,  ?,?,?,  ?,?,?,?,  ?,?
                )
            """, (
                # ── 기존 30개 ──────────────────────────────────────────────
                ts_text, snap.bias, snap.entry_signal, float(snap.confidence),
                float(s["futures_price"]), float(s["spot_price"]),
                float(s["basis"]), float(s["basis_ema"]),
                float(s["basis_ema_delta"]), float(s["basis_slope"]),
                float(s["volume_burst"]), float(s["foreign_buy"]),
                float(s["oi_value"]), float(s["oi_delta"]),
                float(s["trade_qty"]), float(s["cum_volume"]),
                float(s["last_trade_strength"]), float(s["best_ask"]),
                float(s["best_bid"]), float(snap.long_score),
                float(snap.short_score), str(snap.reason),
                float(snap.trend_long_score), float(snap.trend_short_score),
                float(snap.trend_weight_long), float(snap.trend_weight_short),
                float(snap.ema_fast), float(snap.ema_slow), float(snap.ema_fast_slope),
                str(s.get("param_hash", "")),
                # ── 신규 23개: 가격 지표 ───────────────────────────────────
                f(s.get("vwap")),
                f(s.get("atr")),
                f(s.get("price_vs_vwap")),
                # ── 수급 모드 ──────────────────────────────────────────────
                str(s.get("foreign_signal_mode") or ""),
                # ── Flow 엔진 출력 ─────────────────────────────────────────
                f(s.get("flow_score")),
                f(s.get("flow_long_weight")),
                f(s.get("flow_short_weight")),
                f(s.get("flow_llm_weight_adjust")),
                f(s.get("flow_order_size")),
                # ── LLM 필터 시그널 ────────────────────────────────────────
                f(s.get("llm_filter_score")),
                str(s.get("llm_filter_direction") or ""),
                i(s.get("llm_filter_valid"), 0),
                s.get("llm_filter_ts"),
                i(s.get("llm_call_id")),
                # ── 레벨·모드·세션 ─────────────────────────────────────────
                i(s.get("selected_level") if s.get("selected_level") is not None
                  else s.get("level_no")),
                str(s.get("mode_type") or "expert"),
                str(s.get("session_phase") or ""),
                # ── 야간 레짐 ──────────────────────────────────────────────
                str(s.get("night_regime") or ""),
                str(s.get("night_entry") or ""),
                f(s.get("night_confidence")),
                f(s.get("night_raw_score")),
                # ── 일별 운영 상태 ─────────────────────────────────────────
                i(s.get("today_trade_count")),
                f(s.get("daily_loss_limit_left")),
            ))

    def update_regime_event(self, ts_text: str, snap, state_dict: Dict[str, object]) -> None:
        bias = snap.bias
        with self.lock:
            cur = self.conn.cursor()
            if bias == "NEUTRAL":
                if self.active_regime_id is not None:
                    cur.execute("SELECT start_ts FROM regime_events WHERE id = ?", (self.active_regime_id,))
                    row = cur.fetchone()
                    if row:
                        duration_sec = max(0.0, self._to_epoch(ts_text) - self._to_epoch(row[0]))
                        cur.execute("""
                            UPDATE regime_events
                            SET end_ts=?, duration_sec=?, end_price=?, status='CLOSED'
                            WHERE id=?
                        """, (ts_text, duration_sec, float(state_dict["futures_price"]), self.active_regime_id))
                    self.active_regime_id, self.active_regime_bias = None, None
                return

            if self.active_regime_id is None or self.active_regime_bias != bias:
                if self.active_regime_id is not None:
                    cur.execute("SELECT start_ts FROM regime_events WHERE id = ?", (self.active_regime_id,))
                    row = cur.fetchone()
                    if row:
                        duration_sec = max(0.0, self._to_epoch(ts_text) - self._to_epoch(row[0]))
                        cur.execute("""
                            UPDATE regime_events
                            SET end_ts=?, duration_sec=?, end_price=?, status='CLOSED'
                            WHERE id=?
                        """, (ts_text, duration_sec, float(state_dict["futures_price"]), self.active_regime_id))

                cur.execute("""
                    INSERT INTO regime_events (
                        bias, start_ts, start_price,
                        max_confidence, min_basis, max_basis, tick_count, status
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, 'OPEN')
                """, (
                    bias, ts_text, float(state_dict["futures_price"]), float(snap.confidence),
                    float(state_dict["basis"]), float(state_dict["basis"]), 1
                ))
                self.active_regime_id, self.active_regime_bias = cur.lastrowid, bias
                return

            cur.execute("""
                UPDATE regime_events SET
                    max_confidence = CASE WHEN max_confidence < ? THEN ? ELSE max_confidence END,
                    min_basis = CASE WHEN min_basis > ? THEN ? ELSE min_basis END,
                    max_basis = CASE WHEN max_basis < ? THEN ? ELSE max_basis END,
                    tick_count = tick_count + 1
                WHERE id = ?
            """, (
                float(snap.confidence), float(snap.confidence),
                float(state_dict["basis"]), float(state_dict["basis"]),
                float(state_dict["basis"]), float(state_dict["basis"]),
                self.active_regime_id
            ))

    def insert_trade(self, trade: dict) -> None:
        def _j(v):
            return json.dumps(v, ensure_ascii=False) if isinstance(v, dict) else v

        with self.lock:
            self.conn.execute("""
                INSERT INTO trades (
                    session_id, open_ts, close_ts, direction,
                    entry_price, exit_price, pnl_ticks, exit_reason,
                    entry_bias, entry_long_score, entry_short_score, entry_confidence,
                    entry_basis, entry_slope, entry_vol_burst, entry_params, entry_reason,
                    exit_bias, exit_long_score, exit_short_score, exit_confidence,
                    exit_params, exit_reason_detail, config_hash,
                    max_favorable_pt, max_adverse_excursion, hold_ticks, cum_equity,
                    entry_llm_call_id, entry_llm_score, entry_llm_direction,
                    entry_trend_weight_long, entry_trend_weight_short,
                    entry_trend_long_score, entry_trend_short_score,
                    param_store_hash, param_store_version,
                    param_hash,
                    foreign_signal_mode,
                    level_no,
                    simulation_mode
                ) VALUES (?,?,?,?, ?,?,?,?, ?,?,?,?, ?,?,?,?,?, ?,?,?,?, ?,?,?, ?,?,?,?, ?,?,?, ?,?,?,?, ?,?,?, ?,?,?)
            """, (
                trade.get("session_id", ""),
                trade.get("open_ts", ""),
                trade.get("close_ts"),
                int(trade.get("direction", 0)),
                float(trade.get("entry_price", 0)),
                float(trade.get("exit_price") or 0) or None,
                float(trade.get("pnl_ticks") or 0) if trade.get("pnl_ticks") is not None else None,
                trade.get("exit_reason"),
                trade.get("entry_bias"),
                float(trade.get("entry_long_score") or 0),
                float(trade.get("entry_short_score") or 0),
                float(trade.get("entry_confidence") or 0),
                float(trade.get("entry_basis") or 0),
                float(trade.get("entry_slope") or 0),
                float(trade.get("entry_vol_burst") or 0),
                _j(trade.get("entry_params")),
                trade.get("entry_reason"),
                trade.get("exit_bias"),
                float(trade.get("exit_long_score") or 0),
                float(trade.get("exit_short_score") or 0),
                float(trade.get("exit_confidence") or 0),
                _j(trade.get("exit_params")),
                trade.get("exit_reason_detail"),
                trade.get("config_hash", ""),
                float(trade.get("max_favorable_pt") or 0),
                float(trade.get("max_adverse_excursion") or 0),
                int(trade.get("hold_ticks") or 0),
                float(trade.get("cum_equity") or 0),
                int(trade.get("entry_llm_call_id") or 0),
                float(trade.get("entry_llm_score") if trade.get("entry_llm_score") is not None else -1.0),
                str(trade.get("entry_llm_direction") or "UNKNOWN"),
                float(trade.get("entry_trend_weight_long") or 0),
                float(trade.get("entry_trend_weight_short") or 0),
                float(trade.get("entry_trend_long_score") or 0),
                float(trade.get("entry_trend_short_score") or 0),
                str(trade.get("param_store_hash") or ""),
                int(trade.get("param_store_version") or 0),
                str(trade.get("param_hash") or ""),
                str(trade.get("foreign_signal_mode") or "composite"),
                trade.get("level_no"),                              # NULL 허용
                str(trade.get("simulation_mode") or "expert"),
            ))
            self.conn.commit()

    def flush_tick_batch(self) -> None:
        with self.lock:
            self.conn.commit()

    def insert_entry_event(self, ts_text: str, snap, state_dict: Dict[str, object]) -> None:
        if snap.entry_signal not in ("LONG_READY", "SHORT_READY"):
            return
        with self.lock:
            cur = self.conn.cursor()
            cur.execute("""
                INSERT INTO entry_events (
                    ts, bias, entry_signal, futures_price,
                    basis, basis_slope, basis_ema_delta, volume_burst,
                    confidence, long_score, short_score, reason
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                ts_text, snap.bias, snap.entry_signal, float(state_dict["futures_price"]),
                float(state_dict["basis"]), float(state_dict["basis_slope"]),
                float(state_dict["basis_ema_delta"]), float(state_dict["volume_burst"]),
                float(snap.confidence), float(snap.long_score), float(snap.short_score), str(snap.reason)
            ))
