# [Directory] MMEAN
# [File] config_manager.py
from __future__ import annotations

import hashlib
import json
import sqlite3
import threading
import time
from typing import Any, Dict


# -------------------------------------------------------------------
# 기본 파라미터 정의
# -------------------------------------------------------------------
DEFAULT_CONFIG: Dict[str, Any] = {
    # ── regime_engine (smooth 기준) ──────────────────────────────
    "foreign_strong":       350.0,
    "foreign_mid":          180.0,
    "foreign_weak":          50.0,
    "oi_strong":             80.0,
    "oi_mid":                35.0,
    "oi_negative_strong":   -80.0,
    "oi_negative_mid":      -35.0,
    "basis_strong":           0.35,
    "basis_mid":              0.18,
    "basis_weak":             0.05,
    "ema_delta_strong":       0.015,
    "slope_long_ready":       0.012,
    "slope_short_ready":     -0.012,
    "volume_burst_ready":     1.60,
    "enter_score":            5.0,
    "enter_gap":              1.5,
    "enter_confirm_count":    3,
    "hold_score":             4.0,
    "hold_gap":               1.0,
    "exit_fail_count":        2,
    "entry_confirm_count":    2,
    "trade_strength_strong":  125.0,   # 체결강도 강한 기준
    "trade_strength_mid":     112.0,   # 체결강도 중간 기준

    # ── EMA 추세 점수 (trend score) ──────────────────────────────
    "trend_ema_period_fast":   20,     # 단기 EMA 기간
    "trend_ema_period_slow":   60,     # 장기 EMA 기간
    "trend_ema_slope_window":   5,     # slope 계산 틱 수
    "trend_ema_slope_threshold": 0.02, # slope 유효 기준 (절대값)
    "trend_ema_align_score":    1.5,   # EMA 정배열/역배열 기본 점수
    "trend_ema_slope_score":    0.5,   # EMA slope 보조 점수
    "trend_weight_long":        0.8,   # long 방향 추세 가중치 (0.0 = 비활성)
    "trend_weight_short":       0.6,   # short 방향 추세 가중치
    "trend_min_delta":          0.05,  # 최소 변경폭 (이하 변화는 무시)

    # ── 장전 수동 옵션 (premarket manual mode) ───────────────────
    # 허용값: MANIA_2 / MANIA_1 / NORMAL / FEAR_1 / FEAR_2
    # 기본 NORMAL = 보정 없음. 특수상황(금리충격·전쟁 등)에만 변경.
    # 방향 강제 금지 — weight / size 미세 보정만 적용.
    "premarket_manual_mode":  "NORMAL",

    # ── 장중 LLM 영향도 (intraday LLM influence level) ──────────
    # 허용값: LOW / MID / HIGH
    # LLM weight_adjust 반영 계수만 변경 (방향 결정권 없음)
    # LOW=0.5× / MID=1.0× / HIGH=1.5× (최종 clamp ±0.20 유지)
    # 기본 MID — 운영 실험 후 기본값 재조정 예정
    "llm_intraday_influence": "MID",

    # ── night_regime_engine ─────────────────────────────────────
    "night_ma_gap_strong":  0.008,   # 야간 MA 갭 강한 기준
    "night_ma_gap_weak":    0.003,   # 야간 MA 갭 약한 기준
    "night_vol_high":       1.1,     # 야간 거래량 높음 기준
    "night_vol_low":        0.6,     # 야간 거래량 낮음 기준
    "night_score_bull":     0.45,    # 야간 상승 레짐 점수
    "night_score_bear":    -0.45,    # 야간 하락 레짐 점수

    # ── sim_engine ──────────────────────────────────────────────
    # sim_engine — ENV 기본값과 동일하게 유지
    "sim_tp_ticks":           0,    # 0=TP비활성(트레일링 권장)
    "sim_sl_ticks":          10,
    "sim_trailing_ticks":    12,
    "sim_trailing_activate":  8,
    "sim_neutral_exit_ticks":20,
    "sim_profit_protect":     6,
    "sim_slippage_ticks":     1,
}


def _make_hash(cfg: Dict[str, Any]) -> str:
    """설정 딕셔너리 → SHA1 앞 8자리 hex."""
    raw = json.dumps(cfg, sort_keys=True, ensure_ascii=False)
    return hashlib.sha1(raw.encode()).hexdigest()[:8]


class ConfigManager:
    """
    엔진 파라미터 중앙 관리자.
    - get() : 현재 설정 딕셔너리 반환
    - update(patch) : 부분 업데이트 → config_hash 재계산 → DB 스냅샷
    - get_hash() : 현재 config_hash (8자)
    """

    def __init__(self, db_path: str) -> None:
        self._lock = threading.Lock()
        self._cfg: Dict[str, Any] = dict(DEFAULT_CONFIG)
        self._hash: str = _make_hash(self._cfg)
        self._db_path = db_path
        self._conn = sqlite3.connect(db_path, timeout=30, check_same_thread=False)
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA busy_timeout=30000")
        self._create_table()
        self._save_snapshot("init")

    # ------------------------------------------------------------------
    # 공개 API
    # ------------------------------------------------------------------
    def get(self) -> Dict[str, Any]:
        with self._lock:
            return dict(self._cfg)

    def get_hash(self) -> str:
        with self._lock:
            return self._hash

    def update(self, patch: Dict[str, Any]) -> Dict[str, Any]:
        """
        patch: 변경할 키-값만 담은 딕셔너리.
        알 수 없는 키는 무시. 타입은 기존 DEFAULT_CONFIG 기준으로 강제 변환.
        반환: 변경 후 전체 설정.
        """
        with self._lock:
            changed = {}
            for k, v in patch.items():
                if k not in DEFAULT_CONFIG:
                    continue
                default_type = type(DEFAULT_CONFIG[k])
                try:
                    cast_v = default_type(v)
                except (ValueError, TypeError):
                    continue
                if self._cfg[k] != cast_v:
                    changed[k] = cast_v
                    self._cfg[k] = cast_v

            if changed:
                self._hash = _make_hash(self._cfg)
                self._save_snapshot("update", changed)

            return dict(self._cfg)

    def reset_to_default(self) -> Dict[str, Any]:
        with self._lock:
            self._cfg = dict(DEFAULT_CONFIG)
            self._hash = _make_hash(self._cfg)
            self._save_snapshot("reset")
            return dict(self._cfg)

    def as_json(self) -> str:
        with self._lock:
            return json.dumps({"config": self._cfg, "hash": self._hash}, ensure_ascii=False)

    # ------------------------------------------------------------------
    # DB
    # ------------------------------------------------------------------
    def _create_table(self) -> None:
        self._conn.execute("""
            CREATE TABLE IF NOT EXISTS config_snapshots (
                id        INTEGER PRIMARY KEY AUTOINCREMENT,
                ts        TEXT    NOT NULL,
                hash      TEXT    NOT NULL,
                action    TEXT    NOT NULL,
                changed   TEXT,
                config    TEXT    NOT NULL
            )
        """)
        self._conn.commit()

    def _save_snapshot(self, action: str, changed: Dict[str, Any] | None = None) -> None:
        ts = time.strftime("%Y-%m-%d %H:%M:%S")
        self._conn.execute(
            "INSERT INTO config_snapshots (ts, hash, action, changed, config) VALUES (?,?,?,?,?)",
            (
                ts,
                self._hash,
                action,
                json.dumps(changed, ensure_ascii=False) if changed else None,
                json.dumps(self._cfg, sort_keys=True, ensure_ascii=False),
            ),
        )
        self._conn.commit()

    def get_snapshots(self, limit: int = 20) -> list:
        cur = self._conn.execute(
            "SELECT ts, hash, action, changed FROM config_snapshots ORDER BY id DESC LIMIT ?",
            (limit,),
        )
        return [{"ts": r[0], "hash": r[1], "action": r[2], "changed": r[3]} for r in cur.fetchall()]
