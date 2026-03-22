# [Directory] MMEAN
# [File] sim_engine.py
"""
SimEngine v3 — 트레일링 스탑 + 청산 트리거 조정

선물 스펙:
  호가단위  0.05pt  |  1틱 = 12,500원  |  1pt = 250,000원

트레일링 스탑 동작:
  1) 진입 직후: 초기 SL 고정 (sl_ticks 기준)
  2) 가격이 trailing_activate_ticks 이상 유리하게 이동하면 → 트레일링 시작
  3) 트레일링 활성 후: 최고가(LONG)/최저가(SHORT) 기준 trailing_ticks 추적
  4) tp_ticks=0 이면 TP 비활성 (트레일링만으로 청산)

청산 트리거 — 트레일링 활성/비활성에 따라 민감도 조정:
  ┌─────────────────────┬──────────────────┬──────────────────────┐
  │ 트리거               │ 트레일링 비활성   │ 트레일링 활성         │
  ├─────────────────────┼──────────────────┼──────────────────────┤
  │ T0  TAKE_PROFIT     │ tp_price 도달     │ tp_ticks>0 시만 동작  │
  │ T1  STOP_LOSS       │ 초기 sl 고정      │ 트레일링 sl로 교체    │
  │ T2  FORCE_EXIT_EOD  │ 항상 동작         │ 항상 동작             │
  │ T3  BIAS_REVERSAL   │ 항상 동작         │ 항상 동작             │
  │ T4  SIGNAL_CONFLICT │ 즉시 청산         │ 무시 (추세 중 노이즈)  │
  │ T5  NEUTRAL_TIMEOUT │ neutral_exit_ticks│ neutral_exit_ticks×3  │
  └─────────────────────┴──────────────────┴──────────────────────┘

.env 파라미터:
  MMEAN_SIM_ENABLED              = 1
  MMEAN_SIM_TP_TICKS             = 0    # 0이면 TP 비활성 (트레일링 권장 시)
  MMEAN_SIM_SL_TICKS             = 10   # 초기 손절 틱
  MMEAN_SIM_TRAILING_TICKS       = 12   # 트레일링 추적 거리 틱 (0이면 트레일링 비활성)
  MMEAN_SIM_TRAILING_ACTIVATE    = 8    # 트레일링 시작 조건: 진입가 대비 N틱 유리 시
  MMEAN_SIM_NEUTRAL_EXIT_TICKS   = 3    # NEUTRAL 청산 틱 (트레일링 활성 시 ×3 적용)
  MMEAN_SIM_SLIPPAGE_TICKS       = 1
  MMEAN_SIM_FORCE_EXIT_HOUR      = 14
  MMEAN_SIM_FORCE_EXIT_MIN       = 45
"""
from __future__ import annotations

import logging
import sqlite3
import threading
from dataclasses import dataclass
from datetime import datetime
from typing import Optional

log = logging.getLogger("MMEAN.sim")

# ─── 선물 스펙 (변경 금지) ────────────────────────────────────────
TICK_UNIT   = 0.05
TICK_VALUE  = 12_500
POINT_VALUE = 250_000


@dataclass
class SimPosition:
    direction:    int
    entry_price:  float
    entry_time:   str
    entry_bias:   str
    tp_price:     float
    sl_price:     float
    initial_sl:   float
    trailing_active:  bool  = False
    extreme_price:    float = 0.0
    neutral_ticks:    int   = 0
    # 진입 조건 스냅샷 (trades 테이블 기록용)
    entry_long_score:  float = 0.0
    entry_short_score: float = 0.0
    entry_confidence:  float = 0.0
    entry_basis:       float = 0.0
    entry_slope:       float = 0.0
    entry_vol_burst:   float = 0.0
    entry_params:      str   = ""
    entry_reason:      str   = ""
    config_hash:       str   = ""
    # LLM 연동 필드 (llm_filter — 기존)
    entry_llm_call_id:  int   = 0
    entry_llm_score:    float = -1.0
    entry_llm_direction: str  = "UNKNOWN"
    # FlowEngine/LLMDecisionLog 연동 필드 (신규)
    entry_flow_order_size:  float = 1.0   # PositionEngine 최종 주문 사이즈
    entry_flow_llm_log_id:  int   = 0     # LLMDecisionLog.record() 반환 id (0=LLM 미사용)
    # 시뮬레이션 모드 기록 (easy=레벨번호, expert=None)
    level_no:             Optional[int] = None   # 이지모드 선택 레벨 (1~20)
    simulation_mode:      str           = "expert"  # "easy" | "expert"
    # ParamStore 추세 파라미터 스냅샷 (거래 분석 fingerprint)
    param_hash:           str   = ""   # 순수 파라미터 hash (AB 비교 핵심)
    param_store_hash:     str   = ""   # snapshot_hash (전체 상태)
    param_store_version:  int   = 0
    entry_trend_weight_long:  float = 0.0
    entry_trend_weight_short: float = 0.0
    entry_trend_long_score:   float = 0.0
    entry_trend_short_score:  float = 0.0
    # 외국인 신호 모드 (composite / delta_only)
    foreign_signal_mode:  str   = "composite"
    # RAG 사전작업: 최대 불리 이동 추적
    _min_adverse_price: float = 0.0


@dataclass
class SimTrade:
    entry_time:       str
    exit_time:        str
    direction:        int
    entry_price:      float
    exit_price:       float
    tp_price:         float
    sl_price:         float     # 실제 청산 시점 sl_price
    initial_sl:       float
    pnl:              float
    exit_reason:      str
    entry_bias:       str
    exit_bias:        str
    hold_ticks:       int
    trailing_active:  bool      # 트레일링이 활성화된 상태에서 청산됐는지
    max_favorable_pt:    float   # 보유 중 최대 유리 이동 (pt)
    max_adverse_excursion: float  # 보유 중 최대 불리 이동 (pt, 항상 양수)
    entry_llm_call_id:   int     # llm_calls.id FK (llm_filter 기존)
    entry_llm_score:     float   # 진입 시 LLM opportunity_score
    entry_llm_direction: str     # 진입 시 LLM direction_bias
    entry_flow_order_size:  float = 1.0   # PositionEngine 최종 주문 사이즈
    entry_flow_llm_log_id:  int   = 0     # LLMDecisionLog row id
    foreign_signal_mode:    str   = "composite"  # 진입 시 외국인 신호 모드
    level_no:               Optional[int] = None   # 이지모드 레벨 번호
    simulation_mode:        str           = "expert"  # "easy" | "expert"


class SimEngine:

    def __init__(self,
                 db_path:               str,
                 tp_ticks:              int   = 0,
                 sl_ticks:              int   = 10,
                 trailing_ticks:        int   = 12,
                 trailing_activate_ticks: int = 8,
                 neutral_exit_ticks:    int   = 20,
                 profit_protect_ticks:  int   = 6,
                 slippage_ticks:        int   = 1,
                 force_exit_hour:       int   = 14,
                 force_exit_min:        int   = 45,
                 recorder=None,
                 cfg_mgr=None,
                 llm_filter=None) -> None:

        self.tp_ticks                = tp_ticks
        self.sl_ticks                = sl_ticks
        self.trailing_ticks          = trailing_ticks
        self.trailing_activate_ticks = trailing_activate_ticks
        self.neutral_exit_ticks      = neutral_exit_ticks
        self.profit_protect_ticks    = profit_protect_ticks
        self.slippage_pt             = slippage_ticks * TICK_UNIT
        self.force_exit_hour         = force_exit_hour
        self.force_exit_min          = force_exit_min
        self._recorder               = recorder   # RegimeRecorder 주입
        self._cfg_mgr                = cfg_mgr    # ConfigManager 주입
        self._llm_filter             = llm_filter  # LLMOpportunityFilter 주입 (None=비활성)

        self._lock        = threading.Lock()
        self._pos: Optional[SimPosition] = None
        self._tick_count  = 0
        self._equity      = 0.0

        self._conn = sqlite3.connect(db_path, timeout=30, check_same_thread=False)
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA synchronous=NORMAL")
        self._conn.execute("PRAGMA busy_timeout=30000")
        self._setup_tables()
        self._restore_open_pos()   # 재시작 시 미결 포지션 복원

        log.info(
            "SimEngine v3 | TP=%s SL=%d틱 TRAIL=%s(활성:%d틱) "
            "NEUTRAL=%d틱(수익>=%d틱시무시) SLIP=%d틱 강제=%02d:%02d",
            f"{tp_ticks}틱" if tp_ticks > 0 else "OFF",
            sl_ticks,
            f"{trailing_ticks}틱" if trailing_ticks > 0 else "OFF",
            trailing_activate_ticks,
            neutral_exit_ticks, profit_protect_ticks,
            slippage_ticks, force_exit_hour, force_exit_min,
        )

    # ─── DB 셋업 ──────────────────────────────────────────────────
    def _setup_tables(self) -> None:
        self._conn.executescript("""
        CREATE TABLE IF NOT EXISTS sim_trades (
            id                    INTEGER PRIMARY KEY AUTOINCREMENT,
            entry_time            TEXT    NOT NULL,
            exit_time             TEXT    NOT NULL,
            direction             INTEGER NOT NULL,
            entry_price           REAL    NOT NULL,
            exit_price            REAL    NOT NULL,
            tp_price              REAL,
            sl_price              REAL,
            initial_sl            REAL,
            pnl                   REAL    NOT NULL,
            exit_reason           TEXT    NOT NULL,
            entry_bias            TEXT,
            exit_bias             TEXT,
            hold_ticks            INTEGER,
            trailing_active       INTEGER DEFAULT 0,
            max_favorable_pt      REAL    DEFAULT 0,
            max_adverse_excursion REAL    DEFAULT 0,
            cum_equity            REAL,
            entry_llm_call_id     INTEGER DEFAULT 0,
            entry_llm_score       REAL    DEFAULT -1,
            entry_llm_direction   TEXT    DEFAULT 'UNKNOWN',
            entry_flow_order_size REAL    DEFAULT 1.0,
            entry_flow_llm_log_id INTEGER DEFAULT 0,
            foreign_signal_mode   TEXT    DEFAULT 'composite',
            level_no              INTEGER NULL,
            simulation_mode       TEXT    DEFAULT 'expert'
        );
        CREATE TABLE IF NOT EXISTS sim_equity (
            id            INTEGER PRIMARY KEY AUTOINCREMENT,
            ts            TEXT  NOT NULL,
            futures_price REAL,
            equity        REAL  NOT NULL,
            open_pnl      REAL  NOT NULL,
            total_equity  REAL  NOT NULL,
            tp_price      REAL,
            sl_price      REAL,
            trailing_active  INTEGER DEFAULT 0,
            extreme_price    REAL,
            bias          TEXT,
            entry_signal  TEXT,
            has_position  INTEGER NOT NULL DEFAULT 0
        );
        CREATE TABLE IF NOT EXISTS sim_open_pos (
            id   INTEGER PRIMARY KEY CHECK (id = 1),
            data TEXT    NOT NULL
        );
        """)
        # 구버전 DB 컬럼 마이그레이션
        migrations = [
            ("sim_trades", "initial_sl",            "REAL"),
            ("sim_trades", "trailing_active",       "INTEGER DEFAULT 0"),
            ("sim_trades", "max_favorable_pt",      "REAL DEFAULT 0"),
            ("sim_trades", "max_adverse_excursion", "REAL DEFAULT 0"),
            ("sim_trades", "entry_llm_call_id",     "INTEGER DEFAULT 0"),
            ("sim_trades", "entry_llm_score",       "REAL DEFAULT -1"),
            ("sim_trades", "entry_llm_direction",   "TEXT DEFAULT 'UNKNOWN'"),
            ("sim_trades", "entry_flow_order_size", "REAL DEFAULT 1.0"),
            ("sim_trades", "entry_flow_llm_log_id", "INTEGER DEFAULT 0"),
            ("sim_trades", "foreign_signal_mode",   "TEXT DEFAULT 'composite'"),
            ("sim_trades", "level_no",              "INTEGER NULL"),
            ("sim_trades", "simulation_mode",       "TEXT DEFAULT 'expert'"),
            ("sim_equity", "trailing_active",       "INTEGER DEFAULT 0"),
            ("sim_equity", "extreme_price",         "REAL"),
        ]
        for table, col, dtype in migrations:
            try:
                self._conn.execute(f"ALTER TABLE {table} ADD COLUMN {col} {dtype}")
            except Exception:
                pass
        self._conn.commit()

    # ─── 미결 포지션 영속성 ────────────────────────────────────────
    def _persist_open_pos(self) -> None:
        """현재 미결 포지션을 DB에 직렬화 저장 (재시작 복원용)."""
        import json as _json
        pos = self._pos
        if pos is None:
            return
        data = {
            "direction": pos.direction, "entry_price": pos.entry_price,
            "entry_time": pos.entry_time, "entry_bias": pos.entry_bias,
            "tp_price": pos.tp_price, "sl_price": pos.sl_price,
            "initial_sl": pos.initial_sl, "trailing_active": pos.trailing_active,
            "extreme_price": pos.extreme_price, "neutral_ticks": pos.neutral_ticks,
            "entry_long_score": pos.entry_long_score,
            "entry_short_score": pos.entry_short_score,
            "entry_confidence": pos.entry_confidence, "entry_basis": pos.entry_basis,
            "entry_slope": pos.entry_slope, "entry_vol_burst": pos.entry_vol_burst,
            "entry_params": pos.entry_params, "entry_reason": pos.entry_reason,
            "config_hash": pos.config_hash,
            "entry_llm_call_id": pos.entry_llm_call_id,
            "entry_llm_score": pos.entry_llm_score,
            "entry_llm_direction": pos.entry_llm_direction,
            "entry_flow_order_size": pos.entry_flow_order_size,
            "entry_flow_llm_log_id": pos.entry_flow_llm_log_id,
            "foreign_signal_mode": pos.foreign_signal_mode,
            "level_no":            pos.level_no,
            "simulation_mode":     pos.simulation_mode,
            "param_hash": pos.param_hash, "param_store_hash": pos.param_store_hash,
            "param_store_version": pos.param_store_version,
            "entry_trend_weight_long": pos.entry_trend_weight_long,
            "entry_trend_weight_short": pos.entry_trend_weight_short,
            "entry_trend_long_score": pos.entry_trend_long_score,
            "entry_trend_short_score": pos.entry_trend_short_score,
            "_min_adverse_price": pos._min_adverse_price,
            "_tick_count": self._tick_count,
            "_equity": self._equity,
        }
        self._conn.execute(
            "INSERT OR REPLACE INTO sim_open_pos (id, data) VALUES (1, ?)",
            (_json.dumps(data, ensure_ascii=False),),
        )
        self._conn.commit()

    def _restore_open_pos(self) -> None:
        """시작 시 DB에서 미결 포지션 복원. 데이터 이상 시 레코드 삭제 후 무시."""
        import json as _json
        row = self._conn.execute(
            "SELECT data FROM sim_open_pos WHERE id = 1"
        ).fetchone()
        if row is None:
            return
        try:
            d = _json.loads(row[0])
            self._pos = SimPosition(
                direction    = d["direction"],
                entry_price  = d["entry_price"],
                entry_time   = d["entry_time"],
                entry_bias   = d["entry_bias"],
                tp_price     = d["tp_price"],
                sl_price     = d["sl_price"],
                initial_sl   = d["initial_sl"],
                trailing_active  = d.get("trailing_active", False),
                extreme_price    = d.get("extreme_price", d["entry_price"]),
                neutral_ticks    = d.get("neutral_ticks", 0),
                entry_long_score  = d.get("entry_long_score",  0.0),
                entry_short_score = d.get("entry_short_score", 0.0),
                entry_confidence  = d.get("entry_confidence",  0.0),
                entry_basis       = d.get("entry_basis",       0.0),
                entry_slope       = d.get("entry_slope",       0.0),
                entry_vol_burst   = d.get("entry_vol_burst",   0.0),
                entry_params      = d.get("entry_params",  ""),
                entry_reason      = d.get("entry_reason",  ""),
                config_hash       = d.get("config_hash",   ""),
                entry_llm_call_id    = d.get("entry_llm_call_id",    0),
                entry_llm_score      = d.get("entry_llm_score",      -1.0),
                entry_llm_direction  = d.get("entry_llm_direction",  "UNKNOWN"),
                param_hash           = d.get("param_hash",           ""),
                param_store_hash     = d.get("param_store_hash",     ""),
                param_store_version  = d.get("param_store_version",  0),
                entry_trend_weight_long  = d.get("entry_trend_weight_long",  0.0),
                entry_trend_weight_short = d.get("entry_trend_weight_short", 0.0),
                entry_trend_long_score   = d.get("entry_trend_long_score",   0.0),
                entry_trend_short_score  = d.get("entry_trend_short_score",  0.0),
                entry_flow_order_size    = d.get("entry_flow_order_size",   1.0),
                entry_flow_llm_log_id    = d.get("entry_flow_llm_log_id",   0),
                foreign_signal_mode      = d.get("foreign_signal_mode",     "composite"),
                level_no                 = d.get("level_no"),
                simulation_mode          = d.get("simulation_mode",         "expert"),
            )
            self._pos._min_adverse_price = d.get("_min_adverse_price", d["entry_price"])
            self._tick_count = d.get("_tick_count", 0)
            self._equity     = d.get("_equity",     0.0)
            dir_str = "LONG" if self._pos.direction == 1 else "SHORT"
            log.warning(
                "[SIM] 미결 포지션 복원 | %s | 진입=%.2f | time=%s",
                dir_str, self._pos.entry_price, self._pos.entry_time,
            )
        except Exception as exc:
            log.error("[SIM] 미결 포지션 복원 실패 — 레코드 삭제: %s", exc)
            self._conn.execute("DELETE FROM sim_open_pos WHERE id = 1")
            self._conn.commit()

    # ─── 계산 ─────────────────────────────────────────────────────
    def _calc_pnl(self, direction: int, entry: float, exit_: float) -> float:
        adj_entry = entry + direction * self.slippage_pt
        adj_exit  = exit_ - direction * self.slippage_pt
        return (adj_exit - adj_entry) * direction * POINT_VALUE

    def _calc_initial_tp_sl(self, direction: int,
                             entry: float,
                             tp_override: int | None = None,
                             sl_override: int | None = None) -> tuple[float, float]:
        """(tp_price, sl_price). tp_ticks=0이면 tp=0.
        tp_override / sl_override: LLM이 이번 진입 한정 추천한 틱 수."""
        tp_ticks = tp_override if tp_override is not None else self.tp_ticks
        sl_ticks = sl_override if sl_override is not None else self.sl_ticks
        tp = (round(entry + direction * tp_ticks * TICK_UNIT, 4)
              if tp_ticks > 0 else 0.0)
        sl = round(entry - direction * sl_ticks * TICK_UNIT, 4)
        return tp, sl

    def _update_trailing(self, pos: SimPosition, price: float) -> None:
        """트레일링 활성화 체크 + extreme_price / sl_price 갱신."""
        if self.trailing_ticks <= 0:
            return

        # 현재 유리 이동 틱 수
        favorable_pt = (price - pos.entry_price) * pos.direction
        favorable_ticks = favorable_pt / TICK_UNIT

        # 트레일링 활성화 조건
        if not pos.trailing_active:
            if favorable_ticks >= self.trailing_activate_ticks:
                pos.trailing_active = True
                pos.extreme_price   = price
                log.info("[SIM] 트레일링 활성 | extreme=%.2f | 유리이동=%.1f틱",
                         price, favorable_ticks)

        # 최대 불리 이동과 독립적으로 최대 불리 이동 추적 (MAE)
        if pos.direction == 1:   # LONG: 최저가 = 불리
            if pos._min_adverse_price == 0.0 or price < pos._min_adverse_price:
                pos._min_adverse_price = price
        else:                    # SHORT: 최고가 = 불리
            if pos._min_adverse_price == 0.0 or price > pos._min_adverse_price:
                pos._min_adverse_price = price

        # 트레일링 활성 상태: extreme 갱신 + SL 추적
        if pos.trailing_active:
            if pos.direction == 1:   # LONG: 최고가 추적
                if price > pos.extreme_price:
                    pos.extreme_price = price
            else:                    # SHORT: 최저가 추적
                if price < pos.extreme_price:
                    pos.extreme_price = price

            # 새 SL = extreme - direction * trailing_ticks
            new_sl = round(
                pos.extreme_price - pos.direction * self.trailing_ticks * TICK_UNIT,
                4
            )
            # SL은 진입 방향으로만 이동 (뒤로 물리지 않음)
            if pos.direction == 1:
                pos.sl_price = max(pos.sl_price, new_sl)
            else:
                pos.sl_price = min(pos.sl_price, new_sl)

    def _check_tp_sl(self, pos: SimPosition,
                     price: float) -> Optional[str]:
        """TP/SL 도달 여부."""
        # TP 체크 (활성 시)
        if pos.tp_price and pos.tp_price > 0:
            if pos.direction == 1 and price >= pos.tp_price:
                return "TAKE_PROFIT"
            if pos.direction == -1 and price <= pos.tp_price:
                return "TAKE_PROFIT"
        # SL 체크 (트레일링 or 고정)
        if pos.direction == 1 and price <= pos.sl_price:
            return "TRAILING_STOP" if pos.trailing_active else "STOP_LOSS"
        if pos.direction == -1 and price >= pos.sl_price:
            return "TRAILING_STOP" if pos.trailing_active else "STOP_LOSS"
        return None

    def _is_force_exit_time(self, ts: str) -> bool:
        """주간(14:45)과 야간(05:50) 모두 처리.
        야간: force_exit_hour < 12 → 새벽 시간대만 체크
              장 시작(18~23시)에는 절대 강제청산 안 함
        """
        try:
            t = datetime.strptime(ts[:19], "%Y-%m-%d %H:%M:%S")
            fh, fm = self.force_exit_hour, self.force_exit_min
            if fh < 12:
                # 야간 세션(18:00~06:00): 18~23시는 장 초반 — 차단 안 함
                if t.hour >= 12:
                    return False
                return (t.hour, t.minute) >= (fh, fm)
            else:
                # 주간 세션: 기존 로직
                return (t.hour, t.minute) >= (fh, fm)
        except Exception:
            return False

    # ─── 진입/청산 ────────────────────────────────────────────────
    def _try_open(self, ts: str, price: float,
                  bias: str, entry_signal: str,
                  snap: dict | None = None) -> None:
        """LLM 필터 게이팅 후 진입 여부 결정."""
        long_ok  = (entry_signal == "LONG_READY"  and bias == "LONG_BIAS")
        short_ok = (entry_signal == "SHORT_READY" and bias == "SHORT_BIAS")
        if not (long_ok or short_ok):
            return

        direction = +1 if long_ok else -1

        # LLM 필터 게이팅 + tp/sl override 적용
        tp_override: int | None = None
        sl_override: int | None = None

        if self._llm_filter is not None:
            allow, opp = self._llm_filter.should_allow_entry(bias, entry_signal)
            if not allow:
                log.debug(
                    "[SIM] LLM 필터 차단 | signal=%s | score=%.2f | dir=%s",
                    entry_signal,
                    float(opp.get("opportunity_score", 0.0)) if opp else 0.0,
                    opp.get("direction_bias", "?") if opp else "?",
                )
                return
            # LLM이 이번 진입 한정 TP/SL override 추천하면 적용
            if opp:
                _tp = opp.get("tp_ticks")
                _sl = opp.get("sl_ticks")
                if isinstance(_tp, int) and 4 <= _tp <= 30:
                    tp_override = _tp
                if isinstance(_sl, int) and 4 <= _sl <= 30:
                    sl_override = _sl
                if tp_override or sl_override:
                    log.debug(
                        "[SIM] LLM TP/SL override | tp=%s | sl=%s",
                        tp_override, sl_override,
                    )

        self._open(ts, price, bias, direction, snap,
                   tp_override=tp_override, sl_override=sl_override)

    def _open(self, ts: str, price: float,
              bias: str, direction: int,
              snap: dict | None = None,
              tp_override: int | None = None,
              sl_override: int | None = None) -> None:
        """tp_override / sl_override: LLM이 이번 진입 한정 추천한 틱 수."""
        tp, sl = self._calc_initial_tp_sl(direction, price,
                                          tp_override=tp_override,
                                          sl_override=sl_override)
        cfg_str = ""
        cfg_hash = ""
        if self._cfg_mgr:
            import json as _json
            cfg_str  = _json.dumps(self._cfg_mgr.get(), ensure_ascii=False)
            cfg_hash = self._cfg_mgr.get_hash()
        s = snap or {}
        self._pos = SimPosition(
            direction=direction, entry_price=price,
            entry_time=ts, entry_bias=bias,
            tp_price=tp, sl_price=sl, initial_sl=sl,
            extreme_price=price,
            entry_long_score  = float(s.get("long_score",  0)),
            entry_short_score = float(s.get("short_score", 0)),
            entry_confidence  = float(s.get("confidence",  0)),
            entry_basis       = float(s.get("basis",       0)),
            entry_slope       = float(s.get("basis_slope", 0)),
            entry_vol_burst   = float(s.get("volume_burst",0)),
            entry_params      = cfg_str,
            entry_reason      = str(s.get("reason", "")),
            config_hash       = cfg_hash,
            # ParamStore 추세 파라미터 fingerprint (거래 분석 핵심)
            param_hash            = str(s.get("param_hash",          "")),
            param_store_hash      = str(s.get("param_store_hash",    "")),
            param_store_version   = int(s.get("param_store_version", 0)),
            entry_trend_weight_long  = float(s.get("trend_weight_long",  0.0)),
            entry_trend_weight_short = float(s.get("trend_weight_short", 0.0)),
            entry_trend_long_score   = float(s.get("trend_long_score",   0.0)),
            entry_trend_short_score  = float(s.get("trend_short_score",  0.0)),
        )
        self._tick_count = 0

        # LLM 필터 정보 주입
        if self._llm_filter is not None:
            opp = self._llm_filter.get_latest() or {}
            meta = self._llm_filter.get_latest_meta() or {}
            self._pos.entry_llm_score     = float(opp.get("opportunity_score", -1.0))
            self._pos.entry_llm_direction = str(opp.get("direction_bias", "UNKNOWN"))
            # llm_call_id는 메타에서 가져옴 (없으면 0)
            self._pos.entry_llm_call_id   = int(meta.get("llm_call_id", 0))
        # FlowEngine 파이프라인 연동 (snap에서 읽기)
        self._pos.entry_flow_order_size = float(s.get("flow_order_size", 1.0))
        self._pos.entry_flow_llm_log_id = int(s.get("flow_llm_log_id", 0))
        self._pos.foreign_signal_mode   = str(s.get("foreign_signal_mode", "composite"))
        # 시뮬레이션 모드 기록 (easy=레벨번호 / expert=None)
        _sel_level = s.get("selected_level")
        self._pos.level_no        = int(_sel_level) if isinstance(_sel_level, int) else None
        self._pos.simulation_mode = str(s.get("simulation_mode", "expert"))
        self._pos._min_adverse_price = price  # MAE 초기값 = 진입가

        log.info(
            "[SIM] 진입 %s | %.2f  TP=%s  SL=%.2f  TRAIL=%s",
            "LONG" if direction == 1 else "SHORT", price,
            f"{tp:.2f}" if tp else "OFF", sl,
            f"{self.trailing_ticks}틱(활성:{self.trailing_activate_ticks}틱)"
            if self.trailing_ticks > 0 else "OFF",
        )
        self._persist_open_pos()   # 재시작 복원용 — 진입 즉시 DB 저장

    def _close(self, ts: str, price: float,
               exit_bias: str, reason: str,
               exit_snap: dict | None = None,
               data_gate_ok: bool = True) -> SimTrade:
        pos = self._pos
        pnl = self._calc_pnl(pos.direction, pos.entry_price, price)
        self._equity += pnl
        max_fav = (pos.extreme_price - pos.entry_price) * pos.direction
        # MAE: 불리 방향 반대 → 항상 양수
        if pos._min_adverse_price > 0.0:
            raw_mae = (pos.entry_price - pos._min_adverse_price) * pos.direction
        else:
            raw_mae = 0.0
        max_adverse = round(max(0.0, raw_mae), 4)

        trade = SimTrade(
            entry_time           = pos.entry_time,
            exit_time            = ts,
            direction            = pos.direction,
            entry_price          = pos.entry_price,
            exit_price           = price,
            tp_price             = pos.tp_price,
            sl_price             = pos.sl_price,
            initial_sl           = pos.initial_sl,
            pnl                  = round(pnl, 0),
            exit_reason          = reason,
            entry_bias           = pos.entry_bias,
            exit_bias            = exit_bias,
            hold_ticks           = self._tick_count,
            trailing_active      = pos.trailing_active,
            max_favorable_pt     = round(max_fav, 4),
            max_adverse_excursion= max_adverse,
            entry_llm_call_id    = pos.entry_llm_call_id,
            entry_llm_score      = pos.entry_llm_score,
            entry_llm_direction  = pos.entry_llm_direction,
            entry_flow_order_size = pos.entry_flow_order_size,
            entry_flow_llm_log_id = pos.entry_flow_llm_log_id,
            foreign_signal_mode   = pos.foreign_signal_mode,
            level_no              = pos.level_no,
            simulation_mode       = pos.simulation_mode,
        )
        # 미결 포지션 DB 레코드 삭제 (청산 완료)
        # commit을 즉시 호출하여 WAL write-lock 해제 후 recorder(다른 커넥션) write 진행
        # commit 없이 recorder.insert_trade() 호출 시 두 커넥션이 동시에 write lock 경쟁
        # → busy_timeout 30s 후 "database is locked" 연쇄 발생
        self._conn.execute("DELETE FROM sim_open_pos WHERE id = 1")
        self._conn.commit()

        # trades 테이블 완결 레코드 기록 (data_gate가 차단 중이면 stale 가격 오염 방지)
        if self._recorder and data_gate_ok:
            es = exit_snap or {}
            cfg_str = ""
            if self._cfg_mgr:
                import json as _json
                cfg_str = _json.dumps(self._cfg_mgr.get(), ensure_ascii=False)
            self._recorder.insert_trade({
                "session_id":         pos.config_hash,
                "open_ts":            pos.entry_time,
                "close_ts":           ts,
                "direction":          pos.direction,
                "entry_price":        pos.entry_price,
                "exit_price":         price,
                "pnl_ticks":          round(pnl / TICK_VALUE, 2),
                "exit_reason":        reason,
                "entry_bias":         pos.entry_bias,
                "entry_long_score":   pos.entry_long_score,
                "entry_short_score":  pos.entry_short_score,
                "entry_confidence":   pos.entry_confidence,
                "entry_basis":        pos.entry_basis,
                "entry_slope":        pos.entry_slope,
                "entry_vol_burst":    pos.entry_vol_burst,
                "entry_params":       pos.entry_params,
                "entry_reason":       pos.entry_reason,
                "exit_bias":          exit_bias,
                "exit_long_score":    float(es.get("long_score",  0)),
                "exit_short_score":   float(es.get("short_score", 0)),
                "exit_confidence":    float(es.get("confidence",  0)),
                "exit_params":        cfg_str,
                "exit_reason_detail": str(es.get("reason", "")),
                "config_hash":        pos.config_hash,
                "max_favorable_pt":      round(max_fav, 4),
                "max_adverse_excursion": max_adverse,
                "hold_ticks":            self._tick_count,
                "cum_equity":            round(self._equity, 0),
                "entry_llm_call_id":     pos.entry_llm_call_id,
                "entry_llm_score":       pos.entry_llm_score,
                "entry_llm_direction":   pos.entry_llm_direction,
                "entry_flow_order_size": pos.entry_flow_order_size,
                "entry_flow_llm_log_id": pos.entry_flow_llm_log_id,
                "foreign_signal_mode":   pos.foreign_signal_mode,
                "level_no":              pos.level_no,
                "simulation_mode":       pos.simulation_mode,
                # ParamStore 추세 파라미터 fingerprint
                "param_hash":              pos.param_hash,
                "param_store_hash":        pos.param_store_hash,
                "param_store_version":     pos.param_store_version,
                "entry_trend_weight_long": pos.entry_trend_weight_long,
                "entry_trend_weight_short":pos.entry_trend_weight_short,
                "entry_trend_long_score":  pos.entry_trend_long_score,
                "entry_trend_short_score": pos.entry_trend_short_score,
            })
        self._pos        = None
        self._tick_count = 0
        log.info(
            "[SIM] 청산 %s | pnl=%+.0f원 | max_fav=%.2fpt | max_adv=%.2fpt | trailing=%s | llm_score=%.2f | cum=%+.0f원",
            reason, pnl, max_fav, max_adverse,
            "ON" if trade.trailing_active else "OFF",
            pos.entry_llm_score, self._equity,
        )
        return trade

    # ─── DB 저장 ──────────────────────────────────────────────────
    def _save_trade(self, trade: SimTrade) -> None:
        """sim_trades: 대시보드용 간이 로그 (trades가 RAG 원본 기준).
        두 테이블 컬럼 동기화 유지. direction_bias(JSON 응답 필드) != direction(DB 컬럼)."""
        self._conn.execute("""
            INSERT INTO sim_trades
              (entry_time, exit_time, direction, entry_price, exit_price,
               tp_price, sl_price, initial_sl, pnl, exit_reason,
               entry_bias, exit_bias, hold_ticks,
               trailing_active, max_favorable_pt, max_adverse_excursion,
               cum_equity, entry_llm_call_id, entry_llm_score, entry_llm_direction,
               entry_flow_order_size, entry_flow_llm_log_id,
               foreign_signal_mode, level_no, simulation_mode)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """, (trade.entry_time, trade.exit_time, trade.direction,
              trade.entry_price, trade.exit_price,
              trade.tp_price or None, trade.sl_price, trade.initial_sl,
              trade.pnl, trade.exit_reason,
              trade.entry_bias, trade.exit_bias, trade.hold_ticks,
              int(trade.trailing_active), trade.max_favorable_pt,
              trade.max_adverse_excursion,
              self._equity,
              trade.entry_llm_call_id,
              trade.entry_llm_score if trade.entry_llm_score >= 0 else None,
              trade.entry_llm_direction,
              trade.entry_flow_order_size,
              trade.entry_flow_llm_log_id,
              trade.foreign_signal_mode,
              trade.level_no,
              trade.simulation_mode))
        self._conn.commit()

    def _save_equity(self, ts: str, price: float,
                     bias: str, entry_signal: str) -> None:
        open_pnl = tp_p = sl_p = extreme_p = 0.0
        trailing = 0
        if self._pos:
            open_pnl  = self._calc_pnl(
                self._pos.direction, self._pos.entry_price, price)
            tp_p      = self._pos.tp_price or None
            sl_p      = self._pos.sl_price
            extreme_p = self._pos.extreme_price
            trailing  = int(self._pos.trailing_active)
        self._conn.execute("""
            INSERT INTO sim_equity
              (ts, futures_price, equity, open_pnl, total_equity,
               tp_price, sl_price, trailing_active, extreme_price,
               bias, entry_signal, has_position)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
        """, (ts, price,
              round(self._equity, 0), round(open_pnl, 0),
              round(self._equity + open_pnl, 0),
              tp_p, sl_p, trailing, extreme_p,
              bias, entry_signal, 1 if self._pos else 0))
        self._conn.commit()

    # ─── 메인 틱 처리 ─────────────────────────────────────────────
    def on_tick(self, ts: str, futures_price: float,
                bias: str, entry_signal: str,
                long_score: float = 0.0,
                short_score: float = 0.0,
                snap: dict | None = None,
                data_gate_ok: bool = True) -> None:
        """
        snap: analyzer_app의 현재 state 딕셔너리 (진입/청산 조건 기록용).
        data_gate_ok: False면 trades 테이블 기록 생략 (stale 데이터 오염 방지).
        """
        with self._lock:
            self._tick_count += 1
            trade: Optional[SimTrade] = None

            if self._pos:
                pos     = self._pos
                is_long = pos.direction == 1

                raw_pnl_pt    = (futures_price - pos.entry_price) * pos.direction
                profit_ticks  = round(raw_pnl_pt / TICK_UNIT)

                # data_gate=False(simulated/stale)일 때 trailing 갱신 차단
                # 재연결 직후 쓰레기 가격(350, 0 등)으로 extreme/SL이 오염되는 것을 방지
                if data_gate_ok:
                    self._update_trailing(pos, futures_price)

                tpsl = self._check_tp_sl(pos, futures_price)
                if tpsl:
                    exit_p = (pos.tp_price if tpsl == "TAKE_PROFIT"
                              else pos.sl_price)
                    trade  = self._close(ts, exit_p, bias, tpsl, snap, data_gate_ok)

                elif self._is_force_exit_time(ts):
                    trade = self._close(ts, futures_price, bias, "FORCE_EXIT_EOD", snap, data_gate_ok)

                elif (is_long     and bias == "SHORT_BIAS") or \
                     (not is_long and bias == "LONG_BIAS"):
                    trade = self._close(ts, futures_price, bias, "BIAS_REVERSAL", snap, data_gate_ok)

                elif not pos.trailing_active and (
                    (is_long     and entry_signal == "SHORT_READY") or
                    (not is_long and entry_signal == "LONG_READY")
                ):
                    trade = self._close(ts, futures_price, bias, "SIGNAL_CONFLICT", snap, data_gate_ok)

                elif bias == "NEUTRAL":
                    if profit_ticks >= self.profit_protect_ticks:
                        pos.neutral_ticks = 0
                    else:
                        pos.neutral_ticks += 1
                        if pos.neutral_ticks >= self.neutral_exit_ticks:
                            trade = self._close(ts, futures_price, bias,
                                                "NEUTRAL_TIMEOUT", snap, data_gate_ok)
                else:
                    pos.neutral_ticks = 0

            if self._pos is None and trade is None:
                self._try_open(ts, futures_price, bias, entry_signal, snap)

            elif self._pos is None and trade is not None \
                    and trade.exit_reason == "BIAS_REVERSAL":
                self._try_open(ts, futures_price, bias, entry_signal, snap)

            if trade:
                self._save_trade(trade)
            self._save_equity(ts, futures_price, bias, entry_signal)

    # ─── 상태 조회 ────────────────────────────────────────────────
    def get_state(self) -> dict:
        with self._lock:
            pos = self._pos
            return {
                "has_position":        pos is not None,
                "direction":           pos.direction           if pos else 0,
                "entry_price":         pos.entry_price         if pos else 0.0,
                "tp_price":            pos.tp_price            if pos else 0.0,
                "sl_price":            pos.sl_price            if pos else 0.0,
                "initial_sl":          pos.initial_sl          if pos else 0.0,
                "trailing_active":     pos.trailing_active     if pos else False,
                "extreme_price":       pos.extreme_price       if pos else 0.0,
                "entry_time":          pos.entry_time          if pos else "",
                "entry_llm_call_id":   pos.entry_llm_call_id  if pos else 0,
                "entry_llm_score":     pos.entry_llm_score     if pos else -1.0,
                "entry_llm_direction": pos.entry_llm_direction if pos else "UNKNOWN",
                "entry_flow_order_size": pos.entry_flow_order_size if pos else 1.0,
                "entry_flow_llm_log_id": pos.entry_flow_llm_log_id if pos else 0,
                "cum_equity":          self._equity,
            }
