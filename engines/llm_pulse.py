# engines/llm_pulse.py
"""
LLMPulse — 5분 인터벌 장세 종합 평가기

목적:
  - 판단 근거용 LLM 정책(기존)과 별개로 5분마다 독립 장세 평가
  - mmean.db pulse_history 테이블에 영구 기록 → 재시작 후에도 이력 유지
  - 방향 전환 빈도/지속시간 통계, 다일간 심층 분석 제공

pulse_history 테이블 스키마:
  id            INTEGER PK AUTOINCREMENT
  date          TEXT       YYYY-MM-DD
  ts            TEXT       HH:MM:SS
  datetime_full TEXT       YYYY-MM-DD HH:MM:SS  (정렬·범위 쿼리용)
  direction     TEXT       BULLISH|NEUTRAL|BEARISH
  confidence    REAL       0.0~1.0
  summary       TEXT       2~3문장 종합 평가
  key_factor    TEXT       핵심 근거 10단어 이내
  changed       INTEGER    0/1 — 직전 대비 방향 전환 여부
  duration_min  REAL       NULL=진행 중, 채워지면 해당 방향 지속 분수
  -- 컨텍스트 스냅샷 (심층 분석용)
  flow_score    REAL
  foreign_buy   REAL
  basis         REAL
  futures_price REAL
  vwap          REAL
  bias          TEXT
  entry_signal  TEXT
"""
from __future__ import annotations

import json
import logging
import os
import sqlite3
import threading
from collections import deque
from datetime import datetime, timedelta
from typing import Any, Deque, Dict, List, Optional

import requests

log = logging.getLogger("MMEAN.LLMPulse")

_HAIKU_MODEL    = "claude-haiku-4-5-20251001"
_HAIKU_URL      = "https://api.anthropic.com/v1/messages"
_HAIKU_TIMEOUT  = 15
_MAX_TOKENS     = 450
_HISTORY_MAXLEN = 48   # 인메모리 최대 48개 (4시간분)

VALID_DIRECTIONS = {"BULLISH", "NEUTRAL", "BEARISH"}

_SYSTEM_PROMPT = """\
당신은 KOSPI200 현물 매매를 돕는 장중 장세 판단 엔진이다.
목적: 지금 현물을 사야 할지(BULLISH), 팔거나 관망해야 할지(BEARISH), 방향이 없는지(NEUTRAL) 판단.

[판단 기준]
BULLISH 조건 (하나 이상 충족):
  - long_score > short_score+1.5 이고 flow_score > 0
  - 외국인 5분 delta 양수이고 가격이 VWAP 위 (누적 총량 무시, 최근 5분 변화만 판단)
  - flow_score 상승 추이이고 LONG비율 > 60%

BEARISH 조건 (하나 이상 충족):
  - short_score > long_score+1.0 이고 flow_score < 0
  - 외국인 5분 delta 음수이고 가격이 VWAP 아래 (누적 총량 무시, 최근 5분 변화만 판단)
  - flow_score 하락 추이이고 SHORT비율 > 60%

NEUTRAL: 위 조건이 모두 불충족이거나 신호가 상충할 때만 선택.
확신 없다고 NEUTRAL을 남발하지 말 것 — 애매하면 우세한 방향으로 낮은 confidence로 판단.

반드시 아래 JSON 형식으로만 응답 (다른 텍스트 없이):
{"direction": "...", "confidence": 0.00, "summary": "...", "key_factor": "..."}

direction: BULLISH | NEUTRAL | BEARISH
confidence: 0.0~1.0 (BULLISH/BEARISH 최소 0.45 이상)
summary: 현물 매매 관점 종합 평가 2~3문장 (한국어, 매수/관망/매도 의견 포함)
key_factor: 판단 핵심 근거 10단어 이내 (한국어)"""

_CREATE_SQL = """
CREATE TABLE IF NOT EXISTS pulse_history (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    date          TEXT    NOT NULL,
    ts            TEXT    NOT NULL,
    datetime_full TEXT    NOT NULL,
    direction     TEXT    NOT NULL,
    confidence    REAL    NOT NULL,
    summary       TEXT    NOT NULL DEFAULT '',
    key_factor    TEXT    NOT NULL DEFAULT '',
    changed       INTEGER NOT NULL DEFAULT 0,
    duration_min  REAL,
    flow_score    REAL,
    foreign_buy   REAL,
    basis         REAL,
    futures_price REAL,
    vwap          REAL,
    bias          TEXT,
    entry_signal  TEXT
);
CREATE INDEX IF NOT EXISTS idx_pulse_date ON pulse_history(date);
CREATE INDEX IF NOT EXISTS idx_pulse_datetime ON pulse_history(datetime_full);
"""


class LLMPulse:
    """
    5분 인터벌 장세 종합 평가기.

    사용:
        pulse = LLMPulse(runtime, interval_sec=300)
        pulse.start()
        ...
        pulse.stop()
        data = pulse.get_data()        # 오늘 이력 + 통계
        data = pulse.get_analysis(30)  # 30일 심층 분석
    """

    def __init__(self, runtime: Any, interval_sec: int = 300) -> None:
        self._runtime    = runtime
        self._db_path    = runtime.db_path
        self._intraday_db = os.path.join(os.path.dirname(runtime.db_path), "intraday.db")
        self._interval   = interval_sec
        self._stop_event = threading.Event()
        self._thread: Optional[threading.Thread] = None
        self._history: Deque[Dict] = deque(maxlen=_HISTORY_MAXLEN)
        self._lock       = threading.Lock()
        self._api_key    = os.getenv("ANTHROPIC_API_KEY", "").strip()

        self._init_db()
        self._init_intraday_db()
        self._load_today_from_db()

        if not self._api_key:
            log.warning("LLMPulse: ANTHROPIC_API_KEY 없음 — 비활성 상태로 시작")

    # ── DB 초기화 ──────────────────────────────────────────────────

    def _init_db(self) -> None:
        """pulse_history 테이블 생성 (없으면). WAL 모드 활성화."""
        try:
            conn = sqlite3.connect(self._db_path, timeout=30)
            # WAL 모드: 읽기-쓰기 동시 허용 → JudgmentLog 등 타 스레드와 충돌 방지
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute("PRAGMA synchronous=NORMAL")   # WAL과 함께 성능·안전 균형
            conn.executescript(_CREATE_SQL)
            conn.close()
            log.info("LLMPulse DB 초기화 완료 (WAL) | %s", self._db_path)
        except Exception as e:
            log.error("LLMPulse DB 초기화 실패: %s", e)

    def _init_intraday_db(self) -> None:
        """intraday.db에 llm_pulse_log 테이블 생성."""
        try:
            conn = sqlite3.connect(self._intraday_db, timeout=10)
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute("""
                CREATE TABLE IF NOT EXISTS llm_pulse_log (
                    id            INTEGER PRIMARY KEY AUTOINCREMENT,
                    session_date  TEXT NOT NULL,
                    ts            TEXT NOT NULL,
                    direction     TEXT NOT NULL,
                    confidence    REAL NOT NULL,
                    key_factor    TEXT NOT NULL DEFAULT '',
                    summary       TEXT NOT NULL DEFAULT '',
                    changed       INTEGER NOT NULL DEFAULT 0,
                    flow_score    REAL,
                    foreign_buy   REAL,
                    futures_price REAL,
                    bias          TEXT,
                    entry_signal  TEXT
                )
            """)
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_llm_pulse_date ON llm_pulse_log(session_date)"
            )
            conn.commit()
            conn.close()
        except Exception as e:
            log.warning("LLMPulse intraday DB 초기화 실패 (계속): %s", e)

    def _save_intraday_log(self, snap: Dict, ctx: Dict) -> None:
        """llm_pulse_log에 1건 INSERT."""
        today = datetime.now().strftime("%Y-%m-%d")
        try:
            conn = sqlite3.connect(self._intraday_db, timeout=5)
            conn.execute(
                """INSERT INTO llm_pulse_log
                   (session_date, ts, direction, confidence, key_factor, summary,
                    changed, flow_score, foreign_buy, futures_price, bias, entry_signal)
                   VALUES (?,?,?,?,?,?, ?,?,?,?,?,?)""",
                (
                    today, snap["ts"],
                    snap["direction"], snap["confidence"],
                    snap["key_factor"], snap["summary"],
                    1 if snap["changed"] else 0,
                    ctx.get("flow_score"), ctx.get("foreign_buy"),
                    ctx.get("futures_price"), ctx.get("bias"),
                    ctx.get("entry_signal"),
                ),
            )
            conn.commit()
            conn.close()
        except Exception as e:
            log.warning("LLMPulse intraday INSERT 실패 (계속): %s", e)

    def _load_today_from_db(self) -> None:
        """재시작 시 오늘 이력을 DB에서 복원."""
        today = datetime.now().strftime("%Y-%m-%d")
        try:
            conn = sqlite3.connect(self._db_path, timeout=5)
            conn.row_factory = sqlite3.Row
            rows = conn.execute(
                """SELECT id, ts, direction, confidence, summary, key_factor,
                          changed, duration_min,
                          flow_score, foreign_buy, basis, futures_price, vwap,
                          bias, entry_signal
                   FROM pulse_history
                   WHERE date = ?
                   ORDER BY id""",
                (today,),
            ).fetchall()
            conn.close()
            with self._lock:
                self._history.clear()
                for r in rows:
                    self._history.append({
                        "row_id":       r["id"],
                        "ts":           r["ts"],
                        "direction":    r["direction"],
                        "confidence":   r["confidence"],
                        "summary":      r["summary"],
                        "key_factor":   r["key_factor"],
                        "changed":      bool(r["changed"]),
                        "duration_min": r["duration_min"],
                    })
            log.info("LLMPulse 오늘 이력 복원 | %d건 | date=%s", len(rows), today)
        except Exception as e:
            log.warning("LLMPulse 오늘 이력 복원 실패 (계속): %s", e)

    def _db_insert(self, snap: Dict, ctx: Dict) -> Optional[int]:
        """스냅샷 1건 INSERT → row_id 반환."""
        now_full = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        today    = datetime.now().strftime("%Y-%m-%d")
        try:
            conn = sqlite3.connect(self._db_path, timeout=15)
            cur  = conn.execute(
                """INSERT INTO pulse_history
                   (date, ts, datetime_full, direction, confidence,
                    summary, key_factor, changed,
                    flow_score, foreign_buy, basis, futures_price, vwap,
                    bias, entry_signal)
                   VALUES (?,?,?,?,?, ?,?,?, ?,?,?,?,?, ?,?)""",
                (
                    today, snap["ts"], now_full,
                    snap["direction"], snap["confidence"],
                    snap["summary"], snap["key_factor"],
                    1 if snap["changed"] else 0,
                    ctx.get("flow_score"), ctx.get("foreign_buy"),
                    ctx.get("basis"), ctx.get("futures_price"),
                    ctx.get("vwap"), ctx.get("bias"),
                    ctx.get("entry_signal"),
                ),
            )
            row_id = cur.lastrowid
            conn.commit()
            conn.close()
            return row_id
        except Exception as e:
            log.warning("LLMPulse DB INSERT 실패: %s", e)
            return None

    def _db_update_duration(self, row_id: int, duration_min: float) -> None:
        """직전 스냅샷의 duration_min 업데이트."""
        try:
            conn = sqlite3.connect(self._db_path, timeout=15)
            conn.execute(
                "UPDATE pulse_history SET duration_min=? WHERE id=?",
                (duration_min, row_id),
            )
            conn.commit()
            conn.close()
        except Exception as e:
            log.warning("LLMPulse DB UPDATE duration 실패: %s", e)

    # ── 수명 주기 ──────────────────────────────────────────────────

    def start(self) -> None:
        if self._thread and self._thread.is_alive():
            return
        self._stop_event.clear()
        self._thread = threading.Thread(
            target=self._loop,
            daemon=True,
            name="MMEAN-LLMPulse",
        )
        self._thread.start()
        log.info("LLMPulse 시작 | interval=%ds | model=%s", self._interval, _HAIKU_MODEL)

    def stop(self) -> None:
        self._stop_event.set()
        if self._thread:
            self._thread.join(timeout=5)
        log.info("LLMPulse 종료")

    # ── 메인 루프 ──────────────────────────────────────────────────

    def _loop(self) -> None:
        # 시작 직후 30초 대기(시스템 안정화) 후 즉시 1회 실행
        # → 5분 기다리지 않고 바로 첫 항목 추가
        self._stop_event.wait(30)
        if not self._stop_event.is_set():
            try:
                self._run_once()
            except Exception as e:
                log.warning("LLMPulse 초기 실행 오류 (계속): %s", e)

        while not self._stop_event.is_set():
            self._stop_event.wait(self._interval)
            if self._stop_event.is_set():
                break
            try:
                self._run_once()
            except Exception as e:
                log.warning("LLMPulse 루프 오류 (계속): %s", e)

    def _run_once(self) -> None:
        now_hhmm = datetime.now().strftime("%H:%M")
        if now_hhmm < "09:00" or now_hhmm > "15:45":
            return
        if not self._api_key:
            log.warning("LLMPulse: API 키 없음 — 스킵")
            return

        ticks = self._fetch_recent_ticks(minutes=5)
        if len(ticks) < 3:
            # WS 단절·data_gate 차단 구간 폴백: lookback 15분으로 확장
            ticks = self._fetch_recent_ticks(minutes=15)
            if len(ticks) < 3:
                log.warning("LLMPulse: 틱 부족 (%d개, 15분 lookback) — 스킵", len(ticks))
                return
            log.info("LLMPulse: 5분 틱 부족 → 15분 lookback 사용 (%d개)", len(ticks))

        tick_summary = self._summarize(ticks)
        state = self._runtime.state

        # market_view에서 direction/reason 추출
        mv = state.get("market_view") or {}
        if isinstance(mv, str):
            try:
                mv = json.loads(mv)
            except Exception:
                mv = {}

        ctx_snap = {
            "bias":               state.get("bias", "NEUTRAL"),
            "entry_signal":       state.get("entry_signal", "WAIT"),
            "flow_score":         round(float(state.get("flow_score",    0.0)), 3),
            "foreign_buy":        tick_summary["foreign_buy"],   # 5분 delta (누적 총량 아님)
            "foreign_buy_total":  round(float(state.get("foreign_buy", 0.0))),  # 참고용 누적
            "basis":              round(float(state.get("basis",         0.0)), 3),
            "vwap":               round(float(state.get("vwap",          0.0)), 2),
            "futures_price":      round(float(state.get("futures_price", 0.0)), 2),
            "atr":                round(float(state.get("atr",           0.0)), 3),
            "long_score":         round(float(state.get("long_score",    0.0)), 2),
            "short_score":        round(float(state.get("short_score",   0.0)), 2),
            "regime":             state.get("flow_regime_label", "NEUTRAL"),
            "session_phase":      state.get("session_phase", ""),
            "spot_price":         round(float(state.get("spot_price",    0.0)), 2),
            "price_vs_vwap":      round(float(state.get("price_vs_vwap", 0.0)), 2),
            "mv_direction":       mv.get("direction", ""),
            "mv_reason":          mv.get("reason", ""),
        }

        with self._lock:
            prev = list(self._history)[-1] if self._history else None

        result = self._call_haiku(tick_summary, ctx_snap, prev)
        if result is None:
            log.warning("LLMPulse: LLM 호출 실패 — 이번 사이클 스킵")
            return

        ts      = datetime.now().strftime("%H:%M:%S")
        changed = (prev is None) or (result["direction"] != prev["direction"])

        snap = {
            "ts":           ts,
            "direction":    result["direction"],
            "confidence":   result["confidence"],
            "summary":      result["summary"],
            "key_factor":   result["key_factor"],
            "changed":      changed,
            "duration_min": None,
        }

        with self._lock:
            # 직전 스냅샷 duration_min 계산 및 DB 업데이트
            if self._history:
                prev_snap = self._history[-1]
                if prev_snap.get("duration_min") is None:
                    try:
                        prev_dt = datetime.strptime(
                            datetime.now().strftime("%Y-%m-%d") + " " + prev_snap["ts"],
                            "%Y-%m-%d %H:%M:%S",
                        )
                        elapsed = round((datetime.now() - prev_dt).total_seconds() / 60, 1)
                    except Exception:
                        elapsed = round(self._interval / 60, 1)
                    prev_snap["duration_min"] = elapsed
                    prev_row_id = prev_snap.get("row_id")
                    if prev_row_id:
                        self._db_update_duration(prev_row_id, elapsed)

            self._history.append(snap)

        # DB INSERT (동기 — 5분 간격이므로 블로킹 부담 없음)
        row_id = self._db_insert(snap, ctx_snap)
        if row_id is not None:
            with self._lock:
                for s in self._history:
                    if s is snap:
                        s["row_id"] = row_id
                        break

        # intraday.db에도 동시 기록 (휘발성 이력)
        self._save_intraday_log(snap, ctx_snap)

        log.info(
            "LLMPulse | %s dir=%-8s conf=%.2f changed=%s | %s",
            ts, result["direction"], result["confidence"],
            changed, result["key_factor"],
        )

    # ── DB 틱 조회 ─────────────────────────────────────────────────

    def _fetch_recent_ticks(self, minutes: int = 5) -> List[Any]:
        cutoff = (datetime.now() - timedelta(minutes=minutes)).strftime("%Y-%m-%d %H:%M:%S")
        today  = datetime.now().strftime("%Y-%m-%d")
        try:
            conn = sqlite3.connect(
                f"file:{self._db_path}?mode=ro", uri=True, timeout=5,
            )
            conn.row_factory = sqlite3.Row
            rows = conn.execute(
                """SELECT ts, futures_price, bias, long_score, short_score,
                          volume_burst, flow_score, foreign_buy, basis
                   FROM regime_ticks
                   WHERE ts >= ?
                     AND substr(ts, 1, 10) = ?
                     AND futures_price > 100
                   ORDER BY ts""",
                (cutoff, today),
            ).fetchall()
            conn.close()
            return rows
        except Exception as e:
            log.warning("LLMPulse DB 틱 조회 실패: %s", e)
            return []

    # ── 틱 요약 ────────────────────────────────────────────────────

    def _summarize(self, ticks: List[Any]) -> Dict:
        prices      = [float(r["futures_price"]) for r in ticks]
        biases      = [r["bias"] for r in ticks if r["bias"]]
        flow_scores = [float(r["flow_score"]) for r in ticks if r["flow_score"] is not None]
        vol_bursts  = [float(r["volume_burst"]) for r in ticks if r["volume_burst"] is not None]
        bases       = [float(r["basis"]) for r in ticks if r["basis"] is not None]
        foreign     = [float(r["foreign_buy"]) for r in ticks if r["foreign_buy"] is not None]

        long_ratio  = biases.count("LONG")  / len(biases) if biases else 0.5
        short_ratio = biases.count("SHORT") / len(biases) if biases else 0.5
        avg_flow  = sum(flow_scores) / len(flow_scores) if flow_scores else 0.0
        avg_vol   = sum(vol_bursts)  / len(vol_bursts)  if vol_bursts  else 1.0
        avg_basis = sum(bases)       / len(bases)       if bases       else 0.0
        # 누적값 대신 5분 내 delta (첫→마지막): 오전 누적 편향 제거
        last_foreign = round(foreign[-1] - foreign[0], 0) if len(foreign) >= 2 else 0.0

        if len(flow_scores) >= 4:
            mid = len(flow_scores) // 2
            flow_trend = (
                "상승" if sum(flow_scores[mid:]) / len(flow_scores[mid:])
                          > sum(flow_scores[:mid]) / len(flow_scores[:mid])
                else "하락"
            )
        else:
            flow_trend = "데이터부족"

        price_chg = round(prices[-1] - prices[0], 2) if len(prices) >= 2 else 0.0

        return {
            "tick_count":   len(ticks),
            "price_change": price_chg,
            "long_ratio":   round(long_ratio, 2),
            "short_ratio":  round(short_ratio, 2),
            "avg_flow":     round(avg_flow, 3),
            "flow_trend":   flow_trend,
            "avg_vol":      round(avg_vol, 2),
            "vol_state":    "HIGH" if avg_vol >= 1.8 else ("LOW" if avg_vol <= 0.8 else "MID"),
            "avg_basis":    round(avg_basis, 3),
            "foreign_buy":  round(last_foreign),
        }

    # ── Haiku 호출 ─────────────────────────────────────────────────

    def _call_haiku(
        self,
        summary: Dict,
        ctx: Dict,
        prev: Optional[Dict],
    ) -> Optional[Dict]:

        prev_str = ""
        if prev:
            prev_str = (
                f"\n[직전 5분 평가({prev.get('ts', '')})] "
                f"{prev.get('direction')} / {prev.get('key_factor', '')}"
            )

        vwap_diff = ctx['futures_price'] - ctx['vwap']
        user_prompt = (
            f"현재 시각: {datetime.now().strftime('%H:%M')} | 장세: {ctx['session_phase']}\n"
            f"\n[최근 5분 틱 통계]\n"
            f"- 틱수: {summary['tick_count']}, 가격변동: {summary['price_change']:+.1f}pt\n"
            f"- LONG비율: {summary['long_ratio']:.0%} / SHORT비율: {summary['short_ratio']:.0%}\n"
            f"- flow_score: {summary['avg_flow']:+.3f} (추이: {summary['flow_trend']})\n"
            f"- 변동성: {summary['avg_vol']:.2f} [{summary['vol_state']}]\n"
            f"- 베이시스: {summary['avg_basis']:+.3f}\n"
            f"- 외국인 5분 delta: {summary['foreign_buy']:+,} (오전 누적 총량 무관·최근 5분 변화)\n"
            f"\n[현재 엔진 상태]\n"
            f"- 편향: {ctx['bias']} | 레짐: {ctx['regime']} | 진입신호: {ctx['entry_signal']}\n"
            f"- long_score: {ctx['long_score']} / short_score: {ctx['short_score']}\n"
            f"- flow_score: {ctx['flow_score']:+.3f}\n"
            f"- 현물가: {ctx['spot_price']:.2f} | 선물가: {ctx['futures_price']:.2f}\n"
            f"- VWAP: {ctx['vwap']:.2f} (현물가 대비 {vwap_diff:+.2f}pt)\n"
            f"- ATR: {ctx['atr']:.3f}\n"
            + (f"- 3분 장세 판단: {ctx['mv_direction']} — {ctx['mv_reason'][:60]}\n" if ctx['mv_direction'] else "")
            + f"{prev_str}"
        )

        try:
            resp = requests.post(
                _HAIKU_URL,
                headers={
                    "x-api-key":         self._api_key,
                    "anthropic-version": "2023-06-01",
                    "content-type":      "application/json",
                },
                json={
                    "model":      _HAIKU_MODEL,
                    "max_tokens": _MAX_TOKENS,
                    "system":     _SYSTEM_PROMPT,
                    "messages":   [{"role": "user", "content": user_prompt}],
                },
                timeout=_HAIKU_TIMEOUT,
            )
            resp.raise_for_status()
            raw = resp.json()["content"][0]["text"].strip()
        except Exception as e:
            log.warning("LLMPulse Haiku HTTP 오류: %s", e)
            return None

        if raw.startswith("```"):
            lines = raw.split("\n")
            raw = "\n".join(lines[1:-1] if lines[-1].strip() == "```" else lines[1:])

        try:
            parsed = json.loads(raw)
        except (json.JSONDecodeError, ValueError) as e:
            log.warning("LLMPulse JSON 파싱 실패: %s | raw=%s", e, raw[:200])
            return None

        direction  = str(parsed.get("direction",  "")).strip().upper()
        confidence = parsed.get("confidence", 0.0)
        summary_t  = str(parsed.get("summary",    "")).strip()
        key_factor = str(parsed.get("key_factor", "")).strip()

        if direction not in VALID_DIRECTIONS:
            log.warning("LLMPulse 유효하지 않은 direction=%s", direction)
            return None
        try:
            confidence = max(0.0, min(1.0, float(confidence)))
        except (TypeError, ValueError):
            confidence = 0.5

        return {
            "direction":  direction,
            "confidence": round(confidence, 2),
            "summary":    summary_t[:300],
            "key_factor": key_factor[:100],
        }

    # ── 외부 API — 오늘 데이터 ────────────────────────────────────

    def get_data(self) -> Dict:
        """오늘 이력 + 통계 반환 (/api/llm_pulse)."""
        with self._lock:
            history = [
                {k: v for k, v in s.items() if k != "row_id"}
                for s in self._history
            ]
        stats = self._compute_stats(history)
        current = history[-1] if history else {}
        return {
            "active":       bool(self._api_key and self._thread and self._thread.is_alive()),
            "interval_sec": self._interval,
            "current":      current,
            "history":      history,
            "stats":        stats,
        }

    # ── 외부 API — 다일간 심층 분석 ──────────────────────────────

    def get_analysis(self, days: int = 30) -> Dict:
        """
        최근 N일 pulse_history 심층 분석 (/api/llm_pulse/analysis?days=N).

        반환:
          daily      : 날짜별 요약 [{date, total, changes, stability_pct,
                                     bullish_pct, neutral_pct, bearish_pct,
                                     avg_dur_bullish, avg_dur_neutral, avg_dur_bearish}]
          overall    : 전체 집계 {total_days, total_snaps, avg_change_per_day,
                                  direction_dist, avg_duration, longest_run}
          hourly     : 시간대별 방향 분포 [{hour, bullish, neutral, bearish, total}]
          transitions: 방향 전환 패턴 [{from_dir, to_dir, count}]
        """
        since = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d")
        try:
            conn = sqlite3.connect(self._db_path, timeout=10)
            conn.row_factory = sqlite3.Row

            # ① 원시 데이터
            rows = conn.execute(
                """SELECT date, ts, direction, confidence, changed, duration_min,
                          flow_score, foreign_buy
                   FROM pulse_history
                   WHERE date >= ?
                   ORDER BY datetime_full""",
                (since,),
            ).fetchall()

            # ② 날짜별 집계
            daily_map: Dict[str, Dict] = {}
            for r in rows:
                d = r["date"]
                if d not in daily_map:
                    daily_map[d] = {
                        "date": d, "total": 0, "changes": 0,
                        "bullish": 0, "neutral": 0, "bearish": 0,
                        "dur_b": [], "dur_n": [], "dur_be": [],
                    }
                dm = daily_map[d]
                dm["total"] += 1
                if r["changed"]:
                    dm["changes"] += 1
                dir_ = r["direction"]
                if dir_ == "BULLISH":
                    dm["bullish"] += 1
                    if r["duration_min"] is not None:
                        dm["dur_b"].append(float(r["duration_min"]))
                elif dir_ == "NEUTRAL":
                    dm["neutral"] += 1
                    if r["duration_min"] is not None:
                        dm["dur_n"].append(float(r["duration_min"]))
                elif dir_ == "BEARISH":
                    dm["bearish"] += 1
                    if r["duration_min"] is not None:
                        dm["dur_be"].append(float(r["duration_min"]))

            daily = []
            for d, dm in sorted(daily_map.items()):
                t = max(dm["total"], 1)
                daily.append({
                    "date":            d,
                    "total":           dm["total"],
                    "changes":         dm["changes"],
                    "stability_pct":   round((1 - dm["changes"] / max(dm["total"] - 1, 1)) * 100)
                                        if dm["total"] > 1 else 100,
                    "bullish_pct":     round(dm["bullish"] / t * 100),
                    "neutral_pct":     round(dm["neutral"] / t * 100),
                    "bearish_pct":     round(dm["bearish"] / t * 100),
                    "avg_dur_bullish": round(sum(dm["dur_b"])  / len(dm["dur_b"]),  1) if dm["dur_b"]  else 0.0,
                    "avg_dur_neutral": round(sum(dm["dur_n"])  / len(dm["dur_n"]),  1) if dm["dur_n"]  else 0.0,
                    "avg_dur_bearish": round(sum(dm["dur_be"]) / len(dm["dur_be"]), 1) if dm["dur_be"] else 0.0,
                })

            # ③ 전체 집계
            total_snaps = len(rows)
            total_days  = len(daily_map)
            total_b  = sum(1 for r in rows if r["direction"] == "BULLISH")
            total_n  = sum(1 for r in rows if r["direction"] == "NEUTRAL")
            total_be = sum(1 for r in rows if r["direction"] == "BEARISH")
            total_changes = sum(1 for r in rows if r["changed"])

            all_dur: Dict[str, List[float]] = {"BULLISH": [], "NEUTRAL": [], "BEARISH": []}
            for r in rows:
                if r["duration_min"] is not None and r["direction"] in all_dur:
                    all_dur[r["direction"]].append(float(r["duration_min"]))

            # 연속 최장 실행 (Longest consecutive run)
            longest_run = {"direction": None, "count": 0, "date": None}
            cur_dir, cur_cnt, cur_date = None, 0, None
            for r in rows:
                if r["direction"] == cur_dir:
                    cur_cnt += 1
                else:
                    cur_dir, cur_cnt, cur_date = r["direction"], 1, r["date"]
                if cur_cnt > longest_run["count"]:
                    longest_run = {"direction": cur_dir, "count": cur_cnt, "date": cur_date}

            overall = {
                "total_days":         total_days,
                "total_snaps":        total_snaps,
                "avg_change_per_day": round(total_changes / max(total_days, 1), 1),
                "direction_dist": {
                    "BULLISH": round(total_b  / max(total_snaps, 1) * 100),
                    "NEUTRAL": round(total_n  / max(total_snaps, 1) * 100),
                    "BEARISH": round(total_be / max(total_snaps, 1) * 100),
                },
                "avg_duration": {
                    k: round(sum(v) / len(v), 1) if v else 0.0
                    for k, v in all_dur.items()
                },
                "longest_run": longest_run,
            }

            # ④ 시간대별 분포 (시간 단위)
            hourly_map: Dict[int, Dict] = {}
            for r in rows:
                try:
                    h = int(r["ts"][:2])
                except Exception:
                    continue
                if h not in hourly_map:
                    hourly_map[h] = {"hour": h, "bullish": 0, "neutral": 0, "bearish": 0, "total": 0}
                hm = hourly_map[h]
                hm["total"] += 1
                dir_ = r["direction"]
                if dir_ == "BULLISH":   hm["bullish"] += 1
                elif dir_ == "NEUTRAL": hm["neutral"] += 1
                elif dir_ == "BEARISH": hm["bearish"] += 1

            hourly = [
                {**v,
                 "bullish_pct": round(v["bullish"] / max(v["total"], 1) * 100),
                 "bearish_pct": round(v["bearish"] / max(v["total"], 1) * 100)}
                for v in sorted(hourly_map.values(), key=lambda x: x["hour"])
            ]

            # ⑤ 방향 전환 패턴
            trans_map: Dict[str, int] = {}
            for i in range(1, len(rows)):
                if rows[i]["changed"]:
                    key = f"{rows[i-1]['direction']}→{rows[i]['direction']}"
                    trans_map[key] = trans_map.get(key, 0) + 1

            transitions = [
                {"from_dir": k.split("→")[0], "to_dir": k.split("→")[1], "count": v}
                for k, v in sorted(trans_map.items(), key=lambda x: -x[1])
            ]

            conn.close()
            return {
                "ok":          True,
                "days":        days,
                "since":       since,
                "daily":       daily,
                "overall":     overall,
                "hourly":      hourly,
                "transitions": transitions,
            }

        except Exception as e:
            log.error("LLMPulse 심층 분석 실패: %s", e)
            return {"ok": False, "error": str(e)}

    # ── 통계 계산 (오늘용) ─────────────────────────────────────────

    def _compute_stats(self, history: List[Dict]) -> Dict:
        if not history:
            return {
                "change_count": 0,
                "avg_duration": {"BULLISH": 0.0, "NEUTRAL": 0.0, "BEARISH": 0.0},
                "current_direction": None,
                "current_duration_min": 0.0,
                "stability_pct": 100,
                "total_snapshots": 0,
            }

        change_count = sum(
            1 for i, s in enumerate(history)
            if i > 0 and s.get("changed")
        )
        stability_pct = (
            round((1 - change_count / max(len(history) - 1, 1)) * 100)
            if len(history) > 1 else 100
        )

        dur_buckets: Dict[str, List[float]] = {"BULLISH": [], "NEUTRAL": [], "BEARISH": []}
        for s in history:
            d  = s.get("direction")
            dm = s.get("duration_min")
            if d in dur_buckets and dm is not None:
                dur_buckets[d].append(float(dm))

        avg_duration = {
            k: round(sum(v) / len(v), 1) if v else 0.0
            for k, v in dur_buckets.items()
        }

        latest = history[-1]
        current_dir = latest.get("direction")
        try:
            latest_ts = datetime.strptime(
                datetime.now().strftime("%Y-%m-%d") + " " + latest["ts"],
                "%Y-%m-%d %H:%M:%S",
            )
            current_duration_min = round(
                (datetime.now() - latest_ts).total_seconds() / 60, 1
            )
        except Exception:
            current_duration_min = 0.0

        return {
            "change_count":         change_count,
            "avg_duration":         avg_duration,
            "current_direction":    current_dir,
            "current_duration_min": current_duration_min,
            "stability_pct":        stability_pct,
            "total_snapshots":      len(history),
        }
