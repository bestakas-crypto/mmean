"""
day_pattern.py — 일별 장세 패턴 자동 분류기

기능:
  - regime_ticks 데이터를 분석해 하루의 장세 패턴을 자동 분류
  - 결과를 mmean.db의 session_patterns 테이블에 저장
  - 누락된 날짜만 자동 처리 (이미 분류된 날은 건너뜀)

패턴 종류:
  TREND_UP      : 하루종일 우상향
  TREND_DOWN    : 하루종일 우하향
  REVERSAL_UP   : 하락 후 반전 상승
  REVERSAL_DOWN : 상승 후 반전 하락
  V_SHAPE       : 급락 후 급반등
  MORNING_SPIKE : 장초 급등/급락 후 횡보
  CLOSE_RUSH    : 장후반 방향성 급변
  RANGE_TIGHT   : 좁은 박스권 (범위 < 10틱)
  RANGE_WIDE    : 넓은 박스권 (방향 없는 혼조)
  CHOPPY        : 방향 없는 혼조

실행:
  python day_pattern.py                     # 누락 날짜만 자동 생성
  python day_pattern.py --rebuild           # 전체 재생성
  python day_pattern.py --date 2026-03-18   # 특정 날짜만
  python day_pattern.py --show              # 저장된 패턴 출력
  python day_pattern.py --show --date 2026-03-18
"""

from __future__ import annotations

import argparse
import json
import logging
import sqlite3
import sys
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Tuple

_STORAGE = Path(__file__).resolve().parent / "storage"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
log = logging.getLogger("DayPattern")

MMEAN_DB = str(_STORAGE / "mmean.db")

# ── 시간대 구분 (데이장 기준) ──────────────────────────────────────────
SEGMENTS = {
    "morning":   ("08:44", "10:30"),
    "midday":    ("10:30", "13:00"),
    "afternoon": ("13:00", "15:45"),
}

# ── 패턴 분류 임계값 ──────────────────────────────────────────────────
T = {
    "trend_min_change":   8.0,   # 추세 인정 최소 가격변동 (틱)
    "range_tight_max":    8.0,   # 박스권(좁음) 최대 일중 범위
    "range_wide_min":    15.0,   # 박스권(넓음) 최소 일중 범위
    "spike_ratio":        0.50,  # 스파이크: 전체 변동의 50% 이상이 한 구간에서
    "reversal_min":       5.0,   # 반전 인정 최소 변동
    "close_rush_min":     6.0,   # 장후반 급변 최소 변동
    "bias_long_min":      0.55,  # LONG bias 비율 최소 (추세 인정)
    "bias_short_min":     0.55,
    "vol_high":           1.80,  # 고변동성 volume_burst 평균
    "vol_low":            0.80,
}


def _connect(db_path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path, timeout=30)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.row_factory = sqlite3.Row
    return conn


def _ensure_table(conn: sqlite3.Connection) -> None:
    conn.executescript("""
    CREATE TABLE IF NOT EXISTS session_patterns (
        id              INTEGER PRIMARY KEY AUTOINCREMENT,
        date            TEXT NOT NULL,
        session_mode    TEXT NOT NULL DEFAULT 'day',
        pattern_type    TEXT NOT NULL,
        sub_pattern     TEXT,
        price_open      REAL,
        price_close     REAL,
        price_high      REAL,
        price_low       REAL,
        price_change    REAL,
        range_ticks     REAL,
        volatility      TEXT,
        foreign_flow    TEXT,
        bias_long_ratio REAL,
        bias_short_ratio REAL,
        seg_morning_chg REAL,
        seg_midday_chg  REAL,
        seg_afternoon_chg REAL,
        auto_tags       TEXT,
        user_note       TEXT,
        tick_count      INTEGER,
        created_at      TEXT,
        UNIQUE(date, session_mode)
    );
    CREATE INDEX IF NOT EXISTS idx_session_patterns_date
        ON session_patterns(date);
    CREATE INDEX IF NOT EXISTS idx_session_patterns_pattern
        ON session_patterns(pattern_type);
    """)
    conn.commit()


def _load_ticks(conn: sqlite3.Connection, date: str,
                session_mode: str = "day") -> List[sqlite3.Row]:
    if session_mode == "day":
        time_start, time_end = "08:44:00", "15:46:00"
    else:
        time_start, time_end = "18:00:00", "23:59:59"

    rows = conn.execute("""
        SELECT ts, futures_price, bias, long_score, short_score,
               volume_burst, foreign_buy, flow_score
        FROM regime_ticks
        WHERE substr(ts, 1, 10) = ?
          AND substr(ts, 12, 8) >= ?
          AND substr(ts, 12, 8) <= ?
          AND futures_price > 100
        ORDER BY ts
    """, (date, time_start, time_end)).fetchall()
    return rows


def _segment_prices(ticks: List[sqlite3.Row]) -> Dict[str, List[float]]:
    """시간대별 가격 분리"""
    segs: Dict[str, List[float]] = {k: [] for k in SEGMENTS}
    for t in ticks:
        hhmm = t["ts"][11:16]
        for seg, (s, e) in SEGMENTS.items():
            if s <= hhmm < e:
                segs[seg].append(float(t["futures_price"]))
                break
    return segs


def _seg_change(prices: List[float]) -> float:
    """구간 가격 변동 (시작→끝)"""
    if len(prices) < 2:
        return 0.0
    return round(prices[-1] - prices[0], 2)


def _classify_pattern(ticks: List[sqlite3.Row]) -> Dict:
    """패턴 분류 메인 로직"""
    if len(ticks) < 10:
        return {"pattern_type": "UNKNOWN", "sub_pattern": "데이터부족"}

    prices = [float(t["futures_price"]) for t in ticks]
    p_open  = prices[0]
    p_close = prices[-1]
    p_high  = max(prices)
    p_low   = min(prices)
    p_change = round(p_close - p_open, 2)
    p_range  = round(p_high - p_low, 2)

    # 시간대별 변동
    segs = _segment_prices(ticks)
    seg_chg = {k: _seg_change(v) for k, v in segs.items()}
    morning_chg   = seg_chg["morning"]
    midday_chg    = seg_chg["midday"]
    afternoon_chg = seg_chg["afternoon"]

    # bias 분포
    biases = [t["bias"] for t in ticks]
    long_r  = biases.count("LONG")  / len(biases)
    short_r = biases.count("SHORT") / len(biases)

    # 변동성
    vols = [float(t["volume_burst"] or 0) for t in ticks]
    avg_vol = sum(vols) / len(vols) if vols else 0.0
    if avg_vol >= T["vol_high"]:
        volatility = "HIGH"
    elif avg_vol <= T["vol_low"]:
        volatility = "LOW"
    else:
        volatility = "MID"

    # 외국인 흐름
    fb_vals = [float(t["foreign_buy"] or 0) for t in ticks]
    avg_fb = sum(fb_vals) / len(fb_vals) if fb_vals else 0.0
    if avg_fb > 30:
        foreign_flow = "BUY"
    elif avg_fb < -30:
        foreign_flow = "SELL"
    else:
        foreign_flow = "NEUTRAL"

    # ── 패턴 분류 규칙 ─────────────────────────────────────────────
    pattern = "CHOPPY"
    sub = None

    # 1. 좁은 박스권
    if p_range <= T["range_tight_max"]:
        pattern = "RANGE_TIGHT"

    # 2. 추세 (하루종일)
    elif abs(p_change) >= T["trend_min_change"]:
        all_same_dir = (
            (morning_chg > 0 and midday_chg > -3 and afternoon_chg > -3) or
            (morning_chg < 0 and midday_chg < 3  and afternoon_chg < 3)
        )
        if p_change > 0 and all_same_dir:
            pattern = "TREND_UP"
            if long_r >= T["bias_long_min"]:
                sub = "STRONG"
        elif p_change < 0 and all_same_dir:
            pattern = "TREND_DOWN"
            if short_r >= T["bias_short_min"]:
                sub = "STRONG"

        # 반전 패턴
        elif morning_chg < -T["reversal_min"] and afternoon_chg > T["reversal_min"]:
            pattern = "REVERSAL_UP"
        elif morning_chg > T["reversal_min"] and afternoon_chg < -T["reversal_min"]:
            pattern = "REVERSAL_DOWN"

        # V자
        elif (morning_chg < -T["reversal_min"] and
              midday_chg > T["reversal_min"] and
              abs(morning_chg) >= T["reversal_min"] * 1.5):
            pattern = "V_SHAPE"

        # 장후반 급변
        elif abs(afternoon_chg) >= T["close_rush_min"] and abs(p_change) < T["trend_min_change"]:
            pattern = "CLOSE_RUSH"
            sub = "UP" if afternoon_chg > 0 else "DOWN"

        # 넓은 박스권
        else:
            pattern = "RANGE_WIDE"

    # 3. 장초 스파이크
    if pattern in ("CHOPPY", "RANGE_WIDE"):
        if (len(segs["morning"]) > 0 and
                abs(morning_chg) >= T["reversal_min"] and
                abs(morning_chg) / (p_range + 0.01) >= T["spike_ratio"]):
            pattern = "MORNING_SPIKE"
            sub = "UP" if morning_chg > 0 else "DOWN"

    # auto_tags 생성
    auto_tags = []
    if p_change > 5:   auto_tags.append("DAY_POSITIVE")
    elif p_change < -5: auto_tags.append("DAY_NEGATIVE")
    else:               auto_tags.append("DAY_FLAT")
    if volatility == "HIGH":  auto_tags.append("VOL_HIGH")
    if volatility == "LOW":   auto_tags.append("VOL_LOW")
    if foreign_flow == "BUY":  auto_tags.append("FOREIGN_BUY")
    if foreign_flow == "SELL": auto_tags.append("FOREIGN_SELL")
    if long_r >= T["bias_long_min"]:   auto_tags.append("BIAS_BULL")
    if short_r >= T["bias_short_min"]: auto_tags.append("BIAS_BEAR")

    return {
        "pattern_type":     pattern,
        "sub_pattern":      sub,
        "price_open":       round(p_open, 2),
        "price_close":      round(p_close, 2),
        "price_high":       round(p_high, 2),
        "price_low":        round(p_low, 2),
        "price_change":     p_change,
        "range_ticks":      p_range,
        "volatility":       volatility,
        "foreign_flow":     foreign_flow,
        "bias_long_ratio":  round(long_r, 3),
        "bias_short_ratio": round(short_r, 3),
        "seg_morning_chg":  morning_chg,
        "seg_midday_chg":   midday_chg,
        "seg_afternoon_chg": afternoon_chg,
        "auto_tags":        json.dumps(auto_tags, ensure_ascii=False),
        "tick_count":       len(ticks),
    }


def _get_available_dates(conn: sqlite3.Connection,
                          session_mode: str = "day") -> List[str]:
    """regime_ticks에서 유효한 날짜 목록 반환"""
    if session_mode == "day":
        time_cond = "substr(ts,12,5) >= '08:44' AND substr(ts,12,5) <= '15:45'"
    else:
        time_cond = "substr(ts,12,5) >= '18:00'"

    rows = conn.execute(f"""
        SELECT substr(ts,1,10) as date
        FROM regime_ticks
        WHERE {time_cond}
          AND futures_price > 100
        GROUP BY substr(ts,1,10)
        HAVING COUNT(*) >= 100
        ORDER BY date
    """).fetchall()
    return [r["date"] for r in rows]


def _get_done_dates(conn: sqlite3.Connection,
                    session_mode: str = "day") -> List[str]:
    rows = conn.execute(
        "SELECT date FROM session_patterns WHERE session_mode=?",
        (session_mode,)
    ).fetchall()
    return [r["date"] for r in rows]


def process_date(conn: sqlite3.Connection, date: str,
                 session_mode: str = "day",
                 user_note: Optional[str] = None) -> Optional[Dict]:
    ticks = _load_ticks(conn, date, session_mode)
    if len(ticks) < 10:
        log.warning("데이터 부족 | date=%s ticks=%d", date, len(ticks))
        return None

    result = _classify_pattern(ticks)
    result["date"]         = date
    result["session_mode"] = session_mode
    result["user_note"]    = user_note
    result["created_at"]   = datetime.now().strftime("%Y-%m-%dT%H:%M:%S")

    conn.execute("""
        INSERT OR REPLACE INTO session_patterns
        (date, session_mode, pattern_type, sub_pattern,
         price_open, price_close, price_high, price_low,
         price_change, range_ticks, volatility, foreign_flow,
         bias_long_ratio, bias_short_ratio,
         seg_morning_chg, seg_midday_chg, seg_afternoon_chg,
         auto_tags, user_note, tick_count, created_at)
        VALUES (?,?,?,?, ?,?,?,?, ?,?,?,?, ?,?, ?,?,?, ?,?,?,?)
    """, (
        result["date"], result["session_mode"],
        result["pattern_type"], result["sub_pattern"],
        result["price_open"], result["price_close"],
        result["price_high"], result["price_low"],
        result["price_change"], result["range_ticks"],
        result["volatility"], result["foreign_flow"],
        result["bias_long_ratio"], result["bias_short_ratio"],
        result["seg_morning_chg"], result["seg_midday_chg"],
        result["seg_afternoon_chg"],
        result["auto_tags"], result["user_note"],
        result["tick_count"], result["created_at"],
    ))
    conn.commit()
    return result


def show_patterns(conn: sqlite3.Connection,
                  date: Optional[str] = None) -> None:
    where = "WHERE date=?" if date else ""
    params = (date,) if date else ()
    rows = conn.execute(f"""
        SELECT date, session_mode, pattern_type, sub_pattern,
               price_change, range_ticks, volatility, foreign_flow,
               seg_morning_chg, seg_midday_chg, seg_afternoon_chg,
               auto_tags, user_note
        FROM session_patterns
        {where}
        ORDER BY date, session_mode
    """, params).fetchall()

    if not rows:
        print("저장된 패턴 없음")
        return

    print(f"\n{'='*70}")
    print(f"  Session Patterns ({len(rows)}건)")
    print(f"{'='*70}")
    for r in rows:
        sub = f"/{r['sub_pattern']}" if r['sub_pattern'] else ""
        tags = json.loads(r['auto_tags'] or "[]")
        print(f"\n  [{r['date']}] {r['session_mode'].upper()}")
        print(f"  패턴: {r['pattern_type']}{sub}")
        print(f"  가격변동: {r['price_change']:+.1f}틱 | 일중범위: {r['range_ticks']:.1f}틱")
        print(f"  구간: 오전{r['seg_morning_chg']:+.1f} / 점심{r['seg_midday_chg']:+.1f} / 오후{r['seg_afternoon_chg']:+.1f}")
        print(f"  변동성: {r['volatility']} | 외국인: {r['foreign_flow']}")
        print(f"  태그: {' '.join(tags)}")
        if r['user_note']:
            print(f"  메모: {r['user_note']}")
    print(f"\n{'='*70}\n")


def main() -> None:
    p = argparse.ArgumentParser(description="일별 장세 패턴 자동 분류")
    p.add_argument("--db",      default=MMEAN_DB)
    p.add_argument("--date",    help="특정 날짜 (YYYY-MM-DD)")
    p.add_argument("--rebuild", action="store_true", help="전체 재생성")
    p.add_argument("--show",    action="store_true", help="저장된 패턴 출력")
    p.add_argument("--note",    help="사용자 메모 (--date 와 함께)")
    p.add_argument("--session", default="day", choices=["day", "night"])
    args = p.parse_args()

    conn = _connect(args.db)
    _ensure_table(conn)

    if args.show:
        show_patterns(conn, args.date)
        return

    if args.date:
        result = process_date(conn, args.date, args.session, args.note)
        if result:
            sub = f"/{result['sub_pattern']}" if result['sub_pattern'] else ""
            log.info("완료 | %s → %s%s | 변동%+.1f틱 | %s",
                     args.date, result["pattern_type"], sub,
                     result["price_change"], result["auto_tags"])
        return

    # 전체 자동 처리
    available = _get_available_dates(conn, args.session)
    if not args.rebuild:
        done = set(_get_done_dates(conn, args.session))
        targets = [d for d in available if d not in done]
    else:
        # 오늘 제외 (장중 데이터는 불완전)
        today = datetime.now().strftime("%Y-%m-%d")
        targets = [d for d in available if d < today]
        log.info("rebuild 모드: 기존 데이터 삭제 후 재생성")
        conn.execute("DELETE FROM session_patterns WHERE session_mode=?",
                     (args.session,))
        conn.commit()

    # 오늘 날짜 제외 (장중은 패턴 미확정)
    today = datetime.now().strftime("%Y-%m-%d")
    targets = [d for d in targets if d < today]

    if not targets:
        log.info("처리할 날짜 없음 (모두 완료됨)")
        show_patterns(conn)
        return

    log.info("처리 대상: %d일 %s", len(targets), targets)
    for date in targets:
        result = process_date(conn, date, args.session)
        if result:
            sub = f"/{result['sub_pattern']}" if result['sub_pattern'] else ""
            log.info("  %s → %-15s | 변동%+5.1f틱 | 범위%5.1f틱 | %s",
                     date, f"{result['pattern_type']}{sub}",
                     result["price_change"], result["range_ticks"],
                     result["foreign_flow"])

    log.info("전체 완료: %d일 처리", len(targets))
    show_patterns(conn)
    conn.close()


if __name__ == "__main__":
    main()
