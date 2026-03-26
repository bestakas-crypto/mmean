# [Directory] MMEAN
# [File] sim_report.py
"""
SimReport — 시뮬레이션 결과 분석 리포트

사용법:
  python sim_report.py [--db <프로젝트루트>/storage/mmean.db] [--period all|day|month]

출력:
  - 전체 / 일별 / 월별 손익 요약
  - 최대 낙폭 (MDD)
  - 승률 / 평균 RR / 거래 수
  - 손익곡선 (터미널 ASCII)
"""
from __future__ import annotations

import argparse
import sqlite3
from datetime import datetime
from pathlib import Path
from typing import List, Tuple

_DEFAULT_MMEAN_DB = str(Path(__file__).resolve().parent.parent / "storage" / "mmean.db")


# ─── 선물 스펙 ────────────────────────────────────────────────────
POINT_VALUE = 250_000
TICK_UNIT   = 0.05
TICK_VALUE  = 12_500


# ─── DB 로드 ─────────────────────────────────────────────────────
def load_trades(db_path: str) -> List[dict]:
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    rows = conn.execute("""
        SELECT * FROM sim_trades ORDER BY exit_time ASC
    """).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def load_equity(db_path: str, limit: int = 2000) -> List[dict]:
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    rows = conn.execute("""
        SELECT ts, total_equity, equity, open_pnl, bias, has_position
        FROM sim_equity ORDER BY id DESC LIMIT ?
    """, (limit,)).fetchall()
    conn.close()
    return list(reversed([dict(r) for r in rows]))


# ─── 통계 계산 ────────────────────────────────────────────────────
def calc_mdd(equity_series: List[float]) -> Tuple[float, float]:
    """(MDD 금액, MDD%) 반환."""
    peak = equity_series[0] if equity_series else 0.0
    mdd  = 0.0
    for v in equity_series:
        if v > peak:
            peak = v
        dd = peak - v
        if dd > mdd:
            mdd = dd
    pct = (mdd / peak * 100) if peak > 0 else 0.0
    return round(mdd, 0), round(pct, 2)


def calc_stats(trades: List[dict]) -> dict:
    if not trades:
        return {}
    pnls   = [t["pnl"] for t in trades]
    wins   = [p for p in pnls if p > 0]
    losses = [p for p in pnls if p < 0]
    total  = sum(pnls)
    n      = len(pnls)
    win_rate = len(wins) / n * 100 if n else 0

    avg_win  = sum(wins)  / len(wins)  if wins   else 0
    avg_loss = sum(losses)/ len(losses)if losses else 0
    rr       = abs(avg_win / avg_loss) if avg_loss else float("inf")

    equity   = [t["cum_equity"] for t in trades]
    mdd, mdd_pct = calc_mdd(equity)

    # 최장 연속 손실
    max_consec_loss = cur_loss = 0
    for p in pnls:
        if p < 0:
            cur_loss += 1
            max_consec_loss = max(max_consec_loss, cur_loss)
        else:
            cur_loss = 0

    return {
        "trades":           n,
        "total_pnl":        round(total, 0),
        "win_rate":         round(win_rate, 1),
        "avg_win":          round(avg_win, 0),
        "avg_loss":         round(avg_loss, 0),
        "rr":               round(rr, 2),
        "mdd":              mdd,
        "mdd_pct":          mdd_pct,
        "max_consec_loss":  max_consec_loss,
        "profit_factor":    round(sum(wins) / abs(sum(losses)), 2) if losses else float("inf"),
    }


def group_by_day(trades: List[dict]) -> dict:
    grouped: dict = {}
    for t in trades:
        day = t["exit_time"][:10]
        grouped.setdefault(day, []).append(t)
    return grouped


def group_by_month(trades: List[dict]) -> dict:
    grouped: dict = {}
    for t in trades:
        mo = t["exit_time"][:7]
        grouped.setdefault(mo, []).append(t)
    return grouped


# ─── 출력 헬퍼 ────────────────────────────────────────────────────
def fmt_krw(v: float) -> str:
    sign = "+" if v >= 0 else ""
    return f"{sign}{v:,.0f}원"


def print_header(title: str) -> None:
    print()
    print("━" * 62)
    print(f"  {title}")
    print("━" * 62)


def print_stats(stats: dict, label: str = "") -> None:
    if not stats:
        print("  거래 없음")
        return
    if label:
        print(f"\n  [{label}]")
    print(f"  거래 수        : {stats['trades']}건")
    print(f"  누적 손익      : {fmt_krw(stats['total_pnl'])}")
    print(f"  승률           : {stats['win_rate']:.1f}%")
    print(f"  평균 수익      : {fmt_krw(stats['avg_win'])}")
    print(f"  평균 손실      : {fmt_krw(stats['avg_loss'])}")
    print(f"  손익비(RR)     : {stats['rr']:.2f}")
    print(f"  Profit Factor  : {stats['profit_factor']:.2f}")
    print(f"  MDD            : {fmt_krw(stats['mdd'])}  ({stats['mdd_pct']:.2f}%)")
    print(f"  최장 연속손실  : {stats['max_consec_loss']}건")


def ascii_curve(equity_series: List[float], width: int = 55,
                height: int = 10) -> str:
    """터미널 ASCII 손익곡선."""
    if len(equity_series) < 2:
        return "  (데이터 부족)"
    lo, hi = min(equity_series), max(equity_series)
    span   = hi - lo or 1
    lines  = []
    for row in range(height, -1, -1):
        thresh = lo + span * row / height
        line   = ""
        step   = max(1, len(equity_series) // width)
        for i in range(0, len(equity_series), step):
            v = equity_series[i]
            if abs(v - thresh) < span / height:
                line += "·" if v >= 0 else "×"
            elif v >= thresh:
                line += "│" if i > 0 and equity_series[max(0,i-step)] < thresh else " "
            else:
                line += " "
        label = f"{thresh/1_000_000:+.1f}M" if abs(thresh) >= 1_000_000 \
                else f"{thresh/1_000:+.0f}K"
        lines.append(f"  {label:>7} │{line}")
    lines.append("  " + " " * 8 + "└" + "─" * width)
    return "\n".join(lines)


def print_exit_reason_summary(trades: List[dict]) -> None:
    reasons: dict = {}
    for t in trades:
        r = t["exit_reason"]
        reasons[r] = reasons.get(r, 0) + 1
    print("\n  [청산 사유 분포]")
    for r, cnt in sorted(reasons.items(), key=lambda x: -x[1]):
        bar = "█" * min(30, cnt)
        print(f"  {r:<22} {cnt:>4}건  {bar}")

    # 트레일링 활성/비활성 비교
    trail_on  = [t for t in trades if t.get("trailing_active")]
    trail_off = [t for t in trades if not t.get("trailing_active")]
    if trail_on or trail_off:
        print("\n  [트레일링 활성 여부별 PnL 비교]")
        for label, group in [("트레일링 활성", trail_on), ("트레일링 비활성", trail_off)]:
            if not group:
                continue
            pnls    = [t["pnl"] for t in group]
            avg_pnl = sum(pnls) / len(pnls)
            wins    = sum(1 for p in pnls if p > 0)
            # max_favorable_pt 평균
            avg_fav = sum(t.get("max_favorable_pt", 0) for t in group) / len(group)
            print(f"  {label:<16} {len(group):>4}건 | "
                  f"평균PnL {avg_pnl:+,.0f}원 | "
                  f"승률 {wins/len(group)*100:.0f}% | "
                  f"평균최대유리이동 {avg_fav:.2f}pt")



def print_daily(trades: List[dict]) -> None:
    grouped = group_by_day(trades)
    print("\n  [일별 손익]")
    print(f"  {'날짜':<12} {'거래':>5} {'손익':>14} {'승률':>7} {'MDD':>12}")
    print("  " + "-" * 55)
    for day in sorted(grouped):
        ts = grouped[day]
        st = calc_stats(ts)
        eq = [t["cum_equity"] for t in ts]
        mdd, _ = calc_mdd(eq)
        print(f"  {day:<12} {st['trades']:>5}건 "
              f"{fmt_krw(st['total_pnl']):>14} "
              f"{st['win_rate']:>6.1f}% "
              f"{fmt_krw(-mdd):>12}")


def print_monthly(trades: List[dict]) -> None:
    grouped = group_by_month(trades)
    print("\n  [월별 손익]")
    print(f"  {'월':>8} {'거래':>5} {'손익':>14} {'승률':>7} {'MDD':>12}")
    print("  " + "-" * 55)
    for mo in sorted(grouped):
        ts = grouped[mo]
        st = calc_stats(ts)
        eq = [t["cum_equity"] for t in ts]
        mdd, _ = calc_mdd(eq)
        print(f"  {mo:>8}  {st['trades']:>5}건 "
              f"{fmt_krw(st['total_pnl']):>14} "
              f"{st['win_rate']:>6.1f}% "
              f"{fmt_krw(-mdd):>12}")


def print_recent_trades(trades: List[dict], n: int = 15) -> None:
    print(f"\n  [최근 거래 {n}건]")
    print(f"  {'진입시각':<22} {'방향':<6} {'진입가':>8} {'청산가':>8} "
          f"{'최대유리':>8} {'PnL':>12} {'T':>2} {'사유'}")
    print("  " + "-" * 85)
    for t in trades[-n:]:
        side    = "LONG " if t["direction"] == 1 else "SHORT"
        trail   = "✓" if t.get("trailing_active") else " "
        max_fav = t.get("max_favorable_pt", 0) or 0
        print(f"  {t['entry_time']:<22} {side} "
              f"{t['entry_price']:>8.2f} {t['exit_price']:>8.2f} "
              f"{max_fav:>+8.2f}pt "
              f"{fmt_krw(t['pnl']):>12} {trail:>2}  {t['exit_reason']}")


# ─── 메인 ────────────────────────────────────────────────────────
def run_report(db_path: str) -> None:
    trades = load_trades(db_path)
    equity_rows = load_equity(db_path, limit=5000)

    if not trades:
        print("sim_trades 데이터 없음. 시뮬레이션 엔진이 아직 거래를 기록하지 않았습니다.")
        return

    # ── 전체 통계 ──
    print_header("MMEAN 시뮬레이션 리포트 — 전체")
    stats = calc_stats(trades)
    print_stats(stats)

    # ── ASCII 손익곡선 ──
    print_header("손익곡선 (총 손익 기준)")
    eq_series = [t["cum_equity"] for t in trades]
    print(ascii_curve(eq_series))

    # ── 청산 사유 ──
    print_header("청산 사유 분포")
    print_exit_reason_summary(trades)

    # ── 일별 ──
    print_header("일별 요약")
    print_daily(trades)

    # ── 월별 ──
    if len(group_by_day(trades)) > 20:
        print_header("월별 요약")
        print_monthly(trades)

    # ── 최근 거래 ──
    print_header("최근 거래 내역")
    print_recent_trades(trades)

    # ── open PnL 실시간 상태 ──
    if equity_rows:
        last = equity_rows[-1]
        print_header("현재 포지션 상태")
        has_pos = bool(last.get("has_position"))
        print(f"  포지션 보유    : {'YES' if has_pos else 'NO'}")
        print(f"  실현 누적 손익 : {fmt_krw(last['equity'])}")
        print(f"  미실현 손익    : {fmt_krw(last['open_pnl'])}")
        print(f"  총 평가 손익   : {fmt_krw(last['total_equity'])}")
        print(f"  현재 bias      : {last['bias']}")

    print()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="MMEAN 시뮬레이션 리포트")
    parser.add_argument("--db",     default=_DEFAULT_MMEAN_DB,
                        help="mmean.db 경로")
    parser.add_argument("--watch",  action="store_true",
                        help="10초마다 자동 갱신")
    args = parser.parse_args()

    if args.watch:
        import time
        while True:
            print("\033[2J\033[H", end="")   # 터미널 클리어
            run_report(args.db)
            print(f"  [자동갱신] 10초 후 새로고침...  {datetime.now():%H:%M:%S}")
            time.sleep(10)
    else:
        run_report(args.db)
