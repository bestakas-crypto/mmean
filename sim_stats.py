import sqlite3

conn = sqlite3.connect(r'C:\mmean\storage\sim.db', timeout=5)
c = conn.cursor()

print("=== sim.db 학습 현황 ===\n")

# 전체 run 수
c.execute("SELECT COUNT(*), run_type, status FROM sim_runs GROUP BY run_type, status ORDER BY run_type")
print("[1] run 유형별 실행 수:")
for r in c.fetchall():
    print(f"  {r[1]:15s} {r[2]:10s} {r[0]:>8,}건")

# 날짜별 run 수
c.execute("""
    SELECT source_dates, COUNT(*) as cnt, 
           AVG(objective_score) as avg_obj,
           MAX(objective_score) as max_obj,
           SUM(trade_count) as total_trades
    FROM sim_runs 
    WHERE status='done'
    GROUP BY source_dates 
    ORDER BY source_dates
""")
print("\n[2] 날짜별 탐색 현황:")
for r in c.fetchall():
    print(f"  {r[0]:12s}  run={r[1]:>7,}  avg_obj={r[2]:>7.2f}  max_obj={r[3]:>7.2f}  trades={r[4]:>9,}")

# 상위 10개
c.execute("""
    SELECT r.source_dates, r.run_type, r.objective_score, 
           s.win_rate, s.profit_factor, s.trade_count, s.max_drawdown
    FROM sim_runs r
    JOIN sim_run_summary s ON r.id = s.run_id
    WHERE r.status='done' AND r.objective_score IS NOT NULL
    ORDER BY r.objective_score DESC
    LIMIT 10
""")
print("\n[3] 상위 10개 run (objective 기준):")
print(f"  {'날짜':12s} {'유형':12s} {'obj':>8} {'승률':>6} {'PF':>6} {'매매':>6} {'MDD':>8}")
for r in c.fetchall():
    print(f"  {r[0]:12s} {r[1]:12s} {r[2]:>8.2f} {r[3]:>6.1%} {r[4]:>6.2f} {r[5]:>6} {r[6]:>8.2f}")

# 전체 체결 수
c.execute("SELECT COUNT(*), SUM(pnl_ticks), AVG(pnl_ticks) FROM sim_trades")
r = c.fetchone()
print(f"\n[4] sim_trades 전체: {r[0]:,}건 | 총손익={r[1]:,.1f}틱 | 평균={r[2]:.3f}틱")

# 날짜별 체결 수
c.execute("""
    SELECT session_date, COUNT(*), AVG(pnl_ticks), SUM(CASE WHEN pnl_ticks>0 THEN 1 ELSE 0 END)*1.0/COUNT(*) as wr
    FROM sim_trades
    GROUP BY session_date ORDER BY session_date
""")
print("\n[5] 날짜별 체결 현황:")
for r in c.fetchall():
    print(f"  {r[0]:12s}  체결={r[1]:>8,}건  avg_pnl={r[2]:>7.3f}  승률={r[3]:>5.1%}")

conn.close()
