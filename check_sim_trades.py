import sqlite3
conn = sqlite3.connect(r'C:\mmean\storage\mmean.db', timeout=5)
c = conn.cursor()

print("=== sim_trades 날짜별 체결 ===")
c.execute("""
    SELECT date(entry_time) as dt, COUNT(*) as cnt,
           SUM(pnl) as total_pnl, AVG(pnl) as avg_pnl,
           SUM(CASE WHEN pnl>0 THEN 1 ELSE 0 END) as wins
    FROM sim_trades
    GROUP BY date(entry_time)
    ORDER BY dt
""")
for r in c.fetchall():
    wr = r[4]/r[1]*100 if r[1] else 0
    print(f"  {r[0]}  체결={r[1]:>3}건  pnl={r[2]:>8.2f}pt  avg={r[3]:>6.2f}  승률={wr:.0f}%")

print("\n=== sim_equity 날짜별 최종 누적손익 ===")
c.execute("""
    SELECT date(ts) as dt, MAX(cum_equity) as max_eq, MIN(cum_equity) as min_eq,
           COUNT(*) as ticks
    FROM sim_equity
    GROUP BY date(ts)
    ORDER BY dt
""")
for r in c.fetchall():
    print(f"  {r[0]}  max_equity={r[1]:>8.2f}  min={r[2]:>8.2f}  ticks={r[3]:,}")

print("\n=== 최근 sim_trades 5건 ===")
c.execute("SELECT entry_time, exit_time, direction, entry_price, exit_price, pnl, exit_reason FROM sim_trades ORDER BY id DESC LIMIT 5")
for r in c.fetchall():
    d = "LONG" if r[2]==1 else "SHORT"
    print(f"  {r[0]} → {r[1]}  {d}  {r[3]}→{r[4]}  pnl={r[5]:.2f}  {r[6]}")

conn.close()
