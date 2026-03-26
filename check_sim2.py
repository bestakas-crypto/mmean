import sqlite3
conn = sqlite3.connect(r'C:\mmean\storage\mmean.db', timeout=5)
c = conn.cursor()

# sim_equity 컬럼 확인
c.execute("PRAGMA table_info(sim_equity)")
print("=== sim_equity 컬럼 ===")
for r in c.fetchall():
    print(f"  {r[1]} {r[2]}")

# sim_trades 컬럼 확인
print("\n=== sim_trades 최근 5건 ===")
c.execute("PRAGMA table_info(sim_trades)")
cols = [r[1] for r in c.fetchall()]
print("컬럼:", cols)

c.execute("SELECT * FROM sim_trades ORDER BY id DESC LIMIT 5")
for row in c.fetchall():
    print(dict(zip(cols, row)))

# 17일 이후 entry_signal 분포
print("\n=== regime_ticks entry_signal 분포 (03-17 이후) ===")
c.execute("""
    SELECT substr(ts,1,10) as dt, entry_signal, COUNT(*) 
    FROM regime_ticks 
    WHERE ts >= '2026-03-17'
    GROUP BY dt, entry_signal
    ORDER BY dt, entry_signal
""")
for r in c.fetchall():
    print(f"  {r[0]}  {r[1]:20s}  {r[2]:,}건")

conn.close()
