import sqlite3
conn = sqlite3.connect(r'C:/mmean/storage/sim.db')
c = conn.cursor()

# sim_runs 구조 및 분포
c.execute("PRAGMA table_info(sim_runs)")
cols = [r[1] for r in c.fetchall()]
print("sim_runs columns:", cols)

c.execute("SELECT COUNT(*) FROM sim_runs")
print(f"\nsim_runs 전체: {c.fetchone()[0]:,}")

# 성과 컬럼 확인
c.execute("SELECT * FROM sim_runs LIMIT 2")
rows = c.fetchall()
for r in rows:
    for i,v in enumerate(r):
        print(f"  {cols[i]:30s}: {v}")
    print("---")

conn.close()
