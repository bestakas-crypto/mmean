import sqlite3
conn = sqlite3.connect(r'C:/mmean/storage/pulse_sim.db')
c = conn.cursor()

# 날짜별 건수 (dates 컬럼)
c.execute("SELECT dates, run_type, COUNT(*), MIN(objective), MAX(objective) FROM pulse_runs GROUP BY dates, run_type ORDER BY dates")
print("=== dates / run_type counts ===")
for r in c.fetchall():
    print(f"  {r[0]}  {r[1]:8s}  cnt={r[2]:>8,}  obj_min={r[3]:.2f}  obj_max={r[4]:.2f}")

# objective 분포
c.execute("SELECT COUNT(*) FROM pulse_runs WHERE objective < -50")
print(f"\nobjective < -50: {c.fetchone()[0]:,}")
c.execute("SELECT COUNT(*) FROM pulse_runs WHERE objective >= -50 AND objective < 0")
print(f"objective -50~0: {c.fetchone()[0]:,}")
c.execute("SELECT COUNT(*) FROM pulse_runs WHERE objective >= 0")
print(f"objective >= 0 : {c.fetchone()[0]:,}")

conn.close()
