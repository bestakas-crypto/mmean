import sqlite3
conn = sqlite3.connect(r'C:/mmean/storage/pulse_sim.db')
c = conn.cursor()
c.execute("SELECT run_type, COUNT(*), MIN(objective), MAX(objective), AVG(objective) FROM pulse_runs GROUP BY run_type")
for r in c.fetchall():
    print(f"  {r[0]:10s}  cnt={r[1]:>9,}  min={r[2]:.2f}  max={r[3]:.2f}  avg={r[4]:.2f}")
conn.close()
