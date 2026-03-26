import sqlite3

conn = sqlite3.connect(r'C:\mmean\storage\sim.db', timeout=5)
c = conn.cursor()

# 03-23 run 상세
c.execute("""
    SELECT id, run_type, run_started_at, run_finished_at, 
           trade_count, objective_score, status, error_message
    FROM sim_runs 
    WHERE source_dates LIKE '%2026-03-23%'
    ORDER BY id
""")
print("[03-23 sim_runs 전체]")
for r in c.fetchall():
    print(f"  id={r[0]} type={r[1]} started={r[2]} finished={r[3]}")
    print(f"  trades={r[4]} obj={r[5]} status={r[6]} err={r[7]}")
    print()

# 가장 빠른 run 날짜
c.execute("SELECT MIN(run_started_at), MAX(run_started_at) FROM sim_runs")
r = c.fetchone()
print(f"[전체 run 기간] {r[0]} ~ {r[1]}")

# 03-18 첫 run
c.execute("SELECT MIN(run_started_at) FROM sim_runs WHERE source_dates LIKE '%2026-03-18%'")
print(f"[03-18 첫 run] {c.fetchone()[0]}")

# 03-23 첫 run
c.execute("SELECT MIN(run_started_at) FROM sim_runs WHERE source_dates LIKE '%2026-03-23%'")
print(f"[03-23 첫 run] {c.fetchone()[0]}")

conn.close()
