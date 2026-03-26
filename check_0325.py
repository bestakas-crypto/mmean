import sqlite3

day = "2026-03-25"
ps = sqlite3.connect("Z:/storage/pulse_sim.db")
ps.execute("PRAGMA journal_mode=WAL")

# 최신 10건만 확인
rows = ps.execute("SELECT id, created_at FROM pulse_runs ORDER BY id DESC LIMIT 10").fetchall()
print("최근 10건:")
for r in rows:
    print(f"  id={r[0]}  {r[1]}")

# 첫 10건
rows = ps.execute("SELECT id, created_at FROM pulse_runs ORDER BY id ASC LIMIT 10").fetchall()
print("\n첫 10건:")
for r in rows:
    print(f"  id={r[0]}  {r[1]}")

ps.close()
