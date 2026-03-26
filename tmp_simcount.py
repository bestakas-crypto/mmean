import sqlite3

# sim.db
try:
    c1 = sqlite3.connect('Z:/storages/sim.db')
    r1 = c1.execute("SELECT COUNT(*) FROM sim_runs WHERE created_at LIKE '2026-03-25%'").fetchone()[0]
    print(f'sim.db 3/25: {r1:,}')
except Exception as e:
    print(f'sim.db 오류: {e}')

# pulse_sim.db
try:
    c2 = sqlite3.connect('Z:/storages/pulse_sim.db')
    r2 = c2.execute("SELECT COUNT(*) FROM pulse_runs WHERE created_at LIKE '2026-03-25%'").fetchone()[0]
    print(f'pulse_sim.db 3/25: {r2:,}')
except Exception as e:
    print(f'pulse_sim.db 오류: {e}')
