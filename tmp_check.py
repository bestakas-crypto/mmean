import sqlite3

checks = [
    ('mmean.db',    'C:/mmean/storage/mmean.db',    'SELECT MAX(ts) FROM regime_ticks'),
    ('sim.db',      'C:/mmean/storage/sim.db',       'SELECT MAX(created_at) FROM sim_trades'),
    ('pulse_sim.db','C:/mmean/storage/pulse_sim.db', 'SELECT MAX(created_at) FROM sim_results'),
]

for name, path, sql in checks:
    try:
        conn = sqlite3.connect(path)
        row = conn.execute(sql).fetchone()
        print(f'{name}: {row[0]}')
        conn.close()
    except Exception as e:
        print(f'{name}: 오류={e}')
