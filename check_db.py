import sqlite3, os

dbs = [
    r'C:/mmean/storage/pulse_sim.db',
    r'C:/mmean/storage/sim.db',
    r'C:/mmean/storage/mmean.db',
]
for path in dbs:
    size = os.path.getsize(path)
    conn = sqlite3.connect(path)
    c = conn.cursor()
    c.execute("SELECT name FROM sqlite_master WHERE type='table'")
    tables = [r[0] for r in c.fetchall()]
    print(f'\n[{os.path.basename(path)}] size={size/1024/1024:.1f}MB tables={tables}')
    for t in tables:
        c.execute(f'SELECT COUNT(*) FROM [{t}]')
        print(f'  {t}: {c.fetchone()[0]:,}')
    conn.close()
