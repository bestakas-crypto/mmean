import sqlite3, os

dbs = {
    "mmean.db":     r"C:\mmean\storage\mmean.db",
    "sim.db":       r"C:\mmean\storage\sim.db",
    "pulse_sim.db": r"C:\mmean\storage\pulse_sim.db",
}

for name, path in dbs.items():
    if not os.path.exists(path):
        print(f"\n[{name}] 파일 없음")
        continue
    size_mb = os.path.getsize(path) / 1024 / 1024
    conn = sqlite3.connect(f"file:{path}?mode=ro", uri=True, timeout=5)
    c = conn.cursor()
    tables = c.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name").fetchall()
    print(f"\n{'='*55}")
    print(f"[{name}]  {size_mb:.1f} MB")
    for t in tables:
        tn = t[0]
        if tn == "sqlite_sequence":
            continue
        cnt = c.execute(f"SELECT COUNT(*) FROM {tn}").fetchone()[0]
        print(f"  {tn:40s} {cnt:>10,}건")
    conn.close()
