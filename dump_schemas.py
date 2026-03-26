import sqlite3, os

dbs = {
    "mmean.db":     r"C:\mmean\storage\mmean.db",
    "sim.db":       r"C:\mmean\storage\sim.db",
    "pulse_sim.db": r"C:\mmean\storage\pulse_sim.db",
}

for name, path in dbs.items():
    if not os.path.exists(path):
        print(f"\n=== {name} — 파일 없음 ===")
        continue
    print(f"\n{'='*60}")
    print(f"=== {name} ===")
    conn = sqlite3.connect(f"file:{path}?mode=ro", uri=True, timeout=5)
    conn.row_factory = sqlite3.Row
    tables = conn.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name").fetchall()
    for t in tables:
        tname = t["name"]
        cols = conn.execute(f"PRAGMA table_info({tname})").fetchall()
        cnt = conn.execute(f"SELECT COUNT(*) FROM {tname}").fetchone()[0]
        print(f"\n  [{tname}] ({cnt:,}건)")
        for c in cols:
            pk = " PK" if c["pk"] else ""
            nn = " NOT NULL" if c["notnull"] else ""
            df = f" DEFAULT {c['dflt_value']}" if c["dflt_value"] is not None else ""
            print(f"    {c['name']:35s} {c['type']:15s}{pk}{nn}{df}")
    conn.close()
