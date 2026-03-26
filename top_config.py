import sqlite3, json
conn = sqlite3.connect(r'C:/mmean/storage/pulse_sim.db')
c = conn.cursor()

for date in ['2026-03-23', '2026-03-24']:
    print(f"\n{'='*60}")
    print(f"  {date} TOP 1 (entry>=5)")
    print(f"{'='*60}")
    c.execute("""
        SELECT objective, direction_acc, confirmed_ratio, entry_rate,
               signal_count, entry_count, config_json
        FROM pulse_runs
        WHERE dates=? AND entry_count >= 5
        ORDER BY objective DESC
        LIMIT 1
    """, (date,))
    row = c.fetchone()
    if row:
        print(f"  objective    : {row[0]:.2f}")
        print(f"  direction_acc: {row[1]:.2f}")
        print(f"  confirmed_r  : {row[2]:.2f}")
        print(f"  entry_rate   : {row[3]:.2f}")
        print(f"  signal_count : {row[4]}")
        print(f"  entry_count  : {row[5]}")
        cfg = json.loads(row[6])
        print(f"  --- config ---")
        for k, v in sorted(cfg.items()):
            print(f"  {k:30s}: {v}")

conn.close()
