import sqlite3
conn = sqlite3.connect(r'C:/mmean/storage/pulse_sim.db')
c = conn.cursor()

for date in ['2026-03-23', '2026-03-24']:
    print(f"\n=== {date} (entry>=5, obj>=50) TOP 20 ===")
    print(f"{'#':>3} {'obj':>7} {'dir_acc':>7} {'conf_r':>7} {'e_rate':>7} {'sig':>5} {'entry':>5} {'config_hash'}")
    print("-"*80)
    c.execute("""
        SELECT objective, direction_acc, confirmed_ratio, entry_rate,
               signal_count, entry_count, config_hash, config_json
        FROM pulse_runs
        WHERE dates=? AND entry_count >= 5 AND objective >= 50
        ORDER BY objective DESC
        LIMIT 20
    """, (date,))
    rows = c.fetchall()
    if not rows:
        print("  (없음)")
        continue
    for i, r in enumerate(rows, 1):
        print(f"{i:>3} {r[0]:>7.2f} {r[1]:>7.2f} {r[2]:>7.2f} {r[3]:>7.2f} {r[4]:>5} {r[5]:>5}  {r[6]}")

conn.close()
