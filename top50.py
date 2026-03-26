import sqlite3, json
conn = sqlite3.connect(r'C:/mmean/storage/pulse_sim.db')
c = conn.cursor()
c.execute("""
    SELECT id, dates, run_type, objective, direction_acc, confirmed_ratio, 
           entry_rate, signal_count, entry_count, config_json
    FROM pulse_runs 
    ORDER BY objective DESC 
    LIMIT 50
""")
rows = c.fetchall()
print(f"{'#':>3} {'id':>8} {'dates':<12} {'type':>9} {'obj':>7} {'dir_acc':>7} {'conf_r':>7} {'e_rate':>7} {'sig':>5} {'entry':>5}")
print("-"*90)
for i, r in enumerate(rows, 1):
    print(f"{i:>3} {r[0]:>8} {r[1]:<12} {r[2]:>9} {r[3]:>7.2f} {r[4]:>7.2f} {r[5]:>7.2f} {r[6]:>7.2f} {r[7]:>5} {r[8]:>5}")
conn.close()
