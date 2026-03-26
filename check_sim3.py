import sqlite3
conn = sqlite3.connect(r'C:\mmean\storage\mmean.db', timeout=5)
c = conn.cursor()

# entry_gate 분포 확인 (03-18 이후)
print("=== flow_entry_gate 분포 (03-18~) ===")
c.execute("""
    SELECT substr(ts,1,10) as dt, flow_entry_gate, COUNT(*)
    FROM regime_ticks
    WHERE ts >= '2026-03-18' AND flow_entry_gate IS NOT NULL
    GROUP BY dt, flow_entry_gate
    ORDER BY dt
""")
for r in c.fetchall():
    print(f"  {r[0]}  gate={r[1]:10s}  {r[2]:,}건")

# flow_final_gate_reason 분포
print("\n=== flow_final_gate_reason 분포 (03-18~) ===")
c.execute("""
    SELECT substr(ts,1,10) as dt, flow_final_gate_reason, COUNT(*)
    FROM regime_ticks
    WHERE ts >= '2026-03-18' AND flow_final_gate_reason IS NOT NULL
    GROUP BY dt, flow_final_gate_reason
    ORDER BY dt
""")
for r in c.fetchall():
    print(f"  {r[0]}  reason={r[1]:25s}  {r[2]:,}건")

# sim_equity 최근 확인
print("\n=== sim_equity 날짜별 ===")
c.execute("""
    SELECT date(ts) as dt, COUNT(*), MAX(total_equity), MIN(total_equity)
    FROM sim_equity GROUP BY dt ORDER BY dt
""")
for r in c.fetchall():
    print(f"  {r[0]}  ticks={r[1]:,}  max_equity={r[2]:.0f}  min={r[3]:.0f}")

conn.close()
