import sqlite3
conn = sqlite3.connect(r'C:\mmean\storage\mmean.db', timeout=5)
c = conn.cursor()

# regime_ticks 컬럼 목록
c.execute("PRAGMA table_info(regime_ticks)")
cols = [r[1] for r in c.fetchall()]
# entry/gate 관련 컬럼만
gate_cols = [c2 for c2 in cols if any(k in c2 for k in ['gate','entry','signal','llm_filter'])]
print("gate/entry 관련 컬럼:", gate_cols)

# entry_signal 분포 + llm_filter
print("\n=== entry_signal + llm_filter_valid (03-18~) ===")
c2 = conn.cursor()
c2.execute("""
    SELECT substr(ts,1,10) as dt, entry_signal,
           llm_filter_valid, COUNT(*) as cnt
    FROM regime_ticks
    WHERE ts >= '2026-03-18'
    GROUP BY dt, entry_signal, llm_filter_valid
    ORDER BY dt, cnt DESC
""")
for r in c2.fetchall():
    print(f"  {r[0]}  signal={r[1]:12s}  llm_valid={r[2]}  {r[3]:,}건")

# sim_equity has_position 분포
print("\n=== sim_equity has_position (03-18~) ===")
c2.execute("""
    SELECT date(ts), has_position, COUNT(*)
    FROM sim_equity
    WHERE ts >= '2026-03-18'
    GROUP BY date(ts), has_position
    ORDER BY date(ts)
""")
for r in c2.fetchall():
    print(f"  {r[0]}  has_pos={r[1]}  {r[2]:,}건")

conn.close()
