import sqlite3

day = "2026-03-23"

def count(conn, sql, params=()):
    try:
        return conn.execute(sql, params).fetchone()[0]
    except:
        return "오류"

src_sim = sqlite3.connect("C:/mmean/storage/back/sim.db")
dst_sim = sqlite3.connect("Z:/storage/sim.db")
src_ps  = sqlite3.connect("C:/mmean/storage/back/pulse_sim.db")
dst_ps  = sqlite3.connect("Z:/storage/pulse_sim.db")

print(f"{'='*60}")
print(f"[sim.db] 3/23 기준 비교")
print(f"{'='*60}")

# sim_runs: source_dates에 날짜 포함된 것
q_runs = "SELECT COUNT(*) FROM sim_runs WHERE source_dates LIKE ?"
sc = count(src_sim, q_runs, (f'%{day}%',))
dc = count(dst_sim, q_runs, (f'%{day}%',))
diff = f"  ← SRC {sc-dc:+d}" if isinstance(sc,int) and isinstance(dc,int) and sc!=dc else ""
print(f"  sim_runs       : SRC={sc:>7}  DST={dc:>7}{diff}")

# sim_trades: session_date
q_tr = "SELECT COUNT(*) FROM sim_trades WHERE session_date = ?"
sc = count(src_sim, q_tr, (day,))
dc = count(dst_sim, q_tr, (day,))
diff = f"  ← SRC {sc-dc:+d}" if isinstance(sc,int) and isinstance(dc,int) and sc!=dc else ""
print(f"  sim_trades     : SRC={sc:>7}  DST={dc:>7}{diff}")

# sim_run_sessions: session_date
q_sess = "SELECT COUNT(*) FROM sim_run_sessions WHERE session_date = ?"
sc = count(src_sim, q_sess, (day,))
dc = count(dst_sim, q_sess, (day,))
diff = f"  ← SRC {sc-dc:+d}" if isinstance(sc,int) and isinstance(dc,int) and sc!=dc else ""
print(f"  sim_run_sessions: SRC={sc:>7}  DST={dc:>7}{diff}")

# sim_run_summary: run_id 기준으로 조인
q_smry = """SELECT COUNT(*) FROM sim_run_summary
            WHERE run_id IN (SELECT id FROM sim_runs WHERE source_dates LIKE ?)"""
sc = count(src_sim, q_smry, (f'%{day}%',))
dc = count(dst_sim, q_smry, (f'%{day}%',))
diff = f"  ← SRC {sc-dc:+d}" if isinstance(sc,int) and isinstance(dc,int) and sc!=dc else ""
print(f"  sim_run_summary: SRC={sc:>7}  DST={dc:>7}{diff}")

print(f"\n{'='*60}")
print(f"[pulse_sim.db] 3/23 기준 비교")
print(f"{'='*60}")

q_pr = "SELECT COUNT(*) FROM pulse_runs WHERE created_at LIKE ?"
sc = count(src_ps, q_pr, (f'{day}%',))
dc = count(dst_ps, q_pr, (f'{day}%',))
diff = f"  ← SRC {sc-dc:+d}" if isinstance(sc,int) and isinstance(dc,int) and sc!=dc else ""
print(f"  pulse_runs     : SRC={sc:>7}  DST={dc:>7}{diff}")

for conn in [src_sim, dst_sim, src_ps, dst_ps]:
    conn.close()

print("\n[완료]")
