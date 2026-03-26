import sqlite3, os

db = r'C:/mmean/storage/sim.db'
conn = sqlite3.connect(db, timeout=60)
c = conn.cursor()

# 1. 삭제 대상 run id 수집 (objective<0 OR random)
print("삭제 대상 집계 중...")
c.execute("SELECT COUNT(*) FROM sim_runs WHERE objective_score < 0 OR run_type = 'random'")
print(f"  sim_runs 삭제 대상: {c.fetchone()[0]:,}")

# 2. 연계 sim_trades 삭제
print("sim_trades 연계 삭제...")
c.execute("""
    DELETE FROM sim_trades WHERE run_id IN (
        SELECT id FROM sim_runs WHERE objective_score < 0 OR run_type = 'random'
    )
""")
print(f"  sim_trades 삭제: {c.rowcount:,}건")

# 3. sim_runs 삭제
print("sim_runs 삭제...")
c.execute("DELETE FROM sim_runs WHERE objective_score < 0 OR run_type = 'random'")
print(f"  sim_runs 삭제: {c.rowcount:,}건")

# 4. 연계 테이블들
for tbl in ['sim_run_summary', 'sim_observations', 'sim_run_sessions']:
    c.execute(f"SELECT COUNT(*) FROM {tbl}")
    total = c.fetchone()[0]
    # run_id 기준으로 정리 (없는 run_id 참조 레코드 삭제)
    c.execute(f"DELETE FROM {tbl} WHERE run_id NOT IN (SELECT id FROM sim_runs)")
    print(f"  {tbl} 정리: {c.rowcount:,}건 (전체 {total:,})")

conn.commit()

c.execute("SELECT COUNT(*) FROM sim_runs")
print(f"\n잔존 sim_runs: {c.fetchone()[0]:,}")
c.execute("SELECT COUNT(*) FROM sim_trades")
print(f"잔존 sim_trades: {c.fetchone()[0]:,}")

print("\nVACUUM 시작...")
conn.execute("VACUUM")
conn.close()

size = os.path.getsize(db)
print(f"파일 크기: {size/1024/1024:.1f} MB")
