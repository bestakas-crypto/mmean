import sqlite3, os

db = r'C:/mmean/storage/pulse_sim.db'
conn = sqlite3.connect(db)
c = conn.cursor()

c.execute("SELECT COUNT(*) FROM pulse_runs WHERE objective < 0")
cnt = c.fetchone()[0]
print(f"삭제 대상: {cnt:,}건")

c.execute("DELETE FROM pulse_runs WHERE objective < 0")
conn.commit()

c.execute("SELECT COUNT(*) FROM pulse_runs")
print(f"삭제 후 잔존: {c.fetchone()[0]:,}건")

print("VACUUM 시작...")
conn.execute("VACUUM")
conn.close()

size = os.path.getsize(db)
print(f"파일 크기: {size/1024/1024:.1f} MB")
