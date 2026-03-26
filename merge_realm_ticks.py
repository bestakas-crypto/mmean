import sqlite3

SRC_DB = "C:/mmean/storage/back/mmean.db"
DST_DB = "Z:/storage/mmean.db"

today = "2026-03-25"
T_START = f"{today} 08:00:00"
T_END   = f"{today} 13:00:00"

src = sqlite3.connect(SRC_DB)
sc = src.cursor()

# id 제외한 컬럼 목록
sc.execute("PRAGMA table_info(regime_ticks)")
all_cols = [c[1] for c in sc.fetchall()]
cols = [c for c in all_cols if c != "id"]
cols_str = ", ".join(cols)
placeholders = ", ".join(["?"] * len(cols))

# 소스에서 해당 시간대 데이터 읽기
sc.execute(f"SELECT {cols_str} FROM regime_ticks WHERE ts >= ? AND ts <= ? ORDER BY ts",
           (T_START, T_END))
rows = sc.fetchall()
print(f"[SRC] 읽은 건수: {len(rows)}")
if rows:
    print(f"  범위: {rows[0][0]} ~ {rows[-1][0]}")

src.close()

if not rows:
    print("삽입할 데이터 없음.")
    exit()

# DST에 삽입
dst = sqlite3.connect(DST_DB)
dc = dst.cursor()

# 삽입 전 DST 오늘 현황
dc.execute("SELECT COUNT(*) FROM regime_ticks WHERE ts LIKE ?", (f"{today}%",))
before = dc.fetchone()[0]
print(f"[DST] 삽입 전 오늘 건수: {before}")

# INSERT OR IGNORE (ts가 UNIQUE이면 중복 스킵)
inserted = 0
skipped = 0
for row in rows:
    try:
        dc.execute(f"INSERT OR IGNORE INTO regime_ticks ({cols_str}) VALUES ({placeholders})", row)
        if dc.rowcount > 0:
            inserted += 1
        else:
            skipped += 1
    except Exception as e:
        print(f"  오류: {e} / ts={row[0]}")
        break

dst.commit()

# 삽입 후 확인
dc.execute("SELECT MIN(ts), MAX(ts), COUNT(*) FROM regime_ticks WHERE ts LIKE ?", (f"{today}%",))
row = dc.fetchone()
print(f"[DST] 삽입 후 오늘: {row[0]} ~ {row[1]}, {row[2]}건")
print(f"  삽입: {inserted}건 / 스킵(중복): {skipped}건")

dst.close()
print("\n[완료]")
