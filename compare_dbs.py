import sqlite3

day = "2026-03-23"

dbs = {
    "sim":       ("C:/mmean/storage/back/sim.db",       "Z:/storage/sim.db"),
    "pulse_sim": ("C:/mmean/storage/back/pulse_sim.db", "Z:/storage/pulse_sim.db"),
}

for name, (src_path, dst_path) in dbs.items():
    print(f"\n{'='*50}")
    print(f"[{name}]")

    src = sqlite3.connect(src_path)
    dst = sqlite3.connect(dst_path)
    sc = src.cursor()
    dc = dst.cursor()

    # 테이블 목록
    sc.execute("SELECT name FROM sqlite_master WHERE type='table'")
    tables = [r[0] for r in sc.fetchall()]
    print(f"  테이블: {tables}")

    for tbl in tables:
        if tbl.startswith("sqlite_"):
            continue

        # 날짜 컬럼 탐색
        sc.execute(f"PRAGMA table_info({tbl})")
        cols = [c[1] for c in sc.fetchall()]
        date_col = None
        for c in cols:
            if c in ("ts", "created_at", "timestamp", "date", "run_date", "start_ts", "end_ts"):
                date_col = c
                break

        if date_col:
            try:
                sc.execute(f"SELECT COUNT(*) FROM {tbl} WHERE {date_col} LIKE ?", (f"{day}%",))
                src_cnt = sc.fetchone()[0]

                dc.execute(f"SELECT name FROM sqlite_master WHERE type='table' AND name=?", (tbl,))
                exists = dc.fetchone()
                if exists:
                    dc.execute(f"SELECT COUNT(*) FROM {tbl} WHERE {date_col} LIKE ?", (f"{day}%",))
                    dst_cnt = dc.fetchone()[0]
                else:
                    dst_cnt = "없음"

                diff = f"  ← 차이 {src_cnt - dst_cnt}" if isinstance(dst_cnt, int) and src_cnt != dst_cnt else ""
                print(f"  {tbl}.{date_col}: SRC={src_cnt}  DST={dst_cnt}{diff}")
            except Exception as e:
                print(f"  {tbl}: 오류 - {e}")
        else:
            try:
                sc.execute(f"SELECT COUNT(*) FROM {tbl}")
                src_cnt = sc.fetchone()[0]
                dc.execute(f"SELECT name FROM sqlite_master WHERE type='table' AND name=?", (tbl,))
                exists = dc.fetchone()
                if exists:
                    dc.execute(f"SELECT COUNT(*) FROM {tbl}")
                    dst_cnt = dc.fetchone()[0]
                else:
                    dst_cnt = "없음"
                print(f"  {tbl} (날짜컬럼없음 - 전체): SRC={src_cnt}  DST={dst_cnt}")
            except Exception as e:
                print(f"  {tbl}: 오류 - {e}")

    src.close()
    dst.close()
