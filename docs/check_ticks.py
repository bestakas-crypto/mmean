import sqlite3, datetime
conn = sqlite3.connect('C:/mmean/storage/mmean.db')
today = datetime.datetime.now().strftime('%Y-%m-%d')
rows = conn.execute(
    'SELECT ts, futures_price FROM regime_ticks WHERE substr(ts,1,10)=? ORDER BY ts DESC LIMIT 10',
    (today,)
).fetchall()
with open('C:/mmean/docs/tick_check_result.txt', 'w') as f:
    f.write(f'today={today}\ncount={len(rows)}\n')
    for r in rows:
        f.write(str(r) + '\n')
    if not rows:
        f.write('NO ROWS\n')
conn.close()
