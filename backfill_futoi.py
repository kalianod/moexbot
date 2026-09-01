"""[NEW 2026-08-31, v2] Бэкфилл futoi (YUR) из API — по-дневная загрузка
(обход лимита ISS в 1000 строк на запрос)."""
import ssl
_dc = ssl.create_default_context
def _nv(*a, **k):
    c = _dc(*a, **k); c.check_hostname=False; c.verify_mode=ssl.CERT_NONE; return c
ssl.create_default_context = _nv

import sqlite3
import pandas as pd
from datetime import date, timedelta
from moexalgo import Ticker, session
from dotenv import load_dotenv
import os

load_dotenv('/home/kalian/moexbot/.env')
session.TOKEN = os.getenv('MOEXALGOPACK_TOKEN')
DB = '/home/kalian/moexbot/futoi.db'
SYMBOLS = ['SiU6', 'CRU6', 'MXU6']
D_START, D_END = date(2026, 6, 27), date(2026, 8, 31)

conn = sqlite3.connect(DB)
schema_cols = [r[1] for r in conn.execute("PRAGMA table_info(futoi_data)").fetchall()]

for sym in SYMBOLS:
    print(f"\n[{sym}] по-дневной бэкфилл...")
    existing = set(conn.execute(
        "SELECT clgroup, systime FROM futoi_data WHERE symbol=?", (sym,)).fetchall())
    insert_cols = [c for c in schema_cols if c != 'id' and (c in ['symbol'] or True) and (c in ['symbol'] or c in schema_cols)]
    insert_cols = [c for c in schema_cols if c != 'id']  # вставляем все колонки схемы кроме id
    total_new = 0
    d = D_START
    while d <= D_END:
        ds = str(d)
        try:
            df = Ticker(sym).futoi(start=ds, end=ds)
        except Exception:
            d += timedelta(days=1); continue
        if df is None or df.empty:
            d += timedelta(days=1); continue
        df['systime_s'] = pd.to_datetime(df['systime']).dt.strftime('%Y-%m-%d %H:%M:%S')
        rows = []
        for _, r in df.iterrows():
            key = (r['clgroup'], r['systime_s'])
            if key in existing:
                continue
            existing.add(key)
            vals = []
            for c in insert_cols:
                if c == 'symbol':
                    vals.append(sym)
                elif c in df.columns:
                    v = r[c]
                    vals.append(None if pd.isna(v) else v)
                else:
                    vals.append(None)
            rows.append(tuple(vals))
        if rows:
            ph = ', '.join(['?'] * len(insert_cols))
            conn.executemany(
                f"INSERT INTO futoi_data ({', '.join(insert_cols)}) VALUES ({ph})", rows)
            conn.commit()
            total_new += len(rows)
            print(f"  {ds}: +{len(rows)}")
        d += timedelta(days=1)
    print(f"[{sym}] итого новых строк: {total_new}")

print("\nПокрытие YUR (SiU6):")
for d in ['2026-08-03', '2026-08-05', '2026-08-06', '2026-08-07']:
    n = conn.execute("SELECT COUNT(*) FROM futoi_data WHERE symbol='SiU6' AND clgroup='YUR' AND tradedate=?",
                     (d,)).fetchone()[0]
    print(f"  {d}: YUR={n}")
conn.close()
print("\n✅ Бэкфилл futoi v2 завершён")
