#!/usr/bin/env python3
"""Мини-backfill futoi_data только для NGV6 за 30 дней."""
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
SYMBOL = 'NGV6'
D_START, D_END = date(2026, 9, 14), date(2026, 10, 11)

conn = sqlite3.connect(DB)
schema_cols = [r[1] for r in conn.execute("PRAGMA table_info(futoi_data)").fetchall()]
insert_cols = [c for c in schema_cols if c != 'id']

print(f"[{SYMBOL}] backfill {D_START} → {D_END}")
existing = set(conn.execute(
    "SELECT clgroup, systime FROM futoi_data WHERE symbol=?", (SYMBOL,)).fetchall())
total_new = 0
d = D_START
while d <= D_END:
    ds = str(d)
    try:
        df = Ticker(SYMBOL).futoi(start=ds, end=ds)
    except Exception as e:
        print(f"  {ds}: ошибка {str(e)[:80]}")
        d += timedelta(days=1); continue
    if df is None or df.empty:
        print(f"  {ds}: пусто")
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
                vals.append(SYMBOL)
            elif c in df.columns:
                v = r[c]
                vals.append(None if pd.isna(v) else v)
            else:
                vals.append(None)
        rows.append(tuple(vals))
    if rows:
        ph = ', '.join(['?'] * len(insert_cols))
        conn.executemany(f"INSERT INTO futoi_data ({', '.join(insert_cols)}) VALUES ({ph})", rows)
        conn.commit()
        total_new += len(rows)
        print(f"  {ds}: +{len(rows)}")
    else:
        print(f"  {ds}: всё уже есть")
    d += timedelta(days=1)

print(f"\n[{SYMBOL}] итого новых строк: {total_new}")
conn.close()
print("✅ Готово")
