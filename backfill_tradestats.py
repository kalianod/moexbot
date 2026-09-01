"""[NEW 2026-08-31] Backfill tradestats (aggressive buy/sell volumes) в БД.
Запуск: python backfill_tradestats.py [--days N]"""
import ssl
_dc = ssl.create_default_context
def _nv(*a, **k):
    c = _dc(*a, **k); c.check_hostname=False; c.verify_mode=ssl.CERT_NONE; return c
ssl.create_default_context = _nv

import sqlite3
import argparse
from datetime import date, timedelta
from moexalgo import Ticker, session
from dotenv import load_dotenv
import os

load_dotenv('/home/kalian/moexbot/.env')
session.TOKEN = os.getenv('MOEXALGOPACK_TOKEN')
DB_PATH = '/home/kalian/moexbot/futoi.db'
SYMBOLS = ['SiU6', 'CRU6', 'MXU6']

parser = argparse.ArgumentParser()
parser.add_argument('--days', type=int, default=14)
args = parser.parse_args()

# 1. Создаём таблицу
conn = sqlite3.connect(DB_PATH)
conn.execute("""
CREATE TABLE IF NOT EXISTS tradestats_data (
    symbol TEXT NOT NULL,
    tradedate TEXT NOT NULL,
    tradetime TEXT NOT NULL,
    vol INTEGER,
    vol_b INTEGER,
    vol_s INTEGER,
    disb REAL,
    val_b REAL,
    val_s REAL,
    trades_b INTEGER,
    trades_s INTEGER,
    oi_open INTEGER,
    oi_close INTEGER,
    d_oi INTEGER,
    systime TEXT,
    PRIMARY KEY(symbol, tradedate, tradetime)
)""")
conn.commit()

end_d = date.today()
start_d = end_d - timedelta(days=args.days)

for sym in SYMBOLS:
    print(f"\n[{sym}] загрузка {start_d} → {end_d}...")
    try:
        df = Ticker(sym).tradestats(start=str(start_d), end=str(end_d))
        if df is None or df.empty:
            print(f"[{sym}] пусто")
            continue
        df['symbol'] = sym
        df['tradedate'] = df['tradedate'].astype(str)
        df['tradetime'] = df['tradetime'].astype(str)
        df['d_oi'] = df['oi_close'] - df['oi_open']
        df['systime'] = 'backfill'

        rows = []
        for _, r in df.iterrows():
            rows.append((
                r['symbol'], r['tradedate'], r['tradetime'],
                int(r.get('vol') or 0),
                int(r.get('vol_b') or 0), int(r.get('vol_s') or 0),
                float(r.get('disb') or 0),
                float(r.get('val_b') or 0), float(r.get('val_s') or 0),
                int(r.get('trades_b') or 0), int(r.get('trades_s') or 0),
                int(r.get('oi_open') or 0), int(r.get('oi_close') or 0),
                int(r['d_oi']), r['systime']
            ))
        conn.executemany(
            "INSERT OR REPLACE INTO tradestats_data "
            "(symbol,tradedate,tradetime,vol,vol_b,vol_s,disb,val_b,val_s,trades_b,trades_s,oi_open,oi_close,d_oi,systime) "
            "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            rows
        )
        conn.commit()
        print(f"[{sym}] ✅ сохранено {len(rows)} строк")
    except Exception as e:
        print(f"[{sym}] ❌ {type(e).__name__}: {e}")

# 2. Итоговая статистика
print("\n" + "="*60)
print("Итог по БД:")
for sym in SYMBOLS:
    cur = conn.execute(
        "SELECT COUNT(*), MIN(tradedate), MAX(tradedate) FROM tradestats_data WHERE symbol=?",
        (sym,))
    cnt, dmin, dmax = cur.fetchone()
    print(f"  {sym}: {cnt} баров, {dmin} → {dmax}")
conn.close()
