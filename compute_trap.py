"""🪤/ Ловушка: LONG (юрлица шортят на бычьем) и SHORT (юрлица лонгуют на медвежьем)."""
import sqlite3
import numpy as np
import pandas as pd

DB = '/home/kalian/moexbot/futoi.db'
SYMBOLS = ['SiU6', 'CRU6', 'MXU6']
W, MP = 96, 48

conn = sqlite3.connect(DB)
conn.execute("DROP TABLE IF EXISTS trap_events")
conn.execute("""CREATE TABLE trap_events (
    symbol TEXT, bar_ts TEXT, d_net_y REAL, d_oi REAL, direction TEXT,
    PRIMARY KEY(symbol, bar_ts, direction))""")

for sym in SYMBOLS:
    yur = pd.read_sql_query("SELECT systime,pos_long,pos_short FROM futoi_data "
        "WHERE symbol=? AND clgroup='YUR' ORDER BY systime",
        conn, params=(sym,), parse_dates=['systime']).set_index('systime').resample('5min').last().dropna()
    ts = pd.read_sql_query("SELECT tradedate,tradetime,d_oi FROM tradestats_data "
        "WHERE symbol=? ORDER BY tradedate,tradetime", conn, params=(sym,))
    ts['t'] = pd.to_datetime(ts['tradedate'] + ' ' + ts['tradetime']); ts = ts.set_index('t')
    yd = yur[['pos_long','pos_short']].diff(); yd['d_net_y'] = yd['pos_long'] - yd['pos_short']
    df = yd.join(ts[['d_oi']], how='inner').dropna()
    df['thr_y'] = df['d_net_y'].abs().rolling(W,MP).quantile(0.90).shift(1)
    df = df.dropna()

    rows = []
    long_m  = (df['d_net_y'] < -df['thr_y']) & (df['d_oi'] > 0)  # юрлица шортят → ЛОНГ (контрариан)
    short_m = (df['d_net_y'] >  df['thr_y']) & (df['d_oi'] > 0)  # юрлица лонгуют → ШОРТ (контрариан)
    for t, r in df[long_m].iterrows():
        rows.append((sym, t.strftime('%Y-%m-%d %H:%M:00'), float(r['d_net_y']), float(r['d_oi']), 'LONG'))
    for t, r in df[short_m].iterrows():
        rows.append((sym, t.strftime('%Y-%m-%d %H:%M:00'), float(r['d_net_y']), float(r['d_oi']), 'SHORT'))
    conn.executemany("INSERT OR REPLACE INTO trap_events VALUES(?,?,?,?,?)", rows)
    conn.commit()
    print(f"[{sym}] trap: LONG={int(long_m.sum())}, SHORT={int(short_m.sum())}")
conn.close()
print("✅ trap_events (оба направления)")
