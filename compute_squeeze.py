"""🔥/🩸 Squeeze: LONG (юрлица закрывают шорты) и SHORT (юрлица закрывают лонги)."""
import sqlite3
import numpy as np
import pandas as pd

DB = '/home/kalian/moexbot/futoi.db'
SYMBOLS = ['SiU6', 'CRU6', 'MXU6']
W, MP = 96, 48

conn = sqlite3.connect(DB)
conn.execute("DROP TABLE IF EXISTS squeeze_events")
conn.execute("""CREATE TABLE squeeze_events (
    symbol TEXT, bar_ts TEXT, d_net_y REAL, d_oi REAL, ret_bar REAL, direction TEXT,
    PRIMARY KEY(symbol, bar_ts, direction))""")

for sym in SYMBOLS:
    yur = pd.read_sql_query("SELECT systime,pos_long,pos_short FROM futoi_data "
        "WHERE symbol=? AND clgroup='YUR' ORDER BY systime",
        conn, params=(sym,), parse_dates=['systime']).set_index('systime').resample('5min').last().dropna()
    ts = pd.read_sql_query("SELECT tradedate,tradetime,d_oi FROM tradestats_data "
        "WHERE symbol=? ORDER BY tradedate,tradetime", conn, params=(sym,))
    ts['t'] = pd.to_datetime(ts['tradedate'] + ' ' + ts['tradetime']); ts = ts.set_index('t')
    cnd = pd.read_sql_query("SELECT begin,open,close FROM candles_cache WHERE symbol=? AND period='5min' ORDER BY begin",
        conn, params=(sym,), parse_dates=['begin']).set_index('begin').sort_index()
    yd = yur[['pos_long','pos_short']].diff(); yd['d_net_y'] = yd['pos_long'] - yd['pos_short']
    df = yd.join(ts[['d_oi']], how='inner')
    df['open'] = cnd['open'].reindex(df.index); df['close'] = cnd['close'].reindex(df.index)
    df['ret_bar'] = df['close']/df['open'] - 1
    df = df.dropna()
    df['thr_y'] = df['d_net_y'].abs().rolling(W,MP).quantile(0.90).shift(1)
    df = df.dropna()

    rows = []
    up = (df['d_net_y'] > df['thr_y']) & (df['d_oi'] < 0) & (df['ret_bar'] > 0)   # short squeeze → ЛОНГ
    dn = (df['d_net_y'] < -df['thr_y']) & (df['d_oi'] < 0) & (df['ret_bar'] < 0)  # long squeeze → ШОРТ
    for t, r in df[up].iterrows():
        rows.append((sym, t.strftime('%Y-%m-%d %H:%M:00'), float(r['d_net_y']), float(r['d_oi']), float(r['ret_bar']), 'LONG'))
    for t, r in df[dn].iterrows():
        rows.append((sym, t.strftime('%Y-%m-%d %H:%M:00'), float(r['d_net_y']), float(r['d_oi']), float(r['ret_bar']), 'SHORT'))
    conn.executemany("INSERT OR REPLACE INTO squeeze_events VALUES(?,?,?,?,?,?)", rows)
    conn.commit()
    print(f"[{sym}] squeeze: LONG={int(up.sum())}, SHORT={int(dn.sum())}")
conn.close()
print("✅ squeeze_events (оба направления)")
