"""[NEW] 🪤 Ловушка юрлиц: YUR SELL экстремум + OI↑ (новые шорты на бычьем рынке).
Контрариан против юрлиц: они открывают шорты → их вынесет ростом."""
import sqlite3
import numpy as np
import pandas as pd

DB = '/home/kalian/moexbot/futoi.db'
SYMBOLS = ['SiU6', 'CRU6', 'MXU6']
W, MP = 96, 48

conn = sqlite3.connect(DB)
conn.execute("""CREATE TABLE IF NOT EXISTS trap_events (
    symbol TEXT, bar_ts TEXT, d_net_y REAL, d_oi REAL,
    PRIMARY KEY(symbol, bar_ts))""")
conn.execute("DELETE FROM trap_events"); conn.commit()

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
    # YUR экстренно ПРОДАЮТ (шортят) + OI РАСТЁТ (открывают НОВЫЕ шорты)
    mask = (df['d_net_y'] < -df['thr_y']) & (df['d_oi'] > 0)
    rows = [(sym, t.strftime('%Y-%m-%d %H:%M:00'), float(r['d_net_y']), float(r['d_oi']))
            for t, r in df[mask].iterrows()]
    conn.executemany("INSERT OR REPLACE INTO trap_events VALUES(?,?,?,?)", rows)
    conn.commit()
    print(f"[{sym}] ловушек: {len(rows)}")
conn.close()
print("✅ trap_events рассчитан")
