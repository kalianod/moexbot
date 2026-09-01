"""[NEW] Расчёт слоёв A (дивергенция Ф/Ю) и B (пробой ромб+whale).
Запуск: python compute_signals.py"""
import sqlite3
import numpy as np
import pandas as pd

DB = '/home/kalian/moexbot/futoi.db'
SYMBOLS = ['SiU6', 'CRU6', 'MXU6']
W, MP = 96, 48

conn = sqlite3.connect(DB)
conn.execute("""CREATE TABLE IF NOT EXISTS divergence_events (
    symbol TEXT, bar_ts TEXT, div_type TEXT, d_net_f REAL, d_net_y REAL,
    PRIMARY KEY(symbol, bar_ts, div_type))""")
conn.execute("""CREATE TABLE IF NOT EXISTS whale_events (
    symbol TEXT, bar_ts TEXT, whale_type TEXT, d_net_f REAL, conc REAL,
    PRIMARY KEY(symbol, bar_ts, whale_type))""")
conn.execute("""CREATE TABLE IF NOT EXISTS breakout_combo (
    symbol TEXT, bar_ts TEXT, combo_type TEXT,
    PRIMARY KEY(symbol, bar_ts, combo_type))""")
conn.execute("DELETE FROM divergence_events"); conn.execute("DELETE FROM whale_events")
conn.execute("DELETE FROM breakout_combo"); conn.commit()

for sym in SYMBOLS:
    print(f"[{sym}] расчёт...")
    fiz = pd.read_sql_query("SELECT systime,pos_long,pos_short,pos_long_num,pos_short_num "
        "FROM futoi_data WHERE symbol=? AND clgroup='FIZ' ORDER BY systime",
        conn, params=(sym,), parse_dates=['systime']).set_index('systime').resample('5min').last().dropna()
    yur = pd.read_sql_query("SELECT systime,pos_long,pos_short "
        "FROM futoi_data WHERE symbol=? AND clgroup='YUR' ORDER BY systime",
        conn, params=(sym,), parse_dates=['systime']).set_index('systime').resample('5min').last().dropna()

    fd = fiz[['pos_long','pos_short','pos_long_num','pos_short_num']].diff()
    fd['d_net_f'] = fd['pos_long'] - fd['pos_short']
    fd['d_acct_f'] = fd['pos_long_num'] - fd['pos_short_num']
    fd['conc'] = np.where(fd['d_acct_f'].abs()>1, fd['d_net_f'].abs()/fd['d_acct_f'].abs(), 0)
    yd = yur[['pos_long','pos_short']].diff(); yd['d_net_y'] = yd['pos_long'] - yd['pos_short']

    df = pd.DataFrame({'d_net_f':fd['d_net_f'],'d_acct_f':fd['d_acct_f'],
                       'conc':fd['conc'],'d_net_y':yd['d_net_y']}).dropna()
    df['thr_f'] = df['d_net_f'].abs().rolling(W,MP).quantile(0.90).shift(1)
    df['thr_y'] = df['d_net_y'].abs().rolling(W,MP).quantile(0.90).shift(1)
    df['thr_conc'] = df['conc'].rolling(W,MP).quantile(0.90).shift(1)
    df = df.dropna()

    div_rows, whale_rows = [], []
    for t, r in df.iterrows():
        ts = t.strftime('%Y-%m-%d %H:%M:00')
        sf, sy = np.sign(r['d_net_f']), np.sign(r['d_net_y'])
        big_f = abs(r['d_net_f']) > 0.7*r['thr_f']; big_y = abs(r['d_net_y']) > 0.7*r['thr_y']
        if sf>0 and sy<0 and big_f and big_y:
            div_rows.append((sym, ts, 'F_BUY_Y_SELL', r['d_net_f'], r['d_net_y']))
        elif sf<0 and sy>0 and big_f and big_y:
            div_rows.append((sym, ts, 'F_SELL_Y_BUY', r['d_net_f'], r['d_net_y']))
        if r['conc'] > max(r['thr_conc'],2.0) and abs(r['d_net_f']) > 0.5*r['thr_f']:
            wt = 'WHALE_BUY' if r['d_net_f']>0 else 'WHALE_SELL'
            whale_rows.append((sym, ts, wt, r['d_net_f'], r['conc']))
    conn.executemany("INSERT OR REPLACE INTO divergence_events VALUES(?,?,?,?,?)", div_rows)
    conn.executemany("INSERT OR REPLACE INTO whale_events VALUES(?,?,?,?,?)", whale_rows)

    # Комбо: ромб + whale противоположного знака (тот же бар или ±1)
    romb = pd.read_sql_query("SELECT bar_ts,abs_level FROM yur_absorption "
        "WHERE symbol=? AND abs_level IN ('L2_BUY','L3_BUY','L2_SELL','L3_SELL')",
        conn, params=(sym,), parse_dates=['bar_ts'])
    wh = pd.read_sql_query("SELECT bar_ts,whale_type FROM whale_events WHERE symbol=?",
        conn, params=(sym,), parse_dates=['bar_ts'])
    wh_set = dict(zip(wh['bar_ts'], wh['whale_type']))
    combo_rows = []
    for _, rr in romb.iterrows():
        t = rr['bar_ts']
        for dt in [0, 5, -5]:
            wtype = wh_set.get(t + pd.Timedelta(minutes=dt))
            if wtype is None: continue
            if rr['abs_level'].endswith('BUY') and wtype=='WHALE_SELL':
                combo_rows.append((sym, t.strftime('%Y-%m-%d %H:%M:00'), 'BREAK_DOWN'))
            elif rr['abs_level'].endswith('SELL') and wtype=='WHALE_BUY':
                combo_rows.append((sym, t.strftime('%Y-%m-%d %H:%M:00'), 'BREAK_UP'))
    conn.executemany("INSERT OR REPLACE INTO breakout_combo VALUES(?,?,?)", combo_rows)
    conn.commit()
    print(f"  дивергенций: {len(div_rows)}, whale: {len(whale_rows)}, комбо: {len(combo_rows)}")
conn.close()
print("✅ Слои A и B рассчитаны")
