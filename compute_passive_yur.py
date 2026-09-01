"""[NEW 2026-08-31, v2] Оценка пассивного объёма юрлиц (V1/V2 с outer merge).
V1 считает на futoi+bar_stats, V2 на futoi+tradestats — V1 работает даже без tradestats.
Запуск: python compute_passive_yur.py"""
import sqlite3
import numpy as np
import pandas as pd

DB = '/home/kalian/moexbot/futoi.db'
WINDOW = 96
MIN_PERIODS = 48
Z_SHOW = 2.0
SYMBOLS = ['SiU6', 'CRU6', 'MXU6']

conn = sqlite3.connect(DB)
conn.execute("""
CREATE TABLE IF NOT EXISTS legal_passive_estimate (
    symbol TEXT NOT NULL,
    bar_ts TEXT NOT NULL,
    delta_jur REAL,
    imb_book REAL,
    imb_aggr REAL,
    score_v1 REAL,
    z_v1 REAL,
    score_v2 REAL,
    z_v2 REAL,
    conf REAL,
    agree INTEGER,
    PRIMARY KEY(symbol, bar_ts)
)""")
conn.execute("DELETE FROM legal_passive_estimate")  # чистим перед полным пересчётом
conn.commit()


def rolling_residual(y, x, w, mp):
    """Rolling-OLS y ~ x, коэффициенты со shift(1) (без lookahead). Возвращает (resid, z)."""
    cov = y.rolling(w, min_periods=mp).cov(x)
    var = x.rolling(w, min_periods=mp).var()
    k = (cov / var).shift(1)
    a = (y.rolling(w, min_periods=mp).mean() - k * x.rolling(w, min_periods=mp).mean()).shift(1)
    k = k.bfill()
    a = a.bfill()
    resid = y - (a + k * x)
    rs = resid.rolling(w, min_periods=mp).std().shift(1).bfill()
    z = resid / rs.replace(0, np.nan)
    return resid, z


for sym in SYMBOLS:
    print(f"\n[{sym}] расчёт...")

    # 1. FUTOI YUR -> delta_jur
    f = pd.read_sql_query(
        "SELECT systime, pos_long, pos_short FROM futoi_data "
        "WHERE symbol=? AND clgroup='YUR' ORDER BY systime",
        conn, params=(sym,), parse_dates=['systime'])
    if f.empty:
        print("  нет futoi YUR"); continue
    f = f.set_index('systime').resample('5min').last().dropna()
    net = f['pos_long'] - f['pos_short']
    dj = net.diff().rename('delta_jur')

    # 2. bar_stats -> imb_book (для V1)
    bs = pd.read_sql_query(
        "SELECT tradedate, tradetime, bid_vol, ask_vol FROM bar_stats "
        "WHERE symbol=? ORDER BY tradedate, tradetime", conn, params=(sym,))
    if not bs.empty:
        bs['bar_ts'] = pd.to_datetime(bs['tradedate'] + ' ' + bs['tradetime'])
        bs = bs.set_index('bar_ts')
        bs['imb_book'] = (bs['bid_vol'] - bs['ask_vol']) / (bs['bid_vol'] + bs['ask_vol'] + 1e-9)
        df1 = pd.DataFrame(dj).join(bs[['imb_book']], how='inner').dropna()
    else:
        df1 = pd.DataFrame()

    # 3. tradestats -> disb (для V2)
    ts = pd.read_sql_query(
        "SELECT tradedate, tradetime, disb FROM tradestats_data "
        "WHERE symbol=? ORDER BY tradedate, tradetime", conn, params=(sym,))
    if not ts.empty:
        ts['bar_ts'] = pd.to_datetime(ts['tradedate'] + ' ' + ts['tradetime'])
        ts = ts.set_index('bar_ts')
        df2 = pd.DataFrame(dj).join(ts[['disb']], how='inner').dropna()
    else:
        df2 = pd.DataFrame()

    if df1.empty and df2.empty:
        print("  нет данных ни для V1 ни для V2"); continue

    # 4. V1 (стакан)
    if not df1.empty and len(df1) >= MIN_PERIODS + 10:
        r1, z1 = rolling_residual(df1['delta_jur'], df1['imb_book'], WINDOW, MIN_PERIODS)
        df1 = df1.assign(score_v1=r1, z_v1=z1)
    else:
        df1 = df1.assign(score_v1=np.nan, z_v1=np.nan)

    # 5. V2 (агрессия)
    if not df2.empty and len(df2) >= MIN_PERIODS + 10:
        r2, z2 = rolling_residual(df2['delta_jur'], df2['disb'], WINDOW, MIN_PERIODS)
        df2 = df2.assign(score_v2=r2, z_v2=z2)
    else:
        df2 = df2.assign(score_v2=np.nan, z_v2=np.nan)

    # 6. Outer merge
    df = df1.join(df2[['disb', 'score_v2', 'z_v2']], how='outer')
    df['conf'] = np.clip(np.minimum(df['z_v1'].abs(), df['z_v2'].abs()) / 3.0, 0, 1)
    df['agree'] = ((np.sign(df['z_v1']) == np.sign(df['z_v2']))
                   & df['z_v1'].notna() & df['z_v2'].notna()).astype(int)

    # 7. Пишем в БД
    rows = [(sym, t.strftime('%Y-%m-%d %H:%M:00'),
             float(r['delta_jur']) if pd.notna(r['delta_jur']) else None,
             float(r['imb_book']) if pd.notna(r['imb_book']) else None,
             float(r['disb']) if pd.notna(r['disb']) else None,
             float(r['score_v1']) if pd.notna(r['score_v1']) else None,
             float(r['z_v1']) if pd.notna(r['z_v1']) else None,
             float(r['score_v2']) if pd.notna(r['score_v2']) else None,
             float(r['z_v2']) if pd.notna(r['z_v2']) else None,
             float(r['conf']) if pd.notna(r['conf']) else None,
             int(r['agree']) if pd.notna(r['agree']) else None)
            for t, r in df.iterrows()]
    conn.executemany(
        "INSERT OR REPLACE INTO legal_passive_estimate "
        "(symbol,bar_ts,delta_jur,imb_book,imb_aggr,score_v1,z_v1,score_v2,z_v2,conf,agree) "
        "VALUES (?,?,?,?,?,?,?,?,?,?,?)", rows)
    conn.commit()

    sig1 = int(df['z_v1'].abs().ge(Z_SHOW).sum())
    sig2 = int(df['z_v2'].abs().ge(Z_SHOW).sum())
    both = int(((df['z_v1'].abs() >= Z_SHOW) & (df['z_v2'].abs() >= Z_SHOW) & (df['agree'] == 1)).sum())
    # статистика за 09.07
    day_09 = df[df.index.strftime('%Y-%m-%d') == '2026-07-09']
    print(f"  всего баров: {len(df)} | |z|>={Z_SHOW}: V1={sig1}, V2={sig2}, оба+agree={both}")
    print(f"  за 2026-07-09: {len(day_09)} баров | V1 сигналов: {int(day_09['z_v1'].abs().ge(Z_SHOW).sum())} | V2: {int(day_09['z_v2'].abs().ge(Z_SHOW).sum())}")
    print(df[['delta_jur', 'z_v1', 'z_v2', 'conf']].tail(5).round(2).to_string())

conn.close()
print("\n✅ Готово: таблица legal_passive_estimate пересчитана")
