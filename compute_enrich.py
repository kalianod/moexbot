"""[NEW] Дописывает обогащающие колонки в legal_passive_estimate (UPDATE, без перезаписи).
Восстанавливает: aggr_abs, score_v25, z_v25, z_v25_mad, z_dj_mad, d_long, d_short, d_gross, class."""
import sqlite3
import numpy as np
import pandas as pd

DB = '/home/kalian/moexbot/futoi.db'
SYMBOLS = ['SiU6', 'CRU6', 'MXU6']
W, MP = 96, 48

def mad(x):
    m = np.median(x); return np.median(np.abs(x - m))
def robust_z(s, w=W, mp=MP):
    med = s.rolling(w, min_periods=mp).median().shift(1)
    m = s.rolling(w, min_periods=mp).apply(mad, raw=True).shift(1)
    return (s - med) / (1.4826 * m).where((1.4826 * m) > 1e-8)

def rolling_residual2(y, x1, x2, w, mp):
    n = len(y); res = np.full(n, np.nan)
    X = np.column_stack([np.ones(n), x1, x2])
    for i in range(w, n):
        win = slice(i - w, i)  # shift(1): коэффициенты без текущей строки
        beta, *_ = np.linalg.lstsq(X[win], y[win], rcond=None)
        res[i] = y[i] - X[i] @ beta
    return res

conn = sqlite3.connect(DB)
for sym in SYMBOLS:
    lpe = pd.read_sql_query("SELECT bar_ts, delta_jur FROM legal_passive_estimate "
        "WHERE symbol=? ORDER BY bar_ts", conn, params=(sym,), parse_dates=['bar_ts']).set_index('bar_ts')
    yur = pd.read_sql_query("SELECT systime,pos_long,pos_short FROM futoi_data "
        "WHERE symbol=? AND clgroup='YUR' ORDER BY systime",
        conn, params=(sym,), parse_dates=['systime']).set_index('systime').resample('5min').last()
    ts = pd.read_sql_query("SELECT tradedate,tradetime,vol_b,vol_s FROM tradestats_data "
        "WHERE symbol=? ORDER BY tradedate,tradetime", conn, params=(sym,))
    ts['bar_ts'] = pd.to_datetime(ts['tradedate'] + ' ' + ts['tradetime'])
    ts = ts.set_index('bar_ts'); ts = ts[~ts.index.duplicated(keep='last')]

    df = lpe.join(yur[['pos_long','pos_short']], how='left')
    df['d_long'] = df['pos_long'].diff(); df['d_short'] = df['pos_short'].diff()
    df['d_gross'] = df['d_long'] + df['d_short']
    df = df.join(ts[['vol_b','vol_s']], how='left')
    df['aggr_abs'] = df['vol_b'] - df['vol_s']
    df = df.dropna(subset=['delta_jur','aggr_abs'])

    g = df['vol_b'] + df['vol_s']
    df['score_v25'] = rolling_residual2(df['delta_jur'].values, df['aggr_abs'].values, g.values, W, MP)
    sz = pd.Series(df['score_v25'].values, index=df.index)
    df['z_v25'] = ((sz - sz.rolling(W,MP).mean().shift(1)) / sz.rolling(W,MP).std().shift(1)).values
    df['z_v25_mad'] = robust_z(sz).values
    df['z_dj_mad'] = robust_z(df['delta_jur']).values

    # class: directional buy/sell (для абсорбции)
    th_l = df['d_long'].abs().rolling(W,MP).quantile(0.75).shift(1)
    th_s = df['d_short'].abs().rolling(W,MP).quantile(0.75).shift(1)
    def cls(r):
        if pd.isna(r['d_long']) or pd.isna(th_l.get(r.name, np.nan)): return 'other'
        if r['d_long'] > th_l.get(r.name, 1e9) and r['d_short'] < -th_s.get(r.name, 1e9): return 'buy_directional'
        if r['d_long'] < -th_l.get(r.name, 1e9) and r['d_short'] > th_s.get(r.name, 1e9): return 'sell_directional'
        return 'other'
    df['class'] = [cls(r) for _, r in df.iterrows()]

    upd = [(float(r['aggr_abs']), float(r['score_v25']) if pd.notna(r['score_v25']) else None,
            float(r['z_v25']) if pd.notna(r['z_v25']) else None,
            float(r['z_v25_mad']) if pd.notna(r['z_v25_mad']) else None,
            float(r['z_dj_mad']) if pd.notna(r['z_dj_mad']) else None,
            float(r['d_long']), float(r['d_short']), float(r['d_gross']), r['class'],
            sym, t.strftime('%Y-%m-%d %H:%M:00')) for t, r in df.iterrows()]
    conn.executemany("UPDATE legal_passive_estimate SET aggr_abs=?, score_v25=?, z_v25=?, z_v25_mad=?, "
                     "z_dj_mad=?, d_long=?, d_short=?, d_gross=?, class=? WHERE symbol=? AND bar_ts=?", upd)
    conn.commit()
    n_dir = (df['class'] != 'other').sum()
    print(f"[{sym}] обогащено строк: {len(upd)}, directional: {n_dir}")
conn.close()
print("✅ enrich готов")
