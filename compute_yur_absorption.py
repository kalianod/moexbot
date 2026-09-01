"""[NEW 2026-09-01] Absorption V3: YUR direction + opposite aggression + price resilience.
Запуск: python compute_yur_absorption.py"""
import sqlite3
import numpy as np
import pandas as pd

DB = '/home/kalian/moexbot/futoi.db'
SYMBOLS = ['SiU6', 'CRU6', 'MXU6']
W, MP = 96, 48

def mad(x):
    m = np.median(x); return np.median(np.abs(x - m))

def robust_z(s, w=W, mp=MP):
    """Robust z-score: rolling median/MAD с shift(1)."""
    med = s.rolling(w, min_periods=mp).median().shift(1)
    m = s.rolling(w, min_periods=mp).apply(mad, raw=True).shift(1)
    denom = 1.4826 * m
    z = (s - med) / denom.where(denom > 1e-8)
    return z

conn = sqlite3.connect(DB)
conn.execute("""
CREATE TABLE IF NOT EXISTS yur_absorption (
    symbol TEXT NOT NULL,
    bar_ts TEXT NOT NULL,
    -- YUR direction
    class TEXT,
    jur_z REAL,
    -- Aggression
    aggr_abs REAL,
    aggr_z REAL,
    -- Price
    ret_bp REAL,
    resilience_bp REAL,
    resilience_z REAL,
    -- Absorption score
    abs_buy_score REAL,
    abs_sell_score REAL,
    abs_level TEXT,
    PRIMARY KEY(symbol, bar_ts)
)""")
conn.execute("DELETE FROM yur_absorption")
conn.commit()

for sym in SYMBOLS:
    print(f"\n[{sym}] расчёт абсорбции...")
    
    # 1. Данные из legal_passive_estimate
    lpe = pd.read_sql_query("""
        SELECT bar_ts, delta_jur, class, z_dj_mad, aggr_abs, imb_book
        FROM legal_passive_estimate
        WHERE symbol=?
        ORDER BY bar_ts
    """, conn, params=(sym,), parse_dates=['bar_ts']).set_index('bar_ts')
    
    # 2. Свечи для доходности бара
    cnd = pd.read_sql_query("""
        SELECT begin, open, close
        FROM candles_cache
        WHERE symbol=? AND period='5min'
        ORDER BY begin
    """, conn, params=(sym,), parse_dates=['begin']).set_index('begin').sort_index()
    
    cnd['ret_bp'] = (cnd['close'] / cnd['open'] - 1) * 1e4
    cnd.index = cnd.index + pd.Timedelta(minutes=5)  # align: begin + 5min = bar_ts
    
    # 3. Merge
    df = lpe.join(cnd[['ret_bp']], how='inner').dropna(subset=['aggr_abs', 'ret_bp']).copy()
    
    # 4. Robust z-scores
    df['jur_z'] = df['z_dj_mad']
    df['aggr_z'] = robust_z(df['aggr_abs'])
    
    # 5. Модель ожидаемой реакции цены: ret_bp = a + b * aggr_abs (rolling OLS, shift 1)
    cov = df['ret_bp'].rolling(W, min_periods=MP).cov(df['aggr_abs'])
    var = df['aggr_abs'].rolling(W, min_periods=MP).var()
    b = (cov / var).shift(1)
    a = (df['ret_bp'].rolling(W, min_periods=MP).mean() - 
         b * df['aggr_abs'].rolling(W, min_periods=MP).mean()).shift(1)
    a, b = a.bfill(), b.bfill()
    
    df['expected_ret_bp'] = a + b * df['aggr_abs']
    df['resilience_bp'] = df['ret_bp'] - df['expected_ret_bp']
    df['resilience_z'] = robust_z(df['resilience_bp'])
    
    # 6. Absorption scores (4 группы)
    # BUY absorption: YUR BUY + opposite aggression (SELL) + price resilience
    df['abs_buy_score'] = (
        df['jur_z'].clip(lower=0) *          # YUR покупает
        (-df['aggr_z']).clip(lower=0) *      # агрессия продажа
        df['resilience_z'].clip(lower=0)     # цена не падает
    )
    
    # SELL absorption: YUR SELL + opposite aggression (BUY) + price resilience
    df['abs_sell_score'] = (
        (-df['jur_z']).clip(lower=0) *       # YUR продаёт
        df['aggr_z'].clip(lower=0) *         # агрессия покупка
        (-df['resilience_z']).clip(lower=0)  # цена не растёт
    )
    
    # 7. Levels (L1, L2, L3)
    def classify_absorption(row):
        # BUY
        if row['class'] == 'buy_directional':
            if row['aggr_z'] <= -1.5 and row['resilience_z'] >= 1.5:
                return 'L3_BUY'
            elif row['aggr_z'] <= -1.0 and row['resilience_z'] >= 0:
                return 'L2_BUY'
            elif row['aggr_z'] < 0:
                return 'L1_BUY'
        # SELL
        elif row['class'] == 'sell_directional':
            if row['aggr_z'] >= 1.5 and row['resilience_z'] <= -1.5:
                return 'L3_SELL'
            elif row['aggr_z'] >= 1.0 and row['resilience_z'] <= 0:
                return 'L2_SELL'
            elif row['aggr_z'] > 0:
                return 'L1_SELL'
        return None
    
    df['abs_level'] = df.apply(classify_absorption, axis=1)
    
    # 8. Статистика
    for lvl in ['L1_BUY', 'L2_BUY', 'L3_BUY', 'L1_SELL', 'L2_SELL', 'L3_SELL']:
        n = (df['abs_level'] == lvl).sum()
        print(f"  {lvl}: {n:5d} ({n/len(df):.1%})")
    
    # 9. Запись в БД
    rows = [(sym, t.strftime('%Y-%m-%d %H:%M:00'),
             r['class'], float(r['jur_z']),
             float(r['aggr_abs']), float(r['aggr_z']),
             float(r['ret_bp']), float(r['resilience_bp']), float(r['resilience_z']),
             float(r['abs_buy_score']), float(r['abs_sell_score']),
             r['abs_level'])
            for t, r in df.iterrows()]
    
    conn.executemany("""
        INSERT INTO yur_absorption
        (symbol, bar_ts, class, jur_z, aggr_abs, aggr_z, ret_bp, resilience_bp, resilience_z,
         abs_buy_score, abs_sell_score, abs_level)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, rows)
    conn.commit()
    print(f"  ✅ сохранено {len(rows)} строк")

conn.close()
print("\n✅ Absorption V3 готов: таблица yur_absorption")
