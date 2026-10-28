# compute_bybit_otc.py
# [NEW 2026-10-28] Расчет аномалий OI для Bybit (4H, только положительные)
import sqlite3
import requests
import pandas as pd
from pathlib import Path

DB_PATH = Path(__file__).parent / 'futoi.db'
SYMBOL = 'ETHUSDT'
CATEGORY = 'linear'
WINDOW_SIZE = 50
Z_THRESHOLD = 3.0

def fetch_bybit_data():
    # 1. Kline
    url_kline = f"https://api.bybit.com/v5/market/kline?category={CATEGORY}&symbol={SYMBOL}&interval=240&limit=200"
    r_k = requests.get(url_kline).json()
    if r_k['retCode'] != 0: raise Exception(f"Kline error: {r_k}")
    
    df_k = pd.DataFrame(r_k['result']['list'], columns=['timestamp', 'open', 'high', 'low', 'close', 'volume', 'turnover'])
    df_k['timestamp'] = pd.to_datetime(df_k['timestamp'].astype(float), unit='ms')
    df_k['volume'] = df_k['volume'].astype(float)
    df_k['close'] = df_k['close'].astype(float)
    df_k = df_k.set_index('timestamp').sort_index()

    # 2. Open Interest
    url_oi = f"https://api.bybit.com/v5/market/open-interest?category={CATEGORY}&symbol={SYMBOL}&intervalTime=4h&limit=200"
    r_oi = requests.get(url_oi).json()
    if r_oi['retCode'] != 0: raise Exception(f"OI error: {r_oi}")
    
    df_oi = pd.DataFrame(r_oi['result']['list'], columns=['timestamp', 'openInterest'])
    df_oi['timestamp'] = pd.to_datetime(df_oi['timestamp'].astype(float), unit='ms')
    df_oi['openInterest'] = df_oi['openInterest'].astype(float)
    df_oi = df_oi.set_index('timestamp').sort_index()

    # 3. Merge
    df = pd.merge_asof(df_k[['close', 'volume']], df_oi[['openInterest']], left_index=True, right_index=True, direction='nearest')
    return df.dropna()

def compute_and_save():
    print(f"=== Расчет Bybit OTC для {SYMBOL} ===")
    try:
        df = fetch_bybit_data()
    except Exception as e:
        print(f"❌ Ошибка загрузки данных: {e}")
        return
        
    # Считаем ΔOI
    df['delta_oi'] = df['openInterest'].diff()
    df = df.dropna()

    # Считаем Z-score скользящим окном
    df['rolling_mean'] = df['delta_oi'].rolling(window=WINDOW_SIZE, min_periods=10).mean()
    df['rolling_std'] = df['delta_oi'].rolling(window=WINDOW_SIZE, min_periods=10).std()
    df['z_score'] = (df['delta_oi'] - df['rolling_mean']) / df['rolling_std'].replace(0, pd.NA)
    
    # Флаг: только положительные аномалии
    df['is_outlier'] = ((df['delta_oi'] > 0) & (df['z_score'] > Z_THRESHOLD)).astype(int)
    
    # Сохраняем в БД
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    insert_sql = """
        INSERT OR REPLACE INTO bybit_otc_events 
        (symbol, timestamp, close, volume, open_interest, delta_oi, z_score, is_outlier)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    """
    
    saved = 0
    for ts, row in df.iterrows():
        cursor.execute(insert_sql, (
            SYMBOL,
            ts.strftime('%Y-%m-%d %H:%M:%S'),
            row['close'],
            row['volume'],
            row['openInterest'],
            row['delta_oi'],
            row['z_score'] if pd.notna(row['z_score']) else None,
            int(row['is_outlier'])
        ))
        saved += 1
        
    conn.commit()
    conn.close()
    
    outliers = df[df['is_outlier'] == 1]
    print(f"✅ Сохранено {saved} баров.")
    print(f"🔍 Найдено аномалий (Z > {Z_THRESHOLD}): {len(outliers)}")
    if not outliers.empty:
        print(outliers[['close', 'delta_oi', 'z_score']].tail(5).to_string())

if __name__ == '__main__':
    compute_and_save()