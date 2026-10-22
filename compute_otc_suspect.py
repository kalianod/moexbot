#!/usr/bin/env python3
"""
Вычисляет флаг «подозрение на OTC» по методике ATAS:
    OTC_SUSPECT = 1, если |ΔOI| > k × Volume
Источники:
  - futoi_data: суммарный OI = SUM(pos_long) по FIZ+YUR каждые 5 мин
  - bar_stats: биржевой объём vol за те же 5-мин бары
Стыковка: tradetime = конец бара [T-5мин, T) для обеих таблиц.
Записывает результаты в таблицу otc_suspect_events (UPSERT).
"""
import sqlite3
import pandas as pd
from pathlib import Path
from datetime import datetime

# ==== Пути ====
BASE_DIR = Path(__file__).parent
DB_PATH = BASE_DIR / 'futoi.db'

# ==== Параметры методики ====
K_MAIN = 2.0      # основной порог: |ΔOI| > 2×V
K_STRICT = 4.0    # строгий порог: |ΔOI| > 4×V (для выделения насыщенных цветов)
MIN_VOLUME = 50   # отсечь шумные бары с минимальной активностью

# ==== Таблица результата ====
TABLE_NAME = 'otc_suspect_events'

CREATE_TABLE_SQL = f"""
CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    symbol TEXT NOT NULL,
    bar_ts DATETIME NOT NULL,
    oi_total INTEGER NOT NULL,
    oi_prev INTEGER,
    delta_oi INTEGER,
    volume INTEGER,
    delta_oi_ratio REAL,
    otc_suspect INTEGER NOT NULL DEFAULT 0,
    otc_strict INTEGER NOT NULL DEFAULT 0,
    oi_direction TEXT,
    threshold_k REAL DEFAULT {K_MAIN},
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(symbol, bar_ts)
);
CREATE INDEX IF NOT EXISTS idx_otc_suspect_symbol_ts 
    ON {TABLE_NAME}(symbol, bar_ts);
"""

def ensure_table():
    """Создаёт таблицу и индексы, если их нет."""
    conn = sqlite3.connect(DB_PATH)
    try:
        conn.executescript(CREATE_TABLE_SQL)
        conn.commit()
    finally:
        conn.close()

def compute_for_symbol(symbol: str, days: int = 3):
    """
    Для заданного символа за последние N дней:
    1. Читает futoi_data и bar_stats
    2. Считает OI_t = sum(pos_long), ΔOI_t, V_t
    3. Ставит флаги otc_suspect и otc_strict
    4. Записывает в otc_suspect_events (UPSERT)
    Возвращает: (всего_баров, с_флагом_k2, с_флагом_k4)
    """
    conn = sqlite3.connect(DB_PATH)
    try:
        # 1. Суммарный OI на каждый снапшот
        # [FIX 2026-10-22] Защита от дублей: берем MAX(pos_long) по каждой группе (FIZ/YUR), а потом суммируем
        oi = pd.read_sql_query("""
            SELECT tradedate, tradetime, SUM(max_pos) AS oi_total
            FROM (
                SELECT tradedate, tradetime, clgroup, MAX(pos_long) AS max_pos
                FROM futoi_data 
                WHERE symbol=? AND tradedate >= date('now', ?)
                GROUP BY tradedate, tradetime, clgroup
            )
            GROUP BY tradedate, tradetime
            ORDER BY tradedate, tradetime
        """, conn, params=[symbol, f'-{days} days'])
        
        if oi.empty:
            return (0, 0, 0)
        
        oi['dt'] = pd.to_datetime(oi['tradedate'] + ' ' + oi['tradetime'])
        oi = oi.sort_values('dt').reset_index(drop=True)
        
        # [FIX 2026-10-22] Считаем дельту ТОЛЬКО внутри одного дня, чтобы не ловить ночные/выходные гэпы
        oi['delta_oi'] = oi.groupby('tradedate')['oi_total'].diff()
        oi['oi_prev'] = oi.groupby('tradedate')['oi_total'].shift(1)
        
        # 2. Объём из bar_stats (стыковка: tradetime = конец бара)
        bs = pd.read_sql_query("""
            SELECT tradedate, tradetime, vol
            FROM bar_stats 
            WHERE symbol=? AND tradedate >= date('now', ?)
        """, conn, params=[symbol, f'-{days} days'])
        
        if bs.empty:
            return (0, 0, 0)
        
        bs['dt'] = pd.to_datetime(bs['tradedate'] + ' ' + bs['tradetime'])
        
        # 3. JOIN по dt
        merged = oi.merge(bs[['dt', 'vol']], on='dt', how='left')
        merged['bar_start'] = merged['dt'] - pd.Timedelta(minutes=5)
        
        # 4. Расчёт флагов
        merged['ratio'] = merged['delta_oi'].abs() / merged['vol'].replace(0, pd.NA)
        merged['otc_suspect'] = (
            (merged['vol'] >= MIN_VOLUME) & 
            (merged['ratio'] > K_MAIN)
        ).astype(int)
        merged['otc_strict'] = (
            (merged['vol'] >= MIN_VOLUME) & 
            (merged['ratio'] > K_STRICT)
        ).astype(int)
        
        # 5. Направление
        merged['oi_direction'] = merged['delta_oi'].apply(
            lambda x: 'open' if pd.notna(x) and x > 0 else 
                     ('close' if pd.notna(x) and x < 0 else 'flat')
        )
        
        # 6. UPSERT в БД
        rows_to_write = merged[merged['delta_oi'].notna()].copy()
        if rows_to_write.empty:
            return (len(merged), 0, 0)
        
        upsert_sql = f"""
            INSERT INTO {TABLE_NAME} 
                (symbol, bar_ts, oi_total, oi_prev, delta_oi, volume, 
                 delta_oi_ratio, otc_suspect, otc_strict, oi_direction, threshold_k)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(symbol, bar_ts) DO UPDATE SET
                oi_total=excluded.oi_total,
                oi_prev=excluded.oi_prev,
                delta_oi=excluded.delta_oi,
                volume=excluded.volume,
                delta_oi_ratio=excluded.delta_oi_ratio,
                otc_suspect=excluded.otc_suspect,
                otc_strict=excluded.otc_strict,
                oi_direction=excluded.oi_direction,
                threshold_k=excluded.threshold_k,
                updated_at=CURRENT_TIMESTAMP
        """
        
        data = []
        for _, row in rows_to_write.iterrows():
            data.append((
                symbol,
                row['bar_start'].strftime('%Y-%m-%d %H:%M:%S'),
                int(row['oi_total']),
                int(row['oi_prev']) if pd.notna(row['oi_prev']) else None,
                int(row['delta_oi']) if pd.notna(row['delta_oi']) else None,
                int(row['vol']) if pd.notna(row['vol']) else None,
                float(row['ratio']) if pd.notna(row['ratio']) else None,
                int(row['otc_suspect']),
                int(row['otc_strict']),
                row['oi_direction'],
                K_MAIN
            ))
        
        conn.executemany(upsert_sql, data)
        conn.commit()
        
        total_bars = len(rows_to_write)
        flagged_k2 = int(rows_to_write['otc_suspect'].sum())
        flagged_k4 = int(rows_to_write['otc_strict'].sum())
        
        return (total_bars, flagged_k2, flagged_k4)
        
    finally:
        conn.close()

def main():
    """Точка входа: обрабатывает все активные символы."""
    import sys
    sys.path.insert(0, str(BASE_DIR))
    from manage_symbols import list_symbols
    
    ensure_table()
    
    symbols = list_symbols()
    print(f"[{datetime.now():%Y-%m-%d %H:%M:%S}] Расчёт OTC-suspect для {len(symbols)} символов...")
    
    total_stats = {'bars': 0, 'k2': 0, 'k4': 0}
    
    for sym in symbols:
        try:
            bars, k2, k4 = compute_for_symbol(sym, days=3)
            total_stats['bars'] += bars
            total_stats['k2'] += k2
            total_stats['k4'] += k4
            print(f"  {sym}: {bars} баров, {k2} с флагом k=2, {k4} с флагом k=4")
        except Exception as e:
            print(f"  {sym}: ОШИБКА — {e}")
    
    print(f"\nИтого: {total_stats['bars']} баров, "
          f"{total_stats['k2']} с otc_suspect (k=2), "
          f"{total_stats['k4']} с otc_strict (k=4)")

if __name__ == '__main__':
    main()
