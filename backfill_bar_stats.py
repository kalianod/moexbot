#!/usr/bin/env python3
"""
Утилита одноразовой исторической дозагрузки bar_stats.
Использование:
    python backfill_bar_stats.py 2026-07-27 2026-07-31

Не трогает основной сборщик moex_futoi_alert_multi.py.
Заполняет таблицу bar_stats в futoi.db данными OBStats + TradeStats + YUR FUTOI.
"""
import os
import sys
import sqlite3
from datetime import datetime, date, timedelta
from dotenv import load_dotenv
from moexalgo import session, Ticker
import pandas as pd
import time

DB_PATH = os.path.join(os.path.dirname(__file__), 'futoi.db')
SYMBOLS = ["SiU6", "CRU6"]

load_dotenv()
session.TOKEN = os.getenv("MOEXALGOPACK_TOKEN")


def backfill_range(symbol, start_dt, end_dt):
    """Загружает данные за диапазон дат и пишет в bar_stats."""
    fut = Ticker(symbol)
    print(f"\n[{symbol}] Загрузка {start_dt} -> {end_dt} ...")

    try:
        df_ob = fut.obstats(start=start_dt, end=end_dt)
    except Exception as e:
        print(f"  ❌ OBStats ошибка: {e}")
        df_ob = pd.DataFrame()

    try:
        df_ts = fut.tradestats(start=start_dt, end=end_dt)
    except Exception as e:
        print(f"  ❌ TradeStats ошибка: {e}")
        df_ts = pd.DataFrame()

    try:
        df_f = fut.futoi(start=start_dt, end=end_dt)
        df_yur = df_f[df_f['clgroup'] == 'YUR'].copy() if df_f is not None and not df_f.empty else pd.DataFrame()
    except Exception as e:
        print(f"  ❌ FUTOI ошибка: {e}")
        df_yur = pd.DataFrame()

    if df_ob.empty or df_ts.empty:
        print(f"  ⚠️ Недостаточно данных для {symbol} (OB/TS пусты)")
        return 0

    # Объединяем по времени
    df_ob_ts = df_ob[['tradedate', 'tradetime', 'mid_price', 'vol_b_l3', 'vol_s_l3']].copy()
    df_ob_ts.columns = ['tradedate', 'tradetime', 'price', 'bid_vol', 'ask_vol']

    df_ts_data = df_ts[['tradedate', 'tradetime', 'vol', 'oi_open', 'oi_close']].copy()
    df_ts_data['d_oi'] = df_ts_data['oi_close'] - df_ts_data['oi_open']

    df_merged = df_ob_ts.merge(df_ts_data, on=['tradedate', 'tradetime'], how='inner')

    # Подмешиваем YUR
    if not df_yur.empty:
        df_yur_agg = df_yur.groupby(['tradedate', 'tradetime']).agg(
            yur_long=('pos_long_num', 'last'),
            yur_short=('pos_short_num', 'last')
        ).reset_index()
        df_merged = df_merged.merge(df_yur_agg, on=['tradedate', 'tradetime'], how='left')

    df_merged['symbol'] = symbol
    df_merged['systime'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    df_merged['tradedate'] = df_merged['tradedate'].astype(str)
    df_merged['tradetime'] = df_merged['tradetime'].astype(str)

    cols = ['symbol', 'tradedate', 'tradetime', 'price', 'vol', 'bid_vol', 'ask_vol',
            'd_oi', 'yur_long', 'yur_short', 'systime']
    existing_cols = [c for c in cols if c in df_merged.columns]
    df_to_save = df_merged[existing_cols]

    # Запись в БД
    conn = sqlite3.connect(DB_PATH)
    inserted = 0
    try:
        for _, row in df_to_save.iterrows():
            cols_str = ', '.join(row.index)
            placeholders = ', '.join(['?'] * len(row))
            sql = f"INSERT OR REPLACE INTO bar_stats ({cols_str}) VALUES ({placeholders})"
            conn.execute(sql, tuple(row))
            inserted += 1
        conn.commit()
        print(f"  ✅ Сохранено {inserted} строк в bar_stats")
    except Exception as e:
        print(f"  ❌ Ошибка записи: {e}")
    finally:
        conn.close()

    return inserted


def main():
    if len(sys.argv) < 3:
        print("Использование: python backfill_bar_stats.py YYYY-MM-DD YYYY-MM-DD")
        print("Пример: python backfill_bar_stats.py 2026-07-27 2026-07-31")
        sys.exit(1)

    start_dt = datetime.strptime(sys.argv[1], '%Y-%m-%d').date()
    end_dt = datetime.strptime(sys.argv[2], '%Y-%m-%d').date()
    print(f"📅 Диапазон: {start_dt} -> {end_dt}")
    print(f"📈 Тикеры: {SYMBOLS}")

    total = 0
    for symbol in SYMBOLS:
        total += backfill_range(symbol, start_dt, end_dt)
        time.sleep(2)  # пауза между тикерами

    print(f"\n🏁 Итого сохранено: {total} строк")


if __name__ == "__main__":
    main()
