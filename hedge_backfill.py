#!/usr/bin/env python3
"""
[NEW 2026-08-30] Ретроспективный анализ индексов за 6 месяцев
Заполняет таблицы hedge_candles и hedge_signals для журнала эффективности хеджей
"""
import requests
import pandas as pd
import sqlite3
import json
from datetime import datetime, timedelta
from pathlib import Path

# ==================== КОНФИГУРАЦИЯ ====================
DB_PATH = '/home/kalian/moexbot/hedgebot.db'
BASE_URL = "https://iss.moex.com/iss"
DAYS_BACK = 180  # 6 месяцев

HEDGE_CONFIG = {
    'IMOEX': {
        'name': 'Индекс МосБиржи',
        'logic': 'standard',
        'threshold': 0.005,
        'type': 'index'
    },
    'CNYRUB_TOM': {
        'name': 'Юань/Рубль',
        'logic': 'inverse',
        'threshold': 0.005,
        'type': 'currency'
    },
    'GLDRUB_TOM': {
        'name': 'Золото/Рубль',
        'logic': 'inverse',
        'threshold': 0.005,
        'type': 'commodity'
    },
    # MCFTR оставлен в конфиге, но не обрабатывается (исключён из списка бота)
}


def get_candles(index: str, days: int = DAYS_BACK):
    """Получение дневных свечей из MOEX API"""
    try:
        config = HEDGE_CONFIG[index]
        
        if config['type'] == 'index':
            url = f"{BASE_URL}/engines/stock/markets/index/boards/SNDX/securities/{index}/candles.json"
        elif config['type'] in ('currency', 'commodity'):
            url = f"{BASE_URL}/engines/currency/markets/selt/boards/CETS/securities/{index}/candles.json"
        else:
            print(f"❌ Неизвестный тип: {config['type']}")
            return None
        
        start_date = (datetime.now() - timedelta(days=days)).strftime('%Y-%m-%d')
        params = {
            'from': start_date,
            'till': datetime.now().strftime('%Y-%m-%d'),
            'interval': 24,  # дневные
            'iss.meta': 'off'
        }
        
        response = requests.get(url, params=params, timeout=30)
        
        if response.status_code == 200:
            data = response.json()
            if 'candles' in data and 'data' in data['candles']:
                candles_data = data['candles']['data']
                if candles_data:
                    df = pd.DataFrame(candles_data, columns=[
                        'open', 'close', 'high', 'low', 'value', 'volume', 'begin', 'end'
                    ])
                    df['date'] = pd.to_datetime(df['begin'])
                    df.set_index('date', inplace=True)
                    df = df.sort_index()
                    
                    print(f"✅ {index}: получено {len(df)} свечей")
                    return df
        
        print(f"⚠️ {index}: нет данных")
        return None
        
    except Exception as e:
        print(f"❌ {index}: ошибка {e}")
        return None


def calculate_signal(df, index, threshold=0.005):
    """Расчёт сигналов хеджирования с учётом состояния (конечный автомат)"""
    signals = []
    config = HEDGE_CONFIG[index]
    logic = config['logic']
    
    # [FIX 2026-08-30] Состояние хеджа: False = закрыт, True = открыт
    hedge_open = False
    
    # Проходим по свечам начиная со 2-й (нужна предыдущая)
    for i in range(1, len(df)):
        current = df.iloc[i]
        prev = df.iloc[i - 1]
        
        current_close = current['close']
        prev_high = prev['high']
        prev_low = prev['low']
        
        # Рассчитываем сырой сигнал (без учёта состояния)
        raw_signal = None
        
        if logic == 'standard':
            # Standard: ОТКРЫТЬ если ниже минимума, ЗАКРЫТЬ если выше максимума
            buy_threshold = prev_high * (1 + threshold)
            sell_threshold = prev_low * (1 - threshold)
            
            if current_close > buy_threshold:
                raw_signal = "ЗАКРЫТЬ ХЕДЖ"
            elif current_close < sell_threshold:
                raw_signal = "ОТКРЫТЬ ХЕДЖ"
        
        elif logic == 'inverse':
            # Inverse: ОТКРЫТЬ если выше минимума, ЗАКРЫТЬ если ниже максимума
            buy_threshold = prev_low * (1 + threshold)
            sell_threshold = prev_high * (1 - threshold)
            
            if current_close > buy_threshold:
                raw_signal = "ОТКРЫТЬ ХЕДЖ"
            elif current_close < sell_threshold:
                raw_signal = "ЗАКРЫТЬ ХЕДЖ"
        
        # [FIX 2026-08-30] Применяем конечный автомат
        if raw_signal == "ОТКРЫТЬ ХЕДЖ":
            if not hedge_open:
                # Хедж был закрыт, открываем
                hedge_open = True
                signals.append({
                    'date': df.index[i].strftime('%Y-%m-%d'),
                    'time': '23:50:00',
                    'signal': "ОТКРЫТЬ ХЕДЖ",
                    'price': float(current_close)
                })
            # else: хедж уже открыт, повторный сигнал не формируем
            
        elif raw_signal == "ЗАКРЫТЬ ХЕДЖ":
            if hedge_open:
                # Хедж был открыт, закрываем
                hedge_open = False
                signals.append({
                    'date': df.index[i].strftime('%Y-%m-%d'),
                    'time': '23:50:00',
                    'signal': "ЗАКРЫТЬ ХЕДЖ",
                    'price': float(current_close)
                })
            # else: хедж уже закрыт, повторный сигнал не формируем
    
    return signals


def save_candles(index, df):
    """Сохранение свечей в SQLite"""
    try:
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        
        # Удаляем старые свечи для этого индекса
        cursor.execute("DELETE FROM hedge_candles WHERE index_name = ?", (index,))
        
        # Сериализуем DataFrame
        df_dict = {
            'index': df.index.astype(str).tolist(),
            'columns': df.columns.tolist(),
            'data': df.values.tolist()
        }
        data_json = json.dumps(df_dict)
        timestamp = datetime.now().isoformat()
        cache_key = f"{index}_candles_{DAYS_BACK}"
        
        cursor.execute(
            """INSERT OR REPLACE INTO hedge_candles 
               (cache_key, index_name, data_json, timestamp, created_at)
               VALUES (?, ?, ?, ?, datetime('now'))""",
            (cache_key, index, data_json, timestamp)
        )
        
        conn.commit()
        conn.close()
        
        print(f"✅ {index}: свечи сохранены в SQLite ({len(df)} шт.)")
        
    except Exception as e:
        print(f"❌ {index}: ошибка сохранения свечей {e}")


def save_signals(index, signals):
    """Сохранение сигналов в SQLite"""
    try:
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        
        # Получаем существующие даты сигналов, чтобы не дублировать
        cursor.execute(
            "SELECT date, time FROM hedge_signals WHERE index_name = ?",
            (index,)
        )
        existing = {(row[0], row[1]) for row in cursor.fetchall()}
        
        # Добавляем новые сигналы
        added = 0
        for sig in signals:
            key = (sig['date'], sig['time'])
            if key not in existing:
                cursor.execute(
                    """INSERT INTO hedge_signals 
                       (index_name, signal, price, timestamp, date, time, created_at)
                       VALUES (?, ?, ?, ?, ?, ?, datetime('now'))""",
                    (index, sig['signal'], sig['price'], 
                     f"{sig['date']}T{sig['time']}", sig['date'], sig['time'])
                )
                added += 1
                existing.add(key)
        
        conn.commit()
        conn.close()
        
        print(f"✅ {index}: добавлено {added} новых сигналов (всего {len(signals)} рассчитано)")
        
    except Exception as e:
        print(f"❌ {index}: ошибка сохранения сигналов {e}")


def main():
    print("🚀 Ретроспективный анализ индексов за 6 месяцев")
    print("=" * 60)
    
    total_signals = 0
    
    for index in HEDGE_CONFIG.keys():
        print(f"\n📊 Обработка {index}...")
        
        # Получаем свечи
        df = get_candles(index, DAYS_BACK)
        
        if df is None or len(df) < 2:
            print(f"⚠️ {index}: недостаточно данных")
            continue
        
        # Сохраняем свечи
        save_candles(index, df)
        
        # Рассчитываем сигналы
        signals = calculate_signal(df, index)
        print(f"📈 {index}: рассчитано {len(signals)} сигналов")
        
        # Сохраняем сигналы
        if signals:
            save_signals(index, signals)
            total_signals += len(signals)
    
    print("\n" + "=" * 60)
    print(f"🏁 Завершено. Всего сигналов добавлено: {total_signals}")
    
    # Итоговая статистика
    try:
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        
        cursor.execute("SELECT COUNT(*) FROM hedge_signals")
        total = cursor.fetchone()[0]
        
        cursor.execute("SELECT index_name, COUNT(*) FROM hedge_signals GROUP BY index_name")
        by_index = cursor.fetchall()
        
        conn.close()
        
        print(f"\n📊 Итого в БД:")
        print(f"  Всего сигналов: {total}")
        for idx, cnt in by_index:
            print(f"  {idx}: {cnt}")
            
    except Exception as e:
        print(f"❌ Ошибка статистики: {e}")


if __name__ == "__main__":
    main()
