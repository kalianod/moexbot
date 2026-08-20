import sqlite3
import pandas as pd
import os
from datetime import date, timedelta
from dotenv import load_dotenv
from moexalgo import Ticker, session

load_dotenv()
session.TOKEN = os.getenv('MOEXALGOPACK_TOKEN')

DB_PATH = '/home/kalian/moexbot/futoi.db'
SYMBOLS = ['SiU6', 'CRU6']

# Период для загрузки: весь июль 2026
START_DATE = date(2026, 7, 1)
END_DATE = date(2026, 7, 31)

print(f"Начинаем загрузку исторических данных FUTOI с {START_DATE} по {END_DATE}...")

# Разбиваем период на интервалы по 3 дня, чтобы не превысить лимит в 1000 строк на запрос
current_start = START_DATE
total_saved = 0

conn = sqlite3.connect(DB_PATH)

while current_start <= END_DATE:
    current_end = min(current_start + timedelta(days=3), END_DATE)
    start_str = current_start.strftime('%Y-%m-%d')
    end_str = current_end.strftime('%Y-%m-%d')
    
    print(f"\nЗапрос периода: {start_str} - {end_str}")
    
    for symbol in SYMBOLS:
        try:
            print(f"  Загрузка {symbol}...", end=" ")
            df = Ticker(symbol).futoi(start=start_str, end=end_str)
            
            if df is not None and not df.empty:
                df['symbol'] = symbol
                
                # Удаляем возможные дубликаты перед сохранением
                df = df.drop_duplicates(subset=['systime', 'clgroup'], keep='last')
                
                # Сохраняем в БД
                df.to_sql('futoi_data', conn, if_exists='append', index=False)
                saved_count = len(df)
                total_saved += saved_count
                print(f"✅ Сохранено {saved_count} строк")
            else:
                print("⚠️ Пусто")
        except Exception as e:
            print(f"❌ Ошибка: {e}")
    
    current_start = current_end + timedelta(days=1)

conn.close()
print(f"\n🎉 Загрузка завершена! Всего сохранено новых записей: {total_saved}")
