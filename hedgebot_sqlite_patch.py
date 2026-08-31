#!/usr/bin/env python3
# Тест класса SQLiteStorage

import sqlite3
import json
from pathlib import Path
from datetime import datetime
from typing import Optional, List, Dict
import pandas as pd


class SQLiteStorage:
    """Класс для работы с SQLite базой данных"""
    
    def __init__(self, db_path: str = "hedgebot.db"):
        self.db_path = Path(db_path)
        self.connection = None
        self.connect()
    
    def connect(self):
        """Подключение к базе данных"""
        try:
            self.connection = sqlite3.connect(str(self.db_path), check_same_thread=False)
            self.connection.row_factory = sqlite3.Row
            print(f"✅ Подключено к SQLite: {self.db_path}")
        except Exception as e:
            print(f"❌ Ошибка подключения к SQLite: {e}")
            self.connection = None
    
    def disconnect(self):
        """Отключение от базы данных"""
        if self.connection:
            self.connection.close()
            self.connection = None
    
    # === Методы для кэша свечей ===
    
    def get_cache(self, cache_key: str) -> Optional[pd.DataFrame]:
        """Получить DataFrame из кэша"""
        if not self.connection:
            return None
        
        try:
            cursor = self.connection.cursor()
            cursor.execute(
                "SELECT data_json, timestamp FROM hedge_candles WHERE cache_key = ?",
                (cache_key,)
            )
            row = cursor.fetchone()
            
            if row:
                data_json = row['data_json']
                timestamp = row['timestamp']
                
                # Восстанавливаем DataFrame
                df_dict = json.loads(data_json)
                df = pd.DataFrame(
                    df_dict['data'],
                    columns=df_dict['columns'],
                    index=pd.DatetimeIndex(df_dict['index'])
                )
                
                print(f"✅ Кэш HIT для {cache_key} (SQLite)")
                return df
            else:
                print(f"ℹ️ Кэш MISS для {cache_key}")
                return None
                
        except Exception as e:
            print(f"❌ Ошибка чтения кэша из SQLite: {e}")
            return None
    
    def set_cache(self, cache_key: str, df: pd.DataFrame):
        """Сохранить DataFrame в кэш"""
        if not self.connection:
            return
        
        try:
            # Сериализуем DataFrame
            df_dict = {
                'index': df.index.astype(str).tolist(),
                'columns': df.columns.tolist(),
                'data': df.values.tolist()
            }
            data_json = json.dumps(df_dict)
            timestamp = datetime.now().isoformat()
            
            # Определяем имя индекса из ключа
            index_name = cache_key.split('_')[0]
            
            cursor = self.connection.cursor()
            cursor.execute(
                """INSERT OR REPLACE INTO hedge_candles 
                   (cache_key, index_name, data_json, timestamp, created_at)
                   VALUES (?, ?, ?, ?, datetime('now'))""",
                (cache_key, index_name, data_json, timestamp)
            )
            self.connection.commit()
            print(f"✅ Кэш сохранён в SQLite: {cache_key}")
            
        except Exception as e:
            print(f"❌ Ошибка сохранения кэша в SQLite: {e}")
            self.connection.rollback()
    
    # === Методы для истории сигналов ===
    
    def add_signal(self, index_name: str, signal: str, price: float, timestamp: datetime):
        """Добавить сигнал в историю"""
        if not self.connection:
            return
        
        try:
            cursor = self.connection.cursor()
            cursor.execute(
                """INSERT INTO hedge_signals 
                   (index_name, signal, price, timestamp, date, time, created_at)
                   VALUES (?, ?, ?, ?, ?, ?, datetime('now'))""",
                (
                    index_name,
                    signal,
                    price,
                    timestamp.isoformat(),
                    timestamp.strftime('%Y-%m-%d'),
                    timestamp.strftime('%H:%M:%S')
                )
            )
            self.connection.commit()
            print(f"✅ Сигнал добавлен в SQLite: {index_name} - {signal} по {price}")
            
        except Exception as e:
            print(f"❌ Ошибка добавления сигнала в SQLite: {e}")
            self.connection.rollback()
    
    def get_recent_signals(self, index_name: str, limit: int = 5) -> List[Dict]:
        """Получить последние сигналы для индекса"""
        if not self.connection:
            return []
        
        try:
            cursor = self.connection.cursor()
            cursor.execute(
                """SELECT index_name, signal, price, timestamp, date, time
                   FROM hedge_signals
                   WHERE index_name = ?
                   ORDER BY created_at DESC
                   LIMIT ?""",
                (index_name, limit)
            )
            rows = cursor.fetchall()
            
            signals = []
            for row in rows:
                signals.append({
                    'index': row['index_name'],
                    'signal': row['signal'],
                    'price': row['price'],
                    'timestamp': datetime.fromisoformat(row['timestamp']),
                    'date': row['date'],
                    'time': row['time']
                })
            
            return list(reversed(signals))  # Хронологический порядок
            
        except Exception as e:
            print(f"❌ Ошибка чтения сигналов из SQLite: {e}")
            return []
    
    def get_today_signals(self, index_name: str) -> List[Dict]:
        """Получить сегодняшние сигналы для индекса"""
        if not self.connection:
            return []
        
        try:
            today = datetime.now().strftime('%Y-%m-%d')
            cursor = self.connection.cursor()
            cursor.execute(
                """SELECT index_name, signal, price, timestamp, date, time
                   FROM hedge_signals
                   WHERE index_name = ? AND date = ?
                   ORDER BY created_at ASC""",
                (index_name, today)
            )
            rows = cursor.fetchall()
            
            signals = []
            for row in rows:
                signals.append({
                    'index': row['index_name'],
                    'signal': row['signal'],
                    'price': row['price'],
                    'timestamp': datetime.fromisoformat(row['timestamp']),
                    'date': row['date'],
                    'time': row['time']
                })
            
            return signals
            
        except Exception as e:
            print(f"❌ Ошибка чтения сегодняшних сигналов из SQLite: {e}")
            return []
    
    # === Методы для состояний бота ===
    
    def load_states(self):
        """Загрузить состояния бота"""
        if not self.connection:
            return {}, {}
        
        try:
            cursor = self.connection.cursor()
            cursor.execute(
                "SELECT states_json, global_stats_json FROM hedge_states WHERE id = 1"
            )
            row = cursor.fetchone()
            
            if row:
                states = json.loads(row['states_json'])
                global_stats = json.loads(row['global_stats_json'])
                print(f"✅ Состояния загружены из SQLite")
                return states, global_stats
            else:
                print(f"ℹ️ Состояния не найдены в SQLite")
                return {}, {}
                
        except Exception as e:
            print(f"❌ Ошибка загрузки состояний из SQLite: {e}")
            return {}, {}
    
    def save_states(self, states: Dict, global_stats: Dict):
        """Сохранить состояния бота"""
        if not self.connection:
            return
        
        try:
            states_json = json.dumps(states, default=str)
            global_stats_json = json.dumps(global_stats, default=str)
            last_save = datetime.now().isoformat()
            
            cursor = self.connection.cursor()
            cursor.execute(
                """UPDATE hedge_states 
                   SET states_json = ?, global_stats_json = ?, last_save = ?
                   WHERE id = 1""",
                (states_json, global_stats_json, last_save)
            )
            
            if cursor.rowcount == 0:
                cursor.execute(
                    """INSERT INTO hedge_states (id, states_json, global_stats_json, last_save)
                       VALUES (1, ?, ?, ?)""",
                    (states_json, global_stats_json, last_save)
                )
            
            self.connection.commit()
            print(f"✅ Состояния сохранены в SQLite")
            
        except Exception as e:
            print(f"❌ Ошибка сохранения состояний в SQLite: {e}")
            self.connection.rollback()


if __name__ == "__main__":
    print("🧪 Тестирование SQLiteStorage...")
    
    storage = SQLiteStorage("hedgebot.db")
    
    # Тест кэша
    print("\n1. Сохранение кэша:")
    df = pd.DataFrame({'open': [100, 101], 'close': [102, 103], 'high': [105, 106], 'low': [99, 100]})
    df.index = pd.date_range('2026-01-01', periods=2)
    storage.set_cache("IMOEX_candles_5", df)
    
    print("\n2. Чтение кэша:")
    df_loaded = storage.get_cache("IMOEX_candles_5")
    print(f"Загружено строк: {len(df_loaded) if df_loaded is not None else 0}")
    if df_loaded is not None:
        print(df_loaded)
    
    # Тест сигналов
    print("\n3. Добавление сигнала:")
    storage.add_signal("IMOEX", "ОТКРЫТЬ ХЕДЖ", 3250.50, datetime.now())
    storage.add_signal("CNYRUB_TOM", "ЗАКРЫТЬ ХЕДЖ", 11.47, datetime.now())
    
    print("\n4. Чтение сигналов:")
    signals = storage.get_recent_signals("IMOEX", 5)
    print(f"Получено сигналов: {len(signals)}")
    for s in signals:
        print(f"  - {s['signal']} по {s['price']}")
    
    # Тест состояний
    print("\n5. Сохранение состояний:")
    test_states = {"IMOEX": {"position": "hedge_open", "current_signal": "ОТКРЫТЬ ХЕДЖ"}}
    test_stats = {"total_signals": 1, "days_active": 5}
    storage.save_states(test_states, test_stats)
    
    print("\n6. Чтение состояний:")
    states, stats = storage.load_states()
    print(f"Состояния: {states}")
    print(f"Статистика: {stats}")
    
    storage.disconnect()
    print("\n✅ Тест завершён успешно")
