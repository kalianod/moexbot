#!/usr/bin/env python3
import requests
import pandas as pd
from datetime import datetime, timedelta
import logging
from typing import Optional

logger = logging.getLogger(__name__)

class MoexIndexDirect:
    """Прямой доступ к данным индексов MOEX через ISS API"""
    
    def __init__(self):
        self.base_url = "https://iss.moex.com/iss"
        self.session = requests.Session()
    
    def get_index_history(self, index: str = 'IMOEX', days: int = 30) -> Optional[pd.DataFrame]:
        """Получение исторических данных индекса"""
        try:
            # Правильный endpoint для исторических данных индексов
            url = f"{self.base_url}/statistics/engines/stock/markets/index/analytics/{index}.json"
            
            params = {
                'from': (datetime.now() - timedelta(days=days)).strftime('%Y-%m-%d'),
                'till': datetime.now().strftime('%Y-%m-%d'),
                'iss.meta': 'off',
                'limit': 100
            }
            
            response = self.session.get(url, params=params)
            if response.status_code == 200:
                data = response.json()
                
                # Дебаг структуры ответа
                print(f"📊 Структура ответа для {index}: {list(data.keys())}")
                
                # Ищем таблицу с данными
                for table_name, table_data in data.items():
                    if 'data' in table_data and table_data['data']:
                        print(f"�� Найдена таблица '{table_name}' с {len(table_data['data'])} записями")
                        
                        # Получаем метаданные
                        metadata = table_data.get('metadata', [])
                        if metadata:
                            columns = [col['name'] for col in metadata]
                            print(f"📝 Колонки: {columns}")
                            
                            # Создаем DataFrame
                            df = pd.DataFrame(table_data['data'], columns=columns)
                            
                            # Ищем колонки с датой и значением
                            date_cols = [col for col in df.columns if 'date' in col.lower()]
                            value_cols = [col for col in df.columns if any(x in col.lower() for x in ['close', 'value', 'last'])]
                            
                            if date_cols and value_cols:
                                date_col = date_cols[0]
                                value_col = value_cols[0]
                                
                                df['date'] = pd.to_datetime(df[date_col])
                                df.set_index('date', inplace=True)
                                df = df.sort_index()
                                
                                # Переименовываем ценовую колонку
                                df['Close'] = df[value_col]
                                
                                logger.info(f"✅ Получено {len(df)} записей для {index}")
                                return df[['Close']]
                
                print("❌ Не удалось найти подходящие данные в ответе")
                return None
            else:
                logger.error(f"❌ HTTP ошибка {response.status_code}")
                return None
                
        except Exception as e:
            logger.error(f"❌ Ошибка получения данных {index}: {e}")
            return None
    
    def get_index_current(self, index: str = 'IMOEX') -> Optional[float]:
        """Получение текущего значения индекса"""
        try:
            # Endpoint для текущих данных
            url = f"{self.base_url}/engines/stock/markets/index/boards/SNDX/securities/{index}.json"
            params = {'iss.meta': 'off'}
            
            response = self.session.get(url, params=params)
            if response.status_code == 200:
                data = response.json()
                
                # Ищем в marketdata
                if 'marketdata' in data:
                    marketdata = data['marketdata']
                    if 'data' in marketdata and marketdata['data']:
                        # Получаем колонки и данные
                        metadata = marketdata.get('metadata', [])
                        if metadata:
                            columns = [col['name'] for col in metadata]
                            row_data = marketdata['data'][0]
                            
                            # Ищем колонку с последним значением
                            value_cols = [col for col in columns if any(x in col.lower() for x in ['last', 'close', 'value'])]
                            if value_cols:
                                value_col = value_cols[0]
                                value_idx = columns.index(value_col)
                                current_value = row_data[value_idx]
                                
                                logger.info(f"✅ Текущее значение {index}: {current_value}")
                                return current_value
                
                print("❌ Не удалось найти текущее значение")
                return None
            else:
                logger.error(f"❌ HTTP ошибка {response.status_code}")
                return None
                
        except Exception as e:
            logger.error(f"❌ Ошибка получения текущего значения {index}: {e}")
            return None
    
    def explore_endpoints(self, index: str = 'IMOEX'):
        """Исследование доступных endpoint'ов"""
        print(f"\n🔍 Исследование endpoint'ов для {index}")
        print("=" * 50)
        
        endpoints = [
            f"/statistics/engines/stock/markets/index/analytics/{index}.json",
            f"/engines/stock/markets/index/boards/SNDX/securities/{index}.json",
            f"/history/engines/stock/markets/index/securities/{index}.json",
            f"/engines/stock/markets/index/boards/SNDX/securities/{index}/candles.json"
        ]
        
        for endpoint in endpoints:
            print(f"\n📡 Endpoint: {endpoint}")
            url = self.base_url + endpoint
            
            try:
                response = self.session.get(url, params={'iss.meta': 'off', 'limit': 5})
                if response.status_code == 200:
                    data = response.json()
                    print(f"✅ Статус: {response.status_code}")
                    print(f"   Ключи: {list(data.keys())}")
                    
                    for key, value in data.items():
                        if isinstance(value, dict) and 'data' in value:
                            data_count = len(value['data'])
                            print(f"   📊 Таблица '{key}': {data_count} записей")
                            if data_count > 0:
                                # Покажем первую запись для понимания структуры
                                first_record = value['data'][0]
                                print(f"   📝 Первая запись: {first_record}")
                else:
                    print(f"❌ Статус: {response.status_code}")
                    
            except Exception as e:
                print(f"❌ Ошибка: {e}")

def test_direct_api():
    """Тестирование прямого API"""
    print("🧪 Тестирование прямого доступа к MOEX API...")
    
    api = MoexIndexDirect()
    
    # Сначала исследуем endpoint'ы
    api.explore_endpoints('IMOEX')
    
    print("\n" + "="*50)
    print("📊 ТЕСТИРОВАНИЕ ОСНОВНЫХ МЕТОДОВ")
    print("="*50)
    
    # Тест 1: Текущее значение
    print("\n1. 💹 Текущее значение IMOEX...")
    current_value = api.get_index_current('IMOEX')
    if current_value:
        print(f"   ✅ Успешно: {current_value}")
    else:
        print("   ❌ Не удалось получить")
    
    # Тест 2: Исторические данные
    print("\n2. 📈 Исторические данные IMOEX...")
    history_df = api.get_index_history('IMOEX', days=10)
    if history_df is not None and len(history_df) > 0:
        print(f"   ✅ Успешно! {len(history_df)} записей")
        print(f"   📅 Период: {history_df.index[0].strftime('%Y-%m-%d')} - {history_df.index[-1].strftime('%Y-%m-%d')}")
        print(f"   💹 Значения: {history_df['Close'].min():.2f} - {history_df['Close'].max():.2f}")
        print(f"   📊 Последние 3 значения:")
        print(history_df.tail(3))
    else:
        print("   ❌ Данные не получены")
    
    # Тест 3: Другие индексы
    print("\n3. 📋 Тестирование других индексов...")
    other_indexes = ['RTSI', 'MOEX10']
    for index in other_indexes:
        print(f"\n   🔍 {index}...")
        current = api.get_index_current(index)
        if current:
            print(f"      ✅ Текущее значение: {current}")
        else:
            print(f"      ❌ Не удалось получить")

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    test_direct_api()
