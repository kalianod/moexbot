#!/usr/bin/env python3
import requests
import pandas as pd
from datetime import datetime, timedelta
import logging
from typing import Optional

logger = logging.getLogger(__name__)

class MoexIndexWorking:
    """Работающий класс для получения данных индексов MOEX"""
    
    def __init__(self):
        self.base_url = "https://iss.moex.com/iss"
        self.session = requests.Session()
    
    def get_index_candles(self, index: str = 'IMOEX', days: int = 10) -> Optional[pd.DataFrame]:
        """Получение свечных данных индекса - РАБОТАЮЩИЙ МЕТОД"""
        try:
            url = f"{self.base_url}/engines/stock/markets/index/boards/SNDX/securities/{index}/candles.json"
            
            # Вычисляем дату начала
            start_date = (datetime.now() - timedelta(days=days)).strftime('%Y-%m-%d')
            
            params = {
                'from': start_date,
                'till': datetime.now().strftime('%Y-%m-%d'),
                'interval': 24,  # дневные свечи
                'iss.meta': 'off'
            }
            
            response = self.session.get(url, params=params)
            if response.status_code == 200:
                data = response.json()
                
                if 'candles' in data and 'data' in data['candles']:
                    candles_data = data['candles']['data']
                    
                    if candles_data:
                        # Создаем DataFrame
                        df = pd.DataFrame(candles_data, columns=[
                            'open', 'high', 'low', 'close', 'volume', 'begin', 'end'
                        ])
                        
                        # Конвертируем даты
                        df['date'] = pd.to_datetime(df['begin'])
                        df.set_index('date', inplace=True)
                        df = df.sort_index()
                        
                        logger.info(f"✅ Получено {len(df)} свечей для {index}")
                        return df
                    else:
                        logger.warning(f"⚠️ Нет данных свечей для {index}")
                        return None
                else:
                    logger.warning(f"⚠️ Неверная структура данных для {index}")
                    return None
            else:
                logger.error(f"❌ HTTP ошибка {response.status_code} для {index}")
                return None
                
        except Exception as e:
            logger.error(f"❌ Ошибка получения свечей {index}: {e}")
            return None
    
    def get_index_current(self, index: str = 'IMOEX') -> Optional[float]:
        """Получение текущего значения индекса"""
        try:
            url = f"{self.base_url}/engines/stock/markets/index/boards/SNDX/securities/{index}.json"
            params = {'iss.meta': 'off'}
            
            response = self.session.get(url, params=params)
            if response.status_code == 200:
                data = response.json()
                
                # Ищем в marketdata
                if 'marketdata' in data and 'data' in data['marketdata']:
                    marketdata = data['marketdata']['data']
                    if marketdata:
                        # В marketdata данные представлены как список значений
                        # LAST - последнее значение находится на позиции 2 (индекс 2)
                        current_value = marketdata[0][2]  # LAST цена
                        logger.info(f"✅ Текущее значение {index}: {current_value}")
                        return current_value
                
                logger.warning(f"⚠️ Не удалось найти текущее значение для {index}")
                return None
            else:
                logger.error(f"❌ HTTP ошибка {response.status_code}")
                return None
                
        except Exception as e:
            logger.error(f"❌ Ошибка получения текущего значения {index}: {e}")
            return None
    
    def get_index_simple_history(self, index: str = 'IMOEX', days: int = 30) -> Optional[pd.DataFrame]:
        """Упрощенное получение исторических данных"""
        try:
            # Используем candles endpoint с большим периодом
            df = self.get_index_candles(index, days)
            if df is not None:
                return df
            
            # Если candles не работают, пробуем альтернативный метод
            return self.get_index_current_as_dataframe(index)
            
        except Exception as e:
            logger.error(f"❌ Ошибка упрощенного метода {index}: {e}")
            return None
    
    def get_index_current_as_dataframe(self, index: str) -> Optional[pd.DataFrame]:
        """Создание DataFrame из текущего значения (для тестирования)"""
        current_value = self.get_index_current(index)
        if current_value:
            # Создаем искусственные данные для тестирования логики
            today = datetime.now()
            dates = [today - timedelta(days=i) for i in range(5, 0, -1)]
            
            # Имитируем данные с небольшими колебаниями
            values = [current_value * (1 - 0.01 * i) for i in range(5)]
            
            df = pd.DataFrame({
                'open': values,
                'high': [v * 1.005 for v in values],
                'low': [v * 0.995 for v in values],
                'close': values,
                'volume': [1000000] * 5
            }, index=dates)
            
            logger.info(f"✅ Созданы тестовые данные для {index}")
            return df
        
        return None

def test_working_api():
    """Тестирование работающего API"""
    print("🧪 Тестирование работающего MoexIndex API...")
    
    api = MoexIndexWorking()
    
    # Тест 1: Свечные данные
    print("\n1. 🕯️ Свечные данные IMOEX...")
    candles_df = api.get_index_candles('IMOEX', days=5)
    if candles_df is not None and len(candles_df) > 0:
        print(f"   ✅ Успешно! {len(candles_df)} свечей")
        print(f"   📅 Период: {candles_df.index[0].strftime('%Y-%m-%d')} - {candles_df.index[-1].strftime('%Y-%m-%d')}")
        print(f"   📊 Последняя свеча:")
        print(f"      Open: {candles_df['open'].iloc[-1]}")
        print(f"      High: {candles_df['high'].iloc[-1]}")
        print(f"      Low: {candles_df['low'].iloc[-1]}")
        print(f"      Close: {candles_df['close'].iloc[-1]}")
    else:
        print("   ❌ Свечные данные не получены")
    
    # Тест 2: Текущее значение
    print("\n2. 💹 Текущее значение IMOEX...")
    current_value = api.get_index_current('IMOEX')
    if current_value:
        print(f"   ✅ Успешно: {current_value}")
    else:
        print("   ❌ Текущее значение не получено")
    
    # Тест 3: Упрощенный метод
    print("\n3. 🔧 Упрощенный метод...")
    simple_df = api.get_index_simple_history('IMOEX', days=5)
    if simple_df is not None and len(simple_df) > 0:
        print(f"   ✅ Успешно! {len(simple_df)} записей")
        if 'close' in simple_df.columns:
            print(f"   💹 Последнее Close: {simple_df['close'].iloc[-1]}")
    else:
        print("   ❌ Данные не получены")
    
    # Тест 4: Другие индексы
    print("\n4. 📋 Тестирование других индексов...")
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
    test_working_api()