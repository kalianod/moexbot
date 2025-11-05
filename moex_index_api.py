#!/usr/bin/env python3
import asyncio
import aiohttp
import pandas as pd
from datetime import datetime, timedelta
import logging
from typing import Optional, Dict, List

logger = logging.getLogger(__name__)

class MoexIndexAPI:
    """Класс для работы с данными индексов MOEX через прямое API"""
    
    def __init__(self):
        self.base_url = "https://iss.moex.com/iss"
        self.session = None
    
    async def __aenter__(self):
        self.session = aiohttp.ClientSession()
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self.session:
            await self.session.close()
    
    async def get_index_history(self, index: str = 'IMOEX', days: int = 30) -> Optional[pd.DataFrame]:
        """Получение исторических данных индекса"""
        try:
            url = f"{self.base_url}/history/engines/stock/markets/index/boards/SNDX/securities/{index}.json"
            
            params = {
                'from': (datetime.now() - timedelta(days=days)).strftime('%Y-%m-%d'),
                'till': datetime.now().strftime('%Y-%m-%d'),
                'interval': 24
            }
            
            async with self.session.get(url, params=params) as response:
                if response.status == 200:
                    data = await response.json()
                    
                    # Ищем таблицу с историческими данными
                    for table_name, table_data in data.items():
                        if 'history' in table_name and 'data' in table_data and len(table_data['data']) > 0:
                            columns = [col['name'] for col in table_data['metadata']]
                            df = pd.DataFrame(table_data['data'], columns=columns)
                            
                            # Конвертируем даты и сортируем
                            if 'TRADEDATE' in df.columns:
                                df['TRADEDATE'] = pd.to_datetime(df['TRADEDATE'])
                                df = df.sort_values('TRADEDATE')
                                df.set_index('TRADEDATE', inplace=True)
                            
                            logger.info(f"✅ Получено {len(df)} исторических записей для {index}")
                            return df
            
            logger.warning(f"⚠️ Не удалось получить исторические данные для {index}")
            return None
            
        except Exception as e:
            logger.error(f"❌ Ошибка получения исторических данных {index}: {e}")
            return None
    
    async def get_index_candles(self, index: str = 'IMOEX', days: int = 10) -> Optional[pd.DataFrame]:
        """Получение свечных данных индекса"""
        try:
            url = f"{self.base_url}/engines/stock/markets/index/boards/SNDX/securities/{index}/candles.json"
            
            params = {
                'from': (datetime.now() - timedelta(days=days)).strftime('%Y-%m-%d'),
                'till': datetime.now().strftime('%Y-%m-%d'),
                'interval': 24
            }
            
            async with self.session.get(url, params=params) as response:
                if response.status == 200:
                    data = await response.json()
                    
                    if 'candles' in data:
                        candles_data = data['candles']
                        if 'data' in candles_data and len(candles_data['data']) > 0:
                            columns = [col['name'] for col in candles_data['metadata']]
                            df = pd.DataFrame(candles_data['data'], columns=columns)
                            
                            # Конвертируем даты и переименовываем колонки
                            if 'begin' in df.columns:
                                df['date'] = pd.to_datetime(df['begin'])
                                df.set_index('date', inplace=True)
                                df = df.sort_index()
                                
                                # Стандартизируем названия колонок
                                column_mapping = {
                                    'open': 'Open',
                                    'high': 'High', 
                                    'low': 'Low',
                                    'close': 'Close',
                                    'volume': 'Volume'
                                }
                                
                                for ru_col, en_col in column_mapping.items():
                                    if ru_col in df.columns:
                                        df[en_col] = df[ru_col]
                            
                            logger.info(f"✅ Получено {len(df)} свечей для {index}")
                            return df
            
            logger.warning(f"⚠️ Не удалось получить свечные данные для {index}")
            return None
            
        except Exception as e:
            logger.error(f"❌ Ошибка получения свечных данных {index}: {e}")
            return None
    
    async def get_available_indexes(self) -> List[Dict]:
        """Получение списка доступных индексов"""
        try:
            url = f"{self.base_url}/statistics/engines/stock/markets/index/analytics.json"
            
            async with self.session.get(url) as response:
                if response.status == 200:
                    data = await response.json()
                    
                    if 'analytics' in data:
                        analytics_data = data['analytics']
                        if 'data' in analytics_data and len(analytics_data['data']) > 0:
                            columns = [col['name'] for col in analytics_data['metadata']]
                            df = pd.DataFrame(analytics_data['data'], columns=columns)
                            
                            indexes = []
                            for _, row in df.iterrows():
                                indexes.append({
                                    'secid': row.get('secid'),
                                    'shortname': row.get('shortname'),
                                    'boardid': row.get('boardid')
                                })
                            
                            logger.info(f"✅ Найдено {len(indexes)} индексов")
                            return indexes
            
            return []
            
        except Exception as e:
            logger.error(f"❌ Ошибка получения списка индексов: {e}")
            return []

async def test_index_api():
    """Тестирование API индексов"""
    print("🧪 Тестирование MoexIndexAPI...")
    
    async with MoexIndexAPI() as api:
        # Тест 1: Список индексов
        print("\n1. 📋 Получение списка индексов...")
        indexes = await api.get_available_indexes()
        if indexes:
            popular = [idx for idx in indexes if idx['secid'] in ['IMOEX', 'RTSI', 'MOEX10', 'RGBI']]
            for idx in popular:
                print(f"   📈 {idx['secid']}: {idx.get('shortname', 'N/A')}")
        
        # Тест 2: Исторические данные IMOEX
        print("\n2. 📈 Исторические данные IMOEX...")
        history_df = await api.get_index_history('IMOEX', days=5)
        if history_df is not None and len(history_df) > 0:
            print(f"   ✅ Получено {len(history_df)} записей")
            print(f"   📅 Период: {history_df.index[0].strftime('%Y-%m-%d')} - {history_df.index[-1].strftime('%Y-%m-%d')}")
            if 'CLOSE' in history_df.columns:
                print(f"   💹 Последнее значение: {history_df['CLOSE'].iloc[-1]}")
        
        # Тест 3: Свечные данные IMOEX
        print("\n3. 🕯️ Свечные данные IMOEX...")
        candles_df = await api.get_index_candles('IMOEX', days=5)
        if candles_df is not None and len(candles_df) > 0:
            print(f"   ✅ Получено {len(candles_df)} свечей")
            print(f"   📊 Последняя свеча:")
            print(f"      Open: {candles_df['Open'].iloc[-1]}")
            print(f"      High: {candles_df['High'].iloc[-1]}")
            print(f"      Low: {candles_df['Low'].iloc[-1]}")
            print(f"      Close: {candles_df['Close'].iloc[-1]}")

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    asyncio.run(test_index_api())
