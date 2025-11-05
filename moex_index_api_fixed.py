#!/usr/bin/env python3
import asyncio
import aiohttp
import pandas as pd
from datetime import datetime, timedelta
import logging
import json
from typing import Optional, Dict, List

logger = logging.getLogger(__name__)

class MoexIndexAPIFixed:
    """Исправленный класс для работы с данными индексов MOEX"""
    
    def __init__(self):
        self.base_url = "https://iss.moex.com/iss"
        self.session = None
    
    async def __aenter__(self):
        self.session = aiohttp.ClientSession()
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self.session:
            await self.session.close()
    
    async def _make_request(self, url: str, params: dict = None) -> dict:
        """Универсальный метод для запросов к MOEX API"""
        try:
            async with self.session.get(url, params=params) as response:
                if response.status == 200:
                    # MOEX API может возвращать разные форматы, пробуем JSON
                    content_type = response.headers.get('content-type', '')
                    
                    if 'application/json' in content_type:
                        return await response.json()
                    else:
                        # Если не JSON, пробуем распарсить как текст
                        text = await response.text()
                        try:
                            return json.loads(text)
                        except json.JSONDecodeError:
                            # Если это не JSON, возвращаем текст для дебага
                            logger.warning(f"⚠️ Ответ не в JSON формате: {text[:200]}...")
                            return {'raw_text': text}
                else:
                    logger.error(f"❌ HTTP ошибка {response.status} для {url}")
                    return None
                    
        except Exception as e:
            logger.error(f"❌ Ошибка запроса {url}: {e}")
            return None
    
    async def get_index_history(self, index: str = 'IMOEX', days: int = 30) -> Optional[pd.DataFrame]:
        """Получение исторических данных индекса - исправленная версия"""
        try:
            url = f"{self.base_url}/history/engines/stock/markets/index/boards/SNDX/securities/{index}.json"
            
            params = {
                'from': (datetime.now() - timedelta(days=days)).strftime('%Y-%m-%d'),
                'till': datetime.now().strftime('%Y-%m-%d'),
                'interval': 24
            }
            
            data = await self._make_request(url, params)
            if not data:
                return None
            
            # Дебаг: посмотрим структуру ответа
            logger.info(f"📊 Структура ответа: {list(data.keys())}")
            
            # MOEX API возвращает данные в специфическом формате
            # Ищем таблицу с историческими данными
            for key in data:
                if 'history' in key.lower():
                    table_data = data[key]
                    if 'data' in table_data and table_data['data']:
                        columns = [col['name'] for col in table_data.get('metadata', [])]
                        df = pd.DataFrame(table_data['data'], columns=columns)
                        
                        # Конвертируем даты
                        if 'TRADEDATE' in df.columns:
                            df['date'] = pd.to_datetime(df['TRADEDATE'])
                            df.set_index('date', inplace=True)
                            df = df.sort_index()
                            
                            logger.info(f"✅ Получено {len(df)} исторических записей для {index}")
                            logger.info(f"📋 Колонки: {df.columns.tolist()}")
                            return df
            
            # Альтернативный поиск данных
            for key, value in data.items():
                if isinstance(value, dict) and 'data' in value and value['data']:
                    # Пробуем создать DataFrame из любой таблицы с данными
                    try:
                        columns = [col['name'] for col in value.get('metadata', [])]
                        df = pd.DataFrame(value['data'], columns=columns)
                        
                        # Ищем колонку с датой
                        date_columns = [col for col in df.columns if 'date' in col.lower() or 'begin' in col.lower()]
                        if date_columns:
                            date_col = date_columns[0]
                            df['date'] = pd.to_datetime(df[date_col])
                            df.set_index('date', inplace=True)
                            df = df.sort_index()
                            
                            logger.info(f"✅ Альтернативный метод: {len(df)} записей для {index}")
                            return df
                    except Exception as e:
                        logger.warning(f"⚠️ Не удалось обработать таблицу {key}: {e}")
                        continue
            
            logger.warning(f"⚠️ Не удалось найти исторические данные для {index}")
            return None
            
        except Exception as e:
            logger.error(f"❌ Ошибка получения исторических данных {index}: {e}")
            return None
    
    async def get_index_candles(self, index: str = 'IMOEX', days: int = 10) -> Optional[pd.DataFrame]:
        """Получение свечных данных индекса - исправленная версия"""
        try:
            url = f"{self.base_url}/engines/stock/markets/index/boards/SNDX/securities/{index}/candles.json"
            
            params = {
                'from': (datetime.now() - timedelta(days=days)).strftime('%Y-%m-%d'),
                'till': datetime.now().strftime('%Y-%m-%d'),
                'interval': 24
            }
            
            data = await self._make_request(url, params)
            if not data:
                return None
            
            logger.info(f"📊 Структура свечных данных: {list(data.keys())}")
            
            # Ищем таблицу со свечами
            if 'candles' in data:
                candles_data = data['candles']
                if 'data' in candles_data and candles_data['data']:
                    columns = [col['name'] for col in candles_data.get('metadata', [])]
                    df = pd.DataFrame(candles_data['data'], columns=columns)
                    
                    # Конвертируем даты
                    if 'begin' in df.columns:
                        df['date'] = pd.to_datetime(df['begin'])
                        df.set_index('date', inplace=True)
                        df = df.sort_index()
                        
                        # Стандартизируем названия колонок
                        column_mapping = {
                            'open': 'Open', 'high': 'High', 'low': 'Low', 'close': 'Close', 'value': 'Volume'
                        }
                        
                        for ru_col, en_col in column_mapping.items():
                            if ru_col in df.columns:
                                df[en_col] = df[ru_col]
                        
                        logger.info(f"✅ Получено {len(df)} свечей для {index}")
                        logger.info(f"📋 Колонки свечей: {df.columns.tolist()}")
                        return df
            
            logger.warning(f"⚠️ Не удалось найти свечные данные для {index}")
            return None
            
        except Exception as e:
            logger.error(f"❌ Ошибка получения свечных данных {index}: {e}")
            return None
    
    async def get_index_simple_data(self, index: str = 'IMOEX', days: int = 30) -> Optional[pd.DataFrame]:
        """Упрощенный метод получения данных индекса"""
        try:
            # Пробуем получить данные через другой endpoint
            url = f"{self.base_url}/statistics/engines/stock/markets/index/analytics/{index}.json"
            
            params = {
                'from': (datetime.now() - timedelta(days=days)).strftime('%Y-%m-%d'),
                'till': datetime.now().strftime('%Y-%m-%d')
            }
            
            data = await self._make_request(url, params)
            if not data:
                return None
            
            logger.info(f"📊 Структура аналитики: {list(data.keys())}")
            
            # Пробуем найти данные в разных таблицах
            for table_name, table_data in data.items():
                if isinstance(table_data, dict) and 'data' in table_data and table_data['data']:
                    try:
                        columns = [col['name'] for col in table_data.get('metadata', [])]
                        df = pd.DataFrame(table_data['data'], columns=columns)
                        
                        # Ищем колонки с датой и ценой
                        date_cols = [col for col in df.columns if 'date' in col.lower()]
                        price_cols = [col for col in df.columns if any(x in col.lower() for x in ['close', 'value', 'price'])]
                        
                        if date_cols and price_cols:
                            date_col = date_cols[0]
                            price_col = price_cols[0]
                            
                            df['date'] = pd.to_datetime(df[date_col])
                            df.set_index('date', inplace=True)
                            df = df.sort_index()
                            
                            # Переименовываем ценовую колонку
                            df['Close'] = df[price_col]
                            
                            logger.info(f"✅ Упрощенный метод: {len(df)} записей для {index}")
                            return df[['Close']]
                            
                    except Exception as e:
                        logger.warning(f"⚠️ Ошибка обработки таблицы {table_name}: {e}")
                        continue
            
            return None
            
        except Exception as e:
            logger.error(f"❌ Ошибка упрощенного метода для {index}: {e}")
            return None
    
    async def debug_index_data(self, index: str = 'IMOEX'):
        """Метод для дебага - показывает полную структуру ответа"""
        print(f"\n🔍 ДЕБАГ ДАННЫХ ДЛЯ {index}")
        print("=" * 50)
        
        endpoints = [
            f"/history/engines/stock/markets/index/boards/SNDX/securities/{index}.json",
            f"/engines/stock/markets/index/boards/SNDX/securities/{index}/candles.json",
            f"/statistics/engines/stock/markets/index/analytics/{index}.json"
        ]
        
        for endpoint in endpoints:
            print(f"\n📡 Endpoint: {endpoint}")
            url = self.base_url + endpoint
            
            data = await self._make_request(url)
            if data:
                print(f"✅ Ответ получен")
                print(f"   Ключи: {list(data.keys())}")
                
                for key, value in data.items():
                    if isinstance(value, dict) and 'metadata' in value:
                        columns = [col['name'] for col in value.get('metadata', [])]
                        data_count = len(value.get('data', []))
                        print(f"   📋 Таблица '{key}': {data_count} записей, {len(columns)} колонок")
                        print(f"      Колонки: {columns}")
            else:
                print(f"❌ Ошибка запроса")

async def test_fixed_api():
    """Тестирование исправленного API"""
    print("🧪 Тестирование исправленного MoexIndexAPI...")
    
    async with MoexIndexAPIFixed() as api:
        # Сначала запустим дебаг чтобы понять структуру данных
        await api.debug_index_data('IMOEX')
        
        print("\n" + "="*50)
        print("📊 ТЕСТИРОВАНИЕ ОСНОВНЫХ МЕТОДОВ")
        print("="*50)
        
        # Тест 1: Исторические данные
        print("\n1. 📈 Исторические данные IMOEX...")
        history_df = await api.get_index_history('IMOEX', days=5)
        if history_df is not None and len(history_df) > 0:
            print(f"   ✅ Успешно! {len(history_df)} записей")
            print(f"   📅 Период: {history_df.index[0].strftime('%Y-%m-%d')} - {history_df.index[-1].strftime('%Y-%m-%d')}")
            # Покажем доступные колонки
            close_cols = [col for col in history_df.columns if 'close' in col.lower() or 'value' in col.lower()]
            if close_cols:
                close_col = close_cols[0]
                print(f"   💹 Последнее значение ({close_col}): {history_df[close_col].iloc[-1]}")
        else:
            print("   ❌ Данные не получены")
        
        # Тест 2: Свечные данные
        print("\n2. 🕯️ Свечные данные IMOEX...")
        candles_df = await api.get_index_candles('IMOEX', days=5)
        if candles_df is not None and len(candles_df) > 0:
            print(f"   ✅ Успешно! {len(candles_df)} свечей")
            if 'Close' in candles_df.columns:
                print(f"   💹 Последнее Close: {candles_df['Close'].iloc[-1]}")
        else:
            print("   ❌ Данные не получены")
        
        # Тест 3: Упрощенный метод
        print("\n3. 🔧 Упрощенный метод IMOEX...")
        simple_df = await api.get_index_simple_data('IMOEX', days=5)
        if simple_df is not None and len(simple_df) > 0:
            print(f"   ✅ Успешно! {len(simple_df)} записей")
            if 'Close' in simple_df.columns:
                print(f"   💹 Последнее Close: {simple_df['Close'].iloc[-1]}")
        else:
            print("   ❌ Данные не получены")

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    asyncio.run(test_fixed_api())