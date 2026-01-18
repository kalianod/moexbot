import os
import time
import requests
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from dotenv import load_dotenv
import logging
import logging.handlers
import json
import warnings
from typing import Dict, List, Optional, Tuple, Any
from dataclasses import dataclass
from functools import lru_cache
import hashlib

warnings.filterwarnings('ignore')

# ========== НАСТРОЙКИ ЛОГИРОВАНИЯ С РОТАЦИЕЙ ==========
if not os.path.exists('logs'):
    os.makedirs('logs')

logger = logging.getLogger('MomentumBotMOEX')
logger.setLevel(logging.INFO)

formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

file_handler = logging.handlers.RotatingFileHandler(
    f'logs/momentum_bot_moex_{datetime.now().strftime("%Y%m")}.log',
    maxBytes=10*1024*1024,
    backupCount=5
)
file_handler.setFormatter(formatter)

console_handler = logging.StreamHandler()
console_handler.setFormatter(formatter)

logger.addHandler(file_handler)
logger.addHandler(console_handler)
# ========== КОНЕЦ НАСТРОЕК ЛОГИРОВАНИЯ ==========

# ИМПОРТ apimoex С ОБРАБОТКОЙ ОШИБОК - ИСПРАВЛЕННЫЙ ПОРЯДОК
try:
    import apimoex
    HAS_APIMOEX = True
    logger.info("✅ apimoex успешно импортирован")
except ImportError as e:
    HAS_APIMOEX = False
    logger.error(f"❌ apimoex не установлен: {e}")
    logger.error("⚠️ Установите: pip install apimoex")
except Exception as e:
    HAS_APIMOEX = False
    logger.error(f"❌ Ошибка импорта apimoex: {e}")

load_dotenv()

@dataclass
class AssetData:
    """Класс для хранения данных актива"""
    symbol: str
    name: str
    current_price: float
    price_12m_ago: float
    price_6m_ago: float
    price_1m_ago: float
    price_1w_ago: float
    volume_24h: float
    momentum_12m: float
    momentum_6m: float
    momentum_1m: float
    absolute_momentum: float
    absolute_momentum_6m: float
    combined_momentum: float
    sma_fast: float
    sma_slow: float
    sma_signal: bool
    timestamp: datetime
    market_type: str
    sector: str = ''
    currency: str = 'rub'
    source: str = 'moex'


class MOEXDataFetcher:
    """Класс для получения данных с Московской биржи С ИСПОЛЬЗОВАНИЕМ apimoex"""
    
    def __init__(self):
        # СОХРАНЯЕМ СТАРЫЙ КОНСТРУКТОР
        self.session = requests.Session()  # Создаем сессию для apimoex
        self.session.headers.update({'User-Agent': 'MomentumBotMOEX/1.0'})
        
        # Кэш для списка акций на 180 дней
        self.stocks_cache_file = 'logs/moex_stocks_cache.json'
        self.stocks_cache_ttl = 180 * 24 * 3600  # 180 дней в секундах
        
        # Бенчмарк - индекс полной доходности MCFTR
        self.benchmark_symbol = 'MCFTR'  # Изменено на индекс полной доходности
        
        logger.info(f"✅ MOEXDataFetcher инициализирован. apimoex доступен: {HAS_APIMOEX}")
        
        # Проверяем подключение к MOEX API
        self.test_moex_connection()
    
    def test_moex_connection(self):
        """Проверка подключения к MOEX API"""
        try:
            test_url = f"https://iss.moex.com/iss/engines/stock/markets/index/boards/SNDX/securities/{self.benchmark_symbol}.json"
            response = self.session.get(test_url, timeout=10)
            if response.status_code == 200:
                logger.info("✅ Подключение к MOEX API успешно")
                return True
            else:
                logger.warning(f"⚠️ MOEX API недоступен, код: {response.status_code}")
                return False
        except Exception as e:
            logger.error(f"❌ Ошибка подключения к MOEX API: {e}")
            return False
    
    def get_200_popular_stocks(self) -> List[Dict]:
        """
        Получение списка 200 популярных российских акций
        Кэшируется на 180 дней
        """
        # Проверяем кэш
        if os.path.exists(self.stocks_cache_file):
            try:
                with open(self.stocks_cache_file, 'r') as f:
                    cache_data = json.load(f)
                
                cache_time = datetime.fromisoformat(cache_data['timestamp'])
                age = (datetime.now() - cache_time).total_seconds()
                
                if age < self.stocks_cache_ttl:
                    logger.info(f"✅ Используем кэшированный список акций (возраст: {age/86400:.1f} дней)")
                    return cache_data['stocks']
                else:
                    logger.info(f"Кэш устарел ({age/86400:.1f} дней), обновляем...")
            except Exception as e:
                logger.error(f"Ошибка чтения кэша: {e}")
        
        logger.info("📊 Получение списка 200 популярных акций с MOEX...")
        
        try:
            # Используем apimoex для получения списка акций
            if HAS_APIMOEX:
                try:
                    # Получаем все акции с основной площадки TQBR
                    all_stocks = apimoex.get_board_securities(
                        self.session, 
                        board='TQBR',
                        columns=('SECID', 'SHORTNAME', 'ISSUECAPITALIZATION')
                    )
                    
                    if all_stocks:
                        # Сортируем по капитализации (если есть) или берем первые 200
                        sorted_stocks = sorted(
                            all_stocks,
                            key=lambda x: x.get('ISSUECAPITALIZATION', 0) or 0,
                            reverse=True
                        )
                        
                        # Берем топ-200
                        top_200 = sorted_stocks[:200]
                        
                        # Форматируем в нужный формат
                        stocks_list = []
                        for stock in top_200:
                            symbol = stock.get('SECID', '')
                            name = stock.get('SHORTNAME', symbol)
                            
                            # Определяем сектор по названию (упрощенно)
                            sector = self.determine_sector(symbol, name)
                            
                            stocks_list.append({
                                'symbol': symbol,
                                'name': name,
                                'sector': sector
                            })
                        
                        # Сохраняем в кэш
                        cache_data = {
                            'timestamp': datetime.now().isoformat(),
                            'stocks': stocks_list
                        }
                        
                        with open(self.stocks_cache_file, 'w') as f:
                            json.dump(cache_data, f, indent=2)
                        
                        logger.info(f"✅ Получено {len(stocks_list)} акций, сохранено в кэш")
                        return stocks_list
                        
                except Exception as e:
                    logger.error(f"Ошибка получения акций через apimoex: {e}")
            
            # Резервный метод через прямое API
            logger.info("Используем резервный метод получения акций...")
            url = "https://iss.moex.com/iss/engines/stock/markets/shares/boards/TQBR/securities.json"
            params = {
                'iss.meta': 'off',
                'securities.columns': 'SECID,SHORTNAME,ISSUECAPITALIZATION'
            }
            
            response = self.session.get(url, params=params, timeout=30)
            
            if response.status_code == 200:
                data = response.json()
                securities = data.get('securities', {}).get('data', [])
                
                if securities:
                    # Преобразуем в список словарей
                    stocks_data = []
                    for item in securities:
                        if len(item) >= 3:
                            symbol = item[0]
                            name = item[1]
                            market_cap = item[2] if len(item) > 2 else 0
                            
                            # Определяем сектор
                            sector = self.determine_sector(symbol, name)
                            
                            stocks_data.append({
                                'symbol': symbol,
                                'name': name,
                                'market_cap': market_cap,
                                'sector': sector
                            })
                    
                    # Сортируем по капитализации
                    stocks_data.sort(key=lambda x: x.get('market_cap', 0) or 0, reverse=True)
                    
                    # Берем топ-200 и форматируем
                    stocks_list = []
                    for i, stock in enumerate(stocks_data[:200], 1):
                        stocks_list.append({
                            'symbol': stock['symbol'],
                            'name': stock['name'],
                            'sector': stock['sector']
                        })
                    
                    # Сохраняем в кэш
                    cache_data = {
                        'timestamp': datetime.now().isoformat(),
                        'stocks': stocks_list
                    }
                    
                    with open(self.stocks_cache_file, 'w') as f:
                        json.dump(cache_data, f, indent=2)
                    
                    logger.info(f"✅ Получено {len(stocks_list)} акций через API, сохранено в кэш")
                    return stocks_list
        
        except Exception as e:
            logger.error(f"❌ Критическая ошибка получения списка акций: {e}")
            # В случае ошибки пытаемся загрузить из кэша даже если он старый
            if os.path.exists(self.stocks_cache_file):
                try:
                    with open(self.stocks_cache_file, 'r') as f:
                        cache_data = json.load(f)
                    logger.warning(f"⚠️ Используем старый кэш из-за ошибки")
                    return cache_data['stocks']
                except:
                    pass
        
        # Если ничего не получилось
        logger.error("❌ Не удалось получить список акций")
        return []
    
    def determine_sector(self, symbol: str, name: str) -> str:
        """Определение сектора по символу или названию"""
        # Простая логика определения сектора
        symbol_lower = symbol.lower()
        name_lower = name.lower()
        
        # Финансы
        finance_keywords = ['сбер', 'втб', 'тиньк', 'tcs', 'моск.бир', 'банк', 'газпромбанк', 'открытие']
        if any(kw in symbol_lower or kw in name_lower for kw in finance_keywords):
            return 'Финансы'
        
        # Нефть и газ
        oil_keywords = ['газп', 'лукойл', 'роснефт', 'новат', 'сургут', 'татнефт', 'башнефт', 'транснефт']
        if any(kw in symbol_lower or kw in name_lower for kw in oil_keywords):
            return 'Нефть и газ'
        
        # Металлургия
        metal_keywords = ['норни', 'полюс', 'распад', 'мечел', 'ммк', 'нлмк', 'северст', 'русал']
        if any(kw in symbol_lower or kw in name_lower for kw in metal_keywords):
            return 'Металлургия'
        
        # IT
        it_keywords = ['яндекс', 'vк', 'озон', 'positive', 'ксел', 'qiwi', 'tinkoff']
        if any(kw in symbol_lower or kw in name_lower for kw in it_keywords):
            return 'IT'
        
        # Телеком
        telecom_keywords = ['мтс', 'ростелеком', 'билайн', 'мегафон', 'tele2']
        if any(kw in symbol_lower or kw in name_lower for kw in telecom_keywords):
            return 'Телеком'
        
        # Ритейл
        retail_keywords = ['магнит', 'x5', 'лента', 'дикси', 'метр', 'окей']
        if any(kw in symbol_lower or kw in name_lower for kw in retail_keywords):
            return 'Ритейл'
        
        # Химия
        chem_keywords = ['фосагро', 'акрон', 'куйбышев', 'нижнекамск']
        if any(kw in symbol_lower or kw in name_lower for kw in chem_keywords):
            return 'Химия'
        
        # Энергетика
        energy_keywords = ['русгидро', 'интер рао', 'фск', 'россет', 'эн+']
        if any(kw in symbol_lower or kw in name_lower for kw in energy_keywords):
            return 'Энергетика'
        
        # Транспорт
        transport_keywords = ['аэрофлот', 'совкомфлот', 'новороссийск', 'транспорт']
        if any(kw in symbol_lower or kw in name_lower for kw in transport_keywords):
            return 'Транспорт'
        
        return 'Другое'
    
    def get_current_price(self, symbol: str) -> Tuple[Optional[float], Optional[float], str]:
        """
        Получение текущей цены БЕЗ ЗАПРОСА ОБЪЕМА
        """
        source = 'unknown'
        
        try:
            # Пробуем несколько различных API endpoint
            endpoints = [
                (f"https://iss.moex.com/iss/engines/stock/markets/shares/boards/TQBR/securities/{symbol}.json", 'TQBR'),
                (f"https://iss.moex.com/iss/engines/stock/markets/index/boards/SNDX/securities/{symbol}.json", 'SNDX'),
            ]
            
            for url, board_type in endpoints:
                try:
                    response = self.session.get(url, timeout=10)
                    if response.status_code == 200:
                        data = response.json()
                        
                        # Парсим данные
                        marketdata = data.get('marketdata', {}).get('data', [])
                        if marketdata:
                            # Берем первую запись (последние данные)
                            row = marketdata[0]
                            columns = data.get('marketdata', {}).get('columns', [])
                            
                            # Находим индекс колонки LAST
                            price_idx = columns.index('LAST') if 'LAST' in columns else -1
                            
                            if price_idx != -1 and len(row) > price_idx:
                                price = row[price_idx]
                                
                                if price and price > 0:
                                    source = f'moex_api_{board_type}'
                                    logger.debug(f"✅ Найден {symbol} на {board_type}: {price}")
                                    return price, 0, source  # Объем не запрашиваем, возвращаем 0
                except Exception as e:
                    logger.debug(f"Endpoint {board_type} для {symbol}: {e}")
                    continue
        
        except Exception as e:
            logger.error(f"❌ Ошибка получения цены для {symbol}: {e}")
        
        logger.warning(f"⚠️ Не удалось получить цену для {symbol}")
        return None, 0, source
    
    def get_historical_data(self, symbol: str, days: int = 400) -> Optional[pd.DataFrame]:
        """
        Получение исторических данных за указанное количество дней
        """
        try:
            # Определяем даты
            end_date = datetime.now()
            start_date = end_date - timedelta(days=days)
            start_date_str = start_date.strftime('%Y-%m-%d')
            end_date_str = end_date.strftime('%Y-%m-%d')
            
            logger.debug(f"Запрос исторических данных для {symbol} с {start_date_str} по {end_date_str}")
            
            # Используем apimoex если доступен
            if HAS_APIMOEX:
                try:
                    # Пробуем разные площадки
                    for board in ['TQBR', 'TQTD', 'SNDX']:
                        try:
                            data = apimoex.get_board_candles(
                                self.session,
                                security=symbol,
                                board=board,
                                interval=24,  # Дневные свечи
                                start=start_date_str,
                                end=end_date_str
                            )
                            
                            if data and len(data) > 0:
                                df = pd.DataFrame(data)
                                df = df.rename(columns={'end': 'timestamp'})
                                df['timestamp'] = pd.to_datetime(df['timestamp'])
                                df = df.sort_values('timestamp')
                                logger.info(f"✅ apimoex: получено {len(df)} свечей для {symbol} на {board}")
                                return df
                        except Exception as e:
                            logger.debug(f"apimoex {board} для {symbol}: {e}")
                            continue
                except Exception as e:
                    logger.debug(f"apimoex общая ошибка для {symbol}: {e}")
            
            # Резервный метод через прямое API
            logger.debug(f"Используем резервный API для исторических данных {symbol}")
            
            # Пробуем разные типы инструментов
            for market, board in [('shares', 'TQBR'), ('index', 'SNDX')]:
                url = f"https://iss.moex.com/iss/engines/stock/markets/{market}/boards/{board}/securities/{symbol}/candles.json"
                params = {
                    'from': start_date_str,
                    'till': end_date_str,
                    'interval': 24,
                    'candles.columns': 'open,close,high,low,value,volume,end'
                }
                
                try:
                    response = self.session.get(url, params=params, timeout=30)
                    
                    if response.status_code == 200:
                        data = response.json()
                        candles = data.get('candles', {}).get('data', [])
                        
                        if candles:
                            df = pd.DataFrame(candles, columns=['open', 'close', 'high', 'low', 'value', 'volume', 'timestamp'])
                            df['timestamp'] = pd.to_datetime(df['timestamp'])
                            df = df.sort_values('timestamp')
                            logger.info(f"✅ Старый метод: получено {len(df)} свечей для {symbol}")
                            return df
                except Exception as e:
                    logger.debug(f"Старый метод для {symbol} ({market}/{board}): {e}")
                    continue
                    
        except Exception as e:
            logger.error(f"❌ Ошибка получения исторических данных для {symbol}: {e}")
        
        logger.warning(f"⚠️ Не удалось получить исторические данные для {symbol}")
        return None
    
    def get_price_on_date(self, df: pd.DataFrame, target_date: datetime) -> Optional[float]:
        """
        Получение цены на конкретную дату (или ближайшую предыдущую)
        """
        if df is None or len(df) == 0:
            return None
        
        # Ищем дату, которая меньше или равна целевой дате
        mask = df['timestamp'] <= target_date
        available_dates = df[mask]
        
        if len(available_dates) == 0:
            # Если нет данных до целевой даты, берем самую раннюю
            return df['close'].iloc[0]
        
        # Берем ближайшую дату к целевой (но не позже)
        closest_idx = available_dates['timestamp'].sub(target_date).abs().idxmin()
        return df.loc[closest_idx, 'close']


class MomentumBotMOEX:
    """Бот momentum стратегии для Московской биржи"""
    
    def __init__(self):
        # ВСЕ СТАРЫЕ ПАРАМЕТРЫ СОХРАНЕНЫ
        self.telegram_token = os.getenv('TELEGRAM_TOKEN')
        self.telegram_chat_id = os.getenv('TELEGRAM_CHAT_ID')
        
        # Проверяем наличие токена и chat_id
        if not self.telegram_token:
            logger.error("❌ TELEGRAM_TOKEN не найден в переменных окружения")
            logger.error("❌ Добавьте TELEGRAM_TOKEN=ваш_токен_бота в файл .env")
        
        if not self.telegram_chat_id:
            logger.error("❌ TELEGRAM_CHAT_ID не найден в переменных окружения")
            logger.error("❌ Добавьте TELEGRAM_CHAT_ID=ваш_chat_id в файл .env")
        
        # Инициализация фетчера данных (теперь с apimoex)
        self.data_fetcher = MOEXDataFetcher()
        
        # Параметры стратегии - ИЗМЕНЕНО
        self.top_assets_count = 200  # Анализируем 200 акций
        self.selected_count = 10     # Выбираем 10 лучших - ИЗМЕНЕНО
        
        # Проверка каждые 4 часа
        self.check_interval = 4 * 3600  # 4 часа в секундах
        
        # Критерии фильтрации
        self.min_12m_momentum = 0.0  # 12M моментум должен быть >= 0%
        
        # Веса для моментума
        self.weights = {'12M': 0.40, '6M': 0.35, '1M': 0.25}
        
        # Параметры SMA
        self.sma_fast_period = 10
        self.sma_slow_period = 30
        
        # Бенчмарк - индекс полной доходности
        self.benchmark_symbol = 'MCFTR'  # ИЗМЕНЕНО
        self.benchmark_name = 'Индекс Мосбиржи полной доходности'  # ИЗМЕНЕНО
        
        # Текущий портфель
        self.current_portfolio: Dict[str, Dict] = {}
        self.signal_history: List[Dict] = []
        self.asset_ranking: List[AssetData] = []
        
        # Кэши - ИЗМЕНЕНО
        self._cache = {
            'top_assets': {'data': None, 'timestamp': None, 'ttl': 24*3600},  # 24 часа
            'historical_data': {},
            'benchmark_data': {'data': None, 'timestamp': None, 'ttl': 3600},  # 1 час
            'stocks_list': {'data': None, 'timestamp': None, 'ttl': 180*24*3600}  # 180 дней
        }
        
        # Статистика
        self.errors_count = 0
        self.max_retries = 3
        
        # Telegram
        self.telegram_retry_delay = 2
        self.max_telegram_retries = 3
        
        # Тестовый режим ОТКЛЮЧЕН - ИЗМЕНЕНО
        self.test_mode = False
        
        logger.info("🚀 Momentum Bot для Московской биржи инициализирован")
        logger.info(f"📊 Параметры: Топ {self.selected_count} из {self.top_assets_count} акций")
        logger.info(f"⚙️ Фильтры: 12M > {self.min_12m_momentum}%")
        logger.info(f"📈 Источник данных: {'apimoex' if HAS_APIMOEX else 'MOEX API (apimoex недоступен)'}")
        logger.info(f"⏰ Проверка: каждые {self.check_interval//3600} часа")
        logger.info(f"📊 Бенчмарк: {self.benchmark_symbol} ({self.benchmark_name})")
        
        if self.telegram_token and self.telegram_chat_id:
            logger.info("✅ Telegram настроен корректно")
        else:
            logger.warning("⚠️ Telegram не настроен. Сообщения не будут отправляться.")
    
    def clear_cache(self):
        """Очистка кэша данных"""
        logger.info("🧹 Очистка кэша данных...")
        self._cache = {
            'top_assets': {'data': None, 'timestamp': None, 'ttl': 24*3600},
            'historical_data': {},
            'benchmark_data': {'data': None, 'timestamp': None, 'ttl': 3600},
            'stocks_list': {'data': None, 'timestamp': None, 'ttl': 180*24*3600}
        }
        logger.info("✅ Кэш очищен")
    
    def get_stocks_list(self) -> List[Dict]:
        """
        Получение списка 200 популярных акций
        Кэшируется на 180 дней
        """
        cache = self._cache['stocks_list']
        
        # Проверяем кэш
        if cache['data'] and cache['timestamp']:
            cache_age = (datetime.now() - cache['timestamp']).total_seconds()
            if cache_age < cache['ttl']:
                logger.info(f"✅ Используем кэшированный список акций (возраст: {cache_age/86400:.1f} дней)")
                return cache['data']
        
        # Получаем новый список
        logger.info("📊 Получение списка 200 популярных акций...")
        stocks_list = self.data_fetcher.get_200_popular_stocks()
        
        if not stocks_list:
            logger.error("❌ КРИТИЧЕСКАЯ ОШИБКА: Не удалось получить список акций")
            # ТЕСТОВЫЙ РЕЖИМ ОТКЛЮЧЕН - ВЫХОДИМ С ОШИБКОЙ
            raise Exception("Не удалось получить список акций с MOEX API")
        
        # Сохраняем в кэш
        self._cache['stocks_list'] = {
            'data': stocks_list,
            'timestamp': datetime.now(),
            'ttl': 180*24*3600  # 180 дней
        }
        
        logger.info(f"✅ Получено {len(stocks_list)} акций, сохранено в кэш на 180 дней")
        return stocks_list
    
    def get_top_assets(self) -> List[Dict]:
        """
        Получение топ активов для анализа
        Использует кэшированный список 200 акций
        """
        try:
            # Проверяем кэш топ активов (24 часа)
            cache = self._cache['top_assets']
            if cache['data'] and cache['timestamp']:
                cache_age = (datetime.now() - cache['timestamp']).total_seconds()
                if cache_age < cache['ttl']:
                    logger.info(f"📊 Используем кэшированные топ активов (возраст: {cache_age/3600:.1f} часов)")
                    return cache['data']
            
            logger.info("📊 Формирование списка активов для анализа...")
            
            # Получаем список 200 акций
            all_stocks = self.get_stocks_list()
            
            if not all_stocks:
                logger.error("❌ Нет данных об акциях")
                return []
            
            all_assets = []
            filtered_assets = []  # Для отладки
            
            # Обрабатываем акции
            for i, stock in enumerate(all_stocks[:self.top_assets_count], 1):
                symbol = stock['symbol']
                name = stock['name']
                
                try:
                    # Получаем текущую цену (без объема)
                    price, _, source = self.data_fetcher.get_current_price(symbol)
                    
                    # Проверяем получены ли данные
                    if price is None or price <= 0:
                        filtered_assets.append(f"⚠️ {symbol}: не удалось получить цену")
                        logger.warning(f"⚠️ Не удалось получить цену для {symbol}")
                        continue
                    
                    all_assets.append({
                        'symbol': symbol,
                        'name': name,
                        'sector': stock.get('sector', ''),
                        'current_price': price,
                        'volume_24h': 0,  # Объем не используем
                        'source': source,
                        'market_type': 'stock'
                    })
                    
                    logger.debug(f"  ✅ {symbol}: {price:.2f} руб")
                    
                    # Пауза чтобы не перегружать API
                    if i % 20 == 0:  # Каждые 20 акций
                        time.sleep(0.5)
                            
                except Exception as e:
                    filtered_assets.append(f"❌ {symbol}: ошибка {str(e)[:50]}")
                    logger.error(f"  ❌ {symbol}: {e}")
                    continue
            
            # Добавляем бенчмарк
            try:
                price, _, source = self.data_fetcher.get_current_price(self.benchmark_symbol)
                if price and price > 0:
                    all_assets.append({
                        'symbol': self.benchmark_symbol,
                        'name': self.benchmark_name,
                        'sector': 'Индекс',
                        'current_price': price,
                        'volume_24h': 0,
                        'source': source,
                        'market_type': 'index'
                    })
                    logger.info(f"  ✅ {self.benchmark_symbol}: {price:.2f} руб (индекс)")
            except Exception as e:
                logger.error(f"Ошибка получения бенчмарка: {e}")
            
            # Проверяем количество полученных активов
            if len(all_assets) == 0:
                logger.error("❌ Не удалось получить данные ни для одного актива")
                # ТЕСТОВЫЙ РЕЖИМ ОТКЛЮЧЕН - ВЫХОДИМ С ОШИБКОЙ
                raise Exception("Не удалось получить данные по акциям")
            
            # Кэшируем
            self._cache['top_assets'] = {
                'data': all_assets,
                'timestamp': datetime.now(),
                'ttl': 24*3600  # 24 часа
            }
            
            logger.info(f"✅ Сформирован список из {len(all_assets)} активов")
            return all_assets
            
        except Exception as e:
            logger.error(f"❌ Критическая ошибка получения топ активов: {e}")
            # Отправляем сообщение в Telegram
            if self.telegram_token and self.telegram_chat_id:
                self.send_telegram_message(
                    f"❌ *КРИТИЧЕСКАЯ ОШИБКА*\n"
                    f"Не удалось получить данные акций:\n"
                    f"```{str(e)[:100]}```\n"
                    f"Бот остановлен.",
                    silent=False
                )
            raise  # Пробрасываем исключение дальше
    
    @lru_cache(maxsize=200)
    def get_cached_historical_data(self, symbol: str, days: int = 400) -> Optional[pd.DataFrame]:
        """
        Получение исторических данных с кэшированием на 1 час
        """
        cache_key = f"{symbol}_{days}"
        
        if cache_key in self._cache['historical_data']:
            cache_data = self._cache['historical_data'][cache_key]
            cache_age = (datetime.now() - cache_data['timestamp']).total_seconds()
            if cache_age < cache_data['ttl']:
                logger.debug(f"Используем кэшированные исторические данные для {symbol}")
                return cache_data['data']
        
        df = self.data_fetcher.get_historical_data(symbol, days)
        
        if df is not None and len(df) > 0:
            # Проверяем минимальное количество данных
            min_required_days = 250  # Минимум для анализа
            if len(df) < min_required_days:
                logger.warning(f"⚠️ Мало исторических данных для {symbol}: {len(df)} дней (< {min_required_days})")
            
            self._cache['historical_data'][cache_key] = {
                'data': df,
                'timestamp': datetime.now(),
                'ttl': 3600  # 1 час
            }
        else:
            logger.error(f"❌ Не удалось получить исторические данные для {symbol}")
        
        return df
    
    def get_price_for_calendar_date(self, df: pd.DataFrame, target_date: datetime) -> Optional[float]:
        """
        Получение цены на конкретную календарную дату
        Если на эту дату нет торгов, берем ближайшую предыдущую
        """
        if df is None or len(df) == 0:
            return None
        
        # Преобразуем target_date в datetime без времени для сравнения
        target_date = target_date.replace(hour=0, minute=0, second=0, microsecond=0)
        
        # Ищем ближайшую дату <= целевой
        mask = df['timestamp'].dt.date <= target_date.date()
        available_dates = df[mask]
        
        if len(available_dates) == 0:
            # Если нет данных до целевой даты, берем самую раннюю
            logger.debug(f"Нет данных до {target_date.date()}, берем самую раннюю")
            return df['close'].iloc[0]
        
        # Берем ближайшую дату к целевой
        closest_idx = available_dates['timestamp'].sub(target_date).abs().idxmin()
        closest_date = df.loc[closest_idx, 'timestamp'].date()
        
        if closest_date != target_date.date():
            logger.debug(f"Для даты {target_date.date()} используем ближайшую {closest_date}")
        
        return df.loc[closest_idx, 'close']
    
    def get_benchmark_data(self) -> Optional[Dict[str, float]]:
        """Получение данных бенчмарка (индекс полной доходности)"""
        try:
            cache = self._cache['benchmark_data']
            if cache['data'] and cache['timestamp']:
                cache_age = (datetime.now() - cache['timestamp']).total_seconds()
                if cache_age < cache['ttl']:
                    return cache['data']
            
            logger.info(f"📊 Получение данных бенчмарка {self.benchmark_symbol}...")
            
            # Получаем исторические данные
            df = self.get_cached_historical_data(self.benchmark_symbol, 400)
            if df is None or len(df) < 126:
                logger.error(f"❌ Недостаточно данных бенчмарка {self.benchmark_symbol}")
                return None
            
            current_price = df['close'].iloc[-1]
            
            # Рассчитываем даты для календарных периодов
            current_date = datetime.now()
            
            # 1 неделя назад (понедельник предыдущей недели)
            week_ago = current_date - timedelta(days=7)
            # Находим понедельник недели назад
            week_ago = week_ago - timedelta(days=week_ago.weekday())
            
            # 1 месяц назад (ровно 30 дней)
            month_ago = current_date - timedelta(days=30)
            
            # 6 месяцев назад (ровно 180 дней)
            six_months_ago = current_date - timedelta(days=180)
            
            # 12 месяцев назад (ровно 365 дней)
            year_ago = current_date - timedelta(days=365)
            
            # Получаем цены на эти даты
            price_1w_ago = self.get_price_for_calendar_date(df, week_ago)
            price_1m_ago = self.get_price_for_calendar_date(df, month_ago)
            price_6m_ago = self.get_price_for_calendar_date(df, six_months_ago)
            price_12m_ago = self.get_price_for_calendar_date(df, year_ago)
            
            # Рассчитываем моментумы
            try:
                # Моментум 1M (месяц без последней недели)
                # Используем цену неделю назад и месяц назад
                momentum_1m = ((price_1w_ago - price_1m_ago) / price_1m_ago) * 100 if price_1m_ago > 0 else 0
                
                # Моментум 6M (6 месяцев без последнего месяца)
                # Используем цену месяц назад и 6 месяцев назад
                momentum_6m = ((price_1m_ago - price_6m_ago) / price_6m_ago) * 100 if price_6m_ago > 0 else 0
                
                # Моментум 12M (12 месяцев без последнего месяца)
                # Используем цену месяц назад и 12 месяцев назад
                momentum_12m = ((price_1m_ago - price_12m_ago) / price_12m_ago) * 100 if price_12m_ago > 0 else 0
                
                # Абсолютный моментум 6M (от 6 месяцев назад до текущего момента)
                absolute_momentum_6m = ((current_price - price_6m_ago) / price_6m_ago) * 100 if price_6m_ago > 0 else 0
                
                # Абсолютный моментум 12M (от 12 месяцев назад до текущего момента)
                absolute_momentum_12m = ((current_price - price_12m_ago) / price_12m_ago) * 100 if price_12m_ago > 0 else 0
                
            except ZeroDivisionError:
                logger.error(f"❌ Ошибка деления на ноль для бенчмарка {self.benchmark_symbol}")
                return None
            
            benchmark_data = {
                'symbol': self.benchmark_symbol,
                'name': self.benchmark_name,
                'current_price': current_price,
                'price_1w_ago': price_1w_ago,
                'price_1m_ago': price_1m_ago,
                'price_6m_ago': price_6m_ago,
                'price_12m_ago': price_12m_ago,
                'momentum_1m': momentum_1m,
                'momentum_6m': momentum_6m,
                'momentum_12m': momentum_12m,
                'absolute_momentum_6m': absolute_momentum_6m,
                'absolute_momentum_12m': absolute_momentum_12m,
                'timestamp': datetime.now()
            }
            
            self._cache['benchmark_data'] = {
                'data': benchmark_data,
                'timestamp': datetime.now(),
                'ttl': 3600  # 1 час
            }
            
            logger.info(f"✅ Данные бенчмарка: 6M моментум = {absolute_momentum_6m:.2f}%, 12M моментум = {absolute_momentum_12m:.2f}%")
            
            return benchmark_data
            
        except Exception as e:
            logger.error(f"❌ Ошибка получения данных бенчмарка: {e}")
            return None
    
    def calculate_momentum_values(self, asset_info: Dict) -> Optional[AssetData]:
        """Расчет значений моментума с использованием календарных дней"""
        try:
            symbol = asset_info['symbol']
            name = asset_info['name']
            source = asset_info.get('source', 'unknown')
            
            logger.debug(f"📈 Расчет моментума для {symbol} ({name})...")
            
            # Получаем исторические данные
            df = self.get_cached_historical_data(symbol, 400)
            if df is None or len(df) == 0:
                logger.error(f"❌ Нет исторических данных для {symbol}")
                return None
            
            # Проверяем минимальное количество данных
            if len(df) < 100:
                logger.warning(f"⚠️ Мало исторических данных для {symbol}: {len(df)} дней")
                return None
            
            # Текущая цена (последняя доступная)
            current_price = df['close'].iloc[-1]
            
            if current_price <= 0:
                logger.error(f"❌ Некорректная цена для {symbol}: {current_price}")
                return None
            
            # Рассчитываем календарные даты
            current_date = datetime.now()
            
            # 1 неделя назад (понедельник предыдущей недели)
            week_ago = current_date - timedelta(days=7)
            week_ago = week_ago - timedelta(days=week_ago.weekday())  # Находим понедельник
            
            # 1 месяц назад (ровно 30 календарных дней)
            month_ago = current_date - timedelta(days=30)
            
            # 6 месяцев назад (ровно 180 календарных дней)
            six_months_ago = current_date - timedelta(days=180)
            
            # 12 месяцев назад (ровно 365 календарных дней)
            year_ago = current_date - timedelta(days=365)
            
            # Получаем цены на календарные даты
            price_1w_ago = self.get_price_for_calendar_date(df, week_ago)
            price_1m_ago = self.get_price_for_calendar_date(df, month_ago)
            price_6m_ago = self.get_price_for_calendar_date(df, six_months_ago)
            price_12m_ago = self.get_price_for_calendar_date(df, year_ago)
            
            # Проверяем полученные цены
            if None in [price_1w_ago, price_1m_ago, price_6m_ago, price_12m_ago]:
                logger.error(f"❌ Не удалось получить цены на календарные даты для {symbol}")
                return None
            
            # Расчет моментумов согласно новой логике
            try:
                # Моментум 1M: изменение за последний месяц без последней недели
                # (цена неделю назад - цена месяц назад) / цена месяц назад
                momentum_1m = ((price_1w_ago - price_1m_ago) / price_1m_ago) * 100 if price_1m_ago > 0 else 0
                
                # Моментум 6M: изменение за 6 месяцев без последнего месяца
                # (цена месяц назад - цена 6 месяцев назад) / цена 6 месяцев назад
                momentum_6m = ((price_1m_ago - price_6m_ago) / price_6m_ago) * 100 if price_6m_ago > 0 else 0
                
                # Моментум 12M: изменение за 12 месяцев без последнего месяца
                # (цена месяц назад - цена 12 месяцев назад) / цена 12 месяцев назад
                momentum_12m = ((price_1m_ago - price_12m_ago) / price_12m_ago) * 100 if price_12m_ago > 0 else 0
                
                # Абсолютный моментум: изменение от 12 месяцев назад до текущего момента
                absolute_momentum = ((current_price - price_12m_ago) / price_12m_ago) * 100 if price_12m_ago > 0 else 0
                
                # Абсолютный моментум 6M: изменение от 6 месяцев назад до текущего момента
                absolute_momentum_6m = ((current_price - price_6m_ago) / price_6m_ago) * 100 if price_6m_ago > 0 else 0
                
            except ZeroDivisionError:
                logger.error(f"❌ Ошибка деления на ноль для {symbol}")
                return None
            
            # Комбинированный моментум
            combined_momentum = (
                momentum_12m * self.weights['12M'] +
                momentum_6m * self.weights['6M'] +
                momentum_1m * self.weights['1M']
            )
            
            # Расчет SMA
            sma_fast = df['close'].tail(self.sma_fast_period).mean()
            sma_slow = df['close'].tail(self.sma_slow_period).mean()
            sma_signal = sma_fast > sma_slow
            
            volume_24h = asset_info.get('volume_24h', 0)
            sector = asset_info.get('sector', '')
            market_type = asset_info.get('market_type', 'stock')
            
            logger.debug(f"  {symbol}: Цена {current_price:.2f}, 12M: {momentum_12m:+.1f}%, 6M: {absolute_momentum_6m:+.1f}%, 1M: {momentum_1m:+.1f}%, SMA: {'🟢' if sma_signal else '🔴'}")
            
            return AssetData(
                symbol=symbol,
                name=name,
                current_price=current_price,
                price_12m_ago=price_12m_ago,
                price_6m_ago=price_6m_ago,
                price_1m_ago=price_1m_ago,
                price_1w_ago=price_1w_ago,
                volume_24h=volume_24h,
                momentum_12m=momentum_12m,
                momentum_6m=momentum_6m,
                momentum_1m=momentum_1m,
                absolute_momentum=absolute_momentum,
                absolute_momentum_6m=absolute_momentum_6m,
                combined_momentum=combined_momentum,
                sma_fast=sma_fast,
                sma_slow=sma_slow,
                sma_signal=sma_signal,
                timestamp=datetime.now(),
                market_type=market_type,
                sector=sector,
                currency='rub',
                source=source
            )
            
        except Exception as e:
            logger.error(f"❌ Ошибка расчета моментума для {symbol}: {e}")
            return None
    
    def analyze_assets(self) -> List[AssetData]:
        """Анализ активов с новой логикой фильтрации"""
        top_assets = self.get_top_assets()
        if not top_assets:
            logger.error("❌ Нет активов для анализа")
            return []
        
        logger.info(f"📊 Анализ {len(top_assets)} активов...")
        
        analyzed_assets = []
        benchmark_data = self.get_benchmark_data()
        
        # Статистика фильтрации
        filter_stats = {
            'total': 0,
            'passed_all': 0,
            'passed_12m': 0,
            'passed_sma': 0,
            'passed_benchmark': 0,
            'failed_12m': 0,
            'failed_sma': 0,
            'failed_benchmark': 0,
            'no_data': 0,
            'errors': 0
        }
        
        for i, asset_info in enumerate(top_assets):
            symbol = asset_info['symbol']
            
            # Пропускаем бенчмарк в анализе
            if symbol == self.benchmark_symbol:
                continue
                
            filter_stats['total'] += 1
            
            try:
                asset_data = self.calculate_momentum_values(asset_info)
                if asset_data is None:
                    filter_stats['no_data'] += 1
                    logger.debug(f"  ⚠️ {symbol}: нет данных для анализа")
                    continue
                
                # ФИЛЬТР 1: 12M Momentum ≥ 0%
                if asset_data.momentum_12m < self.min_12m_momentum:
                    filter_stats['failed_12m'] += 1
                    logger.debug(f"  ❌ {symbol}: низкий 12M моментум ({asset_data.momentum_12m:+.1f}% < {self.min_12m_momentum}%)")
                    continue
                filter_stats['passed_12m'] += 1
                
                # ФИЛЬТР 2: Положительный SMA сигнал
                if not asset_data.sma_signal:
                    filter_stats['failed_sma'] += 1
                    logger.debug(f"  ❌ {symbol}: отрицательный SMA сигнал")
                    continue
                filter_stats['passed_sma'] += 1
                
                # ФИЛЬТР 3: Сравнение с бенчмарком
                if benchmark_data:
                    if asset_data.absolute_momentum_6m <= benchmark_data['absolute_momentum_6m']:
                        filter_stats['failed_benchmark'] += 1
                        logger.debug(f"  ❌ {symbol}: 6M моментум ({asset_data.absolute_momentum_6m:+.1f}%) <= бенчмарку ({benchmark_data['absolute_momentum_6m']:+.1f}%)")
                        continue
                    filter_stats['passed_benchmark'] += 1
                else:
                    # Если нет данных бенчмарка, пропускаем этот фильтр
                    logger.warning("Нет данных бенчмарка, пропускаем сравнение")
                
                analyzed_assets.append(asset_data)
                filter_stats['passed_all'] += 1
                logger.debug(f"  ✅ {symbol}: добавлен в анализ")
                
            except Exception as e:
                filter_stats['errors'] += 1
                logger.error(f"Ошибка анализа {symbol}: {e}")
                continue
        
        # Сортируем по комбинированному моментуму (по убыванию)
        analyzed_assets.sort(key=lambda x: x.combined_momentum, reverse=True)
        
        # Берем топ-10 (ИЗМЕНЕНО с 5 на 10)
        selected_assets = analyzed_assets[:self.selected_count]
        
        # Детальная статистика фильтрации
        logger.info("=" * 60)
        logger.info(f"📊 ИТОГ анализа: {len(selected_assets)} активов отобрано из {filter_stats['total']}")
        if benchmark_data:
            logger.info(f"📈 Бенчмарк {self.benchmark_symbol}: 6M моментум = {benchmark_data['absolute_momentum_6m']:+.1f}%")
        logger.info(f"📊 Статистика фильтрации:")
        logger.info(f"  • Всего акций: {filter_stats['total']}")
        logger.info(f"  • Прошли все фильтры: {filter_stats['passed_all']}")
        logger.info(f"  • Прошли 12M моментум: {filter_stats['passed_12m']} (провалили: {filter_stats['failed_12m']})")
        logger.info(f"  • Прошли SMA: {filter_stats['passed_sma']} (провалили: {filter_stats['failed_sma']})")
        if benchmark_data:
            logger.info(f"  • Прошли сравнение с бенчмарком: {filter_stats['passed_benchmark']} (провалили: {filter_stats['failed_benchmark']})")
        logger.info(f"  • Без данных: {filter_stats['no_data']}")
        logger.info(f"  • Ошибки анализа: {filter_stats['errors']}")
        
        if filter_stats['passed_all'] == 0:
            logger.warning("⚠️ Все активы отфильтрованы по критериям")
        
        if selected_assets:
            logger.info("🏆 Топ активов:")
            for i, asset in enumerate(selected_assets, 1):
                vs_benchmark = f" vs бенчмарк: {asset.absolute_momentum_6m - benchmark_data['absolute_momentum_6m']:+.1f}%" if benchmark_data else ""
                logger.info(f"  {i:2d}. {asset.symbol}: {asset.combined_momentum:+.2f}% (12M: {asset.momentum_12m:+.1f}%, 6M: {asset.absolute_momentum_6m:+.1f}%{vs_benchmark})")
        
        return selected_assets
    
    def generate_signals(self, assets: List[AssetData]) -> List[Dict]:
        """
        Генерация сигналов с новой логикой
        BUY: топ-10, Absolute Momentum 12M > 0%, SMA положительный, акции нет в портфеле
        SELL: акция в портфеле и (Absolute Momentum 12M < 0% ИЛИ SMA отрицательный ИЛИ Absolute Momentum 6M < Benchmark)
        """
        signals = []
        benchmark_data = self.get_benchmark_data()
        
        # Создаем множество топ-10 акций для быстрой проверки
        top_10_symbols = {asset.symbol for asset in assets[:10]}
        
        for asset in assets:
            symbol = asset.symbol
            current_status = self.current_portfolio.get(symbol, {}).get('status', 'OUT')
            
            # BUY сигнал (только для топ-10)
            if symbol in top_10_symbols:
                if (asset.absolute_momentum > 0 and  # Absolute Momentum 12M > 0%
                    asset.sma_signal and            # SMA положительный
                    current_status != 'IN'):        # Акции нет в портфеле
                    
                    signal = {
                        'symbol': symbol,
                        'action': 'BUY',
                        'price': asset.current_price,
                        'absolute_momentum': asset.absolute_momentum,
                        'absolute_momentum_6m': asset.absolute_momentum_6m,
                        'momentum_12m': asset.momentum_12m,
                        'momentum_6m': asset.momentum_6m,
                        'momentum_1m': asset.momentum_1m,
                        'combined_momentum': asset.combined_momentum,
                        'sma_fast': asset.sma_fast,
                        'sma_slow': asset.sma_slow,
                        'market_type': asset.market_type,
                        'sector': asset.sector,
                        'reason': f"Топ-10, Моментум 12M: {asset.absolute_momentum:+.1f}%, SMA положительный",
                        'timestamp': datetime.now()
                    }
                    
                    # Проверяем, не превышен ли лимит портфеля (10 позиций)
                    active_positions = sum(1 for v in self.current_portfolio.values() if v.get('status') == 'IN')
                    if active_positions >= self.selected_count:
                        # Ищем худшую позицию для замены
                        worst_position = None
                        worst_momentum = float('inf')
                        
                        for pos_symbol, pos_data in self.current_portfolio.items():
                            if pos_data.get('status') == 'IN':
                                # Получаем данные позиции для сравнения
                                pos_asset = next((a for a in assets if a.symbol == pos_symbol), None)
                                if pos_asset:
                                    if pos_asset.combined_momentum < worst_momentum:
                                        worst_momentum = pos_asset.combined_momentum
                                        worst_position = pos_symbol
                        
                        # Если нашли худшую позицию и она хуже текущей акции
                        if worst_position and worst_momentum < asset.combined_momentum:
                            # Продаем худшую позицию
                            entry_data = self.current_portfolio.get(worst_position, {})
                            entry_price = entry_data.get('entry_price', 0)
                            profit_percent = ((asset.current_price - entry_price) / entry_price) * 100 if entry_price > 0 else 0
                            
                            sell_signal = {
                                'symbol': worst_position,
                                'action': 'SELL',
                                'price': asset.current_price,
                                'entry_price': entry_price,
                                'profit_percent': profit_percent,
                                'reason': f"Замена на более перспективную акцию ({symbol})",
                                'timestamp': datetime.now()
                            }
                            
                            signals.append(sell_signal)
                            self.current_portfolio[worst_position] = {
                                'status': 'OUT',
                                'exit_time': datetime.now(),
                                'exit_price': asset.current_price,
                                'profit_percent': profit_percent,
                                'name': entry_data.get('name', worst_position)
                            }
                            logger.info(f"📉 SELL для замены {worst_position}: {profit_percent:+.2f}%")
                    
                    # Добавляем новую позицию
                    self.current_portfolio[symbol] = {
                        'entry_time': datetime.now(),
                        'entry_price': asset.current_price,
                        'status': 'IN',
                        'name': asset.name,
                        'sector': asset.sector,
                        'source': asset.source
                    }
                    
                    signals.append(signal)
                    logger.info(f"📈 BUY для {symbol} ({asset.name})")
            
            # SELL сигнал (только для акций в портфеле)
            elif current_status == 'IN':
                sell_reason = ""
                should_sell = False
                
                # Условие 1: Absolute Momentum 12M < 0%
                if asset.absolute_momentum < 0:
                    sell_reason = "Моментум 12M < 0%"
                    should_sell = True
                
                # Условие 2: SMA отрицательный
                elif not asset.sma_signal:
                    sell_reason = "SMA отрицательный"
                    should_sell = True
                
                # Условие 3: Absolute Momentum 6M < Benchmark (если есть данные бенчмарка)
                elif benchmark_data and asset.absolute_momentum_6m < benchmark_data['absolute_momentum_6m']:
                    sell_reason = f"6M моментум ({asset.absolute_momentum_6m:+.1f}%) < бенчмарка ({benchmark_data['absolute_momentum_6m']:+.1f}%)"
                    should_sell = True
                
                if should_sell:
                    entry_data = self.current_portfolio.get(symbol, {})
                    entry_price = entry_data.get('entry_price', asset.current_price)
                    profit_percent = ((asset.current_price - entry_price) / entry_price) * 100
                    
                    signal = {
                        'symbol': symbol,
                        'action': 'SELL',
                        'price': asset.current_price,
                        'entry_price': entry_price,
                        'profit_percent': profit_percent,
                        'absolute_momentum': asset.absolute_momentum,
                        'absolute_momentum_6m': asset.absolute_momentum_6m,
                        'reason': f"Выход: {sell_reason}",
                        'timestamp': datetime.now()
                    }
                    
                    self.current_portfolio[symbol] = {
                        'status': 'OUT',
                        'exit_time': datetime.now(),
                        'exit_price': asset.current_price,
                        'profit_percent': profit_percent,
                        'name': entry_data.get('name', asset.name)
                    }
                    
                    signals.append(signal)
                    logger.info(f"📉 SELL для {symbol}: {profit_percent:+.2f}% ({sell_reason})")
        
        return signals
    
    def send_telegram_message(self, message: str, silent: bool = False) -> bool:
        """Отправка сообщения в Telegram"""
        if not self.telegram_token or not self.telegram_chat_id:
            if not silent:
                logger.warning("⚠️ Нет данных для Telegram")
            return False
        
        for attempt in range(self.max_telegram_retries):
            try:
                url = f"https://api.telegram.org/bot{self.telegram_token}/sendMessage"
                data = {
                    "chat_id": self.telegram_chat_id,
                    "text": message,
                    "parse_mode": "Markdown",
                    "disable_web_page_preview": True,
                    "disable_notification": silent
                }
                
                response = requests.post(url, data=data, timeout=10)
                
                if response.status_code == 200:
                    if not silent:
                        logger.debug("✅ Сообщение отправлено в Telegram")
                    return True
                else:
                    error_msg = f"Ошибка Telegram (попытка {attempt+1}): {response.status_code}"
                    if not silent:
                        logger.warning(error_msg)
                    
            except Exception as e:
                error_msg = f"Ошибка отправки Telegram (попытка {attempt+1}): {e}"
                if not silent:
                    logger.warning(error_msg)
            
            if attempt < self.max_telegram_retries - 1:
                time.sleep(self.telegram_retry_delay)
        
        if not silent:
            logger.error("❌ Не удалось отправить сообщение в Telegram после всех попыток")
        return False
    
    def load_state(self):
        """Загрузка состояния с обработкой пустого файла"""
        try:
            if os.path.exists('logs/bot_state_moex.json'):
                with open('logs/bot_state_moex.json', 'r') as f:
                    content = f.read().strip()
                    if not content:
                        logger.warning("Файл состояния пуст, используется состояние по умолчанию")
                        return
                    state = json.loads(content)
                
                self.current_portfolio = state.get('current_portfolio', {})
                
                # Конвертируем строки времени обратно в datetime
                for symbol, data in self.current_portfolio.items():
                    if 'entry_time' in data and isinstance(data['entry_time'], str):
                        data['entry_time'] = datetime.fromisoformat(data['entry_time'].replace('Z', '+00:00'))
                    if 'exit_time' in data and isinstance(data['exit_time'], str):
                        data['exit_time'] = datetime.fromisoformat(data['exit_time'].replace('Z', '+00:00'))
                
                self.signal_history = state.get('signal_history', [])
                self.errors_count = state.get('errors_count', 0)
                
                active_count = len([v for v in self.current_portfolio.values() if v.get('status') == 'IN'])
                logger.info(f"💾 Состояние загружено. Активных позиций: {active_count}")
            else:
                logger.info("📁 Файл состояния не найден, используется состояние по умолчанию")
        except json.JSONDecodeError as e:
            logger.error(f"❌ Ошибка парсинга JSON в файле состояния: {e}")
            logger.warning("🔄 Используется состояние по умолчанию")
        except Exception as e:
            logger.error(f"❌ Ошибка загрузки состояния: {e}")
            logger.warning("🔄 Используется состояние по умолчанию")
    
    def format_active_positions(self) -> str:
        """Форматирование списка активных позиций"""
        active_positions = {k: v for k, v in self.current_portfolio.items() 
                          if v.get('status') == 'IN'}
        
        if not active_positions:
            return "📊 *АКТИВНЫХ ПОЗИЦИЙ НЕТ*\nВсе средства в рублях"
        
        message = "📊 *АКТИВНЫЕ ПОЗИЦИИ:*\n"
        message += "═══════════════════════════\n"
        
        total_profit = 0
        position_count = 0
        
        for symbol, data in active_positions.items():
            entry_price = data.get('entry_price', 0)
            entry_time = data.get('entry_time', datetime.now())
            name = data.get('name', symbol)
            sector = data.get('sector', '')
            
            # Получаем текущую цену
            try:
                price, _, source = self.data_fetcher.get_current_price(symbol)
                if price and price > 0:
                    profit_percent = ((price - entry_price) / entry_price) * 100
                    profit_emoji = "📈" if profit_percent > 0 else "📉"
                    
                    message += (
                        f"• {symbol} ({name})\n"
                        f"  📈 Сектор: {sector}\n"
                        f"  💰 Вход: {entry_price:.2f} руб\n"
                        f"  💰 Текущая: {price:.2f} руб\n"
                        f"  📊 P&L: {profit_percent:+.2f}% {profit_emoji}\n"
                        f"  ⏰ Вход: {entry_time.strftime('%d.%m.%Y %H:%M')}\n"
                        f"  ─\n"
                    )
                    
                    total_profit += profit_percent
                    position_count += 1
                else:
                    message += (
                        f"• {symbol} ({name})\n"
                        f"  ⚠️ Не удалось получить текущую цену\n"
                        f"  💰 Вход: {entry_price:.2f} руб\n"
                        f"  ⏰ Вход: {entry_time.strftime('%d.%m.%Y %H:%M')}\n"
                        f"  ─\n"
                    )
            except Exception as e:
                logger.error(f"Ошибка получения цены для {symbol}: {e}")
                continue
        
        if position_count > 0:
            avg_profit = total_profit / position_count
            message += f"═══════════════════════════\n"
            message += f"📈 Средняя прибыль: {avg_profit:+.2f}%\n"
        
        message += f"🔢 Всего позиций: {len(active_positions)}/{self.selected_count}"
        
        return message
    
    def format_signal_message(self, signal: Dict) -> str:
        """Форматирование сигнала"""
        if signal['action'] == 'BUY':
            return (
                f"🎯 *BUY: {signal['symbol']}*\n"
                f"═══════════════════════════\n"
                f"🏢 {signal.get('sector', 'Акция')}\n"
                f"💰 Цена: {signal['price']:.2f} руб\n"
                f"📊 Абсолютный моментум (12M): **{signal['absolute_momentum']:+.1f}%**\n"
                f"📈 Абсолютный моментум (6M): **{signal.get('absolute_momentum_6m', 0):+.1f}%**\n"
                f"📊 Относительный моментум (12M): **{signal['momentum_12m']:+.1f}%**\n"
                f"• 6M: {signal['momentum_6m']:+.1f}%\n"
                f"• 1M: {signal['momentum_1m']:+.1f}%\n"
                f"🎯 Комбинированный: {signal['combined_momentum']:+.1f}%\n"
                f"🕐 Время: {signal['timestamp'].strftime('%H:%M:%S %d.%m.%Y')}\n"
                f"═══════════════════════════\n"
                f"{signal['reason']}"
            )
        else:
            profit_emoji = "📈" if signal['profit_percent'] > 0 else "📉"
            return (
                f"🎯 *SELL: {signal['symbol']}*\n"
                f"═══════════════════════════\n"
                f"💰 Цена входа: {signal['entry_price']:.2f} руб\n"
                f"💰 Цена выхода: {signal['price']:.2f} руб\n"
                f"📊 Прибыль: **{signal['profit_percent']:+.2f}%** {profit_emoji}\n"
                f"📈 Абсолютный моментум: {signal['absolute_momentum']:+.1f}%\n"
                f"📈 Абсолютный моментум 6M: {signal.get('absolute_momentum_6m', 0):+.1f}%\n"
                f"🕐 Время: {signal['timestamp'].strftime('%H:%M:%S %d.%m.%Y')}\n"
                f"═══════════════════════════\n"
                f"{signal['reason']}"
            )
    
    def format_ranking_message(self, assets: List[AssetData]) -> str:
        """Форматирование рейтинга топ-10 акций"""
        benchmark_data = self.get_benchmark_data()
        
        message = f"📊 *MOMENTUM РЕЙТИНГ МОСБИРЖИ (Топ-10)*\n"
        message += f"Отбор: {len(assets)} из {self.top_assets_count} акций\n"
        
        if benchmark_data:
            message += f"📈 Бенчмарк ({self.benchmark_symbol}): {benchmark_data['absolute_momentum_6m']:+.1f}% (6M)\n"
        
        message += "═══════════════════════════\n"
        
        if not assets:
            message += "⚠️ *Нет активов, соответствующих критериям*\n"
            message += "═══════════════════════════\n"
            return message
        
        for i, asset in enumerate(assets[:10], 1):  # Показываем топ-10
            status = "🟢 IN" if self.current_portfolio.get(asset.symbol, {}).get('status') == 'IN' else "⚪ OUT"
            
            # Сравнение с бенчмарком
            benchmark_comparison = ""
            if benchmark_data:
                vs_benchmark = asset.absolute_momentum_6m - benchmark_data['absolute_momentum_6m']
                if vs_benchmark > 0:
                    benchmark_comparison = f"\n📈 vs бенчмарк: +{vs_benchmark:.1f}%"
                else:
                    benchmark_comparison = f"\n📉 vs бенчмарк: {vs_benchmark:.1f}%"
            
            message += (
                f"#{i} {asset.symbol} ({asset.name[:20]}) {status}\n"
                f"🏢 {asset.sector}\n"
                f"💰 {asset.current_price:.2f} руб\n"
                f"📊 Моментум:\n"
                f"  • 12M: **{asset.momentum_12m:+.1f}%**\n"
                f"  • 6M: {asset.absolute_momentum_6m:+.1f}%{benchmark_comparison}\n"
                f"  • 1M: {asset.momentum_1m:+.1f}%\n"
                f"  • Комбинированный: **{asset.combined_momentum:+.1f}%**\n"
                f"📉 SMA: {'🟢 Растущий' if asset.sma_signal else '🔴 Падающий'}\n"
                f"──\n"
            )
        
        message += "═══════════════════════════\n"
        message += "*ПАРАМЕТРЫ СТРАТЕГИИ:*\n"
        message += f"• Анализ: {self.top_assets_count} акций\n"
        message += f"• Отбор: топ {self.selected_count}\n"
        message += f"• Требование 12M моментум: > {self.min_12m_momentum}%\n"
        message += f"• Бенчмарк: {self.benchmark_symbol}\n"
        message += f"• SMA: {self.sma_fast_period}/{self.sma_slow_period} дней\n"
        message += f"• Веса: 12M({self.weights['12M']*100:.0f}%), 6M({self.weights['6M']*100:.0f}%), 1M({self.weights['1M']*100:.0f}%)\n"
        message += f"• Проверка: каждые {self.check_interval//3600} часа\n"
        
        active_count = sum(1 for v in self.current_portfolio.values() if v.get('status') == 'IN')
        if active_count > 0:
            message += f"• Активных позиций: {active_count}/{self.selected_count}\n"
        
        return message
    
    def run_strategy_cycle(self) -> bool:
        """Запуск цикла стратегии"""
        try:
            logger.info("🔄 Запуск цикла стратегии...")
            
            # Очищаем кэш если было много ошибок
            if self.errors_count > 3:
                self.clear_cache()
                logger.info("🔄 Кэш очищен из-за большого количества ошибок")
            
            # Анализируем активы
            assets = self.analyze_assets()
            
            if not assets:
                logger.warning("❌ Нет активов для анализа")
                # Отправляем сообщение о причинах
                benchmark_data = self.get_benchmark_data()
                no_assets_msg = (
                    "📊 *Анализ Мосбиржи*\n"
                    "Нет активов, соответствующих критериям.\n\n"
                    f"• Проверено акций: {self.top_assets_count}\n"
                    f"• Требование 12M моментум: > {self.min_12m_momentum}%\n"
                    f"• Требование SMA: положительный сигнал\n"
                )
                
                if benchmark_data:
                    no_assets_msg += f"• Бенчмарк ({self.benchmark_symbol}): {benchmark_data['absolute_momentum_6m']:+.1f}%\n"
                
                no_assets_msg += "\nВозможно, рынок в нисходящем тренде."
                
                self.send_telegram_message(no_assets_msg)
                
                # Отправляем активные позиции если они есть
                active_positions = self.format_active_positions()
                if "АКТИВНЫХ ПОЗИЦИЙ НЕТ" not in active_positions:
                    self.send_telegram_message(active_positions)
                
                return False
            
            self.asset_ranking = assets
            
            # Генерируем сигналы
            signals = self.generate_signals(assets)
            
            # Отправляем сигналы в Telegram
            for signal in signals:
                message = self.format_signal_message(signal)
                if self.send_telegram_message(message):
                    self.signal_history.append(signal)
                    logger.info(f"✅ Сигнал отправлен: {signal['symbol']} {signal['action']}")
            
            # Отправляем рейтинг
            ranking_message = self.format_ranking_message(assets)
            self.send_telegram_message(ranking_message)
            
            logger.info(f"✅ Цикл завершен. Сигналов: {len(signals)}")
            return True
            
        except Exception as e:
            logger.error(f"❌ Ошибка в цикле: {e}")
            self.errors_count += 1
            
            # Отправляем сообщение об ошибке в Telegram
            error_msg = (
                f"❌ *ОШИБКА АНАЛИЗА*\n"
                f"Произошла ошибка при анализе активов:\n"
                f"```\n{str(e)[:200]}\n```\n"
                f"Ошибок подряд: {self.errors_count}"
            )
            self.send_telegram_message(error_msg)
            
            return False
    
    def save_state(self):
        """Сохранение состояния"""
        try:
            state = {
                'current_portfolio': self.current_portfolio,
                'signal_history': self.signal_history[-100:],
                'last_update': datetime.now().isoformat(),
                'errors_count': self.errors_count,
                'version': 'moex_bot_v5_calendar_momentum'
            }
            
            with open('logs/bot_state_moex.json', 'w') as f:
                json.dump(state, f, default=str, indent=2)
            
            logger.info("💾 Состояние сохранено")
        except Exception as e:
            logger.error(f"Ошибка сохранения: {e}")
    
    def run(self):
        """Основной цикл работы бота"""
        logger.info("=" * 60)
        logger.info("🚀 ЗАПУСК MOMENTUM BOT ДЛЯ МОСБИРЖИ (обновленная версия)")
        logger.info("=" * 60)
        
        # Загружаем состояние
        self.load_state()
        
        # Проверяем доступность MOEX API
        logger.info("🔍 Проверка доступности MOEX API...")
        if not self.data_fetcher.test_moex_connection():
            logger.error("❌ MOEX API недоступен. Проверьте подключение к интернету.")
            if self.telegram_token and self.telegram_chat_id:
                self.send_telegram_message(
                    "❌ *MOEX API НЕДОСТУПЕН*\n"
                    "Проверьте подключение к интернету.\n"
                    "Бот остановлен."
                )
            return  # Завершаем работу, тестовый режим отключен
        else:
            logger.info("✅ MOEX API доступен")
        
        # Приветственное сообщение в Telegram
        if self.telegram_token and self.telegram_chat_id:
            welcome_msg = (
                "🚀 *MOMENTUM BOT ДЛЯ МОСБИРЖИ ЗАПУЩЕН*\n"
                f"📊 Стратегия: Momentum с календарными периодами\n"
                f"🔢 Анализ: {self.top_assets_count} акций, отбор: топ {self.selected_count}\n"
                f"📈 Бенчмарк: {self.benchmark_symbol} ({self.benchmark_name})\n"
                f"⚙️ Фильтры: 12M > {self.min_12m_momentum}%, SMA положительный\n"
                f"📡 Источник данных: {'apimoex' if HAS_APIMOEX else 'MOEX API'}\n"
                f"⏰ Проверка: каждые {self.check_interval//3600} часа\n"
                f"⚡ Версия: календарные моментумы"
            )
            self.send_telegram_message(welcome_msg)
            
            # Отправляем активные позиции при запуске
            active_positions_msg = self.format_active_positions()
            self.send_telegram_message(active_positions_msg)
            
            # Предупреждение если apimoex недоступен
            if not HAS_APIMOEX:
                apimoex_warning = (
                    "⚠️ *ВНИМАНИЕ: apimoex не установлен*\n"
                    "Бот работает в режиме совместимости с прямым API MOEX.\n"
                    "Для лучшей работы установите:\n"
                    "```bash\npip install apimoex\n```"
                )
                self.send_telegram_message(apimoex_warning, silent=True)
        else:
            logger.warning("⚠️ Telegram не настроен, пропускаем приветственное сообщение")
        
        iteration = 0
        
        try:
            while True:
                iteration += 1
                current_time = datetime.now().strftime('%H:%M:%S %d.%m.%Y')
                logger.info(f"🔄 Цикл #{iteration} - {current_time}")
                
                success = self.run_strategy_cycle()
                
                if success:
                    logger.info(f"✅ Цикл #{iteration} успешно завершен")
                    
                    # Сохраняем состояние каждые 3 цикла
                    if iteration % 3 == 0:
                        self.save_state()
                else:
                    logger.warning(f"⚠️ Цикл #{iteration} завершен с проблемами")
                
                # Проверяем количество ошибок
                if self.errors_count > 5:
                    logger.error(f"⚠️ Много ошибок ({self.errors_count}). Пауза 1 час...")
                    if self.telegram_token and self.telegram_chat_id:
                        self.send_telegram_message("⚠️ *МНОГО ОШИБОК* \nБот делает паузу 1 час")
                    time.sleep(3600)
                    self.errors_count = 0
                
                logger.info(f"⏳ Следующая проверка через {self.check_interval//3600} часа(ов)...")
                time.sleep(self.check_interval)
                
        except KeyboardInterrupt:
            logger.info("🛑 Остановка по команде пользователя")
            self.save_state()
            if self.telegram_token and self.telegram_chat_id:
                self.send_telegram_message("🛑 *BOT ОСТАНОВЛЕН ПОЛЬЗОВАТЕЛЕМ*")
        
        except Exception as e:
            logger.error(f"❌ Критическая ошибка в основном цикле: {e}")
            self.errors_count += 1
            if self.telegram_token and self.telegram_chat_id:
                self.send_telegram_message(f"💥 *КРИТИЧЕСКАЯ ОШИБКА* \n{str(e)[:100]}")


def main():
    """Основная функция запуска бота"""
    bot = MomentumBotMOEX()
    
    try:
        bot.run()
    except Exception as e:
        logger.error(f"💀 Фатальная ошибка: {e}")
        if bot.telegram_token and bot.telegram_chat_id:
            bot.send_telegram_message(f"💀 *ФАТАЛЬНАЯ ОШИБКА* \nБот завершил работу: {str(e)[:200]}")


if __name__ == "__main__":
    main()