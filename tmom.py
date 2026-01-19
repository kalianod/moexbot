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
from decimal import Decimal, ROUND_HALF_UP

warnings.filterwarnings('ignore')

# ========== НАСТРОЙКИ ЛОГИРОВАНИЯ С РОТАЦИЕЙ ==========
if not os.path.exists('logs'):
    os.makedirs('logs')

logger = logging.getLogger('MomentumBotTinkoff')
logger.setLevel(logging.INFO)

formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

file_handler = logging.handlers.RotatingFileHandler(
    f'logs/momentum_bot_tinkoff_{datetime.now().strftime("%Y%m")}.log',
    maxBytes=10*1024*1024,
    backupCount=5
)
file_handler.setFormatter(formatter)

console_handler = logging.StreamHandler()
console_handler.setFormatter(formatter)

logger.addHandler(file_handler)
logger.addHandler(console_handler)
# ========== КОНЕЦ НАСТРОЕК ЛОГИРОВАНИЯ ==========

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
    source: str = 'tinkoff'


class TinkoffDataFetcher:
    """Класс для получения данных через Tinkoff API"""
    
    def __init__(self, token: str = None, sandbox: bool = True):
        if token is None:
            token = os.getenv("TINKOFF_TOKEN") or os.getenv("INVEST_TOKEN")
        
        self.token = token
        self.sandbox = sandbox
        self.client = None
        
        # Популярные акции с FIGI
        self.popular_stocks = [
            {'symbol': 'SBER', 'figi': 'BBG004730N88', 'name': 'Сбербанк', 'sector': 'Финансы'},
            {'symbol': 'GAZP', 'figi': 'BBG004730RP0', 'name': 'Газпром', 'sector': 'Нефть и газ'},
            {'symbol': 'LKOH', 'figi': 'BBG004731032', 'name': 'Лукойл', 'sector': 'Нефть и газ'},
            {'symbol': 'ROSN', 'figi': 'BBG004731354', 'name': 'Роснефть', 'sector': 'Нефть и газ'},
            {'symbol': 'NVTK', 'figi': 'BBG00475J7X6', 'name': 'Новатэк', 'sector': 'Нефть и газ'},
            {'symbol': 'GMKN', 'figi': 'BBG004731489', 'name': 'Норникель', 'sector': 'Металлургия'},
            {'symbol': 'PLZL', 'figi': 'BBG000R607Y3', 'name': 'Полюс', 'sector': 'Металлургия'},
            {'symbol': 'YNDX', 'figi': 'BBG006L8G4H1', 'name': 'Яндекс', 'sector': 'IT'},
            {'symbol': 'TCSG', 'figi': 'BBG00QPYJ5H0', 'name': 'TCS Group', 'sector': 'Финансы'},
            {'symbol': 'MOEX', 'figi': 'BBG004730JJ5', 'name': 'Московская биржа', 'sector': 'Финансы'},
            {'symbol': 'MGNT', 'figi': 'BBG004S681W1', 'name': 'Магнит', 'sector': 'Розничная торговля'},
            {'symbol': 'PHOR', 'figi': 'BBG004S68507', 'name': 'ФосАгро', 'sector': 'Химия'},
            {'symbol': 'RUAL', 'figi': 'BBG008F2T3T2', 'name': 'РУСАЛ', 'sector': 'Металлургия'},
            {'symbol': 'VTBR', 'figi': 'BBG004730ZJ9', 'name': 'ВТБ', 'sector': 'Финансы'},
            {'symbol': 'ALRS', 'figi': 'BBG004S683W7', 'name': 'АЛРОСА', 'sector': 'Добыча'},
            {'symbol': 'POLY', 'figi': 'BBG004S681B4', 'name': 'Полиметалл', 'sector': 'Металлургия'},
            {'symbol': 'CHMF', 'figi': 'BBG00475K6C3', 'name': 'Северсталь', 'sector': 'Металлургия'},
            {'symbol': 'SNGS', 'figi': 'BBG0047315D0', 'name': 'Сургутнефтегаз', 'sector': 'Нефть и газ'},
            {'symbol': 'SNGSP', 'figi': 'BBG004S681M2', 'name': 'Сургутнефтегаз-п', 'sector': 'Нефть и газ'},
            {'symbol': 'TATN', 'figi': 'BBG004RVFCY3', 'name': 'Татнефть', 'sector': 'Нефть и газ'},
            {'symbol': 'TATNP', 'figi': 'BBG004S686N5', 'name': 'Татнефть-п', 'sector': 'Нефть и газ'},
            {'symbol': 'IRAO', 'figi': 'BBG004S681G4', 'name': 'Интер РАО', 'sector': 'Энергетика'},
            {'symbol': 'HYDR', 'figi': 'BBG00475KHX6', 'name': 'РусГидро', 'sector': 'Энергетика'},
            {'symbol': 'MTSS', 'figi': 'BBG004S68758', 'name': 'МТС', 'sector': 'Телеком'},
            {'symbol': 'RTKM', 'figi': 'BBG004S68829', 'name': 'Ростелеком', 'sector': 'Телеком'},
            {'symbol': 'FEES', 'figi': 'BBG004S68BH6', 'name': 'ФСК ЕЭС', 'sector': 'Энергетика'},
            {'symbol': 'NLMK', 'figi': 'BBG004S683M2', 'name': 'НЛМК', 'sector': 'Металлургия'},
            {'symbol': 'MAGN', 'figi': 'BBG004S68598', 'name': 'ММК', 'sector': 'Металлургия'},
            {'symbol': 'AFKS', 'figi': 'BBG004S68807', 'name': 'Система', 'sector': 'Холдинги'},
            {'symbol': 'AFLT', 'figi': 'BBG004S681V5', 'name': 'Аэрофлот', 'sector': 'Транспорт'},
        ]
        
        # Индексы
        self.indices = [
            {'symbol': 'IMOEX', 'figi': 'BBG004730QY1', 'name': 'Индекс Мосбиржи', 'sector': 'Индекс'},
            {'symbol': 'RTSI', 'figi': 'BBG0047315J7', 'name': 'Индекс РТС', 'sector': 'Индекс'},
        ]
        
        self.initialize_client()
        logger.info(f"TinkoffDataFetcher инициализирован (sandbox: {sandbox})")
    
    def initialize_client(self):
        """Инициализация клиента Tinkoff API"""
        try:
            if self.sandbox:
                from t_tech.invest.sandbox.client import SandboxClient
                self.client = SandboxClient(self.token)
            else:
                from t_tech.invest.client import Client
                self.client = Client(self.token)
            
            logger.info("Tinkoff API клиент успешно инициализирован")
            return True
        except ImportError:
            logger.error("Tinkoff API не установлен")
            logger.error("Установите: pip install t-tech-investments")
            return False
        except Exception as e:
            logger.error(f"Ошибка инициализации Tinkoff API: {e}")
            return False
    
    def find_instrument_by_symbol(self, symbol: str) -> Optional[Dict]:
        """Поиск инструмента по символу"""
        # Проверяем в популярных акциях
        for stock in self.popular_stocks:
            if stock['symbol'] == symbol:
                return stock
        
        # Проверяем в индексах
        for index in self.indices:
            if index['symbol'] == symbol:
                return index
        
        return None
    
    def get_current_price(self, symbol: str) -> Tuple[Optional[float], Optional[float], str]:
        """Получение текущей цены и объема через Tinkoff API"""
        try:
            if not self.client:
                if not self.initialize_client():
                    return None, None, 'client_not_initialized'
            
            # Находим инструмент
            instrument = self.find_instrument_by_symbol(symbol)
            if not instrument:
                logger.warning(f"Инструмент {symbol} не найден")
                return None, None, 'instrument_not_found'
            
            # Получаем последнюю цену через прямой запрос
            try:
                # Используем метод получения последней цены
                from t_tech.invest.utils import quotation_to_decimal
                
                # Получаем стакан
                order_book = self.client.market_data.get_order_book(
                    figi=instrument['figi'], 
                    depth=1
                )
                
                if order_book and hasattr(order_book, 'last_price'):
                    # Преобразуем цену из quotation в float
                    price_quotation = order_book.last_price
                    price = float(quotation_to_decimal(price_quotation))
                    
                    # Получаем объем из стакана или используем дефолтное значение
                    volume = 0
                    if hasattr(order_book, 'trade_status') and order_book.trade_status:
                        volume = getattr(order_book.trade_status, 'trades24h_volume', 1000000)
                    else:
                        volume = 1000000  # Значение по умолчанию
                    
                    logger.debug(f"Tinkoff: цена для {symbol}: {price}, объем: {volume}")
                    return price, volume, 'tinkoff_api'
                
            except Exception as e:
                logger.debug(f"Не удалось получить стакан для {symbol}: {e}")
            
            # Альтернативный метод: используем стриминг для получения цены
            try:
                from t_tech.invest import SubscribeOrderBookRequest, OrderBookInstrument, MarketDataRequest, SubscriptionAction
                
                # Создаем запрос на подписку
                request = MarketDataRequest(
                    subscribe_order_book_request=SubscribeOrderBookRequest(
                        subscription_action=SubscriptionAction.SUBSCRIPTION_ACTION_SUBSCRIBE,
                        instruments=[OrderBookInstrument(figi=instrument['figi'], depth=1)]
                    )
                )
                
                # Получаем один раз данные
                for marketdata in self.client.market_data_stream.market_data_stream([request]):
                    if marketdata.orderbook and marketdata.orderbook.figi == instrument['figi']:
                        from t_tech.invest.utils import quotation_to_decimal
                        if marketdata.orderbook.bids and marketdata.orderbook.asks:
                            # Берем среднюю цену между bid и ask
                            bid = quotation_to_decimal(marketdata.orderbook.bids[0].price)
                            ask = quotation_to_decimal(marketdata.orderbook.asks[0].price)
                            price = float((bid + ask) / 2)
                            volume = getattr(marketdata.orderbook, 'trade_status', {}).get('trades24h_volume', 1000000)
                            return price, volume, 'tinkoff_stream'
                    break
                    
            except Exception as e:
                logger.debug(f"Не удалось получить данные через стриминг для {symbol}: {e}")
            
            return None, None, 'no_price_data'
                
        except Exception as e:
            logger.error(f"Ошибка получения цены {symbol}: {e}")
            return None, None, f'error: {str(e)[:50]}'
    
    def get_historical_data(self, symbol: str, days: int = 365) -> Optional[pd.DataFrame]:
        """Получение исторических данных через Tinkoff API"""
        try:
            if not self.client:
                if not self.initialize_client():
                    return None
            
            instrument = self.find_instrument_by_symbol(symbol)
            if not instrument:
                logger.error(f"Инструмент {symbol} не найден")
                return None
            
            end_date = datetime.now()
            start_date = end_date - timedelta(days=days)
            
            logger.debug(f"Запрос исторических данных для {symbol} с {start_date} по {end_date}")
            
            # Преобразуем даты в строковый формат
            from datetime import datetime as dt
            
            try:
                # Получаем исторические данные через API
                candles = self.client.market_data.get_candles(
                    figi=instrument['figi'],
                    from_=start_date,
                    to=end_date,
                    interval='day'  # дневные свечи
                )
                
                if not candles:
                    logger.warning(f"Нет исторических данных для {symbol}")
                    return None
                
                # Конвертируем в DataFrame
                data = []
                from t_tech.invest.utils import quotation_to_decimal
                
                for candle in candles:
                    try:
                        close_price = float(quotation_to_decimal(candle.close))
                        volume = candle.volume
                        
                        data.append({
                            'timestamp': candle.time,
                            'open': float(quotation_to_decimal(candle.open)),
                            'close': close_price,
                            'high': float(quotation_to_decimal(candle.high)),
                            'low': float(quotation_to_decimal(candle.low)),
                            'volume': volume
                        })
                    except Exception as e:
                        logger.debug(f"Ошибка обработки свечи: {e}")
                        continue
                
                if not data:
                    logger.error(f"Не удалось обработать данные для {symbol}")
                    return None
                
                df = pd.DataFrame(data)
                df['timestamp'] = pd.to_datetime(df['timestamp'])
                df = df.sort_values('timestamp')
                df = df.drop_duplicates('timestamp')
                
                logger.info(f"Tinkoff API: получено {len(df)} свечей для {symbol}")
                return df
                
            except Exception as e:
                logger.error(f"Ошибка получения исторических данных для {symbol}: {e}")
                return None
            
        except Exception as e:
            logger.error(f"Ошибка получения исторических данных для {symbol}: {e}")
            return None
    
    def test_connection(self):
        """Проверка подключения к Tinkoff API"""
        try:
            if not self.client:
                return self.initialize_client()
            
            # Пробуем получить информацию об аккаунте
            accounts = self.client.users.get_accounts()
            logger.info(f"Подключение к Tinkoff API успешно. Аккаунтов: {len(accounts)}")
            return True
                
        except Exception as e:
            logger.error(f"Ошибка подключения к Tinkoff API: {e}")
            return False


class MomentumBotTinkoff:
    """Бот momentum стратегии для Tinkoff API"""
    
    def __init__(self):
        self.telegram_token = os.getenv('TELEGRAM_TOKEN')
        self.telegram_chat_id = os.getenv('TELEGRAM_CHAT_ID')
        
        # Токен Tinkoff API
        self.tinkoff_token = os.getenv('TINKOFF_TOKEN') or os.getenv('INVEST_TOKEN')
        self.tinkoff_sandbox = os.getenv('TINKOFF_SANDBOX', 'true').lower() == 'true'
        
        if not self.tinkoff_token:
            logger.error("TINKOFF_TOKEN не найден в переменных окружения")
            logger.error("Добавьте TINKOFF_TOKEN=ваш_токен в файл .env")
        
        if not self.telegram_token:
            logger.error("TELEGRAM_TOKEN не найден в переменных окружения")
        
        if not self.telegram_chat_id:
            logger.error("TELEGRAM_CHAT_ID не найден в переменных окружения")
        
        # Инициализация фетчера Tinkoff
        self.data_fetcher = TinkoffDataFetcher(
            token=self.tinkoff_token,
            sandbox=self.tinkoff_sandbox
        )
        
        # Параметры стратегии
        self.top_assets_count = 30
        self.selected_count = 5
        self.check_interval = 4 * 3600
        
        # Критерии фильтрации
        self.min_12m_momentum = 0.0
        self.min_volume_24h = 1000000
        self.min_price = 1
        
        # Веса для моментума
        self.weights = {'12M': 0.40, '6M': 0.35, '1M': 0.25}
        
        # Параметры SMA
        self.sma_fast_period = 10
        self.sma_slow_period = 30
        
        # Бенчмарк
        self.benchmark_symbol = 'IMOEX'
        self.benchmark_name = 'Индекс Мосбиржи'
        
        # Текущий портфель
        self.current_portfolio: Dict[str, Dict] = {}
        self.signal_history: List[Dict] = []
        self.asset_ranking: List[AssetData] = []
        
        # Кэши
        self._cache = {
            'top_assets': {'data': None, 'timestamp': None, 'ttl': 24*3600},
            'historical_data': {},
            'benchmark_data': {'data': None, 'timestamp': None, 'ttl': 3600}
        }
        
        # Статистика
        self.errors_count = 0
        self.max_retries = 3
        
        # Telegram
        self.telegram_retry_delay = 2
        self.max_telegram_retries = 3
        
        # Тестовый режим
        self.test_mode = False
        
        logger.info("Momentum Bot для Tinkoff API инициализирован")
        logger.info(f"Параметры: Топ {self.selected_count} из {self.top_assets_count}")
        logger.info(f"Фильтры: 12M > {self.min_12m_momentum}%, Объем > {self.min_volume_24h:,} руб")
        logger.info(f"Источник данных: Tinkoff API (sandbox: {self.tinkoff_sandbox})")
        logger.info(f"Проверка: каждые {self.check_interval//3600} часа")
        
        if self.telegram_token and self.telegram_chat_id:
            logger.info("Telegram настроен корректно")
        else:
            logger.warning("Telegram не настроен. Сообщения не будут отправляться.")
    
    def get_top_assets(self) -> List[Dict]:
        """Получение топ активов"""
        try:
            cache = self._cache['top_assets']
            if cache['data'] and (datetime.now() - cache['timestamp']).seconds < cache['ttl']:
                logger.info("Используем кэшированные данные активов")
                return cache['data']
            
            logger.info("Формирование списка активов...")
            
            all_assets = []
            filtered_assets = []
            no_data_assets = []
            
            # Проверяем подключение к Tinkoff API
            if not self.data_fetcher.test_connection():
                logger.warning("Tinkoff API недоступен, используем тестовые данные")
                self.test_mode = True
                return self.get_test_assets()
            
            # Добавляем популярные акции
            for stock in self.data_fetcher.popular_stocks[:self.top_assets_count * 2]:
                symbol = stock['symbol']
                name = stock['name']
                
                try:
                    price, volume, source = self.data_fetcher.get_current_price(symbol)
                    
                    if price is None:
                        no_data_assets.append(f"{symbol}: не удалось получить данные")
                        logger.warning(f"Не удалось получить данные для {symbol}")
                        continue
                    
                    asset_info = {
                        'symbol': symbol,
                        'name': name,
                        'price': price,
                        'volume': volume,
                        'source': source
                    }
                    
                    if price < self.min_price:
                        filtered_assets.append(f"{symbol}: цена {price:.2f} < {self.min_price} руб")
                        continue
                    
                    if not volume or volume < self.min_volume_24h:
                        filtered_assets.append(f"{symbol}: объем {volume:,.0f} < {self.min_volume_24h:,} руб")
                        volume = self.min_volume_24h * 2
                    
                    all_assets.append({
                        'symbol': symbol,
                        'name': name,
                        'sector': stock.get('sector', ''),
                        'current_price': price,
                        'volume_24h': volume,
                        'source': source,
                        'market_type': 'stock'
                    })
                    logger.info(f"  {symbol}: {price:.2f} руб, объем: {volume:,.0f}")
                    
                    time.sleep(0.2)
                        
                except Exception as e:
                    filtered_assets.append(f"{symbol}: ошибка {str(e)[:50]}")
                    logger.error(f"  {symbol}: {e}")
                    continue
            
            # Добавляем индексы
            for index in self.data_fetcher.indices:
                symbol = index['symbol']
                name = index['name']
                
                try:
                    price, volume, source = self.data_fetcher.get_current_price(symbol)
                    
                    if price is None or price <= 0:
                        no_data_assets.append(f"{symbol}: не удалось получить данные индекса")
                        logger.warning(f"Не удалось получить данных для индекса {symbol}")
                        continue
                    
                    all_assets.append({
                        'symbol': symbol,
                        'name': name,
                        'sector': index.get('sector', ''),
                        'current_price': price,
                        'volume_24h': volume if volume else 1000000,
                        'source': source,
                        'market_type': 'index'
                    })
                    logger.info(f"  {symbol}: {price:.2f} руб (индекс)")
                    
                    time.sleep(0.2)
                        
                except Exception as e:
                    filtered_assets.append(f"{symbol}: ошибка {str(e)[:50]}")
                    logger.error(f"  {symbol}: {e}")
            
            if len(all_assets) == 0:
                logger.error("Не удалось получить данные ни для одного актива")
                logger.warning("Переход в тестовый режим")
                self.test_mode = True
                return self.get_test_assets()
            
            all_assets.sort(key=lambda x: x['volume_24h'], reverse=True)
            top_assets = all_assets[:self.top_assets_count]
            
            if filtered_assets:
                logger.info("Отфильтрованные активы:")
                for i, msg in enumerate(filtered_assets[:10], 1):
                    logger.info(f"  {i:2d}. {msg}")
            
            if no_data_assets:
                logger.warning("Активы без данных:")
                for i, msg in enumerate(no_data_assets[:10], 1):
                    logger.warning(f"  {i:2d}. {msg}")
            
            self._cache['top_assets'] = {
                'data': top_assets,
                'timestamp': datetime.now(),
                'ttl': 24*3600
            }
            
            logger.info(f"Сформирован список из {len(top_assets)} активов")
            if top_assets:
                logger.info("Первые 5 активов:")
                for i, asset in enumerate(top_assets[:5], 1):
                    logger.info(f"  {i:2d}. {asset['symbol']} - {asset['name']}: {asset['current_price']:.2f} руб")
            
            return top_assets
            
        except Exception as e:
            logger.error(f"Ошибка получения топ активов: {e}")
            logger.warning("Используем тестовые данные из-за ошибки")
            self.test_mode = True
            return self.get_test_assets()
    
    def get_test_assets(self) -> List[Dict]:
        """Получение тестовых данных для отладки"""
        logger.warning("РЕЖИМ ТЕСТИРОВАНИЯ: Используем тестовые данные")
        
        if self.telegram_token and self.telegram_chat_id:
            warning_msg = (
                "РЕЖИМ ТЕСТИРОВАНИЯ\n"
                "Не удалось получить данные с Tinkoff API.\n"
                "Используются тестовые данные для проверки логики."
            )
            self.send_telegram_message(warning_msg, silent=True)
        
        return [
            {'symbol': 'SBER', 'name': 'Сбербанк', 'sector': 'Финансы', 'current_price': 300.0, 'volume_24h': 10000000, 'source': 'test', 'market_type': 'stock'},
            {'symbol': 'GAZP', 'name': 'Газпром', 'sector': 'Нефть и газ', 'current_price': 180.0, 'volume_24h': 8000000, 'source': 'test', 'market_type': 'stock'},
            {'symbol': 'LKOH', 'name': 'Лукойл', 'sector': 'Нефть и газ', 'current_price': 7500.0, 'volume_24h': 5000000, 'source': 'test', 'market_type': 'stock'},
            {'symbol': 'YNDX', 'name': 'Яндекс', 'sector': 'IT', 'current_price': 2500.0, 'volume_24h': 3000000, 'source': 'test', 'market_type': 'stock'},
            {'symbol': 'MOEX', 'name': 'Московская биржа', 'sector': 'Финансы', 'current_price': 176.0, 'volume_24h': 2000000, 'source': 'test', 'market_type': 'stock'},
            {'symbol': 'IMOEX', 'name': 'Индекс Мосбиржи', 'sector': 'Индекс', 'current_price': 3500.0, 'volume_24h': 1000000, 'source': 'test', 'market_type': 'index'},
        ]
    
    @lru_cache(maxsize=100)
    def get_cached_historical_data(self, symbol: str, days: int = 400) -> Optional[pd.DataFrame]:
        """Получение исторических данных с кэшированием"""
        cache_key = f"{symbol}_{days}"
        
        if cache_key in self._cache['historical_data']:
            cache_data = self._cache['historical_data'][cache_key]
            if (datetime.now() - cache_data['timestamp']).seconds < cache_data['ttl']:
                return cache_data['data']
        
        df = self.data_fetcher.get_historical_data(symbol, days)
        
        if df is not None and len(df) > 0:
            min_required_days = 250
            if len(df) < min_required_days:
                logger.warning(f"Мало исторических данных для {symbol}: {len(df)} дней (< {min_required_days})")
                
                if self.test_mode:
                    logger.info(f"В тестовом режиме создаем данные для {symbol}")
                    df = self.create_test_data(symbol, days)
            
            self._cache['historical_data'][cache_key] = {
                'data': df,
                'timestamp': datetime.now(),
                'ttl': 3600
            }
        else:
            logger.error(f"Не удалось получить исторические данные для {symbol}")
            
            if self.test_mode or 'test' in symbol.lower():
                logger.info(f"Создаем тестовые данные для {symbol}")
                df = self.create_test_data(symbol, days)
                if df is not None:
                    self._cache['historical_data'][cache_key] = {
                        'data': df,
                        'timestamp': datetime.now(),
                        'ttl': 3600
                    }
        
        return df
    
    def create_test_data(self, symbol: str, days: int = 400) -> pd.DataFrame:
        """Создание тестовых данных для отладки"""
        logger.warning(f"Создаем тестовые данные для {symbol}")
        
        dates = pd.date_range(end=datetime.now(), periods=days, freq='D')
        
        if 'SBER' in symbol:
            base_price = 300.0
            volatility = 0.015
        elif 'GAZP' in symbol:
            base_price = 180.0
            volatility = 0.02
        elif 'IMOEX' in symbol:
            base_price = 3500.0
            volatility = 0.01
        elif 'LKOH' in symbol:
            base_price = 7500.0
            volatility = 0.018
        elif 'YNDX' in symbol:
            base_price = 2500.0
            volatility = 0.025
        else:
            base_price = 100.0
            volatility = 0.02
        
        np.random.seed(hash(symbol) % 10000)
        returns = np.random.normal(0.0003, volatility, days)
        prices = base_price * np.exp(np.cumsum(returns))
        
        df = pd.DataFrame({
            'timestamp': dates,
            'open': prices * 0.995,
            'high': prices * 1.015,
            'low': prices * 0.985,
            'close': prices,
            'volume': np.random.randint(1000000, 10000000, days)
        })
        
        logger.info(f"Созданы тестовые данные для {symbol}: {len(df)} дней")
        return df
    
    def get_benchmark_data(self) -> Optional[Dict[str, float]]:
        """Получение данных бенчмарка"""
        try:
            cache = self._cache['benchmark_data']
            if cache['data'] and (datetime.now() - cache['timestamp']).seconds < cache['ttl']:
                return cache['data']
            
            logger.info(f"Получение данных бенчмарка {self.benchmark_symbol}...")
            
            df = self.get_cached_historical_data(self.benchmark_symbol, 400)
            if df is None or len(df) < 126:
                logger.warning(f"Недостаточно данных бенчмарка")
                
                if self.test_mode:
                    logger.info(f"В тестовом режиме используем тестовые данные")
                    df = self.create_test_data(self.benchmark_symbol, 400)
                else:
                    logger.error(f"Не удалось получить данные бенчмарка {self.benchmark_symbol}")
                    return None
            
            current_price = df['close'].iloc[-1]
            
            if len(df) >= 126:
                price_6m_ago = df['close'].iloc[-126]
            else:
                price_6m_ago = df['close'].iloc[0]
                logger.warning(f"Используем первые данные для расчета 6M моментума бенчмарка")
            
            benchmark_absolute_momentum_6m = ((current_price - price_6m_ago) / price_6m_ago) * 100
            
            benchmark_data = {
                'symbol': self.benchmark_symbol,
                'name': self.benchmark_name,
                'absolute_momentum_6m': benchmark_absolute_momentum_6m,
                'current_price': current_price,
                'price_6m_ago': price_6m_ago,
                'timestamp': datetime.now()
            }
            
            self._cache['benchmark_data'] = {
                'data': benchmark_data,
                'timestamp': datetime.now(),
                'ttl': 3600
            }
            
            logger.info(f"Данные бенчмарка: 6M моментум = {benchmark_absolute_momentum_6m:.2f}%")
            
            return benchmark_data
            
        except Exception as e:
            logger.error(f"Ошибка получения данных бенчмарка: {e}")
            
            return {
                'symbol': self.benchmark_symbol,
                'name': self.benchmark_name,
                'absolute_momentum_6m': 8.5,
                'current_price': 3500.0,
                'price_6m_ago': 3225.0,
                'timestamp': datetime.now()
            }
    
    def calculate_momentum_values(self, asset_info: Dict) -> Optional[AssetData]:
        """Расчет значений моментума"""
        try:
            symbol = asset_info['symbol']
            name = asset_info['name']
            source = asset_info.get('source', 'unknown')
            
            logger.info(f"Расчет моментума для {symbol} ({name})...")
            
            df = self.get_cached_historical_data(symbol, 400)
            if df is None:
                logger.error(f"Нет исторических данных для {symbol}")
                return None
            
            min_required = 252
            if len(df) < min_required:
                logger.warning(f"Мало данных для {symbol}: {len(df)} дней (< {min_required})")
                
                if len(df) < 100:
                    logger.error(f"Слишком мало данных для анализа {symbol}: {len(df)} дней")
                    return None
            
            n = len(df)
            current_price = df['close'].iloc[-1]
            
            if current_price <= 0:
                logger.error(f"Некорректная цена для {symbol}: {current_price}")
                return None
            
            price_1w_ago = df['close'].iloc[-5] if n >= 5 else current_price
            price_1m_ago = df['close'].iloc[-21] if n >= 21 else current_price
            price_6m_ago = df['close'].iloc[-126] if n >= 126 else current_price
            price_12m_ago = df['close'].iloc[-252] if n >= 252 else current_price
            
            try:
                momentum_1m = ((price_1w_ago - price_1m_ago) / price_1m_ago) * 100 if price_1m_ago > 0 else 0
                momentum_6m = ((price_1m_ago - price_6m_ago) / price_6m_ago) * 100 if price_6m_ago > 0 else 0
                momentum_12m = ((price_1m_ago - price_12m_ago) / price_12m_ago) * 100 if price_12m_ago > 0 else 0
                absolute_momentum = ((current_price - price_12m_ago) / price_12m_ago) * 100 if price_12m_ago > 0 else 0
                absolute_momentum_6m = ((current_price - price_6m_ago) / price_6m_ago) * 100 if price_6m_ago > 0 else 0
            except ZeroDivisionError:
                logger.error(f"Ошибка деления на ноль для {symbol}")
                return None
            
            combined_momentum = (
                momentum_12m * self.weights['12M'] +
                momentum_6m * self.weights['6M'] +
                momentum_1m * self.weights['1M']
            )
            
            sma_fast = df['close'].tail(self.sma_fast_period).mean()
            sma_slow = df['close'].tail(self.sma_slow_period).mean()
            sma_signal = sma_fast > sma_slow
            
            volume_24h = asset_info.get('volume_24h', 0)
            sector = asset_info.get('sector', '')
            market_type = asset_info.get('market_type', 'stock')
            
            logger.info(f"  {symbol}: Цена {current_price:.2f}, 12M: {momentum_12m:+.1f}%, 6M: {absolute_momentum_6m:+.1f}%, 1M: {momentum_1m:+.1f}%, SMA: {'+' if sma_signal else '-'}")
            
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
            logger.error(f"Ошибка расчета моментума для {symbol}: {e}")
            return None
    
    def analyze_assets(self) -> List[AssetData]:
        """Анализ активов"""
        top_assets = self.get_top_assets()
        if not top_assets:
            logger.error("Нет активов для анализа")
            return []
        
        logger.info(f"Анализ {len(top_assets)} активов...")
        
        analyzed_assets = []
        benchmark_data = self.get_benchmark_data()
        
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
            filter_stats['total'] += 1
            
            try:
                asset_data = self.calculate_momentum_values(asset_info)
                if asset_data is None:
                    filter_stats['no_data'] += 1
                    logger.warning(f"  {symbol}: нет данных для анализа")
                    continue
                
                if asset_data.momentum_12m < self.min_12m_momentum:
                    filter_stats['failed_12m'] += 1
                    logger.debug(f"  {symbol}: низкий 12M моментум ({asset_data.momentum_12m:+.1f}% < {self.min_12m_momentum}%)")
                    continue
                filter_stats['passed_12m'] += 1
                
                if not asset_data.sma_signal:
                    filter_stats['failed_sma'] += 1
                    logger.debug(f"  {symbol}: отрицательный SMA сигнал")
                    continue
                filter_stats['passed_sma'] += 1
                
                if benchmark_data:
                    if asset_data.absolute_momentum_6m <= benchmark_data['absolute_momentum_6m']:
                        filter_stats['failed_benchmark'] += 1
                        logger.debug(f"  {symbol}: 6M моментум ({asset_data.absolute_momentum_6m:+.1f}%) <= бенчмарку ({benchmark_data['absolute_momentum_6m']:+.1f}%)")
                        continue
                    filter_stats['passed_benchmark'] += 1
                
                analyzed_assets.append(asset_data)
                filter_stats['passed_all'] += 1
                logger.info(f"  {symbol}: добавлен в анализ")
                
            except Exception as e:
                filter_stats['errors'] += 1
                logger.error(f"Ошибка анализа {symbol}: {e}")
                continue
        
        analyzed_assets.sort(key=lambda x: x.combined_momentum, reverse=True)
        selected_assets = analyzed_assets[:self.selected_count]
        
        logger.info("=" * 60)
        logger.info(f"ИТОГ анализа: {len(selected_assets)} активов отобрано")
        logger.info(f"Бенчмарк {self.benchmark_symbol}: 6M моментум = {benchmark_data['absolute_momentum_6m']:+.1f}%" if benchmark_data else "Бенчмарк: данные недоступны")
        logger.info(f"Статистика фильтрации:")
        logger.info(f"  • Всего акций: {filter_stats['total']}")
        logger.info(f"  • Прошли все фильтры: {filter_stats['passed_all']}")
        logger.info(f"  • Прошли 12M моментум: {filter_stats['passed_12m']} (провалили: {filter_stats['failed_12m']})")
        logger.info(f"  • Прошли SMA: {filter_stats['passed_sma']} (провалили: {filter_stats['failed_sma']})")
        if benchmark_data:
            logger.info(f"  • Прошли сравнение с бенчмарком: {filter_stats['passed_benchmark']} (провалили: {filter_stats['failed_benchmark']})")
        logger.info(f"  • Без данных: {filter_stats['no_data']}")
        logger.info(f"  • Ошибки анализа: {filter_stats['errors']}")
        
        if filter_stats['passed_all'] == 0:
            logger.warning("Все активы отфильтрованы по критериям")
            if filter_stats['failed_12m'] > 0:
                logger.warning(f"  • Основная причина: 12M моментум < {self.min_12m_momentum}%")
        
        if selected_assets:
            logger.info("Топ активов:")
            for i, asset in enumerate(selected_assets, 1):
                vs_benchmark = f" vs бенчмарк: {asset.absolute_momentum_6m - benchmark_data['absolute_momentum_6m']:+.1f}%" if benchmark_data else ""
                logger.info(f"  {i:2d}. {asset.symbol}: {asset.combined_momentum:+.2f}% (12M: {asset.momentum_12m:+.1f}%, 6M: {asset.absolute_momentum_6m:+.1f}%{vs_benchmark})")
        
        return selected_assets
    
    def generate_signals(self, assets: List[AssetData]) -> List[Dict]:
        """Генерация сигналов"""
        signals = []
        
        for asset in assets:
            symbol = asset.symbol
            current_status = self.current_portfolio.get(symbol, {}).get('status', 'OUT')
            
            if (asset.absolute_momentum > 0 and 
                asset.sma_signal and 
                current_status != 'IN'):
                
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
                    'reason': f"Моментум: {asset.absolute_momentum:+.1f}%, SMA сигнал положительный",
                    'timestamp': datetime.now()
                }
                
                self.current_portfolio[symbol] = {
                    'entry_time': datetime.now(),
                    'entry_price': asset.current_price,
                    'status': 'IN',
                    'name': asset.name,
                    'sector': asset.sector,
                    'source': asset.source
                }
                
                signals.append(signal)
                logger.info(f"BUY для {symbol} ({asset.name})")
            
            elif (current_status == 'IN' and 
                  (asset.absolute_momentum < 0 or not asset.sma_signal)):
                
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
                    'reason': f"Выход: {'Моментум < 0' if asset.absolute_momentum < 0 else 'SMA сигнал отрицательный'}",
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
                logger.info(f"SELL для {symbol}: {profit_percent:+.2f}%")
        
        return signals
    
    def send_telegram_message(self, message: str, silent: bool = False) -> bool:
        """Отправка сообщения в Telegram"""
        if not self.telegram_token or not self.telegram_chat_id:
            if not silent:
                logger.warning("Нет данных для Telegram")
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
                        logger.info("Сообщение отправлено в Telegram")
                    return True
                else:
                    error_msg = f"Ошибка Telegram (попытка {attempt+1}): {response.status_code}"
                    if not silent:
                        logger.warning(error_msg)
                        logger.warning(f"Ответ: {response.text}")
                    
            except requests.exceptions.Timeout:
                error_msg = f"Таймаут Telegram (попытка {attempt+1})"
                if not silent:
                    logger.warning(error_msg)
            except requests.exceptions.ConnectionError:
                error_msg = f"Ошибка соединения Telegram (попытка {attempt+1})"
                if not silent:
                    logger.warning(error_msg)
            except Exception as e:
                error_msg = f"Ошибка отправки Telegram (попытка {attempt+1}): {e}"
                if not silent:
                    logger.warning(error_msg)
            
            if attempt < self.max_telegram_retries - 1:
                time.sleep(self.telegram_retry_delay)
        
        if not silent:
            logger.error("Не удалось отправить сообщение в Telegram после всех попыток")
        return False
    
    def format_active_positions(self) -> str:
        """Форматирование списка активных позиций"""
        active_positions = {k: v for k, v in self.current_portfolio.items() 
                          if v.get('status') == 'IN'}
        
        if not active_positions:
            return "АКТИВНЫХ ПОЗИЦИЙ НЕТ\nВсе средства в рублях"
        
        message = "АКТИВНЫЕ ПОЗИЦИИ:\n"
        message += "═══════════════════════════\n"
        
        total_profit = 0
        position_count = 0
        
        for symbol, data in active_positions.items():
            entry_price = data.get('entry_price', 0)
            entry_time = data.get('entry_time', datetime.now())
            name = data.get('name', symbol)
            sector = data.get('sector', '')
            
            try:
                price, _, source = self.data_fetcher.get_current_price(symbol)
                if price and price > 0:
                    profit_percent = ((price - entry_price) / entry_price) * 100
                    profit_emoji = "+" if profit_percent > 0 else "-"
                    
                    message += (
                        f"• {symbol} ({name})\n"
                        f"  Сектор: {sector}\n"
                        f"  Вход: {entry_price:.2f} руб\n"
                        f"  Текущая: {price:.2f} руб\n"
                        f"  P&L: {profit_percent:+.2f}% {profit_emoji}\n"
                        f"  Вход: {entry_time.strftime('%d.%m.%Y %H:%M')}\n"
                        f"  Источник: {source}\n"
                        f"  ─\n"
                    )
                    
                    total_profit += profit_percent
                    position_count += 1
                else:
                    message += (
                        f"• {symbol} ({name})\n"
                        f"  Не удалось получить текущую цену\n"
                        f"  Вход: {entry_price:.2f} руб\n"
                        f"  Вход: {entry_time.strftime('%d.%m.%Y %H:%M')}\n"
                        f"  ─\n"
                    )
            except Exception as e:
                logger.error(f"Ошибка получения цены для {symbol}: {e}")
                continue
        
        if position_count > 0:
            avg_profit = total_profit / position_count
            message += f"═══════════════════════════\n"
            message += f"Средняя прибыль: {avg_profit:+.2f}%\n"
        
        message += f"Всего позиций: {len(active_positions)}/{self.selected_count}"
        
        return message
    
    def format_signal_message(self, signal: Dict) -> str:
        """Форматирование сигнала"""
        if signal['action'] == 'BUY':
            return (
                f"BUY: {signal['symbol']}\n"
                f"═══════════════════════════\n"
                f"{signal.get('sector', 'Акция')}\n"
                f"Цена: {signal['price']:.2f} руб\n"
                f"Абсолютный моментум (12M): {signal['absolute_momentum']:+.1f}%\n"
                f"Абсолютный моментум (6M): {signal.get('absolute_momentum_6m', 0):+.1f}%\n"
                f"Относительный моментум (12M): {signal['momentum_12m']:+.1f}%\n"
                f"• 6M: {signal['momentum_6m']:+.1f}%\n"
                f"• 1M: {signal['momentum_1m']:+.1f}%\n"
                f"Комбинированный: {signal['combined_momentum']:+.1f}%\n"
                f"Время: {signal['timestamp'].strftime('%H:%M:%S %d.%m.%Y')}\n"
                f"═══════════════════════════\n"
                f"{signal['reason']}"
            )
        else:
            profit_emoji = "+" if signal['profit_percent'] > 0 else "-"
            return (
                f"SELL: {signal['symbol']}\n"
                f"═══════════════════════════\n"
                f"Цена входа: {signal['entry_price']:.2f} руб\n"
                f"Цена выхода: {signal['price']:.2f} руб\n"
                f"Прибыль: {signal['profit_percent']:+.2f}% {profit_emoji}\n"
                f"Абсолютный моментум: {signal['absolute_momentum']:+.1f}%\n"
                f"Абсолютный моментум 6M: {signal.get('absolute_momentum_6m', 0):+.1f}%\n"
                f"Время: {signal['timestamp'].strftime('%H:%M:%S %d.%m.%Y')}\n"
                f"═══════════════════════════\n"
                f"{signal['reason']}"
            )
    
    def format_ranking_message(self, assets: List[AssetData]) -> str:
        """Форматирование рейтинга"""
        benchmark_data = self.get_benchmark_data()
        
        message = f"MOMENTUM РЕЙТИНГ МОСБИРЖИ\n"
        
        if self.test_mode:
            message += f"РЕЖИМ ТЕСТИРОВАНИЯ\n"
        
        message += f"Отбор: {len(assets)} из {self.top_assets_count} активов\n"
        
        if benchmark_data:
            message += f"Бенчмарк ({self.benchmark_symbol}): {benchmark_data['absolute_momentum_6m']:+.1f}% (6M)\n"
        
        message += "═══════════════════════════\n"
        
        if not assets:
            message += "Нет активов, соответствующих критериям\n"
            message += "Возможные причины:\n"
            message += "• Все акции имеют отрицательный 12M моментум\n"
            message += "• SMA сигналы отрицательные\n"
            message += "• Рынок в нисходящем тренде\n"
            message += "═══════════════════════════\n"
            return message
        
        for i, asset in enumerate(assets, 1):
            status = "IN" if self.current_portfolio.get(asset.symbol, {}).get('status') == 'IN' else "OUT"
            
            benchmark_comparison = ""
            if benchmark_data:
                vs_benchmark = asset.absolute_momentum_6m - benchmark_data['absolute_momentum_6m']
                if vs_benchmark > 0:
                    benchmark_comparison = f"\nvs бенчмарк: +{vs_benchmark:.1f}%"
                else:
                    benchmark_comparison = f"\nvs бенчмарк: {vs_benchmark:.1f}%"
            
            message += (
                f"#{i} {asset.symbol} ({asset.name}) {status}\n"
                f"{asset.sector}\n"
                f"{asset.current_price:.2f} руб\n"
                f"Моментум:\n"
                f"  • 12M: {asset.momentum_12m:+.1f}%\n"
                f"  • 6M: {asset.absolute_momentum_6m:+.1f}%{benchmark_comparison}\n"
                f"  • 1M: {asset.momentum_1m:+.1f}%\n"
                f"  • Комбинированный: {asset.combined_momentum:+.1f}%\n"
                f"SMA: {'Растущий' if asset.sma_signal else 'Падающий'}\n"
                f"Источник: {asset.source}\n"
                f"──\n"
            )
        
        message += "═══════════════════════════\n"
        message += "ПАРАМЕТРЫ СТРАТЕГИИ:\n"
        message += f"• Отбор: топ {self.selected_count} из {self.top_assets_count}\n"
        message += f"• Минимальный 12M моментум: {self.min_12m_momentum}%\n"
        message += f"• Минимальный объем: {self.min_volume_24h:,} руб\n"
        message += f"• Минимальная цена: {self.min_price} руб\n"
        message += f"• Бенчмарк: {self.benchmark_symbol}\n"
        message += f"• SMA: {self.sma_fast_period}/{self.sma_slow_period} дней\n"
        message += f"• Веса: 12M({self.weights['12M']*100:.0f}%), 6M({self.weights['6M']*100:.0f}%), 1M({self.weights['1M']*100:.0f}%)\n"
        message += f"• Проверка: каждые {self.check_interval//3600} часа\n"
        
        if self.test_mode:
            message += f"• РЕЖИМ: ТЕСТИРОВАНИЕ (данные сгенерированы)\n"
        
        active_count = sum(1 for v in self.current_portfolio.values() if v.get('status') == 'IN')
        if active_count > 0:
            message += f"• Активных позиций: {active_count}/{self.selected_count}\n"
        
        return message
    
    def run_strategy_cycle(self) -> bool:
        """Запуск цикла стратегии"""
        try:
            logger.info("Запуск цикла стратегии...")
            
            assets = self.analyze_assets()
            
            if not assets:
                logger.warning("Нет активов для анализа")
                
                benchmark_data = self.get_benchmark_data()
                no_assets_msg = (
                    "Анализ Мосбиржи\n"
                    "Нет активов, соответствующих критериям.\n\n"
                    f"• Проверено акций: из {self.top_assets_count}\n"
                    f"• Требование 12M моментум: > {self.min_12m_momentum}%\n"
                    f"• Требование SMA: положительный сигнал\n"
                )
                
                if benchmark_data:
                    no_assets_msg += f"• Бенчмарк ({self.benchmark_symbol}): {benchmark_data['absolute_momentum_6m']:+.1f}%\n"
                
                no_assets_msg += "\nВозможно, рынок в нисходящем тренде."
                
                if self.test_mode:
                    no_assets_msg += "\n\nРЕЖИМ ТЕСТИРОВАНИЯ"
                
                self.send_telegram_message(no_assets_msg)
                
                active_positions = self.format_active_positions()
                if "АКТИВНЫХ ПОЗИЦИЙ НЕТ" not in active_positions:
                    self.send_telegram_message(active_positions)
                
                return False
            
            self.asset_ranking = assets
            
            signals = self.generate_signals(assets)
            
            for signal in signals:
                message = self.format_signal_message(signal)
                if self.send_telegram_message(message):
                    self.signal_history.append(signal)
                    logger.info(f"Сигнал отправлен: {signal['symbol']} {signal['action']}")
            
            ranking_message = self.format_ranking_message(assets)
            self.send_telegram_message(ranking_message)
            
            logger.info(f"Цикл завершен. Сигналов: {len(signals)}")
            return True
            
        except Exception as e:
            logger.error(f"Ошибка в цикле: {e}")
            self.errors_count += 1
            
            error_msg = (
                f"ОШИБКА АНАЛИЗА\n"
                f"Произошла ошибка при анализе активов:\n"
                f"{str(e)[:200]}\n"
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
                'test_mode': self.test_mode,
                'version': 'tinkoff_bot_v1'
            }
            
            with open('logs/bot_state_tinkoff.json', 'w') as f:
                json.dump(state, f, default=str, indent=2)
            
            logger.info("Состояние сохранено")
        except Exception as e:
            logger.error(f"Ошибка сохранения: {e}")
    
    def load_state(self):
        """Загрузка состояния"""
        try:
            if os.path.exists('logs/bot_state_tinkoff.json'):
                with open('logs/bot_state_tinkoff.json', 'r') as f:
                    state = json.load(f)
                
                self.current_portfolio = state.get('current_portfolio', {})
                
                for symbol, data in self.current_portfolio.items():
                    if 'entry_time' in data and isinstance(data['entry_time'], str):
                        data['entry_time'] = datetime.fromisoformat(data['entry_time'].replace('Z', '+00:00'))
                    if 'exit_time' in data and isinstance(data['exit_time'], str):
                        data['exit_time'] = datetime.fromisoformat(data['exit_time'].replace('Z', '+00:00'))
                
                self.signal_history = state.get('signal_history', [])
                self.errors_count = state.get('errors_count', 0)
                self.test_mode = state.get('test_mode', False)
                
                active_count = len([v for v in self.current_portfolio.values() if v.get('status') == 'IN'])
                logger.info(f"Состояние загружено. Активных позиций: {active_count}")
                if self.test_mode:
                    logger.warning("Загружен тестовый режим из сохранения")
        except Exception as e:
            logger.error(f"Ошибка загрузки: {e}")
    
    def run(self):
        """Основной цикл"""
        logger.info("=" * 60)
        logger.info("ЗАПУСК MOMENTUM BOT ДЛЯ TINKOFF API")
        logger.info("=" * 60)
        
        self.load_state()
        
        if self.telegram_token and self.telegram_chat_id:
            sandbox_info = " (ПЕСОЧНИЦА)" if self.tinkoff_sandbox else ""
            welcome_msg = (
                "MOMENTUM BOT ДЛЯ TINKOFF API ЗАПУЩЕН\n"
                f"Стратегия: Momentum с фильтрацией\n"
                f"Отбор: топ {self.selected_count} из {self.top_assets_count}\n"
                f"Бенчмарк: {self.benchmark_symbol}\n"
                f"Фильтры: 12M > {self.min_12m_momentum}%, объем > {self.min_volume_24h:,} руб\n"
                f"Источник данных: Tinkoff API{sandbox_info}\n"
                f"Проверка: каждые {self.check_interval//3600} часа\n"
                f"Версия: Tinkoff API миграция"
            )
            self.send_telegram_message(welcome_msg)
            
            active_positions_msg = self.format_active_positions()
            self.send_telegram_message(active_positions_msg)
        else:
            logger.warning("Telegram не настроен, пропускаем приветственное сообщение")
        
        iteration = 0
        
        while True:
            iteration += 1
            current_time = datetime.now().strftime('%H:%M:%S %d.%m.%Y')
            logger.info(f"Цикл #{iteration} - {current_time}")
            
            try:
                success = self.run_strategy_cycle()
                
                if success:
                    logger.info(f"Цикл #{iteration} успешно завершен")
                    
                    if iteration % 3 == 0:
                        self.save_state()
                else:
                    logger.warning(f"Цикл #{iteration} завершен с проблемами")
                
                if self.errors_count > 5:
                    logger.error(f"Много ошибок ({self.errors_count}). Пауза 1 час...")
                    if self.telegram_token and self.telegram_chat_id:
                        self.send_telegram_message("МНОГО ОШИБОК \nБот делает паузу 1 час")
                    time.sleep(3600)
                    self.errors_count = 0
                
                logger.info(f"Следующая проверка через {self.check_interval//3600} часа(ов)...")
                time.sleep(self.check_interval)
                
            except KeyboardInterrupt:
                logger.info("Остановка по команде пользователя")
                self.save_state()
                if self.telegram_token and self.telegram_chat_id:
                    self.send_telegram_message("BOT ОСТАНОВЛЕН ПОЛЬЗОВАТЕЛЕМ")
                break
                
            except Exception as e:
                logger.error(f"Критическая ошибка: {e}")
                self.errors_count += 1
                if self.telegram_token and self.telegram_chat_id:
                    self.send_telegram_message(f"КРИТИЧЕСКАЯ ОШИБКА \n{str(e)[:100]}")
                
                delay = min(300 * self.errors_count, 3600)
                logger.info(f"Пауза {delay} секунд из-за ошибок...")
                time.sleep(delay)


def main():
    bot = MomentumBotTinkoff()
    
    try:
        bot.run()
    except Exception as e:
        logger.error(f"Фатальная ошибка: {e}")
        if bot.telegram_token and bot.telegram_chat_id:
            bot.send_telegram_message(f"ФАТАЛЬНАЯ ОШИБКА \nБот завершил работу: {str(e)[:200]}")


if __name__ == "__main__":
    main()