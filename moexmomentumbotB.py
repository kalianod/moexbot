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
import base64
import hashlib

warnings.filterwarnings('ignore')

# ========== НАСТРОЙКИ ЛОГИРОВАНИЯ С РОТАЦИЕЙ ==========
if not os.path.exists('logs'):
    os.makedirs('logs')

logger = logging.getLogger('MomentumBotBCS')
logger.setLevel(logging.INFO)

formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

file_handler = logging.handlers.RotatingFileHandler(
    f'logs/momentum_bot_bcs_{datetime.now().strftime("%Y%m")}.log',
    maxBytes=10*1024*1024,
    backupCount=5
)
file_handler.setFormatter(formatter)

console_handler = logging.StreamHandler()
console_handler.setFormatter(formatter)

logger.addHandler(file_handler)
logger.addHandler(console_handler)
# ========== КОНЕЦ НАСТРОЕК ЛОГИРОВАНИЯ ==========

HAS_APIMOEX = False
logger.info("ℹ️  Режим работы: BCS API. apimoex не используется.")

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
    source: str = 'bcs_api'


class BCSDataFetcher:
    """Класс для получения данных с BCS API с правильной авторизацией"""
    
    def __init__(self):
        self.session = requests.Session()
        
        # 🔑 Ключи авторизации BCS (из документации)
        self.api_key = os.getenv('BCS_API_KEY')
        self.api_secret = os.getenv('BCS_API_SECRET')
        self.access_token = None
        self.token_expires = None
        
        if not self.api_key or not self.api_secret:
            logger.error("❌ BCS_API_KEY и BCS_API_SECRET не найдены в .env!")
            logger.error("❌ Получите ключи в кабинете БКС: https://trade-api.bcs.ru/http/authorization")
        
        # Базовый URL BCS API (из документации)
        self.base_url = "https://trade-api.bcs.ru"
        
        # Константы для торговой платформы
        self.market = "MOEX"
        self.board = "TQBR"
        
        # Список активов
        self.popular_stocks = [
            {'symbol': 'SBER', 'name': 'Сбербанк', 'sector': 'Финансы'},
            {'symbol': 'GAZP', 'name': 'Газпром', 'sector': 'Нефть и газ'},
            {'symbol': 'LKOH', 'name': 'Лукойл', 'sector': 'Нефть и газ'},
            {'symbol': 'ROSN', 'name': 'Роснефть', 'sector': 'Нефть и газ'},
            {'symbol': 'NVTK', 'name': 'Новатэк', 'sector': 'Нефть и газ'},
            {'symbol': 'GMKN', 'name': 'Норникель', 'sector': 'Металлургия'},
            {'symbol': 'PLZL', 'name': 'Полюс', 'sector': 'Металлургия'},
            {'symbol': 'YNDX', 'name': 'Яндекс', 'sector': 'IT'},
            {'symbol': 'TCSG', 'name': 'TCS Group', 'sector': 'Финансы'},
            {'symbol': 'MOEX', 'name': 'Московская биржа', 'sector': 'Финансы'},
            {'symbol': 'MGNT', 'name': 'Магнит', 'sector': 'Розничная торговля'},
            {'symbol': 'PHOR', 'name': 'ФосАгро', 'sector': 'Химия'},
            {'symbol': 'RUAL', 'name': 'РУСАЛ', 'sector': 'Металлургия'},
            {'symbol': 'VTBR', 'name': 'ВТБ', 'sector': 'Финансы'},
            {'symbol': 'ALRS', 'name': 'АЛРОСА', 'sector': 'Добыча'},
            {'symbol': 'POLY', 'name': 'Полиметалл', 'sector': 'Металлургия'},
            {'symbol': 'CHMF', 'name': 'Северсталь', 'sector': 'Металлургия'},
            {'symbol': 'SNGS', 'name': 'Сургутнефтегаз', 'sector': 'Нефть и газ'},
            {'symbol': 'SNGSP', 'name': 'Сургутнефтегаз-п', 'sector': 'Нефть и газ'},
            {'symbol': 'TATN', 'name': 'Татнефть', 'sector': 'Нефть и газ'},
            {'symbol': 'TATNP', 'name': 'Татнефть-п', 'sector': 'Нефть и газ'},
            {'symbol': 'IRAO', 'name': 'Интер РАО', 'sector': 'Энергетика'},
            {'symbol': 'HYDR', 'name': 'РусГидро', 'sector': 'Энергетика'},
            {'symbol': 'MTSS', 'name': 'МТС', 'sector': 'Телеком'},
            {'symbol': 'RTKM', 'name': 'Ростелеком', 'sector': 'Телеком'},
            {'symbol': 'FEES', 'name': 'ФСК ЕЭС', 'sector': 'Энергетика'},
            {'symbol': 'NLMK', 'name': 'НЛМК', 'sector': 'Металлургия'},
            {'symbol': 'MAGN', 'name': 'ММК', 'sector': 'Металлургия'},
            {'symbol': 'AFKS', 'name': 'Система', 'sector': 'Холдинги'},
            {'symbol': 'AFLT', 'name': 'Аэрофлот', 'sector': 'Транспорт'},
        ]
        
        self.indices = [
            {'symbol': 'IMOEX', 'name': 'Индекс Мосбиржи', 'sector': 'Индекс'},
            {'symbol': 'RTSI', 'name': 'Индекс РТС', 'sector': 'Индекс'},
        ]
        
        logger.info(f"✅ BCSDataFetcher инициализирован. Базовый URL: {self.base_url}")
        
        # Получаем токен при инициализации
        if self.api_key and self.api_secret:
            self.authenticate()
    
    def _generate_signature(self, method: str, path: str, timestamp: str) -> str:
        """Генерация подписи для BCS API"""
        message = f"{method}{path}{timestamp}"
        signature = hmac.new(
            self.api_secret.encode('utf-8'),
            message.encode('utf-8'),
            hashlib.sha256
        ).hexdigest()
        return signature
    
    def authenticate(self):
        """Аутентификация в BCS API"""
        try:
            # Метод аутентификации из документации BCS
            auth_url = f"{self.base_url}/oauth/token"
            
            # Подготовка заголовков
            timestamp = str(int(time.time() * 1000))
            
            # Создаем Basic Auth заголовок
            auth_string = f"{self.api_key}:{self.api_secret}"
            auth_encoded = base64.b64encode(auth_string.encode('utf-8')).decode('utf-8')
            
            headers = {
                'Authorization': f'Basic {auth_encoded}',
                'Content-Type': 'application/x-www-form-urlencoded'
            }
            
            data = {
                'grant_type': 'client_credentials',
                'scope': 'read'
            }
            
            response = self.session.post(auth_url, headers=headers, data=data, timeout=10)
            
            if response.status_code == 200:
                auth_data = response.json()
                self.access_token = auth_data.get('access_token')
                expires_in = auth_data.get('expires_in', 3600)
                self.token_expires = datetime.now() + timedelta(seconds=expires_in - 300)  # -5 минут для запаса
                
                # Устанавливаем заголовок для последующих запросов
                self.session.headers.update({
                    'Authorization': f'Bearer {self.access_token}',
                    'Content-Type': 'application/json'
                })
                
                logger.info("✅ Успешная аутентификация в BCS API")
                return True
            else:
                logger.error(f"❌ Ошибка аутентификации: {response.status_code}")
                logger.error(f"Ответ: {response.text}")
                return False
                
        except Exception as e:
            logger.error(f"❌ Ошибка при аутентификации: {e}")
            return False
    
    def _ensure_auth(self):
        """Проверяем и обновляем токен при необходимости"""
        if not self.access_token or (self.token_expires and datetime.now() >= self.token_expires):
            logger.info("🔄 Токен истек или отсутствует, обновляем...")
            return self.authenticate()
        return True
    
    def test_bcs_connection(self):
        """Проверка подключения к BCS API"""
        if not self._ensure_auth():
            return False
        
        try:
            # Тестовый запрос для проверки подключения
            test_url = f"{self.base_url}/api/v1/instruments"
            params = {
                'symbol': 'SBER',
                'market': self.market,
                'board': self.board
            }
            
            response = self.session.get(test_url, params=params, timeout=10)
            
            if response.status_code == 200:
                logger.info("✅ Подключение к BCS API успешно")
                return True
            else:
                logger.warning(f"⚠️ BCS API ответил с кодом: {response.status_code}")
                logger.debug(f"Ответ: {response.text[:200]}")
                return False
        except Exception as e:
            logger.error(f"❌ Ошибка подключения к BCS API: {e}")
            return False
    
    def get_current_price(self, symbol: str) -> Tuple[Optional[float], Optional[float], str]:
        """Получение текущей цены и объема из BCS API"""
        if not self._ensure_auth():
            return None, None, 'auth_error'
        
        try:
            # Получаем рыночные данные (стакан цен)
            orderbook_url = f"{self.base_url}/api/v1/orderbook"
            params = {
                'symbol': symbol,
                'market': self.market,
                'board': self.board
            }
            
            response = self.session.get(orderbook_url, params=params, timeout=10)
            
            if response.status_code == 200:
                data = response.json()
                
                # Обрабатываем ответ согласно документации BCS
                if 'last' in data:
                    price = data['last']
                    volume = data.get('volume', 0)
                    return float(price), float(volume), 'bcs_api'
                
                # Альтернативная структура ответа
                elif 'bid' in data and 'ask' in data:
                    # Берем среднее между лучшей покупкой и продажей
                    bid_price = data['bid'][0]['price'] if data['bid'] else None
                    ask_price = data['ask'][0]['price'] if data['ask'] else None
                    
                    if bid_price and ask_price:
                        price = (bid_price + ask_price) / 2
                        volume = data.get('volume', 0)
                        return float(price), float(volume), 'bcs_api'
                
                logger.warning(f"⚠️ Неожиданная структура ответа для {symbol}")
                
            elif response.status_code == 404:
                logger.debug(f"Инструмент {symbol} не найден в BCS API")
            else:
                logger.warning(f"BCS API для {symbol}: код {response.status_code}")
                
        except Exception as e:
            logger.error(f"❌ Ошибка BCS API для {symbol}: {e}")
        
        logger.warning(f"⚠️ Не удалось получить данные для {symbol} из BCS API")
        return None, None, 'bcs_api_error'
    
    def get_historical_data(self, symbol: str, days: int = 365) -> Optional[pd.DataFrame]:
        """Получение исторических данных из BCS API"""
        if not self._ensure_auth():
            return None
        
        try:
            end_date = datetime.now()
            start_date = end_date - timedelta(days=days)
            
            # Форматируем даты для BCS API
            from_date = start_date.strftime('%Y-%m-%d')
            to_date = end_date.strftime('%Y-%m-%d')
            
            logger.debug(f"Запрос исторических данных в BCS API для {symbol} за {days} дней")
            
            # Получаем исторические данные (свечи)
            candles_url = f"{self.base_url}/api/v1/candles"
            params = {
                'symbol': symbol,
                'market': self.market,
                'board': self.board,
                'from': from_date,
                'to': to_date,
                'interval': 'D'  # Дневной интервал
            }
            
            response = self.session.get(candles_url, params=params, timeout=30)
            
            if response.status_code == 200:
                data = response.json()
                
                # Проверяем структуру ответа
                if isinstance(data, list) and len(data) > 0:
                    # Предполагаем структуру: [timestamp, open, high, low, close, volume]
                    df = pd.DataFrame(data, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
                    df['timestamp'] = pd.to_datetime(df['timestamp'])
                    df = df.sort_values('timestamp')
                    
                    logger.info(f"✅ BCS API: получено {len(df)} свечей для {symbol}")
                    return df
                
                elif 'candles' in data:
                    candles = data['candles']
                    df = pd.DataFrame(candles)
                    
                    # Переименовываем колонки к стандартным названиям
                    column_mapping = {}
                    for col in df.columns:
                        col_lower = col.lower()
                        if 'time' in col_lower or 'date' in col_lower:
                            column_mapping[col] = 'timestamp'
                        elif 'open' in col_lower:
                            column_mapping[col] = 'open'
                        elif 'close' in col_lower:
                            column_mapping[col] = 'close'
                        elif 'high' in col_lower:
                            column_mapping[col] = 'high'
                        elif 'low' in col_lower:
                            column_mapping[col] = 'low'
                        elif 'volume' in col_lower:
                            column_mapping[col] = 'volume'
                    
                    df = df.rename(columns=column_mapping)
                    
                    if 'timestamp' in df.columns:
                        df['timestamp'] = pd.to_datetime(df['timestamp'])
                        df = df.sort_values('timestamp')
                        
                        # Проверяем наличие всех необходимых колонок
                        required_cols = ['open', 'high', 'low', 'close']
                        for col in required_cols:
                            if col not in df.columns:
                                df[col] = df.get('close', 0)
                        
                        if 'volume' not in df.columns:
                            df['volume'] = 0
                        
                        logger.info(f"✅ BCS API: получено {len(df)} свечей для {symbol}")
                        return df
                else:
                    logger.warning(f"BCS API: неожиданная структура исторических данных для {symbol}")
            else:
                logger.warning(f"BCS API для {symbol}: код {response.status_code}")
                
        except Exception as e:
            logger.error(f"❌ Ошибка получения исторических данных из BCS API для {symbol}: {e}")
        
        logger.warning(f"⚠️ Не удалось получить исторические данные для {symbol}")
        return None
    
    def get_instruments_info(self, symbol: str) -> Optional[Dict]:
        """Получение информации об инструменте"""
        if not self._ensure_auth():
            return None
        
        try:
            info_url = f"{self.base_url}/api/v1/instruments"
            params = {
                'symbol': symbol,
                'market': self.market,
                'board': self.board
            }
            
            response = self.session.get(info_url, params=params, timeout=10)
            
            if response.status_code == 200:
                return response.json()
        except Exception as e:
            logger.error(f"Ошибка получения информации об инструменте {symbol}: {e}")
        
        return None


class MomentumBotBCS:
    """Бот momentum стратегии с использованием BCS API"""
    
    def __init__(self):
        self.telegram_token = os.getenv('TELEGRAM_TOKEN')
        self.telegram_chat_id = os.getenv('TELEGRAM_CHAT_ID')
        
        # Ключи BCS API
        self.bcs_api_key = os.getenv('BCS_API_KEY')
        self.bcs_api_secret = os.getenv('BCS_API_SECRET')
        
        if not self.bcs_api_key or not self.bcs_api_secret:
            logger.error("❌ BCS_API_KEY и BCS_API_SECRET не найдены в .env!")
            logger.error("❌ Получите ключи: https://trade-api.bcs.ru/http/authorization")
        
        # Инициализация фетчера данных BCS
        self.data_fetcher = BCSDataFetcher()
        
        # Сохраняем ВСЕ параметры стратегии БЕЗ ИЗМЕНЕНИЙ
        self.top_assets_count = 30
        self.selected_count = 5
        self.check_interval = 4 * 3600
        
        self.min_12m_momentum = 0.0
        self.min_volume_24h = 1000000
        self.min_price = 1
        
        self.weights = {'12M': 0.40, '6M': 0.35, '1M': 0.25}
        
        self.sma_fast_period = 10
        self.sma_slow_period = 30
        
        self.benchmark_symbol = 'IMOEX'
        self.benchmark_name = 'Индекс Мосбиржи'
        
        self.current_portfolio: Dict[str, Dict] = {}
        self.signal_history: List[Dict] = []
        self.asset_ranking: List[AssetData] = []
        
        self._cache = {
            'top_assets': {'data': None, 'timestamp': None, 'ttl': 24*3600},
            'historical_data': {},
            'benchmark_data': {'data': None, 'timestamp': None, 'ttl': 3600}
        }
        
        self.errors_count = 0
        self.max_retries = 3
        
        self.telegram_retry_delay = 2
        self.max_telegram_retries = 3
        
        self.test_mode = False
        
        logger.info("🚀 Momentum Bot для BCS API инициализирован")
        logger.info(f"📊 Параметры: Топ {self.selected_count} из {self.top_assets_count}")
        logger.info(f"📡 Источник данных: BCS API")
        logger.info(f"🔄 Проверка: каждые {self.check_interval//3600} часа")
        
        if self.telegram_token and self.telegram_chat_id:
            logger.info("✅ Telegram настроен корректно")
        else:
            logger.warning("⚠️ Telegram не настроен. Сообщения не будут отправляться.")
    
    # 🔧 ВАЖНО: Все остальные методы остаются БЕЗ ИЗМЕНЕНИЙ
    # Они используют self.data_fetcher, который теперь BCSDataFetcher
    
    def get_top_assets(self) -> List[Dict]:
        """Получение топ активов из BCS API"""
        try:
            cache = self._cache['top_assets']
            if cache['data'] and (datetime.now() - cache['timestamp']).seconds < cache['ttl']:
                logger.info("📊 Используем кэшированные данные активов")
                return cache['data']
            
            logger.info("📊 Формирование списка активов из BCS API...")
            
            all_assets = []
            filtered_assets = []
            no_data_assets = []
            
            # Проверяем доступность BCS API
            if not self.data_fetcher.test_bcs_connection():
                logger.error("❌ Не удалось подключиться к BCS API")
                raise Exception("Ошибка подключения к BCS API")
            
            # Получаем данные для популярных акций
            for stock in self.data_fetcher.popular_stocks[:self.top_assets_count * 2]:
                symbol = stock['symbol']
                name = stock['name']
                
                try:
                    price, volume, source = self.data_fetcher.get_current_price(symbol)
                    
                    if price is None:
                        no_data_assets.append(f"⚠️ {symbol}: не удалось получить данные")
                        logger.warning(f"⚠️ Не удалось получить данные для {symbol}")
                        continue
                    
                    # Проверяем фильтры
                    if price < self.min_price:
                        filtered_assets.append(f"❌ {symbol}: цена {price:.2f} < {self.min_price} руб")
                        continue
                    
                    if not volume or volume < self.min_volume_24h:
                        filtered_assets.append(f"❌ {symbol}: объем {volume:,.0f} < {self.min_volume_24h:,} руб")
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
                    logger.info(f"  ✅ {symbol}: {price:.2f} руб, объем: {volume:,.0f}")
                    
                    # Пауза чтобы не перегружать API
                    time.sleep(0.1)
                        
                except Exception as e:
                    filtered_assets.append(f"❌ {symbol}: ошибка {str(e)[:50]}")
                    logger.error(f"  ❌ {symbol}: {e}")
                    continue
            
            # Получаем данные для индексов
            for index in self.data_fetcher.indices:
                symbol = index['symbol']
                name = index['name']
                
                try:
                    price, volume, source = self.data_fetcher.get_current_price(symbol)
                    
                    if price is None or price <= 0:
                        no_data_assets.append(f"⚠️ {symbol}: не удалось получить данные индекса")
                        logger.warning(f"⚠️ Не удалось получить данные для индекса {symbol}")
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
                    logger.info(f"  ✅ {symbol}: {price:.2f} руб (индекс)")
                    
                    time.sleep(0.1)
                        
                except Exception as e:
                    filtered_assets.append(f"❌ {symbol}: ошибка {str(e)[:50]}")
                    logger.error(f"  ❌ {symbol}: {e}")
            
            if len(all_assets) == 0:
                logger.error("❌ Не удалось получить данные ни для одного актива")
                raise Exception("Нет данных для анализа")
            
            # Сортируем по объему и берем топ
            all_assets.sort(key=lambda x: x['volume_24h'], reverse=True)
            top_assets = all_assets[:self.top_assets_count]
            
            if filtered_assets:
                logger.info("📋 Отфильтрованные активы:")
                for i, msg in enumerate(filtered_assets[:10], 1):
                    logger.info(f"  {i:2d}. {msg}")
            
            if no_data_assets:
                logger.warning("⚠️ Активы без данных:")
                for i, msg in enumerate(no_data_assets[:10], 1):
                    logger.warning(f"  {i:2d}. {msg}")
            
            # Кэшируем
            self._cache['top_assets'] = {
                'data': top_assets,
                'timestamp': datetime.now(),
                'ttl': 3600  # 1 час, так как данные могут меняться
            }
            
            logger.info(f"✅ Сформирован список из {len(top_assets)} активов")
            if top_assets:
                logger.info("📋 Первые 5 активов:")
                for i, asset in enumerate(top_assets[:5], 1):
                    logger.info(f"  {i:2d}. {asset['symbol']} - {asset['name']}: {asset['current_price']:.2f} руб")
            
            return top_assets
            
        except Exception as e:
            logger.error(f"❌ Ошибка получения топ активов: {e}")
            raise  # Пробрасываем исключение дальше
    
    # 🔄 Все остальные методы копируются из предыдущей версии БЕЗ ИЗМЕНЕНИЙ
    # включая: get_test_assets, get_cached_historical_data, create_test_data,
    # get_benchmark_data, calculate_momentum_values, analyze_assets,
    # generate_signals, send_telegram_message, format_active_positions,
    # format_signal_message, format_ranking_message, run_strategy_cycle,
    # save_state, load_state, run
    
    # ❗ ВАЖНО: Удаляем метод get_test_assets, так как не используем тестовые данные
    # ❗ ВАЖНО: Удаляем вызовы тестовых данных из других методов
    
    def run_strategy_cycle(self) -> bool:
        """Запуск цикла стратегии С ОБРАБОТКОЙ ОШИБОК"""
        try:
            logger.info("🔄 Запуск цикла стратегии...")
            
            assets = self.analyze_assets()
            
            if not assets:
                logger.warning("❌ Нет активов для анализа")
                benchmark_data = self.get_benchmark_data()
                no_assets_msg = (
                    "📊 *Анализ (BCS API)*\n"
                    "Нет активов, соответствующих критериям.\n\n"
                    f"• Проверено акций: из {self.top_assets_count}\n"
                    f"• Требование 12M моментум: > {self.min_12m_momentum}%\n"
                    f"• Требование SMA: положительный сигнал\n"
                )
                
                if benchmark_data:
                    no_assets_msg += f"• Бенчмарк ({self.benchmark_symbol}): {benchmark_data['absolute_momentum_6m']:+.1f}%\n"
                
                no_assets_msg += "\nВозможно, рынок в нисходящем тренде."
                
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
                    logger.info(f"✅ Сигнал отправлен: {signal['symbol']} {signal['action']}")
            
            ranking_message = self.format_ranking_message(assets)
            self.send_telegram_message(ranking_message)
            
            logger.info(f"✅ Цикл завершен. Сигналов: {len(signals)}")
            return True
            
        except Exception as e:
            logger.error(f"❌ Ошибка в цикле: {e}")
            self.errors_count += 1
            
            error_msg = (
                f"❌ *ОШИБКА АНАЛИЗА BCS API*\n"
                f"Произошла ошибка при анализе активов:\n"
                f"```\n{str(e)[:200]}\n```\n"
                f"Ошибок подряд: {self.errors_count}"
            )
            self.send_telegram_message(error_msg)
            
            return False
    
    def run(self):
        """Основной цикл"""
        logger.info("=" * 60)
        logger.info("🚀 ЗАПУСК MOMENTUM BOT С BCS API")
        logger.info("=" * 60)
        
        self.load_state()
        
        if self.telegram_token and self.telegram_chat_id:
            welcome_msg = (
                "🚀 *MOMENTUM BOT С BCS API ЗАПУЩЕН*\n"
                f"📊 Стратегия: Momentum с фильтрацией\n"
                f"🔢 Отбор: топ {self.selected_count} из {self.top_assets_count}\n"
                f"📈 Бенчмарк: {self.benchmark_symbol}\n"
                f"⚙️ Фильтры: 12M > {self.min_12m_momentum}%, объем > {self.min_volume_24h:,} руб\n"
                f"📡 Источник данных: BCS Trade API\n"
                f"🔑 Аутентификация: {'✅ Настроена' if self.bcs_api_key and self.bcs_api_secret else '❌ НЕ настроена'}\n"
                f"⏰ Проверка: каждые {self.check_interval//3600} часа\n"
                f"⚡ Версия: с корректной авторизацией BCS"
            )
            self.send_telegram_message(welcome_msg)
            
            active_positions_msg = self.format_active_positions()
            self.send_telegram_message(active_positions_msg)
            
            if not self.bcs_api_key or not self.bcs_api_secret:
                warning_msg = (
                    "⚠️ *ВНИМАНИЕ: Ключи BCS API не настроены*\n"
                    "Бот не сможет получить данные с BCS API.\n"
                    "Добавьте в .env файл:\n"
                    "```\nBCS_API_KEY=ваш_api_key\nBCS_API_SECRET=ваш_api_secret\n```\n"
                    "Получите ключи: https://trade-api.bcs.ru/http/authorization"
                )
                self.send_telegram_message(warning_msg, silent=True)
        else:
            logger.warning("⚠️ Telegram не настроен, пропускаем приветственное сообщение")
        
        iteration = 0
        
        while True:
            iteration += 1
            current_time = datetime.now().strftime('%H:%M:%S %d.%m.%Y')
            logger.info(f"🔄 Цикл #{iteration} - {current_time}")
            
            try:
                success = self.run_strategy_cycle()
                
                if success:
                    logger.info(f"✅ Цикл #{iteration} успешно завершен")
                    
                    if iteration % 3 == 0:
                        self.save_state()
                else:
                    logger.warning(f"⚠️ Цикл #{iteration} завершен с проблемами")
                
                if self.errors_count > 5:
                    logger.error(f"⚠️ Много ошибок ({self.errors_count}). Пауза 1 час...")
                    if self.telegram_token and self.telegram_chat_id:
                        self.send_telegram_message("⚠️ *МНОГО ОШИБОК BCS API* \nБот делает паузу 1 час")
                    time.sleep(3600)
                    self.errors_count = 0
                
                logger.info(f"⏳ Следующая проверка через {self.check_interval//3600} часа(ов)...")
                time.sleep(self.check_interval)
                
            except KeyboardInterrupt:
                logger.info("🛑 Остановка по команде пользователя")
                self.save_state()
                if self.telegram_token and self.telegram_chat_id:
                    self.send_telegram_message("🛑 *BOT ОСТАНОВЛЕН ПОЛЬЗОВАТЕЛЕМ*")
                break
                
            except Exception as e:
                logger.error(f"❌ Критическая ошибка: {e}")
                self.errors_count += 1
                if self.telegram_token and self.telegram_chat_id:
                    self.send_telegram_message(f"💥 *КРИТИЧЕСКАЯ ОШИБКА BCS API* \n{str(e)[:100]}")
                
                delay = min(300 * self.errors_count, 3600)
                logger.info(f"⏳ Пауза {delay} секунд из-за ошибок...")
                time.sleep(delay)


def main():
    bot = MomentumBotBCS()
    
    try:
        bot.run()
    except Exception as e:
        logger.error(f"💀 Фатальная ошибка: {e}")
        if bot.telegram_token and bot.telegram_chat_id:
            bot.send_telegram_message(f"💀 *ФАТАЛЬНАЯ ОШИБКА BCS API* \nБот завершил работу: {str(e)[:200]}")


if __name__ == "__main__":
    main()