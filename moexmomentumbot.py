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

# Импорты для Московской биржи
try:
    from FinLabPy import FinLab
    HAS_FINLAB = True
except ImportError:
    print("⚠️ FinLabPy не установлен. Запускаю в упрощенном режиме...")
    HAS_FINLAB = False

try:
    from TinvestPy import TInvest
    HAS_TINVEST = True
except ImportError:
    print("⚠️ TinvestPy не установлен. Установите: pip install git+https://github.com/cia76/TinvestPy.git")
    HAS_TINVEST = False

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
    source: str = 'moex'  # 'moex', 'tinvest', 'test'


class MOEXDataFetcher:
    """Класс для получения данных с Московской биржи разными способами"""
    
    def __init__(self):
        self.finlab_client = None
        self.tinvest_client = None
        
        # Инициализация клиентов
        if HAS_FINLAB:
            try:
                self.finlab_client = FinLab()
                logger.info("✅ FinLabPy инициализирован")
            except Exception as e:
                logger.warning(f"⚠️ Ошибка инициализации FinLabPy: {e}")
        
        if HAS_TINVEST:
            tinvest_token = os.getenv('TINVEST_TOKEN')
            if tinvest_token:
                try:
                    self.tinvest_client = TInvest(token=tinvest_token)
                    logger.info("✅ Tinkoff Invest API инициализирован")
                except Exception as e:
                    logger.warning(f"⚠️ Ошибка инициализации Tinvest: {e}")
            else:
                logger.info("ℹ️ Tinvest токен не указан, используем только MOEX API")
        
        # Список популярных акций Мосбиржи
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
        
        # Индексы
        self.indices = [
            {'symbol': 'IMOEX', 'name': 'Индекс Мосбиржи', 'sector': 'Индекс'},
            {'symbol': 'IMOEXF', 'name': 'Индекс Мосбиржи полной доходности', 'sector': 'Индекс'},
            {'symbol': 'RTSI', 'name': 'Индекс РТС', 'sector': 'Индекс'},
        ]
    
    def get_stock_list(self) -> List[Dict]:
        """Получение списка акций"""
        stocks = []
        
        # Пробуем FinLab
        if self.finlab_client:
            try:
                finlab_stocks = self.finlab_client.get_stocks()
                for stock in finlab_stocks:
                    stocks.append({
                        'symbol': stock.get('ticker', ''),
                        'name': stock.get('name', ''),
                        'sector': stock.get('sector', ''),
                        'source': 'finlab'
                    })
                logger.info(f"✅ Получено {len(stocks)} акций через FinLab")
            except Exception as e:
                logger.warning(f"⚠️ Ошибка получения акций через FinLab: {e}")
        
        # Если FinLab не сработал, пробуем Tinvest
        if not stocks and self.tinvest_client:
            try:
                tinvest_stocks = self.tinvest_client.get_stocks()
                for stock in tinvest_stocks:
                    if stock.get('currency') == 'rub' and stock.get('type') == 'Stock':
                        stocks.append({
                            'symbol': stock.get('ticker', ''),
                            'name': stock.get('name', ''),
                            'sector': stock.get('sector', ''),
                            'figi': stock.get('figi', ''),
                            'source': 'tinvest'
                        })
                logger.info(f"✅ Получено {len(stocks)} акций через Tinvest")
            except Exception as e:
                logger.warning(f"⚠️ Ошибка получения акций через Tinvest: {e}")
        
        # Если ничего не получилось, используем список по умолчанию
        if not stocks:
            for stock in self.popular_stocks:
                stocks.append({
                    'symbol': stock['symbol'],
                    'name': stock['name'],
                    'sector': stock['sector'],
                    'source': 'default'
                })
            logger.info(f"ℹ️ Используем список акций по умолчанию: {len(stocks)} акций")
        
        return stocks
    
    def get_current_price(self, symbol: str) -> Tuple[Optional[float], Optional[float], str]:
        """Получение текущей цены и объема"""
        source = 'unknown'
        
        # Пробуем MOEX API напрямую
        try:
            url = f"https://iss.moex.com/iss/engines/stock/markets/shares/securities/{symbol}.json"
            response = requests.get(url, timeout=10)
            
            if response.status_code == 200:
                data = response.json()
                marketdata = data.get('marketdata', {}).get('data', [])
                
                if marketdata:
                    # LAST - индекс 12, VOLUME - индекс 11
                    market_data = marketdata[0]
                    if len(market_data) > 12:
                        price = market_data[12]  # LAST
                        volume = market_data[11]  # VOLUME
                        if price and price > 0:
                            source = 'moex_api'
                            return price, volume, source
        except Exception as e:
            logger.debug(f"MOEX API error for {symbol}: {e}")
        
        # Пробуем FinLab
        if self.finlab_client:
            try:
                data = self.finlab_client.get_current_data(symbol)
                if data and data.get('last'):
                    source = 'finlab'
                    return data.get('last'), data.get('volume', 0), source
            except:
                pass
        
        # Пробуем Tinvest
        if self.tinvest_client:
            try:
                # Ищем FIGI
                stocks = self.tinvest_client.get_stocks()
                figi = None
                for stock in stocks:
                    if stock.get('ticker') == symbol:
                        figi = stock.get('figi')
                        break
                
                if figi:
                    candles = self.tinvest_client.get_candles(
                        figi=figi,
                        interval='day',
                        days=1
                    )
                    
                    if candles:
                        last_candle = candles[-1]
                        source = 'tinvest'
                        return last_candle.get('close'), last_candle.get('volume', 0), source
            except:
                pass
        
        return None, None, source
    
    def get_historical_data(self, symbol: str, days: int = 365) -> Optional[pd.DataFrame]:
        """Получение исторических данных"""
        # Пробуем MOEX API
        try:
            end_date = datetime.now()
            start_date = end_date - timedelta(days=days)
            
            url = f"https://iss.moex.com/iss/engines/stock/markets/shares/securities/{symbol}/candles.json"
            params = {
                'from': start_date.strftime('%Y-%m-%d'),
                'till': end_date.strftime('%Y-%m-%d'),
                'interval': 24,  # Дневные данные
                'candles.columns': 'open,close,high,low,value,volume,end'
            }
            
            response = requests.get(url, params=params, timeout=30)
            
            if response.status_code == 200:
                data = response.json()
                candles = data.get('candles', {}).get('data', [])
                
                if candles:
                    df = pd.DataFrame(candles, columns=['open', 'close', 'high', 'low', 'value', 'volume', 'timestamp'])
                    df['timestamp'] = pd.to_datetime(df['timestamp'])
                    df = df.sort_values('timestamp')
                    logger.debug(f"✅ Получено {len(df)} свечей для {symbol} через MOEX API")
                    return df
        except Exception as e:
            logger.debug(f"MOEX API исторические данные для {symbol}: {e}")
        
        # Пробуем FinLab
        if self.finlab_client:
            try:
                data = self.finlab_client.get_candles(
                    ticker=symbol,
                    interval='D',
                    start_date=start_date.strftime('%Y-%m-%d'),
                    end_date=end_date.strftime('%Y-%m-%d')
                )
                
                if data and len(data) > 0:
                    df = pd.DataFrame(data)
                    df['date'] = pd.to_datetime(df['date'])
                    df = df.rename(columns={'date': 'timestamp'})
                    df = df.sort_values('timestamp')
                    logger.debug(f"✅ Получено {len(df)} свечей для {symbol} через FinLab")
                    return df
            except Exception as e:
                logger.debug(f"FinLab исторические данные для {symbol}: {e}")
        
        # Пробуем Tinvest
        if self.tinvest_client:
            try:
                stocks = self.tinvest_client.get_stocks()
                figi = None
                for stock in stocks:
                    if stock.get('ticker') == symbol:
                        figi = stock.get('figi')
                        break
                
                if figi:
                    candles = self.tinvest_client.get_candles(
                        figi=figi,
                        interval='day',
                        days=days
                    )
                    
                    if candles:
                        df = pd.DataFrame(candles)
                        df['time'] = pd.to_datetime(df['time'])
                        df = df.rename(columns={
                            'time': 'timestamp',
                            'o': 'open',
                            'c': 'close',
                            'h': 'high',
                            'l': 'low',
                            'v': 'volume'
                        })
                        df = df.sort_values('timestamp')
                        logger.debug(f"✅ Получено {len(df)} свечей для {symbol} через Tinvest")
                        return df
            except Exception as e:
                logger.debug(f"Tinvest исторические данные для {symbol}: {e}")
        
        logger.warning(f"⚠️ Не удалось получить исторические данные для {symbol}")
        return None


class MomentumBotMOEX:
    """Бот momentum стратегии для Московской биржи"""
    
    def __init__(self):
        self.telegram_token = os.getenv('TELEGRAM_BOT_TOKEN')
        self.telegram_chat_id = os.getenv('TELEGRAM_CHAT_ID')
        
        # Инициализация фетчера данных
        self.data_fetcher = MOEXDataFetcher()
        
        # Параметры стратегии
        self.top_assets_count = 50
        self.selected_count = 8
        self.check_interval = 86400  # Раз в день
        
        # Критерии фильтрации
        self.min_12m_momentum = 5.0
        self.min_volume_24h = 10000000  # 10 млн руб
        self.min_price = 10  # руб
        
        # Веса для моментума
        self.weights = {'12M': 0.40, '6M': 0.35, '1M': 0.25}
        
        # Параметры SMA
        self.sma_fast_period = 20
        self.sma_slow_period = 50
        
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
        
        logger.info("🚀 Momentum Bot для Московской биржи инициализирован")
        logger.info(f"📊 Параметры: Топ {self.selected_count} из {self.top_assets_count}")
        logger.info(f"⚙️ Фильтры: 12M > {self.min_12m_momentum}%, Объем > {self.min_volume_24h:,} руб")
        logger.info(f"📈 Источники данных: {'FinLab, ' if self.data_fetcher.finlab_client else ''}{'Tinvest, ' if self.data_fetcher.tinvest_client else ''}MOEX API")
    
    def get_top_assets(self) -> List[Dict]:
        """Получение топ активов"""
        try:
            cache = self._cache['top_assets']
            if cache['data'] and (datetime.now() - cache['timestamp']).seconds < cache['ttl']:
                return cache['data']
            
            logger.info("📊 Формирование списка активов...")
            
            all_assets = []
            
            # Получаем список акций
            stocks = self.data_fetcher.get_stock_list()
            
            # Добавляем индексы
            for index in self.data_fetcher.indices:
                stocks.append({
                    'symbol': index['symbol'],
                    'name': index['name'],
                    'sector': index['sector'],
                    'source': 'index'
                })
            
            # Получаем текущие данные для каждого актива
            for i, stock in enumerate(stocks[:self.top_assets_count * 2]):  # Берем больше, потом отфильтруем
                symbol = stock['symbol']
                name = stock['name']
                
                try:
                    price, volume, source = self.data_fetcher.get_current_price(symbol)
                    
                    if price and price >= self.min_price and volume and volume >= self.min_volume_24h:
                        all_assets.append({
                            'symbol': symbol,
                            'name': name,
                            'sector': stock.get('sector', ''),
                            'current_price': price,
                            'volume_24h': volume,
                            'source': source,
                            'market_type': 'index' if 'IMOEX' in symbol or 'RTSI' in symbol else 'stock'
                        })
                        logger.debug(f"  ✅ {symbol}: {price:.2f} руб, объем: {volume:,.0f}")
                    
                    # Пауза чтобы не перегружать API
                    if i % 10 == 0:
                        time.sleep(0.5)
                        
                except Exception as e:
                    logger.debug(f"  ❌ {symbol}: {e}")
                    continue
            
            # Сортируем по объему и берем топ
            all_assets.sort(key=lambda x: x['volume_24h'], reverse=True)
            top_assets = all_assets[:self.top_assets_count]
            
            # Кэшируем
            self._cache['top_assets'] = {
                'data': top_assets,
                'timestamp': datetime.now(),
                'ttl': 24*3600
            }
            
            logger.info(f"✅ Сформирован список из {len(top_assets)} активов")
            logger.info("📋 Топ-10 по объему:")
            for i, asset in enumerate(top_assets[:10], 1):
                logger.info(f"  {i:2d}. {asset['symbol']} - {asset['name']} ({asset['sector']}): {asset['current_price']:.2f} руб, объем: {asset['volume_24h']:,.0f}")
            
            return top_assets
            
        except Exception as e:
            logger.error(f"❌ Ошибка получения топ активов: {e}")
            return []
    
    @lru_cache(maxsize=100)
    def get_cached_historical_data(self, symbol: str, days: int = 400) -> Optional[pd.DataFrame]:
        """Получение исторических данных с кэшированием"""
        cache_key = f"{symbol}_{days}"
        
        if cache_key in self._cache['historical_data']:
            cache_data = self._cache['historical_data'][cache_key]
            if (datetime.now() - cache_data['timestamp']).seconds < cache_data['ttl']:
                return cache_data['data']
        
        df = self.data_fetcher.get_historical_data(symbol, days)
        
        if df is not None:
            self._cache['historical_data'][cache_key] = {
                'data': df,
                'timestamp': datetime.now(),
                'ttl': 3600
            }
        
        return df
    
    def get_benchmark_data(self) -> Optional[Dict[str, float]]:
        """Получение данных бенчмарка"""
        try:
            cache = self._cache['benchmark_data']
            if cache['data'] and (datetime.now() - cache['timestamp']).seconds < cache['ttl']:
                return cache['data']
            
            logger.info(f"📊 Получение данных бенчмарка {self.benchmark_symbol}...")
            
            df = self.get_cached_historical_data(self.benchmark_symbol, 400)
            if df is None or len(df) < 126:
                logger.error(f"❌ Недостаточно данных бенчмарка: {len(df) if df else 0} дней")
                return None
            
            current_price = df['close'].iloc[-1]
            
            # 6-месячный моментум (~126 торговых дней)
            if len(df) >= 126:
                price_6m_ago = df['close'].iloc[-126]
            else:
                price_6m_ago = df['close'].iloc[0]
            
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
            
            logger.info(f"✅ Данные бенчмарка: 6M моментум = {benchmark_absolute_momentum_6m:.2f}%")
            
            return benchmark_data
            
        except Exception as e:
            logger.error(f"❌ Ошибка получения данных бенчмарка: {e}")
            return None
    
    def calculate_momentum_values(self, asset_info: Dict) -> Optional[AssetData]:
        """Расчет значений моментума"""
        try:
            symbol = asset_info['symbol']
            name = asset_info['name']
            source = asset_info.get('source', 'unknown')
            
            logger.info(f"📈 Расчет моментума для {symbol} ({name})...")
            
            df = self.get_cached_historical_data(symbol, 400)
            if df is None:
                logger.info(f"❌ Нет исторических данных для {symbol}")
                return None
            
            # Нужно минимум 252 торговых дня (примерно 1 год)
            if len(df) < 252:
                logger.warning(f"⚠️ Мало данных для {symbol}: {len(df)} дней (< 252)")
                # Можно продолжить, но качество анализа будет ниже
            
            n = len(df)
            current_price = df['close'].iloc[-1]
            
            # Используем торговые дни: 1 неделя = 5 дней, 1 месяц = 21 день, 6 месяцев = 126 дней, 1 год = 252 дня
            price_1w_ago = df['close'].iloc[-5] if n >= 5 else current_price
            price_1m_ago = df['close'].iloc[-21] if n >= 21 else current_price
            price_6m_ago = df['close'].iloc[-126] if n >= 126 else current_price
            price_12m_ago = df['close'].iloc[-252] if n >= 252 else current_price
            
            # Расчет моментумов
            momentum_1m = ((price_1w_ago - price_1m_ago) / price_1m_ago) * 100
            momentum_6m = ((price_1m_ago - price_6m_ago) / price_6m_ago) * 100
            momentum_12m = ((price_1m_ago - price_12m_ago) / price_12m_ago) * 100
            absolute_momentum = ((current_price - price_12m_ago) / price_12m_ago) * 100
            absolute_momentum_6m = ((current_price - price_6m_ago) / price_6m_ago) * 100
            
            # Комбинированный моментум
            combined_momentum = (
                momentum_12m * self.weights['12M'] +
                momentum_6m * self.weights['6M'] +
                momentum_1m * self.weights['1M']
            )
            
            # SMA
            sma_fast = df['close'].tail(self.sma_fast_period).mean()
            sma_slow = df['close'].tail(self.sma_slow_period).mean()
            sma_signal = sma_fast > sma_slow
            
            volume_24h = asset_info.get('volume_24h', 0)
            sector = asset_info.get('sector', '')
            market_type = asset_info.get('market_type', 'stock')
            
            logger.debug(f"  {symbol}: Цена {current_price:.2f}, 12M: {momentum_12m:+.1f}%, 6M: {absolute_momentum_6m:+.1f}%, SMA: {'🟢' if sma_signal else '🔴'}")
            
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
        """Анализ активов"""
        top_assets = self.get_top_assets()
        if not top_assets:
            logger.warning("❌ Нет активов для анализа")
            return []
        
        logger.info(f"📊 Анализ {len(top_assets)} активов...")
        
        analyzed_assets = []
        benchmark_data = self.get_benchmark_data()
        
        for i, asset_info in enumerate(top_assets):
            symbol = asset_info['symbol']
            
            try:
                asset_data = self.calculate_momentum_values(asset_info)
                if asset_data is None:
                    continue
                
                # Фильтр 1: Минимальный 12M моментум
                if asset_data.momentum_12m < self.min_12m_momentum:
                    logger.debug(f"  ❌ {symbol}: низкий 12M моментум ({asset_data.momentum_12m:+.1f}%)")
                    continue
                
                # Фильтр 2: Положительный SMA сигнал
                if not asset_data.sma_signal:
                    logger.debug(f"  ❌ {symbol}: отрицательный SMA сигнал")
                    continue
                
                # Фильтр 3: Сравнение с бенчмарком (если есть данные)
                if benchmark_data:
                    if asset_data.absolute_momentum_6m <= benchmark_data['absolute_momentum_6m']:
                        logger.debug(f"  ❌ {symbol}: 6M моментум ({asset_data.absolute_momentum_6m:+.1f}%) <= бенчмарку ({benchmark_data['absolute_momentum_6m']:+.1f}%)")
                        continue
                
                analyzed_assets.append(asset_data)
                logger.debug(f"  ✅ {symbol}: добавлен в анализ")
                
            except Exception as e:
                logger.error(f"Ошибка анализа {symbol}: {e}")
                continue
        
        # Сортируем по комбинированному моментуму
        analyzed_assets.sort(key=lambda x: x.combined_momentum, reverse=True)
        selected_assets = analyzed_assets[:self.selected_count]
        
        # Логируем результаты
        logger.info("=" * 60)
        logger.info(f"📊 ИТОГ анализа: {len(selected_assets)} активов отобрано")
        
        if benchmark_data:
            logger.info(f"📈 Бенчмарк {self.benchmark_symbol}: 6M моментум = {benchmark_data['absolute_momentum_6m']:+.1f}%")
        
        if selected_assets:
            logger.info("🏆 Топ активов:")
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
            
            # BUY сигнал
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
                logger.info(f"📈 BUY для {symbol} ({asset.name})")
            
            # SELL сигнал
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
                logger.info(f"📉 SELL для {symbol}: {profit_percent:+.2f}%")
        
        return signals
    
    def send_telegram_message(self, message: str) -> bool:
        """Отправка сообщения в Telegram"""
        if not self.telegram_token or not self.telegram_chat_id:
            logger.warning("⚠️ Нет данных для Telegram")
            return False
        
        for attempt in range(self.max_telegram_retries):
            try:
                url = f"https://api.telegram.org/bot{self.telegram_token}/sendMessage"
                data = {
                    "chat_id": self.telegram_chat_id,
                    "text": message,
                    "parse_mode": "Markdown",
                    "disable_web_page_preview": True
                }
                
                response = requests.post(url, data=data, timeout=10)
                
                if response.status_code == 200:
                    return True
                else:
                    logger.warning(f"Ошибка Telegram: {response.status_code}")
                    
            except Exception as e:
                logger.warning(f"Ошибка отправки Telegram: {e}")
            
            if attempt < self.max_telegram_retries - 1:
                time.sleep(self.telegram_retry_delay)
        
        return False
    
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
        """Форматирование рейтинга"""
        benchmark_data = self.get_benchmark_data()
        
        message = f"📊 *MOMENTUM РЕЙТИНГ МОСБИРЖИ*\n"
        message += f"Отбор: {len(assets)} из {self.top_assets_count} активов\n"
        
        if benchmark_data:
            message += f"📈 Бенчмарк ({self.benchmark_symbol}): {benchmark_data['absolute_momentum_6m']:+.1f}% (6M)\n"
        
        message += "═══════════════════════════\n"
        
        for i, asset in enumerate(assets, 1):
            status = "🟢 IN" if self.current_portfolio.get(asset.symbol, {}).get('status') == 'IN' else "⚪ OUT"
            
            # Сравнение с бенчмарком
            benchmark_comparison = ""
            if benchmark_data:
                vs_benchmark = asset.absolute_momentum_6m - benchmark_data['absolute_momentum_6m']
                benchmark_comparison = f"\n📈 vs бенчмарк: {vs_benchmark:+.1f}%"
            
            message += (
                f"#{i} {asset.symbol} ({asset.name}) {status}\n"
                f"🏢 {asset.sector}\n"
                f"💰 {asset.current_price:.2f} руб\n"
                f"📊 Моментум:\n"
                f"  • 12M: **{asset.momentum_12m:+.1f}%**\n"
                f"  • 6M: {asset.absolute_momentum_6m:+.1f}%{benchmark_comparison}\n"
                f"  • Комбинированный: **{asset.combined_momentum:+.1f}%**\n"
                f"📉 SMA: {'🟢 Растущий' if asset.sma_signal else '🔴 Падающий'}\n"
                f"📡 Источник: {asset.source}\n"
                f"──\n"
            )
        
        message += "═══════════════════════════\n"
        message += "*ПАРАМЕТРЫ СТРАТЕГИИ:*\n"
        message += f"• Отбор: топ {self.selected_count} из {self.top_assets_count}\n"
        message += f"• Минимальный 12M моментум: {self.min_12m_momentum}%\n"
        message += f"• Минимальный объем: {self.min_volume_24h:,} руб\n"
        message += f"• Минимальная цена: {self.min_price} руб\n"
        message += f"• Бенчмарк: {self.benchmark_symbol}\n"
        message += f"• SMA: {self.sma_fast_period}/{self.sma_slow_period} дней\n"
        message += f"• Веса: 12M({self.weights['12M']*100:.0f}%), 6M({self.weights['6M']*100:.0f}%), 1M({self.weights['1M']*100:.0f}%)\n"
        
        active_count = sum(1 for v in self.current_portfolio.values() if v.get('status') == 'IN')
        if active_count > 0:
            message += f"• Активных позиций: {active_count}/{self.selected_count}\n"
        
        return message
    
    def run_strategy_cycle(self) -> bool:
        """Запуск цикла стратегии"""
        try:
            logger.info("🔄 Запуск цикла стратегии...")
            
            assets = self.analyze_assets()
            if not assets:
                logger.warning("❌ Нет активов для анализа")
                return False
            
            self.asset_ranking = assets
            
            signals = self.generate_signals(assets)
            
            # Отправляем сигналы
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
            return False
    
    def save_state(self):
        """Сохранение состояния"""
        try:
            state = {
                'current_portfolio': self.current_portfolio,
                'signal_history': self.signal_history[-100:],
                'last_update': datetime.now().isoformat(),
                'errors_count': self.errors_count,
                'version': 'moex_bot_v2'
            }
            
            with open('logs/bot_state_moex.json', 'w') as f:
                json.dump(state, f, default=str, indent=2)
            
            logger.info("💾 Состояние сохранено")
        except Exception as e:
            logger.error(f"Ошибка сохранения: {e}")
    
    def load_state(self):
        """Загрузка состояния"""
        try:
            if os.path.exists('logs/bot_state_moex.json'):
                with open('logs/bot_state_moex.json', 'r') as f:
                    state = json.load(f)
                
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
        except Exception as e:
            logger.error(f"Ошибка загрузки: {e}")
    
    def cleanup_cache(self):
        """Очистка кэша"""
        current_time = datetime.now()
        for cache_type in ['historical_data']:
            to_delete = []
            for key, data in self._cache.get(cache_type, {}).items():
                if (current_time - data['timestamp']).seconds > data['ttl']:
                    to_delete.append(key)
            
            for key in to_delete:
                del self._cache[cache_type][key]
        
        logger.debug("🧹 Очищен кэш")
    
    def run(self):
        """Основной цикл"""
        logger.info("=" * 60)
        logger.info("🚀 ЗАПУСК MOMENTUM BOT ДЛЯ МОСБИРЖИ")
        logger.info("=" * 60)
        
        self.load_state()
        
        # Приветственное сообщение
        welcome_msg = (
            "🚀 *MOMENTUM BOT ДЛЯ МОСБИРЖИ ЗАПУЩЕН*\n"
            f"📊 Стратегия: Momentum с фильтрацией\n"
            f"🔢 Отбор: топ {self.selected_count} из {self.top_assets_count}\n"
            f"📈 Бенчмарк: {self.benchmark_symbol}\n"
            f"⚙️ Фильтры: 12M > {self.min_12m_momentum}%, объем > {self.min_volume_24h:,} руб\n"
            f"📡 Источники данных: "
            f"{'FinLabPy, ' if self.data_fetcher.finlab_client else ''}"
            f"{'Tinvest, ' if self.data_fetcher.tinvest_client else ''}"
            f"MOEX API\n"
            f"⏰ Проверка: раз в день"
        )
        self.send_telegram_message(welcome_msg)
        
        iteration = 0
        
        while True:
            iteration += 1
            current_time = datetime.now().strftime('%H:%M:%S %d.%m.%Y')
            logger.info(f"🔄 Цикл #{iteration} - {current_time}")
            
            try:
                self.cleanup_cache()
                
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
                    logger.error(f"⚠️ Много ошибок ({self.errors_count}). Пауза 3 часа...")
                    self.send_telegram_message("⚠️ *МНОГО ОШИБОК* \nБот делает паузу 3 часа")
                    time.sleep(3 * 3600)
                    self.errors_count = 0
                
                logger.info(f"⏳ Следующая проверка через {self.check_interval//3600} часов...")
                time.sleep(self.check_interval)
                
            except KeyboardInterrupt:
                logger.info("🛑 Остановка по команде пользователя")
                self.save_state()
                self.send_telegram_message("🛑 *BOT ОСТАНОВЛЕН ПОЛЬЗОВАТЕЛЕМ*")
                break
                
            except Exception as e:
                logger.error(f"❌ Критическая ошибка: {e}")
                self.errors_count += 1
                self.send_telegram_message(f"💥 *КРИТИЧЕСКАЯ ОШИБКА* \n{str(e)[:100]}")
                
                delay = min(300 * self.errors_count, 3600)
                logger.info(f"⏳ Пауза {delay} секунд из-за ошибок...")
                time.sleep(delay)


def main():
    bot = MomentumBotMOEX()
    
    try:
        bot.run()
    except Exception as e:
        logger.error(f"💀 Фатальная ошибка: {e}")
        bot.send_telegram_message(f"💀 *ФАТАЛЬНАЯ ОШИБКА* \nБот завершил работу: {str(e)[:200]}")


if __name__ == "__main__":
    main()