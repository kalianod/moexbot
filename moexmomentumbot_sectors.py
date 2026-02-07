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
from typing import Dict, List, Optional, Tuple, Any, Set
from dataclasses import dataclass, field
from functools import lru_cache
from collections import defaultdict
import traceback

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
    atr: float = 0.0
    stop_loss: float = 0.0
    atr_period: int = 14
    timestamp: datetime = field(default_factory=datetime.now)
    market_type: str = 'stock'
    sector: str = ''
    currency: str = 'rub'
    source: str = 'moex'


@dataclass
class SectorPerformance:
    """Класс для хранения эффективности сектора"""
    sector_name: str
    description: str = ''
    priority: int = 0
    top_n: int = 3
    total_stocks: int = 0
    analyzed_stocks: int = 0
    passed_filters: int = 0
    selected_stocks: List[AssetData] = field(default_factory=list)
    avg_combined_momentum: float = 0.0
    avg_absolute_momentum_6m: float = 0.0
    avg_momentum_12m: float = 0.0
    vs_benchmark: float = 0.0
    performance_score: float = 0.0
    avg_atr_percent: float = 0.0


class MOEXDataFetcher:
    """Класс для получения данных с Московской биржи С ИСПОЛЬЗОВАНИЕМ apimoex"""
    
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({'User-Agent': 'MomentumBotMOEX/1.0'})
        
        self.stocks_cache_file = 'logs/moex_stocks_cache.json'
        self.stocks_cache_ttl = 30 * 24 * 3600  # Увеличен с 180 до 30 дней
        
        self.benchmark_symbol = 'MCFTR'
        
        self.sectors_config = self.load_sectors_config()
        
        self.request_delay = 0.5  # Задержка между запросами API
        self.max_retries = 3  # Максимальное количество повторных попыток
        
        logger.info(f"✅ MOEXDataFetcher инициализирован. apimoex доступен: {HAS_APIMOEX}")
        
        self.test_moex_connection()
    
    def load_sectors_config(self) -> Dict:
        """Загрузка конфигурации секторов из файла"""
        config_file = 'sectors_config.json'
        try:
            if os.path.exists(config_file):
                with open(config_file, 'r', encoding='utf-8') as f:
                    config = json.load(f)
                logger.info(f"✅ Конфигурация секторов загружена из {config_file}")
                logger.info(f"📊 Загружено секторов: {len(config.get('sectors', {}))}")
                
                for sector_name, sector_data in config.get('sectors', {}).items():
                    stocks_count = len(sector_data.get('stocks', []))
                    logger.info(f"  • {sector_name}: {stocks_count} акций")
                    
                return config
            else:
                logger.error(f"❌ Файл конфигурации {config_file} не найден")
                return {'sectors': {}, 'default_sector': 'Другое'}
        except Exception as e:
            logger.error(f"❌ Ошибка загрузки конфигурации секторов: {e}")
            return {'sectors': {}, 'default_sector': 'Другое'}
    
    def get_assets_from_config(self) -> List[Dict]:
        """
        Получение списка акций ТОЛЬКО из конфигурационного файла
        """
        logger.info("📊 Получение списка акций из конфигурационного файла...")
        
        assets = []
        total_stocks = 0
        
        for sector_name, sector_data in self.sectors_config.get('sectors', {}).items():
            stocks_list = sector_data.get('stocks', [])
            for stock in stocks_list:
                ticker = stock.get('Ticker', '').upper()
                name = stock.get('Name', ticker)
                
                assets.append({
                    'symbol': ticker,
                    'name': name,
                    'sector': sector_name,
                    'sector_data': sector_data,
                    'source': 'config'
                })
                total_stocks += 1
        
        logger.info(f"✅ Из конфига загружено {total_stocks} акций в {len(self.sectors_config.get('sectors', {}))} секторах")
        
        for i, asset in enumerate(assets[:10]):
            logger.debug(f"  {i+1}. {asset['symbol']} - {asset['name']} ({asset['sector']})")
        
        return assets
    
    def get_200_popular_stocks(self) -> List[Dict]:
        """
        Получение списка 200 популярных российских акций
        Кэшируется на 180 дней
        """
        logger.warning("⚠️ get_200_popular_stocks() вызывается, но используется конфиг")
        return self.get_assets_from_config()
    
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
    
    def get_current_price(self, symbol: str) -> Tuple[Optional[float], Optional[float], str]:
        """
        Получение текущей цены с fallback на PREVPRICE (для неторгового времени)
        """
        source = 'unknown'
        
        for attempt in range(self.max_retries):
            try:
                endpoints = [
                    (f"https://iss.moex.com/iss/engines/stock/markets/shares/boards/TQBR/securities/{symbol}.json", 'TQBR'),
                    (f"https://iss.moex.com/iss/engines/stock/markets/index/boards/SNDX/securities/{symbol}.json", 'SNDX'),
                ]
                
                for url, board_type in endpoints:
                    try:
                        response = self.session.get(url, timeout=10)
                        if response.status_code == 200:
                            data = response.json()
                            
                            # 1. Основной вариант: Marketdata (текущая цена)
                            marketdata = data.get('marketdata', {}).get('data', [])
                            if marketdata:
                                row = marketdata[0]
                                columns = data.get('marketdata', {}).get('columns', [])
                                
                                price_idx = columns.index('LAST') if 'LAST' in columns else -1
                                
                                if price_idx != -1 and len(row) > price_idx:
                                    price = row[price_idx]
                                    
                                    if price is not None:
                                        try:
                                            price_float = float(price)
                                            if price_float > 0:
                                                source = f'moex_api_{board_type}'
                                                logger.debug(f"✅ Найден {symbol} на {board_type}: {price_float}")
                                                return price_float, 0, source
                                        except (ValueError, TypeError) as e:
                                            logger.debug(f"Ошибка преобразования цены {symbol}: {price} -> {e}")

                            # 2. Запасной вариант: Securities (цена закрытия, если рынок закрыт)
                            securities = data.get('securities', {}).get('data', [])
                            sec_cols = data.get('securities', {}).get('columns', [])
                            if securities:
                                sec_row = securities[0]
                                # Проверяем несколько полей цены по очереди
                                for col_name in ['PREVPRICE', 'PREVADMITTEDQUOTE', 'PREVLEGALCLOSEPRICE', 'CLOSE', 'LCURRENTPRICE']:
                                    if col_name in sec_cols:
                                        idx = sec_cols.index(col_name)
                                        if len(sec_row) > idx and sec_row[idx] is not None:
                                            try:
                                                price_float = float(sec_row[idx])
                                                if price_float > 0:
                                                    source = f'moex_sec_{board_type}_{col_name}'
                                                    logger.debug(f"✅ Цена из securities ({col_name}) для {symbol}: {price_float}")
                                                    return price_float, 0, source
                                            except (ValueError, TypeError):
                                                continue
                                                
                        elif response.status_code == 429:  # Too Many Requests
                            logger.warning(f"⚠️ Rate limit для {symbol}, попытка {attempt+1}/{self.max_retries}")
                            time.sleep(2 ** attempt)
                    except Exception as e:
                        logger.debug(f"Endpoint {board_type} для {symbol}: {e}")
                        continue
                
                time.sleep(self.request_delay)
                
            except Exception as e:
                logger.error(f"❌ Ошибка получения цены для {symbol}: {e}")
                if attempt < self.max_retries - 1:
                    time.sleep(1)
                else:
                    logger.error(traceback.format_exc())
        
        logger.warning(f"⚠️ Не удалось получить цену для {symbol}")
        return None, 0, source
    
    def get_historical_data(self, symbol: str, days: int = 400) -> Optional[pd.DataFrame]:
        """
        Получение исторических данных за указанное количество дней
        Добавлена задержка и обработка ошибок API
        """
        for attempt in range(self.max_retries):
            try:
                end_date = datetime.now()
                start_date = end_date - timedelta(days=days)
                start_date_str = start_date.strftime('%Y-%m-%d')
                end_date_str = end_date.strftime('%Y-%m-%d')
                
                logger.debug(f"Запрос исторических данных для {symbol} с {start_date_str} по {end_date_str}")
                
                if HAS_APIMOEX:
                    try:
                        for board in ['TQBR', 'TQTD', 'SNDX']:
                            try:
                                data = apimoex.get_board_candles(
                                    self.session,
                                    security=symbol,
                                    board=board,
                                    interval=24,
                                    start=start_date_str,
                                    end=end_date_str
                                )
                                
                                if data and len(data) > 0:
                                    df = pd.DataFrame(data)
                                    df = df.rename(columns={'end': 'timestamp'})
                                    df['timestamp'] = pd.to_datetime(df['timestamp'])
                                    df = df.sort_values('timestamp')
                                    
                                    for col in ['open', 'close', 'high', 'low']:
                                        if col in df.columns:
                                            df[col] = pd.to_numeric(df[col], errors='coerce')
                                    
                                    logger.info(f"✅ apimoex: получено {len(df)} свечей для {symbol} на {board}")
                                    return df
                            except Exception as e:
                                logger.debug(f"apimoex {board} для {symbol}: {e}")
                                continue
                    except Exception as e:
                        logger.debug(f"apimoex общая ошибка для {symbol}: {e}")
                
                logger.debug(f"Используем резервный API для исторических данных {symbol}")
                
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
                                
                                for col in ['open', 'close', 'high', 'low']:
                                    df[col] = pd.to_numeric(df[col], errors='coerce')
                                
                                logger.info(f"✅ Старый метод: получено {len(df)} свечей для {symbol}")
                                return df
                        elif response.status_code == 429:  # Too Many Requests
                            logger.warning(f"⚠️ Rate limit для {symbol}, попытка {attempt+1}/{self.max_retries}")
                            time.sleep(2 ** attempt)  # Экспоненциальная задержка
                    except Exception as e:
                        logger.debug(f"Старый метод для {symbol} ({market}/{board}): {e}")
                        continue
                        
                # Задержка между запросами
                time.sleep(self.request_delay)
                
            except Exception as e:
                logger.error(f"❌ Ошибка получения исторических данных для {symbol}: {e}")
                if attempt < self.max_retries - 1:
                    time.sleep(1)
                else:
                    logger.error(traceback.format_exc())
        
        logger.warning(f"⚠️ Не удалось получить исторические данные для {symbol}")
        return None
    
    def calculate_atr(self, df: pd.DataFrame, period: int = 14) -> float:
        """
        Расчет Average True Range (ATR) для управления рисками
        """
        try:
            if df is None or len(df) < period:
                logger.warning(f"⚠️ Недостаточно данных для расчета ATR (нужно {period}, есть {len(df) if df else 0})")
                return 0.0
            
            df_calc = df.copy()
            
            df_calc['high_low'] = df_calc['high'] - df_calc['low']
            df_calc['high_close_prev'] = abs(df_calc['high'] - df_calc['close'].shift(1))
            df_calc['low_close_prev'] = abs(df_calc['low'] - df_calc['close'].shift(1))
            
            df_calc['true_range'] = df_calc[['high_low', 'high_close_prev', 'low_close_prev']].max(axis=1)
            
            atr = df_calc['true_range'].rolling(window=period).mean().iloc[-1]
            
            if pd.isna(atr) or atr == 0:
                returns = df_calc['close'].pct_change().dropna()
                if len(returns) > 0:
                    volatility = returns.std() * df_calc['close'].iloc[-1]
                    logger.debug(f"  ATR альтернативный: {volatility:.2f}")
                    return float(volatility)
                return 0.0
            
            logger.debug(f"  ATR: {atr:.2f}")
            return float(atr)
            
        except Exception as e:
            logger.error(f"❌ Ошибка расчета ATR: {e}")
            return 0.0
    
    def get_price_on_date(self, df: pd.DataFrame, target_date: datetime) -> Optional[float]:
        """Получение цены на конкретную дату (или ближайшую предыдущую)"""
        if df is None or len(df) == 0:
            return None
        
        mask = df['timestamp'] <= target_date
        available_dates = df[mask]
        
        if len(available_dates) == 0:
            return df['close'].iloc[0]
        
        closest_idx = available_dates['timestamp'].sub(target_date).abs().idxmin()
        return df.loc[closest_idx, 'close']


class MomentumBotMOEX:
    """Бот momentum стратегии для Московской биржи с секторным отбором"""
    
    def __init__(self):
        self.telegram_token = os.getenv('TELEGRAM_TOKEN')
        self.telegram_chat_id = os.getenv('TELEGRAM_CHAT_ID')
        
        if not self.telegram_token:
            logger.error("❌ TELEGRAM_TOKEN не найден в переменных окружения")
            logger.error("❌ Добавьте TELEGRAM_TOKEN=ваш_токен_бота в файл .env")
        
        if not self.telegram_chat_id:
            logger.error("❌ TELEGRAM_CHAT_ID не найден в переменных окружения")
            logger.error("❌ Добавьте TELEGRAM_CHAT_ID=ваш_chat_id в файл .env")
        
        self.data_fetcher = MOEXDataFetcher()
        
        self.top_assets_count = 200
        self.selected_count = 10
        
        # Изменено: проверка по расписанию 2 раза в день
        self.check_times = ["14:10", "19:10"]  # GMT+3
        self.report_time = "19:30"  # GMT+3 для отправки отчета
        self.check_interval = 12 * 3600  # Фолбэк интервал 12 часов
        
        self.last_notification_time = None
        self.notification_interval = 24 * 3600  # Оповещения раз в 24 часа
        
        self.min_12m_momentum = 0.0
        
        self.weights = {'12M': 0.40, '6M': 0.35, '1M': 0.25}
        
        self.sma_fast_period = 10
        self.sma_slow_period = 30
        
        self.benchmark_symbol = 'MCFTR'
        self.benchmark_name = 'Индекс Мосбиржи полной доходности'
        
        self.atr_period = 14
        self.atr_multiplier = 2.0
        self.min_stop_loss_percent = 5.0
        self.max_stop_loss_percent = 20.0
        
        self.current_portfolio: Dict[str, Dict] = {}
        self.signal_history: List[Dict] = []
        self.asset_ranking: List[AssetData] = []
        
        self.sector_performance: Dict[str, SectorPerformance] = {}
        
        # Увеличен TTL кэша
        self._cache = {
            'top_assets': {'data': None, 'timestamp': None, 'ttl': 48 * 3600},  # 48 часов вместо 24
            'historical_data': {},
            'benchmark_data': {'data': None, 'timestamp': None, 'ttl': 24 * 3600},  # 24 часа вместо 1
            'stocks_list': {'data': None, 'timestamp': None, 'ttl': 30 * 24 * 3600}  # 30 дней вместо 180
        }
        
        self.errors_count = 0
        self.max_retries = 3
        
        self.telegram_retry_delay = 2
        self.max_telegram_retries = 3
        
        self.use_sector_selection = True
        self.test_mode = False
        
        # Задержка между запросами при анализе
        self.analysis_request_delay = 0.5
        
        logger.info("🚀 Momentum Bot для Московской биржи инициализирован")
        logger.info(f"📊 Параметры: Секторный отбор {self.top_assets_count} акций")
        logger.info(f"⚙️ Фильтры: 12M > {self.min_12m_momentum}%, SMA положительный")
        logger.info(f"📈 Источник данных: {'apimoex' if HAS_APIMOEX else 'MOEX API (apimoex недоступен)'}")
        logger.info(f"🕐 Расписание: проверки в 14:10 и 19:10, отчет в 19:30 (GMT+3)")
        logger.info(f"📊 Бенчмарк: {self.benchmark_symbol} ({self.benchmark_name})")
        logger.info(f"🎯 Стратегия: {'Секторный отбор' if self.use_sector_selection else 'Топ-10 отбор'}")
        logger.info(f"⚠️ Управление рисками: ATR({self.atr_period}) стоп-лосс x{self.atr_multiplier}")
        logger.info(f"⏱️ Задержка между запросами: {self.analysis_request_delay} сек")
        
        if self.telegram_token and self.telegram_chat_id:
            logger.info("✅ Telegram настроен корректно")
        else:
            logger.warning("⚠️ Telegram не настроен. Сообщения не будут отправляться.")
    
    def clear_cache(self):
        """Очистка кэша данных"""
        logger.info("🧹 Очистка кэша данных...")
        self._cache = {
            'top_assets': {'data': None, 'timestamp': None, 'ttl': 48*3600},
            'historical_data': {},
            'benchmark_data': {'data': None, 'timestamp': None, 'ttl': 24*3600},
            'stocks_list': {'data': None, 'timestamp': None, 'ttl': 30*24*3600}
        }
        logger.info("✅ Кэш очищен")
    
    def get_stocks_list(self) -> List[Dict]:
        """
        Получение списка акций ТОЛЬКО из конфигурационного файла
        """
        cache = self._cache['stocks_list']
        
        if cache['data'] and cache['timestamp']:
            cache_age = (datetime.now() - cache['timestamp']).total_seconds()
            if cache_age < cache['ttl']:
                logger.info(f"✅ Используем кэшированный список акций из конфига (возраст: {cache_age/86400:.1f} дней)")
                return cache['data']
        
        logger.info("📊 Получение списка акций из конфигурационного файла...")
        stocks_list = self.data_fetcher.get_assets_from_config()
        
        if not stocks_list:
            logger.error("❌ КРИТИЧЕСКАЯ ОШИБКА: Нет акций в конфигурационном файле")
            logger.error("❌ Проверьте файл sectors_config.json")
            raise Exception("Не удалось получить список акций из конфигурационного файла")
        
        self._cache['stocks_list'] = {
            'data': stocks_list,
            'timestamp': datetime.now(),
            'ttl': 30*24*3600  # 30 дней
        }
        
        logger.info(f"✅ Получено {len(stocks_list)} акций из конфига, сохранено в кэш на 30 дней")
        
        sector_stats = {}
        for stock in stocks_list:
            sector = stock.get('sector', 'Другое')
            if sector not in sector_stats:
                sector_stats[sector] = 0
            sector_stats[sector] += 1
        
        for sector, count in sector_stats.items():
            logger.info(f"  • {sector}: {count} акций")
        
        return stocks_list
    
    def get_top_assets(self) -> List[Dict]:
        """
        Получение топ активов для анализа
        """
        try:
            cache = self._cache['top_assets']
            if cache['data'] and cache['timestamp']:
                cache_age = (datetime.now() - cache['timestamp']).total_seconds()
                if cache_age < cache['ttl']:
                    logger.info(f"📊 Используем кэшированные топ активов (возраст: {cache_age/3600:.1f} часов)")
                    return cache['data']
            
            logger.info("📊 Формирование списка активов для анализа из конфига...")
            
            all_stocks = self.get_stocks_list()
            
            if not all_stocks:
                logger.error("❌ Нет данных об акциях в конфиге")
                return []
            
            all_assets = []
            filtered_assets = []
            
            for i, stock in enumerate(all_stocks, 1):
                symbol = stock['symbol']
                name = stock['name']
                
                try:
                    price, _, source = self.data_fetcher.get_current_price(symbol)
                    
                    if price is None or price <= 0:
                        filtered_assets.append(f"⚠️ {symbol}: не удалось получить цену")
                        logger.warning(f"⚠️ Не удалось получить цену для {symbol}")
                        continue
                    
                    all_assets.append({
                        'symbol': symbol,
                        'name': name,
                        'sector': stock.get('sector', ''),
                        'sector_data': stock.get('sector_data', {}),
                        'current_price': price,
                        'volume_24h': 0,
                        'source': source,
                        'market_type': 'stock'
                    })
                    
                    logger.debug(f"  ✅ {symbol}: {price:.2f} руб ({stock.get('sector', 'Другое')})")
                    
                    # Задержка между запросами
                    if i % 5 == 0:
                        time.sleep(self.analysis_request_delay)
                            
                except Exception as e:
                    filtered_assets.append(f"❌ {symbol}: ошибка {str(e)[:50]}")
                    logger.error(f"  ❌ {symbol}: {e}")
                    continue
            
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
            
            if len(all_assets) == 0:
                logger.error("❌ Не удалось получить данные ни для одного актива")
                raise Exception("Не удалось получить данные по акциям")
            
            self._cache['top_assets'] = {
                'data': all_assets,
                'timestamp': datetime.now(),
                'ttl': 48*3600  # 48 часов
            }
            
            logger.info(f"✅ Сформирован список из {len(all_assets)} активов (включая бенчмарк)")
            
            return all_assets
            
        except Exception as e:
            logger.error(f"❌ Критическая ошибка получения топ активов: {e}")
            if self.telegram_token and self.telegram_chat_id:
                self.send_telegram_message(
                    f"❌ *КРИТИЧЕСКАЯ ОШИБКА*\\n"
                    f"Не удалось получить данные акций:\\n"
                    f"```{str(e)[:100]}```\\n"
                    f"Бот остановлен.",
                    silent=False,
                    force=True
                )
            raise
    
    @lru_cache(maxsize=200)
    def get_cached_historical_data(self, symbol: str, days: int = 400) -> Optional[pd.DataFrame]:
        """
        Получение исторических данных с кэшированием на 24 часа
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
            min_required_days = 250
            if len(df) < min_required_days:
                logger.warning(f"⚠️ Мало исторических данных для {symbol}: {len(df)} дней (< {min_required_days})")
            
            self._cache['historical_data'][cache_key] = {
                'data': df,
                'timestamp': datetime.now(),
                'ttl': 24 * 3600  # 24 часа вместо 1
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
        
        target_date = target_date.replace(hour=0, minute=0, second=0, microsecond=0)
        
        mask = df['timestamp'].dt.date <= target_date.date()
        available_dates = df[mask]
        
        if len(available_dates) == 0:
            logger.debug(f"Нет данных до {target_date.date()}, берем самую раннюю")
            return df['close'].iloc[0]
        
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
            
            df = self.get_cached_historical_data(self.benchmark_symbol, 400)
            if df is None or len(df) < 126:
                logger.error(f"❌ Недостаточно данных бенчмарка {self.benchmark_symbol}")
                return None
            
            current_price = df['close'].iloc[-1]
            
            current_date = datetime.now()
            
            week_ago = current_date - timedelta(days=7)
            week_ago = week_ago - timedelta(days=week_ago.weekday())
            
            month_ago = current_date - timedelta(days=30)
            six_months_ago = current_date - timedelta(days=180)
            year_ago = current_date - timedelta(days=365)
            
            price_1w_ago = self.get_price_for_calendar_date(df, week_ago)
            price_1m_ago = self.get_price_for_calendar_date(df, month_ago)
            price_6m_ago = self.get_price_for_calendar_date(df, six_months_ago)
            price_12m_ago = self.get_price_for_calendar_date(df, year_ago)
            
            try:
                momentum_1m = ((price_1w_ago - price_1m_ago) / price_1m_ago) * 100 if price_1m_ago > 0 else 0
                momentum_6m = ((price_1m_ago - price_6m_ago) / price_6m_ago) * 100 if price_6m_ago > 0 else 0
                momentum_12m = ((price_1m_ago - price_12m_ago) / price_12m_ago) * 100 if price_12m_ago > 0 else 0
                absolute_momentum_6m = ((current_price - price_6m_ago) / price_6m_ago) * 100 if price_6m_ago > 0 else 0
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
                'ttl': 24 * 3600  # 24 часа вместо 1
            }
            
            logger.info(f"✅ Данные бенчмарка: 6M моментум = {absolute_momentum_6m:.2f}%, 12M моментум = {absolute_momentum_12m:.2f}%")
            
            return benchmark_data
            
        except Exception as e:
            logger.error(f"❌ Ошибка получения данных бенчмарка: {e}")
            return None
    
    def calculate_momentum_values(self, asset_info: Dict) -> Optional[AssetData]:
        """
        Расчет значений моментума с использованием календарных дней
        """
        try:
            symbol = asset_info['symbol']
            name = asset_info['name']
            source = asset_info.get('source', 'unknown')
            
            logger.debug(f"📈 Расчет моментума для {symbol} ({name})...")
            
            df = self.get_cached_historical_data(symbol, 400)
            if df is None or len(df) == 0:
                logger.error(f"❌ Нет исторических данных для {symbol}")
                return None
            
            if len(df) < 100:
                logger.warning(f"⚠️ Мало исторических данных для {symbol}: {len(df)} дней")
                return None
            
            current_price = df['close'].iloc[-1]
            
            if current_price <= 0:
                logger.error(f"❌ Некорректная цена для {symbol}: {current_price}")
                return None
            
            current_date = datetime.now()
            
            week_ago = current_date - timedelta(days=7)
            week_ago = week_ago - timedelta(days=week_ago.weekday())
            
            month_ago = current_date - timedelta(days=30)
            six_months_ago = current_date - timedelta(days=180)
            year_ago = current_date - timedelta(days=365)
            
            price_1w_ago = self.get_price_for_calendar_date(df, week_ago)
            price_1m_ago = self.get_price_for_calendar_date(df, month_ago)
            price_6m_ago = self.get_price_for_calendar_date(df, six_months_ago)
            price_12m_ago = self.get_price_for_calendar_date(df, year_ago)
            
            if None in [price_1w_ago, price_1m_ago, price_6m_ago, price_12m_ago]:
                logger.error(f"❌ Не удалось получить цены на календарные даты для {symbol}")
                return None
            
            try:
                momentum_1m = ((price_1w_ago - price_1m_ago) / price_1m_ago) * 100 if price_1m_ago > 0 else 0
                momentum_6m = ((price_1m_ago - price_6m_ago) / price_6m_ago) * 100 if price_6m_ago > 0 else 0
                momentum_12m = ((price_1m_ago - price_12m_ago) / price_12m_ago) * 100 if price_12m_ago > 0 else 0
                absolute_momentum = ((current_price - price_12m_ago) / price_12m_ago) * 100 if price_12m_ago > 0 else 0
                absolute_momentum_6m = ((current_price - price_6m_ago) / price_6m_ago) * 100 if price_6m_ago > 0 else 0
                
            except ZeroDivisionError:
                logger.error(f"❌ Ошибка деления на ноль для {symbol}")
                return None
            
            combined_momentum = (
                momentum_12m * self.weights['12M'] +
                momentum_6m * self.weights['6M'] +
                momentum_1m * self.weights['1M']
            )
            
            sma_fast = df['close'].tail(self.sma_fast_period).mean()
            sma_slow = df['close'].tail(self.sma_slow_period).mean()
            sma_signal = sma_fast > sma_slow
            
            atr = self.data_fetcher.calculate_atr(df, period=self.atr_period)
            
            stop_loss = 0.0
            atr_percent = 0.0
            
            if atr > 0 and current_price > 0:
                atr_percent = (atr / current_price) * 100
                
                stop_loss_price = current_price - (self.atr_multiplier * atr)
                
                stop_loss_percent = ((current_price - stop_loss_price) / current_price) * 100
                
                if stop_loss_percent < self.min_stop_loss_percent:
                    stop_loss_price = current_price * (1 - self.min_stop_loss_percent / 100)
                elif stop_loss_percent > self.max_stop_loss_percent:
                    stop_loss_price = current_price * (1 - self.max_stop_loss_percent / 100)
                
                stop_loss = max(stop_loss_price, 0.01)
                
                logger.debug(f"  {symbol}: ATR={atr:.2f} ({atr_percent:.1f}%), Stop-Loss={stop_loss:.2f}")
            
            volume_24h = asset_info.get('volume_24h', 0)
            sector = asset_info.get('sector', '')
            market_type = asset_info.get('market_type', 'stock')
            
            logger.debug(f"  {symbol}: Цена {current_price:.2f}, 12M: {momentum_12m:+.1f}%, 6M: {absolute_momentum_6m:+.1f}%, 1M: {momentum_1m:+.1f}%, SMA: {'🟢' if sma_signal else '🔴'}, SL: {stop_loss:.2f}")
            
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
                atr=atr,
                stop_loss=stop_loss,
                atr_period=self.atr_period,
                timestamp=datetime.now(),
                market_type=market_type,
                sector=sector,
                currency='rub',
                source=source
            )
            
        except Exception as e:
            logger.error(f"❌ Ошибка расчета моментума для {asset_info.get('symbol', 'unknown')}: {e}")
            logger.error(traceback.format_exc())
            return None
    
    def analyze_assets(self) -> List[AssetData]:
        """
        Анализ активов с секторным отбором
        """
        top_assets = self.get_top_assets()
        if not top_assets:
            logger.error("❌ Нет активов для анализа")
            return []
        
        logger.info(f"📊 Анализ {len(top_assets)} активов из конфига...")
        
        benchmark_data = self.get_benchmark_data()
        
        sector_performance = {}
        
        for sector_name, sector_data in self.data_fetcher.sectors_config.get('sectors', {}).items():
            priority = sector_data.get('priority', 99)
            top_n = sector_data.get('top_n', 3)
            description = sector_data.get('description', '')
            
            sector_performance[sector_name] = SectorPerformance(
                sector_name=sector_name,
                description=description,
                priority=priority,
                top_n=top_n
            )
        
        sector_performance['Индекс'] = SectorPerformance(
            sector_name='Индекс',
            description='Индекс Мосбиржи полной доходности',
            priority=0,
            top_n=1
        )
        
        sector_assets = defaultdict(list)
        
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
            
            if symbol == self.benchmark_symbol:
                continue
                
            filter_stats['total'] += 1
            
            try:
                asset_data = self.calculate_momentum_values(asset_info)
                if asset_data is None:
                    filter_stats['no_data'] += 1
                    logger.debug(f"  ⚠️ {symbol}: нет данных для анализа")
                    continue
                
                if asset_data.momentum_12m < self.min_12m_momentum:
                    filter_stats['failed_12m'] += 1
                    logger.debug(f"  ❌ {symbol}: низкий 12M моментум ({asset_data.momentum_12m:+.1f}% < {self.min_12m_momentum}%)")
                    continue
                filter_stats['passed_12m'] += 1
                
                if not asset_data.sma_signal:
                    filter_stats['failed_sma'] += 1
                    logger.debug(f"  ❌ {symbol}: отрицательный SMA сигнал")
                    continue
                filter_stats['passed_sma'] += 1
                
                if benchmark_data:
                    if asset_data.absolute_momentum_6m <= benchmark_data['absolute_momentum_6m']:
                        filter_stats['failed_benchmark'] += 1
                        logger.debug(f"  ❌ {symbol}: 6M моментум ({asset_data.absolute_momentum_6m:+.1f}%) <= бенчмарку ({benchmark_data['absolute_momentum_6m']:+.1f}%)")
                        continue
                    filter_stats['passed_benchmark'] += 1
                else:
                    logger.warning("Нет данных бенчмарка, пропускаем сравнение")
                
                sector = asset_data.sector
                
                if sector not in sector_performance:
                    logger.info(f"  📝 Создаем новый сектор: {sector}")
                    sector_performance[sector] = SectorPerformance(
                        sector_name=sector,
                        description='Автоматически созданный сектор',
                        priority=99,
                        top_n=1
                    )
                
                sector_assets[sector].append(asset_data)
                filter_stats['passed_all'] += 1
                logger.debug(f"  ✅ {symbol}: добавлен в сектор {sector}")
                
                # Задержка между запросами для предотвращения rate limiting
                if i % 5 == 0:
                    time.sleep(self.analysis_request_delay)
                
            except Exception as e:
                filter_stats['errors'] += 1
                logger.error(f"Ошибка анализа {symbol}: {e}")
                logger.error(traceback.format_exc())
                continue
        
        selected_assets = []
        
        for sector_name, assets in sector_assets.items():
            if sector_name not in sector_performance:
                logger.warning(f"⚠️ Сектор {sector_name} не найден в конфиге, создаем с параметрами по умолчанию")
                sector_performance[sector_name] = SectorPerformance(
                    sector_name=sector_name,
                    description='Автоматически созданный сектор',
                    priority=99,
                    top_n=1
                )
            
            performance = sector_performance[sector_name]
            performance.total_stocks = len(assets)
            performance.analyzed_stocks = len(assets)
            
            if assets:
                sorted_assets = sorted(assets, key=lambda x: x.combined_momentum, reverse=True)
                
                top_n = min(performance.top_n, len(sorted_assets))
                sector_selected = sorted_assets[:top_n]
                
                performance.selected_stocks = sector_selected
                performance.passed_filters = len(sector_selected)
                
                if sector_selected:
                    performance.avg_combined_momentum = np.mean([a.combined_momentum for a in sector_selected])
                    performance.avg_absolute_momentum_6m = np.mean([a.absolute_momentum_6m for a in sector_selected])
                    performance.avg_momentum_12m = np.mean([a.momentum_12m for a in sector_selected])
                    
                    atr_percents = []
                    for a in sector_selected:
                        if a.atr > 0 and a.current_price > 0:
                            atr_percent = (a.atr / a.current_price) * 100
                            atr_percents.append(atr_percent)
                    
                    if atr_percents:
                        performance.avg_atr_percent = np.mean(atr_percents)
                    
                    if benchmark_data:
                        performance.vs_benchmark = performance.avg_absolute_momentum_6m - benchmark_data['absolute_momentum_6m']
                    
                    performance.performance_score = performance.avg_combined_momentum * (100 - performance.priority) / 100
                
                selected_assets.extend(sector_selected)
                logger.info(f"  📊 {sector_name}: отобрано {len(sector_selected)}/{len(assets)} акций")
        
        self.sector_performance = sector_performance
        
        selected_assets.sort(key=lambda x: x.combined_momentum, reverse=True)
        
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
        
        logger.info(f"📈 Секторная статистика:")
        for sector_name, performance in sorted(self.sector_performance.items(), 
                                              key=lambda x: x[1].performance_score, reverse=True):
            if performance.selected_stocks:
                logger.info(f"  • {sector_name}: {len(performance.selected_stocks)} акций, средний моментум: {performance.avg_combined_momentum:+.1f}%, ATR: {performance.avg_atr_percent:.1f}%")
        
        if filter_stats['passed_all'] == 0:
            logger.warning("⚠️ Все активы отфильтрованы по критериям")
        
        if selected_assets:
            logger.info("🏆 Топ активов по секторам:")
            for i, asset in enumerate(selected_assets[:20], 1):
                vs_benchmark = f" vs бенчмарк: {asset.absolute_momentum_6m - benchmark_data['absolute_momentum_6m']:+.1f}%" if benchmark_data else ""
                atr_info = f", ATR: {asset.atr:.2f} ({asset.atr/asset.current_price*100:.1f}%)" if asset.atr > 0 else ""
                logger.info(f"  {i:2d}. {asset.symbol} ({asset.sector}): {asset.combined_momentum:+.2f}% (12M: {asset.momentum_12m:+.1f}%, 6M: {asset.absolute_momentum_6m:+.1f}%{vs_benchmark}{atr_info})")
        
        return selected_assets
    
    def generate_signals(self, assets: List[AssetData]) -> List[Dict]:
        """
        Генерация сигналов с секторной логикой
        """
        signals = []
        benchmark_data = self.get_benchmark_data()
        
        asset_dict = {asset.symbol: asset for asset in assets}
        
        selected_symbols = {asset.symbol for asset in assets}
        
        for asset in assets:
            symbol = asset.symbol
            current_status = self.current_portfolio.get(symbol, {}).get('status', 'OUT')
            
            if symbol in selected_symbols:
                if (asset.absolute_momentum > 0 and
                    asset.sma_signal and
                    current_status != 'IN'):
                    
                    active_positions = sum(1 for v in self.current_portfolio.values() if v.get('status') == 'IN')
                    
                    if active_positions < 30:
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
                            'atr': asset.atr,
                            'stop_loss': asset.stop_loss,
                            'market_type': asset.market_type,
                            'sector': asset.sector,
                            'reason': f"{asset.sector}, Моментум 12M: {asset.absolute_momentum:+.1f}%, SMA положительный, ATR: {asset.atr:.2f}",
                            'timestamp': datetime.now()
                        }
                        
                        self.current_portfolio[symbol] = {
                            'entry_time': datetime.now(),
                            'entry_price': asset.current_price,
                            'status': 'IN',
                            'name': asset.name,
                            'sector': asset.sector,
                            'source': asset.source,
                            'stop_loss': asset.stop_loss,
                            'atr': asset.atr,
                            'atr_percent': asset.atr / asset.current_price * 100 if asset.current_price > 0 else 0
                        }
                        
                        signals.append(signal)
                        logger.info(f"📈 BUY для {symbol} ({asset.name}, {asset.sector}), стоп-лосс: {asset.stop_loss:.2f}")
                    else:
                        worst_position = None
                        worst_momentum = float('inf')
                        
                        for pos_symbol, pos_data in self.current_portfolio.items():
                            if pos_data.get('status') == 'IN':
                                pos_asset = asset_dict.get(pos_symbol)
                                if pos_asset:
                                    if pos_asset.combined_momentum < worst_momentum:
                                        worst_momentum = pos_asset.combined_momentum
                                        worst_position = pos_symbol
                        
                        if worst_position and worst_momentum < asset.combined_momentum:
                            entry_data = self.current_portfolio.get(worst_position, {})
                            entry_price = entry_data.get('entry_price', 0)
                            current_price = asset_dict.get(worst_position, asset).current_price
                            profit_percent = ((current_price - entry_price) / entry_price) * 100 if entry_price > 0 else 0
                            
                            sell_signal = {
                                'symbol': worst_position,
                                'action': 'SELL',
                                'price': current_price,
                                'entry_price': entry_price,
                                'profit_percent': profit_percent,
                                'reason': f"Замена на более перспективную акцию ({symbol})",
                                'timestamp': datetime.now()
                            }
                            
                            signals.append(sell_signal)
                            self.current_portfolio[worst_position] = {
                                'status': 'OUT',
                                'exit_time': datetime.now(),
                                'exit_price': current_price,
                                'profit_percent': profit_percent,
                                'name': entry_data.get('name', worst_position)
                            }
                            logger.info(f"📉 SELL для замены {worst_position}: {profit_percent:+.2f}%")
                            
                            buy_signal = {
                                'symbol': symbol,
                                'action': 'BUY',
                                'price': asset.current_price,
                                'absolute_momentum': asset.absolute_momentum,
                                'absolute_momentum_6m': asset.absolute_momentum_6m,
                                'atr': asset.atr,
                                'stop_loss': asset.stop_loss,
                                'reason': f"Замена {worst_position}, {asset.sector}, Моментум 12M: {asset.absolute_momentum:+.1f}%, ATR: {asset.atr:.2f}",
                                'timestamp': datetime.now()
                            }
                            
                            self.current_portfolio[symbol] = {
                                'entry_time': datetime.now(),
                                'entry_price': asset.current_price,
                                'status': 'IN',
                                'name': asset.name,
                                'sector': asset.sector,
                                'source': asset.source,
                                'stop_loss': asset.stop_loss,
                                'atr': asset.atr,
                                'atr_percent': asset.atr / asset.current_price * 100 if asset.current_price > 0 else 0
                            }
                            
                            signals.append(buy_signal)
                            logger.info(f"📈 BUY для {symbol} (замена {worst_position}), стоп-лосс: {asset.stop_loss:.2f}")
            
            elif current_status == 'IN':
                sell_reason = ""
                should_sell = False
                
                if asset.stop_loss > 0 and asset.current_price <= asset.stop_loss:
                    sell_reason = f"Достигнут стоп-лосс ({asset.stop_loss:.2f})"
                    should_sell = True
                
                elif asset.absolute_momentum < 0:
                    sell_reason = "Моментум 12M < 0%"
                    should_sell = True
                
                elif not asset.sma_signal:
                    sell_reason = "SMA отрицательный"
                    should_sell = True
                
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
                        'atr': asset.atr,
                        'stop_loss': asset.stop_loss,
                        'reason': f"Выход: {sell_reason}",
                        'timestamp': datetime.now()
                    }
                    
                    self.current_portfolio[symbol] = {
                        'status': 'OUT',
                        'exit_time': datetime.now(),
                        'exit_price': asset.current_price,
                        'profit_percent': profit_percent,
                        'name': entry_data.get('name', asset.name),
                        'stop_loss_hit': sell_reason.startswith("Достигнут стоп-лосс")
                    }
                    
                    signals.append(signal)
                    logger.info(f"📉 SELL для {symbol}: {profit_percent:+.2f}% ({sell_reason})")
        
        return signals
    
    def should_send_notification(self) -> bool:
        """Проверка, нужно ли отправлять оповещение (раз в 24 часа)"""
        if self.last_notification_time is None:
            return True
        
        time_since_last = (datetime.now() - self.last_notification_time).total_seconds()
        return time_since_last >= self.notification_interval
    
    def send_telegram_message(self, message: str, silent: bool = False, force: bool = False) -> bool:
        """
        Отправка сообщения в Telegram с автоматической разбивкой длинных текстов
        """
        # Проверка лимита частоты отправки
        if force:
            logger.debug(f"📨 Принудительная отправка сообщения (force=True)")
        elif not force and not self.should_send_notification() and not silent:
            logger.debug(f"⏰ Пропускаем оповещение (прошло менее 24 часов)")
            return False
        
        if not self.telegram_token or not self.telegram_chat_id:
            if not silent:
                logger.warning("⚠️ Нет данных для Telegram")
            return False

        # === ЛОГИКА РАЗБИВКИ СООБЩЕНИЙ (Telegram limit ~4096 chars) ===
        messages_to_send = []
        max_len = 4000  # Берем с запасом
        
        if len(message) > max_len:
            logger.info(f"📨 Сообщение длинное ({len(message)} симв.), разбиваем на части...")
            temp_msg = message
            while temp_msg:
                if len(temp_msg) <= max_len:
                    messages_to_send.append(temp_msg)
                    break
                
                # Ищем перенос строки для красивой разбивки
                split_pos = temp_msg.rfind('\n', 0, max_len)
                if split_pos == -1:
                    split_pos = max_len
                
                chunk = temp_msg[:split_pos]
                messages_to_send.append(chunk)
                temp_msg = temp_msg[split_pos:]
        else:
            messages_to_send = [message]

        # === ОТПРАВКА ЧАСТЕЙ ===
        all_success = True
        
        for i, msg_chunk in enumerate(messages_to_send):
            chunk_success = False
            
            # Если частей много, добавляем паузу между ними
            if i > 0:
                time.sleep(0.5)

            for attempt in range(self.max_telegram_retries):
                try:
                    url = f"https://api.telegram.org/bot{self.telegram_token}/sendMessage"
                    data = {
                        "chat_id": self.telegram_chat_id,
                        "text": msg_chunk,
                        "parse_mode": "Markdown",
                        "disable_web_page_preview": True,
                        "disable_notification": silent
                    }
                    
                    response = requests.post(url, data=data, timeout=10)
                    
                    if response.status_code == 200:
                        if not silent:
                            self.last_notification_time = datetime.now()
                        chunk_success = True
                        break # Успех, выходим из цикла попыток
                        
                    elif response.status_code == 400 and data.get('parse_mode'):
                        # Если ошибка форматирования, пробуем без Markdown
                        logger.warning(f"⚠️ Ошибка Telegram 400 (Part {i+1}). Пробуем без Markdown.")
                        data.pop('parse_mode')
                        response = requests.post(url, data=data, timeout=10)
                        if response.status_code == 200:
                            chunk_success = True
                            break
                    else:
                        if not silent:
                            logger.warning(f"Ошибка Telegram (попытка {attempt+1}): {response.status_code}")
                        
                except Exception as e:
                    if not silent:
                        logger.warning(f"Ошибка отправки Telegram (попытка {attempt+1}): {e}")
                
                if attempt < self.max_telegram_retries - 1:
                    time.sleep(self.telegram_retry_delay)
            
            if not chunk_success:
                all_success = False
                logger.error(f"❌ Не удалось отправить часть сообщения #{i+1}")

        return all_success
    
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
                
                for symbol, data in self.current_portfolio.items():
                    if 'entry_time' in data and isinstance(data['entry_time'], str):
                        data['entry_time'] = datetime.fromisoformat(data['entry_time'].replace('Z', '+00:00'))
                    if 'exit_time' in data and isinstance(data['exit_time'], str):
                        data['exit_time'] = datetime.fromisoformat(data['exit_time'].replace('Z', '+00:00'))
                
                self.signal_history = state.get('signal_history', [])
                self.errors_count = state.get('errors_count', 0)
                
                if 'last_notification_time' in state and state['last_notification_time']:
                    self.last_notification_time = datetime.fromisoformat(state['last_notification_time'])
                
                active_count = len([v for v in self.current_portfolio.values() if v.get('status') == 'IN'])
                logger.info(f"💾 Состояние загружено. Активных позиций: {active_count}")
                logger.info(f"⏰ Последнее оповещение: {self.last_notification_time}")
            else:
                logger.info("📁 Файл состояния не найден, используется состояние по умолчанию")
        except json.JSONDecodeError as e:
            logger.error(f"❌ Ошибка парсинга JSON в файле состояния: {e}")
            logger.warning("🔄 Используется состояние по умолчанию")
        except Exception as e:
            logger.error(f"❌ Ошибка загрузки состояния: {e}")
            logger.warning("🔄 Используется состояние по умолчанию")
    
    def format_active_positions(self) -> str:
        """
        Форматирование списка активных позиций (исправленная версия)
        Исправлено: убрано дублирование 6M моментума
        """
        active_positions = {k: v for k, v in self.current_portfolio.items() 
                        if v.get('status') == 'IN'}
        
        if not active_positions:
            return "📊 *АКТИВНЫХ ПОЗИЦИЙ НЕТ*\nВсе средства в рублях"
        
        # Получаем данные бенчмарка
        benchmark_data = self.get_benchmark_data()
        benchmark_momentum = benchmark_data['absolute_momentum_6m'] if benchmark_data else 0
        
        message = "📊 *ПОРТФЕЛЬ:* "
        
        # Группируем позиции по секторам
        sector_positions = defaultdict(list)
        all_profits = []
        sector_stats = {}
        
        for symbol, data in active_positions.items():
            try:
                entry_price = float(data.get('entry_price', 0))
                stop_loss = float(data.get('stop_loss', 0))
                atr_percent = float(data.get('atr_percent', 0))
            except (ValueError, TypeError):
                entry_price = 0.0
                stop_loss = 0.0
                atr_percent = 0.0

            sector = data.get('sector', 'Другое')
            
            try:
                # Получаем текущую цену
                price, _, _ = self.data_fetcher.get_current_price(symbol)
                if price and price > 0:
                    profit_percent = ((price - entry_price) / entry_price) * 100
                    
                    # Получаем полные данные актива из asset_ranking
                    asset_data = None
                    for asset in self.asset_ranking:
                        if asset.symbol == symbol:
                            asset_data = asset
                            break
                    
                    # Если не нашли в asset_ranking, пробуем получить данные отдельно
                    if not asset_data:
                        # Создаем asset_info для вызова calculate_momentum_values
                        asset_info = {
                            'symbol': symbol,
                            'name': data.get('name', symbol),
                            'sector': sector,
                            'source': data.get('source', 'moex'),
                            'market_type': 'stock'
                        }
                        # Получаем данные через calculate_momentum_values
                        asset_data = self.calculate_momentum_values(asset_info)
                    
                    # Информация о позиции
                    pos_info = {
                        'symbol': symbol,
                        'name': data.get('name', symbol),
                        'entry_price': entry_price,
                        'current_price': price,
                        'profit_percent': profit_percent,
                        'stop_loss': stop_loss,
                        'atr_percent': atr_percent,
                        'asset_data': asset_data
                    }
                    
                    sector_positions[sector].append(pos_info)
                    all_profits.append(profit_percent)
                    
                    # Собираем статистику по сектору
                    if sector not in sector_stats:
                        sector_stats[sector] = {
                            'positions': [],
                            'combined_momentums': [],
                            'atr_percents': [],
                            'profits': []
                        }
                    
                    sector_stats[sector]['positions'].append(symbol)
                    sector_stats[sector]['profits'].append(profit_percent)
                    
                    if asset_data:
                        sector_stats[sector]['combined_momentums'].append(asset_data.combined_momentum)
                        if asset_data.atr > 0 and asset_data.current_price > 0:
                            atr_pct = (asset_data.atr / asset_data.current_price) * 100
                            sector_stats[sector]['atr_percents'].append(atr_pct)
                            
            except Exception as e:
                logger.error(f"Ошибка получения данных для {symbol}: {e}")
                continue
        
        # Если нет позиций с данными
        if not sector_positions:
            return "📊 *Нет данных по позициям*"
        
        total_avg = sum(all_profits) / len(all_profits) if all_profits else 0
        message += f"{len(active_positions)} акций | 📈{total_avg:+.2f}%\n\n"
        
        # Выводим позиции по секторам
        for sector, positions in sorted(sector_positions.items()):
            # Сортируем позиции по прибыли
            positions.sort(key=lambda x: x['profit_percent'], reverse=True)
            
            # Средняя прибыль по сектору
            sector_profits = [p['profit_percent'] for p in positions]
            sector_avg = sum(sector_profits) / len(sector_profits) if sector_profits else 0
            
            message += f"🏢 *{sector} ({len(positions)}): {sector_avg:+.2f}%*\n"
            
            for pos in positions:
                emoji = "🟢" if pos['profit_percent'] > 0 else "🔴"
                
                # Основная строка
                main_line = f"• {pos['symbol']} {pos['profit_percent']:+.2f}% {emoji}"
                
                # Цены (без слов "вход" и "текущая")
                price_line = f"({pos['entry_price']:.2f}→{pos['current_price']:.2f})"
                
                # Стоп-лосс
                stop_loss_percent = 0
                if pos['stop_loss'] > 0:
                    stop_loss_percent = ((pos['current_price'] - pos['stop_loss']) / pos['current_price']) * 100
                stop_line = f" SL({pos['stop_loss']:.2f})"
                
                # SMA сигнал
                sma_signal = "↑" if pos['asset_data'] and pos['asset_data'].sma_signal else "↓"
                sma_line = f" | SMA:{sma_signal}"
                
                # Моментумы и сравнение с бенчмарком (ИСПРАВЛЕНО: убрано дублирование 6M)
                momentum_line = ""
                if pos['asset_data']:
                    # Только абсолютный 6M моментум
                    vs_benchmark = pos['asset_data'].absolute_momentum_6m - benchmark_momentum if benchmark_data else 0
                    
                    # Форматируем строку с моментами
                    momentum_line = (
                        f"\nКомби: {pos['asset_data'].combined_momentum:+.1f}%"
                        f"(12M: {pos['asset_data'].momentum_12m:+.1f}%, "
                        f"6M: {pos['asset_data'].absolute_momentum_6m:+.1f}% | "
                        f"бенч: {vs_benchmark:+.1f}%)"
                    )
                
                # Собираем строку
                message += f"{main_line} {price_line}{stop_line}{sma_line}"
                if momentum_line:
                    message += momentum_line
                message += "\n"
            
            message += "\n"
        
        # Секторная статистика
        message += "*Секторная статистика:*\n"
        
        # Эмодзи для секторов
        sector_emojis = {
            'Электроэнергетика': '⚡',
            'Потребительские товары': '🛒',
            'Прочие': '📦',
            'Фармацевтика и медицина': '💊',
            'Металлы и добыча': '⚙️',
            'Информационные технологии': '💻',
            'Нефть и газ': '🛢️',
            'Финансы': '🏦',
            'Другое': '📁'
        }
        
        for sector, stats in sector_stats.items():
            emoji = sector_emojis.get(sector, '📊')
            
            # Средний комбинированный моментум
            avg_momentum = 0
            if stats['combined_momentums']:
                avg_momentum = sum(stats['combined_momentums']) / len(stats['combined_momentums'])
            
            # Средний ATR
            avg_atr = 0
            if stats['atr_percents']:
                avg_atr = sum(stats['atr_percents']) / len(stats['atr_percents'])
            
            # Формируем строку - ВСЕГДА выводим средний моментум, даже если 0
            sector_line = f"{emoji} {sector}: {len(stats['positions'])} акций"
            
            # Всегда выводим средний моментум
            sector_line += f", средний моментум: {avg_momentum:+.1f}%"
            
            # Выводим ATR только если он есть
            if avg_atr != 0:
                sector_line += f", ATR: {avg_atr:.1f}%"
            
            message += f"{sector_line}\n"
        
        return message

    def format_combined_report(self, assets: List[AssetData]) -> str:
        """
        Объединенный отчет: рейтинг акций + эффективность секторов
        Использует новый формат как вы просили
        """
        if not assets:
            return "📊 *Нет данных для отчета*"
        
        benchmark_data = self.get_benchmark_data()
        benchmark_momentum = benchmark_data['absolute_momentum_6m'] if benchmark_data else 0
        current_date = datetime.now().strftime('%d.%m.%Y')
        
        # Группируем активы по секторам
        sector_assets = defaultdict(list)
        for asset in assets:
            sector_assets[asset.sector].append(asset)
        
        # Сортируем активы в каждом секторе по комбинированному моментуму
        for sector in sector_assets:
            sector_assets[sector].sort(key=lambda x: x.combined_momentum, reverse=True)
        
        # Получаем общее количество акций в каждом секторе из конфига
        sector_totals = {}
        for sector_name, sector_data in self.data_fetcher.sectors_config.get('sectors', {}).items():
            sector_totals[sector_name] = len(sector_data.get('stocks', []))
        
        # Сортируем секторы по среднему комбинированному моментуму (самые растущие выше)
        sorted_sectors = []
        for sector, assets_list in sector_assets.items():
            if assets_list:
                avg_momentum = np.mean([a.combined_momentum for a in assets_list])
                avg_vs_benchmark = np.mean([a.absolute_momentum_6m - benchmark_momentum for a in assets_list])
                sorted_sectors.append({
                    'name': sector,
                    'assets': assets_list,
                    'avg_momentum': avg_momentum,
                    'avg_vs_benchmark': avg_vs_benchmark,
                    'total_in_sector': sector_totals.get(sector, len(assets_list))
                })
        
        # Сортируем секторы по среднему моментуму (убывание)
        sorted_sectors.sort(key=lambda x: x['avg_momentum'], reverse=True)
        
        # Эмодзи для секторов
        sector_emojis = {
            'Нефть и газ': '🛢️',
            'Финансы': '🏦',
            'Металлы и добыча': '⚙️',
            'Потребительские товары': '🛒',
            'Электроэнергетика': '⚡',
            'Прочие': '📦',
            'Фармацевтика и медицина': '💊',
            'Информационные технологии': '💻',
            'Индекс': '📈',
            'Другое': '📁'
        }
        
        # Формируем сообщение
        message = f"🎯 MOMENTUM ОБЗОР РОССИЙСКОГО РЫНКА\n"
        message += f"📅 {current_date} | 📈 Бенчмарк MCFTR: {benchmark_momentum:+.1f}% (6M)\n"
        message += "═══════════════════════════\n\n"
        
        # Выводим каждый сектор с топ-3 акциями
        for sector_info in sorted_sectors:
            sector = sector_info['name']
            emoji = sector_emojis.get(sector, '📊')
            selected_count = len(sector_info['assets'])
            total_in_sector = sector_info['total_in_sector']
            avg_momentum = sector_info['avg_momentum']
            avg_vs_benchmark = sector_info['avg_vs_benchmark']
            
            message += f"{emoji} {sector.upper()} ({selected_count}/{total_in_sector}, средний {avg_momentum:+.1f}% | vs бенч: {avg_vs_benchmark:+.1f}%):\n\n"
            
            for i, asset in enumerate(sector_info['assets'][:3], 1):
                vs_benchmark = asset.absolute_momentum_6m - benchmark_momentum
                status = "🟢 IN" if self.current_portfolio.get(asset.symbol, {}).get('status') == 'IN' else "⚪ OUT"
                
                message += f"{i}️⃣ {asset.symbol}: {asset.combined_momentum:+.1f}% | vs бенч: {vs_benchmark:+.1f}% | {asset.current_price:.2f}₽ {status}\n"
                message += f"   12M: {asset.momentum_12m:+.1f}% | 6M: {asset.absolute_momentum_6m:+.1f}% | 1M: {asset.momentum_1m:+.1f}%\n\n"
        
        # Подсчет активных позиций
        active_count = sum(1 for v in self.current_portfolio.values() if v.get('status') == 'IN')
        
        # Находим лучший сектор и самую сильную акцию
        best_sector = sorted_sectors[0] if sorted_sectors else None
        best_asset = max(assets, key=lambda x: x.combined_momentum) if assets else None
        
        message += "═══════════════════════════\n"
        message += f"🎯 Активно: {active_count} акций"
        if best_sector:
            message += f" | 📈 Лучший сектор: {best_sector['name']} ({best_sector['avg_momentum']:+.1f}%)"
        if best_asset:
            message += f"\n⚡ Самый сильный моментум: {best_asset.symbol} ({best_asset.combined_momentum:+.1f}%)"
        message += "\n═══════════════════════════\n\n"
        
        # Топ активов по секторам (топ-10)
        message += "🏆 ТОП АКТИВОВ ПО СЕКТОРАМ:\n\n"
        
        # Сортируем все активы по комбинированному моментуму
        top_assets = sorted(assets, key=lambda x: x.combined_momentum, reverse=True)[:10]
        
        for i, asset in enumerate(top_assets, 1):
            vs_benchmark = asset.absolute_momentum_6m - benchmark_momentum
            atr_percent = (asset.atr / asset.current_price * 100) if asset.atr > 0 and asset.current_price > 0 else 0.0
            
            message += f"{i}. {asset.symbol} ({asset.sector}): {asset.combined_momentum:+.2f}%\n"
            message += f"   12M: {asset.momentum_12m:+.1f}% | 6M: {asset.absolute_momentum_6m:+.1f}%"
            
            # Добавляем 1M моментум только если он значительный
            if abs(asset.momentum_1m) > 0.1:
                message += f" | 1M: {asset.momentum_1m:+.1f}%"
            
            message += f" | vs бенчмарк: {vs_benchmark:+.1f}%\n"
            
            # Добавляем ATR если есть
            if atr_percent > 0:
                message += f"   ATR: {atr_percent:.1f}%\n"
            
            message += "\n"
        
        return message
    
    def format_signal_message(self, signal: Dict) -> str:
        """Форматирование сигнала с информацией о стоп-лоссе"""
        if signal['action'] == 'BUY':
            atr_info = f"📊 ATR: {signal.get('atr', 0):.2f} руб\n"
            stop_loss_info = f"⛔ Стоп-лосс: **{signal.get('stop_loss', 0):.2f} руб**\n"
            
            return (
                f"🎯 *BUY: {signal['symbol']}*\n"
                f"═══════════════════════════\n"
                f"🏢 {signal.get('sector', 'Акция')}\n"
                f"💰 Цена: {signal['price']:.2f} руб\n"
                f"{atr_info}"
                f"{stop_loss_info}"
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
            
            stop_loss_hit = "⛔" if "стоп-лосс" in signal['reason'].lower() else ""
            
            return (
                f"🎯 *SELL: {signal['symbol']}* {stop_loss_hit}\n"
                f"═══════════════════════════\n"
                f"💰 Цена входа: {signal['entry_price']:.2f} руб\n"
                f"💰 Цена выхода: {signal['price']:.2f} руб\n"
                f"📊 Прибыль: **{signal['profit_percent']:+.2f}%** {profit_emoji}\n"
                f"📈 Абсолютный моментум: {signal['absolute_momentum']:+.1f}%\n"
                f"📈 Абсолютный моментум 6M: {signal.get('absolute_momentum_6m', 0):+.1f}%\n"
                f"📊 ATR: {signal.get('atr', 0):.2f} руб\n"
                f"⛔ Стоп-лосс: {signal.get('stop_loss', 0):.2f} руб\n"
                f"🕐 Время: {signal['timestamp'].strftime('%H:%M:%S %d.%m.%Y')}\n"
                f"═══════════════════════════\n"
                f"{signal['reason']}"
            )
    
    def format_ranking_message(self, assets: List[AssetData]) -> str:
        """Форматирование рейтинга по секторам с ATR"""
        benchmark_data = self.get_benchmark_data()
        
        message = f"📊 *MOMENTUM РЕЙТИНГ МОСБИРЖИ (Секторный отбор)*\n"
        message += f"Отбор: топ-3 акции в каждом секторе\n"
        
        if benchmark_data:
            message += f"📈 Бенчмарк ({self.benchmark_symbol}): {benchmark_data['absolute_momentum_6m']:+.1f}% (6M)\n"
        
        message += "═══════════════════════════\n"
        
        if not assets:
            message += "⚠️ *Нет активов, соответствующих критериям*\n"
            message += "═══════════════════════════\n"
            return message
        
        sector_assets = defaultdict(list)
        for asset in assets:
            sector_assets[asset.sector].append(asset)
        
        for sector, sector_stocks in sector_assets.items():
            message += f"🏢 *{sector}:*\n"
            
            sorted_stocks = sorted(sector_stocks, key=lambda x: x.combined_momentum, reverse=True)
            
            for i, asset in enumerate(sorted_stocks[:3], 1):
                status = "🟢 IN" if self.current_portfolio.get(asset.symbol, {}).get('status') == 'IN' else "⚪ OUT"
                
                benchmark_comparison = ""
                if benchmark_data:
                    vs_benchmark = asset.absolute_momentum_6m - benchmark_data['absolute_momentum_6m']
                    if vs_benchmark > 0:
                        benchmark_comparison = f" (+{vs_benchmark:.1f}% vs MCFTR)"
                    else:
                        benchmark_comparison = f" ({vs_benchmark:.1f}% vs MCFTR)"
                
                atr_info = f", ATR: {asset.atr/asset.current_price*100:.1f}%" if asset.atr > 0 else ""
                stop_loss_info = f"\n  ⛔ SL: {asset.stop_loss:.2f} руб" if asset.stop_loss > 0 else ""
                
                message += (
                    f"  #{i} {asset.symbol} {status}\n"
                    f"  💰 {asset.current_price:.2f} руб\n"
                    f"  📊 Моментум: {asset.combined_momentum:+.1f}%\n"
                    f"  📈 6M: {asset.absolute_momentum_6m:+.1f}%{benchmark_comparison}{atr_info}\n"
                    f"  📉 SMA: {'🟢' if asset.sma_signal else '🔴'}"
                    f"{stop_loss_info}\n"
                    f"  ─\n"
                )
            
            message += "\n"
        
        message += "═══════════════════════════\n"
        message += "*ПАРАМЕТРЫ СТРАТЕГИИ:*\n"
        message += f"• Анализ: акции из конфига sectors_config.json\n"
        message += f"• Отбор: топ-3 в каждом секторе\n"
        message += f"• Требование 12M моментум: > {self.min_12m_momentum}%\n"
        message += f"• Бенчмарк: {self.benchmark_symbol}\n"
        message += f"• SMA: {self.sma_fast_period}/{self.sma_slow_period} дней\n"
        message += f"• Веса: 12M({self.weights['12M']*100:.0f}%), 6M({self.weights['6M']*100:.0f}%), 1M({self.weights['1M']*100:.0f}%)\n"
        message += f"• Управление рисками: ATR({self.atr_period}) стоп-лосс x{self.atr_multiplier}\n"
        message += f"• Проверка: каждые {self.check_interval//3600} часа\n"
        message += f"• Оповещение: каждые 24 часа\n"
        
        active_count = sum(1 for v in self.current_portfolio.values() if v.get('status') == 'IN')
        if active_count > 0:
            message += f"• Активных позиций: {active_count}\n"
        
        return message
    
    def get_next_scheduled_time(self, target_times: List[str]) -> datetime:
        """
        Получение следующего запланированного времени для проверки
        target_times: список строк времени в формате "HH:MM" (GMT+3)
        """
        now = datetime.now()
        next_times = []
        
        for time_str in target_times:
            target_time = datetime.strptime(time_str, "%H:%M")
            # Создаем datetime на сегодня с указанным временем
            candidate = datetime(now.year, now.month, now.day, 
                               target_time.hour, target_time.minute)
            
            # Если время уже прошло сегодня, переносим на завтра
            if candidate < now:
                candidate += timedelta(days=1)
            
            next_times.append(candidate)
        
        # Возвращаем ближайшее время
        return min(next_times)
    
    def should_run_check_now(self) -> bool:
        """
        Проверка, нужно ли запускать проверку сейчас
        по расписанию 14:10 и 19:10 GMT+3
        """
        now = datetime.now()
        current_time_str = now.strftime("%H:%M")
        
        # Допуск ±5 минут для запуска
        for check_time in self.check_times:
            check_dt = datetime.strptime(check_time, "%H:%M")
            current_dt = datetime.strptime(current_time_str, "%H:%M")
            
            # Разница в минутах
            diff_minutes = abs((current_dt - check_dt).total_seconds() / 60)
            
            if diff_minutes <= 5:  # В пределах 5 минут от запланированного времени
                return True
        
        return False
    
    def should_send_report_now(self) -> bool:
        """
        Проверка, нужно ли отправлять отчет сейчас
        по расписанию 19:30 GMT+3
        """
        now = datetime.now()
        current_time_str = now.strftime("%H:%M")
        report_time = self.report_time
        
        # Допуск ±5 минут для отправки
        report_dt = datetime.strptime(report_time, "%H:%M")
        current_dt = datetime.strptime(current_time_str, "%H:%M")
        
        # Разница в минутах
        diff_minutes = abs((current_dt - report_dt).total_seconds() / 60)
        
        return diff_minutes <= 5  # В пределах 5 минут от запланированного времени
    
    def run_strategy_cycle(self, send_report: bool = False) -> bool:
        """Запуск цикла стратегии
        send_report: отправлять ли объединенный отчет
        """
        try:
            logger.info("🔄 Запуск цикла стратегии...")
            
            if self.errors_count > 3:
                self.clear_cache()
                logger.info("🔄 Кэш очищен из-за большого количества ошибок")
            
            assets = self.analyze_assets()
            
            if not assets:
                logger.warning("❌ Нет активов для анализа")
                
                if self.should_send_notification() or send_report:
                    benchmark_data = self.get_benchmark_data()
                    no_assets_msg = (
                        "📊 *Анализ Мосбиржи*\n"
                        "Нет активов, соответствующих критериям.\n\n"
                        f"• Акции из конфига: {self.top_assets_count}\n"
                        f"• Требование 12M моментум: > {self.min_12m_momentum}%\n"
                        f"• Требование SMA: положительный сигнал\n"
                        f"• Управление рисками: ATR стоп-лосс x{self.atr_multiplier}\n"
                    )
                    
                    if benchmark_data:
                        no_assets_msg += f"• Бенчмарк ({self.benchmark_symbol}): {benchmark_data['absolute_momentum_6m']:+.1f}%\n"
                    
                    no_assets_msg += "\nВозможно, рынок в нисходящем тренде."
                    
                    self.send_telegram_message(no_assets_msg, force=True)
                
                if self.should_send_notification() or send_report:
                    active_positions = self.format_active_positions()
                    if "АКТИВНЫХ ПОЗИЦИЙ НЕТ" not in active_positions:
                        self.send_telegram_message(active_positions, force=True)
                
                return False
            
            self.asset_ranking = assets
            
            signals = self.generate_signals(assets)
            
            for signal in signals:
                message = self.format_signal_message(signal)
                if self.send_telegram_message(message, force=True):
                    self.signal_history.append(signal)
                    logger.info(f"✅ Сигнал отправлен: {signal['symbol']} {signal['action']}")
            
            # Отправляем объединенный отчет если нужно
            if send_report and self.should_send_report_now():
                combined_report = self.format_combined_report(assets)
                self.send_telegram_message(combined_report)
                logger.info("📊 Объединенный отчет отправлен")
            
            logger.info(f"✅ Цикл завершен. Сигналов: {len(signals)}")
            return True
            
        except Exception as e:
            logger.error(f"❌ Ошибка в цикле: {e}")
            logger.error(traceback.format_exc())
            self.errors_count += 1
            
            error_msg = (
                f"❌ *ОШИБКА АНАЛИЗА*\n"
                f"Произошла ошибка при анализе активов:\n"
                f"```\n{str(e)[:200]}\n```\n"
                f"Ошибок подряд: {self.errors_count}"
            )
            self.send_telegram_message(error_msg, force=True)
            
            return False
    
    def save_state(self):
        """Сохранение состояния"""
        try:
            state = {
                'current_portfolio': self.current_portfolio,
                'signal_history': self.signal_history[-100:],
                'last_update': datetime.now().isoformat(),
                'last_notification_time': self.last_notification_time.isoformat() if self.last_notification_time else None,
                'errors_count': self.errors_count,
                'version': 'moex_bot_v7_sector_selection_atr_scheduled',
                'risk_params': {
                    'atr_period': self.atr_period,
                    'atr_multiplier': self.atr_multiplier,
                    'min_stop_loss_percent': self.min_stop_loss_percent,
                    'max_stop_loss_percent': self.max_stop_loss_percent
                }
            }
            
            with open('logs/bot_state_moex.json', 'w', encoding='utf-8') as f:
                json.dump(state, f, default=str, indent=2, ensure_ascii=False)
            
            logger.info("💾 Состояние сохранено")
        except Exception as e:
            logger.error(f"Ошибка сохранения: {e}")
    
    def run(self):
        """Основной цикл работы бота с расписанием"""
        logger.info("=" * 60)
        logger.info("🚀 ЗАПУСК MOMENTUM BOT ДЛЯ МОСБИРЖИ (Секторный отбор + ATR стоп-лосс)")
        logger.info(f"🕐 Расписание: проверки в {self.check_times[0]} и {self.check_times[1]}, отчет в {self.report_time} (GMT+3)")
        logger.info("=" * 60)
        
        self.load_state()
        
        logger.info("🔍 Проверка доступности MOEX API...")
        if not self.data_fetcher.test_moex_connection():
            logger.error("❌ MOEX API недоступен. Проверьте подключение к интернету.")
            if self.telegram_token and self.telegram_chat_id:
                self.send_telegram_message(
                    "❌ *MOEX API НЕДОСТУПЕН*\n"
                    "Проверьте подключение к интернету.\n"
                    "Бот остановлен.",
                    force=True
                )
            return
        else:
            logger.info("✅ MOEX API доступен")
        
        config_file = 'sectors_config.json'
        if not os.path.exists(config_file):
            logger.error(f"❌ Конфигурационный файл {config_file} не найден!")
            if self.telegram_token and self.telegram_chat_id:
                self.send_telegram_message(
                    f"❌ *КОНФИГУРАЦИОННЫЙ ФАЙЛ НЕ НАЙДЕН*\n"
                    f"Создайте файл {config_file} с секторами и акциями.\n"
                    f"Бот остановлен.",
                    force=True
                )
            return
        
        if self.telegram_token and self.telegram_chat_id:
            welcome_msg = (
                "🚀 *MOMENTUM BOT ДЛЯ МОСБИРЖИ ЗАПУЩЕН*\n"
                f"📊 Стратегия: Momentum с секторным отбором\n"
                f"🔢 Анализ: акции ТОЛЬКО из конфига sectors_config.json\n"
                f"📈 Бенчмарк: {self.benchmark_symbol} ({self.benchmark_name})\n"
                f"⚙️ Фильтры: 12M > {self.min_12m_momentum}%, SMA положительный\n"
                f"⚠️ Управление рисками: ATR({self.atr_period}) стоп-лосс x{self.atr_multiplier}\n"
                f"📡 Источник данных: {'apimoex' if HAS_APIMOEX else 'MOEX API'}\n"
                f"🕐 Расписание: проверки в {self.check_times[0]} и {self.check_times[1]}, отчет в {self.report_time} (GMT+3)\n"
                f"⏱️ Задержка между запросами: {self.analysis_request_delay} сек\n"
                f"⚡ Версия: секторный отбор с расписанием"
            )
            self.send_telegram_message(welcome_msg, force=True)
            
            active_positions_msg = self.format_active_positions()
            self.send_telegram_message(active_positions_msg, force=True)
            
            if not HAS_APIMOEX:
                apimoex_warning = (
                    "⚠️ *ВНИМАНИЕ: apimoex не установлен*\n"
                    "Бот работает в режиме совместимости с прямым API MOEX.\n"
                    "Для лучшей работы установите:\n"
                    "```bash\npip install apimoex\n```"
                )
                self.send_telegram_message(apimoex_warning, silent=True, force=True)
        else:
            logger.warning("⚠️ Telegram не настроен, пропускаем приветственное сообщение")
        
        iteration = 0
        
        try:
            while True:
                iteration += 1
                current_time = datetime.now().strftime('%H:%M:%S %d.%m.%Y')
                logger.info(f"🔄 Итерация #{iteration} - {current_time}")
                
                # Проверяем расписание
                send_report = self.should_send_report_now()
                should_check = self.should_run_check_now()
                
                if should_check or send_report:
                    logger.info(f"⏰ Время для {'проверки и отчета' if send_report else 'проверки'}")
                    success = self.run_strategy_cycle(send_report=send_report)
                    
                    if success:
                        logger.info(f"✅ Итерация #{iteration} успешно завершена")
                        
                        if iteration % 3 == 0:
                            self.save_state()
                    else:
                        logger.warning(f"⚠️ Итерация #{iteration} завершена с проблемами")
                else:
                    # Вычисляем время до следующей проверки
                    next_check_time = self.get_next_scheduled_time(self.check_times)
                    wait_seconds = (next_check_time - datetime.now()).total_seconds()
                    
                    if wait_seconds > 0:
                        logger.info(f"⏳ До следующей проверки в {next_check_time.strftime('%H:%M')}: {wait_seconds/60:.1f} минут")
                        time.sleep(min(wait_seconds, 300))  # Спим не больше 5 минут
                        continue
                
                if self.errors_count > 5:
                    logger.error(f"⚠️ Много ошибок ({self.errors_count}). Пауза 1 час...")
                    if self.telegram_token and self.telegram_chat_id:
                        self.send_telegram_message("⚠️ *МНОГО ОШИБОК* \nБот делает паузу 1 час", force=True)
                    time.sleep(3600)
                    self.errors_count = 0
                
                # Небольшая пауза между итерациями
                time.sleep(60)
                
        except KeyboardInterrupt:
            logger.info("🛑 Остановка по команде пользователя")
            self.save_state()
            if self.telegram_token and self.telegram_chat_id:
                self.send_telegram_message("🛑 *BOT ОСТАНОВЛЕН ПОЛЬЗОВАТЕЛЕМ*", force=True)
        
        except Exception as e:
            logger.error(f"❌ Критическая ошибка в основном цикле: {e}")
            logger.error(traceback.format_exc())
            self.errors_count += 1
            if self.telegram_token and self.telegram_chat_id:
                self.send_telegram_message(f"💥 *КРИТИЧЕСКАЯ ОШИБКА* \n{str(e)[:100]}", force=True)


def main():
    """Основная функция запуска бота"""
    bot = MomentumBotMOEX()
    
    try:
        bot.run()
    except Exception as e:
        logger.error(f"💀 Фатальная ошибка: {e}")
        logger.error(traceback.format_exc())
        if bot.telegram_token and bot.telegram_chat_id:
            bot.send_telegram_message(f"💀 *ФАТАЛЬНАЯ ОШИБКА* \nБот завершил работу: {str(e)[:200]}", force=True)


if __name__ == "__main__":
    main()
