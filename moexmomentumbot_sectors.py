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
import traceback  # Добавлено для детальной отладки

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
    atr: float = 0.0  # Добавлено: ATR для управления рисками
    stop_loss: float = 0.0  # Добавлено: уровень стоп-лосса
    atr_period: int = 14  # Добавлено: период для расчета ATR
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
    avg_atr_percent: float = 0.0  # Добавлено: средний ATR в %


class MOEXDataFetcher:
    """Класс для получения данных с Московской биржи С ИСПОЛЬЗОВАНИЕМ apimoex"""
    
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({'User-Agent': 'MomentumBotMOEX/1.0'})
        
        # Кэш для списка акций на 180 дней
        self.stocks_cache_file = 'logs/moex_stocks_cache.json'
        self.stocks_cache_ttl = 180 * 24 * 3600
        
        # Бенчмарк - индекс полной доходности MCFTR
        self.benchmark_symbol = 'MCFTR'
        
        # Загрузка конфигурации секторов
        self.sectors_config = self.load_sectors_config()
        
        logger.info(f"✅ MOEXDataFetcher инициализирован. apimoex доступен: {HAS_APIMOEX}")
        
        # Проверяем подключение к MOEX API
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
                
                # Логируем количество акций в каждом секторе
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
        Это ЗАМЕНА старого метода get_200_popular_stocks()
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
        
        # Логируем первые 10 акций для проверки
        for i, asset in enumerate(assets[:10]):
            logger.debug(f"  {i+1}. {asset['symbol']} - {asset['name']} ({asset['sector']})")
        
        return assets
    
    # СТАРЫЙ МЕТОД СОХРАНЕН ДЛЯ СОВМЕСТИМОСТИ
    def get_200_popular_stocks(self) -> List[Dict]:
        """
        Получение списка 200 популярных российских акций
        Кэшируется на 180 дней
        Сохранен для обратной совместимости, но НЕ ИСПОЛЬЗУЕТСЯ в новой логике
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
        Получение текущей цены БЕЗ ЗАПРОСА ОБЪЕМА
        ИСПРАВЛЕНО: преобразование строки в float
        """
        source = 'unknown'
        
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
                        
                        marketdata = data.get('marketdata', {}).get('data', [])
                        if marketdata:
                            row = marketdata[0]
                            columns = data.get('marketdata', {}).get('columns', [])
                            
                            price_idx = columns.index('LAST') if 'LAST' in columns else -1
                            
                            if price_idx != -1 and len(row) > price_idx:
                                price = row[price_idx]
                                
                                # ИСПРАВЛЕНО: преобразование строки в float
                                if price is not None:
                                    try:
                                        price_float = float(price)
                                        if price_float > 0:
                                            source = f'moex_api_{board_type}'
                                            logger.debug(f"✅ Найден {symbol} на {board_type}: {price_float}")
                                            return price_float, 0, source
                                    except (ValueError, TypeError) as e:
                                        logger.debug(f"Ошибка преобразования цены {symbol}: {price} -> {e}")
                                        continue
                except Exception as e:
                    logger.debug(f"Endpoint {board_type} для {symbol}: {e}")
                    continue
        
        except Exception as e:
            logger.error(f"❌ Ошибка получения цены для {symbol}: {e}")
            logger.error(traceback.format_exc())
        
        logger.warning(f"⚠️ Не удалось получить цену для {symbol}")
        return None, 0, source
    
    def get_historical_data(self, symbol: str, days: int = 400) -> Optional[pd.DataFrame]:
        """
        Получение исторических данных за указанное количество дней
        """
        try:
            end_date = datetime.now()
            start_date = end_date - timedelta(days=days)
            start_date_str = start_date.strftime('%Y-%m-%d')
            end_date_str = end_date.strftime('%Y-%m-%d')
            
            logger.debug(f"Запрос исторических данных для {symbol} с {start_date_str} по {end_date_str}")
            
            # Используем apimoex если доступен
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
                                
                                # ИСПРАВЛЕНО: убеждаемся, что цены в float
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
            
            # Резервный метод через прямое API
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
                            
                            # ИСПРАВЛЕНО: преобразование в float
                            for col in ['open', 'close', 'high', 'low']:
                                df[col] = pd.to_numeric(df[col], errors='coerce')
                            
                            logger.info(f"✅ Старый метод: получено {len(df)} свечей для {symbol}")
                            return df
                except Exception as e:
                    logger.debug(f"Старый метод для {symbol} ({market}/{board}): {e}")
                    continue
                    
        except Exception as e:
            logger.error(f"❌ Ошибка получения исторических данных для {symbol}: {e}")
            logger.error(traceback.format_exc())
        
        logger.warning(f"⚠️ Не удалось получить исторические данные для {symbol}")
        return None
    
    def calculate_atr(self, df: pd.DataFrame, period: int = 14) -> float:
        """
        Расчет Average True Range (ATR) для управления рисками
        Добавлено: метод для расчета волатильности
        """
        try:
            if df is None or len(df) < period:
                logger.warning(f"⚠️ Недостаточно данных для расчета ATR (нужно {period}, есть {len(df) if df else 0})")
                return 0.0
            
            # Копируем DataFrame чтобы не менять оригинал
            df_calc = df.copy()
            
            # Рассчитываем True Range
            df_calc['high_low'] = df_calc['high'] - df_calc['low']
            df_calc['high_close_prev'] = abs(df_calc['high'] - df_calc['close'].shift(1))
            df_calc['low_close_prev'] = abs(df_calc['low'] - df_calc['close'].shift(1))
            
            df_calc['true_range'] = df_calc[['high_low', 'high_close_prev', 'low_close_prev']].max(axis=1)
            
            # Расчет ATR (сглаженное скользящее среднее)
            atr = df_calc['true_range'].rolling(window=period).mean().iloc[-1]
            
            # Если ATR не рассчитался, используем простую волатильность
            if pd.isna(atr) or atr == 0:
                # Альтернативный расчет: стандартное отклонение цен закрытия
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
        
        # Проверяем наличие токена и chat_id
        if not self.telegram_token:
            logger.error("❌ TELEGRAM_TOKEN не найден в переменных окружения")
            logger.error("❌ Добавьте TELEGRAM_TOKEN=ваш_токен_бота в файл .env")
        
        if not self.telegram_chat_id:
            logger.error("❌ TELEGRAM_CHAT_ID не найден в переменных окружения")
            logger.error("❌ Добавьте TELEGRAM_CHAT_ID=ваш_chat_id в файл .env")
        
        # Инициализация фетчера данных
        self.data_fetcher = MOEXDataFetcher()
        
        # Параметры стратегии
        self.top_assets_count = 200  # Оставляем для совместимости
        self.selected_count = 10
        
        # Проверка каждые 4 часа
        self.check_interval = 4 * 3600
        
        # Время последнего оповещения
        self.last_notification_time = None
        self.notification_interval = 24 * 3600
        
        # Критерии фильтрации
        self.min_12m_momentum = 0.0
        
        # Веса для моментума
        self.weights = {'12M': 0.40, '6M': 0.35, '1M': 0.25}
        
        # Параметры SMA
        self.sma_fast_period = 10
        self.sma_slow_period = 30
        
        # Бенчмарк
        self.benchmark_symbol = 'MCFTR'
        self.benchmark_name = 'Индекс Мосбиржи полной доходности'
        
        # Параметры ATR и стоп-лосса
        self.atr_period = 14  # Период для расчета ATR
        self.atr_multiplier = 2.0  # Множитель для стоп-лосса (entry_price - 2 * ATR)
        self.min_stop_loss_percent = 5.0  # Минимальный стоп-лосс в %
        self.max_stop_loss_percent = 20.0  # Максимальный стоп-лосс в %
        
        # Текущий портфель
        self.current_portfolio: Dict[str, Dict] = {}
        self.signal_history: List[Dict] = []
        self.asset_ranking: List[AssetData] = []
        
        # Секторная производительность
        self.sector_performance: Dict[str, SectorPerformance] = {}
        
        # Кэши
        self._cache = {
            'top_assets': {'data': None, 'timestamp': None, 'ttl': 24*3600},
            'historical_data': {},
            'benchmark_data': {'data': None, 'timestamp': None, 'ttl': 3600},
            'stocks_list': {'data': None, 'timestamp': None, 'ttl': 180*24*3600}
        }
        
        # Статистика
        self.errors_count = 0
        self.max_retries = 3
        
        # Telegram
        self.telegram_retry_delay = 2
        self.max_telegram_retries = 3
        
        # Режим работы
        self.use_sector_selection = True
        self.test_mode = False
        
        logger.info("🚀 Momentum Bot для Московской биржи инициализирован")
        logger.info(f"📊 Параметры: Секторный отбор {self.top_assets_count} акций")
        logger.info(f"⚙️ Фильтры: 12M > {self.min_12m_momentum}%, SMA положительный")
        logger.info(f"📈 Источник данных: {'apimoex' if HAS_APIMOEX else 'MOEX API (apimoex недоступен)'}")
        logger.info(f"⏰ Проверка: каждые {self.check_interval//3600} часа, оповещение: каждые 24 часа")
        logger.info(f"📊 Бенчмарк: {self.benchmark_symbol} ({self.benchmark_name})")
        logger.info(f"🎯 Стратегия: {'Секторный отбор' if self.use_sector_selection else 'Топ-10 отбор'}")
        logger.info(f"⚠️ Управление рисками: ATR({self.atr_period}) стоп-лосс x{self.atr_multiplier}")
        
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
        Получение списка акций ТОЛЬКО из конфигурационного файла
        ИЗМЕНЕНО: используем только акции из sectors_config.json
        """
        cache = self._cache['stocks_list']
        
        # Проверяем кэш
        if cache['data'] and cache['timestamp']:
            cache_age = (datetime.now() - cache['timestamp']).total_seconds()
            if cache_age < cache['ttl']:
                logger.info(f"✅ Используем кэшированный список акций из конфига (возраст: {cache_age/86400:.1f} дней)")
                return cache['data']
        
        # Получаем новый список ИЗ КОНФИГА
        logger.info("📊 Получение списка акций из конфигурационного файла...")
        stocks_list = self.data_fetcher.get_assets_from_config()
        
        if not stocks_list:
            logger.error("❌ КРИТИЧЕСКАЯ ОШИБКА: Нет акций в конфигурационном файле")
            logger.error("❌ Проверьте файл sectors_config.json")
            raise Exception("Не удалось получить список акций из конфигурационного файла")
        
        # Сохраняем в кэш
        self._cache['stocks_list'] = {
            'data': stocks_list,
            'timestamp': datetime.now(),
            'ttl': 180*24*3600
        }
        
        logger.info(f"✅ Получено {len(stocks_list)} акций из конфига, сохранено в кэш на 180 дней")
        
        # Логируем статистику по секторам
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
        ИЗМЕНЕНО: используем только акции из конфига
        """
        try:
            # Проверяем кэш топ активов (24 часа)
            cache = self._cache['top_assets']
            if cache['data'] and cache['timestamp']:
                cache_age = (datetime.now() - cache['timestamp']).total_seconds()
                if cache_age < cache['ttl']:
                    logger.info(f"📊 Используем кэшированные топ активов (возраст: {cache_age/3600:.1f} часов)")
                    return cache['data']
            
            logger.info("📊 Формирование списка активов для анализа из конфига...")
            
            # Получаем список акций ИЗ КОНФИГА
            all_stocks = self.get_stocks_list()
            
            if not all_stocks:
                logger.error("❌ Нет данных об акциях в конфиге")
                return []
            
            all_assets = []
            filtered_assets = []
            
            # Обрабатываем акции
            for i, stock in enumerate(all_stocks, 1):
                symbol = stock['symbol']
                name = stock['name']
                
                try:
                    # Получаем текущую цену
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
                        'sector_data': stock.get('sector_data', {}),
                        'current_price': price,
                        'volume_24h': 0,
                        'source': source,
                        'market_type': 'stock'
                    })
                    
                    logger.debug(f"  ✅ {symbol}: {price:.2f} руб ({stock.get('sector', 'Другое')})")
                    
                    # Пауза чтобы не перегружать API
                    if i % 20 == 0:
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
                raise Exception("Не удалось получить данные по акциям")
            
            # Кэшируем
            self._cache['top_assets'] = {
                'data': all_assets,
                'timestamp': datetime.now(),
                'ttl': 24*3600
            }
            
            logger.info(f"✅ Сформирован список из {len(all_assets)} активов (включая бенчмарк)")
            
            return all_assets
            
        except Exception as e:
            logger.error(f"❌ Критическая ошибка получения топ активов: {e}")
            if self.telegram_token and self.telegram_chat_id:
                self.send_telegram_message(
                    f"❌ *КРИТИЧЕСКАЯ ОШИБКА*\n"
                    f"Не удалось получить данные акций:\n"
                    f"```{str(e)[:100]}```\n"
                    f"Бот остановлен.",
                    silent=False
                )
            raise
    
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
            min_required_days = 250
            if len(df) < min_required_days:
                logger.warning(f"⚠️ Мало исторических данных для {symbol}: {len(df)} дней (< {min_required_days})")
            
            self._cache['historical_data'][cache_key] = {
                'data': df,
                'timestamp': datetime.now(),
                'ttl': 3600
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
                'ttl': 3600
            }
            
            logger.info(f"✅ Данные бенчмарка: 6M моментум = {absolute_momentum_6m:.2f}%, 12M моментум = {absolute_momentum_12m:.2f}%")
            
            return benchmark_data
            
        except Exception as e:
            logger.error(f"❌ Ошибка получения данных бенчмарка: {e}")
            return None
    
    def calculate_momentum_values(self, asset_info: Dict) -> Optional[AssetData]:
        """
        Расчет значений моментума с использованием календарных дней
        ДОБАВЛЕНО: расчет ATR и стоп-лосса
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
            
            # ДОБАВЛЕНО: Расчет ATR и стоп-лосса
            atr = self.data_fetcher.calculate_atr(df, period=self.atr_period)
            
            # Расчет стоп-лосса на основе ATR
            stop_loss = 0.0
            atr_percent = 0.0
            
            if atr > 0 and current_price > 0:
                atr_percent = (atr / current_price) * 100
                
                # Базовая формула стоп-лосса: цена - множитель * ATR
                stop_loss_price = current_price - (self.atr_multiplier * atr)
                
                # Проверяем пределы стоп-лосса в процентах
                stop_loss_percent = ((current_price - stop_loss_price) / current_price) * 100
                
                # Ограничиваем стоп-лосс минимальным и максимальным значениями
                if stop_loss_percent < self.min_stop_loss_percent:
                    stop_loss_price = current_price * (1 - self.min_stop_loss_percent / 100)
                elif stop_loss_percent > self.max_stop_loss_percent:
                    stop_loss_price = current_price * (1 - self.max_stop_loss_percent / 100)
                
                stop_loss = max(stop_loss_price, 0.01)  # Минимум 0.01
                
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
        ИСПРАВЛЕНО: сектора теперь создаются для всех акций из конфига
        """
        top_assets = self.get_top_assets()
        if not top_assets:
            logger.error("❌ Нет активов для анализа")
            return []
        
        logger.info(f"📊 Анализ {len(top_assets)} активов из конфига...")
        
        benchmark_data = self.get_benchmark_data()
        
        # Инициализируем сектора ИЗ КОНФИГА
        sector_performance = {}
        
        # Создаем объекты SectorPerformance для ВСЕХ секторов из конфига
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
        
        # Добавляем сектор "Индекс" для бенчмарка
        sector_performance['Индекс'] = SectorPerformance(
            sector_name='Индекс',
            description='Индекс Мосбиржи полной доходности',
            priority=0,
            top_n=1
        )
        
        # Группируем акции по секторам
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
                    logger.warning("Нет данных бенчмарка, пропускаем сравнение")
                
                # Добавляем акцию в соответствующий сектор
                sector = asset_data.sector
                
                # ИСПРАВЛЕНО: создаем сектор если его нет
                if sector not in sector_performance:
                    logger.info(f"  📝 Создаем новый сектор: {sector}")
                    sector_performance[sector] = SectorPerformance(
                        sector_name=sector,
                        description='Автоматически созданный сектор',
                        priority=99,
                        top_n=1  # По умолчанию берем 1 акцию
                    )
                
                sector_assets[sector].append(asset_data)
                filter_stats['passed_all'] += 1
                logger.debug(f"  ✅ {symbol}: добавлен в сектор {sector}")
                
            except Exception as e:
                filter_stats['errors'] += 1
                logger.error(f"Ошибка анализа {symbol}: {e}")
                logger.error(traceback.format_exc())
                continue
        
        # ИСПРАВЛЕНО: отбираем топ-N акций из каждого сектора
        selected_assets = []
        
        for sector_name, assets in sector_assets.items():
            # Получаем или создаем объект SectorPerformance
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
                # Сортируем по комбинированному моментуму
                sorted_assets = sorted(assets, key=lambda x: x.combined_momentum, reverse=True)
                
                # Берем топ-N акций из сектора
                top_n = min(performance.top_n, len(sorted_assets))
                sector_selected = sorted_assets[:top_n]
                
                performance.selected_stocks = sector_selected
                performance.passed_filters = len(sector_selected)
                
                # Рассчитываем средние показатели сектора
                if sector_selected:
                    performance.avg_combined_momentum = np.mean([a.combined_momentum for a in sector_selected])
                    performance.avg_absolute_momentum_6m = np.mean([a.absolute_momentum_6m for a in sector_selected])
                    performance.avg_momentum_12m = np.mean([a.momentum_12m for a in sector_selected])
                    
                    # ДОБАВЛЕНО: средний ATR в процентах
                    atr_percents = []
                    for a in sector_selected:
                        if a.atr > 0 and a.current_price > 0:
                            atr_percent = (a.atr / a.current_price) * 100
                            atr_percents.append(atr_percent)
                    
                    if atr_percents:
                        performance.avg_atr_percent = np.mean(atr_percents)
                    
                    # Сравнение с бенчмарком
                    if benchmark_data:
                        performance.vs_benchmark = performance.avg_absolute_momentum_6m - benchmark_data['absolute_momentum_6m']
                    
                    # Оценочный балл сектора
                    performance.performance_score = performance.avg_combined_momentum * (100 - performance.priority) / 100
                
                selected_assets.extend(sector_selected)
                logger.info(f"  📊 {sector_name}: отобрано {len(sector_selected)}/{len(assets)} акций")
        
        # Сохраняем данные о производительности секторов
        self.sector_performance = sector_performance
        
        # Сортируем все выбранные акции по комбинированному моментуму
        selected_assets.sort(key=lambda x: x.combined_momentum, reverse=True)
        
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
        
        # Секторная статистика
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
        ДОБАВЛЕНО: проверка стоп-лосса
        """
        signals = []
        benchmark_data = self.get_benchmark_data()
        
        # Создаем словарь активов для быстрого доступа
        asset_dict = {asset.symbol: asset for asset in assets}
        
        # Создаем множество отобранных акций
        selected_symbols = {asset.symbol for asset in assets}
        
        for asset in assets:
            symbol = asset.symbol
            current_status = self.current_portfolio.get(symbol, {}).get('status', 'OUT')
            
            # BUY сигнал (только для отобранных акций)
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
                        
                        # ДОБАВЛЕНО: сохраняем стоп-лосс в портфель
                        self.current_portfolio[symbol] = {
                            'entry_time': datetime.now(),
                            'entry_price': asset.current_price,
                            'status': 'IN',
                            'name': asset.name,
                            'sector': asset.sector,
                            'source': asset.source,
                            'stop_loss': asset.stop_loss,  # Сохраняем стоп-лосс
                            'atr': asset.atr,  # Сохраняем ATR
                            'atr_percent': asset.atr / asset.current_price * 100 if asset.current_price > 0 else 0
                        }
                        
                        signals.append(signal)
                        logger.info(f"📈 BUY для {symbol} ({asset.name}, {asset.sector}), стоп-лосс: {asset.stop_loss:.2f}")
                    else:
                        # Портфель полен, ищем худшую позицию
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
                            
                            # Добавляем новую позицию со стоп-лоссом
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
            
            # SELL сигнал (только для акций в портфеле)
            elif current_status == 'IN':
                sell_reason = ""
                should_sell = False
                
                # ДОБАВЛЕНО: Условие 1: Проверка стоп-лосса
                if asset.stop_loss > 0 and asset.current_price <= asset.stop_loss:
                    sell_reason = f"Достигнут стоп-лосс ({asset.stop_loss:.2f})"
                    should_sell = True
                
                # Условие 2: Absolute Momentum 12M < 0%
                elif asset.absolute_momentum < 0:
                    sell_reason = "Моментум 12M < 0%"
                    should_sell = True
                
                # Условие 3: SMA отрицательный
                elif not asset.sma_signal:
                    sell_reason = "SMA отрицательный"
                    should_sell = True
                
                # Условие 4: Absolute Momentum 6M < Benchmark
                elif benchmark_data and asset.absolute_momentum_6m < benchmark_data['absolute_momentum_6m']:
                    sell_reason = f"6M моментум ({asset.absolute_momentum_6m:+.1f}%) < бенчмарка ({benchmark_data['absolute_momentum_6m']:+.1f}%)"
                    should_sell = True
                
                if should_sell:
                    entry_data = self.current_portfolio.get(symbol, {})
                    entry_price = entry_data.get('entry_price', asset.current_price)
                    profit_percent = ((asset.current_price - entry_price) / entry_price) * 100
                    
                    # ДОБАВЛЕНО: информация о стоп-лоссе в сигнал
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
        Отправка сообщения в Telegram
        """
        if not force and not self.should_send_notification() and not silent:
            logger.debug(f"⏰ Пропускаем оповещение (прошло менее 24 часов)")
            return False
        
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
                        self.last_notification_time = datetime.now()
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
        """Форматирование списка активных позиций с учетом стоп-лосса"""
        active_positions = {k: v for k, v in self.current_portfolio.items() 
                          if v.get('status') == 'IN'}
        
        if not active_positions:
            return "📊 *АКТИВНЫХ ПОЗИЦИЙ НЕТ*\nВсе средства в рублях"
        
        message = "📊 *АКТИВНЫЕ ПОЗИЦИИ:*\n"
        message += "═══════════════════════════\n"
        
        sector_positions = defaultdict(list)
        total_profit = 0
        position_count = 0
        
        for symbol, data in active_positions.items():
            entry_price = data.get('entry_price', 0)
            entry_time = data.get('entry_time', datetime.now())
            name = data.get('name', symbol)
            sector = data.get('sector', 'Другое')
            stop_loss = data.get('stop_loss', 0)
            atr_percent = data.get('atr_percent', 0)
            
            try:
                price, _, source = self.data_fetcher.get_current_price(symbol)
                if price and price > 0:
                    profit_percent = ((price - entry_price) / entry_price) * 100
                    
                    # ДОБАВЛЕНО: расчет расстояния до стоп-лосса
                    stop_loss_distance = 0
                    stop_loss_percent = 0
                    if stop_loss > 0:
                        stop_loss_distance = price - stop_loss
                        stop_loss_percent = (stop_loss_distance / price) * 100
                    
                    sector_positions[sector].append({
                        'symbol': symbol,
                        'name': name,
                        'entry_price': entry_price,
                        'current_price': price,
                        'profit_percent': profit_percent,
                        'entry_time': entry_time,
                        'stop_loss': stop_loss,
                        'stop_loss_distance': stop_loss_distance,
                        'stop_loss_percent': stop_loss_percent,
                        'atr_percent': atr_percent
                    })
                    
                    total_profit += profit_percent
                    position_count += 1
            except Exception as e:
                logger.error(f"Ошибка получения цены для {symbol}: {e}")
                continue
        
        # Выводим позиции по секторам
        for sector, positions in sector_positions.items():
            message += f"🏢 *{sector}:* {len(positions)} позиций\n"
            positions.sort(key=lambda x: x['profit_percent'], reverse=True)
            
            for pos in positions[:5]:
                profit_emoji = "📈" if pos['profit_percent'] > 0 else "📉"
                
                # ДОБАВЛЕНО: информация о стоп-лоссе
                stop_loss_info = ""
                if pos['stop_loss'] > 0:
                    if pos['stop_loss_percent'] > 0:
                        stop_loss_info = f"⛔ Стоп-лосс: {pos['stop_loss']:.2f} руб (-{pos['stop_loss_percent']:.1f}%)"
                    else:
                        stop_loss_info = f"⛔ Стоп-лосс достигнут!"
                
                message += (
                    f"• {pos['symbol']} ({pos['name'][:15]}): {pos['profit_percent']:+.2f}% {profit_emoji}\n"
                    f"  💰 Вход: {pos['entry_price']:.2f} руб\n"
                    f"  💰 Текущая: {pos['current_price']:.2f} руб\n"
                    f"  📊 ATR: {pos['atr_percent']:.1f}%\n"
                )
                
                if stop_loss_info:
                    message += f"  {stop_loss_info}\n"
                
                message += f"  ─\n"
            
            if len(positions) > 5:
                message += f"  ... и еще {len(positions) - 5} позиций\n"
            
            message += f"  ─\n"
        
        if position_count > 0:
            avg_profit = total_profit / position_count
            message += f"═══════════════════════════\n"
            message += f"📈 Средняя прибыль: {avg_profit:+.2f}%\n"
        
        message += f"🔢 Всего позиций: {len(active_positions)}\n"
        message += f"⚠️ Управление рисками: стоп-лосс на основе ATR x{self.atr_multiplier}"
        
        return message
    
    def format_sector_performance(self) -> str:
        """Форматирование эффективности секторов с ATR"""
        if not self.sector_performance:
            return "📊 *Нет данных о секторах*"
        
        benchmark_data = self.get_benchmark_data()
        benchmark_momentum = benchmark_data['absolute_momentum_6m'] if benchmark_data else 0
        
        message = "📊 *ЭФФЕКТИВНОСТЬ СЕКТОРОВ*\n"
        message += "═══════════════════════════\n"
        message += f"📈 Бенчмарк (MCFTR): {benchmark_momentum:+.1f}% (6M)\n"
        message += "═══════════════════════════\n"
        
        sorted_sectors = sorted(
            self.sector_performance.items(),
            key=lambda x: x[1].performance_score if x[1] else 0,
            reverse=True
        )
        
        for sector_name, performance in sorted_sectors:
            if performance and performance.selected_stocks:
                # ДОБАВЛЕНО: информация о ATR
                atr_info = f"📊 Средний ATR: {performance.avg_atr_percent:.1f}%\n" if performance.avg_atr_percent > 0 else ""
                
                message += (
                    f"🏢 *{sector_name}*\n"
                    f"📊 Средний комбинированный моментум: **{performance.avg_combined_momentum:+.1f}%**\n"
                    f"📈 Средний 6M моментум: {performance.avg_absolute_momentum_6m:+.1f}%\n"
                    f"{atr_info}"
                    f"🎯 Сравнение с бенчмарком: {performance.vs_benchmark:+.1f}%\n"
                    f"🔢 Акций отобрано: {len(performance.selected_stocks)}/{performance.total_stocks}\n"
                    f"🏆 Топ акции сектора:\n"
                )
                
                for i, asset in enumerate(performance.selected_stocks[:3], 1):
                    atr_asset = f", ATR: {asset.atr/asset.current_price*100:.1f}%" if asset.atr > 0 else ""
                    message += f"  {i}. {asset.symbol}: {asset.combined_momentum:+.1f}%{atr_asset}\n"
                
                message += f"──\n"
        
        total_selected = sum(len(p.selected_stocks) for p in self.sector_performance.values() if p)
        total_analyzed = sum(p.analyzed_stocks for p in self.sector_performance.values() if p)
        
        message += f"═══════════════════════════\n"
        message += f"📈 Всего отобрано акций: {total_selected}\n"
        message += f"📊 Всего проанализировано: {total_analyzed}\n"
        message += f"⚠️ Управление рисками: ATR({self.atr_period}) стоп-лосс x{self.atr_multiplier}"
        
        return message
    
    def format_signal_message(self, signal: Dict) -> str:
        """Форматирование сигнала с информацией о стоп-лоссе"""
        if signal['action'] == 'BUY':
            # ДОБАВЛЕНО: информация о ATR и стоп-лоссе
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
            
            # ДОБАВЛЕНО: информация о причине выхода
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
        
        # Выводим по секторам
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
                
                # ДОБАВЛЕНО: информация о ATR и стоп-лоссе
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
    
    def run_strategy_cycle(self) -> bool:
        """Запуск цикла стратегии"""
        try:
            logger.info("🔄 Запуск цикла стратегии...")
            
            if self.errors_count > 3:
                self.clear_cache()
                logger.info("🔄 Кэш очищен из-за большого количества ошибок")
            
            # Анализируем активы ТОЛЬКО из конфига
            assets = self.analyze_assets()
            
            if not assets:
                logger.warning("❌ Нет активов для анализа")
                
                if self.should_send_notification():
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
                
                if self.should_send_notification():
                    active_positions = self.format_active_positions()
                    if "АКТИВНЫХ ПОЗИЦИЙ НЕТ" not in active_positions:
                        self.send_telegram_message(active_positions, force=True)
                
                return False
            
            self.asset_ranking = assets
            
            # Генерируем сигналы с учетом стоп-лосса
            signals = self.generate_signals(assets)
            
            # Отправляем сигналы в Telegram
            for signal in signals:
                message = self.format_signal_message(signal)
                if self.send_telegram_message(message, force=True):
                    self.signal_history.append(signal)
                    logger.info(f"✅ Сигнал отправлен: {signal['symbol']} {signal['action']}")
            
            # Отправляем рейтинг
            if self.should_send_notification():
                ranking_message = self.format_ranking_message(assets)
                self.send_telegram_message(ranking_message, force=True)
                
                sector_performance_msg = self.format_sector_performance()
                self.send_telegram_message(sector_performance_msg, force=True)
            
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
                'version': 'moex_bot_v6_sector_selection_atr',
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
        """Основной цикл работы бота"""
        logger.info("=" * 60)
        logger.info("🚀 ЗАПУСК MOMENTUM BOT ДЛЯ МОСБИРЖИ (Секторный отбор + ATR стоп-лосс)")
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
                    "Бот остановлен.",
                    force=True
                )
            return
        else:
            logger.info("✅ MOEX API доступен")
        
        # Проверяем наличие конфига
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
        
        # Приветственное сообщение
        if self.telegram_token and self.telegram_chat_id:
            welcome_msg = (
                "🚀 *MOMENTUM BOT ДЛЯ МОСБИРЖИ ЗАПУЩЕН*\n"
                f"📊 Стратегия: Momentum с секторным отбором\n"
                f"🔢 Анализ: акции ТОЛЬКО из конфига sectors_config.json\n"
                f"📈 Бенчмарк: {self.benchmark_symbol} ({self.benchmark_name})\n"
                f"⚙️ Фильтры: 12M > {self.min_12m_momentum}%, SMA положительный\n"
                f"⚠️ Управление рисками: ATR({self.atr_period}) стоп-лосс x{self.atr_multiplier}\n"
                f"📡 Источник данных: {'apimoex' if HAS_APIMOEX else 'MOEX API'}\n"
                f"⏰ Проверка: каждые {self.check_interval//3600} часа\n"
                f"⏰ Оповещение: 1 раз в 24 часа\n"
                f"⚡ Версия: секторный отбор с ATR стоп-лоссом"
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
                logger.info(f"🔄 Цикл #{iteration} - {current_time}")
                
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
                        self.send_telegram_message("⚠️ *МНОГО ОШИБОК* \nБот делает паузу 1 час", force=True)
                    time.sleep(3600)
                    self.errors_count = 0
                
                logger.info(f"⏳ Следующая проверка через {self.check_interval//3600} часа(ов)...")
                time.sleep(self.check_interval)
                
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