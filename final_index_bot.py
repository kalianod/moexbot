#!/usr/bin/env python3
import asyncio
import logging
import pandas as pd
from telegram import Bot
from telegram.error import TelegramError
import schedule
import time
from datetime import datetime, timedelta
import os
import requests
from dotenv import load_dotenv
import json
import pytz
from typing import Dict, List, Optional, Tuple, Any
import hashlib
from pathlib import Path

# Загрузка переменных окружения
load_dotenv()

TELEGRAM_TOKEN = os.getenv('TELEGRAM_TOKEN')
TELEGRAM_CHAT_ID = os.getenv('TELEGRAM_CHAT_ID')

# Настройка расширенного логирования
LOG_DIR = Path("logs")
LOG_DIR.mkdir(exist_ok=True)

log_file = LOG_DIR / f"index_bot_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_file, encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Дополнительный логгер для детальных данных
detailed_logger = logging.getLogger('detailed')
detailed_handler = logging.FileHandler(LOG_DIR / 'detailed.log', encoding='utf-8')
detailed_handler.setFormatter(logging.Formatter('%(asctime)s - %(message)s'))
detailed_logger.addHandler(detailed_handler)
detailed_logger.setLevel(logging.INFO)

# Московский часовой пояс
MOSCOW_TZ = pytz.timezone('Europe/Moscow')

# Конфигурация индексов с их логикой
INDEX_CONFIG = {
    'IMOEX': {
        'name': 'Индекс МБ',
        'type': 'index',
        'logic': 'standard',  # Хедж на падение: закрыть при росте, открыть при падении
        'threshold': 0.005,  # 0.5%
        'cache_ttl': 300  # 5 минут
    },
    'MCFTR': {
        'name': 'MCFTR',
        'type': 'index',
        'logic': 'standard',
        'threshold': 0.005,
        'cache_ttl': 300
    },
    'CNYRUB_TOM': {
        'name': 'CNYRUB_TOM',
        'type': 'currency',
        'logic': 'inverse',  # Хедж на рост: закрыть при падении, открыть при росте
        'threshold': 0.005,
        'cache_ttl': 300
    },
    'GLDRUB_TOM': {
        'name': 'GLDRUB_TOM',
        'type': 'commodity',
        'logic': 'inverse',
        'threshold': 0.005,
        'cache_ttl': 300
    }
}

class DataCache:
    """Класс для кэширования данных с сохранением в JSON"""
    
    def __init__(self, cache_file: str = "cache.json"):
        self.cache_file = Path(cache_file)
        self.cache = {}
        self.timestamps = {}
        self.load_cache()
        
    def load_cache(self):
        """Загрузить кэш из файла JSON"""
        try:
            if self.cache_file.exists():
                with open(self.cache_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    self.cache = {}
                    for key, value in data.items():
                        # Восстанавливаем DataFrame из словаря
                        if isinstance(value, dict) and 'data' in value and 'timestamp' in value:
                            df_dict = value['data']
                            if df_dict:
                                # Проверяем наличие необходимых ключей для создания DataFrame
                                if 'index' in df_dict and 'columns' in df_dict and 'data' in df_dict:
                                    try:
                                        df = pd.DataFrame(
                                            df_dict['data'],
                                            columns=df_dict['columns'],
                                            index=pd.DatetimeIndex(df_dict['index'])
                                        )
                                        self.cache[key] = (df, datetime.fromisoformat(value['timestamp']))
                                    except Exception as e:
                                        logger.warning(f"Не удалось восстановить DataFrame из кэша для {key}: {e}")
                logger.info(f"✅ Кэш загружен из {self.cache_file}, {len(self.cache)} записей")
                detailed_logger.info(f"Кэш загружен из файла: {len(self.cache)} записей")
            else:
                logger.info("Файл кэша не найден, будет создан новый")
        except Exception as e:
            logger.error(f"❌ Ошибка загрузки кэша: {e}")
            
    def save_cache(self):
        """Сохранить кэш в файл JSON"""
        try:
            cache_data = {}
            for key, (data, timestamp) in self.cache.items():
                if isinstance(data, pd.DataFrame):
                    # Преобразуем DataFrame в словарь для сериализации
                    df_dict = {
                        'index': data.index.astype(str).tolist(),
                        'columns': data.columns.tolist(),
                        'data': data.values.tolist()
                    }
                    cache_data[key] = {
                        'data': df_dict,
                        'timestamp': timestamp.isoformat()
                    }
            
            with open(self.cache_file, 'w', encoding='utf-8') as f:
                json.dump(cache_data, f, ensure_ascii=False, indent=2)
            
            logger.info(f"✅ Кэш сохранен в {self.cache_file}, {len(cache_data)} записей")
            detailed_logger.info(f"Кэш сохранен в файл: {len(cache_data)} записей")
        except Exception as e:
            logger.error(f"❌ Ошибка сохранения кэша: {e}")
            
    def get(self, key: str) -> Optional[Tuple[pd.DataFrame, datetime]]:
        """Получить данные из кэша"""
        if key in self.cache:
            data, timestamp = self.cache[key]
            # Получаем TTL из конфигурации индекса
            index_key = key.split('_')[0]
            ttl = INDEX_CONFIG.get(index_key, {}).get('cache_ttl', 300)
            
            if (datetime.now() - timestamp).seconds < ttl:
                logger.debug(f"Кэш HIT для {key}")
                detailed_logger.info(f"Кэш HIT: {key}, возраст: {(datetime.now() - timestamp).seconds} сек")
                return data
            else:
                logger.debug(f"Кэш EXPIRED для {key}")
                detailed_logger.info(f"Кэш EXPIRED: {key}, возраст: {(datetime.now() - timestamp).seconds} сек")
        else:
            logger.debug(f"Кэш MISS для {key}")
            detailed_logger.info(f"Кэш MISS: {key}")
        return None
    
    def set(self, key: str, data: pd.DataFrame):
        """Сохранить данные в кэш"""
        try:
            self.cache[key] = (data, datetime.now())
            logger.debug(f"Данные закэшированы для {key}")
            detailed_logger.info(f"Кэш SET: {key}, размер данных: {len(data)} строк")
            
            # Автосохранение при каждом изменении
            self.save_cache()
        except Exception as e:
            logger.error(f"❌ Ошибка при кэшировании данных: {e}")
            detailed_logger.error(f"Ошибка кэширования: {key}, ошибка: {str(e)}")
        
    def clear(self):
        """Очистить кэш"""
        self.cache.clear()
        self.timestamps.clear()
        logger.info("Кэш очищен")
        detailed_logger.info("Кэш очищен")
        self.save_cache()

class SignalHistory:
    """Класс для хранения истории сигналов с сохранением в JSON"""
    
    def __init__(self, history_file: str = "history.json", max_history: int = 50):
        self.history_file = Path(history_file)
        self.max_history = max_history
        self.history = {}  # индекс -> список сигналов
        self.performance = {}  # индекс -> статистика
        self.load_history()
        
    def load_history(self):
        """Загрузить историю из файла JSON"""
        try:
            if self.history_file.exists():
                with open(self.history_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    self.history = data.get('history', {})
                    self.performance = data.get('performance', {})
                logger.info(f"✅ История загружена из {self.history_file}")
                detailed_logger.info(f"История загружена: {sum(len(v) for v in self.history.values())} сигналов")
            else:
                logger.info("Файл истории не найден, будет создан новый")
        except Exception as e:
            logger.error(f"❌ Ошибка загрузки истории: {e}")
            
    def save_history(self):
        """Сохранить историю в файл JSON"""
        try:
            history_data = {
                'history': self.history,
                'performance': self.performance,
                'last_update': datetime.now().isoformat()
            }
            
            with open(self.history_file, 'w', encoding='utf-8') as f:
                json.dump(history_data, f, ensure_ascii=False, indent=2, default=str)
            
            logger.info(f"✅ История сохранена в {self.history_file}")
            detailed_logger.info(f"История сохранена: {sum(len(v) for v in self.history.values())} сигналов")
        except Exception as e:
            logger.error(f"❌ Ошибка сохранения истории: {e}")
            
    def add_signal(self, index: str, signal: str, price: float, timestamp: datetime = None):
        """Добавить сигнал в историю"""
        if timestamp is None:
            timestamp = datetime.now()
            
        if index not in self.history:
            self.history[index] = []
            
        signal_record = {
            'timestamp': timestamp.isoformat(),
            'signal': signal,
            'price': price,
            'date': timestamp.strftime('%Y-%m-%d'),
            'time': timestamp.strftime('%H:%M:%S')
        }
        
        self.history[index].append(signal_record)
        
        # Ограничиваем размер истории
        if len(self.history[index]) > self.max_history:
            self.history[index] = self.history[index][-self.max_history:]
            
        logger.debug(f"Сигнал добавлен в историю: {index} - {signal} по {price}")
        detailed_logger.info(f"Сигнал добавлен: {index} - {signal} по {price} в {timestamp}")
        
        # Автосохранение
        self.save_history()
        
    def get_recent_signals(self, index: str, limit: int = 5) -> List[Dict]:
        """Получить последние сигналы для индекса"""
        if index in self.history:
            signals = self.history[index][-limit:]
            # Конвертируем строки обратно в datetime для совместимости
            for signal in signals:
                if 'timestamp' in signal and isinstance(signal['timestamp'], str):
                    signal['timestamp'] = datetime.fromisoformat(signal['timestamp'])
            return signals
        return []
    
    def get_signals_for_date(self, index: str, date_str: str) -> List[Dict]:
        """Получить сигналы за определенную дату"""
        if index in self.history:
            return [s for s in self.history[index] if s['date'] == date_str]
        return []
    
    def get_all_signals(self) -> Dict[str, List[Dict]]:
        """Получить всю историю сигналов"""
        return self.history
    
    def update_performance(self, index: str, signal_type: str, result: str):
        """Обновить статистику эффективности сигналов"""
        if index not in self.performance:
            self.performance[index] = {
                'total_signals': 0,
                'successful_signals': 0,
                'signal_types': {}
            }
            
        self.performance[index]['total_signals'] += 1
        
        if signal_type not in self.performance[index]['signal_types']:
            self.performance[index]['signal_types'][signal_type] = {
                'total': 0,
                'successful': 0
            }
            
        self.performance[index]['signal_types'][signal_type]['total'] += 1
        
        if result == 'success':
            self.performance[index]['successful_signals'] += 1
            self.performance[index]['signal_types'][signal_type]['successful'] += 1
            
        # Автосохранение
        self.save_history()

class MoexIndexAPI:
    """Класс для получения данных индексов MOEX"""
    
    def __init__(self):
        self.base_url = "https://iss.moex.com/iss"
        self.session = requests.Session()
        self.cache = DataCache()
        logger.info("✅ Инициализирован MoexIndexAPI с кэшированием в JSON")
        detailed_logger.info("API MOEX инициализирован")
        
    def get_index_candles_simple(self, index: str = 'IMOEX', days: int = 10):
        """Упрощенный метод получения свечных данных"""
        # Проверяем кэш
        cache_key = f"{index}_candles_{days}"
        cached_data = self.cache.get(cache_key)
        if cached_data is not None:
            detailed_logger.info(f"Используем кэшированные данные для {index} за {days} дней")
            return cached_data
            
        try:
            logger.info(f"🔄 Запрос данных для {index} за {days} дней")
            detailed_logger.info(f"Запрос API MOEX: {index}, дней: {days}")
            
            # Определяем URL в зависимости от типа индекса
            if index in ['IMOEX', 'MCFTR']:
                # Для индексов
                url = f"{self.base_url}/engines/stock/markets/index/boards/SNDX/securities/{index}/candles.json"
                detailed_logger.info(f"URL для индекса {index}: {url}")
            elif index in ['CNYRUB_TOM', 'GLDRUB_TOM']:
                # Для валютных пар
                url = f"{self.base_url}/engines/currency/markets/selt/boards/CETS/securities/{index}/candles.json"
                detailed_logger.info(f"URL для валютной пары {index}: {url}")
            else:
                logger.error(f"❌ Неизвестный индекс: {index}")
                detailed_logger.error(f"Неизвестный индекс: {index}")
                return None
            
            start_date = (datetime.now() - timedelta(days=days)).strftime('%Y-%m-%d')
            end_date = datetime.now().strftime('%Y-%m-%d')
            
            params = {
                'from': start_date,
                'till': end_date,
                'interval': 24,
                'iss.meta': 'off'
            }
            
            detailed_logger.info(f"Параметры запроса: {params}")
            
            response = self.session.get(url, params=params, timeout=30)
            detailed_logger.info(f"Ответ API: статус {response.status_code}")
            
            if response.status_code == 200:
                data = response.json()
                
                if 'candles' in data and 'data' in data['candles']:
                    candles_data = data['candles']['data']
                    
                    if candles_data:
                        df = pd.DataFrame(candles_data, columns=[
                            'open', 'close', 'high', 'low', 'value', 'volume', 'begin', 'end'
                        ])
                        
                        # Конвертируем даты
                        df['date'] = pd.to_datetime(df['begin'])
                        df.set_index('date', inplace=True)
                        df = df.sort_index()
                        
                        # Сохраняем в кэш
                        self.cache.set(cache_key, df)
                        
                        logger.info(f"✅ Упрощенный метод: {len(df)} свечей для {index}")
                        detailed_logger.info(f"Получено {len(df)} свечей для {index} с {start_date} по {end_date}")
                        detailed_logger.info(f"Диапазон цен: {df['close'].min():.2f} - {df['close'].max():.2f}")
                        
                        return df
                    else:
                        logger.warning(f"⚠️ Нет данных свечей для {index}")
                        detailed_logger.warning(f"Нет данных свечей в ответе API для {index}")
                else:
                    logger.warning(f"⚠️ Неожиданная структура данных для {index}")
                    detailed_logger.warning(f"Неожиданная структура данных: {list(data.keys())}")
            else:
                logger.error(f"❌ HTTP ошибка {response.status_code} для {index}")
                detailed_logger.error(f"HTTP ошибка {response.status_code}: {response.text[:200]}")
                    
            return None
                
        except requests.exceptions.Timeout:
            logger.error(f"❌ Таймаут запроса для {index}")
            detailed_logger.error(f"Таймаут запроса API для {index}")
            return None
        except requests.exceptions.ConnectionError:
            logger.error(f"❌ Ошибка соединения для {index}")
            detailed_logger.error(f"Ошибка соединения с API для {index}")
            return None
        except Exception as e:
            logger.error(f"❌ Ошибка упрощенного метода {index}: {e}")
            detailed_logger.error(f"Ошибка получения данных для {index}: {str(e)}")
            return None
    
    def get_index_current(self, index: str = 'IMOEX'):
        """Получение текущего значения индекса"""
        try:
            logger.info(f"🔄 Запрос текущего значения для {index}")
            detailed_logger.info(f"Запрос текущего значения: {index}")
            
            # Определяем URL в зависимости от типа индекса
            if index in ['IMOEX', 'MCFTR']:
                url = f"{self.base_url}/engines/stock/markets/index/boards/SNDX/securities/{index}.json"
            elif index in ['CNYRUB_TOM', 'GLDRUB_TOM']:
                url = f"{self.base_url}/engines/currency/markets/selt/boards/CETS/securities/{index}.json"
            else:
                logger.error(f"❌ Неизвестный индекс: {index}")
                detailed_logger.error(f"Неизвестный индекс: {index}")
                return None
                
            params = {'iss.meta': 'off'}
            
            response = self.session.get(url, params=params, timeout=30)
            detailed_logger.info(f"Ответ API текущего значения: статус {response.status_code}")
            
            if response.status_code == 200:
                data = response.json()
                
                # Ищем в marketdata
                if 'marketdata' in data and 'data' in data['marketdata']:
                    marketdata = data['marketdata']['data']
                    if marketdata:
                        # В marketdata данные представлены как список значений
                        # LAST - последнее значение находится на позиции 12 (индекс 12) для валют, 2 для индексов
                        if index in ['CNYRUB_TOM', 'GLDRUB_TOM']:
                            current_value = marketdata[0][12]  # LAST для валют
                        else:
                            current_value = marketdata[0][2]  # LAST для индексов
                        
                        logger.info(f"✅ Текущее значение {index}: {current_value}")
                        detailed_logger.info(f"Текущее значение {index}: {current_value}")
                        return current_value
                
                logger.warning(f"⚠️ Не удалось найти текущее значение для {index}")
                detailed_logger.warning(f"Не найдено текущее значение в marketdata для {index}")
                return None
            else:
                logger.error(f"❌ HTTP ошибка {response.status_code} для {index}")
                detailed_logger.error(f"HTTP ошибка текущего значения {response.status_code}: {response.text[:200]}")
                return None
                
        except Exception as e:
            logger.error(f"❌ Ошибка получения текущего значения {index}: {e}")
            detailed_logger.error(f"Ошибка получения текущего значения {index}: {str(e)}")
            return None
    
    def get_index_data_reliable(self, index: str = 'IMOEX', days: int = 5):
        """Надежный метод получения данных индекса"""
        detailed_logger.info(f"Надежный метод получения данных: {index}, дней: {days}")
        
        # Пробуем упрощенный метод свечей
        df = self.get_index_candles_simple(index, days)
        if df is not None and len(df) >= 2:
            detailed_logger.info(f"Данные получены упрощенным методом: {len(df)} строк")
            return df
        
        # Если API недоступно, не создаем тестовые данные, а пишем ошибку
        error_msg = f"❌ API MOEX недоступно для получения данных по {index}"
        logger.error(error_msg)
        detailed_logger.error(f"API MOEX недоступно для {index}")
        
        # Отправляем уведомление об ошибке
        return None

class FinalIndexBot:
    def __init__(self, telegram_token, chat_id):
        self.telegram_token = telegram_token
        self.chat_id = chat_id
        
        logger.info("🚀 Инициализация FinalIndexBot")
        detailed_logger.info("Инициализация FinalIndexBot")
        
        # Индексы для отслеживания
        self.indexes = ['IMOEX', 'MCFTR', 'CNYRUB_TOM', 'GLDRUB_TOM']
        detailed_logger.info(f"Отслеживаемые индексы: {self.indexes}")
        
        self.api = MoexIndexAPI()
        self.bot = Bot(token=telegram_token)
        self.history = SignalHistory()
        
        # Состояния для каждого индекса
        self.states = {
            index: {
                'current_signal': None,
                'last_price': None,
                'last_update': None,
                'signal_count': 0,
                'last_signal_time': None
            } for index in self.indexes
        }
        
        self.stats = {
            'total_checks': 0,
            'signals_found': 0,
            'critical_movements': 0,
            'last_check': None,
            'start_time': datetime.now(),
            'errors': 0,
            'successful_checks': 0
        }
        
        # Загружаем предыдущие состояния
        self.load_states()
        
        logger.info("✅ FinalIndexBot инициализирован")
        detailed_logger.info(f"FinalIndexBot инициализирован, стартовое время: {self.stats['start_time']}")
    
    def load_states(self):
        """Загрузить состояния из файла"""
        try:
            states_file = Path("bot_states.json")
            if states_file.exists():
                with open(states_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    for index in self.indexes:
                        if index in data.get('states', {}):
                            self.states[index] = data['states'][index]
                            # Конвертируем строки в datetime
                            for time_key in ['last_update', 'last_signal_time']:
                                if self.states[index][time_key] and isinstance(self.states[index][time_key], str):
                                    self.states[index][time_key] = datetime.fromisoformat(self.states[index][time_key])
                    
                    self.stats.update(data.get('stats', {}))
                    # Конвертируем строки в datetime
                    for time_key in ['last_check', 'start_time']:
                        if self.stats[time_key] and isinstance(self.stats[time_key], str):
                            self.stats[time_key] = datetime.fromisoformat(self.stats[time_key])
                
                logger.info("✅ Состояния бота загружены из файла")
                detailed_logger.info("Состояния бота загружены из файла")
        except Exception as e:
            logger.error(f"❌ Ошибка загрузки состояний: {e}")
            detailed_logger.error(f"Ошибка загрузки состояний: {str(e)}")
    
    def save_states(self):
        """Сохранить состояния в файл"""
        try:
            states_file = Path("bot_states.json")
            # Конвертируем datetime в строки для JSON
            states_to_save = {}
            for index, state in self.states.items():
                states_to_save[index] = state.copy()
                for time_key in ['last_update', 'last_signal_time']:
                    if states_to_save[index][time_key] and isinstance(states_to_save[index][time_key], datetime):
                        states_to_save[index][time_key] = states_to_save[index][time_key].isoformat()
            
            stats_to_save = self.stats.copy()
            for time_key in ['last_check', 'start_time']:
                if stats_to_save[time_key] and isinstance(stats_to_save[time_key], datetime):
                    stats_to_save[time_key] = stats_to_save[time_key].isoformat()
            
            data = {
                'states': states_to_save,
                'stats': stats_to_save,
                'last_save': datetime.now().isoformat()
            }
            
            with open(states_file, 'w', encoding='utf-8') as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
            
            logger.debug("Состояния бота сохранены")
            detailed_logger.info("Состояния бота сохранены в файл")
        except Exception as e:
            logger.error(f"❌ Ошибка сохранения состояний: {e}")
            detailed_logger.error(f"Ошибка сохранения состояний: {str(e)}")
    
    async def send_message(self, text):
        """Отправка сообщения в Telegram"""
        try:
            await self.bot.send_message(chat_id=self.chat_id, text=text, parse_mode='Markdown')
            logger.info("✅ Сообщение отправлено в Telegram")
            detailed_logger.info(f"Сообщение отправлено в Telegram: {text[:100]}...")
            return True
        except TelegramError as e:
            logger.error(f"❌ Ошибка отправки: {e}")
            detailed_logger.error(f"Ошибка отправки в Telegram: {str(e)}")
            self.stats['errors'] += 1
            return False
    
    def get_index_data(self, index):
        """Получение данных индекса"""
        detailed_logger.info(f"Получение данных для индекса: {index}")
        return self.api.get_index_data_reliable(index, days=5)
    
    def calculate_hedge_signal(self, df, index):
        """Расчет сигнала хеджирования для индекса"""
        if df is None or len(df) < 2:
            logger.warning(f"⚠️ Недостаточно данных для {index}")
            detailed_logger.warning(f"Недостаточно данных для расчета сигнала: {index}")
            return "Данные не получены", None
            
        current_candle = df.iloc[-1]
        prev_candle = df.iloc[-2]
        
        current_close = current_candle['close']
        prev_high = prev_candle['high']
        prev_low = prev_candle['low']
        
        detailed_logger.info(f"Расчет сигнала для {index}: цена={current_close:.2f}, prev_high={prev_high:.2f}, prev_low={prev_low:.2f}")
        
        # Получаем конфигурацию индекса
        index_config = INDEX_CONFIG.get(index, {})
        logic_type = index_config.get('logic', 'standard')
        threshold = index_config.get('threshold', 0.005)
        
        signal = None
        
        if logic_type == 'standard':
            # Стандартная логика (для IMOEX, MCFTR)
            # Закрываем хедж при росте, открываем при падении
            buy_threshold = prev_high * (1 + threshold)
            sell_threshold = prev_low * (1 - threshold)
            
            detailed_logger.info(f"Стандартная логика: buy_threshold={buy_threshold:.2f}, sell_threshold={sell_threshold:.2f}")
            
            if current_close > buy_threshold:
                signal = "Закрываем хэдж"
                logger.info(f"🎯 {index}: Закрываем хедж: {current_close:.2f} > {buy_threshold:.2f}")
                detailed_logger.info(f"СИГНАЛ {index}: Закрываем хедж, превышение на {(current_close/buy_threshold-1)*100:.2f}%")
            elif current_close < sell_threshold:
                signal = "Открываем хэдж"
                logger.info(f"🎯 {index}: Открываем хедж: {current_close:.2f} < {sell_threshold:.2f}")
                detailed_logger.info(f"СИГНАЛ {index}: Открываем хедж, падение на {(1-current_close/sell_threshold)*100:.2f}%")
                
        elif logic_type == 'inverse':
            # Обратная логика (для CNYRUB_TOM, GLDRUB_TOM)
            # Закрываем хедж при падении, открываем при росте
            buy_threshold = prev_low * (1 + threshold)
            sell_threshold = prev_high * (1 - threshold)
            
            detailed_logger.info(f"Обратная логика: buy_threshold={buy_threshold:.2f}, sell_threshold={sell_threshold:.2f}")
            
            if current_close > buy_threshold:
                signal = "Открываем хэдж"
                logger.info(f"🎯 {index}: Открываем хедж: {current_close:.2f} > {buy_threshold:.2f}")
                detailed_logger.info(f"СИГНАЛ {index}: Открываем хедж, рост на {(current_close/buy_threshold-1)*100:.2f}%")
            elif current_close < sell_threshold:
                signal = "Закрываем хэдж"
                logger.info(f"🎯 {index}: Закрываем хедж: {current_close:.2f} < {sell_threshold:.2f}")
                detailed_logger.info(f"СИГНАЛ {index}: Закрываем хедж, падение на {(1-current_close/sell_threshold)*100:.2f}%")
        
        if signal is None:
            signal = "Сигнал не сформировался"
            logger.info(f"📊 {index}: Сигнал на хеджирование не сформировался")
            detailed_logger.info(f"СИГНАЛ {index}: Не сформировался, цена в пределах диапазона")
        
        return signal, current_close
    
    def check_critical_movement(self, index: str, current_price: float, prev_price: float = None) -> Tuple[bool, str]:
        """Проверка критического движения цены"""
        if prev_price is None or prev_price == 0:
            return False, ""
            
        change_percent = abs((current_price - prev_price) / prev_price)
        change_abs = abs(current_price - prev_price)
        
        detailed_logger.info(f"Проверка критического движения {index}: {prev_price:.2f} -> {current_price:.2f}, изменение: {change_percent*100:.2f}%")
        
        if change_percent > 0.02:  # 2%
            direction = "рост" if current_price > prev_price else "падение"
            return True, f"📈 Критическое движение {index}: {direction} на {change_percent*100:.1f}% ({change_abs:.2f})"
        
        return False, ""
    
    async def send_critical_alert(self, index: str, message: str):
        """Отправка уведомления о критическом движении"""
        alert_message = (
            f"🚨 *КРИТИЧЕСКОЕ ДВИЖЕНИЕ* 🚨\n\n"
            f"{message}\n"
            f"Время: {datetime.now(MOSCOW_TZ).strftime('%Y-%m-%d %H:%M')}\n"
            f"Рекомендуется проверить позиции!"
        )
        
        sent = await self.send_message(alert_message)
        if sent:
            self.stats['critical_movements'] += 1
            logger.info(f"🚨 Отправлено уведомление о критическом движении для {index}")
            detailed_logger.info(f"Критическое уведомление отправлено: {index}")
    
    def format_signal_table(self, signals_data: List[Dict]) -> str:
        """Форматирование таблицы сигналов с ровными колонками"""
        # Определяем максимальные длины для выравнивания
        max_name_len = max(len(data['name']) for data in signals_data)
        max_name_len = min(max_name_len, 25)  # Ограничиваем максимальную длину
        
        table_lines = []
        table_lines.append("════════ СИГНАЛЫ ИНДЕКСОВ ════════")
        table_lines.append(f"{'ИНДЕКС':<{max_name_len}} {'ЗНАЧЕНИЕ':>10} {'СИГНАЛ':>12} {'ИЗМЕНЕНИЕ':>10}")
        
        for data in signals_data:
            index = data['index']
            name = data['name']
            price = data['price']
            signal = data['signal']
            
            # Обрезаем имя если слишком длинное
            if len(name) > max_name_len:
                display_name = name[:max_name_len-3] + "..."
            else:
                display_name = name
            
            # Определяем иконку сигнала
            if "Открываем" in signal:
                signal_icon = "🟢 ХЕДЖ"
            elif "Закрываем" in signal:
                signal_icon = "🔴 ХЕДЖ"
            else:
                signal_icon = "⚪ НЕТ СИГ"
            
            # Расчет изменения (упрощенный - можно добавить реальный расчет)
            change = "0.0%"
            
            # Форматируем строку с выравниванием
            line = f"{display_name:<{max_name_len}} {price:>10.2f} {signal_icon:>12} {change:>10}"
            table_lines.append(line)
        
        table_lines.append("═══════════════════════════════════")
        
        # Подсчет сигналов
        active_signals = sum(1 for d in signals_data if "хэдж" in d['signal'].lower())
        table_lines.append(f"Сводка: {active_signals} сигнала из {len(signals_data)} индексов")
        table_lines.append(f"Время: {datetime.now(MOSCOW_TZ).strftime('%H:%M, %d.%m.%Y')}")
        
        return "\n".join(table_lines)
    
    def format_history_block(self, index: str) -> str:
        """Форматирование блока истории для индекса"""
        history_records = self.history.get_recent_signals(index, limit=5)
        
        if not history_records:
            return ""
        
        history_lines = []
        index_name = INDEX_CONFIG.get(index, {}).get('name', index)
        
        # Обрезаем длинное имя для заголовка
        if len(index_name) > 20:
            display_name = index_name[:17] + "..."
        else:
            display_name = index_name
        
        history_lines.append(f"┌─────────────────────────────────────┐")
        history_lines.append(f"│         ИСТОРИЯ СИГНАЛОВ {display_name:<20} │")
        history_lines.append(f"├─────────────────────────────────────┤")
        
        for record in history_records:
            # Убедимся, что timestamp - это datetime
            timestamp = record['timestamp']
            if isinstance(timestamp, str):
                timestamp = datetime.fromisoformat(timestamp)
            
            date_str = timestamp.strftime('%d.%m')
            price = record['price']
            signal = record['signal']
            
            if "Открываем" in signal:
                signal_icon = "🟢 ОТКРЫТЬ ХЕДЖ"
            elif "Закрываем" in signal:
                signal_icon = "🔴 ЗАКРЫТЬ ХЕДЖ"
            else:
                signal_icon = "⚪ Без сигнала"
            
            history_lines.append(f"│ {date_str} | {price:8.2f} | {signal_icon:16} │")
        
        # Добавляем текущий день
        today_str = datetime.now().strftime('%d.%m')
        today_price = None
        today_signal = None
        
        # Ищем сегодняшние данные в кэше
        cache_key = f"{index}_candles_5"
        cached_data = self.api.cache.get(cache_key)
        if cached_data is not None:
            today_price = cached_data.iloc[-1]['close'] if len(cached_data) > 0 else None
            today_signal, _ = self.calculate_hedge_signal(cached_data, index)
        
        if today_price:
            if "Открываем" in today_signal:
                today_icon = "🟢 ОТКРЫТЬ ХЕДЖ"
            elif "Закрываем" in today_signal:
                today_icon = "🔴 ЗАКРЫТЬ ХЕДЖ"
            else:
                today_icon = "⚪ Без сигнала"
            
            history_lines.append(f"├─────────────────────────────────────┤")
            history_lines.append(f"│ СЕГОДНЯ ({today_str}):                    │")
            history_lines.append(f"│ 💰 {today_price:8.2f} | {today_icon:16} │")
            
            # Расчет изменения (упрощенный)
            if len(history_records) > 0:
                prev_price = history_records[-1]['price']
                change_percent = ((today_price - prev_price) / prev_price) * 100
                history_lines.append(f"│ 📈 Изменение: {change_percent:+.1f}%           │")
        
        history_lines.append(f"└─────────────────────────────────────┘")
        
        return "\n".join(history_lines)
    
    async def check_all_signals(self):
        """Проверка сигналов для всех индексов"""
        self.stats['total_checks'] += 1
        self.stats['last_check'] = datetime.now()
        
        detailed_logger.info(f"=== НАЧАЛО ПРОВЕРКИ СИГНАЛОВ #{self.stats['total_checks']} ===")
        detailed_logger.info(f"Время начала: {self.stats['last_check']}")
        
        try:
            logger.info(f"🔍 Проверка сигналов индексов (проверка #{self.stats['total_checks']})...")
            
            signals_data = []
            status_messages = []
            critical_alerts = []
            
            for index in self.indexes:
                detailed_logger.info(f"--- Обработка индекса: {index} ---")
                
                df = self.get_index_data(index)
                if df is not None and len(df) >= 2:
                    signal, current_price = self.calculate_hedge_signal(df, index)
                    
                    # Сохраняем данные для таблицы
                    signals_data.append({
                        'index': index,
                        'name': INDEX_CONFIG.get(index, {}).get('name', index),
                        'price': current_price,
                        'signal': signal
                    })
                    
                    # Проверяем критическое движение
                    prev_state = self.states[index]
                    if prev_state['last_price'] is not None:
                        is_critical, alert_msg = self.check_critical_movement(
                            index, current_price, prev_state['last_price']
                        )
                        if is_critical:
                            critical_alerts.append((index, alert_msg))
                            detailed_logger.info(f"Критическое движение обнаружено: {alert_msg}")
                    
                    # Обновляем состояние
                    self.states[index]['current_signal'] = signal
                    self.states[index]['last_price'] = current_price
                    self.states[index]['last_update'] = datetime.now()
                    
                    # Добавляем в историю, если это новый сигнал
                    if signal != prev_state['current_signal'] and "хэдж" in signal.lower():
                        self.history.add_signal(index, signal, current_price)
                        self.states[index]['signal_count'] += 1
                        self.states[index]['last_signal_time'] = datetime.now()
                        self.stats['signals_found'] += 1
                        detailed_logger.info(f"Новый сигнал для {index}: {signal}, добавлен в историю")
                        
                else:
                    error_msg = f"❌ {index}: данные не получены"
                    status_messages.append(error_msg)
                    logger.warning(error_msg)
                    detailed_logger.warning(f"Данные не получены для {index}")
            
            # Формируем итоговое сообщение
            message_lines = []
            
            # Добавляем таблицу сигналов
            if signals_data:
                message_lines.append(self.format_signal_table(signals_data))
                message_lines.append("")  # Пустая строка
                
                # Добавляем историю для каждого индекса (только для индексов с данными)
                for data in signals_data:
                    history_block = self.format_history_block(data['index'])
                    if history_block:
                        message_lines.append(history_block)
                        message_lines.append("")  # Пустая строка
            
            # Добавляем статистику
            message_lines.append("📊 **СТАТИСТИКА БОТА**")
            message_lines.append(f"• Всего проверок: {self.stats['total_checks']}")
            message_lines.append(f"• Найдено сигналов: {self.stats['signals_found']}")
            message_lines.append(f"• Критических движений: {self.stats['critical_movements']}")
            message_lines.append(f"• Время работы: {(datetime.now() - self.stats['start_time']).days} дней")
            message_lines.append(f"• Последняя проверка: {datetime.now().strftime('%H:%M:%S')}")
            
            # Отправляем основное сообщение
            full_message = "\n".join(message_lines)
            sent = await self.send_message(full_message)
            
            if sent:
                self.stats['successful_checks'] += 1
                detailed_logger.info(f"Проверка #{self.stats['total_checks']} успешно отправлена")
            
            # Отправляем уведомления о критических движениях
            for index, alert_msg in critical_alerts:
                await self.send_critical_alert(index, alert_msg)
            
            active_signals = len([d for d in signals_data if 'хэдж' in d['signal'].lower()])
            logger.info(f"✅ Проверка завершена. Сигналов: {active_signals}")
            detailed_logger.info(f"=== ПРОВЕРКА ЗАВЕРШЕНА ===")
            detailed_logger.info(f"Сигналов: {active_signals}, Критических движений: {len(critical_alerts)}")
                
        except Exception as e:
            error_msg = f"❌ Ошибка при проверке сигналов: {str(e)}"
            logger.error(error_msg)
            detailed_logger.error(f"Ошибка при проверке сигналов: {str(e)}")
            self.stats['errors'] += 1
            await self.send_message(error_msg)
        finally:
            # Всегда сохраняем состояния после проверки
            self.save_states()
            detailed_logger.info(f"Состояния сохранены после проверки #{self.stats['total_checks']}")
    
    async def send_daily_report(self):
        """Ежедневный отчет"""
        today = datetime.now(MOSCOW_TZ).strftime('%Y-%m-%d')
        
        detailed_logger.info(f"=== ФОРМИРОВАНИЕ ЕЖЕДНЕВНОГО ОТЧЕТА ===")
        
        # Собираем статистику за день
        daily_signals = 0
        for index in self.indexes:
            daily_signals += len(self.history.get_signals_for_date(index, today))
        
        report = (
            f"📋 **ЕЖЕДНЕВНЫЙ ОТЧЕТ ИНДЕКСОВ**\n"
            f"📅 {today}\n"
            f"📊 Проверок за день: {self.stats['total_checks']}\n"
            f"🎯 Сигналов за день: {daily_signals}\n"
            f"🚨 Критических движений: {self.stats['critical_movements']}\n"
            f"📈 Отслеживается: {len(self.indexes)} индексов\n"
            f"⏰ Следующая проверка: 10:10 и 19:10 (МСК)\n"
            f"✅ Система работает стабильно\n\n"
            f"*Отслеживаемые индексы:*\n"
        )
        
        # Добавляем информацию по каждому индексу
        for index in self.indexes:
            config = INDEX_CONFIG.get(index, {})
            state = self.states[index]
            
            report += f"• {config.get('name', index)}: "
            if state['current_signal']:
                report += f"{state['current_signal']}"
            else:
                report += "нет данных"
            report += f" (сигналов: {state['signal_count']})\n"
        
        sent = await self.send_message(report)
        if sent:
            detailed_logger.info(f"Ежедневный отчет отправлен: {today}")
        
        # Очищаем счетчик критических движений на новый день
        self.stats['critical_movements'] = 0
        detailed_logger.info("Счетчик критических движений сброшен")
        
        # Сохраняем состояния
        self.save_states()

async def main():
    try:
        logger.info("🚀 Запуск бота индексов MOEX")
        detailed_logger.info("=== ЗАПУСК БОТА ИНДЕКСОВ MOEX ===")
        
        bot = FinalIndexBot(TELEGRAM_TOKEN, TELEGRAM_CHAT_ID)
        
        # Приветственное сообщение
        welcome_msg = (
            f"🤖 *БОТ СИГНАЛОВ ИНДЕКСОВ MOEX ЗАПУЩЕН*\n\n"
            f"📈 *Отслеживаемые индексы:*\n"
        )
        
        for index in bot.indexes:
            config = INDEX_CONFIG.get(index, {})
            logic_desc = ""
            if config.get('logic') == 'standard':
                logic_desc = "Открыть хедж при ↓0.5%, закрыть при ↑0.5%"
            else:
                logic_desc = "Открыть хедж при ↑0.5%, закрыть при ↓0.5%"
            
            welcome_msg += f"• *{config.get('name', index)}* ({index}): {logic_desc}\n"
        
        welcome_msg += (
            f"\n⚙️ *Расписание проверок (МСК):*\n"
            f"   • 10:10 и 19:10 - проверка сигналов\n"
            f"   • 09:00 - ежедневный отчет\n\n"
            f"🚨 *Дополнительные функции:*\n"
            f"   • Кэширование данных в JSON (TTL 5 мин)\n"
            f"   • История сигналов в JSON (50 записей)\n"
            f"   • Уведомления о критических движениях (>2%)\n"
            f"   • Подробное логирование в файлы\n"
            f"   • Сохранение состояний бота\n"
        )
        
        await bot.send_message(welcome_msg)
        logger.info("✅ Приветственное сообщение отправлено")
        detailed_logger.info("Приветственное сообщение отправлено")
        
        # Проверка сигналов при первом запуске бота
        logger.info("🔍 Выполнение начальной проверки сигналов...")
        detailed_logger.info("Выполнение начальной проверки сигналов при запуске")
        await bot.check_all_signals()
        
        # Планировщик с московским временем
        def schedule_moscow_time(time_str: str):
            """Конвертирует московское время в локальное для планировщика"""
            moscow_time = datetime.now(MOSCOW_TZ).replace(
                hour=int(time_str.split(':')[0]),
                minute=int(time_str.split(':')[1]),
                second=0,
                microsecond=0
            )
            local_time = moscow_time.astimezone(pytz.utc).astimezone()
            return local_time.strftime('%H:%M')
        
        # Устанавливаем расписание
        schedule.every().day.at(schedule_moscow_time("10:10")).do(
            lambda: asyncio.create_task(bot.check_all_signals())
        )
        schedule.every().day.at(schedule_moscow_time("19:10")).do(
            lambda: asyncio.create_task(bot.check_all_signals())
        )
        schedule.every().day.at(schedule_moscow_time("09:00")).do(
            lambda: asyncio.create_task(bot.send_daily_report())
        )
        
        logger.info("⏰ Финальный бот индексов запущен по московскому времени")
        logger.info(f"📅 Расписание: 10:10, 19:10, 09:00 (МСК)")
        detailed_logger.info("Бот запущен, расписание установлено")
        detailed_logger.info(f"Расписание проверок: 10:10, 19:10, 09:00 (МСК)")
        
        # Бесконечный цикл с обработкой планировщика
        while True:
            schedule.run_pending()
            await asyncio.sleep(60)
            
    except Exception as e:
        logger.error(f"❌ Критическая ошибка запуска: {e}")
        detailed_logger.error(f"КРИТИЧЕСКАЯ ОШИБКА ЗАПУСКА: {str(e)}")
        raise

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("🛑 Бот остановлен пользователем")
        detailed_logger.info("Бот остановлен по команде пользователя")
    except Exception as e:
        logger.error(f"❌ Непредвиденная ошибка: {e}")
        detailed_logger.error(f"Непредвиденная ошибка: {str(e)}")