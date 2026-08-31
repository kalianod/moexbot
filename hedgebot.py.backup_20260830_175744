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
import sys

# Загрузка переменных окружения
load_dotenv()
TELEGRAM_TOKEN = os.getenv('TELEGRAM_TOKEN')
TELEGRAM_CHAT_ID = os.getenv('TELEGRAM_CHAT_ID')

# Проверка обязательных переменных
if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
    print("❌ КРИТИЧЕСКАЯ ОШИБКА: Не заданы TELEGRAM_TOKEN или TELEGRAM_CHAT_ID в .env файле")
    sys.exit(1)

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

# Конфигурация индексов с их логикой (MCFTR оставлен в конфиге, но исключён из списка отслеживаемых)
INDEX_CONFIG = {
    'IMOEX': {
        'name': 'Индекс МосБиржи',
        'type': 'index',
        'logic': 'standard',
        'threshold': 0.005,
        'cache_ttl': 300
    },
    'MCFTR': {
        'name': 'Индекс МосБиржи полный',
        'type': 'index',
        'logic': 'standard',
        'threshold': 0.005,
        'cache_ttl': 300
    },
    'CNYRUB_TOM': {
        'name': 'Юань/Рубль',
        'type': 'currency',
        'logic': 'inverse',
        'threshold': 0.005,
        'cache_ttl': 300
    },
    'GLDRUB_TOM': {
        'name': 'Золото/Рубль',
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
                        if isinstance(value, dict) and 'data' in value and 'timestamp' in value:
                            df_dict = value['data']
                            if df_dict and 'index' in df_dict and 'columns' in df_dict and 'data' in df_dict:
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
            else:
                logger.info("Файл кэша не найден, будет создан новый")
        except json.JSONDecodeError as e:
            logger.error(f"❌ Ошибка парсинга JSON в файле кэша: {e}")
            try:
                backup_file = self.cache_file.with_suffix('.bak')
                self.cache_file.replace(backup_file)
                logger.warning(f"⚠️ Битый кэш перемещен в {backup_file}")
            except Exception as mv_err:
                logger.error(f"Не удалось переместить битый файл: {mv_err}")
            
            logger.info("Создаем новый кэш")
            self.cache = {}
        except Exception as e:
            logger.error(f"❌ Ошибка загрузки кэша: {e}")
            self.cache = {}
    
    def save_cache(self):
        """Сохранить кэш в файл JSON"""
        try:
            cache_data = {}
            for key, (data, timestamp) in self.cache.items():
                if isinstance(data, pd.DataFrame):
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
        except Exception as e:
            logger.error(f"❌ Ошибка сохранения кэша: {e}")
    
    def get(self, key: str) -> Optional[pd.DataFrame]:
        """Получить данные из кэша"""
        if key in self.cache:
            data, timestamp = self.cache[key]
            index_key = key.split('_')[0]
            ttl = INDEX_CONFIG.get(index_key, {}).get('cache_ttl', 300)
            
            if (datetime.now() - timestamp).total_seconds() < ttl:
                logger.debug(f"Кэш HIT для {key}")
                return data
            else:
                logger.debug(f"Кэш EXPIRED для {key}")
                return None
        return None
    
    def set(self, key: str, data: pd.DataFrame):
        """Сохранить данные в кэш"""
        try:
            self.cache[key] = (data, datetime.now())
            logger.debug(f"Данные закэшированы для {key}")
            self.save_cache()
        except Exception as e:
            logger.error(f"❌ Ошибка при кэшировании данных: {e}")


class SignalHistory:
    """Класс для хранения истории сигналов"""
    
    def __init__(self, history_file: str = "history.json", max_history: int = 50):
        self.history_file = Path(history_file)
        self.max_history = max_history
        self.history = {}
        self.performance = {}
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
            else:
                logger.info("Файл истории не найден, будет создан новый")
        except json.JSONDecodeError as e:
            logger.error(f"❌ Ошибка парсинга JSON в файле истории: {e}")
            try:
                backup_file = self.history_file.with_suffix('.bak')
                self.history_file.replace(backup_file)
                logger.warning(f"⚠️ Битая история перемещена в {backup_file}")
            except Exception as mv_err:
                logger.error(f"Не удалось переместить битый файл: {mv_err}")
                
            logger.info("Создаем новую историю")
            self.history = {}
            self.performance = {}
        except Exception as e:
            logger.error(f"❌ Ошибка загрузки истории: {e}")
            self.history = {}
            self.performance = {}
    
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
        
        if len(self.history[index]) > self.max_history:
            self.history[index] = self.history[index][-self.max_history:]
        
        logger.debug(f"Сигнал добавлен в историю: {index} - {signal} по {price}")
        self.save_history()
    
    def get_recent_signals(self, index: str, limit: int = 5) -> List[Dict]:
        """Получить последние сигналы для индекса"""
        if index in self.history:
            signals = self.history[index][-limit:]
            for signal in signals:
                if 'timestamp' in signal and isinstance(signal['timestamp'], str):
                    signal['timestamp'] = datetime.fromisoformat(signal['timestamp'])
            return signals
        return []
    
    def get_today_signals(self, index: str) -> List[Dict]:
        """Получить сегодняшние сигналы для индекса"""
        today = datetime.now().strftime('%Y-%m-%d')
        if index in self.history:
            return [s for s in self.history[index] if s['date'] == today]
        return []


class MoexIndexAPI:
    """Класс для получения данных индексов MOEX"""
    
    def __init__(self):
        self.base_url = "https://iss.moex.com/iss"
        self.session = requests.Session()
        self.cache = DataCache()
    
    def get_index_candles_simple(self, index: str = 'IMOEX', days: int = 10):
        """Упрощенный метод получения свечных данных"""
        cache_key = f"{index}_candles_{days}"
        cached_data = self.cache.get(cache_key)
        if cached_data is not None:
            return cached_data
        
        try:
            if index in ['IMOEX', 'MCFTR']:
                url = f"{self.base_url}/engines/stock/markets/index/boards/SNDX/securities/{index}/candles.json"
            elif index in ['CNYRUB_TOM', 'GLDRUB_TOM']:
                url = f"{self.base_url}/engines/currency/markets/selt/boards/CETS/securities/{index}/candles.json"
            else:
                logger.error(f"❌ Неизвестный индекс: {index}")
                return None
            
            start_date = (datetime.now() - timedelta(days=days)).strftime('%Y-%m-%d')
            params = {
                'from': start_date,
                'till': datetime.now().strftime('%Y-%m-%d'),
                'interval': 24,
                'iss.meta': 'off'
            }
            
            response = self.session.get(url, params=params, timeout=30)
            
            if response.status_code == 200:
                data = response.json()
                if 'candles' in data and 'data' in data['candles']:
                    candles_data = data['candles']['data']
                    if candles_data:
                        df = pd.DataFrame(candles_data, columns=[
                            'open', 'close', 'high', 'low', 'value', 'volume', 'begin', 'end'
                        ])
                        df['date'] = pd.to_datetime(df['begin'])
                        df.set_index('date', inplace=True)
                        df = df.sort_index()
                        
                        self.cache.set(cache_key, df)
                        logger.info(f"✅ Получено {len(df)} свечей для {index}")
                        return df
            
            logger.warning(f"⚠️ Нет данных для {index}")
            return None
        
        except Exception as e:
            logger.error(f"❌ Ошибка получения данных {index}: {e}")
            return None
    
    def get_index_current(self, index: str = 'IMOEX'):
        """Получение текущего значения индекса"""
        try:
            if index in ['IMOEX', 'MCFTR']:
                url = f"{self.base_url}/engines/stock/markets/index/boards/SNDX/securities/{index}.json"
            elif index in ['CNYRUB_TOM', 'GLDRUB_TOM']:
                url = f"{self.base_url}/engines/currency/markets/selt/boards/CETS/securities/{index}.json"
            else:
                logger.error(f"❌ Неизвестный индекс: {index}")
                return None
            
            params = {'iss.meta': 'off'}
            response = self.session.get(url, params=params, timeout=30)
            
            if response.status_code == 200:
                data = response.json()
                if 'marketdata' in data and 'data' in data['marketdata']:
                    marketdata = data['marketdata']['data']
                    if marketdata:
                        columns = data['marketdata']['columns']
                        
                        if index in ['CNYRUB_TOM', 'GLDRUB_TOM']:
                            try:
                                if 'LAST' in columns:
                                    price_index = columns.index('LAST')
                                elif 'CURRENTVALUE' in columns:
                                    price_index = columns.index('CURRENTVALUE')
                                else:
                                    price_index = 12
                                    logger.warning(f"⚠️ Не найдена колонка LAST/CURRENTVALUE для {index}, используем индекс 12")
                                current_value = marketdata[0][price_index]
                            except (IndexError, ValueError) as e:
                                logger.error(f"❌ Ошибка доступа к колонке цены для {index}: {e}")
                                return None
                        else:
                            try:
                                if 'CURRENTVALUE' in columns:
                                    price_index = columns.index('CURRENTVALUE')
                                elif 'LAST' in columns:
                                    price_index = columns.index('LAST')
                                else:
                                    price_index = 2
                                    logger.warning(f"⚠️ Не найдена колонка CURRENTVALUE/LAST для {index}, используем индекс 2")
                                current_value = marketdata[0][price_index]
                            except (IndexError, ValueError) as e:
                                logger.error(f"❌ Ошибка доступа к колонке цены для {index}: {e}")
                                return None
                        
                        logger.info(f"✅ Текущее значение {index}: {current_value}")
                        return current_value
                
                logger.warning(f"⚠️ Не удалось найти текущее значение для {index}")
                return None
            else:
                logger.error(f"❌ HTTP ошибка {response.status_code} для {index}")
                return None
        
        except Exception as e:
            logger.error(f"❌ Ошибка получения текущего значения {index}: {e}")
            return None
    
    def get_index_data_reliable(self, index: str = 'IMOEX', days: int = 5):
        """Надежный метод получения данных индекса"""
        df = self.get_index_candles_simple(index, days)
        if df is not None and len(df) >= 2:
            return df
        
        error_msg = f"❌ API MOEX недоступно для получения данных по {index}"
        logger.error(error_msg)
        return None


class FinalIndexBot:
    def __init__(self, telegram_token, chat_id):
        self.telegram_token = telegram_token
        self.chat_id = chat_id
        logger.info("🚀 Инициализация FinalIndexBot")
        
        # Исключён MCFTR из списка отслеживаемых индексов
        self.indexes = ['IMOEX', 'CNYRUB_TOM', 'GLDRUB_TOM']
        self.api = MoexIndexAPI()
        self.bot = Bot(token=telegram_token)
        self.history = SignalHistory()
        
        self.states = {
            index: {
                'current_signal': None,
                'last_price': None,
                'last_update': None,
                'signal_count': 0,
                'last_signal_time': None,
                'position': None
            } for index in self.indexes
        }
        
        self.daily_stats = {
            'checks_today': 0,
            'signals_today': 0,
            'critical_movements_today': 0,
            'last_check_time': None,
            'report_sent': False
        }
        
        self.global_stats = {
            'total_checks': 0,
            'total_signals': 0,
            'start_time': datetime.now(),
            'days_active': 1
        }
        
        self.load_states()
        logger.info("✅ FinalIndexBot инициализирован")
    
    def load_states(self):
        """Загрузить состояния из файла с обработкой ошибок JSON"""
        try:
            states_file = Path("bot_states.json")
            if states_file.exists():
                with open(states_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    
                    if 'states' in data and 'global_stats' in data:
                        for index in self.indexes:
                            if index in data['states']:
                                self.states[index] = data['states'][index]
                                for time_key in ['last_update', 'last_signal_time']:
                                    if self.states[index][time_key] and isinstance(self.states[index][time_key], str):
                                        self.states[index][time_key] = datetime.fromisoformat(self.states[index][time_key])
                        
                        self.global_stats.update(data['global_stats'])
                        for time_key in ['start_time']:
                            if self.global_stats[time_key] and isinstance(self.global_stats[time_key], str):
                                self.global_stats[time_key] = datetime.fromisoformat(self.global_stats[time_key])
                        
                        logger.info("✅ Состояния бота загружены из файла")
                    else:
                        logger.warning("⚠️ Неверная структура файла состояний, создаем новые")
            else:
                logger.info("Файл состояний не найден, будут созданы новые")
        except json.JSONDecodeError as e:
            logger.error(f"❌ Ошибка парсинга JSON в файле состояний: {e}")
            try:
                states_file = Path("bot_states.json")
                backup_file = states_file.with_suffix('.bak')
                states_file.replace(backup_file)
                logger.warning(f"⚠️ Битая конфигурация перемещена в {backup_file}")
            except Exception as mv_err:
                logger.error(f"Не удалось переместить битый файл: {mv_err}")
            
            logger.info("Создаем новые состояния")
        except Exception as e:
            logger.error(f"❌ Ошибка загрузки состояний: {e}")
    
    def save_states(self):
        """Сохранить состояния в файл"""
        try:
            states_file = Path("bot_states.json")
            states_to_save = {}
            
            for index, state in self.states.items():
                states_to_save[index] = state.copy()
                for time_key in ['last_update', 'last_signal_time']:
                    if states_to_save[index][time_key] and isinstance(states_to_save[index][time_key], datetime):
                        states_to_save[index][time_key] = states_to_save[index][time_key].isoformat()
            
            global_stats_to_save = self.global_stats.copy()
            for time_key in ['start_time']:
                if global_stats_to_save[time_key] and isinstance(global_stats_to_save[time_key], datetime):
                    global_stats_to_save[time_key] = global_stats_to_save[time_key].isoformat()
            
            data = {
                'states': states_to_save,
                'global_stats': global_stats_to_save,
                'last_save': datetime.now().isoformat()
            }
            
            with open(states_file, 'w', encoding='utf-8') as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
            logger.debug("Состояния бота сохранены")
        except Exception as e:
            logger.error(f"❌ Ошибка сохранения состояний: {e}")
    
    async def send_message(self, text):
        """Отправка сообщения в Telegram"""
        try:
            await self.bot.send_message(chat_id=self.chat_id, text=text, parse_mode='Markdown')
            logger.info("✅ Сообщение отправлено в Telegram")
            return True
        except TelegramError as e:
            logger.error(f"❌ Ошибка отправки: {e}")
            return False
    
    def get_index_data(self, index):
        """Получение данных индекса"""
        return self.api.get_index_data_reliable(index, days=5)
    
    def calculate_hedge_signal(self, df, index):
        """Расчет сигнала хеджирования для индекса"""
        if df is None or len(df) < 2:
            logger.warning(f"⚠️ Недостаточно данных для {index}")
            return "Данные не получены", None, None, None
        
        current_candle = df.iloc[-1]
        prev_candle = df.iloc[-2]
        
        current_close = current_candle['close']
        prev_high = prev_candle['high']
        prev_low = prev_candle['low']
        prev_close = prev_candle['close']
        
        # Расчет изменения цены
        price_change = ((current_close - prev_close) / prev_close) * 100
        
        index_config = INDEX_CONFIG.get(index, {})
        logic_type = index_config.get('logic', 'standard')
        threshold = index_config.get('threshold', 0.005)
        
        signal = None
        action = None  # 'open' или 'close' или 'hold'
        
        if logic_type == 'standard':
            buy_threshold = prev_high * (1 + threshold)
            sell_threshold = prev_low * (1 - threshold)
            
            if current_close > buy_threshold:
                signal = "ЗАКРЫТЬ ХЕДЖ"
                action = 'close'
                logger.info(f"🎯 {index}: ЗАКРЫТЬ ХЕДЖ: {current_close:.2f} > {buy_threshold:.2f}")
            elif current_close < sell_threshold:
                signal = "ОТКРЫТЬ ХЕДЖ"
                action = 'open'
                logger.info(f"🎯 {index}: ОТКРЫТЬ ХЕДЖ: {current_close:.2f} < {sell_threshold:.2f}")
        
        elif logic_type == 'inverse':
            buy_threshold = prev_low * (1 + threshold)
            sell_threshold = prev_high * (1 - threshold)
            
            if current_close > buy_threshold:
                signal = "ОТКРЫТЬ ХЕДЖ"
                action = 'open'
                logger.info(f"🎯 {index}: ОТКРЫТЬ ХЕДЖ: {current_close:.2f} > {buy_threshold:.2f}")
            elif current_close < sell_threshold:
                signal = "ЗАКРЫТЬ ХЕДЖ"
                action = 'close'
                logger.info(f"🎯 {index}: ЗАКРЫТЬ ХЕДЖ: {current_close:.2f} < {sell_threshold:.2f}")
        
        if signal is None:
            signal = "НЕТ СИГНАЛА"
            action = 'hold'
            logger.info(f"📊 {index}: НЕТ СИГНАЛА")
        
        return signal, current_close, price_change, action
    
    def check_critical_movement(self, index: str, current_price: float, prev_price: float = None) -> Tuple[bool, str]:
        """Проверка критического движения цены"""
        if prev_price is None or prev_price == 0:
            return False, ""
        
        change_percent = abs((current_price - prev_price) / prev_price)
        change_abs = abs(current_price - prev_price)
        
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
            self.daily_stats['critical_movements_today'] += 1
            logger.info(f"🚨 Отправлено уведомление о критическом движении для {index}")
    
    # Новая вспомогательная функция для определения статуса отображения
    def _get_display_status(self, signal: str, prev_position: Optional[str]) -> str:
        """Возвращает статус для отображения в таблице на основе текущего сигнала и предыдущей позиции"""
        if "ОТКРЫТЬ" in signal:
            return "ОТКРЫТЬ ХЕДЖ"
        elif "ЗАКРЫТЬ" in signal:
            return "ЗАКРЫТЬ ХЕДЖ"
        elif "НЕТ СИГНАЛА" in signal and prev_position == 'hedge_open':
            return "ДЕРЖАТЬ ХЕДЖ"
        else:
            return "---"
    
    def format_signal_table(self, signals_data: List[Dict]) -> str:
        """Форматирование таблицы сигналов с новыми статусами"""
        # Определяем максимальные длины
        max_name_len = max(len(data['name']) for data in signals_data)
        max_name_len = min(max_name_len, 20)  # Ограничиваем имя для мобильных
        
        table_lines = []
        
        # Заголовок вне блока кода (чтобы был жирным)
        table_lines.append("*════ СИГНАЛЫ ИНДЕКСОВ ════*")
        
        # Начало блока кода для моноширинности
        table_lines.append("```")
        
        # Шапка таблицы с увеличенной колонкой для сигнала (16 символов)
        header = f"{'ИНДЕКС':<{max_name_len}} {'ЦЕНА':>8} {'СИГНАЛ':>16} {'%':>5}"
        table_lines.append(header)
        
        # Разделитель строго по длине шапки
        table_lines.append("─" * len(header))
        
        for data in signals_data:
            name = data['name']
            price = data['price']
            change = data['change']
            signal = data['signal']
            prev_position = data.get('prev_position')  # предыдущая позиция, добавленная в send_evening_report
            
            if len(name) > max_name_len:
                display_name = name[:max_name_len-2] + ".."
            else:
                display_name = name
            
            # Получаем статус для отображения
            display_status = self._get_display_status(signal, prev_position)
            
            # Форматируем изменение
            change_display = f"{change:+.1f}"
            
            # Формируем строку с выравниванием
            line = f"{display_name:<{max_name_len}} {price:>8.2f} {display_status:>16} {change_display:>5}"
            table_lines.append(line)
        
        table_lines.append("```")  # Конец блока кода
        
        # Подсчет активных сигналов (только открытые и закрытые)
        active_signals = sum(1 for d in signals_data if "ХЕДЖ" in d['signal'] and d['signal'] != "ДЕРЖАТЬ ХЕДЖ")
        table_lines.append(f"Сводка: {active_signals} новых сигналов из {len(signals_data)}")
        table_lines.append(f"Время: {datetime.now(MOSCOW_TZ).strftime('%H:%M, %d.%m.%Y')}")
        
        return "\n".join(table_lines)
    
    def format_action_recommendations(self, signals_data: List[Dict]) -> str:
        """Форматирование рекомендаций по действиям (без изменений)"""
        recommendations = []
        
        open_actions = [d for d in signals_data if d.get('action') == 'open']
        close_actions = [d for d in signals_data if d.get('action') == 'close']
        
        if open_actions:
            rec_text = "🟢 *ОТКРЫТЬ ХЕДЖ:*\n"
            for data in open_actions:
                rec_text += f" • {data['name']} - {data['price']:.2f} ({data['change']:+.1f}%)\n"
            recommendations.append(rec_text)
        
        if close_actions:
            rec_text = "🔴 *ЗАКРЫТЬ ХЕДЖ:*\n"
            for data in close_actions:
                rec_text += f" • {data['name']} - {data['price']:.2f} ({data['change']:+.1f}%)\n"
            recommendations.append(rec_text)
        
        if not open_actions and not close_actions:
            recommendations.append("⚪ *СЕГОДНЯ НОВЫХ СИГНАЛОВ НЕТ*\n Держите текущие позиции")
        
        return "\n".join(recommendations)
    
    def format_history_block(self, index: str) -> str:
        """Форматирование блока истории для индекса (без изменений)"""
        history_records = self.history.get_today_signals(index)
        if not history_records:
            return ""
        
        history_lines = []
        index_name = INDEX_CONFIG.get(index, {}).get('name', index)
        
        history_lines.append("```")
        
        content_width = max(len(index_name) + 2, 32)
        
        history_lines.append(f"┌{'─' * content_width}┐")
        history_lines.append(f"│ {index_name.center(content_width-2)} │")
        history_lines.append(f"├{'─' * content_width}┤")
        
        for record in history_records:
            timestamp = record['timestamp']
            if isinstance(timestamp, str):
                timestamp = datetime.fromisoformat(timestamp)
            time_str = timestamp.strftime('%H:%M')
            price = record['price']
            signal = record['signal']
            
            if "ОТКРЫТЬ" in signal:
                sig_short = "ОТКРЫТЬ"
            elif "ЗАКРЫТЬ" in signal:
                sig_short = "ЗАКРЫТЬ"
            else:
                sig_short = "НЕТ"
            
            row_content = f"{time_str} | {price:.2f} | {sig_short}"
            padding = content_width - len(row_content) - 2
            if padding < 0:
                padding = 0
            
            history_lines.append(f"│ {row_content}{' ' * padding} │")
        
        history_lines.append(f"└{'─' * content_width}┘")
        history_lines.append("```")
        
        return "\n".join(history_lines)
    
    async def send_evening_report(self):
        """Отправка вечернего отчета (19:10) с учётом предыдущих позиций"""
        logger.info("🌙 Отправка вечернего отчета...")
        
        try:
            # Сохраняем предыдущие состояния до обновления
            prev_states = {index: self.states[index].copy() for index in self.indexes}
            
            signals_data = []
            critical_alerts = []
            
            for index in self.indexes:
                df = self.get_index_data(index)
                if df is not None and len(df) >= 2:
                    signal, current_price, price_change, action = self.calculate_hedge_signal(df, index)
                    
                    # Добавляем prev_position в данные для таблицы
                    prev_position = prev_states[index]['position']
                    
                    signals_data.append({
                        'index': index,
                        'name': INDEX_CONFIG.get(index, {}).get('name', index),
                        'price': current_price,
                        'signal': signal,
                        'change': price_change,
                        'action': action,
                        'prev_position': prev_position  # важно для определения статуса
                    })
                    
                    # Проверяем критическое движение
                    prev_state = self.states[index]
                    if prev_state['last_price'] is not None:
                        is_critical, alert_msg = self.check_critical_movement(
                            index, current_price, prev_state['last_price']
                        )
                        if is_critical:
                            critical_alerts.append((index, alert_msg))
                    
                    # Обновляем состояние и сохраняем сигнал в историю
                    if signal != prev_state['current_signal'] and "ХЕДЖ" in signal:
                        self.history.add_signal(index, signal, current_price)
                        self.states[index]['signal_count'] += 1
                        self.states[index]['last_signal_time'] = datetime.now()
                        self.global_stats['total_signals'] += 1
                        self.daily_stats['signals_today'] += 1
                    
                    # Обновляем позицию
                    if "ОТКРЫТЬ" in signal:
                        self.states[index]['position'] = 'hedge_open'
                    elif "ЗАКРЫТЬ" in signal:
                        self.states[index]['position'] = 'hedge_closed'
                    
                    self.states[index]['current_signal'] = signal
                    self.states[index]['last_price'] = current_price
                    self.states[index]['last_update'] = datetime.now()
                else:
                    logger.warning(f"⚠️ {index}: данные не получены")
            
            # Формируем сообщение
            message_lines = []
            
            # Заголовок
            message_lines.append("🤖 *ВЕЧЕРНИЙ ОТЧЕТ - СИГНАЛЫ ИНДЕКСОВ MOEX*")
            message_lines.append(f"📅 {datetime.now(MOSCOW_TZ).strftime('%d.%m.%Y')}")
            message_lines.append("")
            
            # Рекомендации по действиям (самое важное!)
            message_lines.append("🎯 *РЕКОМЕНДАЦИИ НА ЗАВТРА:*")
            message_lines.append(self.format_action_recommendations(signals_data))
            message_lines.append("")
            
            # Таблица с данными
            if signals_data:
                message_lines.append(self.format_signal_table(signals_data))
                message_lines.append("")
            
            # История сигналов сегодня (только для индексов с сигналами)
            for data in signals_data:
                if data['action'] in ['open', 'close']:
                    history_block = self.format_history_block(data['index'])
                    if history_block:
                        message_lines.append(history_block)
                        message_lines.append("")
            
            # Статистика за день
            message_lines.append("📊 *СТАТИСТИКА ЗА ДЕНЬ:*")
            message_lines.append(f"• Проверок сегодня: {self.daily_stats['checks_today']}")
            message_lines.append(f"• Новых сигналов: {self.daily_stats['signals_today']}")
            message_lines.append(f"• Критических движений: {self.daily_stats['critical_movements_today']}")
            message_lines.append("")
            
            # Общая статистика
            message_lines.append("📈 *ОБЩАЯ СТАТИСТИКА:*")
            message_lines.append(f"• Всего проверок: {self.global_stats['total_checks']}")
            message_lines.append(f"• Всего сигналов: {self.global_stats['total_signals']}")
            message_lines.append(f"• Дней работы: {self.global_stats['days_active']}")
            
            # Отправляем основное сообщение
            full_message = "\n".join(message_lines)
            await self.send_message(full_message)
            
            # Отправляем уведомления о критических движениях
            for index, alert_msg in critical_alerts:
                await self.send_critical_alert(index, alert_msg)
            
            # Обновляем статистику
            self.daily_stats['report_sent'] = True
            self.global_stats['total_checks'] += 1
            self.daily_stats['checks_today'] += 1
            self.daily_stats['last_check_time'] = datetime.now()
            
            # Сохраняем состояния
            self.save_states()
            
            logger.info(f"✅ Вечерний отчет отправлен. Сигналов сегодня: {self.daily_stats['signals_today']}")
        
        except Exception as e:
            error_msg = f"❌ Ошибка при отправке вечернего отчета: {str(e)}"
            logger.error(error_msg)
            await self.send_message(error_msg)
    
    async def reset_daily_stats(self):
        """Сброс ежедневной статистики (выполняется в 00:10)"""
        logger.info("🔄 Сброс ежедневной статистики")
        
        # Увеличиваем счетчик дней
        self.global_stats['days_active'] += 1
        
        # Сбрасываем дневную статистику
        self.daily_stats = {
            'checks_today': 0,
            'signals_today': 0,
            'critical_movements_today': 0,
            'last_check_time': None,
            'report_sent': False
        }
        
        # Сохраняем состояния
        self.save_states()
        logger.info("✅ Ежедневная статистика сброшена")
    
    async def perform_silent_check(self):
        """Тихая проверка без отправки сообщений (для обновления кэша)"""
        logger.info("🔍 Выполнение тихой проверки (обновление кэша)...")
        
        try:
            for index in self.indexes:
                df = self.get_index_data(index)
                if df is not None:
                    logger.debug(f"Данные обновлены для {index}")
            
            self.global_stats['total_checks'] += 1
            self.daily_stats['checks_today'] += 1
            self.save_states()
            
            logger.info("✅ Тихая проверка завершена")
        except Exception as e:
            logger.error(f"❌ Ошибка при тихой проверке: {e}")


def schedule_moscow_time(time_str: str):
    """Конвертирует московское время в локальное для планировщика"""
    try:
        now_moscow = datetime.now(MOSCOW_TZ)
        hour, minute = map(int, time_str.split(':'))
        scheduled_time_moscow = now_moscow.replace(
            hour=hour,
            minute=minute,
            second=0,
            microsecond=0
        )
        if scheduled_time_moscow < now_moscow:
            scheduled_time_moscow += timedelta(days=1)
        local_time = scheduled_time_moscow.astimezone()
        return local_time.strftime('%H:%M')
    except Exception as e:
        logger.error(f"❌ Ошибка конвертации времени {time_str}: {e}")
        return time_str


async def main():
    try:
        logger.info("🚀 Запуск бота индексов MOEX")
        
        bot = FinalIndexBot(TELEGRAM_TOKEN, TELEGRAM_CHAT_ID)
        
        # Приветственное сообщение (обновлено, исключён MCFTR)
        welcome_msg = (
            f"🤖 *БОТ СИГНАЛОВ ИНДЕКСОВ MOEX ЗАПУЩЕН*\n\n"
            f"📈 *Отслеживаемые индексы:*\n"
        )
        
        for index in bot.indexes:
            config = INDEX_CONFIG.get(index, {})
            logic_desc = ""
            if config.get('logic') == 'standard':
                logic_desc = "Закрыть хедж при ↑0.5%, открыть при ↓0.5%"
            else:
                logic_desc = "Закрыть хедж при ↓0.5%, открыть при ↑0.5%"
            welcome_msg += f"• *{config.get('name', index)}*: {logic_desc}\n"
        
        welcome_msg += (
            f"\n⚙️ *Расписание (МСК):*\n"
            f" • 19:10 - вечерний отчет с рекомендациями\n"
            f" • 10:10 - тихая проверка (без уведомлений)\n"
            f" • 00:10 - сброс статистики\n\n"
            f"🎯 *Что вы получите:*\n"
            f" 1. Четкие статусы: ОТКРЫТЬ, ДЕРЖАТЬ, ЗАКРЫТЬ\n"
            f" 2. Только важные уведомления\n"
            f" 3. История сегодняшних сигналов\n"
            f" 4. Статистика за день\n"
        )
        
        await bot.send_message(welcome_msg)
        logger.info("✅ Приветственное сообщение отправлено")
        
        # Вычисляем локальное время для расписания
        silent_check_time = schedule_moscow_time("10:10")
        evening_report_time = schedule_moscow_time("19:10")
        reset_stats_time = schedule_moscow_time("00:10")
        
        logger.info(f"⏰ Время тихой проверки (локальное): {silent_check_time}")
        logger.info(f"⏰ Время вечернего отчета (локальное): {evening_report_time}")
        logger.info(f"⏰ Время сброса статистики (локальное): {reset_stats_time}")
        
        # Устанавливаем расписание
        schedule.every().day.at(silent_check_time).do(
            lambda: asyncio.create_task(bot.perform_silent_check())
        )
        
        schedule.every().day.at(evening_report_time).do(
            lambda: asyncio.create_task(bot.send_evening_report())
        )
        
        schedule.every().day.at(reset_stats_time).do(
            lambda: asyncio.create_task(bot.reset_daily_stats())
        )
        
        logger.info("⏰ Бот запущен по московскому времени")
        logger.info("📅 Расписание (МСК): 10:10 (тихая проверка), 19:10 (вечерний отчет), 00:10 (сброс статистики)")
        
        # Основной цикл
        while True:
            schedule.run_pending()
            await asyncio.sleep(60)
    
    except Exception as e:
        logger.error(f"❌ Критическая ошибка запуска: {e}")
        raise


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("🛑 Бот остановлен пользователем")
    except Exception as e:
        logger.error(f"❌ Непредвиденная ошибка: {e}")