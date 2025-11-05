#!/usr/bin/env python3
import asyncio
import logging
import aiohttp
import aiomoex
import pandas as pd
from telegram import Bot
from telegram.error import TelegramError
import schedule
import time
from datetime import datetime, timedelta
import os
import sys
from dotenv import load_dotenv

# Загружаем переменные из .env файла
load_dotenv()

# Получаем конфигурацию из переменных окружения
TELEGRAM_TOKEN = os.getenv('TELEGRAM_TOKEN')
TELEGRAM_CHAT_ID = os.getenv('TELEGRAM_CHAT_ID')

# Настройка логирования
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

class MultiMoexSignalBot:
    def __init__(self, telegram_token, chat_id):
        self.telegram_token = telegram_token
        self.chat_id = chat_id
        
        # Список инструментов для отслеживания
        self.tickers = ['SBER', 'GAZP', 'LKOH', 'VTBR']
        self.ticker_names = {
            'SBER': 'Сбербанк',
            'GAZP': 'Газпром', 
            'LKOH': 'Лукойл',
            'VTBR': 'ВТБ'
        }
        
        # Проверяем настройки при инициализации
        self.check_settings()
        
        # Создаем бота только после успешной проверки
        self.bot = Bot(token=telegram_token)
    
    def check_settings(self):
        """Проверка всех необходимых настроек"""
        missing_settings = []
        
        if not self.telegram_token:
            missing_settings.append("TELEGRAM_TOKEN")
        if not self.chat_id:
            missing_settings.append("TELEGRAM_CHAT_ID")
        
        if missing_settings:
            error_msg = f"❌ Отсутствуют настройки в .env файле: {', '.join(missing_settings)}"
            logger.error(error_msg)
            raise ValueError(error_msg)
        
        logger.info("✅ Все настройки загружены корректно")
    
    async def send_message(self, text):
        """Отправка сообщения в Telegram"""
        try:
            await self.bot.send_message(chat_id=self.chat_id, text=text)
            logger.info(f"✅ Сообщение отправлено в Telegram")
            return True
        except TelegramError as e:
            logger.error(f"❌ Ошибка отправки сообщения: {e}")
            return False
    
    async def get_stock_data(self, ticker, days=5):
        """Получение данных по акции через aiomoex"""
        try:
            logger.info(f"📊 Получение данных {ticker}...")
            
            end_date = datetime.now()
            start_date = end_date - timedelta(days=days)
            
            async with aiohttp.ClientSession() as session:
                # Получаем дневные свечи для акции
                data = await aiomoex.get_market_candles(
                    session,
                    ticker,
                    interval=24,  # дневной интервал
                    start=start_date.strftime('%Y-%m-%d'),
                    end=end_date.strftime('%Y-%m-%d')
                )
                
                if not data:
                    logger.error(f"❌ Не удалось получить данные для {ticker} - пустой ответ")
                    return None
                
                df = pd.DataFrame(data)
                
                if len(df) == 0:
                    logger.error(f"❌ Получено 0 записей для {ticker}")
                    return None
                
                # Проверяем наличие необходимых колонок
                required_columns = ['begin', 'open', 'high', 'low', 'close']
                if not all(col in df.columns for col in required_columns):
                    logger.error(f"❌ Отсутствуют необходимые колонки для {ticker}. Доступные: {list(df.columns)}")
                    return None
                
                # Преобразуем даты и сортируем
                df['date'] = pd.to_datetime(df['begin'])
                df.set_index('date', inplace=True)
                df = df.sort_index()
                
                logger.info(f"✅ Получено {len(df)} дневных свечей для {ticker}")
                if len(df) > 0:
                    logger.info(f"📈 {ticker}: {df.index[-1].strftime('%Y-%m-%d')}, Close: {df['close'].iloc[-1]:.2f}")
                
                return df
                
        except Exception as e:
            logger.error(f"❌ Ошибка получения данных для {ticker}: {e}")
            return None
    
    def calculate_signals(self, df, ticker):
        """Расчет торговых сигналов для одного инструмента"""
        if df is None or len(df) < 2:
            logger.warning(f"⚠️ Недостаточно данных для расчета сигналов {ticker}")
            return None
            
        # Берем последние две свечи
        current_candle = df.iloc[-1]
        prev_candle = df.iloc[-2]
        
        signals = []
        
        # Используем оригинальные названия колонок (нижний регистр)
        current_close = current_candle['close']
        prev_high = prev_candle['high']
        prev_low = prev_candle['low']
        
        # Сигнал на покупку: текущая цена закрытия > предыдущий high + 0.5%
        buy_threshold = prev_high * 1.005
        if current_close > buy_threshold:
            change_percent = ((current_close - buy_threshold) / buy_threshold) * 100
            logger.info(f"🎯 {ticker}: Сформирован сигнал BUY: {current_close:.2f} > {buy_threshold:.2f} (+{change_percent:.2f}%)")
            signals.append({
                'type': 'BUY',
                'ticker': ticker,
                'price': current_close,
                'threshold': buy_threshold,
                'time': df.index[-1],
                'current_close': current_close,
                'prev_high': prev_high,
                'change_percent': change_percent
            })
        
        # Сигнал на продажу: текущая цена закрытия < предыдущий low - 0.5%
        sell_threshold = prev_low * 0.995
        if current_close < sell_threshold:
            change_percent = ((sell_threshold - current_close) / sell_threshold) * 100
            logger.info(f"🎯 {ticker}: Сформирован сигнал SELL: {current_close:.2f} < {sell_threshold:.2f} (-{change_percent:.2f}%)")
            signals.append({
                'type': 'SELL', 
                'ticker': ticker,
                'price': current_close,
                'threshold': sell_threshold,
                'time': df.index[-1],
                'current_close': current_close,
                'prev_low': prev_low,
                'change_percent': change_percent
            })
        
        if not signals:
            logger.info(f"📊 {ticker}: Сигналов нет - условия не выполнены")
            
        return signals if signals else None
    
    async def check_all_signals(self):
        """Проверка сигналов для всех инструментов"""
        try:
            logger.info("🔍 Проверка сигналов для всех инструментов...")
            
            all_signals = []
            status_messages = []
            
            for ticker in self.tickers:
                # Получаем данные
                df = await self.get_stock_data(ticker)
                if df is None:
                    status_messages.append(f"❌ {ticker}: Ошибка получения данных")
                    continue
                
                # Рассчитываем сигналы
                signals = self.calculate_signals(df, ticker)
                
                if signals:
                    all_signals.extend(signals)
                else:
                    # Добавляем статус для инструмента без сигналов
                    status_msg = self.format_single_status(df, ticker)
                    status_messages.append(status_msg)
            
            # Отправляем сигналы если есть
            if all_signals:
                for signal in all_signals:
                    message = self.format_signal_message(signal)
                    success = await self.send_message(message)
                    if success:
                        logger.info(f"✅ Сигнал {signal['type']} для {signal['ticker']} отправлен")
            
            # Отправляем общий статус
            if status_messages:
                status_header = "📊 **ОБЩИЙ СТАТУС**\n"
                status_header += f"🕒 Время проверки: {datetime.now().strftime('%Y-%m-%d %H:%M')}\n"
                status_header += "────────────────────\n"
                
                full_status = status_header + "\n".join(status_messages)
                await self.send_message(full_status)
                logger.info("✅ Общий статус отправлен")
                
        except Exception as e:
            error_msg = f"❌ Ошибка при проверке сигналов: {str(e)}"
            logger.error(error_msg)
            await self.send_message(error_msg)
    
    def format_signal_message(self, signal):
        """Форматирование сообщения о сигнале"""
        ticker_name = self.ticker_names.get(signal['ticker'], signal['ticker'])
        
        if signal['type'] == 'BUY':
            return (
                "🚀 **СИГНАЛ НА ПОКУПКУ** 🚀\n"
                f"📈 Акция: {ticker_name} ({signal['ticker']})\n"
                f"💰 Текущая цена: {signal['current_close']:.2f} руб.\n"
                f"🎯 Преодолен уровень: {signal['threshold']:.2f} руб.\n"
                f"📊 Предыдущий high: {signal['prev_high']:.2f} руб.\n"
                f"📈 Превышение: +{signal['change_percent']:.2f}%\n"
                f"🕒 Время: {signal['time'].strftime('%Y-%m-%d')}\n"
                f"🔔 Условие: Закрытие > High предыдущей свечи + 0.5%"
            )
        else:
            return (
                "🔻 **СИГНАЛ НА ПРОДАЖУ** 🔻\n"
                f"📉 Акция: {ticker_name} ({signal['ticker']})\n"
                f"💰 Текущая цена: {signal['current_close']:.2f} руб.\n"
                f"🎯 Преодолен уровень: {signal['threshold']:.2f} руб.\n"
                f"📊 Предыдущий low: {signal['prev_low']:.2f} руб.\n"
                f"📉 Снижение: -{signal['change_percent']:.2f}%\n"
                f"🕒 Время: {signal['time'].strftime('%Y-%m-%d')}\n"
                f"🔔 Условие: Закрытие < Low предыдущей свечи - 0.5%"
            )
    
    def format_single_status(self, df, ticker):
        """Форматирование статуса для одного инструмента"""
        if df is None or len(df) == 0:
            return f"❌ {ticker}: Данные не получены"
        
        current_candle = df.iloc[-1]
        prev_candle = df.iloc[-2] if len(df) > 1 else current_candle
        
        current_close = current_candle['close']
        prev_high = prev_candle['high']
        prev_low = prev_candle['low']
        
        buy_threshold = prev_high * 1.005
        sell_threshold = prev_low * 0.995
        
        buy_diff = ((current_close - buy_threshold) / buy_threshold) * 100
        sell_diff = ((sell_threshold - current_close) / sell_threshold) * 100
        
        ticker_name = self.ticker_names.get(ticker, ticker)
        
        return (
            f"📈 {ticker_name}:\n"
            f"   💰 {current_close:.2f} руб. | "
            f"🔼 {buy_diff:+.1f}% | "
            f"🔽 {sell_diff:+.1f}%\n"
        )

async def main():
    try:
        # Создаем бота
        bot = MultiMoexSignalBot(TELEGRAM_TOKEN, TELEGRAM_CHAT_ID)
        
        # Отправляем тестовое сообщение
        tickers_list = ", ".join([f"{bot.ticker_names[t]} ({t})" for t in bot.tickers])
        await bot.send_message(
            f"🤖 Мульти-акционный бот запущен! 🚀\n"
            f"📊 Отслеживаем: {tickers_list}\n"
            f"⏰ Проверка ежедневно в 10:00 и 19:00"
        )
        
        # Первая проверка
        await bot.check_all_signals()
        
        # Планировщик для регулярных проверок
        schedule.every().day.at("10:00").do(
            lambda: asyncio.create_task(bot.check_all_signals())
        )
        
        schedule.every().day.at("19:00").do(
            lambda: asyncio.create_task(bot.check_all_signals())
        )
        
        logger.info("⏰ Мульти-акционный бот запущен. Проверки в 10:00 и 19:00")
        
        # Бесконечный цикл
        while True:
            schedule.run_pending()
            await asyncio.sleep(60)
            
    except ValueError as e:
        logger.error(f"❌ Ошибка конфигурации: {e}")
    except Exception as e:
        logger.error(f"❌ Неожиданная ошибка: {e}")

if __name__ == "__main__":
    asyncio.run(main())
