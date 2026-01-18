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

class EnhancedMoexSignalBot:
    def __init__(self, telegram_token, chat_id):
        self.telegram_token = telegram_token
        self.chat_id = chat_id
        
        # Только рабочие акции
        self.tickers = ['SBER', 'GAZP', 'LKOH', 'VTBR', 'ROSN', 'MGNT']
        self.ticker_names = {
            'SBER': 'Сбербанк',
            'GAZP': 'Газпром', 
            'LKOH': 'Лукойл',
            'VTBR': 'ВТБ',
            'ROSN': 'Роснефть',
            'MGNT': 'Магнит'
        }
        
        # Настройки сигналов
        self.buy_threshold_percent = 0.5  # % для покупки
        self.sell_threshold_percent = 0.5  # % для продажи
        
        self.check_settings()
        self.bot = Bot(token=telegram_token)
        
        # Статистика
        self.stats = {
            'total_checks': 0,
            'signals_found': 0,
            'last_check': None
        }
    
    def check_settings(self):
        """Проверка всех необходимых настроек"""
        if not self.telegram_token or not self.chat_id:
            raise ValueError("❌ Проверьте TELEGRAM_TOKEN и TELEGRAM_CHAT_ID в .env")
        logger.info("✅ Все настройки загружены корректно")
    
    async def send_message(self, text):
        """Отправка сообщения в Telegram"""
        try:
            await self.bot.send_message(chat_id=self.chat_id, text=text)
            logger.info("✅ Сообщение отправлено в Telegram")
            return True
        except TelegramError as e:
            logger.error(f"❌ Ошибка отправки сообщения: {e}")
            return False
    
    async def get_stock_data(self, ticker, days=5):
        """Получение данных по акции"""
        try:
            end_date = datetime.now()
            start_date = end_date - timedelta(days=days)
            
            async with aiohttp.ClientSession() as session:
                data = await aiomoex.get_market_candles(
                    session,
                    ticker,
                    interval=24,
                    start=start_date.strftime('%Y-%m-%d'),
                    end=end_date.strftime('%Y-%m-%d')
                )
                
                if data:
                    df = pd.DataFrame(data)
                    if len(df) > 0:
                        df['date'] = pd.to_datetime(df['begin'])
                        df.set_index('date', inplace=True)
                        df = df.sort_index()
                        return df
            
            return None
                
        except Exception as e:
            logger.error(f"❌ Ошибка получения данных {ticker}: {e}")
            return None
    
    def calculate_signals(self, df, ticker):
        """Расчет торговых сигналов"""
        if df is None or len(df) < 2:
            return None
            
        current_candle = df.iloc[-1]
        prev_candle = df.iloc[-2]
        
        signals = []
        current_close = current_candle['close']
        prev_high = prev_candle['high']
        prev_low = prev_candle['low']
        
        # Сигнал на покупку
        buy_threshold = prev_high * (1 + self.buy_threshold_percent / 100)
        if current_close > buy_threshold:
            change_percent = ((current_close - buy_threshold) / buy_threshold) * 100
            signals.append({
                'type': 'BUY',
                'ticker': ticker,
                'price': current_close,
                'threshold': buy_threshold,
                'time': df.index[-1],
                'change_percent': change_percent,
                'prev_high': prev_high
            })
        
        # Сигнал на продажу
        sell_threshold = prev_low * (1 - self.sell_threshold_percent / 100)
        if current_close < sell_threshold:
            change_percent = ((sell_threshold - current_close) / sell_threshold) * 100
            signals.append({
                'type': 'SELL', 
                'ticker': ticker,
                'price': current_close,
                'threshold': sell_threshold,
                'time': df.index[-1],
                'change_percent': change_percent,
                'prev_low': prev_low
            })
        
        return signals if signals else None
    
    async def check_all_signals(self):
        """Проверка сигналов для всех акций"""
        self.stats['total_checks'] += 1
        self.stats['last_check'] = datetime.now()
        
        try:
            logger.info("🔍 Проверка сигналов для всех акций...")
            
            all_signals = []
            status_messages = []
            successful_checks = 0
            
            for ticker in self.tickers:
                df = await self.get_stock_data(ticker)
                if df is not None:
                    successful_checks += 1
                    signals = self.calculate_signals(df, ticker)
                    
                    if signals:
                        all_signals.extend(signals)
                        self.stats['signals_found'] += len(signals)
                    else:
                        status_messages.append(self.format_single_status(df, ticker))
                else:
                    status_messages.append(f"❌ {ticker}: данные не получены")
            
            # Отправляем сигналы если есть
            if all_signals:
                signal_header = f"🚨 **ОБНАРУЖЕНЫ СИГНАЛЫ** 🚨\n"
                signal_header += f"📅 {datetime.now().strftime('%Y-%m-%d %H:%M')}\n"
                signal_header += "────────────────────\n"
                await self.send_message(signal_header)
                
                for signal in all_signals:
                    message = self.format_signal_message(signal)
                    await self.send_message(message)
                    await asyncio.sleep(1)  # Пауза между сообщениями
            
            # Отправляем общий статус
            if status_messages:
                status_header = self.format_status_header(successful_checks, len(all_signals))
                full_status = status_header + "\n".join(status_messages)
                await self.send_message(full_status)
                
            logger.info(f"✅ Проверка завершена. Успешно: {successful_checks}/{len(self.tickers)}, Сигналов: {len(all_signals)}")
                
        except Exception as e:
            error_msg = f"❌ Ошибка при проверке сигналов: {str(e)}"
            logger.error(error_msg)
            await self.send_message(error_msg)
    
    def format_status_header(self, successful_checks, signals_count):
        """Форматирование заголовка статуса"""
        return (
            f"📊 **СТАТУС СИСТЕМЫ**\n"
            f"🕒 {datetime.now().strftime('%Y-%m-%d %H:%M')}\n"
            f"📈 Акций проверено: {successful_checks}/{len(self.tickers)}\n"
            f"🎯 Сигналов найдено: {signals_count}\n"
            f"📋 Всего проверок: {self.stats['total_checks']}\n"
            f"────────────────────\n"
        )
    
    def format_signal_message(self, signal):
        """Форматирование сообщения о сигнале"""
        ticker_name = self.ticker_names.get(signal['ticker'], signal['ticker'])
        
        if signal['type'] == 'BUY':
            return (
                "�� **СИГНАЛ НА ПОКУПКУ** 🚀\n"
                f"📈 {ticker_name} ({signal['ticker']})\n"
                f"💰 Текущая цена: {signal['price']:.2f} руб.\n"
                f"🎯 Преодолен уровень: {signal['threshold']:.2f} руб.\n"
                f"📊 Предыдущий high: {signal['prev_high']:.2f} руб.\n"
                f"📈 Превышение: +{signal['change_percent']:.2f}%\n"
                f"🕒 Время: {signal['time'].strftime('%Y-%m-%d %H:%M')}\n"
                f"🔔 Условие: Закрытие > High предыдущей свечи + {self.buy_threshold_percent}%"
            )
        else:
            return (
                "🔻 **СИГНАЛ НА ПРОДАЖУ** 🔻\n"
                f"📉 {ticker_name} ({signal['ticker']})\n"
                f"💰 Текущая цена: {signal['price']:.2f} руб.\n"
                f"🎯 Преодолен уровень: {signal['threshold']:.2f} руб.\n"
                f"📊 Предыдущий low: {signal['prev_low']:.2f} руб.\n"
                f"📉 Снижение: -{signal['change_percent']:.2f}%\n"
                f"🕒 Время: {signal['time'].strftime('%Y-%m-%d %H:%M')}\n"
                f"🔔 Условие: Закрытие < Low предыдущей свечи - {self.sell_threshold_percent}%"
            )
    
    def format_single_status(self, df, ticker):
        """Форматирование статуса для одной акции"""
        current_candle = df.iloc[-1]
        prev_candle = df.iloc[-2]
        
        current_close = current_candle['close']
        prev_high = prev_candle['high']
        prev_low = prev_candle['low']
        
        buy_threshold = prev_high * (1 + self.buy_threshold_percent / 100)
        sell_threshold = prev_low * (1 - self.sell_threshold_percent / 100)
        
        buy_diff = ((current_close - buy_threshold) / buy_threshold) * 100
        sell_diff = ((sell_threshold - current_close) / sell_threshold) * 100
        
        ticker_name = self.ticker_names.get(ticker, ticker)
        
        # Эмодзи для визуализации
        buy_emoji = "🟢" if buy_diff >= 0 else "🔴"
        sell_emoji = "🟢" if sell_diff <= 0 else "🔴"
        
        return (
            f"{buy_emoji}{sell_emoji} {ticker_name}:\n"
            f"   💰 {current_close:.2f} руб. | "
            f"🔼 {buy_diff:+.1f}% | "
            f"🔽 {sell_diff:+.1f}%"
        )
    
    async def send_daily_report(self):
        """Ежедневный отчет"""
        report = (
            f"📋 **ЕЖЕДНЕВНЫЙ ОТЧЕТ**\n"
            f"📅 {datetime.now().strftime('%Y-%m-%d')}\n"
            f"📊 Всего проверок: {self.stats['total_checks']}\n"
            f"🎯 Сигналов найдено: {self.stats['signals_found']}\n"
            f"📈 Отслеживается акций: {len(self.tickers)}\n"
            f"⏰ Следующая проверка: 10:00 и 19:00\n"
            f"✅ Система работает стабильно"
        )
        await self.send_message(report)

async def main():
    try:
        bot = EnhancedMoexSignalBot(TELEGRAM_TOKEN, TELEGRAM_CHAT_ID)
        
        # Приветственное сообщение
        tickers_list = "\n".join([f"   • {bot.ticker_names[t]} ({t})" for t in bot.tickers])
        await bot.send_message(
            f"🤖 **УСИЛЕННЫЙ БОТ MOEX ЗАПУЩЕН** 🚀\n\n"
            f"📊 Отслеживаемые акции:\n{tickers_list}\n\n"
            f"⚙️ Настройки сигналов:\n"
            f"   • BUY: закрытие > предыдущий high + {bot.buy_threshold_percent}%\n"
            f"   • SELL: закрытие < предыдущий low - {bot.sell_threshold_percent}%\n\n"
            f"⏰ Проверка: 10:00 и 19:00 ежедневно"
        )
        
        # Первая проверка
        await bot.check_all_signals()
        
        # Планировщик
        schedule.every().day.at("10:00").do(lambda: asyncio.create_task(bot.check_all_signals()))
        schedule.every().day.at("19:00").do(lambda: asyncio.create_task(bot.check_all_signals()))
        schedule.every().day.at("09:00").do(lambda: asyncio.create_task(bot.send_daily_report()))
        
        logger.info("⏰ Усиленный бот запущен. Проверки в 10:00 и 19:00")
        
        while True:
            schedule.run_pending()
            await asyncio.sleep(60)
            
    except Exception as e:
        logger.error(f"❌ Ошибка запуска: {e}")

if __name__ == "__main__":
    asyncio.run(main())
