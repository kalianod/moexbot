#!/usr/bin/env python3
import asyncio
import logging
from telegram import Bot
from telegram.error import TelegramError
import pandas as pd
from moexalgo import Market, Ticker
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

class MoexSignalBot:
    def __init__(self, telegram_token, chat_id):
        self.bot = Bot(token=telegram_token)
        self.chat_id = chat_id
        self.index_ticker = 'IMOEX'  # Индекс Мосбиржи
        
    async def send_message(self, text):
        """Отправка сообщения в Telegram"""
        try:
            await self.bot.send_message(chat_id=self.chat_id, text=text)
            logger.info(f"Сообщение отправлено: {text}")
        except TelegramError as e:
            logger.error(f"Ошибка отправки сообщения: {e}")
    
    def get_index_data(self, days=10):
        """Получение данных по индексу Мосбиржи"""
        try:
            # Используем moexalgo для получения данных
            market = Market('index')
            imoex = Ticker(self.index_ticker, market=market)
            
            # Получаем дневные свечи
            data = imoex.candles(period='D', limit=days)
            
            if data is None or len(data) == 0:
                logger.error("Не удалось получить данные")
                return None
                
            # Конвертируем в DataFrame
            df = pd.DataFrame(data)
            df['date'] = pd.to_datetime(df['begin'])
            df.set_index('date', inplace=True)
            
            # Переименовываем колонки для удобства
            df = df.rename(columns={
                'open': 'Open',
                'high': 'High', 
                'low': 'Low',
                'close': 'Close',
                'volume': 'Volume'
            })
            
            return df.sort_index()
            
        except Exception as e:
            logger.error(f"Ошибка получения данных: {e}")
            return None
    
    def calculate_signals(self, df):
        """Расчет торговых сигналов"""
        if df is None or len(df) < 2:
            return None
            
        # Берем последние две свечи
        current_candle = df.iloc[-1]
        prev_candle = df.iloc[-2]
        
        signals = []
        
        # Сигнал на покупку: текущая цена закрытия > предыдущий high + 0.5%
        buy_threshold = prev_candle['High'] * 1.005
        if current_candle['Close'] > buy_threshold:
            signals.append({
                'type': 'BUY',
                'price': current_candle['Close'],
                'threshold': buy_threshold,
                'time': df.index[-1]
            })
        
        # Сигнал на продажу: текущая цена закрытия < предыдущий low - 0.5%
        sell_threshold = prev_candle['Low'] * 0.995
        if current_candle['Close'] < sell_threshold:
            signals.append({
                'type': 'SELL', 
                'price': current_candle['Close'],
                'threshold': sell_threshold,
                'time': df.index[-1]
            })
            
        return signals if signals else None
    
    async def check_and_send_signals(self):
        """Проверка и отправка сигналов"""
        try:
            logger.info("Проверка сигналов...")
            
            # Получаем данные
            df = self.get_index_data()
            if df is None:
                await self.send_message("❌ Ошибка получения данных индекса ММВБ")
                return
            
            # Рассчитываем сигналы
            signals = self.calculate_signals(df)
            
            if signals:
                for signal in signals:
                    message = self.format_signal_message(signal, df)
                    await self.send_message(message)
            else:
                logger.info("Сигналов нет")
                
        except Exception as e:
            error_msg = f"❌ Ошибка при проверке сигналов: {str(e)}"
            logger.error(error_msg)
            await self.send_message(error_msg)
    
    def format_signal_message(self, signal, df):
        """Форматирование сообщения о сигнале"""
        current_candle = df.iloc[-1]
        prev_candle = df.iloc[-2]
        
        if signal['type'] == 'BUY':
            return (
                "🚀 **СИГНАЛ НА ПОКУПКУ** 🚀\n"
                f"📈 Индекс: IMOEX\n"
                f"💰 Текущая цена: {current_candle['Close']:.2f}\n"
                f"🎯 Преодолен уровень: {signal['threshold']:.2f}\n"
                f"📊 Предыдущий high: {prev_candle['High']:.2f}\n"
                f"🕒 Время: {signal['time'].strftime('%Y-%m-%d %H:%M')}\n"
                f"🔔 Условие: Закрытие > High предыдущей свечи + 0.5%"
            )
        else:
            return (
                "🔻 **СИГНАЛ НА ПРОДАЖУ** 🔻\n"
                f"📉 Индекс: IMOEX\n"
                f"💰 Текущая цена: {current_candle['Close']:.2f}\n"
                f"🎯 Преодолен уровень: {signal['threshold']:.2f}\n"
                f"📊 Предыдущий low: {prev_candle['Low']:.2f}\n"
                f"🕒 Время: {signal['time'].strftime('%Y-%m-%d %H:%M')}\n"
                f"🔔 Условие: Закрытие < Low предыдущей свечи - 0.5%"
            )

async def main():
    # Проверяем конфигурацию
    if not TELEGRAM_TOKEN:
        print("❌ TELEGRAM_TOKEN не найден. Проверьте файл .env")
        return
        
    if not TELEGRAM_CHAT_ID:
        print("❌ TELEGRAM_CHAT_ID не найден. Проверьте файл .env")
        return
    
    # Создаем бота
    bot = MoexSignalBot(TELEGRAM_TOKEN, TELEGRAM_CHAT_ID)
    
    # Отправляем тестовое сообщение
    await bot.send_message("🤖 Бот сигналов Мосбиржи запущен! (VENV + .env)")
    
    # Первая проверка
    await bot.check_and_send_signals()
    
    # Планировщик для регулярных проверок
    schedule.every().day.at("19:00").do(
        lambda: asyncio.create_task(bot.check_and_send_signals())
    )
    
    # Бесконечный цикл
    while True:
        schedule.run_pending()
        await asyncio.sleep(60)

if __name__ == "__main__":
    asyncio.run(main())