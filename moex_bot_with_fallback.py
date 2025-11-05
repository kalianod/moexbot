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

TELEGRAM_TOKEN = os.getenv('TELEGRAM_TOKEN')
TELEGRAM_CHAT_ID = os.getenv('TELEGRAM_CHAT_ID')

logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

class SmartMoexSignalBot:
    def __init__(self, telegram_token, chat_id):
        self.telegram_token = telegram_token
        self.chat_id = chat_id
        
        # Инструменты для отслеживания (акции + попытка индексов)
        self.tickers = ['SBER', 'GAZP', 'LKOH', 'VTBR']  # Рабочие акции
        self.indexes = ['IMOEX', 'RTSI']  # Пробуем индексы
        
        self.ticker_names = {
            'SBER': 'Сбербанк', 'GAZP': 'Газпром', 'LKOH': 'Лукойл', 'VTBR': 'ВТБ',
            'IMOEX': 'Индекс МосБиржи', 'RTSI': 'Индекс РТС'
        }
        
        self.check_settings()
        self.bot = Bot(token=telegram_token)
    
    def check_settings(self):
        """Проверка настроек"""
        if not self.telegram_token or not self.chat_id:
            raise ValueError("❌ Проверьте TELEGRAM_TOKEN и TELEGRAM_CHAT_ID в .env")
        logger.info("✅ Настройки загружены")
    
    async def send_message(self, text):
        """Отправка сообщения в Telegram"""
        try:
            await self.bot.send_message(chat_id=self.chat_id, text=text)
            return True
        except TelegramError as e:
            logger.error(f"❌ Ошибка отправки: {e}")
            return False
    
    async def get_data(self, symbol, is_index=False):
        """Универсальный метод получения данных"""
        try:
            end_date = datetime.now()
            start_date = end_date - timedelta(days=10 if is_index else 5)
            
            async with aiohttp.ClientSession() as session:
                data = await aiomoex.get_market_candles(
                    session,
                    symbol,
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
                        logger.info(f"✅ {symbol}: {len(df)} записей")
                        return df
                
                logger.warning(f"⚠️ {symbol}: данные не получены")
                return None
                
        except Exception as e:
            logger.error(f"❌ Ошибка {symbol}: {e}")
            return None
    
    def calculate_signals(self, df, symbol):
        """Расчет сигналов"""
        if df is None or len(df) < 2:
            return None
            
        current = df.iloc[-1]
        prev = df.iloc[-2]
        
        signals = []
        current_close = current['close']
        prev_high = prev['high']
        prev_low = prev['low']
        
        # BUY сигнал
        buy_threshold = prev_high * 1.005
        if current_close > buy_threshold:
            change = ((current_close - buy_threshold) / buy_threshold) * 100
            signals.append({
                'type': 'BUY', 'symbol': symbol, 'price': current_close,
                'threshold': buy_threshold, 'change': change,
                'prev_high': prev_high, 'time': df.index[-1]
            })
        
        # SELL сигнал
        sell_threshold = prev_low * 0.995
        if current_close < sell_threshold:
            change = ((sell_threshold - current_close) / sell_threshold) * 100
            signals.append({
                'type': 'SELL', 'symbol': symbol, 'price': current_close,
                'threshold': sell_threshold, 'change': change,
                'prev_low': prev_low, 'time': df.index[-1]
            })
        
        return signals if signals else None
    
    async def check_all_signals(self):
        """Проверка всех инструментов"""
        try:
            all_signals = []
            status_messages = []
            
            # Проверяем акции (гарантированно работают)
            for ticker in self.tickers:
                df = await self.get_data(ticker, is_index=False)
                if df is not None:
                    signals = self.calculate_signals(df, ticker)
                    if signals:
                        all_signals.extend(signals)
                    else:
                        status_messages.append(self.format_status(df, ticker))
                else:
                    status_messages.append(f"❌ {ticker}: данные не получены")
            
            # Пробуем индексы (может не работать)
            for index in self.indexes:
                try:
                    df = await self.get_data(index, is_index=True)
                    if df is not None:
                        status_messages.append(f"📈 {index}: {df['close'].iloc[-1]:.2f} ✅")
                    else:
                        status_messages.append(f"📈 {index}: недоступен ❌")
                except:
                    status_messages.append(f"📈 {index}: ошибка получения ❌")
            
            # Отправляем результаты
            if all_signals:
                for signal in all_signals:
                    await self.send_message(self.format_signal(signal))
            
            if status_messages:
                header = "📊 **СТАТУС**\n🕒 " + datetime.now().strftime('%Y-%m-%d %H:%M') + "\n────────────────────\n"
                await self.send_message(header + "\n".join(status_messages))
                
        except Exception as e:
            logger.error(f"❌ Ошибка проверки: {e}")
            await self.send_message(f"❌ Ошибка системы: {e}")
    
    def format_signal(self, signal):
        """Форматирование сигнала"""
        name = self.ticker_names.get(signal['symbol'], signal['symbol'])
        if signal['type'] == 'BUY':
            return f"🚀 **BUY {name}**\n�� {signal['price']:.2f} > 🎯 {signal['threshold']:.2f}\n📈 +{signal['change']:.2f}%"
        else:
            return f"🔻 **SELL {name}**\n💰 {signal['price']:.2f} < 🎯 {signal['threshold']:.2f}\n📉 -{signal['change']:.2f}%"
    
    def format_status(self, df, symbol):
        """Форматирование статуса"""
        current = df.iloc[-1]
        prev = df.iloc[-2]
        current_close = current['close']
        buy_threshold = prev['high'] * 1.005
        sell_threshold = prev['low'] * 0.995
        buy_diff = ((current_close - buy_threshold) / buy_threshold) * 100
        sell_diff = ((sell_threshold - current_close) / sell_threshold) * 100
        
        name = self.ticker_names.get(symbol, symbol)
        return f"📈 {name}: {current_close:.2f} | 🔼 {buy_diff:+.1f}% | 🔽 {sell_diff:+.1f}%"

async def main():
    try:
        bot = SmartMoexSignalBot(TELEGRAM_TOKEN, TELEGRAM_CHAT_ID)
        await bot.send_message("🤖 Умный бот MOEX запущен! 📊")
        await bot.check_all_signals()
        
        schedule.every().day.at("10:00").do(lambda: asyncio.create_task(bot.check_all_signals()))
        schedule.every().day.at("19:00").do(lambda: asyncio.create_task(bot.check_all_signals()))
        
        logger.info("⏰ Бот запущен")
        while True:
            schedule.run_pending()
            await asyncio.sleep(60)
            
    except Exception as e:
        logger.error(f"❌ Ошибка: {e}")

if __name__ == "__main__":
    asyncio.run(main())
