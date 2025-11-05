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
from dotenv import load_dotenv
from moex_index_working import MoexIndexWorking

load_dotenv()

TELEGRAM_TOKEN = os.getenv('TELEGRAM_TOKEN')
TELEGRAM_CHAT_ID = os.getenv('TELEGRAM_CHAT_ID')

logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

class IndexSignalBot:
    def __init__(self, telegram_token, chat_id):
        self.telegram_token = telegram_token
        self.chat_id = chat_id
        
        # Индексы для отслеживания
        self.indexes = ['IMOEX']
        self.index_names = {
            'IMOEX': 'Индекс МосБиржи'
        }
        
        self.api = MoexIndexWorking()
        self.bot = Bot(token=telegram_token)
        
        self.stats = {
            'total_checks': 0,
            'signals_found': 0,
            'last_check': None
        }
    
    async def send_message(self, text):
        """Отправка сообщения в Telegram"""
        try:
            await self.bot.send_message(chat_id=self.chat_id, text=text)
            logger.info("✅ Сообщение отправлено в Telegram")
            return True
        except TelegramError as e:
            logger.error(f"❌ Ошибка отправки: {e}")
            return False
    
    def get_index_data(self, index):
        """Получение данных индекса"""
        # Пробуем получить свечные данные
        df = self.api.get_index_candles(index, days=5)
        if df is not None and len(df) >= 2:
            return df
        
        # Если свечные не работают, используем упрощенный метод
        df = self.api.get_index_simple_history(index, days=5)
        return df
    
    def calculate_signals(self, df, index):
        """Расчет сигналов для индекса"""
        if df is None or len(df) < 2:
            logger.warning(f"⚠️ Недостаточно данных для {index}")
            return None
            
        current_candle = df.iloc[-1]
        prev_candle = df.iloc[-2]
        
        signals = []
        current_close = current_candle['close']
        prev_high = prev_candle['high']
        prev_low = prev_candle['low']
        
        # Сигнал на покупку: текущая цена закрытия > предыдущий high + 0.5%
        buy_threshold = prev_high * 1.005
        if current_close > buy_threshold:
            change_percent = ((current_close - buy_threshold) / buy_threshold) * 100
            logger.info(f"🎯 {index}: Сформирован сигнал BUY")
            signals.append({
                'type': 'BUY',
                'index': index,
                'price': current_close,
                'threshold': buy_threshold,
                'change_percent': change_percent,
                'prev_high': prev_high,
                'time': df.index[-1]
            })
        
        # Сигнал на продажу: текущая цена закрытия < предыдущий low - 0.5%
        sell_threshold = prev_low * 0.995
        if current_close < sell_threshold:
            change_percent = ((sell_threshold - current_close) / sell_threshold) * 100
            logger.info(f"🎯 {index}: Сформирован сигнал SELL")
            signals.append({
                'type': 'SELL',
                'index': index,
                'price': current_close,
                'threshold': sell_threshold,
                'change_percent': change_percent,
                'prev_low': prev_low,
                'time': df.index[-1]
            })
        
        if not signals:
            logger.info(f"📊 {index}: Сигналов нет")
            
        return signals if signals else None
    
    async def check_all_signals(self):
        """Проверка сигналов для всех индексов"""
        self.stats['total_checks'] += 1
        self.stats['last_check'] = datetime.now()
        
        try:
            logger.info("🔍 Проверка сигналов индексов...")
            
            all_signals = []
            status_messages = []
            
            for index in self.indexes:
                df = self.get_index_data(index)
                if df is not None:
                    signals = self.calculate_signals(df, index)
                    
                    if signals:
                        all_signals.extend(signals)
                        self.stats['signals_found'] += len(signals)
                    else:
                        status_messages.append(self.format_status(df, index))
                else:
                    status_messages.append(f"❌ {index}: данные не получены")
            
            # Отправляем сигналы если есть
            if all_signals:
                await self.send_message("🚨 **ОБНАРУЖЕНЫ СИГНАЛЫ ИНДЕКСОВ** 🚨")
                for signal in all_signals:
                    message = self.format_signal_message(signal)
                    await self.send_message(message)
                    await asyncio.sleep(1)
            
            # Отправляем статус
            if status_messages:
                header = self.format_status_header(len(all_signals))
                full_status = header + "\n".join(status_messages)
                await self.send_message(full_status)
                
            logger.info(f"✅ Проверка завершена. Сигналов: {len(all_signals)}")
                
        except Exception as e:
            error_msg = f"❌ Ошибка при проверке сигналов: {str(e)}"
            logger.error(error_msg)
            await self.send_message(error_msg)
    
    def format_status_header(self, signals_count):
        """Форматирование заголовка статуса"""
        return (
            f"📊 **СТАТУС ИНДЕКСОВ**\n"
            f"🕒 {datetime.now().strftime('%Y-%m-%d %H:%M')}\n"
            f"🎯 Сигналов: {signals_count}\n"
            f"📋 Проверок: {self.stats['total_checks']}\n"
            f"────────────────────\n"
        )
    
    def format_signal_message(self, signal):
        """Форматирование сообщения о сигнале"""
        index_name = self.index_names.get(signal['index'], signal['index'])
        
        if signal['type'] == 'BUY':
            return (
                f"🚀 **СИГНАЛ НА ПОКУПКУ** 🚀\n"
                f"📈 {index_name}\n"
                f"💰 Текущее значение: {signal['price']:.2f}\n"
                f"🎯 Преодолен уровень: {signal['threshold']:.2f}\n"
                f"📊 Предыдущий high: {signal['prev_high']:.2f}\n"
                f"📈 Превышение: +{signal['change_percent']:.2f}%\n"
                f"🕒 Время: {signal['time'].strftime('%Y-%m-%d %H:%M')}\n"
                f"🔔 Условие: Закрытие > High предыдущей свечи + 0.5%"
            )
        else:
            return (
                f"🔻 **СИГНАЛ НА ПРОДАЖУ** 🔻\n"
                f"📉 {index_name}\n"
                f"💰 Текущее значение: {signal['price']:.2f}\n"
                f"🎯 Преодолен уровень: {signal['threshold']:.2f}\n"
                f"📊 Предыдущий low: {signal['prev_low']:.2f}\n"
                f"📉 Снижение: -{signal['change_percent']:.2f}%\n"
                f"🕒 Время: {signal['time'].strftime('%Y-%m-%d %H:%M')}\n"
                f"🔔 Условие: Закрытие < Low предыдущей свечи - 0.5%"
            )
    
    def format_status(self, df, index):
        """Форматирование статуса для индекса"""
        current_candle = df.iloc[-1]
        prev_candle = df.iloc[-2]
        
        current_close = current_candle['close']
        prev_high = prev_candle['high']
        prev_low = prev_candle['low']
        
        buy_threshold = prev_high * 1.005
        sell_threshold = prev_low * 0.995
        
        buy_diff = ((current_close - buy_threshold) / buy_threshold) * 100
        sell_diff = ((sell_threshold - current_close) / sell_threshold) * 100
        
        index_name = self.index_names.get(index, index)
        
        return (
            f"📈 {index_name}:\n"
            f"   💰 {current_close:.2f} | "
            f"🔼 {buy_diff:+.1f}% | "
            f"🔽 {sell_diff:+.1f}%"
        )
    
    async def send_daily_report(self):
        """Ежедневный отчет"""
        report = (
            f"📋 **ЕЖЕДНЕВНЫЙ ОТЧЕТ ИНДЕКСОВ**\n"
            f"📅 {datetime.now().strftime('%Y-%m-%d')}\n"
            f"📊 Проверок: {self.stats['total_checks']}\n"
            f"🎯 Сигналов: {self.stats['signals_found']}\n"
            f"📈 Отслеживается: {len(self.indexes)} индексов\n"
            f"⏰ Следующая проверка: 10:00 и 19:00\n"
            f"✅ Система работает"
        )
        await self.send_message(report)

async def main():
    try:
        bot = IndexSignalBot(TELEGRAM_TOKEN, TELEGRAM_CHAT_ID)
        
        # Приветственное сообщение
        indexes_list = ", ".join([f"{bot.index_names[i]}" for i in bot.indexes])
        await bot.send_message(
            f"🤖 **БОТ СИГНАЛОВ ИНДЕКСОВ MOEX ЗАПУЩЕН** 🚀\n\n"
            f"📈 Отслеживаем: {indexes_list}\n"
            f"⚙️ Логика сигналов:\n"
            f"   • BUY: закрытие > предыдущий high + 0.5%\n"
            f"   • SELL: закрытие < предыдущий low - 0.5%\n\n"
            f"⏰ Проверка: 10:00 и 19:00"
        )
        
        # Первая проверка
        await bot.check_all_signals()
        
        # Планировщик
        schedule.every().day.at("10:00").do(lambda: asyncio.create_task(bot.check_all_signals()))
        schedule.every().day.at("19:00").do(lambda: asyncio.create_task(bot.check_all_signals()))
        schedule.every().day.at("09:00").do(lambda: asyncio.create_task(bot.send_daily_report()))
        
        logger.info("⏰ Бот индексов запущен")
        
        while True:
            schedule.run_pending()
            await asyncio.sleep(60)
            
    except Exception as e:
        logger.error(f"❌ Ошибка запуска: {e}")

if __name__ == "__main__":
    asyncio.run(main())
