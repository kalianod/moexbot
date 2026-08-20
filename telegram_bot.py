"""Telegram-бот для FUTOI Dashboard"""
import asyncio
import os
import sqlite3
from datetime import datetime
from dotenv import load_dotenv
from telegram import Update
from telegram.ext import Application, CommandHandler, ContextTypes
from telegram_screenshot import take_dashboard_screenshot, send_alert_screenshot

load_dotenv()

BOT_TOKEN = os.getenv('TELEGRAM_TOKEN')
CHAT_ID = os.getenv('TELEGRAM_CHAT_ID')
DASHBOARD_URL = os.getenv('DASHBOARD_URL', 'http://localhost:8501')

user_settings = {}

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "👋 *FUTOI Dashboard Bot*\n\n"
        "📋 *Команды:*\n"
        "/help - Справка\n"
        "/screenshot [SiU6/CRU6] [ТФ] [дни]\n"
        "/alert [вкл/выкл/статус]\n"
        "/status - Статус системы\n"
        "/settings - Настройки\n\n"
        "*Авто-алерты:* при изменении OI физлиц >500",
        parse_mode='Markdown'
    )

async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "📊 *Справка*\n\n"
        "/screenshot SiU6 1H 2\n"
        "  Скриншот SiU6, 1 час, 2 дня\n\n"
        "/alert включить\n"
        "/alert выключить\n"
        "/alert статус\n\n"
        "/status - проверка системы\n"
        "/settings - текущие настройки",
        parse_mode='Markdown'
    )

async def screenshot(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    
    if str(user_id) != CHAT_ID:
        await update.message.reply_text("❌ Доступ запрещен")
        return
    
    args = context.args
    symbol = args[0] if len(args) > 0 else user_settings.get(user_id, {}).get('symbol', 'SiU6')
    timeframe = args[1] if len(args) > 1 else user_settings.get(user_id, {}).get('timeframe', '10M')
    days = int(args[2]) if len(args) > 2 else user_settings.get(user_id, {}).get('days', 2)
    
    # Нормализация регистра
    symbol_normalized = symbol.upper()
    if symbol_normalized not in ['SIU6', 'CRU6']:
        await update.message.reply_text("❌ Неверный символ. Доступны: SiU6, CRU6")
        return
    
    msg = await update.message.reply_text(f"📸 Делаю скриншот {symbol_normalized} ({timeframe})...")
    
    try:
        screenshot_path = await take_dashboard_screenshot(
            symbol=symbol_normalized,
            timeframe=timeframe,
            days=days,
            filters={
                'show_fiz_buy_plus': True,
                'show_fiz_sell_plus': True,
                'show_fiz_buy_minus': True,
                'show_fiz_sell_minus': True,
            }
        )
        
        # Короткая подпись
        caption = f" {symbol_normalized} ({timeframe})\n📅 {days} дн."
        
        with open(screenshot_path, 'rb') as photo:
            await update.message.reply_photo(
                photo=photo,
                caption=caption
            )
        
        await msg.delete()
        os.remove(screenshot_path)
        
    except Exception as e:
        await msg.edit_text(f"❌ Ошибка: {str(e)[:200]}")

async def alert_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    
    if str(user_id) != CHAT_ID:
        await update.message.reply_text("❌ Доступ запрещен")
        return
    
    args = context.args
    if not args:
        await update.message.reply_text(
            "🔔 *Управление алертами*\n\n"
            "/alert включить\n"
            "/alert выключить\n"
            "/alert статус",
            parse_mode='Markdown'
        )
        return
    
    action = args[0].lower()
    if action in ['включить', 'on', 'enable']:
        user_settings[user_id] = user_settings.get(user_id, {})
        user_settings[user_id]['alerts_enabled'] = True
        await update.message.reply_text("✅ Алерты включены")
    elif action in ['выключить', 'off', 'disable']:
        if user_id in user_settings:
            user_settings[user_id]['alerts_enabled'] = False
        await update.message.reply_text("❌ Алерты выключены")
    elif action == 'статус':
        enabled = user_settings.get(user_id, {}).get('alerts_enabled', True)
        status = "✅ Включены" if enabled else "❌ Выключены"
        await update.message.reply_text(f"🔔 Алерты: {status}")
    else:
        await update.message.reply_text("❌ Неверная команда")

async def status(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    
    if str(user_id) != CHAT_ID:
        await update.message.reply_text("❌ Доступ запрещен")
        return
    
    import urllib.request
    try:
        urllib.request.urlopen(f"{DASHBOARD_URL}", timeout=5)
        dashboard_status = "🟢 Online"
    except:
        dashboard_status = "🔴 Offline"
    
    try:
        conn = sqlite3.connect('/home/kalian/moexbot/futoi.db')
        cursor = conn.execute("SELECT COUNT(*) FROM futoi_data")
        count = cursor.fetchone()[0]
        conn.close()
        db_status = f"🟢 Online ({count:,} записей)"
    except Exception as e:
        db_status = f"🔴 Offline"
    
    await update.message.reply_text(
        f"📊 *Статус*\n\n"
        f"️ Дашборд: {dashboard_status}\n"
        f"💾 БД: {db_status}\n"
        f"�� Бот: 🟢 Online",
        parse_mode='Markdown'
    )

async def settings_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    settings = user_settings.get(user_id, {
        'symbol': 'SiU6',
        'timeframe': '10M',
        'days': 2,
        'alerts_enabled': True
    })
    
    await update.message.reply_text(
        "⚙️ *Настройки*\n\n"
        f"📊 Символ: {settings.get('symbol', 'SiU6')}\n"
        f"⏰ ТФ: {settings.get('timeframe', '10M')}\n"
        f"📅 Период: {settings.get('days', 2)} дн.\n"
        f" Алерты: {'✅' if settings.get('alerts_enabled', True) else '❌'}",
        parse_mode='Markdown'
    )

async def send_auto_alert(symbol: str, delta_contracts: int):
    """Автоматическая отправка алерта"""
    if not BOT_TOKEN or not CHAT_ID:
        print("❌ Telegram токен не настроен")
        return
    
    app = Application.builder().token(BOT_TOKEN).build()
    
    try:
        await send_alert_screenshot(
            bot=app.bot,
            chat_id=CHAT_ID,
            symbol=symbol,
            delta_contracts=delta_contracts,
            timeframe="5M"
        )
        print(f"✅ Авто-алерт: {symbol} ({delta_contracts:+d})")
    except Exception as e:
        print(f"❌ Ошибка авто-алерта: {e}")

def main():
    if not BOT_TOKEN:
        print(" TELEGRAM_TOKEN не найден")
        return
    
    print("🤖 Запуск Telegram бота...")
    print(f"📱 Chat ID: {CHAT_ID}")
    
    application = Application.builder().token(BOT_TOKEN).build()
    
    application.add_handler(CommandHandler("start", start))
    application.add_handler(CommandHandler("help", help_command))
    application.add_handler(CommandHandler("screenshot", screenshot))
    application.add_handler(CommandHandler("alert", alert_command))
    application.add_handler(CommandHandler("status", status))
    application.add_handler(CommandHandler("settings", settings_command))
    
    print("✅ Бот запущен")
    application.run_polling(allowed_updates=Update.ALL_TYPES)

if __name__ == '__main__':
    main()
