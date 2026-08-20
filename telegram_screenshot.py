"""Модуль для отправки скриншотов дашборда в Telegram"""
import asyncio
import os
from datetime import datetime
from playwright.async_api import async_playwright

async def take_dashboard_screenshot(
    symbol: str = "SiU6",
    timeframe: str = "10M",
    days: int = 2,
    output_path: str = None,
    filters: dict = None
) -> str:
    """Делает скриншот дашборда с нужными фильтрами"""
    if output_path is None:
        output_path = f'/tmp/futoi_{symbol}_{datetime.now().strftime("%Y%m%d_%H%M%S")}.png'
    
    from datetime import date, timedelta
    start_date = (date.today() - timedelta(days=days)).strftime('%Y-%m-%d')
    end_date = date.today().strftime('%Y-%m-%d')
    
    dashboard_url = os.getenv('DASHBOARD_URL', 'http://localhost:8501')
    
    # Формируем параметры URL
    params = [
        f"symbol={symbol}",
        f"timeframe={timeframe}",
        f"start_date={start_date}",
        f"end_date={end_date}",
        "hide_header=true",
        "hide_export=true",
    ]
    
    # Добавляем включенные фильтры
    if filters:
        for key, value in filters.items():
            if value:
                params.append(f"{key}=true")
    
    url = f"{dashboard_url}/?{'&'.join(params)}"
    
    print(f"📸 Открываю URL: {url}")
    
    async with async_playwright() as p:
        browser = await p.chromium.launch(
            headless=True,
            args=['--no-sandbox', '--disable-setuid-sandbox', '--disable-dev-shm-usage']
        )
        page = await browser.new_page(viewport={'width': 1280, 'height': 900})
        
        try:
            await page.goto(url, wait_until='networkidle', timeout=30000)
            await page.wait_for_timeout(6000)
            await page.evaluate("window.scrollTo(0, 300)")
            await page.wait_for_timeout(1000)
            await page.screenshot(path=output_path, full_page=False)
            print(f"✅ Скриншот сохранен: {output_path}")
        except Exception as e:
            print(f"❌ Ошибка скриншота: {e}")
            raise
        finally:
            await browser.close()
    
    return output_path

async def send_alert_screenshot(
    bot,
    chat_id: str,
    symbol: str,
    delta_contracts: int,
    timeframe: str = "5M"
):
    """Отправляет скриншот при срабатывании алерта"""
    try:
        # По умолчанию включаем все фильтры физлиц (buy/sell)
        default_filters = {
            'show_fiz_buy_plus': True,
            'show_fiz_sell_plus': True,
            'show_fiz_buy_minus': True,
            'show_fiz_sell_minus': True,
        }
        
        screenshot_path = await take_dashboard_screenshot(
            symbol=symbol,
            timeframe=timeframe,
            days=1,
            filters=default_filters
        )
        
        direction = " РОСТ" if delta_contracts > 0 else "📉 ПАДЕНИЕ"
        message = f"{direction} OI\n{symbol} {timeframe}\nΔ {delta_contracts:+d}"
        
        with open(screenshot_path, 'rb') as photo:
            await bot.send_photo(
                chat_id=chat_id,
                photo=photo,
                caption=message
            )
        
        os.remove(screenshot_path)
        
    except Exception as e:
        print(f"❌ Ошибка отправки скриншота: {e}")
        await bot.send_message(
            chat_id=chat_id,
            text=f"⚠️ Ошибка скриншота: {str(e)[:200]}"
        )
