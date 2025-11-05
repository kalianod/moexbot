#!/usr/bin/env python3
import asyncio
import aiohttp
import aiomoex
import pandas as pd
from datetime import datetime, timedelta

async def explore_moex():
    print("🔍 Исследование доступных данных на MOEX...")
    
    try:
        async with aiohttp.ClientSession() as session:
            # 1. Поиск индексов
            print("📈 Поиск индексов...")
            indexes = await aiomoex.find_securities(session, 'индекс')
            if indexes:
                print(f"✅ Найдено индексов: {len(indexes)}")
                for idx in indexes[:5]:  # Покажем первые 5
                    print(f"   - {idx.get('secid', 'N/A')}: {idx.get('shortname', 'N/A')}")
            
            # 2. Попробуем популярные акции
            print("\n📊 Популярные акции для тестирования:")
            test_tickers = ['SBER', 'GAZP', 'LKOH', 'YNDX', 'VTBR']
            
            for ticker in test_tickers:
                print(f"\n🔍 Проверяем {ticker}...")
                try:
                    data = await aiomoex.get_market_candles(
                        session,
                        ticker,
                        interval=24,
                        start=(datetime.now() - timedelta(days=5)).strftime('%Y-%m-%d'),
                        end=datetime.now().strftime('%Y-%m-%d')
                    )
                    if data:
                        df = pd.DataFrame(data)
                        print(f"✅ {ticker}: {len(df)} записей, последняя цена: {df['close'].iloc[-1] if len(df) > 0 else 'N/A'}")
                    else:
                        print(f"❌ {ticker}: данные не получены")
                except Exception as e:
                    print(f"❌ {ticker}: ошибка - {e}")
            
            # 3. Попробуем получить данные индекса через другой метод
            print("\n🔄 Альтернативные методы для индексов...")
            
            # Метод для индексов
            try:
                print("Попытка получить данные индекса через boards...")
                boards = await aiomoex.get_board_history(session, 'IMOEX')
                if boards:
                    df = pd.DataFrame(boards)
                    print(f"✅ Данные индекса через boards: {len(df)} записей")
                    if len(df) > 0:
                        print(df[['TRADEDATE', 'CLOSE']].tail(3))
                else:
                    print("❌ Данные через boards не получены")
            except Exception as e:
                print(f"❌ Ошибка boards: {e}")
                
    except Exception as e:
        print(f"❌ Общая ошибка: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    asyncio.run(explore_moex())
