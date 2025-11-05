#!/usr/bin/env python3
import asyncio
import aiohttp
import aiomoex
import inspect
import pandas as pd
from datetime import datetime, timedelta

async def explore_aiomoex_methods():
    """Исследование всех доступных методов aiomoex"""
    print("🔍 Исследование доступных методов aiomoex...")
    
    # Получаем все функции из aiomoex
    functions = [name for name in dir(aiomoex) if not name.startswith('_') and callable(getattr(aiomoex, name))]
    
    print("📋 Доступные функции в aiomoex:")
    for func_name in sorted(functions):
        func = getattr(aiomoex, func_name)
        print(f"   📌 {func_name}")
    
    print("\n🧪 Тестирование методов для индексов...")
    
    try:
        async with aiohttp.ClientSession() as session:
            # Протестируем доступные методы
            test_tickers = ['IMOEX', 'SBER']  # индекс и акция для сравнения
            
            for ticker in test_tickers:
                print(f"\n🔍 Тестируем {ticker}:")
                
                # Метод 1: get_board_securities - может работать с индексами
                try:
                    print("   📊 get_board_securities...")
                    securities = await aiomoex.get_board_securities(session, ticker)
                    if securities:
                        print(f"      ✅ Найдено {len(securities)} записей")
                        if len(securities) > 0:
                            print(f"      📝 Первая запись: {list(securities[0].keys())}")
                    else:
                        print("      ❌ Данные не получены")
                except Exception as e:
                    print(f"      ❌ Ошибка: {e}")
                
                # Метод 2: find_securities - поиск инструментов
                try:
                    print("   🔎 find_securities...")
                    found = await aiomoex.find_securities(session, ticker)
                    if found:
                        print(f"      ✅ Найдено {len(found)} записей")
                        for item in found[:2]:  # Покажем первые 2
                            print(f"      📋 {item.get('secid', 'N/A')}: {item.get('shortname', 'N/A')}")
                    else:
                        print("      ❌ Ничего не найдено")
                except Exception as e:
                    print(f"      ❌ Ошибка: {e}")
                
                # Метод 3: get_market_candles - основной метод для свечей
                try:
                    print("   📈 get_market_candles...")
                    data = await aiomoex.get_market_candles(
                        session,
                        ticker,
                        interval=24,
                        start=(datetime.now() - timedelta(days=3)).strftime('%Y-%m-%d'),
                        end=datetime.now().strftime('%Y-%m-%d')
                    )
                    if data:
                        df = pd.DataFrame(data)
                        print(f"      ✅ Получено {len(df)} свечей")
                        if len(df) > 0:
                            print(f"      💹 Последняя цена: {df['close'].iloc[-1]}")
                    else:
                        print("      ❌ Данные не получены")
                except Exception as e:
                    print(f"      ❌ Ошибка: {e}")
                    
    except Exception as e:
        print(f"❌ Общая ошибка: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    asyncio.run(explore_aiomoex_methods())
