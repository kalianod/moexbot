#!/usr/bin/env python3
import asyncio
import aiohttp
import aiomoex
import pandas as pd

async def explore_boards():
    """Исследование доступных торговых площадок"""
    print("🔍 Исследование торговых площадок...")
    
    try:
        async with aiohttp.ClientSession() as session:
            # Получим список всех досок
            print("📋 Получение списка досок...")
            boards = await aiomoex.get_boards(session)
            
            if boards:
                df_boards = pd.DataFrame(boards)
                print(f"✅ Найдено {len(df_boards)} торговых площадок")
                
                # Покажем разные типы площадок
                print("\n📊 Типы площадок:")
                board_types = df_boards['board_group_id'].unique()
                for board_type in sorted(board_types)[:10]:  # Покажем первые 10
                    count = len(df_boards[df_boards['board_group_id'] == board_type])
                    print(f"   🏷️  {board_type}: {count} площадок")
                
                # Поиск площадок для индексов
                print("\n🔍 Поиск площадок для индексов...")
                index_boards = df_boards[df_boards['board_group_id'].str.contains('index', case=False, na=False)]
                if len(index_boards) > 0:
                    print("✅ Найдены площадки для индексов:")
                    for _, board in index_boards.head().iterrows():
                        print(f"   📈 {board['boardid']}: {board.get('title', 'N/A')}")
                else:
                    print("❌ Не найдено специализированных площадок для индексов")
                
                # Проверим основные площадки
                main_boards = ['TQBR', 'SMAL', 'TQTF']  # Основные площадки для акций и фондов
                print(f"\n🔍 Проверка основных площадок {main_boards}...")
                
                for board in main_boards:
                    print(f"\n🏷️  Площадка {board}:")
                    try:
                        securities = await aiomoex.get_board_securities(session, board=board)
                        if securities:
                            df_sec = pd.DataFrame(securities)
                            print(f"   ✅ {len(df_sec)} инструментов")
                            # Покажем несколько инструментов
                            sample = df_sec[['SECID', 'SHORTNAME']].head(3)
                            for _, sec in sample.iterrows():
                                print(f"      📊 {sec['SECID']}: {sec['SHORTNAME']}")
                        else:
                            print("   ❌ Инструменты не найдены")
                    except Exception as e:
                        print(f"   ❌ Ошибка: {e}")
                        
            else:
                print("❌ Не удалось получить список площадок")
                
    except Exception as e:
        print(f"❌ Общая ошибка: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    asyncio.run(explore_boards())
