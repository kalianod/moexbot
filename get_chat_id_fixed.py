#!/usr/bin/env python3
import asyncio
import os
from dotenv import load_dotenv

async def get_chat_id():
    """Правильное получение Chat ID"""
    from telegram import Bot
    
    load_dotenv()
    token = os.getenv('TELEGRAM_TOKEN')
    
    if not token:
        token = input("Введите токен бота: ").strip()
    
    if not token:
        print("❌ Токен не может быть пустым")
        return
    
    try:
        bot = Bot(token=token)
        
        # Получаем информацию о боте
        bot_info = await bot.get_me()
        print(f"🤖 Бот: {bot_info.first_name} (@{bot_info.username})")
        
        # Получаем обновления
        updates = await bot.get_updates()
        
        if not updates:
            print("\n💬 Напишите боту ЛЮБОЕ сообщение в Telegram и нажмите Enter...")
            input()
            updates = await bot.get_updates()
        
        if updates:
            print("\n📋 Найденные чаты:")
            for i, update in enumerate(updates, 1):
                if update.message:
                    chat = update.message.chat
                    print(f"{i}. 👤 {chat.first_name or 'Unknown'} (ID: {chat.id})")
            
            if len(updates) == 1:
                chat_id = updates[0].message.chat.id
                print(f"\n✅ Автоматически выбран Chat ID: {chat_id}")
                return chat_id
            else:
                choice = input(f"\nВыберите чат (1-{len(updates)}): ").strip()
                if choice.isdigit() and 1 <= int(choice) <= len(updates):
                    chat_id = updates[int(choice)-1].message.chat.id
                    print(f"✅ Выбран Chat ID: {chat_id}")
                    return chat_id
        else:
            print("❌ Чаты не найдены. Убедитесь что:")
            print("1. Вы написали боту сообщение")
            print("2. Токен бота правильный")
            
    except Exception as e:
        print(f"❌ Ошибка: {e}")

async def main():
    chat_id = await get_chat_id()
    if chat_id:
        print(f"\n🎉 Ваш Chat ID: {chat_id}")
        
        # Обновляем .env файл
        env_path = os.path.join(os.path.dirname(__file__), '.env')
        with open(env_path, 'r') as f:
            content = f.read()
        
        # Заменяем Chat ID в файле
        lines = []
        for line in content.split('\n'):
            if line.startswith('TELEGRAM_CHAT_ID='):
                lines.append(f'TELEGRAM_CHAT_ID={chat_id}')
            else:
                lines.append(line)
        
        with open(env_path, 'w') as f:
            f.write('\n'.join(lines))
        
        print("✅ Файл .env обновлен!")
        print("\n🔧 Теперь запустите бота:")
        print("python3 moex_bot.py")

if __name__ == "__main__":
    asyncio.run(main())
