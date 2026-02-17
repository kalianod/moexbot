import re

file_path = 'moexmomentumbot_sectors.py'

try:
    with open(file_path, 'r', encoding='utf-8') as f:
        content = f.read()

    # Исправление 1: Для логики ребалансировки (где ищем худшую позицию)
    # Было: entry_price = entry_data.get('entry_price', 0)
    # Стало: entry_price = float(entry_data.get('entry_price', 0))
    
    new_content = content.replace(
        "entry_price = entry_data.get('entry_price', 0)", 
        "entry_price = float(entry_data.get('entry_price', 0))"
    )

    # Исправление 2: Для обычной логики продажи
    # Было: entry_price = entry_data.get('entry_price', asset.current_price)
    # Стало: entry_price = float(entry_data.get('entry_price', asset.current_price))
    
    new_content = new_content.replace(
        "entry_price = entry_data.get('entry_price', asset.current_price)",
        "entry_price = float(entry_data.get('entry_price', asset.current_price))"
    )

    # Проверка, были ли изменения
    if content == new_content:
        print("⚠️ Изменения не внесены. Возможно, код уже исправлен или строки выглядят иначе.")
    else:
        with open(file_path, 'w', encoding='utf-8') as f:
            f.write(new_content)
        print("✅ Исправлено: entry_price теперь всегда будет числом (float). Ошибка '>' должна исчезнуть.")

except Exception as e:
    print(f"❌ Ошибка при обновлении файла: {e}")
