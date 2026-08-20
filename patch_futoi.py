import os

file_path = '/home/kalian/moexbot/moex_futoi_alert_multi.py'
with open(file_path, 'r', encoding='utf-8') as f:
    content = f.read()

# 1. Добавляем импорт sqlite3 и путь к БД
imports_to_add = "\nimport sqlite3\nDB_PATH = os.path.join(os.path.dirname(__file__), 'futoi.db')\n"
if 'import sqlite3' not in content:
    content = content.replace('import pandas as pd', 'import pandas as pd' + imports_to_add)

# 2. Добавляем функцию сохранения перед check_once
func_to_add = """
def save_to_database(symbol, df):
    if df.empty:
        return
    conn = sqlite3.connect(DB_PATH)
    try:
        df_copy = df.copy()
        df_copy['symbol'] = symbol
        df_copy.to_sql('futoi_data', conn, if_exists='append', index=False)
        print(f"[{symbol}] Saved {len(df_copy)} rows to DB")
    except Exception as e:
        print(f"[{symbol}] DB save error: {e}")
    finally:
        conn.close()
"""
if 'def save_to_database' not in content:
    content = content.replace('def check_once(symbol):', func_to_add + '\ndef check_once(symbol):')

# 3. Вызываем функцию сохранения внутри check_once
call_to_add = "    # [ИЗМЕНЕНИЕ]: Сохраняем данные в БД\n    save_to_database(symbol, df)\n"
if 'save_to_database(symbol, df)' not in content:
    content = content.replace('    log_line(f"[{symbol}] futoi FIZ rows={len(df)}", log_file)', '    log_line(f"[{symbol}] futoi FIZ rows={len(df)}", log_file)\n' + call_to_add)

with open(file_path, 'w', encoding='utf-8') as f:
    f.write(content)
print("Patch applied successfully.")
