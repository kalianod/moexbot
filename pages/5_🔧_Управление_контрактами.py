import streamlit as st
import subprocess
import sys
import os
import json
from pathlib import Path

st.set_page_config(page_title="Управление контрактами", page_icon="🔧", layout="wide")

st.title("🔧 Управление контрактами")
st.markdown("Эта страница позволяет безопасно добавлять новые или удалять старые фьючерсные контракты из системы сбора данных, аналитики и дашборда.")

# Пути
BASE_DIR = Path(__file__).parent.parent
MANAGE_SCRIPT = BASE_DIR / "manage_symbols.py"
VENV_PYTHON = BASE_DIR / "venv" / "bin" / "python"
HISTORY_FILE = BASE_DIR / "symbols_history.json"

def run_manage_command(args):
    """Запускает manage_symbols.py и возвращает результат."""
    cmd = [str(VENV_PYTHON), str(MANAGE_SCRIPT)] + args
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=300, cwd=str(BASE_DIR))
        if result.returncode == 0:
            return json.loads(result.stdout)
        else:
            return {"error": result.stderr or "Неизвестная ошибка"}
    except Exception as e:
        return {"error": str(e)}

# 1. Получаем текущий список контрактов
list_result = run_manage_command(["list"])
if "error" in list_result:
    st.error(f"Ошибка получения списка: {list_result['error']}")
    st.stop()

current_symbols = list_result.get("symbols", [])

st.info(f"📊 **Текущие активные контракты:** {', '.join(current_symbols) if current_symbols else 'Нет'}")

st.divider()

# 2. Секция добавления
st.subheader("➕ Добавить новый контракт")
st.markdown("Будет выполнен патч всех файлов конфигурации и загрузка исторических данных (свечи, tradestats, futoi).")

col1, col2 = st.columns([1, 2])
with col1:
    new_symbol = st.text_input("Тикер контракта (например, NGX6):", max_chars=10).strip().upper()
with col2:
    backfill_days = st.slider("Дней истории для загрузки:", min_value=1, max_value=60, value=5)

if st.button("Добавить контракт", type="primary", disabled=not new_symbol):
    if new_symbol in current_symbols:
        st.warning(f"Контракт `{new_symbol}` уже присутствует в системе.")
    else:
        with st.spinner(f"Добавление {new_symbol} и загрузка истории за {backfill_days} дней... (это может занять 1-3 минуты)"):
            result = run_manage_command(["add", new_symbol, str(backfill_days)])
            
            if "error" in result and result["errors"]:
                st.error("❌ Ошибки при добавлении:")
                for err in result["errors"]:
                    st.error(err)
            else:
                st.success(f"✅ Контракт `{new_symbol}` успешно добавлен!")
                st.info("⚠️ **Важно:** Для начала сбора данных в реальном времени необходимо перезапустить сервис коллектора:")
                st.code("sudo systemctl restart futoi-collector.service", language="bash")
                st.info("После перезапуска данные начнут поступать, а сигналы будут рассчитываться автоматически каждые 5 минут.")

st.divider()

# 3. Секция удаления
st.subheader("➖ Удалить существующий контракт")
st.markdown("Контракт будет удалён из всех файлов конфигурации. Опционально можно очистить его исторические данные из базы.")

if not current_symbols:
    st.warning("Нет контрактов для удаления.")
else:
    col1, col2 = st.columns([1, 2])
    with col1:
        symbol_to_remove = st.selectbox("Выберите контракт для удаления:", current_symbols)
    with col2:
        clean_db = st.checkbox("🗑️ Очистить исторические данные этого контракта из базы данных (futoi_data, bar_stats и др.)", value=False)
    
    st.markdown("---")
    st.warning("⚠️ Для подтверждения удаления введите название контракта в поле ниже:")
    confirm_input = st.text_input(f"Введите '{symbol_to_remove}' для подтверждения:", key="confirm_del").strip().upper()
    
    delete_disabled = (confirm_input != symbol_to_remove)
    
    if st.button("Удалить контракт", type="secondary", disabled=delete_disabled):
        with st.spinner(f"Удаление {symbol_to_remove}..."):
            args = ["remove", symbol_to_remove]
            if clean_db:
                args.append("--clean-db")
            
            result = run_manage_command(args)
            
            if "error" in result and result["errors"]:
                st.error("❌ Ошибки при удалении:")
                for err in result["errors"]:
                    st.error(err)
            else:
                st.success(f"✅ Контракт `{symbol_to_remove}` успешно удалён из конфигурации." + (" Данные из БД также очищены." if clean_db else ""))
                st.info("⚠️ **Важно:** Для применения изменений необходимо перезапустить сервис коллектора:")
                st.code("sudo systemctl restart futoi-collector.service", language="bash")

st.divider()

# 4. История операций
st.subheader("📜 История операций")
if HISTORY_FILE.exists():
    try:
        with open(HISTORY_FILE, 'r', encoding='utf-8') as f:
            history = json.load(f)
        
        # Показываем последние 10 операций в обратном порядке
        for item in reversed(history[-10:]):
            ts = item.get('timestamp', 'Unknown')[:19].replace('T', ' ')
            action = item.get('action', 'Unknown')
            sym = item.get('symbol', 'Unknown')
            
            icon = "➕" if "ADD" in action else "➖"
            color = "green" if "ADD" in action else "red"
            
            with st.expander(f"{icon} **{action}** `{sym}` ({ts})"):
                st.json(item.get('details', {}))
    except Exception as e:
        st.error(f"Не удалось прочитать историю: {e}")
else:
    st.info("История операций пока пуста.")
