#!/usr/bin/env python3
"""
[NEW 2026-08-30] Страница "Хедж сигналы" для FUTOI Dashboard
Показывает сигналы хеджирования по индексам: ОТКРЫТЬ / ДЕРЖАТЬ / ЗАКРЫТЬ / ---
"""
import streamlit as st
import pandas as pd
import sqlite3
import json
from datetime import datetime, date, timedelta
from pathlib import Path

st.set_page_config(page_title="Хедж сигналы", layout="wide", initial_sidebar_state="collapsed")

# [FIX] Отключаем кэш для этой страницы
st_autorefresh = None
try:
    from streamlit_autorefresh import st_autorefresh
    st_autorefresh(interval=5 * 60 * 1000, key="hedge-dashboard-refresh")
except ImportError:
    pass

# ==================== КОНФИГУРАЦИЯ ====================
DB_PATH = '/home/kalian/moexbot/hedgebot.db'
HEDGE_CONFIG = {
    'IMOEX': {'name': 'Индекс МосБиржи', 'logic': 'standard'},
    'MCFTR': {'name': 'Индекс МосБиржи полный', 'logic': 'standard'},
    'CNYRUB_TOM': {'name': 'Юань/Рубль', 'logic': 'inverse'},
    'GLDRUB_TOM': {'name': 'Золото/Рубль', 'logic': 'inverse'},
}


# ==================== ФУНКЦИИ ЧТЕНИЯ ДАННЫХ ====================
def load_states():
    """Загрузка текущих состояний бота из SQLite"""
    try:
        conn = sqlite3.connect(DB_PATH)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        cursor.execute("SELECT states_json FROM hedge_states WHERE id = 1")
        row = cursor.fetchone()
        conn.close()
        
        if row and row['states_json']:
            return json.loads(row['states_json'])
        return {}
    except Exception as e:
        st.error(f"Ошибка загрузки состояний: {e}")
        return {}


def load_signals(limit=500):
    """Загрузка истории сигналов из SQLite"""
    try:
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        cursor.execute("""
            SELECT index_name, signal, price, date, time, created_at
            FROM hedge_signals
            ORDER BY created_at DESC
            LIMIT ?
        """, (limit,))
        rows = cursor.fetchall()
        conn.close()
        
        df = pd.DataFrame(rows, columns=['index_name', 'signal', 'price', 'date', 'time', 'created_at'])
        return df
    except Exception as e:
        st.error(f"Ошибка загрузки сигналов: {e}")
        return pd.DataFrame()


def get_display_status(signal, position):
    """Определение статуса для отображения (как в боте)"""
    if signal and "ОТКРЫТЬ" in signal:
        return "ОТКРЫТЬ ХЕДЖ", "🟢"
    elif signal and "ЗАКРЫТЬ" in signal:
        return "ЗАКРЫТЬ ХЕДЖ", "🔴"
    elif signal == "НЕТ СИГНАЛА" and position == 'hedge_open':
        return "ДЕРЖАТЬ ХЕДЖ", "🟡"
    else:
        return "---", "⚪"


def get_status_color(status):
    """Цвет для статуса"""
    if "ОТКРЫТЬ" in status:
        return "success"
    elif "ЗАКРЫТЬ" in status:
        return "error"
    elif "ДЕРЖАТЬ" in status:
        return "warning"
    else:
        return "secondary"


# ==================== ЗАГРУЗКА ДАННЫХ ====================
states = load_states()
signals_df = load_signals(limit=500)

# ==================== ЗАГОЛОВОК ====================
st.markdown("## 🛡️ Хедж сигналы")
st.markdown(f"📅 **Обновлено:** {datetime.now().strftime('%d.%m.%Y %H:%M:%S')}")
st.markdown("---")

# ==================== СЕКЦИЯ 1: КАРТОЧКИ ТЕКУЩИХ СТАТУСОВ ====================
st.markdown("### 📊 Текущие статусы")

index_cards = []
for index, config in HEDGE_CONFIG.items():
    if index not in states:
        continue
    
    state = states[index]
    current_signal = state.get('current_signal', 'НЕТ СИГНАЛА')
    last_price = state.get('last_price', 0)
    position = state.get('position', None)
    last_update = state.get('last_update', '')
    
    display_status, emoji = get_display_status(current_signal, position)
    status_color = get_status_color(display_status)
    
    if last_update:
        try:
            if isinstance(last_update, str):
                update_dt = datetime.fromisoformat(last_update)
                update_str = update_dt.strftime('%H:%M')
            else:
                update_str = str(last_update)
        except:
            update_str = "N/A"
    else:
        update_str = "N/A"
    
    index_cards.append({
        'index': index,
        'name': config['name'],
        'signal': current_signal,
        'price': last_price,
        'position': position,
        'display_status': display_status,
        'emoji': emoji,
        'status_color': status_color,
        'last_update': update_str
    })

if index_cards:
    cols = st.columns(len(index_cards))
    
    for i, card in enumerate(index_cards):
        with cols[i]:
            st.markdown(f"**{card['emoji']} {card['name']}**")
            st.metric(label="Цена", value=f"{card['price']:.2f}")
            st.markdown(f"**Статус:** {card['display_status']}")
            st.caption(f"🕒 {card['last_update']}")
    
    st.markdown("---")
else:
    st.warning("⚠️ Нет данных о состояниях индексов")

# ==================== СЕКЦИЯ 3: ИСТОРИЯ СИГНАЛОВ С ФИЛЬТРАМИ ====================

# [FIX] Создаём колонку с человекочитаемыми именами индексов
signals_df['index_display'] = signals_df['index_name'].map(
    lambda x: HEDGE_CONFIG.get(x, {}).get('name', x)
)

st.markdown("---")
st.markdown("### 🔍 История сигналов")

col1, col2, col3 = st.columns([2, 2, 1])

with col1:
    available_indexes = ['Все'] + [HEDGE_CONFIG.get(idx, {}).get('name', idx) 
                                    for idx in HEDGE_CONFIG.keys() 
                                    if idx in signals_df['index_name'].unique()]
    selected_index = st.selectbox("Индекс:", available_indexes, index=0)

with col2:
    period_options = ['Сегодня', '7 дней', '30 дней', 'Вся история']
    selected_period = st.selectbox("Период:", period_options, index=2)

with col3:
    limit_options = [10, 25, 50, 100]
    selected_limit = st.selectbox("Лимит:", limit_options, index=1)

filtered_df = signals_df.copy()

if selected_index != 'Все':
    index_key = [k for k, v in HEDGE_CONFIG.items() if v['name'] == selected_index][0]
    filtered_df = filtered_df[filtered_df['index_name'] == index_key]

if selected_period == 'Сегодня':
    today = date.today().strftime('%Y-%m-%d')
    filtered_df = filtered_df[filtered_df['date'] == today]
elif selected_period == '7 дней':
    week_ago = (datetime.now() - timedelta(days=7)).strftime('%Y-%m-%d')
    filtered_df = filtered_df[filtered_df['date'] >= week_ago]
elif selected_period == '30 дней':
    month_ago = (datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d')
    filtered_df = filtered_df[filtered_df['date'] >= month_ago]

filtered_df = filtered_df.head(selected_limit)

if not filtered_df.empty:
    history_display = filtered_df[['date', 'time', 'index_display', 'signal', 'price']].copy()
    history_display.columns = ['Дата', 'Время', 'Индекс', 'Сигнал', 'Цена']
    
    # [FIX] Сортировка по дате (свежие вверху)
    history_display = history_display.sort_values('Дата', ascending=False).reset_index(drop=True)
    
    history_display['Сигнал'] = history_display['Сигнал'].map(
        lambda x: f"🟢 {x}" if "ОТКРЫТЬ" in x else f"🔴 {x}"
    )
    
    st.dataframe(history_display, use_container_width=True, hide_index=True)
    st.caption(f"Найдено {len(history_display)} сигналов за выбранный период")
else:
    st.info("ℹ️ Нет сигналов за выбранный период")

# ==================== СЕКЦИЯ 4: ЖУРНАЛ ЭФФЕКТИВНОСТИ ХЕДЖЕЙ ====================
st.markdown("---")
st.markdown("### 📊 Журнал эффективности хеджей")

hedge_journal = []

if not signals_df.empty:
    for index in signals_df['index_name'].unique():
        index_signals = signals_df[signals_df['index_name'] == index].copy()
        index_signals = index_signals.sort_values('date', ascending=True).reset_index(drop=True)
        
        open_price = None
        open_date = None
        open_time = None
        
        for _, row in index_signals.iterrows():
            signal = row['signal']
            price = row['price']
            
            if "ЗАКРЫТЬ" in signal:
                if open_price is not None:
                    if str(row['date']) >= str(open_date):
                        logic = HEDGE_CONFIG.get(index, {}).get('logic', 'standard')
                        if logic == 'standard':
                            change = ((open_price - price) / open_price) * 100
                        else:
                            change = ((price - open_price) / open_price) * 100
                        
                        hedge_journal.append({
                            'index': index,
                            'index_display': HEDGE_CONFIG.get(index, {}).get('name', index),
                            'open_date': open_date,
                            'open_time': open_time,
                            'open_price': open_price,
                            'close_date': row['date'],
                            'close_time': row['time'],
                            'close_price': price,
                            'efficiency': change
                        })
                        open_price = None
                        open_date = None
                        open_time = None
                    else:
                        pass
            
            elif "ОТКРЫТЬ" in signal:
                open_price = price
                open_date = row['date']
                open_time = row['time']
        
        if open_price is not None:
            hedge_journal.append({
                'index': index,
                'index_display': HEDGE_CONFIG.get(index, {}).get('name', index),
                'open_date': open_date,
                'open_time': open_time,
                'open_price': open_price,
                'close_date': '—',
                'close_time': '—',
                'close_price': '—',
                'efficiency': None,
                'status': '🟡 ДЕРЖАТЬ'
            })

if hedge_journal:
    journal_df = pd.DataFrame(hedge_journal)
    
    # [FIX] Сортировка по дате открытия (свежие вверху)
    journal_df = journal_df.sort_values('open_date', ascending=False).reset_index(drop=True)
    
    journal_display = journal_df[['index_display', 'open_date', 'open_price', 
                                   'close_date', 'close_price', 'efficiency']].copy()
    journal_display.columns = ['Индекс', 'Открыт', 'Цена открытия', 
                                'Закрыт', 'Цена закрытия', 'Эффективность %']
    
    # [NEW] Цветовое форматирование эффективности
    def color_efficiency(val):
        if pd.isna(val):
            return ''
        try:
            v = float(val)
            if v > 0:
                return 'background-color: #d4edda; color: #155724'
            elif v < 0:
                return 'background-color: #f8d7da; color: #721c24'
            else:
                return ''
        except:
            return ''
    
    def format_efficiency(val):
        if pd.isna(val):
            return '—'
        try:
            return f"{float(val):+.2f}%"
        except:
            return str(val)
    
    try:
        styled_journal = journal_display.style.applymap(
            color_efficiency, subset=['Эффективность %']
        ).format({'Эффективность %': format_efficiency})
        st.dataframe(styled_journal, use_container_width=True, hide_index=True)
    except Exception:
        # Fallback без стилизации
        journal_display['Эффективность %'] = journal_display['Эффективность %'].apply(format_efficiency)
        st.dataframe(journal_display, use_container_width=True, hide_index=True)
    
    # Статистика
    closed_trades = journal_df[journal_df['efficiency'].notna()]
    if not closed_trades.empty:
        st.markdown("#### 📈 Статистика закрытых хеджей")
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            st.metric("Всего хеджей", len(journal_df))
        with col2:
            st.metric("Закрыто", len(closed_trades))
        with col3:
            positive = (closed_trades['efficiency'] > 0).sum()
            st.metric("Прибыльных", positive)
        with col4:
            if len(closed_trades) > 0:
                avg_change = closed_trades['efficiency'].mean()
                st.metric("Среднее изменение", f"{avg_change:+.2f}%")
            else:
                st.metric("Среднее изменение", "N/A")
else:
    st.info("ℹ️ Журнал хеджей пуст — сигналы накапливаются по мере работы бота")

# ==================== ФУТЕР ====================
st.markdown("---")
st.caption("🤖 Данные обновляются ботом `hedgebot.service` по расписанию: 10:10, 19:10, 00:10 МСК")
st.caption("📊 Страница автообновляется каждые 5 минут")
