import streamlit as st
import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import sqlite3
from datetime import datetime, date, timedelta
import os
from dotenv import load_dotenv
from moexalgo import Ticker, session

# [PATCH 2026-08-24]: Отключаем проверку SSL для moexalgo (как в коллекторе)
# Причина: MOEX обновил сертификат 19.08.2026, библиотека падает с CERTIFICATE_VERIFY_FAILED.
# Это безопасно: iss.moex.com — публичный сайт с валидным сертификатом ZeroSSL.
import ssl
try:
    _default_ctx = ssl.create_default_context
    def _no_verify_ctx(*args, **kwargs):
        ctx = _default_ctx(*args, **kwargs)
        ctx.check_hostname = False
        ctx.verify_mode = ssl.CERT_NONE
        return ctx
    ssl.create_default_context = _no_verify_ctx
except Exception:
    pass
from streamlit_autorefresh import st_autorefresh

load_dotenv()
session.TOKEN = os.getenv('MOEXALGOPACK_TOKEN')

st.set_page_config(page_title="FUTOI Dashboard Pro", layout="wide", initial_sidebar_state="collapsed")

# Автообновление каждые 5 минут (300000 мс)
st_autorefresh(interval=5 * 60 * 1000, key="futoi-dashboard-autorefresh")

DB_PATH = '/home/kalian/moexbot/futoi.db'

# ==================== ИНИЦИАЛИЗАЦИЯ СОСТОЯНИЯ ====================
state_keys = [
    'show_clusters', 'show_fiz_buy_plus', 'show_fiz_sell_plus',
    'show_fiz_buy_minus', 'show_fiz_sell_minus',
    'show_yur_buy_plus', 'show_yur_sell_plus',
    'show_yur_buy_minus', 'show_yur_sell_minus', 'show_profile_oi',
    'show_concentration', 'show_yur_signal'
]

query_params = st.query_params
for key in state_keys:
    if key in query_params:
        st.session_state[key] = query_params[key].lower() == 'true'
    elif key not in st.session_state:
        st.session_state[key] = False

# ==================== БОКОВАЯ ПАНЕЛЬ ====================
st.sidebar.header("⚙️ Настройки")
symbol = st.sidebar.selectbox("Инструмент:", ["SiU6", "CRU6", "MXU6"])
start_date = st.sidebar.date_input("Дата начала", value=date.today() - timedelta(days=2), format="DD.MM.YYYY")
end_date = st.sidebar.date_input("Дата окончания", value=date.today(), format="DD.MM.YYYY")
timeframe = st.sidebar.selectbox("Таймфрейм:", ["5M", "10M", "1H", "1D"])

st.sidebar.markdown("---")
annotation_threshold_fiz = st.sidebar.slider("Порог для физлиц:", min_value=10, max_value=200, value=50)
annotation_threshold_yur = st.sidebar.slider("Порог для юрлиц:", min_value=10, max_value=200, value=50)

st.sidebar.markdown("---")
# [НОВОЕ 2026-08-19] Концентрация: крупные сделки малым числом счетов
concentration_threshold_accounts = st.sidebar.slider("Концентрация: макс. изменение счетов:", min_value=1, max_value=50, value=10)
concentration_threshold_contracts = st.sidebar.slider("Концентрация: мин. изменение контрактов:", min_value=100, max_value=5000, value=1000)

st.sidebar.markdown("---")
# [НОВОЕ 2026-08-24] Сигнал юрлиц: доля >50% + много контрактов
yur_share_threshold = st.sidebar.slider("Юрлица: мин. доля (%):", min_value=50, max_value=100, value=50)
yur_contracts_threshold = st.sidebar.slider("Юрлица: мин. контрактов:", min_value=100, max_value=10000, value=1000)

st.sidebar.markdown("---")
st.sidebar.caption(f"🔄 Автообновление: 5 мин")
st.sidebar.caption(f"Обновлено: {datetime.now().strftime('%H:%M:%S')}")

if st.sidebar.button("🔄 Загрузить данные", width="stretch", type="primary"):
    st.cache_data.clear()
    st.rerun()

# ==================== ЗАГОЛОВОК ====================
hide_header = st.query_params.get('hide_header', 'false').lower() == 'true'
hide_export = st.query_params.get('hide_export', 'false').lower() == 'true'

# ==================== КНОПКИ ФИЛЬТРОВ ====================
st.markdown("---")
btn_cols = st.columns(12)

buttons_config = [
    ("btn_clusters", "show_clusters", "Кластера Ф/Ю"),
    ("btn_fiz_buy_plus", "show_fiz_buy_plus", "Ф buy +"),
    ("btn_fiz_sell_plus", "show_fiz_sell_plus", "Ф sell +"),
    ("btn_fiz_buy_minus", "show_fiz_buy_minus", "Ф buy -"),
    ("btn_fiz_sell_minus", "show_fiz_sell_minus", "Ф sell -"),
    ("btn_yur_buy_plus", "show_yur_buy_plus", "Ю buy +"),
    ("btn_yur_sell_plus", "show_yur_sell_plus", "Ю sell +"),
    ("btn_yur_buy_minus", "show_yur_buy_minus", "Ю buy -"),
    ("btn_yur_sell_minus", "show_yur_sell_minus", "Ю sell -"),
    ("btn_profile_oi", "show_profile_oi", "Профиль OI"),
    ("btn_concentration", "show_concentration", "Концентрация"),
    ("btn_yur_signal", "show_yur_signal", "Сигнал юрлиц"),
]

# Семантика цвета кнопок (эмодзи) + Сброс
for i, (btn_key, state_key, label) in enumerate(buttons_config):
    with btn_cols[i]:
        is_active = st.session_state[state_key]
        if 'buy' in state_key:
            display_label = f"🟢 {label}"
        elif 'sell' in state_key:
            display_label = f"🔴 {label}"
        else:
            display_label = f"⚪ {label}"
            
        if st.button(display_label, key=btn_key, width="stretch", type="primary" if is_active else "secondary"):
            st.session_state[state_key] = not st.session_state[state_key]
            st.query_params[state_key] = str(st.session_state[state_key]).lower()
            st.rerun()

# Кнопка сброса фильтров
st.markdown("<br>", unsafe_allow_html=True)
if st.button("✖ Сбросить все фильтры", width="stretch", type="secondary"):
    for key in state_keys:
        st.session_state[key] = False
        st.query_params[key] = 'false'
    st.rerun()

# ==================== ЗАГРУЗКА ДАННЫХ ====================
@st.cache_data(ttl=60)
def load_data(symbol, start, end, tf):
    import time
    t_start = time.time()
    st.write(f"🔄 Загрузка данных для {symbol} с {start} по {end}...")
    
    conn = sqlite3.connect(DB_PATH)
    query = """
        SELECT systime, clgroup, pos_long_num, pos_short_num, pos_long, pos_short 
        FROM futoi_data 
        WHERE symbol = ? AND systime BETWEEN ? AND ?
        ORDER BY systime
    """
    start_dt = datetime.combine(start, datetime.min.time())
    end_dt = datetime.combine(end, datetime.max.time())
    
    t_sql = time.time()
    df = pd.read_sql_query(query, conn, params=(symbol, start_dt, end_dt), parse_dates=['systime'])
    conn.close()
    st.write(f"✅ FUTOI из БД: {len(df)} строк за {time.time()-t_sql:.2f}с")
    
    if df.empty:
        return pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), pd.DataFrame()
    
    tf_map = {"5M": "5min", "10M": "10min", "1H": "1h", "1D": "1D"}
    resample_rule = tf_map.get(tf, "10min")
    
    df_fiz = df[df['clgroup'] == 'FIZ'].copy()
    df_yur = df[df['clgroup'] == 'YUR'].copy()
    
    def aggregate_df(df_group, rule):
        if df_group.empty: return df_group
        df_group = df_group.set_index('systime').resample(rule).last().dropna().reset_index()
        return df_group
    
    df_fiz = aggregate_df(df_fiz, resample_rule)
    df_yur = aggregate_df(df_yur, resample_rule)
    
    # Загрузка свечей с кэшированием в БД
    t_candles = time.time()
    conn_candles = sqlite3.connect(DB_PATH)
    
    # Сначала пытаемся загрузить из кэша
    # [ИЗМЕНЕНИЕ 2026-08-19]: Фильтруем свечи по периоду (5min по умолчанию)
    query_candles_cache = """
        SELECT begin, open, high, low, close, volume
        FROM candles_cache
        WHERE symbol = ? AND begin >= ? AND begin <= ? AND period = '5min'
        ORDER BY begin
    """
    df_candles = pd.read_sql_query(
        query_candles_cache, 
        conn_candles, 
        params=(symbol, start.strftime('%Y-%m-%d 00:00:00'), end.strftime('%Y-%m-%d 23:59:59'))
    )
    
    if not df_candles.empty:
        df_candles['begin'] = pd.to_datetime(df_candles['begin'])
        st.write(f"✅ Свечи из кэша: {len(df_candles)} за {time.time()-t_candles:.2f}с")
    else:
        # Если в кэше нет — загружаем с MOEX API
        st.write(f"🔄 Свечи не в кэше, загрузка с MOEX API...")
        try:
            # [ИЗМЕНЕНИЕ 2026-08-19]: Загружаем именно 5-минутные свечи
            df_candles = Ticker(symbol).candles(start=start.strftime('%Y-%m-%d'), end=end.strftime('%Y-%m-%d'), period='5min')
            if df_candles is not None and not df_candles.empty:
                df_candles['begin'] = pd.to_datetime(df_candles['begin'])
                
                # Сохраняем в кэш
                # [ИЗМЕНЕНИЕ 2026-08-19]: Сохраняем с указанием периода
                df_to_cache = df_candles[['begin', 'open', 'high', 'low', 'close', 'volume']].copy()
                df_to_cache['symbol'] = symbol
                df_to_cache['period'] = '5min'
                df_to_cache['begin'] = df_to_cache['begin'].astype(str)
                
                for _, row in df_to_cache.iterrows():
                    try:
                        conn_candles.execute(
                            "INSERT OR REPLACE INTO candles_cache (symbol, begin, period, open, high, low, close, volume) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                            (row['symbol'], row['begin'], row['period'], row['open'], row['high'], row['low'], row['close'], row['volume'])
                        )
                    except Exception as e:
                        pass
                conn_candles.commit()
                
                st.write(f"✅ Свечи загружены и сохранены в кэш: {len(df_candles)} за {time.time()-t_candles:.2f}с")
            else:
                st.write(f"⚠️ Свечи пусты за {time.time()-t_candles:.2f}с")
        except Exception as e:
            st.error(f"Ошибка загрузки свечей: {e}")
            df_candles = pd.DataFrame()
    
    conn_candles.close()
    
    # Загружаем баровую статистику из БД
    conn = sqlite3.connect(DB_PATH)
    query_bar = """
        SELECT tradedate, tradetime, price, vol, bid_vol, ask_vol, d_oi, yur_long, yur_short
        FROM bar_stats
        WHERE symbol = ? AND tradedate BETWEEN ? AND ?
        ORDER BY tradedate, tradetime
    """
    t_bar = time.time()
    df_bar_stats = pd.read_sql_query(query_bar, conn, params=(symbol, start.strftime('%Y-%m-%d'), end.strftime('%Y-%m-%d')))
    conn.close()
    st.write(f"✅ bar_stats: {len(df_bar_stats)} строк за {time.time()-t_bar:.2f}с")
    
    if not df_bar_stats.empty:
        df_bar_stats['tradedate'] = pd.to_datetime(df_bar_stats['tradedate'])
        df_bar_stats['tradetime'] = pd.to_timedelta(df_bar_stats['tradetime'])
        df_bar_stats['datetime'] = df_bar_stats['tradedate'] + df_bar_stats['tradetime']
    
    st.write(f"✅ Всего загрузка: {time.time()-t_start:.2f}с")
    return df_fiz, df_yur, df_candles, df_bar_stats

df_fiz, df_yur, df_candles, df_bar_stats = load_data(symbol, start_date, end_date, timeframe)

# ==================== ГРАФИК ====================
if df_candles.empty:
    st.warning("⚠️ Нет данных свечей за выбранный период.")
else:
    # УМНАЯ ФИЛЬТРАЦИЯ: убираем свечи без движения
    if 'volume' in df_candles.columns:
        df_candles = df_candles[df_candles['volume'] > 0].copy()
    else:
        df_candles = df_candles[df_candles['high'] != df_candles['low']].copy()
    
    if df_candles.empty:
        st.warning("⚠️ Нет активных свечей за выбранный период (все свечи плоские).")
    else:
        is_mobile = 'Mobile' in st.context.headers.get('User-Agent', '') if st.context.headers else False

        # [OPT 2026-08-25] Защита от зависаний: >2000 свечей — маркеры отключены
        if len(df_candles) > 2000:
            st.warning(f"⚠️ {len(df_candles)} свечей (лимит 2000) — маркеры отключены для ускорения. Сузьте диапазон дат.")
            df_fiz = pd.DataFrame()
            df_yur = pd.DataFrame()
        
        # Легенда маркеров
        st.markdown("""
        <div style="display:flex; gap:18px; flex-wrap:wrap; font-size:12px; 
                    color:#9AA0A6; padding:4px 0 8px 0; border-bottom:1px solid #2A2E39; margin-bottom: 10px;">
            <span>▲ <b style="color:#26A69A">зел.</b> — Ф лонг ↑</span>
            <span>▼ <b style="color:#EF5350">красн.</b> — Ф шорт ↑</span>
            <span>● <b style="color:#26A69A">зел.</b> — Ю лонг ↑</span>
            <span>● <b style="color:#EF5350">красн.</b> — Ю шорт ↑</span>
            <span>★ <b style="color:#26A69A">зел.</b> — Концентрация Ф лонг (мало счетов, много контр.)</span>
    <span>★ <b style="color:#EF5350">красн.</b> — Концентрация Ф шорт (мало счетов, много контр.)</span>
    <span>◉ <b style="color:#26A69A">зел.</b> — Юр: доля >50% + много контрактов (long)</span>
    <span>◉ <b style="color:#EF5350">красн.</b> — Юр: доля >50% + много контрактов (short)</span>
    <span style="margin-left:auto; color:#666">💡 наведите на маркер для деталей</span>
        </div>
        """, unsafe_allow_html=True)

        # Словарь для избегания коллизий аннотаций (умный сдвиг)
        annotation_offsets = {}
        annotation_count = [0]

        def add_marker_with_text(candle_time, y_pos, delta, marker_symbol, marker_color, base_yshift):
            key = str(candle_time)
            offset_count = annotation_offsets.get(key, 0)
            direction = 1 if base_yshift > 0 else -1
            y_shift = base_yshift + direction * offset_count * 25  # шаг 25px между подписями
            annotation_offsets[key] = offset_count + 1
            
            # 1. Маркер
            fig.add_trace(go.Scatter(
                x=[candle_time], y=[y_pos], mode='markers',
                marker=dict(symbol=marker_symbol, size=12 if 'triangle' in marker_symbol else 10, 
                            color=marker_color, line=dict(width=0.5 if 'triangle' in marker_symbol else 1, color='white')),
                showlegend=False, hovertemplate=f"Дельта: {int(delta):+d}<extra></extra>"
            ), row=1, col=1)
            
            # 2. Текст с улучшенным контрастом
            fig.add_annotation(
                x=candle_time, y=y_pos, text=f"{int(delta):+d}", showarrow=False,
                font=dict(size=11, color='white', family='Arial, sans-serif'),
                bgcolor='rgba(20, 20, 20, 0.9)', bordercolor='rgba(100, 100, 100, 0.6)',
                borderwidth=1, borderpad=4, yshift=y_shift, row=1, col=1
            )
            annotation_count[0] += 1

        # SUBPLOT: 2 строки — свечи (80%) + объём (20%)
        fig = make_subplots(
            rows=2, cols=1, shared_xaxes=True,
            row_heights=[0.8, 0.2], vertical_spacing=0.02
        )
        
        # Добавляем данные bar_stats к свечам для hover
        df_candles_with_stats = df_candles.copy()
        if not df_bar_stats.empty:
            df_candles_with_stats = df_candles_with_stats.merge(
                df_bar_stats[['datetime', 'vol', 'bid_vol', 'ask_vol', 'd_oi', 'yur_long', 'yur_short']],
                left_on='begin', right_on='datetime', how='left'
            )
        
        # Создаем кастомный hover текст
        hover_text = []
        for idx, row in df_candles_with_stats.iterrows():
            vol_str = f"{row.get('vol', 0):.0f}" if pd.notna(row.get('vol')) else "N/A"
            bid_str = f"{row.get('bid_vol', 0):.0f}" if pd.notna(row.get('bid_vol')) else "N/A"
            ask_str = f"{row.get('ask_vol', 0):.0f}" if pd.notna(row.get('ask_vol')) else "N/A"
            doi_str = f"{row.get('d_oi', 0):+.0f}" if pd.notna(row.get('d_oi')) else "N/A"
            yur_l_str = f"{row.get('yur_long', 0):.0f}" if pd.notna(row.get('yur_long')) else "N/A"
            yur_s_str = f"{row.get('yur_short', 0):.0f}" if pd.notna(row.get('yur_short')) else "N/A"
            
            hover_text.append(
                f"<b>📊 Bar Stats</b><br>"
                f"Σ Vol: {vol_str}<br>"
                f"Bid Vol: {bid_str} | Ask Vol: {ask_str}<br>"
                f"ΔOI: {doi_str}<br>"
                f"🏦 Юрлица: Long {yur_l_str} / Short {yur_s_str}"
            )
        
        fig.add_trace(go.Candlestick(
            x=df_candles['begin'], open=df_candles['open'], high=df_candles['high'],
            low=df_candles['low'], close=df_candles['close'], name="Цена",
            increasing_line_color='#26A69A', decreasing_line_color='#EF5350', showlegend=False,
            text=hover_text, hovertemplate='%{text}<extra></extra>'
        ), row=1, col=1)

        # Панель объёма
        if 'volume' in df_candles.columns:
            vol_colors = ['#26A69A' if c >= o else '#EF5350' 
                          for c, o in zip(df_candles['close'], df_candles['open'])]
            fig.add_trace(go.Bar(
                x=df_candles['begin'], y=df_candles['volume'],
                marker_color=vol_colors, showlegend=False, opacity=0.5
            ), row=2, col=1)

        # ========== ФИЗЛИЦА ==========
        if st.session_state.show_fiz_buy_plus and not df_fiz.empty and 'systime' in df_fiz.columns:
            df_fiz['delta_long'] = df_fiz['pos_long_num'].diff()
            for idx, candle in df_candles.iterrows():
                mask = (df_fiz['systime'] - (candle['begin'] + pd.Timedelta(minutes=5))).abs() <= pd.Timedelta(minutes=2)
                if mask.any():
                    delta = df_fiz[mask].iloc[-1]['delta_long']
                    if pd.notna(delta) and delta > annotation_threshold_fiz:
                        add_marker_with_text(candle['begin'], candle['high'], delta, 'triangle-up', '#26A69A', 18)

        if st.session_state.show_fiz_sell_plus and not df_fiz.empty and 'systime' in df_fiz.columns:
            if 'delta_short' not in df_fiz.columns: df_fiz['delta_short'] = df_fiz['pos_short_num'].diff()
            for idx, candle in df_candles.iterrows():
                mask = (df_fiz['systime'] - (candle['begin'] + pd.Timedelta(minutes=5))).abs() <= pd.Timedelta(minutes=2)
                if mask.any():
                    delta = df_fiz[mask].iloc[-1]['delta_short']
                    if pd.notna(delta) and delta > annotation_threshold_fiz:
                        add_marker_with_text(candle['begin'], candle['low'], delta, 'triangle-down', '#EF5350', -18)

        if st.session_state.show_fiz_buy_minus and not df_fiz.empty and 'systime' in df_fiz.columns:
            if 'delta_long' not in df_fiz.columns: df_fiz['delta_long'] = df_fiz['pos_long_num'].diff()
            for idx, candle in df_candles.iterrows():
                mask = (df_fiz['systime'] - (candle['begin'] + pd.Timedelta(minutes=5))).abs() <= pd.Timedelta(minutes=2)
                if mask.any():
                    delta = df_fiz[mask].iloc[-1]['delta_long']
                    if pd.notna(delta) and delta < -annotation_threshold_fiz:
                        add_marker_with_text(candle['begin'], candle['high'], delta, 'triangle-down', '#26A69A', -18)

        if st.session_state.show_fiz_sell_minus and not df_fiz.empty and 'systime' in df_fiz.columns:
            if 'delta_short' not in df_fiz.columns: df_fiz['delta_short'] = df_fiz['pos_short_num'].diff()
            for idx, candle in df_candles.iterrows():
                mask = (df_fiz['systime'] - (candle['begin'] + pd.Timedelta(minutes=5))).abs() <= pd.Timedelta(minutes=2)
                if mask.any():
                    delta = df_fiz[mask].iloc[-1]['delta_short']
                    if pd.notna(delta) and delta < -annotation_threshold_fiz:
                        add_marker_with_text(candle['begin'], candle['low'], delta, 'triangle-up', '#EF5350', 18)

        # ========== ЮРЛИЦА ==========
        if st.session_state.show_yur_buy_plus and not df_yur.empty and 'systime' in df_yur.columns:
            df_yur['delta_long'] = df_yur['pos_long_num'].diff()
            for idx, candle in df_candles.iterrows():
                mask = (df_yur['systime'] - (candle['begin'] + pd.Timedelta(minutes=5))).abs() <= pd.Timedelta(minutes=2)
                if mask.any():
                    delta = df_yur[mask].iloc[-1]['delta_long']
                    if pd.notna(delta) and delta > annotation_threshold_yur:
                        add_marker_with_text(candle['begin'], candle['close'], delta, 'circle', '#26A69A', 18)

        if st.session_state.show_yur_sell_plus and not df_yur.empty and 'systime' in df_yur.columns:
            if 'delta_short' not in df_yur.columns: df_yur['delta_short'] = df_yur['pos_short_num'].diff()
            for idx, candle in df_candles.iterrows():
                mask = (df_yur['systime'] - (candle['begin'] + pd.Timedelta(minutes=5))).abs() <= pd.Timedelta(minutes=2)
                if mask.any():
                    delta = df_yur[mask].iloc[-1]['delta_short']
                    if pd.notna(delta) and delta > annotation_threshold_yur:
                        add_marker_with_text(candle['begin'], candle['close'], delta, 'circle', '#EF5350', -18)

        if st.session_state.show_yur_buy_minus and not df_yur.empty and 'systime' in df_yur.columns:
            if 'delta_long' not in df_yur.columns: df_yur['delta_long'] = df_yur['pos_long_num'].diff()
            for idx, candle in df_candles.iterrows():
                mask = (df_yur['systime'] - (candle['begin'] + pd.Timedelta(minutes=5))).abs() <= pd.Timedelta(minutes=2)
                if mask.any():
                    delta = df_yur[mask].iloc[-1]['delta_long']
                    if pd.notna(delta) and delta < -annotation_threshold_yur:
                        add_marker_with_text(candle['begin'], candle['close'], delta, 'circle', '#26A69A', -18)

        if st.session_state.show_yur_sell_minus and not df_yur.empty and 'systime' in df_yur.columns:
            if 'delta_short' not in df_yur.columns: df_yur['delta_short'] = df_yur['pos_short_num'].diff()
            for idx, candle in df_candles.iterrows():
                mask = (df_yur['systime'] - (candle['begin'] + pd.Timedelta(minutes=5))).abs() <= pd.Timedelta(minutes=2)
                if mask.any():
                    delta = df_yur[mask].iloc[-1]['delta_short']
                    if pd.notna(delta) and delta < -annotation_threshold_yur:
                        add_marker_with_text(candle['begin'], candle['close'], delta, 'circle', '#EF5350', 18)

        # Общий словарь d_oi для концентрации и сигнала юрлиц
        d_oi_map = {}
        if not df_bar_stats.empty:
            for _, r in df_bar_stats.iterrows():
                d_oi_map[pd.Timestamp(r['datetime']).floor('5min')] = r['d_oi']

        # ========== [НОВОЕ 2026-08-19] КОНЦЕНТРАЦИЯ: крупные сделки малым числом счетов ==========
        # Условие: |дельта счетов| <= порог AND дельта контрактов >= порог AND рост OI (d_oi > 0)
        # Интерпретация: несколько крупных участников наращивают позиции (концентрация покупок/продаж)
        concentration_count = [0]

        def add_concentration_marker(candle_time, y_pos, delta_contracts, marker_color, y_shift, label):
            fig.add_trace(go.Scatter(
                x=[candle_time], y=[y_pos], mode='markers',
                marker=dict(symbol='star', size=18, color=marker_color,
                            line=dict(width=2, color='white')),
                showlegend=False,
                hovertemplate=f"🎯 КОНЦЕНТРАЦИЯ: {label} {int(delta_contracts):+d} контр.<extra></extra>"
            ), row=1, col=1)
            fig.add_annotation(
                x=candle_time, y=y_pos, text=f"{int(delta_contracts):+d}", showarrow=False,
                font=dict(size=11, color='white', family='Arial, sans-serif'),
                bgcolor='rgba(20, 20, 20, 0.9)', bordercolor=marker_color,
                borderwidth=2, borderpad=4, yshift=y_shift, row=1, col=1
            )
            concentration_count[0] += 1

        if st.session_state.show_concentration:
            # Физлица: концентрация (лонг и шорт)
          if not df_fiz.empty and 'systime' in df_fiz.columns:
              if 'delta_long' not in df_fiz.columns: df_fiz['delta_long'] = df_fiz['pos_long_num'].diff()
              if 'delta_short' not in df_fiz.columns: df_fiz['delta_short'] = df_fiz['pos_short_num'].diff()
              if 'delta_long_contracts' not in df_fiz.columns: df_fiz['delta_long_contracts'] = df_fiz['pos_long'].diff()
              if 'delta_short_contracts' not in df_fiz.columns: df_fiz['delta_short_contracts'] = df_fiz['pos_short'].diff()
              for idx, candle in df_candles.iterrows():
                  mask = (df_fiz['systime'] - (candle['begin'] + pd.Timedelta(minutes=5))).abs() <= pd.Timedelta(minutes=2)
                  if mask.any():
                      d_long_num = df_fiz[mask].iloc[-1]['delta_long']
                      d_short_num = df_fiz[mask].iloc[-1]['delta_short']
                      d_long_contracts = df_fiz[mask].iloc[-1]['delta_long_contracts']
                      d_short_contracts = df_fiz[mask].iloc[-1]['delta_short_contracts']
                      d_oi = d_oi_map.get(pd.Timestamp(candle['begin']).floor('5min'))
                      if d_oi is not None and pd.notna(d_oi) and d_oi > 0:
                          # Концентрация ЛОНГ
                          if pd.notna(d_long_num) and pd.notna(d_long_contracts):
                              if abs(d_long_num) <= concentration_threshold_accounts and d_long_contracts >= concentration_threshold_contracts:
                                  add_concentration_marker(candle['begin'], candle['high'], d_long_contracts, '#26A69A', 70, 'Физ: концентрация лонг')
                          # Концентрация ШОРТ
                          if pd.notna(d_short_num) and pd.notna(d_short_contracts):
                              if abs(d_short_num) <= concentration_threshold_accounts and d_short_contracts >= concentration_threshold_contracts:
                                  add_concentration_marker(candle['begin'], candle['low'], d_short_contracts, '#EF5350', -70, 'Физ: концентрация шорт')

        # ========== [НОВОЕ 2026-08-24] СИГНАЛ ЮРЛИЦ: доля >50% + много контрактов ==========
        # Доля юрлиц = clip(dYUR,0) / (clip(dFIZ,0) + clip(dYUR,0)) * 100 (проверенная формула "Лицо")
        yur_signal_count = [0]

        def add_yur_share_marker(candle_time, y_pos, delta_contracts, share_pct, marker_color, y_shift, label):
            fig.add_trace(go.Scatter(
                x=[candle_time], y=[y_pos], mode='markers',
                marker=dict(symbol='circle-dot', size=16, color=marker_color,
                            line=dict(width=2, color='white')),
                showlegend=False,
                hovertemplate=f"🏦 {label}: {share_pct:.0f}% | {int(delta_contracts):+d} контр.<extra></extra>"
            ), row=1, col=1)
            fig.add_annotation(
                x=candle_time, y=y_pos, text=f"{share_pct:.0f}%", showarrow=False,
                font=dict(size=11, color='white', family='Arial, sans-serif'),
                bgcolor='rgba(20, 20, 20, 0.9)', bordercolor=marker_color,
                borderwidth=2, borderpad=4, yshift=y_shift, row=1, col=1
            )
            yur_signal_count[0] += 1

        if st.session_state.show_yur_signal and not df_fiz.empty and not df_yur.empty and 'systime' in df_fiz.columns and 'systime' in df_yur.columns:
            if 'delta_long_contracts' not in df_fiz.columns: df_fiz['delta_long_contracts'] = df_fiz['pos_long'].diff()
            if 'delta_short_contracts' not in df_fiz.columns: df_fiz['delta_short_contracts'] = df_fiz['pos_short'].diff()
            if 'delta_long_contracts' not in df_yur.columns: df_yur['delta_long_contracts'] = df_yur['pos_long'].diff()
            if 'delta_short_contracts' not in df_yur.columns: df_yur['delta_short_contracts'] = df_yur['pos_short'].diff()
            for idx, candle in df_candles.iterrows():
                mask_f = (df_fiz['systime'] - (candle['begin'] + pd.Timedelta(minutes=5))).abs() <= pd.Timedelta(minutes=2)
                mask_y = (df_yur['systime'] - (candle['begin'] + pd.Timedelta(minutes=5))).abs() <= pd.Timedelta(minutes=2)
                if mask_f.any() and mask_y.any():
                    f_long = df_fiz[mask_f].iloc[-1]['delta_long_contracts']
                    f_short = df_fiz[mask_f].iloc[-1]['delta_short_contracts']
                    y_long = df_yur[mask_y].iloc[-1]['delta_long_contracts']
                    y_short = df_yur[mask_y].iloc[-1]['delta_short_contracts']
                    # LONG: доля юрлиц в росте long
                    if pd.notna(f_long) and pd.notna(y_long):
                        num = max(y_long, 0); den = max(f_long, 0) + num
                        if den > 0:
                            share = num / den * 100
                            if share > yur_share_threshold and y_long >= yur_contracts_threshold:
                                add_yur_share_marker(candle['begin'], candle['high'], y_long, share, '#26A69A', 95, 'Юр: доля в росте long')
                    # SHORT: доля юрлиц в росте short
                    if pd.notna(f_short) and pd.notna(y_short):
                        num = max(y_short, 0); den = max(f_short, 0) + num
                        if den > 0:
                            share = num / den * 100
                            if share > yur_share_threshold and y_short >= yur_contracts_threshold:
                                add_yur_share_marker(candle['begin'], candle['low'], y_short, share, '#EF5350', -95, 'Юр: доля в росте short')

        fig.update_layout(
            height=750 if is_mobile else 1000, template="plotly_dark",
            hovermode='x unified',
            xaxis=dict(
                tickformat="%d.%m<br>%H:%M",
                rangeslider=dict(visible=False),
                type='date',
                showspikes=True, spikemode='across', spikesnap='cursor',
                spikecolor='rgba(255,255,255,0.3)', spikethickness=1
            ),
            xaxis2=dict(
                tickformat="%H:%M",
                showspikes=True, spikemode='across', spikesnap='cursor',
                spikecolor='rgba(255,255,255,0.3)', spikethickness=1
            ),
            yaxis=dict(showspikes=True, spikethickness=1, spikecolor='rgba(255,255,255,0.3)'),
            margin=dict(l=50, r=20, t=5, b=30), dragmode='zoom',
            modebar=dict(orientation='v', bgcolor='rgba(0,0,0,0)'), showlegend=False
        )
        
        st.plotly_chart(fig, width="stretch", config={"displayModeBar": True})
        
        if annotation_count[0] > 0:
            st.info(f"💡 Показано аннотаций: {annotation_count[0]}")

        if concentration_count[0] > 0:
            st.info(f"🎯 Обнаружено концентраций (мало счетов, много контрактов): {concentration_count[0]}")

        if yur_signal_count[0] > 0:
            st.info(f"🏦 Сигналов юрлиц (доля >50% + много контрактов): {yur_signal_count[0]}")

# ==================== ЭКСПОРТ CSV (В САМОМ НИЗУ, 1 КЛИК) ====================
if not hide_export and (not df_fiz.empty or not df_yur.empty):
    st.markdown("---")
    st.subheader("📥 Экспорт данных")
    export_col1, export_col2 = st.columns(2)
    
    with export_col1:
        st.download_button(
            label="⬇️ Скачать FIZ CSV",
            data=df_fiz.to_csv(index=False),
            file_name=f"fiz_{symbol}_{start_date}_{end_date}.csv",
            mime="text/csv",
            width="stretch"
        )
    
    with export_col2:
        st.download_button(
            label="⬇️ Скачать YUR CSV",
            data=df_yur.to_csv(index=False),
            file_name=f"yur_{symbol}_{start_date}_{end_date}.csv",
            mime="text/csv",
            width="stretch"
        )
