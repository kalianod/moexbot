import streamlit as st
import pandas as pd
import sqlite3
from datetime import date, timedelta
import os

# [SSL-патч] для moexalgo (как в коллекторе и app.py)
import ssl
try:
    _dc = ssl.create_default_context
    def _nv(*a, **k):
        c = _dc(*a, **k); c.check_hostname = False; c.verify_mode = ssl.CERT_NONE; return c
    ssl.create_default_context = _nv
except Exception:
    pass

from moexalgo import Ticker, session
from dotenv import load_dotenv
load_dotenv('/home/kalian/moexbot/.env')
session.TOKEN = os.getenv('MOEXALGOPACK_TOKEN')

st.set_page_config(page_title="Таблица FIZ/YUR", layout="wide")

DB_PATH = '/home/kalian/moexbot/futoi.db'

st.title("📊 Таблица FIZ / YUR (5-минутные бары)")
st.caption("Дельты контрактов и счетов по физлицам и юрлицам. Данные: БД `futoi_data` + дозагрузка из Algopack по кнопке.")

ALL_COLS = ['symbol', 'sess_id', 'seqnum', 'tradedate', 'tradetime', 'ticker', 'clgroup',
            'pos', 'pos_long', 'pos_short', 'pos_long_num', 'pos_short_num', 'systime', 'trade_session_date']

# ---------- служебные функции ----------
@st.cache_data(ttl=3600)
def get_yur_range(symbol):
    conn = sqlite3.connect(DB_PATH)
    res = conn.execute(
        "SELECT MIN(tradedate), MAX(tradedate) FROM futoi_data WHERE symbol = ? AND clgroup = 'YUR'",
        (symbol,)).fetchone()
    conn.close()
    return res

def db_counts(symbol, ds):
    conn = sqlite3.connect(DB_PATH)
    rows = conn.execute(
        "SELECT clgroup, COUNT(*) FROM futoi_data WHERE symbol = ? AND tradedate = ? GROUP BY clgroup",
        (symbol, ds)).fetchall()
    conn.close()
    return dict(rows)

def sync_day_from_api(symbol, d):
    """Проверяет БД; если FIZ/YUR за дату нет — качает день из Algopack и сохраняет.
    Возвращает: >0 сохранено строк, 0 уже в БД, -1 API вернул пусто."""
    ds = d.strftime('%Y-%m-%d')
    have = db_counts(symbol, ds)
    missing = [g for g in ('FIZ', 'YUR') if have.get(g, 0) < 50]
    if not missing:
        return 0
    df = Ticker(symbol).futoi(start=ds, end=ds)
    if df is None or df.empty:
        return -1
    df = df.copy()
    df['symbol'] = symbol
    present = [c for c in ALL_COLS if c in df.columns]
    conn = sqlite3.connect(DB_PATH)
    n = 0
    for _, r in df.iterrows():
        try:
            cols_str = ', '.join(present)
            ph = ', '.join(['?'] * len(present))
            conn.execute(f"INSERT OR REPLACE INTO futoi_data ({cols_str}) VALUES ({ph})",
                         tuple(r[c] for c in present))
            n += 1
        except Exception:
            pass
    conn.commit()
    conn.close()
    return n

@st.cache_data(ttl=300)
def load_day(symbol, selected_date):
    conn = sqlite3.connect(DB_PATH)
    q = """SELECT systime, clgroup, pos_long, pos_short, pos_long_num, pos_short_num
           FROM futoi_data WHERE symbol = ? AND tradedate = ? ORDER BY systime, clgroup"""
    df = pd.read_sql_query(q, conn, params=(symbol, selected_date.strftime('%Y-%m-%d')),
                           parse_dates=['systime'])
    conn.close()
    for c in ['pos_long', 'pos_short', 'pos_long_num', 'pos_short_num']:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors='coerce')
    return df

# ---------- сайдбар ----------
st.sidebar.header("⚙️ Параметры")
symbol = st.sidebar.selectbox("Инструмент:", ["SiU6", "CRU6", "MXU6"])
selected_date = st.sidebar.date_input("Дата:", value=date.today() - timedelta(days=1), format="DD.MM.YYYY")

yr = get_yur_range(symbol)
if yr and yr[0]:
    st.sidebar.info(f"📅 YUR в БД: {yr[0]} → {yr[1]}")

if st.sidebar.button("🔄 Загрузить данные", type="primary", width="stretch"):
    with st.spinner("Проверяю БД, при необходимости качаю из Algopack..."):
        saved = sync_day_from_api(symbol, selected_date)
    st.cache_data.clear()
    if saved > 0:
        st.success(f"✅ Дозагружено из Algopack: {saved} строк (FIZ+YUR)")
    elif saved == 0:
        st.info("ℹ️ Данные уже в БД — показываю их")
    else:
        st.warning("⚠️ API вернул пусто — выходной или торгов не было")

# ---------- загрузка и показ ----------
df = load_day(symbol, selected_date)

if df.empty:
    st.warning(f"⚠️ Нет данных для {symbol} за {selected_date.strftime('%d.%m.%Y')}.")
    st.stop()

fiz = df[df['clgroup'] == 'FIZ']
yur = df[df['clgroup'] == 'YUR']
st.success(f"✅ Загружено: FIZ = {len(fiz)} срезов, YUR = {len(yur)} срезов за {selected_date.strftime('%d.%m.%Y')}")

st.markdown("---")
mode = st.radio("Режим отображения:", ["По бару (дельты)", "Накопительно от начала дня"],
                horizontal=True, index=0)

def build_table(df_group):
    if df_group.empty or 'systime' not in df_group.columns:
        return pd.DataFrame()
    g = df_group.set_index('systime')
    r = g.resample('5min').last().dropna(subset=['pos_long', 'pos_short'])
    if r.empty:
        return pd.DataFrame()
    cols = ['pos_long', 'pos_short', 'pos_long_num', 'pos_short_num']
    if mode == "По бару (дельты)":
        d = r[cols].diff().dropna()
        d['наборL'] = d['pos_long'].clip(lower=0)
        d['наборS'] = (-d['pos_short']).clip(lower=0)
    else:
        first = r[cols].iloc[0]
        d = (r[cols] - first).iloc[1:]
        d['наборL'] = float('nan')
        d['наборS'] = float('nan')
    d['short_abs'] = -d['pos_short']
    d['short_num'] = d['pos_short_num']
    d = d.rename(columns={
        'pos_long': 'Контракты', 'pos_long_num': 'Счета',
        'short_abs': 'Контракты.1', 'short_num': 'Счета.1'})
    d.index = d.index - pd.Timedelta(minutes=5)  # метка = начало бара
    d.index.name = 'Бар'
    return d[['Контракты', 'Счета', 'Контракты.1', 'Счета.1', 'наборL', 'наборS']]

tbl_fiz = build_table(fiz)
tbl_yur = build_table(yur)

if tbl_fiz.empty or tbl_yur.empty:
    st.warning("⚠️ Недостаточно данных для построения таблицы.")
    st.stop()

bid_total = tbl_fiz['наборL'].fillna(0) + tbl_yur['наборL'].fillna(0)
ask_total = tbl_fiz['наборS'].fillna(0) + tbl_yur['наборS'].fillna(0)
face_fiz = (tbl_fiz['наборL'].fillna(0) / bid_total.replace(0, float('nan')) * 100).fillna(0).round(0)
face_yur = (tbl_yur['наборL'].fillna(0) / bid_total.replace(0, float('nan')) * 100).fillna(0).round(0)

final = pd.DataFrame({
    'Ф Лонг (контр)': tbl_fiz['Контракты'], 'Ф Лонг (счета)': tbl_fiz['Счета'],
    'Ф Шорт (контр)': tbl_fiz['Контракты.1'], 'Ф Шорт (счета)': tbl_fiz['Счета.1'],
    'Ю Лонг (контр)': tbl_yur['Контракты'], 'Ю Лонг (счета)': tbl_yur['Счета'],
    'Ю Шорт (контр)': tbl_yur['Контракты.1'], 'Ю Шорт (счета)': tbl_yur['Счета.1'],
    'Набор лонгов (5м)': bid_total,
    'Набор шортов (5м)': ask_total,
    'Лицо Ф %': face_fiz,
    'Лицо Ю %': face_yur,
})

def color_delta(val):
    if pd.isna(val) or val == 0:
        return ''
    try:
        v = float(val)
        return f'color: {"#26A69A" if v > 0 else "#EF5350"}; font-weight: 600'
    except Exception:
        return ''

fmt = '{:+.0f}' if mode == "По бару (дельты)" else '{:.0f}'
st.dataframe(final.style.map(color_delta).format(fmt), width="stretch", height=700)

# ---------- сводка дня ----------
st.markdown("---")
st.subheader("📈 Сводка (последний срез дня)")
col1, col2 = st.columns(2)
for col, df_g, label in [(col1, fiz, 'Физлица'), (col2, yur, 'Юрлица')]:
    if not df_g.empty and 'pos_long' in df_g.columns:
        last = df_g.iloc[-1]
        avg_l = last['pos_long'] / last['pos_long_num'] if last.get('pos_long_num', 0) > 0 else 0
        avg_s = abs(last['pos_short']) / last['pos_short_num'] if last.get('pos_short_num', 0) > 0 else 0
        with col:
            st.markdown(f"""
### {label}
- **Лонг:** {last['pos_long']:,.0f} контр. / {last['pos_long_num']:,.0f} лиц (ср. **{avg_l:.0f}** контр/лицо)
- **Шорт:** {abs(last['pos_short']):,.0f} контр. / {last['pos_short_num']:,.0f} лиц (ср. **{avg_s:.0f}** контр/лицо)
""")

# ---------- экспорт ----------
st.markdown("---")
st.download_button(
    label=f"⬇️ Скачать таблицу CSV ({selected_date.strftime('%Y-%m-%d')})",
    data=final.to_csv(),
    file_name=f"fiz_yur_{symbol}_{selected_date.strftime('%Y%m%d')}.csv",
    mime="text/csv",
    width="stretch"
)
