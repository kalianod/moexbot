import streamlit as st
import pandas as pd
import sqlite3
import plotly.graph_objects as go
from datetime import date

st.set_page_config(page_title="OTC объём", layout="wide")
DB_PATH = '/home/kalian/moexbot/futoi.db'

st.title("💼 OTC объём (внебиржевые / адресные сделки)")
st.caption("Источник: лента `trades()` с флагом `offmarketdeal=1`. История копится с 28.08.2026.")

st.sidebar.header("⚙️ Параметры")
symbol = st.sidebar.selectbox("Инструмент:", ["SiU6", "CRU6", "MXU6"])
selected_date = st.sidebar.date_input("Дата:", value=date.today(), format="DD.MM.YYYY")

@st.cache_data(ttl=300)
def load_otc(symbol, ds):
    conn = sqlite3.connect(DB_PATH)
    q = """SELECT tradeno, tradedate, tradetime, price, quantity, buysell, openposition, oi_delta
           FROM otc_trades WHERE symbol=? AND tradedate=? ORDER BY tradetime"""
    df = pd.read_sql_query(q, conn, params=(symbol, ds))
    conn.close()
    return df

df = load_otc(symbol, selected_date.strftime('%Y-%m-%d'))

if df.empty:
    st.info(f"ℹ️ Нет OTC-сделок по {symbol} за {selected_date.strftime('%d.%m.%Y')}. "
            f"OTC — редкие события; попробуйте другую дату или загляните позже.")
    st.stop()

df['datetime'] = pd.to_datetime(df['tradedate'] + ' ' + df['tradetime'])
df['bar'] = df['datetime'].dt.floor('5min')

# ---------- сводка дня ----------
buy_tot = df.loc[df['buysell'] == 'B', 'quantity'].sum()
sell_tot = df.loc[df['buysell'] == 'S', 'quantity'].sum()
c1, c2, c3, c4 = st.columns(4)
c1.metric("OTC сделок", f"{len(df)}")
c2.metric("Объём OTC", f"{buy_tot + sell_tot:,.0f}")
c3.metric("Buy / Sell", f"{buy_tot:,.0f} / {sell_tot:,.0f}")
bal = (buy_tot - sell_tot) / (buy_tot + sell_tot) * 100 if (buy_tot + sell_tot) else 0
c4.metric("Баланс", f"{bal:+.0f}%")

# ---------- панель: бары по 5 мин ----------
buy = df[df['buysell'] == 'B'].groupby('bar')['quantity'].sum()
sell = df[df['buysell'] == 'S'].groupby('bar')['quantity'].sum()
agg = pd.DataFrame({'buy': buy, 'sell': sell}).fillna(0)
agg['net'] = agg['buy'] - agg['sell']

hover = []
for bar, row in agg.iterrows():
    sub = df[df['bar'] == bar].sort_values('quantity', ascending=False)
    top = sub.iloc[0]
    side = 'Покупатель' if top['buysell'] == 'B' else 'Продавец'
    otype = '🔥 новая позиция' if abs(top['oi_delta']) >= 0.5 * top['quantity'] else '↔️ перекладывание'
    hover.append(
        f"<b>{bar.strftime('%H:%M')}</b><br>"
        f"Buy: {row['buy']:,.0f} | Sell: {row['sell']:,.0f} | Net: {row['net']:+,.0f}<br>"
        f"Крупнейшая: {top['quantity']:,.0f} @ {top['price']:,.0f} — {side}<br>"
        f"~ΔOI: {top['oi_delta']:+,.0f} ({otype})"
    )

fig = go.Figure(go.Bar(
    x=agg.index, y=agg['net'],
    marker_color=['#26A69A' if v >= 0 else '#EF5350' for v in agg['net']],
    text=hover, hoverinfo='text', name='OTC net'))
fig.update_layout(height=320, margin=dict(l=10, r=10, t=10, b=10),
                  yaxis_title="Net объём", xaxis_title="")
st.plotly_chart(fig, use_container_width=True)

# ---------- лента сделок ----------
st.subheader("📋 Лента OTC-сделок")
show = df.sort_values('datetime', ascending=False).copy()
show['time'] = show['datetime'].dt.strftime('%H:%M:%S')
show['сторона'] = show['buysell'].map({'B': '🟢 Покупатель', 'S': '🔴 Продавец'})
show['тип'] = show.apply(
    lambda r: '🔥 новая' if abs(r['oi_delta']) >= 0.5 * r['quantity'] else '↔️ переклад', axis=1)
st.dataframe(show[['time', 'price', 'quantity', 'сторона', 'oi_delta', 'тип']]
             .rename(columns={'time': 'Время', 'price': 'Цена', 'quantity': 'Объём',
                              'oi_delta': '~ΔOI'}),
             width='stretch', height=400)

st.download_button("⬇️ Скачать CSV", data=df.to_csv(),
                   file_name=f"otc_{symbol}_{selected_date.strftime('%Y%m%d')}.csv",
                   mime="text/csv")
