import streamlit as st
import requests
import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots

API_URL = "http://127.0.0.1:9000"
TIMEOUT = 15

STATE_OPTIONS = ["DRAFT", "APPROVED", "DEFERRED"]

INTERVALS = {
    "15m": 15,
    "1h": 60,
    "4h": 240,
}


def api_get(path: str, params: dict | None = None):
    try:
        r = requests.get(f"{API_URL}{path}", params=params or {}, timeout=TIMEOUT)
        r.raise_for_status()
        return r.json()
    except Exception as e:
        st.error(f"API error {path}: {e}")
        return None


def fmt_price(x):
    try:
        return f"{float(x):.4f}"
    except Exception:
        return str(x)


def load_pairs():
    resp = api_get("/pairs")
    if isinstance(resp, list):
        return resp or ["ETHUSDT"]
    if isinstance(resp, dict):
        pairs = resp.get("pairs")
        if isinstance(pairs, list) and pairs:
            return pairs
        tp = resp.get("trading_pairs")
        if isinstance(tp, dict) and tp:
            return list(tp.keys())
    return ["ETHUSDT", "XRPUSDT"]


def get_default_center(symbol: str):
    resp = api_get("/straddle/preview", {"symbol": symbol, "breakeven_offset": 1})
    if resp and resp.get("ok"):
        return float(resp.get("current_price") or 0)
    return 0.0


def normalize_klines(resp):
    if not resp:
        return pd.DataFrame()

    if isinstance(resp, dict):
        data = resp.get("klines") or resp.get("data") or resp.get("result") or []
    else:
        data = resp

    if not data:
        return pd.DataFrame()

    df = pd.DataFrame(data)

    ts_col = None
    for c in ["timestamp", "timestamp_ms", "time", "dt", "datetime"]:
        if c in df.columns:
            ts_col = c
            break

    if ts_col:
        ts = pd.to_numeric(df[ts_col], errors="coerce")
        if ts.notna().any():
            unit = "ms" if float(ts.dropna().median()) > 1e12 else "s"
            df["dt"] = pd.to_datetime(ts, unit=unit, errors="coerce")
        else:
            df["dt"] = pd.to_datetime(df[ts_col], errors="coerce")
    else:
        df["dt"] = pd.date_range(end=pd.Timestamp.utcnow(), periods=len(df), freq="1h")

    for col in ["open", "high", "low", "close", "volume"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    df = df.dropna(subset=["dt", "open", "high", "low", "close"]).sort_values("dt")
    return df


def build_chart(df_k: pd.DataFrame, levels: list, center: float, breakeven_offset: float, symbol: str):
    fig = make_subplots(rows=2, cols=1, shared_xaxes=True, vertical_spacing=0.03, row_heights=[0.78, 0.22])

    if not df_k.empty:
        fig.add_trace(
            go.Candlestick(
                x=df_k["dt"],
                open=df_k["open"],
                high=df_k["high"],
                low=df_k["low"],
                close=df_k["close"],
                name=symbol,
                increasing_line_color="#26a69a",
                decreasing_line_color="#ef5350",
            ),
            row=1,
            col=1,
        )

        if "volume" in df_k.columns:
            colors = ["#26a69a" if c >= o else "#ef5350" for o, c in zip(df_k["open"], df_k["close"])]
            fig.add_trace(
                go.Bar(x=df_k["dt"], y=df_k["volume"], name="Volume", marker_color=colors, opacity=0.45),
                row=2,
                col=1,
            )

    # Center and breakeven
    fig.add_hline(y=center, line_color="#ffffff", line_dash="dot", line_width=1,
                  annotation_text=f"Center {fmt_price(center)}", annotation_position="top left", row=1, col=1)

    lower_be = center - breakeven_offset
    upper_be = center + breakeven_offset
    fig.add_hline(y=lower_be, line_color="#ffca28", line_dash="dash", line_width=1,
                  annotation_text=f"Lower BE {fmt_price(lower_be)}", annotation_position="bottom left", row=1, col=1)
    fig.add_hline(y=upper_be, line_color="#ffca28", line_dash="dash", line_width=1,
                  annotation_text=f"Upper BE {fmt_price(upper_be)}", annotation_position="top left", row=1, col=1)

    for lvl in levels:
        price = float(lvl.get("final_price") or lvl.get("suggested_price") or 0)
        side = lvl.get("side", "?")
        state = lvl.get("state", "DRAFT")

        color = "#26a69a" if side == "Buy" else "#ef5350"

        if state == "DEFERRED":
            dash = "dot"
            width = 1.2
            opacity = 0.65
        elif state == "APPROVED":
            dash = "solid"
            width = 3.0
            opacity = 1.0
        else:
            dash = "solid"
            width = 1.8
            opacity = 0.9

        label = f"{side} {lvl.get('index')} {fmt_price(price)} [{state}]"

        fig.add_hline(
            y=price,
            line_color=color,
            line_dash=dash,
            line_width=width,
            opacity=opacity,
            annotation_text=label,
            annotation_position="top left" if side == "Sell" else "bottom left",
            annotation_font_size=9,
            annotation_font_color=color,
            row=1,
            col=1,
        )

    fig.update_layout(
        height=760,
        xaxis_rangeslider_visible=False,
        template="plotly_dark",
        margin=dict(l=10, r=10, t=30, b=10),
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
    )
    fig.update_yaxes(title_text="Price USDT", row=1, col=1)
    fig.update_yaxes(title_text="Volume", row=2, col=1)
    return fig


st.set_page_config(page_title="Straddle Grid", page_icon="🧩", layout="wide")

st.title("🧩 Straddle Grid Preview")
st.caption("Центр стредла, валютный безубыток, экспоненциальные уровни. Реальные ордера на этом этапе не выставляются.")

col_symbol, col_interval = st.columns([2, 2])
with col_symbol:
    pairs = load_pairs()
    symbol = st.selectbox("Пара", pairs, key="sg_symbol")
with col_interval:
    interval_label = st.selectbox("Таймфрейм", list(INTERVALS.keys()), index=1, key="sg_interval")

st.markdown("---")

c1, c2, c3, c4 = st.columns(4)

with c1:
    default_center = get_default_center(symbol)
    center = st.number_input(
        "Центр стредла (C)",
        min_value=0.0,
        value=float(default_center or 0.0),
        step=0.01,
        format="%.4f",
        key="sg_center",
        help="Цена, вокруг которой строится коридор. Вводится вручную.",
    )

with c2:
    breakeven_offset = st.number_input(
        "Отступ безубытка (B), USDT",
        min_value=0.0,
        value=100.0,
        step=1.0,
        format="%.2f",
        key="sg_be",
        help="Абсолютный отступ вверх и вниз от центра. Например, 100 означает коридор C ± 100.",
    )

with c3:
    quantity = st.number_input(
        "Объём на часть",
        min_value=0.0,
        value=0.01,
        step=0.0001,
        format="%.6f",
        key="sg_qty",
        help="Одинаковый объём для всех частей сетки.",
    )

with c4:
    ratio = st.number_input(
        "Коэффициент экспоненты",
        min_value=1.01,
        value=1.45,
        step=0.01,
        format="%.2f",
        key="sg_ratio",
        help="Чем больше значение, тем плотнее уровни у центра и тем дальше друг от друга дальние уровни.",
    )

c5, c6, c7, c8 = st.columns(4)

with c5:
    inner_parts = st.number_input(
        "Внутренних частей",
        min_value=0,
        max_value=20,
        value=4,
        step=1,
        key="sg_inner",
        help="Сколько уровней внутри зоны безубытка с каждой стороны.",
    )

with c6:
    outer_parts = st.number_input(
        "Внешних частей",
        min_value=0,
        max_value=20,
        value=1,
        step=1,
        key="sg_outer",
        help="Сколько уровней за границей безубытка с каждой стороны.",
    )

with c7:
    inner_fill_pct = st.number_input(
        "Заполнение внутренней зоны, %",
        min_value=1.0,
        max_value=100.0,
        value=90.0,
        step=1.0,
        format="%.1f",
        key="sg_inner_pct",
        help="До какой доли от безубытка доходит последняя внутренняя часть. 90% означает 0.90 * B.",
    )

with c8:
    outer_extra_pct = st.number_input(
        "Выход за безубыток, %",
        min_value=0.0,
        max_value=100.0,
        value=10.0,
        step=1.0,
        format="%.1f",
        key="sg_outer_pct",
        help="Насколько внешняя часть выходит за границу безубытка. 10% означает 110% от B.",
    )

if st.button("📊 Рассчитать черновик", use_container_width=True, key="sg_calc"):
    if center <= 0:
        st.error("Центр стредла должен быть больше 0.")
    elif breakeven_offset <= 0:
        st.error("Отступ безубытка должен быть больше 0.")
    elif quantity <= 0:
        st.error("Объём на часть должен быть больше 0.")
    elif int(inner_parts) + int(outer_parts) <= 0:
        st.error("Сумма внутренних и внешних частей должна быть больше 0.")
    else:
        resp = api_get(
            "/straddle/preview",
            {
                "symbol": symbol,
                "center": center,
                "breakeven_offset": breakeven_offset,
                "quantity": quantity,
                "inner_parts": int(inner_parts),
                "outer_parts": int(outer_parts),
                "ratio": ratio,
                "inner_fill_fraction": inner_fill_pct / 100.0,
                "outer_extra_fraction": outer_extra_pct / 100.0,
            },
        )
        if resp and resp.get("ok"):
            st.session_state["straddle_draft"] = resp.get("levels", [])
            st.session_state["straddle_meta"] = {
                "symbol": symbol,
                "center": resp.get("current_price", center),
                "breakeven_offset": breakeven_offset,
                "params": resp.get("params", {}),
                "buy_count": resp.get("buy_count"),
                "sell_count": resp.get("sell_count"),
            }
            st.success(f"Черновик рассчитан: {len(resp.get('levels', []))} уровней")
        else:
            st.error("Не удалось рассчитать черновик.")

draft = st.session_state.get("straddle_draft")
meta = st.session_state.get("straddle_meta", {})

if not draft:
    st.info("Задай параметры и нажми «Рассчитать черновик».")
    st.stop()

st.markdown("---")
st.subheader("📋 Таблица уровней")

df = pd.DataFrame(draft)

required_cols = [
    "slot_id",
    "symbol",
    "side",
    "index",
    "zone",
    "suggested_price",
    "final_price",
    "quantity",
    "distance_abs",
    "distance_percent",
    "source",
    "state",
]

for col in required_cols:
    if col not in df.columns:
        df[col] = ""

if "state" not in df.columns or df["state"].isna().any():
    df["state"] = df["state"].fillna("DRAFT")

display_df = df[
    [
        "slot_id",
        "side",
        "index",
        "zone",
        "suggested_price",
        "final_price",
        "quantity",
        "distance_abs",
        "distance_percent",
        "source",
        "state",
    ]
].copy()

edited = st.data_editor(
    display_df,
    use_container_width=True,
    hide_index=True,
    num_rows="fixed",
    key="sg_editor",
    disabled=[
        "slot_id",
        "side",
        "index",
        "zone",
        "suggested_price",
        "distance_abs",
        "distance_percent",
        "source",
    ],
    column_config={
        "final_price": st.column_config.NumberColumn(
            "Final price",
            format="%.4f",
            help="Цена, которую ты согласуешь. До выставления можно менять вручную.",
        ),
        "quantity": st.column_config.NumberColumn(
            "Qty",
            format="%.6f",
            help="Объём для этого уровня.",
        ),
        "state": st.column_config.SelectboxColumn(
            "State",
            options=STATE_OPTIONS,
            help="DRAFT — черновик, APPROVED — разрешено выставить, DEFERRED — отложено.",
        ),
    },
)

# Persist edits back to session state
if edited is not None:
    edited_records = edited.to_dict("records")
    st.session_state["straddle_draft"] = edited_records
    draft = edited_records

col_btn1, col_btn2, col_btn3 = st.columns(3)

with col_btn1:
    if st.button("↺ Сбросить цены к предложенным", use_container_width=True, key="sg_reset"):
        reset = []
        for row in draft:
            r = dict(row)
            r["final_price"] = r.get("suggested_price")
            r["state"] = "DRAFT"
            reset.append(r)
        st.session_state["straddle_draft"] = reset
        st.rerun()

with col_btn2:
    approved = [x for x in draft if x.get("state") == "APPROVED"]
    deferred = [x for x in draft if x.get("state") == "DEFERRED"]
    st.metric("APPROVED", len(approved))

with col_btn3:
    st.metric("DEFERRED", len(deferred))

st.markdown("---")
st.subheader("📈 График с уровнями")

kl = api_get(
    "/klines",
    {
        "symbol": meta.get("symbol", symbol),
        "interval": INTERVALS.get(interval_label, 60),
        "limit": 200,
    },
)
df_k = normalize_klines(kl)

fig = build_chart(
    df_k=df_k,
    levels=draft,
    center=float(meta.get("center") or center),
    breakeven_offset=float(meta.get("breakeven_offset") or breakeven_offset),
    symbol=meta.get("symbol", symbol),
)

st.plotly_chart(fig, use_container_width=True)

st.markdown("---")
st.subheader("🚀 Действия")

st.button(
    "✅ Выставить APPROVED (MVP-2)",
    disabled=True,
    use_container_width=True,
    help="Реальное выставление ордеров будет добавлено на следующем этапе после проверки preview и draft-логики.",
)

st.caption(
    "MVP-1: только расчёт, редактирование и визуализация. "
    "Реальные лимитные ордера пока не отправляются на Bybit."
)
