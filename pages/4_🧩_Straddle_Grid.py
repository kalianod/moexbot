import json
import streamlit as st
import requests
import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots

API_URL = "http://127.0.0.1:9000"
TIMEOUT = 25

STATE_OPTIONS = [
    "DRAFT",
    "APPROVED",
    "DEFERRED",
    "PLACED",
    "UNKNOWN",
    "CANCELLED",
    "REJECTED",
]

INTERVALS = {
    "15m": 15,
    "1h": 60,
    "4h": 240,
}

LEVEL_COLUMNS = [
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
    "timeframe",
    "strength",
    "touch_count",
    "state",
    "order_link_id",
    "exchange_order_id",
    "last_error",
]


def api_request(method, path, params=None, payload=None):
    try:
        r = requests.request(
            method,
            f"{API_URL}{path}",
            params=params or {},
            json=payload,
            timeout=TIMEOUT,
        )
        try:
            data = r.json()
        except Exception:
            data = {"ok": False, "error": r.text[:1000]}
        data["_status_code"] = r.status_code
        return data
    except Exception as e:
        return {"ok": False, "error": str(e), "_status_code": None}


def fmt_price(x):
    try:
        return f"{float(x):.4f}"
    except Exception:
        return str(x)


def ensure_columns(df):
    for col in LEVEL_COLUMNS:
        if col not in df.columns:
            df[col] = ""
    return df


def draft_to_df(levels):
    if not levels:
        return pd.DataFrame(columns=LEVEL_COLUMNS)
    df = pd.DataFrame(levels)
    return ensure_columns(df)


def set_session_draft(levels, meta=None):
    st.session_state["straddle_draft"] = levels or []
    st.session_state["straddle_meta"] = meta or {}


def load_draft_from_server(silent=False):
    res = api_request("GET", "/straddle/draft")
    if res and res.get("ok"):
        set_session_draft(res.get("levels", []), res.get("meta", {}))
        return True
    if not silent:
        st.error(f"Не удалось загрузить draft: {res.get('error') if res else 'no response'}")
    return False


def load_slots_from_server(silent=False):
    res = api_request("GET", "/straddle/state")
    if res and res.get("ok"):
        st.session_state["straddle_slots"] = res.get("slots", [])
        return True
    if not silent:
        st.warning(f"Не удалось загруз slots: {res.get('error') if res else 'no response'}")
    return False


def finish_action(res, reload_draft=True, reload_slots=True):
    st.session_state["straddle_action_result"] = res
    if reload_draft:
        load_draft_from_server(silent=True)
    if reload_slots:
        load_slots_from_server(silent=True)
    st.rerun()


def get_default_center(symbol):
    res = api_request("GET", "/straddle/preview", params={"symbol": symbol, "breakeven_offset": 1})
    if res and res.get("ok"):
        try:
            return float(res.get("current_price") or 0)
        except Exception:
            return 0.0
    return 0.0


def build_chart(df_k, levels, center, breakeven_offset, symbol):
    fig = make_subplots(
        rows=2,
        cols=1,
        shared_xaxes=True,
        vertical_spacing=0.03,
        row_heights=[0.78, 0.22],
    )

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
                go.Bar(
                    x=df_k["dt"],
                    y=df_k["volume"],
                    name="Volume",
                    marker_color=colors,
                    opacity=0.45,
                ),
                row=2,
                col=1,
            )

    fig.add_hline(
        y=center,
        line_color="#ffffff",
        line_dash="dot",
        line_width=1,
        annotation_text=f"Center {fmt_price(center)}",
        annotation_position="top left",
        row=1,
        col=1,
    )

    lower_be = center - breakeven_offset
    upper_be = center + breakeven_offset

    fig.add_hline(
        y=lower_be,
        line_color="#ffca28",
        line_dash="dash",
        line_width=1,
        annotation_text=f"Lower BE {fmt_price(lower_be)}",
        annotation_position="bottom left",
        row=1,
        col=1,
    )
    fig.add_hline(
        y=upper_be,
        line_color="#ffca28",
        line_dash="dash",
        line_width=1,
        annotation_text=f"Upper BE {fmt_price(upper_be)}",
        annotation_position="top left",
        row=1,
        col=1,
    )

    for lvl in levels:
        try:
            price = float(lvl.get("final_price") or lvl.get("suggested_price") or 0)
        except Exception:
            price = 0.0

        if price <= 0:
            continue

        side = lvl.get("side", "?")
        state = str(lvl.get("state", "DRAFT")).upper()
        color = "#26a69a" if side == "Buy" else "#ef5350"

        if state == "DEFERRED":
            dash = "dot"
            width = 1.2
            opacity = 0.65
        elif state in ("APPROVED", "PLACED"):
            dash = "solid"
            width = 3.0
            opacity = 1.0
        elif state in ("UNKNOWN", "CANCELLED", "REJECTED"):
            dash = "dashdot"
            width = 2.0
            opacity = 0.8
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


# =====================
# Streamlit page
# =====================

st.set_page_config(page_title="Straddle Grid", page_icon="🧩", layout="wide")

if "straddle_draft" not in st.session_state:
    st.session_state["straddle_draft"] = []
if "straddle_meta" not in st.session_state:
    st.session_state["straddle_meta"] = {}
if "straddle_slots" not in st.session_state:
    st.session_state["straddle_slots"] = []

st.title("🧩 Straddle Grid MVP-2")
st.caption("Центр стредла, валютный безубыток, экспоненциальные уровни. Реальные ордера отправляются только после явного подтверждения.")

if "straddle_action_result" in st.session_state:
    res = st.session_state.pop("straddle_action_result")
    if res.get("ok"):
        st.success(f"✅ {res.get('message') or 'Действие выполнено'}")
    else:
        st.error(f"❌ {res.get('error') or res.get('message') or 'Ошибка действия'}")
    if res.get("results"):
        st.json(res.get("results"))
    elif res.get("skipped"):
        st.json(res.get("skipped"))

col_symbol, col_interval, col_reload = st.columns([2, 2, 1])

with col_symbol:
    pairs = ["ETHUSDT", "XRPUSDT"]
    symbol = st.selectbox("Пара", pairs, key="sg_symbol")

with col_interval:
    interval_label = st.selectbox("Таймфрейм", list(INTERVALS.keys()), index=1, key="sg_interval")

with col_reload:
    st.write("")
    st.write("")
    if st.button("⟳ Загрузить с сервера", use_container_width=True, key="sg_load"):
        ok1 = load_draft_from_server()
        ok2 = load_slots_from_server()
        if ok1:
            st.session_state["straddle_action_result"] = {"ok": True, "message": "Draft загружен с сервера"}
        st.rerun()

st.markdown("---")

c1, c2, c3, c4 = st.columns(4)

with c1:
    default_center = get_default_center(symbol)
    meta_center = st.session_state.get("straddle_meta", {}).get("center")
    center_value = float(meta_center) if meta_center else float(default_center or 0.0)
    center = st.number_input(
        "Центр стредла (C)",
        min_value=0.0,
        value=center_value,
        step=0.01,
        format="%.4f",
        key="sg_center",
        help="Цена, вокруг которой строится коридор. Вводится вручную.",
    )

with c2:
    meta_be = st.session_state.get("straddle_meta", {}).get("breakeven_offset")
    be_value = float(meta_be) if meta_be else 100.0
    breakeven_offset = st.number_input(
        "Отступ безубытка (B), USDT",
        min_value=0.0,
        value=be_value,
        step=1.0,
        format="%.2f",
        key="sg_be",
        help="Абсолютный отступ вверх и вниз от центра. Например, 100 означает коридор C ± 100.",
    )

with c3:
    meta_qty = st.session_state.get("straddle_meta", {}).get("quantity")
    qty_value = float(meta_qty) if meta_qty else 0.01
    quantity = st.number_input(
        "Объём на часть",
        min_value=0.0,
        value=qty_value,
        step=0.0001,
        format="%.6f",
        key="sg_qty",
        help="Одинаковый объём для всех частей сетки.",
    )

with c4:
    meta_ratio = st.session_state.get("straddle_meta", {}).get("ratio")
    ratio_value = float(meta_ratio) if meta_ratio else 1.45
    ratio = st.number_input(
        "Коэффициент экспоненты",
        min_value=1.01,
        value=ratio_value,
        step=0.01,
        format="%.2f",
        key="sg_ratio",
        help="Чем больше значение, тем плотнее уровни у центра и тем дальше друг от друга дальние уровни.",
    )

c5, c6, c7, c8 = st.columns(4)

with c5:
    meta_inner = st.session_state.get("straddle_meta", {}).get("inner_parts")
    inner_value = int(meta_inner) if meta_inner else 4
    inner_parts = st.number_input(
        "Внутренних частей",
        min_value=0,
        max_value=20,
        value=inner_value,
        step=1,
        key="sg_inner",
        help="Сколько уровней внутри зоны безубытка с каждой стороны.",
    )

with c6:
    meta_outer = st.session_state.get("straddle_meta", {}).get("outer_parts")
    outer_value = int(meta_outer) if meta_outer else 1
    outer_parts = st.number_input(
        "Внешних частей",
        min_value=0,
        max_value=20,
        value=outer_value,
        step=1,
        key="sg_outer",
        help="Сколько уровней за границей безубытка с каждой стороны.",
    )

with c7:
    meta_inner_frac = st.session_state.get("straddle_meta", {}).get("inner_fill_fraction")
    inner_frac_value = float(meta_inner_frac) * 100 if meta_inner_frac else 90.0
    inner_fill_pct = st.number_input(
        "Заполнение внутренней зоны, %",
        min_value=1.0,
        max_value=100.0,
        value=inner_frac_value,
        step=1.0,
        format="%.1f",
        key="sg_inner_pct",
        help="До какой доли от безубытка доходит последняя внутренняя часть. 90% означает 0.90 * B.",
    )

with c8:
    meta_outer_frac = st.session_state.get("straddle_meta", {}).get("outer_extra_fraction")
    outer_frac_value = float(meta_outer_frac) * 100 if meta_outer_frac else 10.0
    outer_extra_pct = st.number_input(
        "Выход за безубыток, %",
        min_value=0.0,
        max_value=100.0,
        value=outer_frac_value,
        step=1.0,
        format="%.1f",
        key="sg_outer_pct",
        help="Насколько внешняя часть выходит за границу безубытка. 10% означает 110% от B.",
    )

c9, c10 = st.columns(2)

with c9:
    meta_mp = st.session_state.get("straddle_meta", {}).get("min_profit_percent")
    mp_value = float(meta_mp) if meta_mp is not None else 0.8
    min_profit_percent = st.number_input(
        "Min profit % (exit)",
        min_value=0.01,
        max_value=10.0,
        value=mp_value,
        step=0.01,
        format="%.2f",
        key="sg_min_profit",
        help="Профикит для встречного ордера. Buy entry: exit = entry_price * (1 + %). Sell entry: exit = entry_price * (1 - %).",
    )

with c10:
    st.write("")
    st.caption("Exit price будет рассчитываться от фактической средней цены исполнения entry.")

if st.button("📊 Рассчитать и сохранить черновик", use_container_width=True, key="sg_calc"):
    if center <= 0:
        st.error("Центр стредла должен быть больше 0.")
    elif breakeven_offset <= 0:
        st.error("Отступ безубытка должен быть больше 0.")
    elif quantity <= 0:
        st.error("Объём на часть должен быть больше 0.")
    elif int(inner_parts) + int(outer_parts) <= 0:
        st.error("Сумма внутренних и внешних частей должна быть больше 0.")
    else:
        payload = {
            "symbol": symbol,
            "center": center,
            "breakeven_offset": breakeven_offset,
            "quantity": quantity,
            "inner_parts": int(inner_parts),
            "outer_parts": int(outer_parts),
            "ratio": ratio,
            "inner_fill_fraction": inner_fill_pct / 100.0,
            "outer_extra_fraction": outer_extra_pct / 100.0,
            "min_profit_percent": min_profit_percent,
        }
        res = api_request("POST", "/straddle/draft", payload=payload)
        finish_action(res, reload_draft=True, reload_slots=True)

draft = st.session_state.get("straddle_draft", [])

if not draft:
    st.info("Нажми «Рассчитать и сохранить черновик» или «Загрузить с сервера».")
    st.stop()

st.markdown("---")
st.subheader("📋 Таблица уровней")

df = draft_to_df(draft)

edited = st.data_editor(
    df,
    use_container_width=True,
    hide_index=True,
    num_rows="fixed",
    key="sg_editor",
    disabled=[
        "slot_id",
        "symbol",
        "side",
        "index",
        "zone",
        "suggested_price",
        "distance_abs",
        "distance_percent",
        "source",
        "timeframe",
        "strength",
        "touch_count",
        "order_link_id",
        "exchange_order_id",
        "last_error",
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

current_levels = json.loads(edited.to_json(orient="records"))

slot_ids = [str(x.get("slot_id")) for x in current_levels if x.get("slot_id")]
selected_slots = st.multiselect("Выбрать уровни для действия", slot_ids, key="sg_selected_slots")

btn1, btn2, btn3, btn4 = st.columns(4)

with btn1:
    if st.button("💾 Сохранить изменения на сервер", use_container_width=True, key="sg_save"):
        payload = {
            "levels": current_levels,
            "meta": st.session_state.get("straddle_meta", {}),
        }
        res = api_request("POST", "/straddle/draft", payload=payload)
        finish_action(res, reload_draft=True, reload_slots=False)

with btn2:
    if st.button("✅ Утвердить выбранные", use_container_width=True, disabled=not selected_slots, key="sg_approve"):
        payload = {"slot_ids": selected_slots, "state": "APPROVED"}
        res = api_request("POST", "/straddle/set-state", payload=payload)
        finish_action(res, reload_draft=True, reload_slots=False)

with btn3:
    if st.button("⏸ Отложить выбранные", use_container_width=True, disabled=not selected_slots, key="sg_defer"):
        payload = {"slot_ids": selected_slots, "state": "DEFERRED"}
        res = api_request("POST", "/straddle/set-state", payload=payload)
        finish_action(res, reload_draft=True, reload_slots=False)

with btn4:
    if st.button("↺ Вернуть выбранные в DRAFT", use_container_width=True, disabled=not selected_slots, key="sg_draft_back"):
        payload = {"slot_ids": selected_slots, "state": "DRAFT"}
        res = api_request("POST", "/straddle/set-state", payload=payload)
        finish_action(res, reload_draft=True, reload_slots=False)

approved_count = sum(1 for x in current_levels if str(x.get("state", "")).upper() == "APPROVED")
deferred_count = sum(1 for x in current_levels if str(x.get("state", "")).upper() == "DEFERRED")
placed_count = sum(1 for x in current_levels if str(x.get("state", "")).upper() in {"PLACED", "UNKNOWN"})

m1, m2, m3, m4 = st.columns(4)
m1.metric("APPROVED", approved_count)
m2.metric("DEFERRED", deferred_count)
m3.metric("PLACED/UNKNOWN", placed_count)
m4.metric("Всего уровней", len(current_levels))

st.markdown("---")
st.subheader("🚀 Реальное выставление APPROVED уровней")

st.warning(
    "Ниже будут отправлены реальные лимитные ордера на Bybit только для уровней со статусом APPROVED. "
    "Перед первым боевым тестом убедись, что объём минимальный и пара правильная."
)

confirm_place = st.checkbox("Подтверждаю отправку реальных лимитных ордеров на Bybit", key="sg_confirm_place")

if st.button(
    "📤 Выставить APPROVED (реальные ордера)",
    use_container_width=True,
    disabled=not confirm_place or approved_count == 0,
    key="sg_place_approved",
):
    payload = {"confirm": True, "symbol": symbol}
    res = api_request("POST", "/straddle/place-approved", payload=payload)
    finish_action(res, reload_draft=True, reload_slots=True)

st.markdown("---")
st.subheader("🧾 Состояние слотов")

slots = st.session_state.get("straddle_slots", [])
if slots:
    slots_df = pd.DataFrame(slots)
    st.dataframe(slots_df, use_container_width=True, hide_index=True)
else:
    st.info("Слотов пока нет. Они появятся после реального выставления APPROVED уровней.")

st.markdown("---")
st.subheader("🆘 Straddle emergency stop")

st.error(
    "Эта кнопка отменит все открытые ордера по выбранной паре и переведёт активные Straddle-слоты в CANCELLED. "
    "Используй только если уверен, что на этой паре нет других важных ордеров, которые нельзя отменять."
)

confirm_stop = st.checkbox("Подтверждаю аварийную остановку Straddle и отмену ордеров по паре", key="sg_confirm_stop")

if st.button(
    "🆘 Emergency stop Straddle",
    use_container_width=True,
    disabled=not confirm_stop,
    key="sg_emergency_stop",
):
    payload = {"confirm": True, "symbol": symbol}
    res = api_request("POST", "/straddle/emergency-stop", payload=payload)
    finish_action(res, reload_draft=True, reload_slots=True)

st.markdown("---")
st.subheader("📈 График с уровнями")

kl = api_request(
    "GET",
    "/klines",
    params={
        "symbol": symbol,
        "interval": INTERVALS.get(interval_label, 60),
        "limit": 200,
    },
)

df_k = pd.DataFrame()
if kl and kl.get("ok") is not False:
    data = kl.get("klines") or kl.get("data") or kl.get("result") or []
    if data:
        df_k = pd.DataFrame(data)

        ts_col = None
        for c in ["timestamp", "timestamp_ms", "time", "dt", "datetime"]:
            if c in df_k.columns:
                ts_col = c
                break

        if ts_col:
            ts = pd.to_numeric(df_k[ts_col], errors="coerce")
            if ts.notna().any():
                unit = "ms" if float(ts.dropna().median()) > 1e12 else "s"
                df_k["dt"] = pd.to_datetime(ts, unit=unit, errors="coerce")
            else:
                df_k["dt"] = pd.to_datetime(df_k[ts_col], errors="coerce")
        else:
            df_k["dt"] = pd.date_range(end=pd.Timestamp.utcnow(), periods=len(df_k), freq="1h")

        for col in ["open", "high", "low", "close", "volume"]:
            if col in df_k.columns:
                df_k[col] = pd.to_numeric(df_k[col], errors="coerce")

        df_k = df_k.dropna(subset=["dt", "open", "high", "low", "close"]).sort_values("dt")

meta = st.session_state.get("straddle_meta", {})
chart_center = float(meta.get("center") or center or 0)
chart_be = float(meta.get("breakeven_offset") or breakeven_offset or 0)

fig = build_chart(
    df_k=df_k,
    levels=current_levels,
    center=chart_center,
    breakeven_offset=chart_be,
    symbol=symbol,
)

st.plotly_chart(fig, use_container_width=True)

st.caption(
    "MVP-2: draft сохраняется на сервере, APPROVED уровни можно отправить реальными лимитными ордерами. "
    "Auto rearm и полноценная reconciliation будут в следующих этапах."
)
