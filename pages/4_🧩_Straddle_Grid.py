import json
import streamlit as st
import requests
import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots

API_URL = "http://127.0.0.1:9000"
TIMEOUT = 25

INTERVALS = {
    "5m": 5,
    "15m": 15,
    "1h": 60,
    "4h": 240,
    "1d": "D",
    "1w": "W",
}


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
        st.warning(f"Не удалось загрузить slots: {res.get('error') if res else 'no response'}")
    return False


def finish_action(res, reload_draft=True, reload_slots=True):
    st.session_state["straddle_action_result"] = res
    if reload_draft:
        load_draft_from_server(silent=True)
    if reload_slots:
        load_slots_from_server(silent=True)
    st.rerun()


def build_chart(df_k, levels, center, breakeven_offset, symbol):
    fig = make_subplots(
        rows=1,
        cols=1,
        shared_xaxes=True,
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
        last_close = float(df_k["close"].iloc[-1])
        fig.add_hline(
            y=last_close,
            line_color="#42a5f5",
            line_width=1,
            line_dash="solid",
            annotation_text=f"Last {fmt_price(last_close)}",
            annotation_position="bottom right",
            row=1,
            col=1,
        )

    if center > 0:
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

        if state in ("APPROVED", "PLACED", "ENTRY_OPEN"):
            dash, width, opacity = "solid", 3.0, 1.0
        elif state == "COMPLETED":
            dash, width, opacity = "dot", 1.2, 0.5
        elif state in ("UNKNOWN", "CANCELLED", "REJECTED"):
            dash, width, opacity = "dashdot", 2.0, 0.8
        else:
            dash, width, opacity = "solid", 1.8, 0.9

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
        height=500,
        xaxis_rangeslider_visible=False,
        template="plotly_dark",
        margin=dict(l=10, r=10, t=30, b=10),
    )
    fig.update_yaxes(title_text="Price USDT", row=1, col=1)
    return fig


# ===================== PAGE START =====================

st.set_page_config(page_title="Straddle Grid", page_icon="🧩", layout="wide")

for k, v in {
    "straddle_draft": [],
    "straddle_meta": {},
    "straddle_slots": [],
    "straddle_selected": [],
}.items():
    if k not in st.session_state:
        st.session_state[k] = v

if "straddle_initialized" not in st.session_state:
    load_draft_from_server(silent=True)
    load_slots_from_server(silent=True)
    st.session_state["straddle_initialized"] = True

st.title("🧩 Straddle Grid")
st.caption(
    "Центр стредла, валютный безубыток, экспоненциальные уровни. "
    "Реальные ордера отправляются только после явного подтверждения."
)

# ==================== СИДБАР ====================

with st.sidebar:
    st.header("⚙️ Параметры")
    pairs = ["ETHUSDT", "XRPUSDT"]
    symbol = st.selectbox("Пара", pairs, key="sg_symbol")
    interval_label = st.selectbox("Таймфрейм", list(INTERVALS.keys()), index=2, key="sg_interval")

    st.markdown("---")

    if st.button("⟳ Загрузить с сервера", use_container_width=True, key="sg_load"):
        ok1 = load_draft_from_server()
        load_slots_from_server()
        if ok1:
            st.session_state["straddle_action_result"] = {"ok": True, "message": "Данные загружены с сервера"}
        st.rerun()

# ==================== СТРОКА СОСТОЯНИЯ ====================

lc_res = api_request("GET", "/straddle/lifecycle/status")
pos_res = api_request("GET", "/position", params={"symbol": symbol})
oo_res = api_request("GET", "/open_orders", params={"symbol": symbol})

lc_ok = bool(lc_res.get("ok")) if lc_res else False
lifecycle_enabled = bool(lc_res.get("lifecycle_enabled")) if lc_res else False
auto_exit = bool(lc_res.get("auto_exit_effective")) if lc_res else False
auto_rearm = bool(lc_res.get("auto_rearm_effective")) if lc_res else False
emergency_active = bool(lc_res.get("emergency_stop_active")) if lc_res else False

pos_data = (pos_res or {}).get("position", {})
pos_size = float(pos_data.get("size") or 0)
pos_side = pos_data.get("side") or "—"
pos_pnl = float(pos_data.get("unrealised_pnl") or 0)

oo_count = int((oo_res or {}).get("count") or 0)

sc1, sc2, sc3, sc4, sc5, sc6 = st.columns(6)
sc1.metric("Пара", symbol)
sc2.metric("Позиция", f"{pos_size} {pos_side}" if pos_size != 0 else "Нет")
sc3.metric("PnL", f"{pos_pnl:+.4f}" if pos_size != 0 else "—")
sc4.metric("Ордера", str(oo_count))
sc5.metric("Автопилот", "🟢" if lifecycle_enabled else "⛔")
sc6.metric("Emergency", "🆘" if emergency_active else "—")

st.markdown("---")

# ==================== РЕЗУЛЬТАТ ДЕЙСТВИЯ ====================

if "straddle_action_result" in st.session_state:
    res = st.session_state.pop("straddle_action_result")
    if res.get("ok"):
        st.success(f"✅ {res.get('message') or 'Действие выполнено'}")
    else:
        st.error(f"❌ {res.get('error') or res.get('message') or 'Ошибка действия'}")
    if res.get("results"):
        with st.expander("Детали результата"):
            st.json(res.get("results"))
    elif res.get("skipped"):
        with st.expander("Пропущенные слоты"):
            st.json(res.get("skipped"))

# ==================== ПАРАМЕТРЫ ЧЕРНОВИКА ====================

meta = st.session_state.get("straddle_meta", {})
draft = st.session_state.get("straddle_draft", [])

with st.expander("⚙️ Параметры черновика", expanded=not draft):
    c1, c2, c3, c4 = st.columns(4)
    with c1:
        meta_center = meta.get("center")
        center_value = float(meta_center) if meta_center else 0.0
        center = st.number_input(
            "Центр стредла (C)", min_value=0.0, value=center_value, step=0.01, format="%.4f",
            key="sg_center", help="Цена, вокруг которой строится коридор."
        )
    with c2:
        meta_be = meta.get("breakeven_offset")
        be_value = float(meta_be) if meta_be else 100.0
        breakeven_offset = st.number_input(
            "Отступ безубытка (B), USDT", min_value=0.0, value=be_value, step=1.0, format="%.2f",
            key="sg_be", help="Абсолютный отступ вверх и вниз от центра."
        )
    with c3:
        meta_qty = meta.get("quantity")
        qty_value = float(meta_qty) if meta_qty else 0.01
        quantity = st.number_input(
            "Объём на часть", min_value=0.0, value=qty_value, step=0.0001, format="%.6f",
            key="sg_qty", help="Одинаковый объём для всех частей сетки."
        )
    with c4:
        meta_ratio = meta.get("ratio")
        ratio_value = float(meta_ratio) if meta_ratio else 1.45
        ratio = st.number_input(
            "Коэффициент экспоненты", min_value=1.01, value=ratio_value, step=0.01, format="%.2f",
            key="sg_ratio", help="Чем больше, тем плотнее уровни у центра."
        )

    c5, c6, c7, c8 = st.columns(4)
    with c5:
        meta_inner = meta.get("inner_parts")
        inner_value = int(meta_inner) if meta_inner else 4
        inner_parts = st.number_input(
            "Внутренних частей", min_value=0, max_value=20, value=inner_value, step=1,
            key="sg_inner", help="Уровни внутри зоны безубытка с каждой стороны."
        )
    with c6:
        meta_outer = meta.get("outer_parts")
        outer_value = int(meta_outer) if meta_outer else 1
        outer_parts = st.number_input(
            "Внешних частей", min_value=0, max_value=20, value=outer_value, step=1,
            key="sg_outer", help="Уровни за границей безубытка с каждой стороны."
        )
    with c7:
        meta_inner_frac = meta.get("inner_fill_fraction")
        inner_frac_value = float(meta_inner_frac) * 100 if meta_inner_frac else 90.0
        inner_fill_pct = st.number_input(
            "Заполнение внутренней зоны, %", min_value=1.0, max_value=100.0, value=inner_frac_value, step=1.0, format="%.1f",
            key="sg_inner_pct", help="До какой доли от безубытка доходит последняя внутренняя часть."
        )
    with c8:
        meta_outer_frac = meta.get("outer_extra_fraction")
        outer_frac_value = float(meta_outer_frac) * 100 if meta_outer_frac else 10.0
        outer_extra_pct = st.number_input(
            "Выход за безубыток, %", min_value=0.0, max_value=100.0, value=outer_frac_value, step=1.0, format="%.1f",
            key="sg_outer_pct", help="Насколько внешняя часть выходит за границу безубытка."
        )

    c9, c10 = st.columns(2)
    with c9:
        meta_mp = meta.get("min_profit_percent")
        mp_value = float(meta_mp) if meta_mp is not None else 0.8
        min_profit_percent = st.number_input(
            "Min profit % (exit)", min_value=0.01, max_value=10.0, value=mp_value, step=0.01, format="%.2f",
            key="sg_min_profit", help="Профит для встречного ордера."
        )
    with c10:
        st.write("")
        st.caption("Exit price рассчитывается от фактической средней цены исполнения entry.")

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

# ==================== ТАБЛИЦА УРОВНЕЙ ====================

if not draft:
    st.info("Нажми «Рассчитать и сохранить черновик» или «Загрузить с сервера» в сайдбаре.")
    st.stop()

st.subheader("📋 Уровни")

rows = []
for lvl in draft:
    rows.append({
        "select": False,
        "slot_id": lvl.get("slot_id", ""),
        "side": lvl.get("side", ""),
        "final_price": float(lvl.get("final_price") or 0),
        "quantity": float(lvl.get("quantity") or 0),
        "state": lvl.get("state", "DRAFT"),
        "distance_abs": float(lvl.get("distance_abs") or 0),
        "distance_percent": float(lvl.get("distance_percent") or 0),
    })

df = pd.DataFrame(rows)

edited = st.data_editor(
    df,
    use_container_width=True,
    hide_index=True,
    num_rows="fixed",
    key="sg_editor",
    disabled=[
        "slot_id",
        "side",
        "state",
        "distance_abs",
        "distance_percent",
    ],
    column_config={
        "select": st.column_config.CheckboxColumn("✓", help="Выбрать уровень для выставления"),
        "slot_id": st.column_config.TextColumn("Slot ID", width="medium"),
        "side": st.column_config.TextColumn("Side", width="small"),
        "final_price": st.column_config.NumberColumn("Цена", format="%.4f", help="Цена уровня. До выставления можно менять."),
        "quantity": st.column_config.NumberColumn("Qty", format="%.6f", help="Объём."),
        "state": st.column_config.TextColumn("Статус", width="small"),
        "distance_abs": st.column_config.NumberColumn("Dist, $", format="%.2f", help="Абсолютное расстояние от центра."),
        "distance_percent": st.column_config.NumberColumn("Dist, %", format="%.2f", help="Расстояние от центра в процентах."),
    },
)

selected_rows = edited[edited["select"] == True]
selected_slot_ids = selected_rows["slot_id"].tolist()

b1, b2 = st.columns([2, 1])
with b1:
    if st.button(
        f"🚀 Утвердить и Выставить выбранные ({len(selected_slot_ids)})",
        use_container_width=True,
        disabled=len(selected_slot_ids) == 0,
        key="sg_place_selected",
        type="primary",
    ):
        if selected_slot_ids:
            res_state = api_request("POST", "/straddle/set-state", payload={
                "slot_ids": selected_slot_ids,
                "state": "APPROVED",
            })
            if not res_state.get("ok"):
                st.error(f"Ошибка утверждения: {res_state.get('error')}")
                st.stop()
            res_place = api_request("POST", "/straddle/place-approved", payload={
                "confirm": True,
                "symbol": symbol,
            })
            finish_action(res_place, reload_draft=True, reload_slots=True)

with b2:
    if st.button("💾 Сохранить изменения", use_container_width=True, key="sg_save"):
        changed_levels = []
        for _, row in edited.iterrows():
            lvl_data = {
                "slot_id": row["slot_id"],
                "final_price": row["final_price"],
                "quantity": row["quantity"],
                "state": row["state"],
            }
            changed_levels.append(lvl_data)
        payload = {
            "levels": changed_levels,
            "meta": st.session_state.get("straddle_meta", {}),
        }
        res = api_request("POST", "/straddle/draft", payload=payload)
        finish_action(res, reload_draft=True, reload_slots=False)

m1, m2, m3, m4 = st.columns(4)
draft_count = sum(1 for x in draft if str(x.get("state", "")).upper() == "DRAFT")
placed_count = sum(1 for x in draft if str(x.get("state", "")).upper() in {"PLACED", "ENTRY_OPEN", "UNKNOWN"})
completed_count = sum(1 for x in draft if str(x.get("state", "")).upper() == "COMPLETED")
m1.metric("DRAFT", draft_count)
m2.metric("PLACED/ACTIVE", placed_count)
m3.metric("COMPLETED", completed_count)
m4.metric("Всего уровней", len(draft))

# ==================== ГРАФИК ====================

st.markdown("---")
st.subheader("📈 График")

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
    data = kl.get("candles") or kl.get("raw") or []
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

chart_center = float(meta.get("center") or center or 0) if meta else 0
chart_be = float(meta.get("breakeven_offset") or breakeven_offset or 0) if meta else 0

fig = build_chart(
    df_k=df_k,
    levels=draft,
    center=chart_center,
    breakeven_offset=chart_be,
    symbol=symbol,
)

st.plotly_chart(fig, use_container_width=True)

# ==================== СОСТОЯНИЕ СЛОТОВ ====================

st.markdown("---")
st.subheader("🧾 Состояние слотов")

slots = st.session_state.get("straddle_slots", [])
if slots:
    slot_rows = []
    for s in slots:
        slot_rows.append({
            "slot_id": s.get("slot_id", ""),
            "side": s.get("side", ""),
            "state": s.get("state", ""),
            "price": float(s.get("price") or 0),
            "quantity": float(s.get("quantity") or 0),
            "entry_filled": float(s.get("entry_filled_qty") or 0),
            "exit_created": float(s.get("exit_created_qty") or 0),
            "exit_filled": float(s.get("exit_filled_qty") or 0),
            "last_error": s.get("last_error") or "",
        })
    slot_df = pd.DataFrame(slot_rows)
    st.dataframe(slot_df, use_container_width=True, hide_index=True)
    with st.expander("Полные данные слотов (JSON)"):
        st.json(slots)
else:
    st.info("Слотов пока нет. Они появятся после выставления ордеров.")

# ==================== АВТОПИЛОТ (LIFECYCLE) ====================

st.markdown("---")
st.subheader("🔄 Автопилот (Lifecycle)")

if lc_res and lc_ok:
    lc1, lc2, lc3, lc4 = st.columns(4)
    lc1.metric("Автопилот", "🟢 Вкл" if lifecycle_enabled else "⛔ Выкл")
    lc2.metric("Auto Exit", "✅" if auto_exit else "❌")
    lc3.metric("Auto Rearm", "✅" if auto_rearm else "❌")
    lc4.metric("Интервал, сек", lc_res.get("poll_interval_seconds", "—"))

    st.markdown("---")

    # Одна кнопка-тоггл для автопилота
    if lifecycle_enabled:
        st.success("✅ Автопилот работает. Бот сам выставляет встречные ордера и перезаряжает уровни.")
        confirm_toggle = st.checkbox("Подтверждаю выключение автопилота", key="sg_confirm_lc_toggle")
        if st.button("🔴 Выключить автопилот", use_container_width=True, disabled=not confirm_toggle, key="sg_lc_disable"):
            res = api_request("POST", "/straddle/lifecycle/disable", payload={"confirm": True})
            finish_action(res, reload_draft=False, reload_slots=False)
    else:
        st.warning("⛔ Автопилот выключен. Встречные ордера не выставляются автоматически.")
        confirm_toggle = st.checkbox("Подтверждаю включение автопилота", key="sg_confirm_lc_toggle")
        if st.button("🟢 Включить автопилот", use_container_width=True, disabled=not confirm_toggle, key="sg_lc_enable"):
            res = api_request("POST", "/straddle/lifecycle/enable", payload={"confirm": True})
            finish_action(res, reload_draft=False, reload_slots=False)

    st.markdown("---")

    # Reconcile (Сверка с биржей)
    st.markdown("**🔄 Сверка с биржей (Reconcile)**")
    st.caption("Сверяет локальное состояние бота с реальными ордерами на бирже. Не выставляет и не отменяет ордера.")

    r1, r2 = st.columns(2)
    with r1:
        if st.button("🔍 Проверить (без изменений)", use_container_width=True, key="sg_lc_reconcile_dry"):
            res = api_request("POST", "/straddle/reconcile", payload={"dry_run": True, "symbol": symbol})
            st.session_state["straddle_reconcile_report"] = res
            if res and res.get("ok"):
                st.session_state["straddle_action_result"] = {
                    "ok": True,
                    "message": (
                        f"Сверка: обновлено={len(res.get('updated', []) or [])}, "
                        f"ghosts={len(res.get('ghosts', []) or [])}, orphans={len(res.get('orphans', []) or [])}"
                    ),
                }
            else:
                st.session_state["straddle_action_result"] = res or {"ok": False, "error": "no response"}
            st.rerun()
    with r2:
        confirm_reconcile = st.checkbox("Подтверждаю применить сверку", key="sg_confirm_reconcile")
        if st.button("💾 Применить сверку", use_container_width=True, disabled=not confirm_reconcile, key="sg_lc_reconcile_apply"):
            res = api_request("POST", "/straddle/reconcile", payload={"dry_run": False, "confirm": True, "symbol": symbol})
            finish_action(res, reload_draft=True, reload_slots=True)

    # Показать отчет сверки если есть
    report = st.session_state.get("straddle_reconcile_report")
    if report:
        with st.expander("📊 Отчёт сверки"):
            rr1, rr2, rr3 = st.columns(3)
            rr1.metric("Обновлено", len(report.get("updated", []) or []))
            rr2.metric("Ghosts", len(report.get("ghosts", []) or []))
            rr3.metric("Orphans", len(report.get("orphans", []) or []))
            if report.get("errors"):
                st.error(report.get("errors"))
            st.json(report.get("slots", []) or report)
# ==================== УПРАВЛЕНИЕ СТАТУСАМИ ====================

st.markdown("---")
st.subheader("🔧 Управление статусами")

cancelled_count = sum(1 for x in draft if str(x.get("state", "")).upper() == "CANCELLED")

if cancelled_count > 0:
    st.warning(f"⚠️ Найдено {cancelled_count} слотов в статусе CANCELLED (после Emergency Stop).")
    if st.button(
        f"♻️ Сбросить все CANCELLED → DRAFT ({cancelled_count})",
        use_container_width=True,
        key="sg_reset_all_cancelled",
        type="secondary",
    ):
        res = api_request("POST", "/straddle/reset-cancelled", payload={})
        if res and res.get("ok"):
            st.success(f"✅ Сброшено {len(res.get('changed', []))} слотов")
        else:
            st.error(f"❌ Ошибка: {res.get('error', 'unknown')}")
        finish_action(res, reload_draft=True, reload_slots=True)
else:
    st.info("✅ Нет слотов в статусе CANCELLED.")

# Выбор конкретных слотов для изменения статуса
if selected_slot_ids:
    st.markdown(f"**Выбрано слотов:** {len(selected_slot_ids)}")
    new_state = st.selectbox(
        "Новый статус для выбранных",
        ["DRAFT", "APPROVED"],
        key="sg_new_state",
    )
    if st.button(
        f"🔄 Применить статус {new_state} к выбранным",
        use_container_width=True,
        key="sg_apply_state",
    ):
        res = api_request("POST", "/straddle/set-state", payload={
            "slot_ids": selected_slot_ids,
            "state": new_state,
        })
        finish_action(res, reload_draft=True, reload_slots=True)
# ==================== АВАРИЙНЫЙ КОНТУР ====================

st.markdown("---")
st.subheader("🆘 Аварийный контур")
st.caption("Опасные действия. Используй только в экстренных ситуациях.")

e1, e2, e3 = st.columns(3)

with e1:
    st.markdown("**🆘 Emergency Stop**")
    st.caption("Отменяет ВСЕ ордера и блокирует новые. Позиция остаётся открытой.")
    confirm_stop = st.checkbox("Подтверждаю аварийную остановку", key="sg_confirm_stop")
    if st.button("🆘 Emergency Stop", use_container_width=True, disabled=not confirm_stop, key="sg_emergency_stop"):
        res = api_request("POST", "/straddle/emergency-stop", payload={"confirm": True, "symbol": symbol})
        finish_action(res, reload_draft=True, reload_slots=True)

with e2:
    st.markdown("**💥 Закрыть позицию**")
    st.caption("Закрывает позицию по рынку (рыночный ордер). Не отменяет лимитные ордера.")
    confirm_close = st.checkbox("Подтверждаю закрытие позиции", key="sg_confirm_close")
    if st.button("💥 Закрыть позицию", use_container_width=True, disabled=not confirm_close, key="sg_market_close"):
        res = api_request("POST", "/straddle/market-close", payload={"confirm": True, "symbol": symbol})
        finish_action(res, reload_draft=True, reload_slots=True)

with e3:
    st.markdown("**🔓 Сбросить Emergency**")
    st.caption("Снимает блокировку после аварийной остановки. Используй когда разобрался.")
    confirm_reset = st.checkbox("Подтверждаю сброс", key="sg_confirm_reset")
    if st.button("🔓 Сбросить блокировку", use_container_width=True, disabled=not confirm_reset, key="sg_emergency_reset"):
        res = api_request("POST", "/straddle/emergency-reset", payload={"confirm": True, "reason": "dashboard_reset"})
        finish_action(res, reload_draft=False, reload_slots=False)