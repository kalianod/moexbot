# [ADD 2026-09-28] Streamlit page for Bybit Grid Bot monitoring/control.
# This page is additive to the existing MOEX dashboard.
# It communicates with the internal GridBot HTTP API at 127.0.0.1:9000.
# Trading control remains disabled unless ENABLE_TRADING_CONTROL=true in gridbot/.env.

import os
import json
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import streamlit as st
import requests

try:
    from streamlit_autorefresh import st_autorefresh
    HAS_AUTOREFRESH = True
except Exception:
    HAS_AUTOREFRESH = False

# ==================== CONFIG ====================

API_BASE = os.getenv("GRIDBOT_API_BASE", "http://127.0.0.1:9000")
GRIDBOT_DIR = Path(os.getenv("GRIDBOT_DIR", "/home/kalian/gridbot"))
LOG_DIR = GRIDBOT_DIR / "logs"

DEFAULT_SYMBOL = "ETHUSDT"
DEFAULT_INTERVAL = "60"
DEFAULT_KLINE_LIMIT = 200
DEFAULT_TRADE_LIMIT = 100

INTERVAL_LABELS = {
    "15": "M15",
    "60": "H1",
    "240": "H4",
    "1440": "D1",
}

# ==================== API HELPERS ====================

def api_get(path, params=None, timeout=8):
    """Read-only GET to GridBot API with safe error handling."""
    url = f"{API_BASE}{path}"
    try:
        r = requests.get(url, params=params, timeout=timeout)
        try:
            data = r.json()
        except Exception:
            data = {
                "ok": False,
                "error": f"Non-JSON response: HTTP {r.status_code}",
                "text": r.text[:500],
            }
        data["_status_code"] = r.status_code
        return data
    except requests.exceptions.RequestException as e:
        return {
            "ok": False,
            "error": f"API request failed: {e}",
            "_status_code": None,
        }


def api_post(path, json_data=None, timeout=12):
    """POST to GridBot API control endpoints with safe error handling."""
    url = f"{API_BASE}{path}"
    try:
        r = requests.post(url, json=json_data or {}, timeout=timeout)
        try:
            data = r.json()
        except Exception:
            data = {
                "ok": False,
                "error": f"Non-JSON response: HTTP {r.status_code}",
                "text": r.text[:500],
            }
        data["_status_code"] = r.status_code
        return data
    except requests.exceptions.RequestException as e:
        return {
            "ok": False,
            "error": f"API request failed: {e}",
            "_status_code": None,
        }


def tail_text_file(path, lines=120, block_size=8192):
    """Efficiently read last N lines from a text file without loading whole file."""
    try:
        p = Path(path)
        if not p.exists() or not p.is_file():
            return f"Файл не найден: {p}"

        with open(p, "rb") as f:
            f.seek(0, 2)
            file_size = f.tell()
            if file_size == 0:
                return ""

            data = b""
            while f.tell() > 0 and data.count(b"\n") <= lines:
                read_size = min(block_size, f.tell())
                f.seek(-read_size, 1)
                data = f.read(read_size) + data

        text = data.decode("utf-8", errors="ignore")
        return "\n".join(text.splitlines()[-lines:])
    except Exception as e:
        return f"Ошибка чтения лога: {e}"


def latest_log_file():
    """Return most recent GridBot log file by mtime."""
    try:
        files = list(LOG_DIR.glob("trading_bot_*.log"))
        if not files:
            return None
        return max(files, key=lambda p: p.stat().st_mtime)
    except Exception:
        return None


def fmt_num(value, digits=4):
    try:
        if value is None or value == "":
            return "—"
        return f"{float(value):,.{digits}f}"
    except Exception:
        return str(value)


def safe_float(value, default=0.0):
    try:
        if value is None or value == "":
            return default
        return float(value)
    except Exception:
        return default


def ts_to_datetime(ms):
    try:
        return datetime.fromtimestamp(int(ms) / 1000, tz=timezone.utc)
    except Exception:
        return None


# ==================== PAGE START ====================

st.title("🤖 Bybit Grid Bot Dashboard")
st.caption(f"API: {API_BASE} | GridBot dir: {GRIDBOT_DIR}")

# Health check first
health = api_get("/health")

if not health.get("ok"):
    st.error(f"❌ GridBot API недоступен: {health.get('error', 'неизвестная ошибка')}")
    st.info(
        "Проверьте, что сервис запущен:\n\n"
        "```bash\n"
        "sudo systemctl status gridbot.service\n"
        "curl -s http://127.0.0.1:9000/health | python3 -m json.tool\n"
        "```"
    )
    st.stop()

enable_control = bool(health.get("enable_control"))
telegram_enabled = bool(health.get("telegram_enabled"))
base_url = health.get("base_url", "?")

col_h1, col_h2, col_h3, col_h4 = st.columns(4)
col_h1.success("✅ API online")
col_h2.info(f"Base URL: {base_url}")
col_h3.warning(f"Control: {'🟢 ON' if enable_control else '🔴 OFF'}")
col_h4.info(f"Telegram: {'🟢 ON' if telegram_enabled else '🔴 OFF'}")

if not enable_control:
    st.warning(
        "⚠️ Управление торговлей currently выключено (`ENABLE_TRADING_CONTROL=false`). "
        "Кнопки control вернут HTTP 423. Это безопасный режим для реального счёта."
    )

# Auto-refresh control
auto_refresh = st.sidebar.toggle("Автообновление (15 сек)", value=True, key="bot_auto_refresh")
if auto_refresh and HAS_AUTOREFRESH:
    st_autorefresh(interval=15000, key="gridbot_page_refresh")
elif auto_refresh and not HAS_AUTOREFRESH:
    st.sidebar.warning("streamlit_autorefresh не установлен. Используйте кнопку обновления вручную.")

if st.sidebar.button("🔄 Обновить сейчас"):
    st.rerun()

st.markdown("---")

# Load core data
status = api_get("/status", params={"exchange": "1"})
orders = api_get("/orders")
settings = api_get("/settings")
balance = api_get("/balance")
stats = api_get("/stats")

active_pair = status.get("active_pair")
chart_symbol = active_pair or DEFAULT_SYMBOL

# Persist last control result across reruns
if "last_control_result" not in st.session_state:
    st.session_state["last_control_result"] = None

tab_overview, tab_chart, tab_control, tab_logs = st.tabs(
    ["📊 Обзор", "📈 График", "🎮 Управление", "📜 Логи"]
)

# ==================== TAB 1: OVERVIEW ====================

with tab_overview:
    st.subheader("Состояние бота")

    if not status.get("ok"):
        st.error(f"Ошибка /status: {status.get('error')}")
    else:
        c1, c2, c3, c4, c5 = st.columns(5)
        c1.metric("Запущен", "🟢 Да" if status.get("is_running") else "🔴 Нет")
        c2.metric("Активная пара", status.get("active_pair") or "—")
        c3.metric("Ожидает пару", "Да" if status.get("waiting_for_pair_selection") else "Нет")
        c4.metric("Сделок", status.get("trade_count", 0))
        c5.metric("Прибыль, USDT", fmt_num(status.get("total_profit"), 4))

        c6, c7, c8, c9, c10 = st.columns(5)
        c6.metric("Активные ордера", status.get("orders_stats", {}).get("active", 0))
        c7.metric("Filled", status.get("orders_stats", {}).get("filled", 0))
        c8.metric("Closing", status.get("orders_stats", {}).get("closing", 0))
        c9.metric("Completed", status.get("orders_stats", {}).get("completed", 0))
        c10.metric("Total open", status.get("orders_stats", {}).get("total_open", 0))

    st.markdown("---")

    st.subheader("Баланс Bybit")
    if not balance.get("ok"):
        st.error(f"Ошибка /balance: {balance.get('error')}")
    else:
        bal = balance.get("balance", {})
        if not bal.get("ok"):
            st.error(f"Bybit balance error: {bal.get('retMsg') or bal.get('error')}")
        else:
            b1, b2, b3 = st.columns(3)
            b1.metric("Total equity, USDT", fmt_num(bal.get("total_equity"), 4))
            b2.metric("Available, USDT", fmt_num(bal.get("total_available_balance"), 4))
            b3.metric("Wallet balance, USDT", fmt_num(bal.get("total_wallet_balance"), 4))

            coins = bal.get("coins", [])
            if coins:
                df_coins = pd.DataFrame(coins)
                # Keep only meaningful columns if present
                preferred = ["coin", "equity", "usdValue", "walletBalance", "unrealisedPnl", "cumRealisedPnl", "locked"]
                cols = [c for c in preferred if c in df_coins.columns]
                st.dataframe(df_coins[cols], use_container_width=True, hide_index=True)

    st.markdown("---")

    st.subheader("Позиция и открытые ордера на бирже")
    if active_pair:
        pos = api_get("/position", params={"symbol": active_pair})
        open_orders = api_get("/open_orders", params={"symbol": active_pair})

        p1, p2, p3, p4 = st.columns(4)
        if pos.get("ok"):
            position = pos.get("position", {})
            p1.metric("Size", fmt_num(position.get("size"), 6))
            p2.metric("Side", position.get("side") or "None")
            p3.metric("Avg price", fmt_num(position.get("avg_price"), 4))
            p4.metric("Unrealised PnL", fmt_num(position.get("unrealised_pnl"), 4))
        else:
            st.error(f"Ошибка /position: {pos.get('error')}")

        if open_orders.get("ok"):
            st.write(f"Открытых ордеров на бирже: **{open_orders.get('count', 0)}**")
            exchange_orders = open_orders.get("orders", [])
            if exchange_orders:
                df_ex = pd.DataFrame(exchange_orders)
                preferred_ex = ["orderId", "side", "orderType", "price", "qty", "avgPrice", "orderStatus", "createdTime", "updatedTime"]
                cols_ex = [c for c in preferred_ex if c in df_ex.columns]
                st.dataframe(df_ex[cols_ex], use_container_width=True, hide_index=True)
            else:
                st.info("Активных ордеров на бирже нет.")
        else:
            st.error(f"Ошибка /open_orders: {open_orders.get('error')}")
    else:
        st.info("Активная пара не выбрана. Позиция и ордера биржи не запрашиваются.")

    st.markdown("---")

    # [ADD 2026-09-28] Реальный PnL через /v5/execution/list
    st.subheader("💰 Реальный PnL (Bybit execution history)")

    pnl_symbol = active_pair or DEFAULT_SYMBOL
    pnl_col1, pnl_col2 = st.columns([2, 3])
    pnl_hours = pnl_col1.selectbox(
        "Период",
        options=[1, 6, 12, 24, 72, 168],
        format_func=lambda x: f"{x} ч" if x < 48 else f"{x//24} дн",
        index=3,
        key="pnl_hours"
    )
    pnl_limit = pnl_col2.select_slider(
        "Макс. исполнений",
        options=[50, 100, 200],
        value=100,
        key="pnl_limit"
    )

    if st.button("📊 Рассчитать реальный PnL", use_container_width=True, key="calc_pnl_btn"):
        pnl_resp = api_get("/pnl", params={
            "symbol": pnl_symbol,
            "hours": int(pnl_hours),
            "limit": int(pnl_limit),
        })
        st.session_state["real_pnl_summary"] = pnl_resp
        st.session_state["real_pnl_symbol"] = pnl_symbol

    pnl_resp = st.session_state.get("real_pnl_summary")
    pnl_sym = st.session_state.get("real_pnl_symbol", pnl_symbol)

    if pnl_resp:
        if not pnl_resp.get("ok"):
            st.error(f"Ошибка /pnl: {pnl_resp.get('error')}")
        else:
            summary = pnl_resp.get("summary", {})
            executions = pnl_resp.get("executions", [])

            p1, p2, p3, p4 = st.columns(4)
            p1.metric("Buy сделок", summary.get("buy_count", 0))
            p2.metric("Sell сделок", summary.get("sell_count", 0))
            p3.metric("Комиссии, USDT", fmt_num(summary.get("total_fee"), 4))
            p4.metric("Net flow, USDT", fmt_num(summary.get("net_flow"), 4))

            # PnL за выбранный период
            period_pnl = summary.get("period_realized_pnl")
            closed_trades = summary.get("closed_trades_count", 0)
            period_fee = summary.get("period_total_fee", 0.0)
            
            # Общий накопительный PnL аккаунта (за всё время)
            account_cum_pnl = summary.get("account_cumulative_pnl")
            
            # Нереализованный PnL по текущей позиции
            pos_unrealized = summary.get("position_unrealized_pnl", 0.0)
            pos_size = summary.get("position_size", 0)
            pos_side = summary.get("position_side", "None")
            
            # Баланс
            total_equity = summary.get("total_equity")
            available_balance = summary.get("available_balance")
            
            st.markdown(f"**Закрытых позиций за период:** {closed_trades}")
            
            col_pnl1, col_pnl2 = st.columns(2)
            
            with col_pnl1:
                st.markdown("**📊 PnL за выбранный период**")
                if period_pnl is not None:
                    if period_pnl >= 0:
                        st.success(f"**+{period_pnl:.4f} USDT**")
                    else:
                        st.error(f"**{period_pnl:.4f} USDT**")
                    st.caption(f"Комиссии за период: {period_fee:.4f} USDT")
                else:
                    st.info("Нет закрытых позиций за период")
            
            with col_pnl2:
                st.markdown("**💰 Текущая позиция**")
                if pos_size > 0:
                    st.info(f"{pos_side} {pos_size:.4f}")
                    if pos_unrealized >= 0:
                        st.caption(f"Unrealized: +{pos_unrealized:.4f} USDT")
                    else:
                        st.caption(f"Unrealized: {pos_unrealized:.4f} USDT")
                else:
                    st.info("Позиция закрыта")
            
            st.markdown("---")
            st.caption(f"💡 *Справочно: Общий накопительный PnL аккаунта: **{account_cum_pnl:.4f} USDT** (за всё время существования аккаунта)*" if account_cum_pnl is not None else "")
            
            if total_equity is not None and available_balance is not None:
                try:
                    eq_val = float(total_equity)
                    ab_val = float(available_balance)
                    st.caption(f"Баланс: equity {eq_val:.2f} USDT | доступно {ab_val:.2f} USDT")
                except (ValueError, TypeError):
                    st.caption(f"Баланс: equity {total_equity} USDT | доступно {available_balance} USDT")

            if executions:
                df_exec = pd.DataFrame(executions)
                df_exec["time"] = df_exec["execTime"].apply(lambda x: ts_to_datetime(x)).dt.strftime("%Y-%m-%d %H:%M:%S")
                show_cols = [c for c in ["time", "side", "execType", "price", "qty", "value", "fee"] if c in df_exec.columns]
                st.dataframe(df_exec[show_cols], use_container_width=True, hide_index=True)
    else:
        st.info("Нажмите «Рассчитать реальный PnL» для загрузки данных с Bybit")

    st.markdown("---")

    st.subheader("Сетка бота (grid_orders)")
    if not orders.get("ok"):
        st.error(f"Ошибка /orders: {orders.get('error')}")
    else:
        grid_orders = orders.get("grid_orders", [])
        if grid_orders:
            df_grid = pd.DataFrame(grid_orders)
            preferred_grid = [
                "type", "price", "quantity", "status", "order_id",
                "original_price", "closing_order_id", "closing_price",
                "source", "timeframe", "strength", "touch_count"
            ]
            cols_grid = [c for c in preferred_grid if c in df_grid.columns]
            st.dataframe(df_grid[cols_grid], use_container_width=True, hide_index=True)
        else:
            st.info("Сетка пуста. Это нормально, пока бот не выбрал пару или не запущен.")

        o1, o2, o3, o4 = st.columns(4)
        o1.metric("Occupied levels", len(orders.get("occupied_price_levels", {})))
        o2.metric("Cooldown levels", len(orders.get("level_cooldown", {})))
        o3.metric("Recently placed", len(orders.get("recently_placed_orders", {})))
        o4.metric("My order IDs", len(orders.get("my_order_ids", [])))

# ==================== TAB 2: CHART ====================

with tab_chart:
    st.subheader("График свечей, ордеров и сделок")

    # Symbol selector: default to active pair if exists
    pair_options = list((settings.get("trading_pairs") or {}).keys()) or ["ETHUSDT", "XRPUSDT"]
    if chart_symbol not in pair_options:
        pair_options = [chart_symbol] + pair_options

    c1, c2, c3, c4 = st.columns([2, 2, 2, 3])
    chart_symbol = c1.selectbox("Пара", pair_options, index=pair_options.index(chart_symbol) if chart_symbol in pair_options else 0, key="chart_symbol")
    interval_code = c2.selectbox("Таймфрейм", list(INTERVAL_LABELS.keys()), index=list(INTERVAL_LABELS.keys()).index(DEFAULT_INTERVAL), format_func=lambda x: INTERVAL_LABELS[x], key="chart_interval")
    kline_limit = c3.number_input("Свечей", min_value=50, max_value=1000, value=DEFAULT_KLINE_LIMIT, step=50, key="chart_kline_limit")
    trade_limit = c4.number_input("Сделок", min_value=20, max_value=500, value=DEFAULT_TRADE_LIMIT, step=20, key="chart_trade_limit")

    klines_resp = api_get("/klines", params={"symbol": chart_symbol, "interval": interval_code, "limit": int(kline_limit)})
    trades_resp = api_get("/trades", params={"symbol": chart_symbol, "limit": int(trade_limit)})
    orders_resp = api_get("/orders") if chart_symbol == active_pair else None

    if not klines_resp.get("ok"):
        st.error(f"Ошибка /klines: {klines_resp.get('error')}")
    else:
        candles = klines_resp.get("candles", [])
        if not candles:
            st.warning("Свечи не получены.")
        else:
            df = pd.DataFrame(candles)
            df["timestamp"] = pd.to_datetime(df["timestamp_ms"], unit="ms", utc=True)
            df = df.sort_values("timestamp")

            # Determine current price: prefer exchange snapshot from /status if active pair matches, else last close
            current_price = None
            if chart_symbol == active_pair:
                ex = status.get("exchange") or {}
                current_price = ex.get("price")
            if current_price is None:
                current_price = float(df["close"].iloc[-1])

            fig = make_subplots(
                rows=2,
                cols=1,
                shared_xaxes=True,
                vertical_spacing=0.03,
                row_heights=[0.78, 0.22],
                subplot_titles=(f"{chart_symbol} {INTERVAL_LABELS[interval_code]}", "Volume"),
            )

            fig.add_trace(
                go.Candlestick(
                    x=df["timestamp"],
                    open=df["open"],
                    high=df["high"],
                    low=df["low"],
                    close=df["close"],
                    name=chart_symbol,
                    increasing_line_color="#26a69a",
                    decreasing_line_color="#ef5350",
                ),
                row=1,
                col=1,
            )

            # Volume bars colored by candle direction
            volume_colors = [
                "#26a69a" if row.close >= row.open else "#ef5350"
                for row in df.itertuples()
            ]
            fig.add_trace(
                go.Bar(
                    x=df["timestamp"],
                    y=df["volume"],
                    name="Volume",
                    marker_color=volume_colors,
                    opacity=0.6,
                ),
                row=2,
                col=1,
            )

            # Grid orders only if chart symbol equals active bot pair
            if orders_resp and orders_resp.get("ok"):
                grid_orders = orders_resp.get("grid_orders", [])
                for order in grid_orders:
                    price = safe_float(order.get("price"))
                    if price <= 0:
                        continue
                    side = order.get("type", "?")
                    status_order = order.get("status", "?")
                    color = "#26a69a" if side == "Buy" else "#ef5350"
                    if status_order == "closing":
                        dash = "dot"
                        width = 1
                    elif status_order == "active":
                        dash = "solid"
                        width = 2
                    else:
                        dash = "dash"
                        width = 1

                    label = f"{side} {status_order} {fmt_num(price, 4)}"
                    fig.add_hline(
                        y=price,
                        line_color=color,
                        line_dash=dash,
                        line_width=width,
                        annotation_text=label,
                        annotation_position="top right",
                        annotation_font_size=10,
                        row=1,
                        col=1,
                    )

            # Closed trades from Bybit history
            if trades_resp.get("ok"):
                trades = trades_resp.get("trades", [])
                buy_x, buy_y, buy_text = [], [], []
                sell_x, sell_y, sell_text = [], [], []

                for t in trades:
                    if t.get("orderStatus") != "Filled":
                        continue
                    ts = ts_to_datetime(t.get("updatedTime") or t.get("createdTime"))
                    price = safe_float(t.get("avgPrice") or t.get("price"))
                    side = t.get("side")
                    if ts is None or price <= 0 or side not in ("Buy", "Sell"):
                        continue

                    qty = t.get("cumExecQty") or t.get("qty") or ""
                    fee = t.get("cumExecFee") or ""
                    text = f"{side} {qty} @ {price} fee={fee} id={t.get('orderId', '')[:8]}"

                    if side == "Buy":
                        buy_x.append(ts)
                        buy_y.append(price)
                        buy_text.append(text)
                    else:
                        sell_x.append(ts)
                        sell_y.append(price)
                        sell_text.append(text)

                if buy_x:
                    fig.add_trace(
                        go.Scatter(
                            x=buy_x,
                            y=buy_y,
                            mode="markers",
                            name="Buy fill",
                            marker=dict(symbol="triangle-up", size=13, color="#26a69a", line=dict(width=1, color="white")),
                            text=buy_text,
                            hoverinfo="text+x+y",
                        ),
                        row=1,
                        col=1,
                    )

                if sell_x:
                    fig.add_trace(
                        go.Scatter(
                            x=sell_x,
                            y=sell_y,
                            mode="markers",
                            name="Sell fill",
                            marker=dict(symbol="triangle-down", size=13, color="#ef5350", line=dict(width=1, color="white")),
                            text=sell_text,
                            hoverinfo="text+x+y",
                        ),
                        row=1,
                        col=1,
                    )

            # Current price line
            fig.add_hline(
                y=current_price,
                line_color="#42a5f5",
                line_width=2,
                annotation_text=f"Last {fmt_num(current_price, 4)}",
                annotation_position="bottom right",
                row=1,
                col=1,
            )

            fig.update_layout(
                height=760,
                margin=dict(l=10, r=10, t=40, b=10),
                xaxis_rangeslider_visible=False,
                legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
                hovermode="x unified",
            )
            fig.update_yaxes(title_text="Price USDT", row=1, col=1)
            fig.update_yaxes(title_text="Volume", row=2, col=1)

            # [ADD 2026-09-29] Кнопка предпросмотра уровней
            st.markdown("---")
            st.subheader("🔮 Предпросмотр уровней сетки")
            
            preview_col1, preview_col2 = st.columns([3, 2])
            with preview_col1:
                preview_symbol = st.selectbox(
                    "Пара для анализа",
                    pair_options,
                    index=pair_options.index(chart_symbol) if chart_symbol in pair_options else 0,
                    key="preview_symbol"
                )
            with preview_col2:
                st.write("")
                st.write("")
                if st.button("📊 Рассчитать уровни", use_container_width=True, key="calc_levels_btn"):
                    preview_resp = api_get("/levels-preview", params={"symbol": preview_symbol})
                    st.session_state["levels_preview"] = preview_resp
                    st.session_state["levels_preview_symbol"] = preview_symbol
            
            preview_resp = st.session_state.get("levels_preview")
            preview_sym = st.session_state.get("levels_preview_symbol", preview_symbol)
            
            if preview_resp:
                if not preview_resp.get("ok"):
                    st.error(f"Ошибка /levels-preview: {preview_resp.get('error')}")
                else:
                    current_price = preview_resp.get("current_price", 0)
                    levels = preview_resp.get("levels", [])
                    buy_count = preview_resp.get("buy_count", 0)
                    sell_count = preview_resp.get("sell_count", 0)
                    
                    st.success(
                        f"**{preview_sym}** | Текущая цена: **{current_price:.2f} USDT** | "
                        f"Уровней: **{len(levels)}** (Buy: {buy_count}, Sell: {sell_count})"
                    )
                    
                    if levels:
                        # Таблица уровней
                        df_levels = pd.DataFrame(levels)
                        df_display = df_levels.rename(columns={
                            'level': '#',
                            'price': 'Цена',
                            'type': 'Тип',
                            'source': 'Источник',
                            'timeframe': 'ТФ',
                            'strength': 'Сила',
                            'touch_count': 'Касания',
                            'distance_percent': 'Расст. %',
                            'value_usdt': 'Стоим. USDT',
                            'expected_profit': 'Ожид. прибыль',
                        })
                        show_cols = ['#', 'Цена', 'Тип', 'Источник', 'ТФ', 'Сила', 'Касания', 'Расст. %', 'Стоим. USDT']
                        st.dataframe(
                            df_display[show_cols],
                            use_container_width=True,
                            hide_index=True,
                            height=300
                        )
                        
                        # Добавляем уровни на график свечей
                        for lev in levels:
                            price = lev['price']
                            level_type = lev['type']
                            source = lev['source']
                            strength = lev['strength']
                            
                            color = "#26a69a" if level_type == "Buy" else "#ef5350"
                            if source == "imbalance":
                                dash = "dot"
                                width = 1.5
                            else:
                                dash = "solid"
                                width = 1
                            
                            if strength >= 7:
                                width += 0.8
                            
                            label = f"{level_type} {price:.2f} ({source}/{strength})"
                            fig.add_hline(
                                y=price,
                                line_color=color,
                                line_dash=dash,
                                line_width=width,
                                annotation_text=label,
                                annotation_position="top left",
                                annotation_font_size=9,
                                annotation_font_color=color,
                                row=1,
                                col=1,
                            )
                        
                        st.plotly_chart(fig, use_container_width=True, key="chart_with_levels")
                        st.info("💡 Уровни добавлены на график: сплошная = technical, пунктир = imbalance, толще = сильнее")
                    else:
                        st.warning("Уровни не рассчитаны")
            else:
                st.info("Нажмите 'Рассчитать уровни' для предварительного анализа уровней сетки")

            st.markdown("---")

            # Trade table below chart
            st.write("Последние исполненные ордера Bybit")
            if trades_resp.get("ok") and trades_resp.get("trades"):
                df_trades = pd.DataFrame(trades_resp.get("trades", []))
                preferred_trades = [
                    "orderId", "symbol", "side", "orderType", "price", "avgPrice",
                    "qty", "cumExecQty", "cumExecValue", "cumExecFee",
                    "orderStatus", "createdTime", "updatedTime", "orderLinkId"
                ]
                cols_trades = [c for c in preferred_trades if c in df_trades.columns]
                # Convert times for readability
                if "createdTime" in df_trades.columns:
                    df_trades["createdTime_dt"] = df_trades["createdTime"].apply(lambda x: ts_to_datetime(x)).dt.strftime("%Y-%m-%d %H:%M:%S")
                if "updatedTime" in df_trades.columns:
                    df_trades["updatedTime_dt"] = df_trades["updatedTime"].apply(lambda x: ts_to_datetime(x)).dt.strftime("%Y-%m-%d %H:%M:%S")
                show_cols = cols_trades + [c for c in ["createdTime_dt", "updatedTime_dt"] if c in df_trades.columns]
                st.dataframe(df_trades[show_cols].head(100), use_container_width=True, hide_index=True)
            else:
                st.info("История исполненных ордеров пуста.")

# ==================== TAB 3: CONTROL ====================

with tab_control:
    st.subheader("Управление ботом")

    if st.session_state.get("last_control_result"):
        res = st.session_state["last_control_result"]
        status_code = res.get("_status_code")
        if res.get("ok"):
            st.success(f"✅ Успех: HTTP {status_code}")
        elif status_code == 423:
            st.warning(f"🔒 Управление выключено: HTTP {status_code}. {res.get('error')}")
        elif status_code == 428:
            st.error(f"⚠️ Требуется подтверждение: HTTP {status_code}. {res.get('error')}")
        else:
            st.error(f"❌ Ошибка: HTTP {status_code}. {res.get('error') or res}")
        st.json(res)
        st.markdown("---")

    if not enable_control:
        st.info(
            "Чтобы включить управление, нужно в `/home/kalian/gridbot/.env` установить:\n\n"
            "```env\n"
            "ENABLE_TRADING_CONTROL=true\n"
            "```\n\n"
            "и перезапустить `gridbot.service`. На реальном счёте включайте только после проверки всех защит."
        )

    c1, c2, c3 = st.columns(3)

    with c1:
        st.markdown("**Мониторинг**")
        if st.button("🚀 Запустить мониторинг", use_container_width=True):
            st.session_state["last_control_result"] = api_post("/control/start", {})
            st.rerun()

        if st.button("⏸ Остановить мониторинг", use_container_width=True):
            st.session_state["last_control_result"] = api_post("/control/stop", {})
            st.rerun()

    with c2:
        st.markdown("**Сетка**")
        pair_options = list((settings.get("trading_pairs") or {}).keys()) or ["ETHUSDT", "XRPUSDT"]
        start_pair = st.selectbox("Пара для запуска сетки", pair_options, key="start_pair_select")
        confirm_start = st.checkbox("Подтверждаю запуск реальной сетки", key="confirm_start_pair")
        if st.button("▶ Запустить сетку пары", use_container_width=True, disabled=not confirm_start):
            st.session_state["last_control_result"] = api_post("/control/start-pair", {"pair": start_pair, "confirm": True})
            st.rerun()

        confirm_restart = st.checkbox("Подтверждаю перезапуск сетки", key="confirm_restart_grid")
        if st.button("🔄 Перезапустить сетку", use_container_width=True, disabled=not confirm_restart):
            st.session_state["last_control_result"] = api_post("/control/restart-grid", {"confirm": True})
            st.rerun()

    with c3:
        st.markdown("**Аварийное управление**")
        confirm_emergency = st.checkbox("Подтверждаю экстренную остановку и отмену ордеров", key="confirm_emergency")
        if st.button("🆘 Экстренная остановка", use_container_width=True, disabled=not confirm_emergency):
            st.session_state["last_control_result"] = api_post("/control/emergency-stop", {"confirm": True})
            st.rerun()

    st.markdown("---")
    st.subheader("Настройки пары")

    trading_pairs = settings.get("trading_pairs") or {}
    if not trading_pairs:
        st.warning("Настройки пар не получены из /settings.")
    else:
        settings_pair = st.selectbox("Пара для изменения настроек", list(trading_pairs.keys()), key="settings_pair_select")
        current = trading_pairs.get(settings_pair, {})

        with st.form("settings_form"):
            sc1, sc2, sc3, sc4 = st.columns(4)
            quantity = sc1.number_input(
                "Объём ордера",
                min_value=0.0001,
                value=float(current.get("quantity", 0.01)),
                step=0.0001,
                format="%.6f",
            )
            grid_levels = sc2.number_input(
                "Уровней",
                min_value=1,
                max_value=20,
                value=int(current.get("grid_levels", 4)),
                step=1,
            )
            spread = sc3.number_input(
                "Спред %",
                min_value=0.1,
                max_value=50.0,
                value=float(current.get("grid_spread_percent", 5.0)),
                step=0.1,
                format="%.2f",
            )
            profit = sc4.number_input(
                "Прибыль %",
                min_value=0.1,
                max_value=5.0,
                value=float(current.get("min_profit_percent", 0.5)),
                step=0.1,
                format="%.2f",
            )

            submitted = st.form_submit_button("💾 Сохранить настройки", use_container_width=True)
            if submitted:
                results = []
                payload_map = {
                    "quantity": quantity,
                    "grid_levels": grid_levels,
                    "grid_spread_percent": spread,
                    "min_profit_percent": profit,
                }
                for key, value in payload_map.items():
                    res = api_post("/control/settings", {"pair": settings_pair, "key": key, "value": value})
                    results.append({"key": key, "value": value, "response": res})
                st.session_state["last_control_result"] = {
                    "ok": all(r["response"].get("ok") for r in results),
                    "results": results,
                    "_status_code": 200 if all(r["response"].get("ok") for r in results) else None,
                }
                st.rerun()

# ==================== TAB 4: LOGS ====================

with tab_logs:
    st.subheader("Логи GridBot")

    log_file = latest_log_file()
    if log_file is None:
        st.warning(f"Логи не найдены в {LOG_DIR}")
    else:
        st.write(f"Файл: `{log_file}`")
        log_lines = st.number_input("Строк лога", min_value=20, max_value=1000, value=200, step=50, key="log_lines")
        log_text = tail_text_file(log_file, int(log_lines))
        st.code(log_text or "(пусто)", language="text")

    st.markdown("---")
    st.subheader("Файлы состояния")
    for fname in ["trading_config.json", "trading_stats.json", "grid_orders.json"]:
        fpath = GRIDBOT_DIR / fname
        st.write(f"`{fpath}` — {'существует' if fpath.exists() else 'отсутствует'}")
        if fpath.exists():
            try:
                with open(fpath, "r", encoding="utf-8") as f:
                    data = json.load(f)
                st.json(data)
            except Exception as e:
                st.error(f"Ошибка чтения JSON: {e}")

# ==================== FOOTER ====================

st.markdown("---")
st.caption(
    f"GridBot Dashboard page | API {API_BASE} | "
    f"control={'ON' if enable_control else 'OFF'} | "
    f"last render {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')}"
)
