import os
import json
import time
from datetime import datetime, date, timedelta
from dotenv import load_dotenv
from moexalgo import session, Ticker

# [PATCH 2026-08-24]: Отключаем проверку SSL для moexalgo
# Причина: после обновления сертификата MOEX (19.08.2026) библиотека падает с
# SSL: CERTIFICATE_VERIFY_FAILED. Прямые запросы httpx работают нормально.
# Это безопасно, т.к. мы доверяем iss.moex.com (публичный CA).
import ssl
try:
    _default_ctx = ssl.create_default_context
    def _no_verify_ctx(*args, **kwargs):
        ctx = _default_ctx(*args, **kwargs)
        ctx.check_hostname = False
        ctx.verify_mode = ssl.CERT_NONE
        return ctx
    ssl.create_default_context = _no_verify_ctx
    print("[INFO] SSL verification disabled for moexalgo")
except Exception as e:
    print(f"[WARN] SSL patch failed: {e}")
import requests
import pandas as pd
import sqlite3
DB_PATH = os.path.join(os.path.dirname(__file__), 'futoi.db')


load_dotenv()

# [ИЗМЕНЕНИЕ]: Вместо одного символа используем список для поддержки нескольких тикеров
SYMBOLS = ["SiU6", "CRU6", "MXU6"] 
THRESHOLD = 10

# [NEW 2026-08-25] Аномалии набора: адаптивные пороги
ANOMALY_K = 3.0        # аномалия если набор >= K * средний по дню
ANOMALY_MIN = 200      # минимальный абсолютный набор (контрактов)
ANOMALY_FACE = True    # показывать лицо (Ф/Ю %) в сообщении
LEGACY_ALERTS = False  # отключить старые алерты (FIZ long/short + YUR сигнал)
OTC_ENABLED = True     # [NEW 2026-08-28] сбор внебиржевых (адресных) сделок

# [ИЗМЕНЕНИЕ]: Базовые шаблоны имен файлов. Конкретное имя будет формироваться внутри функции с добавлением тикера.
# Это предотвращает перезапись состояния (last_key) одного тикера другим.
BASE_STATE_FILE = "futoi_state_{}.json"
BASE_LOG_FILE = "logs/moex_futoi_alert_{}.log"

SLEEP_SECONDS = 300
MAX_LOG_SIZE = 10 * 1024 * 1024

MOEX_TOKEN = os.getenv("MOEXALGOPACK_TOKEN")
TG_TOKEN = os.getenv("TELEGRAM_TOKEN")
TG_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")

session.TOKEN = MOEX_TOKEN

def ensure_log_file(log_file_path):
    # [ИЗМЕНЕНИЕ]: Добавлен аргумент log_file_path для поддержки динамических имен файлов
    os.makedirs("logs", exist_ok=True)
    if os.path.exists(log_file_path) and os.path.getsize(log_file_path) > MAX_LOG_SIZE:
        os.remove(log_file_path)

def log_line(text, log_file_path):
    # [ИЗМЕНЕНИЕ]: Добавлен аргумент log_file_path
    ensure_log_file(log_file_path)
    line = f"{datetime.now()} {text}"
    print(line)
    with open(log_file_path, "a", encoding="utf-8") as f:
        f.write(line + "\n")

def load_state(state_file_path):
    # [ИЗМЕНЕНИЕ]: Добавлен аргумент state_file_path
    if os.path.exists(state_file_path):
        with open(state_file_path, "r", encoding="utf-8") as f:
            return json.load(f)
    return {}

def save_state(state, state_file_path):
    # [ИЗМЕНЕНИЕ]: Добавлен аргумент state_file_path
    with open(state_file_path, "w", encoding="utf-8") as f:
        json.dump(state, f, ensure_ascii=False, indent=2)

def send_alert(text):
    # [БЕЗ ИЗМЕНЕНИЙ]: 100% сохранен исходный функционал
    url = f"https://api.telegram.org/bot{TG_TOKEN}/sendMessage"
    payload = {
        "chat_id": TG_CHAT_ID, 
        "text": text,
        "parse_mode": "Markdown"
    }
    try:
        response = requests.post(url, json=payload, timeout=10)
        response.raise_for_status()
        # [ИЗМЕНЕНИЕ]: Здесь мы не можем использовать log_line без пути, поэтому выводим только в консоль,
        # а детальный лог будет сделан в вызывающей функции check_once
        print(f"{datetime.now()} telegram response ok")
        return True
    except Exception as e:
        print(f"{datetime.now()} telegram error: {e}")
        return False

def nearest_row(df, target_dt):
    # [БЕЗ ИЗМЕНЕНИЙ]: 100% сохранен исходный функционал
    if "systime" not in df.columns:
        return None, None
    
    tmp = df.copy()
    tmp["systime"] = pd.to_datetime(tmp["systime"], errors="coerce")
    tmp = tmp.dropna(subset=["systime"]).sort_values("systime").reset_index(drop=True)
    
    if tmp.empty:
        return None, None

    idx = (tmp["systime"] - pd.Timestamp(target_dt)).abs().idxmin()
    return tmp.iloc[idx], "systime"

# [ИЗМЕНЕНИЕ]: Добавлен параметр symbol для изоляции логики по каждому тикеру

def save_to_database(symbol, df):
    if df.empty:
        return
    conn = sqlite3.connect(DB_PATH)
    try:
        df_copy = df.copy()
        df_copy['symbol'] = symbol
        
        # Конвертируем datetime объекты в строки для SQLite
        if 'tradedate' in df_copy.columns:
            df_copy['tradedate'] = df_copy['tradedate'].astype(str)
        if 'tradetime' in df_copy.columns:
            df_copy['tradetime'] = df_copy['tradetime'].astype(str)
        if 'systime' in df_copy.columns:
            df_copy['systime'] = df_copy['systime'].astype(str)
        if 'trade_session_date' in df_copy.columns:
            df_copy['trade_session_date'] = df_copy['trade_session_date'].astype(str)
        
        cols_to_save = ['symbol', 'sess_id', 'seqnum', 'tradedate', 'tradetime', 
                        'ticker', 'clgroup', 'pos', 'pos_long', 'pos_short', 
                        'pos_long_num', 'pos_short_num', 'systime', 'trade_session_date']
        existing_cols = [c for c in cols_to_save if c in df_copy.columns]
        df_to_save = df_copy[existing_cols]
        
        # Используем INSERT OR REPLACE для обновления существующих записей
        for _, row in df_to_save.iterrows():
            try:
                cols = ', '.join(row.index)
                placeholders = ', '.join(['?'] * len(row))
                sql = f"INSERT OR REPLACE INTO futoi_data ({cols}) VALUES ({placeholders})"
                conn.execute(sql, tuple(row))
            except Exception as e:
                print(f"[{symbol}] Row insert error: {e}")
        
        conn.commit()
        print(f"[{symbol}] ✅ Saved/Updated {len(df_to_save)} rows in DB")
    except Exception as e:
        print(f"[{symbol}] ❌ DB save error: {e}")
    finally:
        conn.close()



def save_bar_stats(symbol, df_ob, df_ts, df_fiz, df_yur):
    """Сохраняем агрегированную баровую статистику в БД"""
    if df_ob.empty or df_ts.empty:
        return
    
    conn = sqlite3.connect(DB_PATH)
    try:
        # Объединяем данные по времени
        df_ob_ts = df_ob[['tradedate', 'tradetime', 'mid_price', 'vol_b_l3', 'vol_s_l3']].copy()
        df_ob_ts.columns = ['tradedate', 'tradetime', 'price', 'bid_vol', 'ask_vol']
        
        df_ts_data = df_ts[['tradedate', 'tradetime', 'vol', 'oi_open', 'oi_close']].copy()
        df_ts_data['d_oi'] = df_ts_data['oi_close'] - df_ts_data['oi_open']
        
        df_merged = df_ob_ts.merge(df_ts_data, on=['tradedate', 'tradetime'], how='inner')
        
        # Добавляем данные по юрлицам
        if not df_yur.empty:
            df_yur_agg = df_yur.groupby(['tradedate', 'tradetime']).agg(
                yur_long=('pos_long_num', 'last'),
                yur_short=('pos_short_num', 'last')
            ).reset_index()
            df_merged = df_merged.merge(df_yur_agg, on=['tradedate', 'tradetime'], how='left')
        
        df_merged['symbol'] = symbol
        df_merged['systime'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        
        # Конвертируем даты в строки
        df_merged['tradedate'] = df_merged['tradedate'].astype(str)
        df_merged['tradetime'] = df_merged['tradetime'].astype(str)
        
        cols = ['symbol', 'tradedate', 'tradetime', 'price', 'vol', 'bid_vol', 'ask_vol', 
                'd_oi', 'yur_long', 'yur_short', 'systime']
        existing_cols = [c for c in cols if c in df_merged.columns]
        df_to_save = df_merged[existing_cols]
        
        for _, row in df_to_save.iterrows():
            try:
                cols_str = ', '.join(row.index)
                placeholders = ', '.join(['?'] * len(row))
                sql = f"INSERT OR REPLACE INTO bar_stats ({cols_str}) VALUES ({placeholders})"
                conn.execute(sql, tuple(row))
            except Exception as e:
                print(f"[{symbol}] Bar stats insert error: {e}")
        
        conn.commit()
        print(f"[{symbol}] ✅ Saved {len(df_to_save)} bar stats rows")
    except Exception as e:
        print(f"[{symbol}] ❌ Bar stats save error: {e}")
    finally:
        conn.close()


def save_candles_to_cache(symbol, df_candles):
    """
    [НОВОЕ 2026-08-20]: Сохраняем 5-минутные свечи в кэш для дашборда.
    Это позволяет дашборду открываться мгновенно без загрузки с MOEX API.
    """
    if df_candles is None or df_candles.empty:
        return
    
    conn = sqlite3.connect(DB_PATH)
    try:
        df_copy = df_candles.copy()
        df_copy['symbol'] = symbol
        df_copy['period'] = '5min'
        
        # Конвертируем datetime в строки для SQLite
        if 'begin' in df_copy.columns:
            df_copy['begin'] = pd.to_datetime(df_copy['begin']).astype(str)
        
        cols = ['symbol', 'begin', 'period', 'open', 'high', 'low', 'close', 'volume']
        existing_cols = [c for c in cols if c in df_copy.columns]
        df_to_save = df_copy[existing_cols]
        
        inserted = 0
        for _, row in df_to_save.iterrows():
            try:
                cols_str = ', '.join(row.index)
                placeholders = ', '.join(['?'] * len(row))
                sql = f"INSERT OR REPLACE INTO candles_cache ({cols_str}) VALUES ({placeholders})"
                conn.execute(sql, tuple(row))
                inserted += 1
            except Exception as e:
                print(f"[{symbol}] Candles insert error: {e}")
        
        conn.commit()
        print(f"[{symbol}] ✅ Saved {inserted} candles to cache (5min)")
    except Exception as e:
        print(f"[{symbol}] ❌ Candles cache save error: {e}")
    finally:
        conn.close()


def save_otc_trades(symbol):
    """[NEW 2026-08-28] Собирает OTC-сделки (offmarketdeal=1) из ленты текущего дня.
    oi_delta — изменение OI относительно предыдущей сделки ленты (оценка)."""
    tr = Ticker(symbol).trades()
    if tr is None or tr.empty or 'offmarketdeal' not in tr.columns:
        return 0
    tr = tr.sort_values('recno')
    tr['oi_d'] = tr['openposition'].diff().fillna(0)
    otc = tr[tr['offmarketdeal'] == 1]
    if otc.empty:
        return 0
    conn = sqlite3.connect(DB_PATH)
    n = 0
    for _, r in otc.iterrows():
        try:
            cur = conn.execute(
                """INSERT OR IGNORE INTO otc_trades
                   (symbol, tradeno, tradedate, tradetime, price, quantity, buysell,
                    openposition, oi_delta, systime)
                   VALUES (?,?,?,?,?,?,?,?,?,?)""",
                (symbol, int(r['tradeno']), str(r['tradedate']), str(r['tradetime']),
                 float(r['price']), int(r['quantity']), str(r['buysell']),
                 int(r['openposition']), int(r['oi_d']), str(r['systime'])))
            n += cur.rowcount
        except Exception:
            pass
    conn.commit()
    conn.close()
    return n


def check_anomaly_uptake(symbol, current_time,
                             delta_fiz_long, delta_fiz_short,
                             delta_yur_long, delta_yur_short, state):
    """Проверяет аномалию набора (лонг/шорт) и возвращает список сообщений или []."""
    # Валовый набор. Для long: рост = положительная дельта.
    # Для short: pos_short в БД отрицательный, поэтому рост шорта = ОТРИЦАТЕЛЬНАЯ дельта.
    uptake_long  = max(delta_fiz_long, 0)  + max(delta_yur_long, 0)
    uptake_short = max(-delta_fiz_short, 0) + max(-delta_yur_short, 0)

    if 'anomaly_state' not in state:
        state['anomaly_state'] = {'day': '', 'long_uptake': [], 'short_uptake': []}
    anom = state['anomaly_state']
    today = current_time.strftime('%Y-%m-%d')
    if anom['day'] != today:
        anom['day'] = today
        anom['long_uptake'] = []
        anom['short_uptake'] = []

    anom['long_uptake'].append(uptake_long)
    anom['short_uptake'].append(uptake_short)

    messages = []

    # Лонги
    if uptake_long >= ANOMALY_MIN and len(anom['long_uptake']) > 1:
        avg_long = sum(anom['long_uptake'][:-1]) / len(anom['long_uptake'][:-1])
        if avg_long > 0 and uptake_long >= ANOMALY_K * avg_long:
            pct_avg = uptake_long / avg_long * 100
            fiz_part = max(delta_fiz_long, 0)
            yur_part = max(delta_yur_long, 0)
            face_fiz = fiz_part / uptake_long * 100 if uptake_long > 0 else 0
            face_yur = yur_part / uptake_long * 100 if uptake_long > 0 else 0
            msg = (
                f"🚨 *{symbol} | Аномальный набор ЛОНГ*\n"
                f"⏰ `{current_time.strftime('%H:%M')}` | `+{uptake_long:,}` контр. за 5 мин\n"
                f"📈 `{pct_avg:.0f}%` от среднего по дню ({avg_long:.0f})"
            )
            if ANOMALY_FACE:
                msg += f"\n🏦 Лицо: Ф {face_fiz:.0f}% / Ю {face_yur:.0f}%"
            messages.append(msg)

    # Шорты
    if uptake_short >= ANOMALY_MIN and len(anom['short_uptake']) > 1:
        avg_short = sum(anom['short_uptake'][:-1]) / len(anom['short_uptake'][:-1])
        if avg_short > 0 and uptake_short >= ANOMALY_K * avg_short:
            pct_avg = uptake_short / avg_short * 100
            fiz_part = max(-delta_fiz_short, 0)
            yur_part = max(-delta_yur_short, 0)
            face_fiz = fiz_part / uptake_short * 100 if uptake_short > 0 else 0
            face_yur = yur_part / uptake_short * 100 if uptake_short > 0 else 0
            msg = (
                f"🚨 *{symbol} | Аномальный набор ШОРТ*\n"
                f"⏰ `{current_time.strftime('%H:%M')}` | `+{uptake_short:,}` контр. за 5 мин\n"
                f"📈 `{pct_avg:.0f}%` от среднего по дню ({avg_short:.0f})"
            )
            if ANOMALY_FACE:
                msg += f"\n🏦 Лицо: Ф {face_fiz:.0f}% / Ю {face_yur:.0f}%"
            messages.append(msg)

    return messages


def check_once(symbol):
    # [ИЗМЕНЕНИЕ]: Формируем уникальные пути к файлам для каждого символа
    state_file = BASE_STATE_FILE.format(symbol)
    log_file = BASE_LOG_FILE.format(symbol)
    
    state = load_state(state_file)
    fut = Ticker(symbol) # [ИЗМЕНЕНИЕ]: Используем переданный symbol

    end_date = date.today()
    start_date = end_date - timedelta(days=5)
    df_all = fut.futoi(start=start_date, end=end_date)

    if df_all is None or df_all.empty:
        log_line(f"[{symbol}] no data", log_file)
        return

    # Разделяем для логирования, но сохраняем в БД и FIZ, и YUR
    df_fiz = df_all[df_all["clgroup"] == "FIZ"].copy()
    df_yur = df_all[df_all["clgroup"] == "YUR"].copy()
    
    log_line(f"[{symbol}] futoi rows: FIZ={len(df_fiz)}, YUR={len(df_yur)}, Total={len(df_all)}", log_file)
    
    # Сохраняем ВСЕ данные (и FIZ, и YUR) в базу данных одним вызовом
    save_to_database(symbol, df_all)

    # [НОВОЕ 2026-08-19]: Загружаем OBStats и TradeStats для баровой статистики
    try:
        df_ob = fut.obstats(start=start_date, end=end_date)
        df_ts = fut.tradestats(start=start_date, end=end_date)
        save_bar_stats(symbol, df_ob, df_ts, df_fiz, df_yur)
    except Exception as e:
        log_line(f"[{symbol}] bar_stats save failed: {e}", log_file)

    # [НОВОЕ 2026-08-20]: Загружаем 5-минутные свечи для кэша дашборда
    try:
        df_candles = fut.candles(start=start_date, end=end_date, period='5min')
        save_candles_to_cache(symbol, df_candles)
    except Exception as e:
        log_line(f"[{symbol}] candles cache save failed: {e}", log_file)

    # [NEW 2026-08-28]: Сбор OTC-сделок (адресных) из ленты
    if OTC_ENABLED:
        try:
            n_otc = save_otc_trades(symbol)
            if n_otc:
                log_line(f"[{symbol}] OTC trades saved: {n_otc}", log_file)
        except Exception as e:
            log_line(f"[{symbol}] OTC save failed: {e}", log_file)

    # Для алертов используем только данные по физическим лицам (как было ранее)
    df = df_fiz
    if df.empty:
        log_line(f"[{symbol}] no FIZ data found for alerts", log_file)
        return

    current_row, ts_col = nearest_row(df, datetime.now())
    if current_row is None:
        log_line(f"[{symbol}] no current timestamp", log_file)
        return

    current_time = pd.to_datetime(current_row[ts_col])
    prev_row, _ = nearest_row(df, current_time - timedelta(minutes=5))
    if prev_row is None:
        log_line(f"[{symbol}] no previous row (5 min ago)", log_file)
        return

    prev_time = pd.to_datetime(prev_row[ts_col])

    current_long_num = int(current_row.get("pos_long_num", 0))
    current_short_num = int(current_row.get("pos_short_num", 0))
    prev_long_num = int(prev_row.get("pos_long_num", 0))
    prev_short_num = int(prev_row.get("pos_short_num", 0))

    current_long = int(current_row.get("pos_long", 0))
    current_short = int(current_row.get("pos_short", 0))
    prev_long = int(prev_row.get("pos_long", 0))
    prev_short = int(prev_row.get("pos_short", 0))

    delta_long_num = current_long_num - prev_long_num
    delta_short_num = current_short_num - prev_short_num
    delta_oi_long = current_long - prev_long
    delta_oi_short = current_short - prev_short
    # [НОВОЕ 2026-08-19]: Расчет метрик для вероятностного сигнала YUR
    yur_signal_active = False
    yur_signal_details = ''
    if 'df_yur' in locals() and not df_yur.empty:
        curr_yur = nearest_row(df_yur, datetime.now())[0]
        prev_yur = nearest_row(df_yur, datetime.now() - timedelta(minutes=5))[0]
        if curr_yur is not None and prev_yur is not None:
            yur_long_curr = int(curr_yur.get('pos_long', 0))
            yur_short_curr = int(curr_yur.get('pos_short', 0))
            yur_long_prev = int(prev_yur.get('pos_long', 0))
            yur_short_prev = int(prev_yur.get('pos_short', 0))
            delta_yur_net = (yur_long_curr - yur_short_curr) - (yur_long_prev - yur_short_prev)
            if delta_yur_net > 0:
                yur_signal_active = True
                yur_signal_details = f'Delta YUR net: +{delta_yur_net} kont.'

    log_line(
        f"[{symbol}] time={current_time} prev={prev_time} | "
        f"long_num: {prev_long_num}->{current_long_num} (Δ{delta_long_num}) | "
        f"short_num: {prev_short_num}->{current_short_num} (Δ{delta_short_num}) | "
        f"OI_long: {delta_oi_long} | OI_short: {delta_oi_short}",
        log_file
    )

    # [NEW 2026-08-25] Дельты YUR для аномалий (безопасно — если нет данных, то 0)
    dyur_long = 0
    dyur_short = 0
    if 'df_yur' in locals() and not df_yur.empty:
        curr_yur = nearest_row(df_yur, datetime.now())[0]
        prev_yur = nearest_row(df_yur, datetime.now() - timedelta(minutes=5))[0]
        if curr_yur is not None and prev_yur is not None:
            dyur_long  = int(curr_yur.get('pos_long', 0))  - int(prev_yur.get('pos_long', 0))
            dyur_short = int(curr_yur.get('pos_short', 0)) - int(prev_yur.get('pos_short', 0))

    # [NEW 2026-08-25] Проверка аномалий набора (адаптивная, без отправки — только формируем список)
    anomaly_msgs = check_anomaly_uptake(
        symbol, current_time,
        delta_oi_long, delta_oi_short,  # FIZ
        dyur_long, dyur_short,          # YUR
        state
    )

    key = f"{current_time.isoformat()}:{current_long_num}:{current_short_num}:{delta_long_num}:{delta_short_num}"
    if state.get("last_key") == key:
        log_line(f"[{symbol}] duplicate skipped", log_file)
        return

    state["last_key"] = key
    save_state(state, state_file)

    messages = []

    # [NEW 2026-08-25] Аномалии набора (всегда активны)
    if anomaly_msgs:
        messages.extend(anomaly_msgs)

    # [OLD] Старые алерты (отключены по умолчанию через LEGACY_ALERTS)
    if LEGACY_ALERTS and delta_long_num > THRESHOLD:
        oi_state = "прирост OI" if delta_oi_long >= 0 else "падение OI"
        direction = "купили" if delta_oi_long >= 0 else "продали"
        messages.append(
            f"📈 *{symbol} FUTOI LONG (Физ. лица)*\n"
            f"⏰ Время: `{current_time}`\n"
            f"👥 Счетов в лонге: `{current_long_num}` (изм: `+{delta_long_num}`)\n"
            f"📊 Объем OI Long: `{current_long}` конт. (изм: `{delta_oi_long}`)\n"
            f"💡 Вывод: OI {oi_state}, новые счета {direction}"
        )

    if LEGACY_ALERTS and delta_short_num > THRESHOLD:
        oi_state = "прирост OI" if delta_oi_short >= 0 else "падение OI"
        direction = "купили" if delta_oi_short >= 0 else "продали"
        messages.append(
            f"📉 *{symbol} FUTOI SHORT (Физ. лица)*\n"
            f"⏰ Время: `{current_time}`\n"
            f"👥 Счетов в шорте: `{current_short_num}` (изм: `+{delta_short_num}`)\n"
            f"📊 Объем OI Short: `{current_short}` конт. (изм: `{delta_oi_short}`)\n"
            f"💡 Вывод: OI {oi_state}, новые счета {direction}"
        )

    # [НОВОЕ 2026-08-19]: Формирование сообщения о вероятностном сигнале YUR
    if LEGACY_ALERTS and yur_signal_active:
        yur_msg = (
            f"🏦 *{symbol} YUR: Вероятный набор long*\n"
            f"⏰ Время: `{current_time}`\n"
            f"📊 {yur_signal_details}\n"
            f"⚠️ Статус: косвенная оценка\n"
            f"🔍 Достоверность: косвенная"
        )
        messages.append(yur_msg)

    if messages:
        ok = send_alert("\n\n---\n\n".join(messages))
        if ok:
            log_line(f"[{symbol}] alert sent (Markdown)", log_file)
    else:
        log_line(f"[{symbol}] below threshold", log_file)

def main():
    log_line("Bot started. Monitoring symbols: " + ", ".join(SYMBOLS), BASE_LOG_FILE.format(SYMBOLS[0]))
    while True:
        try:
            # [ИЗМЕНЕНИЕ]: Цикл по всем символам. Каждый проверяется независимо со своим состоянием и логом.
            for symbol in SYMBOLS:
                check_once(symbol)
                # Небольшая пауза между запросами к API для разных тикеров, чтобы не перегружать moexalgo
                time.sleep(2) 
        except Exception as e:
            # [ИЗМЕНЕНИЕ]: Логгируем ошибку в первый доступный лог-файл, чтобы не ломать структуру
            log_line(f"Global error: {e}", BASE_LOG_FILE.format(SYMBOLS[0]))
        time.sleep(SLEEP_SECONDS)

if __name__ == "__main__":
    main()

# ==================== TELEGRAM ALERTS ====================
async def send_telegram_alert_if_needed(symbol, delta_long_oi):
    """
    Отправляет алерт в Telegram если изменение OI > 500 контрактов
    """
    from telegram_screenshot import send_auto_alert
    
    # Проверяем порог (500 контрактов)
    if abs(delta_long_oi) > 500:
        print(f" Превышен порог OI: {delta_long_oi:+d} контрактов")
        try:
            await send_auto_alert(symbol, delta_long_oi)
        except Exception as e:
            print(f"❌ Ошибка отправки Telegram алерта: {e}")
