# notify_otc_email.py
# [NEW 2026-10-28] Дайджест OTC-аномалий на почту (раз в 180 минут)
import sqlite3
import smtplib
from email.mime.text import MIMEText
from datetime import datetime, timedelta
from pathlib import Path
import os
import pandas as pd
from dotenv import load_dotenv

load_dotenv(Path(__file__).parent / '.env')
DB_PATH = Path(__file__).parent / 'futoi.db'

# Настройки SMTP
SMTP_HOST = os.getenv('SMTP_HOST', 'smtp.yandex.ru')
SMTP_PORT = int(os.getenv('SMTP_PORT', 465))
SMTP_USER = os.getenv('SMTP_USER')
SMTP_PASS = os.getenv('SMTP_PASSWORD')
EMAIL_TO = os.getenv('EMAIL_TO', SMTP_USER)
INTERVAL_MIN = 180

def ensure_state_table(conn):
    conn.execute("""
        CREATE TABLE IF NOT EXISTS otc_email_state (
            id INTEGER PRIMARY KEY CHECK (id = 1),
            last_sent_at DATETIME
        )
    """)
    conn.commit()

def send_email(subject, body):
    if not SMTP_USER or not SMTP_PASS:
        print("⚠️ SMTP credentials not set in .env. Email not sent.")
        return False
        
    msg = MIMEText(body, 'plain', 'utf-8')
    msg['Subject'] = subject
    msg['From'] = SMTP_USER
    msg['To'] = EMAIL_TO

    try:
        with smtplib.SMTP_SSL(SMTP_HOST, SMTP_PORT) as server:
            server.login(SMTP_USER, SMTP_PASS)
            server.send_message(msg)
        print(f"✅ Письмо отправлено на {EMAIL_TO}")
        return True
    except Exception as e:
        print(f"❌ Ошибка отправки письма: {e}")
        return False

def main():
    conn = sqlite3.connect(DB_PATH)
    ensure_state_table(conn)
    
    # 1. Проверяем, когда отправляли в последний раз
    cursor = conn.cursor()
    cursor.execute("SELECT last_sent_at FROM otc_email_state WHERE id = 1")
    row = cursor.fetchone()
    
    now = datetime.now()
    if row and row[0]:
        last_sent = datetime.strptime(row[0], '%Y-%m-%d %H:%M:%S')
        minutes_passed = (now - last_sent).total_seconds() / 60
        if minutes_passed < INTERVAL_MIN:
            print(f"⏳ С момента последней отправки прошло {minutes_passed:.0f} мин. (нужно {INTERVAL_MIN}). Пропуск.")
            conn.close()
            return
    else:
        # Если таблица пуста, берем последние 180 минут
        last_sent = now - timedelta(minutes=INTERVAL_MIN)

    last_sent_str = last_sent.strftime('%Y-%m-%d %H:%M:%S')

    # 2.1 Ищем события MOEX
    q_moex = """
        SELECT symbol, bar_ts as ts, delta_oi, volume, delta_oi_ratio, oi_direction as direction, otc_strict
        FROM otc_suspect_events
        WHERE (otc_suspect = 1 OR otc_strict = 1) AND bar_ts > ?
        ORDER BY bar_ts DESC
    """
    df_moex = pd.read_sql_query(q_moex, conn, params=(last_sent_str,))
    
    # 2.2 Ищем события Bybit (только is_outlier = 1)
    q_bybit = """
        SELECT symbol, timestamp as ts, delta_oi, volume, z_score
        FROM bybit_otc_events
        WHERE is_outlier = 1 AND timestamp > ?
        ORDER BY timestamp DESC
    """
    df_bybit = pd.read_sql_query(q_bybit, conn, params=(last_sent_str,))

    if df_moex.empty and df_bybit.empty:
        print(f"📭 Нет новых OTC-событий после {last_sent_str}. Письмо не отправляем.")
        conn.close()
        return

    # 3. Формируем текст письма
    total_events = len(df_moex) + len(df_bybit)
    subject = f"OTC Аномалии: {total_events} событий (с {last_sent.strftime('%d.%m %H:%M')})"
    
    body = f"Найдены аномалии открытого интереса за период с {last_sent.strftime('%d.%m %H:%M')} по {now.strftime('%d.%m %H:%M')}:\n\n"
    
    if not df_moex.empty:
        body += "=== MOEX (Futures) ===\n"
        for _, r in df_moex.iterrows():
            strict_mark = "🔥 СТРОГАЯ (k=4)" if r['otc_strict'] == 1 else "⚠️ Базовая (k=2)"
            body += f"📊 {r['symbol']} | {r['ts']} | {strict_mark}\n"
            body += f"   ΔOI: {int(r['delta_oi']):+,d} | Объём: {int(r['volume']):,d} | Ratio: {r['delta_oi_ratio']:.2f}\n"
            body += f"   Направление: {'Открытие' if r['direction']=='open' else 'Закрытие'}\n\n"
            
    if not df_bybit.empty:
        body += "=== Bybit (ETHUSDT Perp) ===\n"
        for _, r in df_bybit.iterrows():
            body += f"📊 {r['symbol']} | {r['ts']} (4H)\n"
            body += f"   ΔOI: {int(r['delta_oi']):+,d} ETH | Объём: {int(r['volume']):,d} ETH | Z-score: {r['z_score']:.2f}\n\n"
            
    body += "\n---\nАвтоматическое сообщение от MOEX Dashboard Bot"

    # 4. Отправка
    if send_email(subject, body):
        cursor.execute("""
            INSERT INTO otc_email_state (id, last_sent_at) VALUES (1, ?)
            ON CONFLICT(id) DO UPDATE SET last_sent_at = excluded.last_sent_at
        """, (now.strftime('%Y-%m-%d %H:%M:%S'),))
        conn.commit()
    
    conn.close()

if __name__ == '__main__':
    main()