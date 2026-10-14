#!/usr/bin/env python3
"""
Управление контрактами: добавление/удаление символа во всех файлах проекта
и (опционально) очистка БД.

Используется дашбордом (страница "Управление контрактами"), но может
запускаться вручную для тестирования.

Улучшения безопасности:
- Backup каждого файла перед изменением (.bak_YYYYMMDD_HHMMSS)
- Проверка синтаксиса (compile) с автооткатом при ошибке
- Dry-run режим
- Защита от удаления последнего символа
- Retry для SQLite (database is locked)
- SSL-patch для moexalgo
"""
import os
import re
import json
import time
import shutil
import sqlite3
import subprocess
import sys
from datetime import datetime, date, timedelta
from pathlib import Path

# ==== Пути ====
BASE_DIR = Path(__file__).parent
DB_PATH = BASE_DIR / 'futoi.db'
HISTORY_FILE = BASE_DIR / 'symbols_history.json'
VENV_PYTHON = BASE_DIR / 'venv' / 'bin' / 'python'

# ==== Файлы, в которых нужно править список символов ====
FILES_TO_MANAGE = [
    ('moex_futoi_alert_multi.py', 'collector'),
    ('compute_passive_yur.py', 'compute'),
    ('compute_yur_absorption.py', 'compute'),
    ('compute_trap.py', 'compute'),
    ('compute_enrich.py', 'compute'),
    ('compute_signals.py', 'compute'),
    ('compute_squeeze.py', 'compute'),
    ('app.py', 'dashboard'),
    ('pages/1_📊_Таблица_FIZ_YUR.py', 'dashboard'),
    ('pages/2_💼_OTC_объём.py', 'dashboard'),
    ('backfill_bar_stats.py', 'backfill'),
    ('backfill_tradestats.py', 'backfill'),
    ('backfill_futoi.py', 'backfill'),
]

# ==== Таблицы, из которых удаляются данные символа (при clean_db=True) ====
TABLES_TO_CLEAN = [
    'futoi_data', 'bar_stats', 'tradestats_data', 'candles_cache',
    'squeeze_events', 'trap_events', 'yur_absorption', 'divergence_events',
    'whale_events', 'breakout_combo', 'otc_trades', 'legal_passive_estimate',
]


# ============ Утилиты ============

def _backup(filepath):
    """Создаёт резервную копию файла. Возвращает путь к бэкапу."""
    ts = datetime.now().strftime('%Y%m%d_%H%M%S')
    backup = f"{filepath}.bak_{ts}"
    shutil.copy2(filepath, backup)
    return backup


def _check_syntax(filepath):
    """Проверяет синтаксис Python-файла. Возвращает True/False."""
    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            source = f.read()
        compile(source, filepath, 'exec')
        return True
    except SyntaxError:
        return False


def _db_execute_with_retry(conn, sql, params=(), max_attempts=5, delay=1.0):
    """Выполняет SQL (execute) с retry на database is locked."""
    for attempt in range(max_attempts):
        try:
            cur = conn.execute(sql, params)
            return cur
        except sqlite3.OperationalError as e:
            if 'locked' in str(e).lower() and attempt < max_attempts - 1:
                time.sleep(delay)
                continue
            raise
    raise RuntimeError("Не удалось выполнить SQL после нескольких попыток")


def _db_executemany_with_retry(conn, sql, rows, max_attempts=5, delay=1.0):
    """Выполняет SQL (executemany) с retry на database is locked."""
    for attempt in range(max_attempts):
        try:
            conn.executemany(sql, rows)
            return
        except sqlite3.OperationalError as e:
            if 'locked' in str(e).lower() and attempt < max_attempts - 1:
                time.sleep(delay)
                continue
            raise


# ============ Логирование операций ============

def log_operation(action, symbol, details):
    """Записывает операцию в symbols_history.json."""
    history = []
    if HISTORY_FILE.exists():
        try:
            with open(HISTORY_FILE, 'r', encoding='utf-8') as f:
                history = json.load(f)
        except Exception:
            history = []
    history.append({
        'timestamp': datetime.now().isoformat(),
        'action': action,
        'symbol': symbol,
        'details': details,
    })
    with open(HISTORY_FILE, 'w', encoding='utf-8') as f:
        json.dump(history, f, ensure_ascii=False, indent=2)


def get_history(limit=50):
    """Возвращает последние N операций из истории."""
    if not HISTORY_FILE.exists():
        return []
    try:
        with open(HISTORY_FILE, 'r', encoding='utf-8') as f:
            history = json.load(f)
        return history[-limit:]
    except Exception:
        return []


# ============ Чтение / изменение списков символов ============

def _extract_symbols(content):
    """Извлекает список символов из первой найденной конструкции SYMBOLS=[...] или selectbox."""
    m = re.search(r'SYMBOLS\s*=\s*\[([^\]]*)\]', content)
    if m:
        return re.findall(r"['\"]([^'\"]+)['\"]", m.group(1))
    m = re.search(r'selectbox\(["\']Инструмент[^"\']*["\']\s*,\s*\[([^\]]*)\]', content)
    if m:
        return re.findall(r"['\"]([^'\"]+)['\"]", m.group(1))
    return None


def read_symbols_from_file(filepath):
    """Возвращает список символов, найденных в файле, или None."""
    if not os.path.exists(filepath):
        return None
    with open(filepath, 'r', encoding='utf-8') as f:
        content = f.read()
    return _extract_symbols(content)


def list_symbols():
    """Активные символы — читаем из коллектора (moex_futoi_alert_multi.py)."""
    filepath = BASE_DIR / 'moex_futoi_alert_multi.py'
    return read_symbols_from_file(str(filepath)) or []


def add_symbol_to_file(filepath, symbol, dry_run=False):
    """Добавляет символ в файл. Возвращает (success, message)."""
    if not os.path.exists(filepath):
        return False, "Файл не найден"

    with open(filepath, 'r', encoding='utf-8') as f:
        content = f.read()

    if f"'{symbol}'" in content or f'"{symbol}"' in content:
        return True, "Уже присутствует"

    patterns = [
        (r'(SYMBOLS\s*=\s*\[)([^\]]*)(\])',),
        (r'(selectbox\(["\']Инструмент[^"\']*["\']\s*,\s*\[)([^\]]*)(\])',),
    ]

    for (pattern,) in patterns:
        match = re.search(pattern, content)
        if not match:
            continue

        current_list = match.group(2)
        if not current_list.strip():
            return False, "Пустой список, нельзя определить кавычки"

        quote = '"' if '"' in current_list else "'"
        cleaned = current_list.rstrip()
        if cleaned.endswith(','):
            cleaned = cleaned.rstrip(',')

        new_list = f"{cleaned}, {quote}{symbol}{quote}"
        new_content = content[:match.start(2)] + new_list + content[match.end(2):]

        if dry_run:
            return True, f"[dry-run] добавит {quote}{symbol}{quote}"

        backup = _backup(filepath)
        with open(filepath, 'w', encoding='utf-8') as f:
            f.write(new_content)

        if not _check_syntax(filepath):
            shutil.copy2(backup, filepath)
            return False, "Ошибка синтаксиса после замены, откат выполнен"

        return True, "Добавлен"

    return False, "Паттерн не найден"


def remove_symbol_from_file(filepath, symbol, dry_run=False):
    """Удаляет символ из файла. Возвращает (success, message)."""
    if not os.path.exists(filepath):
        return False, "Файл не найден"

    with open(filepath, 'r', encoding='utf-8') as f:
        content = f.read()

    if f"'{symbol}'" not in content and f'"{symbol}"' not in content:
        return True, "Отсутствует"

    original = content
    esc = re.escape(symbol)

    patterns = [
        rf',\s*["\']{esc}["\']',
        rf'["\']{esc}["\']\s*,?\s*',
    ]

    replaced = False
    for pattern in patterns:
        new_content = re.sub(pattern, '', content, count=1)
        if new_content != content:
            content = new_content
            replaced = True
            break

    content = re.sub(r'\[\s*,', '[', content)
    content = re.sub(r',\s*\]', ']', content)

    if not replaced or content == original:
        return False, "Не удалось удалить (паттерн не сработал)"

    if dry_run:
        return True, f"[dry-run] удалит {symbol}"

    backup = _backup(filepath)
    with open(filepath, 'w', encoding='utf-8') as f:
        f.write(content)

    if not _check_syntax(filepath):
        shutil.copy2(backup, filepath)
        return False, "Ошибка синтаксиса после удаления, откат выполнен"

    return True, "Удалён"


# ============ Backfill для futoi (API moexalgo) ============

def backfill_futoi_for_symbol(symbol, days):
    """Загружает исторические данные futoi для конкретного символа через moexalgo."""
    import ssl
    _dc = ssl.create_default_context
    def _nv(*a, **k):
        c = _dc(*a, **k)
        c.check_hostname = False
        c.verify_mode = ssl.CERT_NONE
        return c
    ssl.create_default_context = _nv

    import pandas as pd
    from moexalgo import Ticker, session
    from dotenv import load_dotenv

    load_dotenv(BASE_DIR / '.env')
    session.TOKEN = os.getenv('MOEXALGOPACK_TOKEN')

    end_d = date.today()
    start_d = end_d - timedelta(days=days)

    conn = sqlite3.connect(DB_PATH)
    try:
        schema_cols = [r[1] for r in conn.execute("PRAGMA table_info(futoi_data)").fetchall()]
        insert_cols = [c for c in schema_cols if c != 'id']

        existing = set(conn.execute(
            "SELECT clgroup, systime FROM futoi_data WHERE symbol=?", (symbol,)
        ).fetchall())

        total_new = 0
        d = start_d
        while d <= end_d:
            ds = str(d)
            try:
                df = Ticker(symbol).futoi(start=ds, end=ds)
            except Exception:
                d += timedelta(days=1)
                continue
            if df is None or df.empty:
                d += timedelta(days=1)
                continue

            df['systime_s'] = pd.to_datetime(df['systime']).dt.strftime('%Y-%m-%d %H:%M:%S')
            rows = []
            for _, r in df.iterrows():
                key = (r['clgroup'], r['systime_s'])
                if key in existing:
                    continue
                existing.add(key)
                vals = []
                for c in insert_cols:
                    if c == 'symbol':
                        vals.append(symbol)
                    elif c in df.columns:
                        v = r[c]
                        vals.append(None if pd.isna(v) else v)
                    else:
                        vals.append(None)
                rows.append(tuple(vals))

            if rows:
                ph = ', '.join(['?'] * len(insert_cols))
                sql = f"INSERT INTO futoi_data ({', '.join(insert_cols)}) VALUES ({ph})"
                _db_executemany_with_retry(conn, sql, rows)
                conn.commit()
                total_new += len(rows)

            d += timedelta(days=1)

        return {'success': True, 'message': f'Загружено {total_new} строк', 'rows': total_new}
    except Exception as e:
        return {'success': False, 'message': str(e), 'rows': 0}
    finally:
        conn.close()


# ============ Основные операции ============

def add_symbol(symbol, backfill_days=5, dry_run=False, run_backfill=True):
    """Добавляет символ во все файлы и (опционально) запускает backfill."""
    results = {
        'symbol': symbol,
        'dry_run': dry_run,
        'files': [],
        'backfill': [],
        'errors': [],
    }

    if symbol in list_symbols():
        results['errors'].append(f"Символ {symbol} уже в списке активных")
        return results

    for filename, _kind in FILES_TO_MANAGE:
        filepath = BASE_DIR / filename
        success, msg = add_symbol_to_file(str(filepath), symbol, dry_run=dry_run)
        results['files'].append({'file': filename, 'success': success, 'message': msg})
        if not success:
            results['errors'].append(f"{filename}: {msg}")

    if dry_run or not run_backfill or results['errors']:
        log_operation('ADD_DRYRUN' if dry_run else 'ADD_FILES_ONLY', symbol, {
            'backfill_days': backfill_days,
            'files_changed': sum(1 for f in results['files'] if f['success']),
            'errors': results['errors'],
        })
        return results

    end_d = date.today()
    start_d = end_d - timedelta(days=backfill_days)

    try:
        r = subprocess.run(
            [str(VENV_PYTHON), str(BASE_DIR / 'backfill_bar_stats.py'),
             start_d.isoformat(), end_d.isoformat()],
            capture_output=True, text=True, timeout=600, cwd=str(BASE_DIR)
        )
        results['backfill'].append({
            'script': 'backfill_bar_stats.py',
            'success': r.returncode == 0,
            'output': (r.stdout or '')[-500:],
        })
        if r.returncode != 0:
            results['errors'].append(f"backfill_bar_stats: {(r.stderr or '')[-200:]}")
    except Exception as e:
        results['errors'].append(f"backfill_bar_stats: {e}")

    try:
        r = subprocess.run(
            [str(VENV_PYTHON), str(BASE_DIR / 'backfill_tradestats.py'),
             '--days', str(backfill_days)],
            capture_output=True, text=True, timeout=600, cwd=str(BASE_DIR)
        )
        results['backfill'].append({
            'script': 'backfill_tradestats.py',
            'success': r.returncode == 0,
            'output': (r.stdout or '')[-500:],
        })
        if r.returncode != 0:
            results['errors'].append(f"backfill_tradestats: {(r.stderr or '')[-200:]}")
    except Exception as e:
        results['errors'].append(f"backfill_tradestats: {e}")

    futoi_result = backfill_futoi_for_symbol(symbol, backfill_days)
    results['backfill'].append({
        'script': f'backfill_futoi({symbol})',
        'success': futoi_result['success'],
        'output': futoi_result['message'],
    })
    if not futoi_result['success']:
        results['errors'].append(f"backfill_futoi: {futoi_result['message']}")

    log_operation('ADD', symbol, {
        'backfill_days': backfill_days,
        'files_changed': sum(1 for f in results['files'] if f['success']),
        'errors': results['errors'],
    })
    return results


def remove_symbol(symbol, clean_db=False, dry_run=False):
    """Удаляет символ из файлов и (опционально) из БД."""
    results = {
        'symbol': symbol,
        'dry_run': dry_run,
        'clean_db': clean_db,
        'files': [],
        'db_clean': [],
        'errors': [],
    }

    current = list_symbols()
    if symbol not in current:
        results['errors'].append(f"Символ {symbol} не найден в списке активных")
        return results
    if len(current) <= 1:
        results['errors'].append("Нельзя удалить последний активный символ")
        return results

    for filename, _kind in FILES_TO_MANAGE:
        filepath = BASE_DIR / filename
        success, msg = remove_symbol_from_file(str(filepath), symbol, dry_run=dry_run)
        results['files'].append({'file': filename, 'success': success, 'message': msg})
        if not success:
            results['errors'].append(f"{filename}: {msg}")

    if clean_db and not dry_run:
        conn = sqlite3.connect(DB_PATH)
        try:
            for table in TABLES_TO_CLEAN:
                try:
                    cur = _db_execute_with_retry(
                        conn, f"DELETE FROM {table} WHERE symbol=?", (symbol,)
                    )
                    deleted = cur.rowcount
                    results['db_clean'].append({'table': table, 'deleted': deleted})
                except Exception as e:
                    results['errors'].append(f"Таблица {table}: {e}")
            conn.commit()
        finally:
            conn.close()
    elif clean_db and dry_run:
        results['db_clean'].append({'table': '(dry-run)', 'deleted': 0})

    if not dry_run:
        state_file = BASE_DIR / f'futoi_state_{symbol}.json'
        if state_file.exists():
            state_file.unlink()
            results['files'].append({
                'file': str(state_file.name), 'success': True, 'message': 'State-файл удалён'
            })

    log_operation('REMOVE_DRYRUN' if dry_run else 'REMOVE', symbol, {
        'clean_db': clean_db,
        'files_changed': sum(1 for f in results['files'] if f['success']),
        'db_records_deleted': sum(t['deleted'] for t in results['db_clean']),
        'errors': results['errors'],
    })
    return results


# ============ CLI ============

if __name__ == '__main__':
    if len(sys.argv) < 2:
        print("Использование:")
        print("  python manage_symbols.py list")
        print("  python manage_symbols.py add <symbol> [days] [--dry-run]")
        print("  python manage_symbols.py remove <symbol> [--clean-db] [--dry-run]")
        print("  python manage_symbols.py history [limit]")
        sys.exit(1)

    action = sys.argv[1]

    if action == 'list':
        symbols = list_symbols()
        print(json.dumps({"symbols": symbols}, ensure_ascii=False))

    elif action == 'add' and len(sys.argv) >= 3:
        symbol = sys.argv[2]
        days = 5
        dry = '--dry-run' in sys.argv
        args = [a for a in sys.argv[3:] if not a.startswith('--')]
        if args:
            try:
                days = int(args[0])
            except ValueError:
                print(f"❌ Некорректное число дней: {args[0]}")
                sys.exit(1)
        result = add_symbol(symbol, backfill_days=days, dry_run=dry)
        print(json.dumps(result, ensure_ascii=False, indent=2))

    elif action == 'remove' and len(sys.argv) >= 3:
        symbol = sys.argv[2]
        clean_db = '--clean-db' in sys.argv
        dry = '--dry-run' in sys.argv
        result = remove_symbol(symbol, clean_db=clean_db, dry_run=dry)
        print(json.dumps(result, ensure_ascii=False, indent=2))

    elif action == 'history':
        limit = int(sys.argv[2]) if len(sys.argv) > 2 else 20
        hist = get_history(limit)
        print(json.dumps(hist, ensure_ascii=False, indent=2))

    else:
        print("Неверные аргументы")
        sys.exit(1)
