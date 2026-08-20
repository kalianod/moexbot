import sqlite3
import os

DB_PATH = os.path.join(os.path.dirname(__file__), 'futoi.db')

def init_database():
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS futoi_data (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            symbol TEXT NOT NULL,
            ticker TEXT,
            systime TIMESTAMP NOT NULL,
            tradedate DATE,
            tradetime TIME,
            clgroup TEXT NOT NULL,
            pos_long INTEGER,
            pos_short INTEGER,
            pos_long_num INTEGER,
            pos_short_num INTEGER,
            pos INTEGER,
            sess_id INTEGER,
            seqnum INTEGER,
            UNIQUE(symbol, systime, clgroup)
        )
    ''')
    
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_symbol_time ON futoi_data(symbol, systime)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_symbol_date ON futoi_data(symbol, tradedate)')
    
    conn.commit()
    conn.close()
    print(f"База данных пересоздана: {DB_PATH}")

if __name__ == "__main__":
    init_database()
