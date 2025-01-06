from datetime import datetime
import sqlite3


DATABASE = 'benchmark_results.db'


def initialize_database():
    conn = sqlite3.connect(DATABASE)
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS benchmark_results (
            id INTEGER PRIMARY KEY,
            filename TEXT NOT NULL,
            parse_time REAL NOT NULL,
            execution_date TEXT NOT NULL,
            file_content TEXT NOT NULL
        )
    ''')
    conn.commit()
    conn.close()


def check_previous_execution(filepath: str, file_content: str):
    conn = sqlite3.connect(DATABASE)
    cursor = conn.cursor()
    cursor.execute('''
        SELECT file_content, parse_time FROM benchmark_results
        WHERE filename = ?
        ORDER BY execution_date DESC
        LIMIT 1
    ''', (filepath,))
    row = cursor.fetchone()
    conn.close()
    if row:
        previous_content = row[0]
        return True, bool(previous_content == file_content), row[1]
    return False, False, 0


def save_benchmark_result(filepath: str, parse_time: float, file_content: str):
    conn = sqlite3.connect(DATABASE)
    cursor = conn.cursor()

    execution_date = datetime.now().isoformat()
    cursor.execute('''
        INSERT INTO benchmark_results (filename, parse_time, execution_date, file_content)
        VALUES (?, ?, ?, ?)
    ''', (filepath, parse_time, execution_date, file_content))
    conn.commit()
    conn.close()
