from datetime import datetime
import os
import sqlite3
import logging


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


def reset_database():
    if os.path.exists(DATABASE):
        os.remove(DATABASE)
    initialize_database()
    logging.info('Database reset successfully.')


def check_previous_execution(filepath: str, file_content: str):
    conn = sqlite3.connect(DATABASE)
    cursor = conn.cursor()
    cursor.execute('''
        SELECT file_content, parse_time FROM benchmark_results
        WHERE filename = ?
        ORDER BY execution_date DESC
    ''', (filepath,))
    row = cursor.fetchall()
    conn.close()
    if row:
        best_parse_time = min([r[1] for r in row])
        previous_content = row[0][0]
        return True, bool(previous_content == file_content), row[0][1], best_parse_time
    return False, False, 0, 999999


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
