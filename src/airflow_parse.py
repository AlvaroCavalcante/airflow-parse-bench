import importlib
import importlib.machinery
import importlib.util
import argparse
import sys
import logging
import os
import sqlite3
from datetime import datetime
from colorama import Fore, Style, init
from tabulate import tabulate

from airflow.models.dag import DAG
from airflow.utils import timezone
from airflow.utils.file import get_unique_dag_module_name

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


def get_file_content(filepath: str):
    with open(filepath, 'r') as file:
        return file.read()


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


def save_benchmark_result(filepath: str, parse_time: float):
    conn = sqlite3.connect(DATABASE)
    cursor = conn.cursor()
    file_content = get_file_content(filepath)
    execution_date = datetime.now().isoformat()
    cursor.execute('''
        INSERT INTO benchmark_results (filename, parse_time, execution_date, file_content)
        VALUES (?, ?, ?, ?)
    ''', (filepath, parse_time, execution_date, file_content))
    conn.commit()
    conn.close()


def parse(filepath: str):
    """
    Simplified version of the Airflow parse method.
    It loads the Python file as a module into memory.
    """
    try:
        mod_name = get_unique_dag_module_name(filepath)

        if mod_name in sys.modules:
            del sys.modules[mod_name]

        loader = importlib.machinery.SourceFileLoader(mod_name, filepath)
        spec = importlib.util.spec_from_loader(mod_name, loader)
        new_module = importlib.util.module_from_spec(spec)
        sys.modules[spec.name] = new_module
        loader.exec_module(new_module)
        return [new_module]
    except Exception as e:
        logging.error(f"Failed to parse {filepath}, error: {e}")
        return []


def process_modules(mods: list):
    """
    Simplified version of the Airflow process_modules method.
    It identifies the module DAGs and validates if it's a valid DAG instance.
    """
    top_level_dags = {
        (o, m) for m in mods for o in m.__dict__.values() if isinstance(o, DAG)}

    found_dags = []

    for dag, mod in top_level_dags:
        dag.fileloc = mod.__file__
        try:
            dag.validate()
        except Exception as error:
            logging.error(f"Error to validate DAG: {error}")
        else:
            found_dags.append(dag)

    return found_dags


def process_dag_file(filepath: str):
    file_parse_start_dttm = timezone.utcnow()

    if filepath is None or not os.path.isfile(filepath):
        logging.error(f"Error: incorrect or invalid file path: {filepath}")
        return

    mods = parse(filepath)
    process_modules(mods)

    file_parse_end_dttm = timezone.utcnow()
    return (file_parse_end_dttm - file_parse_start_dttm).total_seconds()


def compare_results(current_parse_time_dict: dict, previous_parse_time_dict: dict):
    table_data = []
    for filename, current_parse_time in current_parse_time_dict.items():
        filename = filename.split("/")[-1]
        previous_parse_time = previous_parse_time_dict.get(filename, 0)
        difference = current_parse_time - previous_parse_time
        sign = "+" if difference > 0 else "-"
        color = Fore.RED if difference > 0 else Fore.GREEN
        difference_str = f'{color}{sign}{abs(difference)} seconds{Style.RESET_ALL}'
        table_data.append([filename, current_parse_time,
                          previous_parse_time, difference_str])

    headers = ["Filename", "Current Parse Time",
               "Previous Parse Time", "Difference"]
    table = tabulate(table_data, headers, tablefmt="grid")
    print(table)


if __name__ == "__main__":
    initialize_database()

    parser = argparse.ArgumentParser(
        description="Measures the parsing time of an Airflow DAG.")
    parser.add_argument("--path", dest="path", type=str, required=True,
                        help="Path to the Python file containing the DAG or to the folder with the DAGs.")
    args = parser.parse_args()
    current_parse_time_dict = {}
    previous_parse_time_dict = {}

    if args.path.endswith(".py"):
        python_files = [args.path]
    else:
        folder_files = os.listdir(args.path)
        python_files = list(
            filter(lambda file: file.endswith(".py"), folder_files))

        logging.info(
            f"{len(python_files)} Python files identified on provided path.")

    for filepath in python_files:
        file_content = get_file_content(filepath)
        is_previously_parsed, is_same_file_content, previous_parse_time = check_previous_execution(
            filepath, file_content)

        if is_same_file_content:
            current_parse_time_dict[filepath] = previous_parse_time
            previous_parse_time_dict[filepath] = previous_parse_time
            continue
        elif is_previously_parsed:
            previous_parse_time_dict[filepath] = previous_parse_time

        parse_time = process_dag_file(filepath)
        current_parse_time_dict[filepath] = parse_time

        save_benchmark_result(filepath, parse_time)

    compare_results(current_parse_time_dict, previous_parse_time_dict)
