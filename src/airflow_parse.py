import argparse
import logging
import os
import sys
import subprocess

from tqdm import tqdm
from colorama import Fore, Style, init
from tabulate import tabulate

import bench_db_utils
import dag_parse


def get_file_content(filepath: str):
    try:
        with open(filepath, 'r') as file:
            return file.read()
    except Exception as error:
        logging.error(f"Failed to read the file content: {error}")
        return None


def compare_results(current_parse_time_dict: dict, previous_parse_time_dict: dict, best_parse_time_dict: dict, order: str):
    table_data = []
    for filename, current_parse_time in current_parse_time_dict.items():
        previous_parse_time = previous_parse_time_dict.get(filename, 0)
        best_parse_time = best_parse_time_dict.get(filename, 0)
        filename = os.path.basename(filename)

        difference_str = "0"
        if previous_parse_time:
            difference = round(current_parse_time - previous_parse_time, 4)
            if difference != 0:
                sign = "+" if difference > 0 else "-"
                color = Fore.RED if difference > 0 else Fore.GREEN
                difference_str = f'{color}{sign}{abs(difference)} seconds{Style.RESET_ALL}'
        table_data.append([filename, current_parse_time,
                          previous_parse_time, difference_str, best_parse_time])

    reverse_order = True if order == 'desc' else False
    table_data = sorted(
        table_data, key=lambda data: data[1], reverse=reverse_order)
    headers = ["Filename", "Current Parse Time",
               "Previous Parse Time", "Difference", "Best Parse Time"]
    table = tabulate(table_data, headers, tablefmt="grid")
    print(table)


def get_python_modules(args):
    if args.path.endswith(".py"):
        python_files = [args.path]
    else:
        folder_files = os.listdir(args.path)
        folder_files = [os.path.join(args.path, file) for file in folder_files]

        python_files = list(
            filter(lambda file: file.endswith(".py"), folder_files))

        logging.info(
            f"{len(python_files)} Python files identified on provided path.")

    return python_files


def run_dag_parse(filepath: str, num_iterations: int):
    if num_iterations > 1:
        parse_time = get_average_parse_time(filepath, num_iterations)
    else:
        parse_time = dag_parse.process_dag_file(filepath)

    return parse_time


def get_average_parse_time(filepath: str, num_iterations: int):
    parse_times = []
    python_command = sys.executable

    for _ in range(num_iterations):
        python_result = subprocess.run(
            [python_command, 'src/dag_parse.py', '--filepath', filepath], capture_output=True, text=True)

        if python_result.returncode != 0:
            logging.error(python_result.stdout)
            break

        parse_time = float(python_result.stdout.strip().split()[-1])
        parse_times.append(parse_time)

    parse_time = round(sum(parse_times) / len(parse_times),
                       3) if parse_times else 0
    return parse_time


def main():
    init(autoreset=True)

    args = define_arguments()

    if args.reset_db:
        bench_db_utils.reset_database()
    else:
        bench_db_utils.initialize_database()

    current_parse_time_dict = {}
    previous_parse_time_dict = {}
    best_parse_time_dict = {}

    python_files = get_python_modules(args)

    for filepath in tqdm(python_files, colour="green"):
        file_content = get_file_content(filepath)
        if not file_content:
            continue

        is_previously_parsed, is_same_file_content, previous_parse_time, best_parse_time = bench_db_utils.check_previous_execution(
            filepath, file_content)

        if args.skip_unchanged and is_same_file_content:
            current_parse_time_dict[filepath] = previous_parse_time
            previous_parse_time_dict[filepath] = previous_parse_time
            best_parse_time_dict[filepath] = best_parse_time
            continue
        elif is_previously_parsed:
            previous_parse_time_dict[filepath] = previous_parse_time

        parse_time = run_dag_parse(filepath, args.num_iterations)

        if not parse_time:
            continue

        current_parse_time_dict[filepath] = parse_time
        best_parse_time = min(parse_time, best_parse_time)
        best_parse_time_dict[filepath] = best_parse_time

        bench_db_utils.save_benchmark_result(
            filepath, parse_time, file_content)

    if current_parse_time_dict:
        compare_results(current_parse_time_dict,
                        previous_parse_time_dict, best_parse_time_dict, args.order)
    else:
        logging.warning("No valid DAGs were found, finishing process.")


def define_arguments():
    parser = argparse.ArgumentParser(
        description="Measures the parsing time of an Airflow DAG.")
    parser.add_argument("--path", dest="path", type=str, required=True,
                        help="Path to the Python file containing the DAG or to the folder with the DAGs.")
    parser.add_argument("--order", dest="order", type=str, choices=['asc', 'desc'], default='asc',
                        help="Order to display the results: 'asc' for ascending, 'desc' for descending.")
    parser.add_argument("--reset-db", dest="reset_db", action="store_true",
                        help="Reset the database before running the benchmarking.")
    parser.add_argument("--skip-unchanged", dest="skip_unchanged", action="store_true",
                        help="Skip parsing files that have not changed.")
    parser.add_argument("--num-iterations", dest="num_iterations", type=int, default=1,
                        help="Number of times to execute each DAG parse. The parse time is the average of all iterations.")
    args = parser.parse_args()
    return args


if __name__ == "__main__":
    main()
