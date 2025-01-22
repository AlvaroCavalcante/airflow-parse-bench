import sys
import os
import logging
import argparse
import importlib
import importlib.machinery
import importlib.util

from airflow.utils import timezone
from airflow.models.dag import DAG
from airflow.utils.file import get_unique_dag_module_name


def add_dag_directory_to_sys_path(filepath: str):
    dag_directory = os.path.dirname(filepath)
    if dag_directory not in sys.path:
        sys.path.append(dag_directory)


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
        logging.error(
            f"Failed to parse {os.path.basename(filepath)}, error: {e}")
        exit_subprocess_execution()
        return []


def exit_subprocess_execution():
    if __name__ == "__main__":
        sys.exit(1)


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
    try:
        add_dag_directory_to_sys_path(filepath)
        file_parse_start_dttm = timezone.utcnow()

        if filepath is None or not os.path.isfile(filepath):
            logging.error(f"Error: incorrect or invalid file path: {filepath}")
            exit_subprocess_execution()
            return 0

        mods = parse(filepath)

        if not mods:
            return 0

        found_dags = process_modules(mods)

        if not found_dags:
            logging.error(
                f"No valid DAGs found in {os.path.basename(filepath)}")
            exit_subprocess_execution()
            return 0

        file_parse_end_dttm = timezone.utcnow()
        return round((file_parse_end_dttm - file_parse_start_dttm).total_seconds(), 3)
    except Exception as error:
        logging.error(
            f"Failed to process {os.path.basename(filepath)}, error: {error}")
        exit_subprocess_execution()
        return 0


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Measures the parsing time of an Airflow DAG.")
    parser.add_argument("--filepath", dest="filepath", type=str, required=True,
                        help="Path to the Python file containing the DAG.")
    args = parser.parse_args()

    parse_time = process_dag_file(args.filepath)
    print(parse_time)
