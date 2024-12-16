import importlib
import importlib.machinery
import importlib.util
import argparse
import sys
import logging
import os

from airflow.models.dag import DAG
from airflow.utils import timezone
from airflow.utils.file import get_unique_dag_module_name


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

    logging.info(
        f'Time took to parse {file_parse_end_dttm - file_parse_start_dttm}')


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Measures the parsing time of an Airflow DAG.")
    parser.add_argument("--path", dest="path", type=str, required=True,
                        help="Path to the Python file containing the DAG or to the folder with the DAGs.")
    args = parser.parse_args()

    if args.path.endswith(".py"):
        process_dag_file(args.path)
    else:
        folder_files = os.listdir(args.path)
        python_files = list(
            filter(lambda file: file.endswith(".py"), folder_files))

        logging.info(
            f"{len(python_files)} Python files identified on provided path.")

        for filepath in python_files:
            process_dag_file(filepath)
