# Airflow Dag Parse Benchmarking

**Stop creating bad DAGs!**

Use this tool to measure and compare the parse time of your DAGs, identify bottlenecks, and optimize your Airflow environment for better performance.

# Contents

- [How it works](#how)
- [Installation](#installation)
    - [Install your Airflow dependencies](#install-dependencies)
    - [Configure your Airflow Variables](#configure-variables)
- [Usage](#usage)
    - [Additional Options](#options)
- [Roadmap](#roadmap)
- [Contribute](#contribute)

# How It Works <a id="how"></a>
Retrieving parse metrics from an Airflow cluster is straightforward, but measuring the effectiveness of code optimizations can be tedious. Each code change requires redeploying the Python file to your cloud provider, waiting for the DAG to be parsed, and then extracting a new report — a slow and time-consuming process.

This tool simplifies the process of measuring and comparing DAG parse times. It uses the same parse method as Airflow (from the Airflow repository) to measure the time taken to parse your DAGs locally, storing results for future comparisons.

# Installation <a id="installation"></a>
It's recommended to use a [virtualenv](https://docs.python.org/3/library/venv.html) to avoid library conflicts. Once set up, you can install the package by running the following command:

```bash
pip install airflow-parse-bench
```

## Install your Airflow dependencies <a id="install-dependencies"></a>
The command above installs only the essential library dependencies (Airflow and Airflow providers). You’ll need to manually install any additional libraries that your DAGs depend on.

For example, if a DAG uses ```boto3``` to interact with AWS, ensure that boto3 is installed in your environment. Otherwise, you'll encounter parse errors.

## Configure your Airflow Variables <a id="configure-variables"></a>
If your DAGs use **Airflow Variables**, you must define them locally as well. Use placeholder values, as the actual values aren't required for parsing purposes. 

To setup Airflow Variables locally, you can use the following command:

```bash
airflow variables set MY_VARIABLE 'ANY TEST VALUE'
```
Without this, you'll encounter an error like:
```bash
error: 'Variable MY_VARIABLE does not exist'
```

# Usage <a id="usage"></a>
To measure the parse time of a single Python file, just run:

```bash
airflow-parse-bench --path your_path/dag_test.py
```
The output will look like this:
![lib_output](assets/lib_output.png)

The result table includes the following columns:

- **Filename**: The name of the Python module containing the DAG. This unique name is the key to store DAG information.
- **Current Parse Time**: The time (in seconds) taken to parse the DAG.
- **Previous Parse Time**: The parse time from the previous run.

- **Difference**: The difference between the current and previous parse times.
- **Best Parse Time**: The best parse time recorded for the DAG.

You can also measure the parse time for all Python files in a directory by running:

```bash
airflow-parse-bench --path your_path/your_dag_folder
```
This time, the output table will display parse times for all Python files in the folder:
![lib_output](assets/multiple_lib_output.png)

## Additional Options <a id="options"></a>
The library supports some additional arguments to customize the results. To see all available options, run:

```bash
airflow-parse-bench --help
```
It will display the following options:
- **--path**: The path to the Python file or directory containing the DAGs.
- **--order**: The order in which the results are displayed. You can choose between 'asc' (ascending) or 'desc' (descending).
- **--num-iterations**: The number of times to parse each DAG. The parse time will be averaged across iterations.
- **--skip-unchanged**: Skip DAGs that haven't changed since the last run.
- **--reset-db**: Clear all stored data in the local database, starting a fresh execution.

> **Note**: If a Python file has parsing errors or contains no valid DAGs, it will be excluded from the results table, and an error message will be displayed.  

# Roadmap <a id="roadmap"></a>
This project is still in its early stages, and there are many improvements planned for the future. Some of the features we're considering include:

- **Cloud DAG Parsing:** Automatically download and parse DAGs from cloud providers like AWS S3 or Google Cloud Storage.
- **Parallel Parsing:** Speed up processing by parsing multiple DAGs simultaneously.
- **Support .airflowignore:** Ignore files and directories specified in the ```.airflowignore``` file.

If you’d like to suggest a feature or report a bug, please open a new issue!

# Contributing <a id="contribute"></a>
This project is open to contributions! If you want to collaborate to improve the tool, please follow these steps:

1.  Open a new issue to discuss the feature or bug you want to address.
2.  Once approved, fork the repository and create a new branch.
3.  Implement the changes.
4.  Create a pull request with a detailed description of the changes.