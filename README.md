# Airflow Dag Parse Benchmarking

A Python tool to parse Airflow DAGs and measure the time taken to parse them.

# How It Works

While retrieving parse metrics from an Airflow cluster is straightforward, measuring the effectiveness of your code optimizations can be less so. Every time you modify your code, you need to redeploy the updated Python file to your cloud provider, wait for the DAG to be parsed, and then extract a new report - a slow and time-consuming process.

To address this challenge, this tool simplifies measuring and comparing the parse times of your DAGs. It uses the same parse method as Airflow (taken from the Airflow repository), measures the time taken to parse the DAGs, and stores the results for further comparisons.


# Installing

# Usage

# Roadmap