import os
from pathlib import Path
from setuptools import setup


def read(file_name):
    with open(
        os.path.join(
            Path(os.path.dirname(__file__)),
            file_name)
    ) as _file:
        return _file.read()


long_description = read('README.md')

setup(
    name='airflow-parse-bench',
    version='1.0.1',
    description="Easily measure and compare your Airflow DAGs' parse time.",
    url='https://github.com/AlvaroCavalcante/airflow-parse-bench',
    download_url='https://github.com/AlvaroCavalcante/airflow-parse-bench',
    license='Apache License 2.0',
    author='Alvaro Leandro Cavalcante Carneiro',
    author_email='alvaroleandro250@gmail.com',

    py_modules=['airflow_parse', 'bench_db_utils', 'dag_parse'],
    package_dir={'': 'src'},

    long_description=long_description,
    long_description_content_type='text/markdown',
    keywords=[
        'airflow',
        'python',
        'python3',
        'dag',
        'parse',
        'benchmark',
        'apache',
        'data',
        'data-engineering',
        'benchmarking'
    ],
    entry_points={
        'console_scripts': [
            'airflow-parse-bench=airflow_parse:main',
        ],
    },
    classifiers=[
        'Programming Language :: Python',
        'Programming Language :: Python :: 3',
        'Natural Language :: English',
        'Operating System :: OS Independent',
        'Intended Audience :: Developers',
        'Intended Audience :: Information Technology',
        'Development Status :: 4 - Beta',
        'License :: OSI Approved :: Apache Software License',
        'Topic :: System :: Benchmark',
        'Topic :: Software Development :: Libraries :: Python Modules',
        'Framework :: Apache Airflow',
    ],

    python_requires='>=3.8',
    install_requires=[
        'apache-airflow==2.10.4',
        'apache-airflow-providers-apache-beam==5.9.1',
        'apache-airflow-providers-common-compat==1.2.2',
        'apache-airflow-providers-common-io==1.4.2',
        'apache-airflow-providers-common-sql==1.20.0',
        'apache-airflow-providers-sqlite==3.9.0',
        'apache-airflow-providers-standard==0.0.2',
        'apache-airflow-providers-google==10.26.0',
        'colorama==0.4.6',
        'tqdm==4.67.1'
    ]
)
