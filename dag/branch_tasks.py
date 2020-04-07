from airflow import DAG
from airflow.operators.python_operator import BranchPythonOperator

from datetime import datetime

from print_data_DAG import FromExcelToCsvParser

import os

INPUT_PATH = '/home/excel/'
OUTPUT_PATH = '/home/csv/'

default_args = {
    'owner': 'bikzar',
    'start_date': datetime(2020, 4, 7),
    'provide_context': True,
}


def find_last_file():
    if not os.path.exists(INPUT_PATH):
        raise FileNotFoundError(
            "Folder not found: {}".format(INPUT_PATH))

    file_list = []

    for root, dirs, files in os.walk(INPUT_PATH):
        for f in files:
            if f.endswith('xls') or f.endswith('xlsx'):
                file_list.append(os.path.join(root, f))
            else:
                os.remove(os.path.join(root, f))

    file_list.sort(key=os.path.getctime, reverse=True)

    return file_list[0]


def choice_branch(**kwargs):
    file_path = find_last_file()
    kwargs['ti'].xcom_push(key='input_list', value=[file_path])

    file_name = os.path.basename(file_path).rsplit('.', 1)[0]

    if len(file_name) % 2 == 0:
        return 'process_excel_file_even'

    return 'process_excel_file_odd'


with DAG('xcom_branch_DAG', default_args=default_args) as dag:
    branch = BranchPythonOperator(
        task_id='branch_excel_or_scv',
        python_callable=choice_branch,
    )

    process_excel_1 = FromExcelToCsvParser(
        task_id='process_excel_file_odd',
        output_path=OUTPUT_PATH,
        prefix="1",
    )

    process_excel_2 = FromExcelToCsvParser(
        task_id='process_excel_file_even',
        output_path=OUTPUT_PATH,
        prefix="2",
    )

branch >> [process_excel_1, process_excel_2]
