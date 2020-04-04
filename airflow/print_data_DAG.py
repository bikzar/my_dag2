from airflow import DAG
from airflow.operators.python_operator import BaseOperator
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from airflow.utils.decorators import apply_defaults
from airflow.utils.state import State

from datetime import datetime

import os
import pandas as pd


class FromExcelToCsvParser(BaseOperator):
    @apply_defaults
    def __init__(
            self,
            xls_file: str,
            csv_file: str,
            *args,
            **kwargs):
        super().__init__(*args, **kwargs)
        self.__xls_file = xls_file
        self.__csv_file = csv_file

    def execute(self, context):
        if os.path.exists(self.__xls_file):
            excel_file = pd.read_excel(self.__xls_file)  # Can fail if file not Excel type
            excel_file.to_csv(self.__csv_file, index=False)
            print("File has been successfully written to {}".format(self.__csv_file))
        else:
            print("File not found: {}".format(self.__xls_file))
            context['dag'].set_dag_runs_state(State.FAILED)


EXCEL_FILE_PATH = '/home/excel/input.xlsx'
CSV_FILE_PATH = '/home/csv/output.csv'

default_args = {
    'owner': 'bikzar',
    'start_date': days_ago(1),
}


def print_current_date():
    print(datetime.now())


with DAG('my_simple_dag', default_args=default_args) as dag:
    task_1 = PythonOperator(
        task_id='print_date_to_logs',
        python_callable=print_current_date,
    )

    task_2 = FromExcelToCsvParser(
        task_id='pars_excel_to_file',
        xls_file=EXCEL_FILE_PATH,
        csv_file=CSV_FILE_PATH,
    )

task_1 >> task_2
