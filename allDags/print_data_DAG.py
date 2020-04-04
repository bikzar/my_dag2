from airflow import DAG
from airflow.operators.python_operator import BaseOperator
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from airflow.utils.decorators import apply_defaults

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
        self.xls_file = xls_file
        self.csv_file = csv_file

    def execute(self, context):
        if os.path.exists(self.xls_file):
            excel_file = pd.read_excel(self.xls_file)  # Can fail if file not Excel type
            excel_file.to_csv(self.csv_file, index=False)
            os.remove(self.xls_file)
            print("File has been successfully written to {}".format(self.csv_file))
        else:
            raise FileNotFoundError("File not found: {}".format(self.xls_file))


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
        xls_file='/home/excel/input.xlsx',
        csv_file='/home/csv/output.csv',
    )

task_1 >> task_2
