from airflow import AirflowException
from airflow import DAG
from airflow.operators.python_operator import BaseOperator
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from airflow.utils.decorators import apply_defaults

from datetime import datetime
from typing import Iterable

import os
import pandas as pd


class FromExcelToCsvParser(BaseOperator):
    @apply_defaults
    def __init__(self, output_path: str,
                 prefix='', xls_path_list: Iterable[str] = None,
                 sheet_name: str = None, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.xls_path_list = xls_path_list
        self.output_path = output_path
        self.sheet_name = sheet_name
        self.prefix = prefix

    def execute(self, context):

        if not self.xls_path_list:
            alternative_path_list = context['ti'].xcom_pull(key='input_list')
            if not alternative_path_list:
                raise AirflowException("Not defined path_list.")
            self.xls_path_list = alternative_path_list

        for file in self.xls_path_list:
            if file and os.path.exists(file):
                file_type = os.path.basename(file).split('.')[-1]
                if file_type != "xls" and file_type != "xlsx":
                    raise TypeError("File type {} incorrect.".format(file_type))
            else:
                raise FileNotFoundError("File not exists: {}".format(file))

            excel_file = pd.read_excel(file, self.sheet_name)

            for sheet in self.__get_sheet_list(excel_file):
                self.__save_file_sheet_to_csv(sheet, file)

            os.remove(file)

    def __save_file_sheet_to_csv(self, file_sheet, xls_file):
        file_name = os.path.basename(xls_file).split('.')[0]
        new_name = '{}{}_{}_{}.{}'.format(self.output_path, self.prefix,
                                          file_name,
                                          datetime.now().strftime("%Y-%m-%d_%X_%f"),
                                          'csv')
        file_sheet.to_csv(new_name, index=False)
        print("File has been parsed to {}".format(new_name))

    def __get_sheet_list(self, excel_file):
        sheet_list = []
        if isinstance(excel_file, dict):
            sheet_list.extend(excel_file.values())
        else:
            sheet_list.append(excel_file)

        return sheet_list


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
        xls_path_list=['/home/excel/input.xlsx'],
        output_path='/home/csv/',
    )

task_1 >> task_2
