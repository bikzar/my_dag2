from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import BranchPythonOperator
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago

from datetime import datetime, timedelta
from print_data_DAG import FromExcelToCsvParser

import os

INPUT_PATH = '/home/excel/'
OUTPUT_PATH = '/home/csv/'

default_args = {
    'owner': 'bikzar',
    'start_date': days_ago(1),
    'provide_context': True,
}


def choice_branch(**kwargs):
    file_type = kwargs['task_instance'].xcom_pull(key='file_type')

    if file_type and file_type.endswith('xls'):
        return 'process_excel_file'
    else:
        return 'copy_csv'


def check_file_type(**kwargs):
    files = os.scandir(INPUT_PATH)

    for f in files:
        if f.name.endswith('xls') or f.name.endswith('xlsx'):
            kwargs['task_instance'].xcom_push(key='file_type', value='xls')
            print('Excel file has been found in folder.')
            break
        elif f.name.endswith('csv'):
            print('CSV file has been found in folder.')
            break
        else:
            raise FileNotFoundError('In folder no files.')


# Move all *.csv file to output folder and add to them date when it have been copied.
# And folder will be created for output if it's not exist.

move_command = """
    mkdir {{params.out}} 2>/dev/null

    for f in {{params.input}}*.csv; do
       mv "$f" {{params.out}}output_$(date +'%F_%T_%N').csv
    done
"""

# Every hour in the folder /home/excel/ will be appeared scv files or excel file. Excel should be pars to cvs and
# save to /home/csv/output/output_(yyyy-mm-dd)_(hh:mm:ss)_(nan_sek).csv folder and CSV file should be just copy to the
# same folder /home/csv/output/output_(yyyy-mm-dd)_(hh:mm:ss)_(nan_sek).csv.

with DAG('xcom_branch_DAG',
         default_args=default_args,
         schedule_interval='0 0 * * * *'  # start task every hour and wait files 30 minutes
         ) as dag:
    file_checker = PythonOperator(
        task_id='check_file_input',
        retries=5,                         # in real it 6 retries
        retry_delay=timedelta(minutes=5),  # 6 retries * 5 minutes = 30 minutes for waiting files in input
        python_callable=check_file_type,
    )

    branch = BranchPythonOperator(
        task_id='branch_excel_or_scv',
        python_callable=choice_branch,
    )

    process_excel = FromExcelToCsvParser(
        task_id='process_excel_file',
        xls_file=INPUT_PATH + 'input.xlsx',
        csv_file=OUTPUT_PATH + 'output' + datetime.now().strftime("%Y-%m-%d_%X_%f") + '.csv',
    )

    copy_csv = BashOperator(
        task_id='copy_csv',
        bash_command=move_command,
        params={'input': INPUT_PATH, 'out': OUTPUT_PATH}
    )

file_checker >> branch >> [process_excel, copy_csv]
