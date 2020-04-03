from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago

from datetime import datetime

default_args = {
    'owner': 'bikzar',
    'start_date': days_ago(1),
}

dag = DAG(
    'my_simple_dag',
    default_args=default_args,
)


def print_current_date():
    print(datetime.now())


task_1 = PythonOperator(
    task_id='print_date_to_logs',
    python_callable=print_current_date,
    dag=dag,
)
