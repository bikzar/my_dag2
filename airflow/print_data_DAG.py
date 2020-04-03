from datetime import timedelta, datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago

default_args = {
    'owner': 'bikzar',
    'depends_on_past': False,
    'start_date': days_ago(2),
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

dag = DAG(
    'my_simple_dag',
    default_args=default_args,
    description='A simple DAG',
    schedule_interval='* */20 * * *',
)


def print_current_date():
    print(datetime.now())


task_1 = PythonOperator(
    task_id='print_date_to_logs',
    python_callable=print_current_date,
    dag=dag,
)

