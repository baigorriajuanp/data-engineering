from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import os

def run_main():
    os.system("python Pre-entrega\__main__.py")

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'tercera-entrega',
    default_args=default_args,
    description='tercera-entrega',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 1, 1),
    catchup=False,
) as dag:

    run_my_script = PythonOperator(
        task_id='run_my_main',
        python_callable=run_main,
    )

    run_my_script