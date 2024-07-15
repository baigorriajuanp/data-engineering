from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
from modules.data_from_api import extract_crypto_data
from modules.data_transformation import transform_data
from modules.data_cleaner import clean_and_transform_data
from modules.upload_rs import load_data_to_redshift
from modules.alerting import check_and_send_alert
import pandas as pd
import os
from dotenv import load_dotenv

load_dotenv()

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 7, 15),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'pipeline_dag',
    default_args=default_args,
    description='ETL pipeline DAG',
    schedule_interval=timedelta(days=1),
)

REDSHIFT_USERNAME = os.getenv('REDSHIFT_USERNAME')
REDSHIFT_PASSWORD = os.getenv('REDSHIFT_PASSWORD')
REDSHIFT_HOST = os.getenv('REDSHIFT_HOST')
REDSHIFT_PORT = os.getenv('REDSHIFT_PORT')
REDSHIFT_DBNAME = os.getenv('REDSHIFT_DBNAME')
REDSHIFT_CONN_STR = f'redshift+psycopg2://{REDSHIFT_USERNAME}:{REDSHIFT_PASSWORD}@{REDSHIFT_HOST}:{REDSHIFT_PORT}/{REDSHIFT_DBNAME}'
REDSHIFT_TABLE = 'crypto_data'
API_URL = 'https://api.coingecko.com/api/v3/coins/markets?vs_currency=usd'

def extract(**kwargs):
    raw_data = extract_crypto_data(API_URL)
    kwargs['ti'].xcom_push(key='raw_data', value=raw_data)

def transform(**kwargs):
    raw_data = kwargs['ti'].xcom_pull(key='raw_data', task_ids='extract')
    transformed_data = transform_data(raw_data)
    cleaned_data = clean_and_transform_data(transformed_data)
    kwargs['ti'].xcom_push(key='cleaned_data', value=cleaned_data)

def load(**kwargs):
    cleaned_data = kwargs['ti'].xcom_pull(key='cleaned_data', task_ids='transform')
    df = pd.DataFrame(cleaned_data)
    load_data_to_redshift(df, REDSHIFT_TABLE, REDSHIFT_CONN_STR)

def alert(**kwargs):
    cleaned_data = kwargs['ti'].xcom_pull(key='cleaned_data', task_ids='transform')
    check_and_send_alert(cleaned_data)

extract_task = PythonOperator(
    task_id='extract',
    python_callable=extract,
    provide_context=True,
    dag=dag,
)

transform_task = PythonOperator(
    task_id='transform',
    python_callable=transform,
    provide_context=True,
    dag=dag,
)gt

load_task = PythonOperator(
    task_id='load',
    python_callable=load,
    provide_context=True,
    dag=dag,
)

alert_task = PythonOperator(
    task_id='alert',
    python_callable=alert,
    provide_context=True,
    dag=dag,
)

extract_task >> transform_task >> [load_task, alert_task]