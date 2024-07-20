from datetime import datetime, timedelta
from modules.data_from_api import extract_crypto_data
from modules.data_transformation import transform_data
from modules.data_cleaner import clean_and_transform_data
from modules.upload_rs import load_data_to_redshift
from modules.alerting import check_and_send_alert
from modules.csv_adquisition import load_data_from_csv
import pandas as pd
import os
import json

from dotenv import load_dotenv
from airflow import DAG
from airflow.operators.python_operator import PythonOperator

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
    catchup=False,
)

REDSHIFT_USERNAME = os.getenv('REDSHIFT_USERNAME')
REDSHIFT_PASSWORD = os.getenv('REDSHIFT_PASSWORD')
REDSHIFT_HOST = os.getenv('REDSHIFT_HOST')
REDSHIFT_PORT = os.getenv('REDSHIFT_PORT')
REDSHIFT_DBNAME = os.getenv('REDSHIFT_DBNAME')
REDSHIFT_CONN_STR = f'redshift+psycopg2://{REDSHIFT_USERNAME}:{REDSHIFT_PASSWORD}@{REDSHIFT_HOST}:{REDSHIFT_PORT}/{REDSHIFT_DBNAME}'
REDSHIFT_TABLE = 'crypto_data'
API_URL = os.getenv('API_URL')

def extract(**kwargs):
    raw_data = extract_crypto_data(API_URL)
    print(f"Extracted data: {raw_data}")
    kwargs['ti'].xcom_push(key='raw_data', value=raw_data)

def transform(**kwargs):
    ti = kwargs['ti']
    raw_data = ti.xcom_pull(task_ids='extract', key='raw_data')
    
    if raw_data is None:
        raise ValueError("No data was found in XCom from 'extract' task.")
    
    transformed_data = transform_data(raw_data)
    cleaned_data = clean_and_transform_data(transformed_data)
    # Convertir el DataFrame a JSON
    df = pd.DataFrame(cleaned_data)
    cleaned_data = df.to_json()
    # Enviar los datos JSON a XCom
    ti.xcom_push(key='cleaned_data', value=cleaned_data)

def load(**kwargs):
    try:
            # Recupera los datos serializados desde XCom
            cleaned_data_serialized = kwargs['ti'].xcom_pull(key='cleaned_data', task_ids='transform')
            # Deserializa los datos (ajusta el método de deserialización según sea necesario)
            cleaned_data = json.loads(cleaned_data_serialized)
            # Convierte los datos a un DataFrame
            df = pd.DataFrame(cleaned_data)
            df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')  # convertir de milisegundos a timestamp
            df['timestamp'] = df['timestamp'].astype('datetime64[ns]')
            # Carga los datos en Redshift
            load_data_to_redshift(df, REDSHIFT_TABLE, REDSHIFT_CONN_STR)
            print("Data loaded successfully into Redshift.")

    except Exception as e:
        print(f"An error occurred: {e}")
        raise  # Re-raise the exception to mark the task as failed in Airflow


def alert(**kwargs):
    # Recupera los datos serializados desde XCom
    cleaned_data_serialized = kwargs['ti'].xcom_pull(key='cleaned_data', task_ids='transform')
    # Deserializa los datos (ajusta el método de deserialización según sea necesario)
    cleaned_data = json.loads(cleaned_data_serialized)
    # Convierte los datos a un DataFrame
    df = pd.DataFrame(cleaned_data)
    df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')  # convertir de milisegundos a timestamp
    df['timestamp'] = df['timestamp'].astype('datetime64[ns]')
    # Convertir DataFrame a lista de diccionarios
    cleaned_data_list = df.to_dict(orient='records')
    check_and_send_alert(cleaned_data_list)

def load_from_csv(**kwargs):
    file_path = '/path/to/your/csv_file.csv'
    load_data_from_csv(file_path, REDSHIFT_TABLE, REDSHIFT_CONN_STR)

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
)

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

load_from_csv_task = PythonOperator(
    task_id='load_from_csv',
    python_callable=load_from_csv,
    provide_context=True,
    dag=dag,
)

extract_task >> transform_task >> [load_task, alert_task]
# Tarea independiente para cargar desde CSV
load_from_csv_task
