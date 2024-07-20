import os
from modules.upload_rs import load_data_to_redshift
from modules.data_transformation import transform_data
from modules.data_from_api import extract_crypto_data
from modules.remove_atypical_values import remove_outliers
from modules.data_cleaner import clean_and_transform_data
from parameters import url
from dotenv import load_dotenv
import pandas as pd

load_dotenv()

def main():
    REDSHIFT_USERNAME= os.getenv('REDSHIFT_USERNAME')
    REDSHIFT_PASSWORD= os.getenv('REDSHIFT_PASSWORD')
    REDSHIFT_HOST= os.getenv('REDSHIFT_HOST')
    REDSHIFT_PORT= os.getenv('REDSHIFT_PORT')
    REDSHIFT_DBNAME= os.getenv('REDSHIFT_DBNAME')

    raw_data = extract_crypto_data(url)
    transformed_data = transform_data(raw_data)
    cleaned_data = clean_and_transform_data(transformed_data) # Incorporación en entregable Semana 2
    df = pd.DataFrame(cleaned_data)

    redshift_conn_str = f'redshift+psycopg2://{REDSHIFT_USERNAME}:{REDSHIFT_PASSWORD}@{REDSHIFT_HOST}:{REDSHIFT_PORT}/{REDSHIFT_DBNAME}'
    redshift_table = 'crypto_data'
    load_data_to_redshift(df, redshift_table, redshift_conn_str)


if __name__ == "__main__":
    main()