import requests
import pandas as pd
from sqlalchemy import create_engine, Table, Column, Integer, String, Float, MetaData
from sqlalchemy.dialects.postgresql import TIMESTAMP
import datetime

# URL de la API para obtener datos de precios de criptomonedas
url = 'https://api.coingecko.com/api/v3/coins/markets?vs_currency=usd'

# Función para extraer datos de la API
def extract_crypto_data(api_url):
    response = requests.get(api_url)
    if response.status_code == 200:
        data = response.json()
        return data
    else:
        raise Exception(f"Error al obtener datos de la API: {response.status_code}")

# Función para transformar los datos en un diccionario de Python
def transform_data(data):
    transformed_data = []
    for coin in data:
        coin_data = {
            'id': coin['id'],
            'symbol': coin['symbol'],
            'name': coin['name'],
            'current_price': coin['current_price'],
            'market_cap': coin['market_cap'],
            'total_volume': coin['total_volume'],
            'timestamp': datetime.datetime.now()
        }
        transformed_data.append(coin_data)
    return transformed_data

# Función para cargar los datos en Amazon Redshift
def load_data_to_redshift(df, redshift_table, redshift_conn_str):
    
    engine = create_engine(redshift_conn_str)
    metadata = MetaData()
    
    # Definir la estructura de la tabla
    table = Table(
        redshift_table, metadata,
        Column('id', String, primary_key=True),
        Column('symbol', String),
        Column('name', String),
        Column('current_price', Float),
        Column('market_cap', Float),
        Column('total_volume', Float),
        Column('timestamp', TIMESTAMP)
    )

    # Crear la tabla si no existe
    metadata.create_all(engine)
    
    # Cargar los datos en Redshift
    with engine.connect() as connection:
        for index, row in df.iterrows():
            insert_stmt = table.insert().values(
                id=row['id'],
                symbol=row['symbol'],
                name=row['name'],
                current_price=row['current_price'],
                market_cap=row['market_cap'],
                total_volume=row['total_volume'],
                timestamp=row['timestamp']
            )
            connection.execute(insert_stmt)

    print(f"Datos cargados en la tabla {redshift_table}")

# Ejecución del proceso ETL
if __name__ == "__main__":
    
    raw_data = extract_crypto_data(url)
    transformed_data = transform_data(raw_data)
    df = pd.DataFrame(transformed_data)
    redshift_conn_str = 'redshift+psycopg2://baigorriajuanp_coderhouse:5iQ2iPP1I4@data-engineer-cluster.cyhh5bfevlmn.us-east-1.redshift.amazonaws.com:5439/data-engineer-database'
    redshift_table = 'crypto_data'
    load_data_to_redshift(df, redshift_table, redshift_conn_str)