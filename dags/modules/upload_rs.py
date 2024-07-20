from sqlalchemy import create_engine, Table, Column, Integer, String, Float, MetaData
from sqlalchemy.dialects.postgresql import TIMESTAMP
import datetime
from io import StringIO
import pandas as pd

# Función para cargar los datos en Amazon Redshift
def load_data_to_redshift(df, redshift_table, redshift_conn_str):
    
    engine = create_engine(redshift_conn_str)
    metadata = MetaData()
    
    # Definir la estructura de la tabla
    table = Table(
        redshift_table, metadata,
        Column('id', String, primary_key=True),
        Column('id_hash', String),
        Column('symbol', String),
        Column('name', String),
        Column('current_price', Float),
        Column('market_cap', Float),
        Column('total_volume', Float),
        Column('timestamp', TIMESTAMP)
    )

    # Crear la tabla si no existe
    metadata.create_all(engine)
    
    # Convertir la columna 'timestamp' de bigint a timestamp
    df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')  # Asumiendo que el valor está en milisegundos
    
    # Cargar los datos en Redshift
    with engine.connect() as connection:
        for index, row in df.iterrows():
            insert_stmt = table.insert().values(
                id=row['id'],
                id_hash=row['id_hash'],
                symbol=row['symbol'],
                name=row['name'],
                current_price=row['current_price'],
                market_cap=row['market_cap'],
                total_volume=row['total_volume'],
                timestamp=row['timestamp']
            )
            connection.execute(insert_stmt)
    
    print(f"Datos cargados en la tabla {redshift_table}")

def load_data_from_csv(file_path, redshift_table, redshift_conn_str):
    
    df = pd.read_csv(file_path)
    
    
    csv_buffer = StringIO()
    df.to_csv(csv_buffer, index=False)
    engine = create_engine(redshift_conn_str)
    df.to_sql(redshift_table, engine, index=False, if_exists='append')

    print("Data loaded from CSV to Redshift successfully")