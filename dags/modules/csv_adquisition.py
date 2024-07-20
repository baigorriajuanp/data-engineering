from sqlalchemy import create_engine, Table, Column, Integer, String, Float, MetaData
from sqlalchemy.dialects.postgresql import TIMESTAMP
import datetime
from io import StringIO


def load_data_from_csv(file_path, redshift_table, redshift_conn_str):
    
    df = pd.read_csv(file_path)
    
    
    csv_buffer = StringIO()
    df.to_csv(csv_buffer, index=False)
    engine = create_engine(redshift_conn_str)
    df.to_sql(redshift_table, engine, index=False, if_exists='append')

    print("Data loaded from CSV to Redshift successfully")