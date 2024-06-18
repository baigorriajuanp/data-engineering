import pandas as pd
import datetime    


def clean_and_transform_data(data):
    cleaned_data = []
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
        cleaned_data.append(coin_data)
    
    df = pd.DataFrame(cleaned_data)
    
    # Eliminación de datos duplicados:
    df.drop_duplicates(subset=['id'], keep='last', inplace=True)

    # Eliminación de datos nulos:
    df.dropna(inplace=True)

    # Verificación y conversión de datos:
    df['current_price'] = pd.to_numeric(df['current_price'], errors='coerce')
    df['market_cap'] = pd.to_numeric(df['market_cap'], errors='coerce')
    df['total_volume'] = pd.to_numeric(df['total_volume'], errors='coerce')
    df['timestamp'] = pd.to_datetime(df['timestamp'], errors='coerce')

    # Verificación de rangos válidos:
    df = df[df['current_price'] >= 0]
    df = df[df['market_cap'] >= 0]
    df = df[df['total_volume'] >= 0]

    #Verificación de fechas futuras:
    now = pd.Timestamp.now()
    df = df[df['timestamp'] <= now]

    # Verificación de registros incompletos
    df = df[df['name'].notna() & df['symbol'].notna()]

    # Verificación de duplicados en combinaciones clave
    df.drop_duplicates(subset=['id', 'timestamp'], keep='last', inplace=True)

    # Normalización de datos
    df['symbol'] = df['symbol'].str.lower()
    df['name'] = df['name'].str.title()

    return df