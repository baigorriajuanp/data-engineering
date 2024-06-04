import datetime

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
