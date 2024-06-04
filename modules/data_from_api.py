import requests

# Función para extraer datos de la API
def extract_crypto_data(api_url):
    response = requests.get(api_url)
    if response.status_code == 200:
        data = response.json()
        return data
    else:
        raise Exception(f"Error al obtener datos de la API: {response.status_code}")