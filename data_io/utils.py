import requests

def fetch_data(url, params=None):
    response = requests.get(url, params=params)
    response.raise_for_status()  # Raise an exception for HTTP errors
    return response.json()