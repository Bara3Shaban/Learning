import requests
from datetime import datetime
from typing import Dict

def fetch_weather(city:str)->Dict:
    url = "https://api.open-meteo.com/v1/forecast"

    params={
        "latitude": 31.95,   
        "longitude": 35.91,
        "current_weather": True
    }
    response = requests.get(url,params=params)
    response.raise_for_status()

    return response.json()