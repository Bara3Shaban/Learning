import json
import os
from datetime import datetime,timezone
import pandas as pd


def transform_weather_raw_to_staging(
    raw_file_path:str,
    staging_base_path:str
)->str:
    with open(raw_file_path,'r')as raw_file:
        raw_data = json.load(raw_file)
        record = {
            "city":raw_data["city"],
            "country": raw_data["sys"]["country"],
            "temperature_c": raw_data["main"]["temp"],
            "feels_like_c": raw_data["main"]["feels_like"],
            "humidity": raw_data["main"]["humidity"],
            "pressure": raw_data["main"]["pressure"],
            "weather_main": raw_data["weather"][0]["main"],
            "weather_description": raw_data["weather"][0]["description"],
            "wind_speed": raw_data["wind"]["speed"],
            "clouds_percent": raw_data["clouds"]["all"],
            "observation_time_utc": datetime.fromtimestamp(
            raw_data["dt"], tz=timezone.utc),
            "ingestion_time_utc": datetime.now(timezone.utc),
        }
    df = pd.dataframe([record])
    print(df)
