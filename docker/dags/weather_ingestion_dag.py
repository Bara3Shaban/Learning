from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from pathlib import Path
import json

from ingestion.weather_api import fetch_weather
from models.pydantic.weather import WeatherResponse

STAGING_DIR = Path("/opt/airflow/data_lake/staging/weather")


def ingest_weather_task(**context):
    data = fetch_weather(city="Amman")
    weather=WeatherResponse.parse_obj(data)
    staging_record = weather.to_staging_dict()

    STAGING_DIR.mkdir(parents=True,exist_ok=True)
    output_file  = STAGING_DIR / f"{weather.current_weather.time}.json"



    with open(output_file,"w") as file:
        json.dump(staging_record,file,indent=4,default=str)
        print(f"Saved weather data to{output_file}")


with DAG(
    dag_id="weather_raw_ingestion",
    start_date=datetime(2025, 1, 1),
    schedule_interval="* * * * *",
    catchup=False,
    tags=["ingestion", "weather"],
) as dag:

    ingest_weather = PythonOperator(
        task_id="fetch_weather_data",
        python_callable=ingest_weather_task,
    )
