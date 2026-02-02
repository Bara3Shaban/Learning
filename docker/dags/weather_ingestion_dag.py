from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from pathlib import Path
import json

from ingestion.weather_api import fetch_weather
from models.raw.pydantic.raw_weather_model import WeatherResponse
#from processing.weather_staging import transform_weather_raw_to_staging

RAW_DIR = Path("/opt/airflow/data_lake/raw")
STAGING_DIR = Path("/opt/airflow/data_lake/staging/weather")


def ingest_weather_task(**context):
    ds=context["ds"]
    raw = fetch_weather(city="Amman")
    
    raw_path = RAW_DIR / f"dt={ds}"

    raw_path.mkdir(parents=True,exist_ok=True)
    raw_path = raw_path / "weather.json"
    with open(raw_path,"w") as file:
        json.dump(raw,file,indent=4,default=str)


    #weather=WeatherResponse.parse_obj(data)
    #staging_record = weather.to_staging_dict()
    
    
    #transform_weather_raw_to_staging(raw_file_path = "docker/data_lake/raw/weather/dt=('2026-01-31',)/weather_amman.json",staging_base_path="test")


    #STAGING_DIR.mkdir(parents=True,exist_ok=True)
    #output_file  = STAGING_DIR / f"{weather.current_weather.time}.json"



    #with open(output_file,"w") as file:
    #    json.dump(staging_record,file,indent=4,default=str)
    #    print(f"Saved weather data to{output_file}")


with DAG(
    dag_id="weather_raw_ingestion",
    start_date=datetime(2025, 1, 1),
    schedule_interval="* 1 * * *",
    catchup=False,
    tags=["ingestion", "weather"],
) as dag:

    ingest_weather = PythonOperator(
        task_id="fetch_weather_data",
        python_callable=ingest_weather_task,
    )
