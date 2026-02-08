from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator,BigQueryCheckOperator


from ingestion.client import fetch_weather
from ingestion.bigquery_raw import upload_weather_raw
from airflow.models import Variable




project_id = Variable.get("gcp_project_id")







def ingest_weather_raw(project_id:str,**context):
    batch_id = context["run_id"]

    payload = fetch_weather(
        latitude=31.95,
        longitude=35.91,
    )
    upload_weather_raw(
        payload=payload,
        project_id=project_id,
        batch_id=batch_id,
    )
    



with DAG(
    dag_id="weather_raw_ingestion",
    start_date=datetime(2025, 1, 1),
    schedule_interval="* 1 * * *",
    catchup=False,
    tags=["ingestion", "weather"],
    template_searchpath=["/opt/airflow/src"],
    default_args={"project_id": "learning-486315"},
    user_defined_macros={"project_id": 'learning-486315'}
) as dag:

    """ ingest_weather = PythonOperator(
        task_id="fetch_weather_data",
        python_callable=ingest_weather_raw,
        op_kwargs={"project_id": "learning-486315"}
    )

    run_weather_staging = BigQueryInsertJobOperator(
        task_id="raw_to_staging_weather",
        configuration={
            "query":{
                "query": "{% include 'pipelines/weather_sql_elt/staging/weather_staging.sql' %}",
                "useLegacySql":False,
            }
        },
        params={"project_id": f"{project_id}"},
    )
 """
    run_weather_staging_check = BigQueryCheckOperator(
        task_id = "weather_staging_check",
        sql = "{% include 'pipelines/weather_sql_elt/quality/staging_weather_checks.sql' %}",
        use_legacy_sql=False,
        params= {
            "project_id": f"{project_id}"
            },
    )
    
    ls_task = BashOperator(
    task_id="list_directory_contents",
    bash_command="ls -la",
    trigger_rule="all_success",
    )
 
    echo_task = BashOperator(
    task_id="echo_hello",
    bash_command="echo 'Hello, Airflow'",
    trigger_rule="one_failed",
    )

    run_weather_staging_check >> ls_task
    run_weather_staging_check >> echo_task
    #ingest_weather >> run_weather_staging >> run_weather_staging_check


""" from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from pathlib import Path
import json

from ingestion.client import fetch_weather
from models.raw.pydantic.raw_weather_model import WeatherResponse
from ingestion.bigquery_raw import upload_weather_raw
#from processing.weather_staging import transform_weather_raw_to_staging

RAW_DIR = Path("/opt/airflow/data_lake/raw")
STAGING_DIR = Path("/opt/airflow/data_lake/staging/weather")


def ingest_weather_task(**context):
    
    upload_weather_raw(
        rows=[
            {
                "temp": 30,
                "city": "Dammam",
                "dt_iso": "2026-02-03T12:00:00Z"
            }
        ],
        project_id="learning-486315",
    )

    
    '''ds=context["ds"]
    raw = fetch_weather(city="Amman")
    
    raw_path = RAW_DIR / f"dt={ds}"

    raw_path.mkdir(parents=True,exist_ok=True)
    raw_path = raw_path / "weather.json"
    with open(raw_path,"w") as file:
        json.dump(raw,file,indent=4,default=str)'''


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
 """



