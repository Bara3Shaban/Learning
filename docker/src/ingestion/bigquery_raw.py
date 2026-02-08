import json
import tempfile
from datetime import datetime, timezone
from typing import Iterable, Dict, Any

from google.cloud import bigquery

from shared.bigquery_client import get_bigquery_client


def upload_weather_raw(
    payload:Dict[str,Any],
    project_id:str,
    batch_id:str,
    dataset_id:str="raw",
    table_id:str="weather",
    source_system:str="weather_api",
) -> None:
    client = get_bigquery_client(project_id)
    table_ref = f"{project_id}.{dataset_id}.{table_id}"

    ingestion_ts = datetime.now(timezone.utc)
    raw_date = ingestion_ts.date().isoformat()
    row = {
        "ingestion_ts":ingestion_ts.isoformat(),
        "raw_date":raw_date,
        "source_system":source_system,
        "batch_id":batch_id,
        "payload":payload
    }

    

    with tempfile.NamedTemporaryFile(mode='w',suffix='json',delete=False)as file:
        file.write(json.dumps(row)+"\n")
        temp_file_path = file.name


    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
    )

    with open(temp_file_path, "rb") as source_file:
        load_job = client.load_table_from_file(
            source_file,
            table_ref,
            job_config=job_config,
        )

    load_job.result()  