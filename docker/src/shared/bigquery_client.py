from typing import Optional
from google.cloud import bigquery


def get_bigquery_client(project_id: Optional[str] = None) -> bigquery.Client:
    if project_id:
        return bigquery.Client(project=project_id)
    return bigquery.Client()