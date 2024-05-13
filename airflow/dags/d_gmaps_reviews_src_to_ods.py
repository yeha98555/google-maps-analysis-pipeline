import os
from datetime import datetime, timedelta

from google.cloud import bigquery, storage

from airflow.decorators import dag, task

RAW_BUCKET = os.environ.get("GCP_GCS_RAW_BUCKET")
BLOB_NAME = "gmaps_reviews"
BQ_SRC_DATASET = os.environ.get("BIGQUERY_SRC_DATASET")
TABLE_NAME = "ods_gmaps_reviews"
GCS_CLIENT = storage.Client()
BQ_CLIENT = bigquery.Client()

default_args = {
    "owner": "airflow",
    "start_date": datetime(2024, 5, 1),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


@dag(
    default_args=default_args,
    schedule_interval=None,  # Because this is triggered by another DAG
    catchup=False,
    tags=["gmaps_reviews"],
)
def d_gmaps_reviews_src_to_ods():
    @task
    def test_dags():
        print("hello")

    test_dags()


d_gmaps_reviews_src_to_ods()
