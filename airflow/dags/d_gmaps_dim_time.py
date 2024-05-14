import os
from datetime import datetime, timedelta

from google.cloud import bigquery
from utils.gcp import query_bq

from airflow.decorators import dag, task

BQ_ODS_DATASET = os.environ.get("BIGQUERY_ODS_DATASET")
BQ_DIM_DATASET = os.environ.get("BIGQUERY_DIM_DATASET")
TABLE_NAME = "ods-gmaps_reviews"
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
    tags=["gmaps"],
)
def d_gmaps_dim_time():
    @task
    def l_dim_time(
        src_dataset: str, src_table: str, dest_dataset: str, dest_table: str
    ):
        query = f"""
        CREATE OR REPLACE TABLE `{dest_dataset}`.`{dest_table}` AS
        SELECT
          DISTINCT
          published_at AS date,
          EXTRACT(YEAR FROM published_at) AS year,
          EXTRACT(MONTH FROM published_at) AS month,
          EXTRACT(DAY FROM published_at) AS day,
          CASE
            WHEN EXTRACT(MONTH FROM published_at) IN (1, 2, 3) THEN 1
            WHEN EXTRACT(MONTH FROM published_at) IN (4, 5, 6) THEN 2
            WHEN EXTRACT(MONTH FROM published_at) IN (7, 8, 9) THEN 3
            WHEN EXTRACT(MONTH FROM published_at) IN (10, 11, 12) THEN 4
          END AS quarter
        FROM
          `{src_dataset}`.`{src_table}`
        """
        query_bq(BQ_CLIENT, query)
        return "dim-time created."

    l_dim_time(
        src_dataset=BQ_ODS_DATASET,
        src_table=TABLE_NAME,
        dest_dataset=BQ_DIM_DATASET,
        dest_table="dim-reviews_time",
    )


d_gmaps_dim_time()
