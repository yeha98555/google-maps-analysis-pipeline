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
def d_gmaps_dim_users():
    @task
    def l_dim_users(
        src_dataset: str, src_table: str, dest_dataset: str, dest_table: str
    ):
        query = f"""
        CREATE OR REPLACE TABLE `{dest_dataset}`.`{dest_table}` AS
        SELECT DISTINCT
          `user_name`,
          `user_is_local_guide`,
          `user_url`,
        FROM
          `{src_dataset}`.`{src_table}`
        """
        query_bq(BQ_CLIENT, query)
        return "dim-users created."

    l_dim_users(
        src_dataset=BQ_ODS_DATASET,
        src_table=TABLE_NAME,
        dest_dataset=BQ_DIM_DATASET,
        dest_table="dim-reviews_users",
    )


d_gmaps_dim_users()
