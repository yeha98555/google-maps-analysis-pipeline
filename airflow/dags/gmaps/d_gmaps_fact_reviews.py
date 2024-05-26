import os
from datetime import datetime, timedelta

import pandas as pd
from google.cloud import bigquery
from utils.gcp import query_bq

from airflow.decorators import dag, task

BQ_ODS_DATASET = os.environ.get("BIGQUERY_ODS_DATASET")
BQ_FACT_DATASET = os.environ.get("BIGQUERY_FACT_DATASET")
ODS_TABLE_NAME = "ods-gmaps_reviews"
FACT_TABLE_NAME = "fact-reviews"
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
def d_gmaps_fact_reviews():
    @task
    def etl_load_reviews(
        src_dataset: str, src_table: str, dest_dataset: str, dest_table: str
    ) -> pd.DataFrame:
        query = f"""
        CREATE OR REPLACE TABLE `{dest_dataset}.{dest_table}`
        AS
        SELECT DISTINCT
          `review_id`,
          `place_name`,
          `user_name`,
          `rating`,
          `published_at`,
          `review_text`,
        FROM
          `{src_dataset}`.`{src_table}`
        """
        query_bq(client=BQ_CLIENT, sql_query=query)

    etl_load_reviews(BQ_ODS_DATASET, ODS_TABLE_NAME, BQ_FACT_DATASET, FACT_TABLE_NAME)


d_gmaps_fact_reviews()
