import os
from datetime import datetime, timedelta

from google.cloud import bigquery
from utils.gcp import query_bq

from airflow.decorators import dag, task

BQ_ODS_DATASET = os.environ.get("BIGQUERY_ODS_DATASET")
BQ_FACT_DATASET = os.environ.get("BIGQUERY_FACT_DATASET")
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
def d_gmaps_fact_reviews():
    @task
    def l_fact_reviews(
        src_dataset: str, src_table: str, dest_dataset: str, dest_table: str
    ):
        query = f"""
        CREATE OR REPLACE TABLE `{dest_dataset}`.`{dest_table}` AS
        SELECT DISTINCT
          `review_id`,
          `place_id`,
          `user_name`,
          `rating`,
          `published_at`,
        FROM
          `{src_dataset}`.`{src_table}`
        """
        query_bq(BQ_CLIENT, query)
        return "fact-reviews created."

    l_fact_reviews(
        src_dataset=BQ_ODS_DATASET,
        src_table=TABLE_NAME,
        dest_dataset=BQ_FACT_DATASET,
        dest_table="fact-reviews",
    )


d_gmaps_fact_reviews()
