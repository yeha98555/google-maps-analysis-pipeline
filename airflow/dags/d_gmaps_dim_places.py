import os
from datetime import datetime, timedelta

from google.cloud import bigquery
from utils.gcp import query_bq

from airflow.decorators import dag, task

BQ_ODS_DATASET = os.environ.get("BIGQUERY_ODS_DATASET")
BQ_DIM_DATASET = os.environ.get("BIGQUERY_DIM_DATASET")
TABLE_NAME = "ods-gmaps_places"
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
def d_gmaps_dim_places():
    @task
    def l_dim_places(
        src_dataset: str, src_table: str, dest_dataset: str, dest_table: str
    ):
        query = f"""
        CREATE OR REPLACE TABLE `{dest_dataset}`.`{dest_table}` AS
        SELECT DISTINCT
          `place_id`,
          `place_name`,
          `detailed_address`.`country_code` AS `country`,
          REGEXP_EXTRACT(`detailed_address`.`state`, r"^(.*?[市|縣])") AS city,
          REGEXP_EXTRACT(`detailed_address`.`state`, r"(?:[市|縣])(.*)$") AS region,
          `address`,
          `google_place_url`,
          `main_category`,
        FROM
          `{src_dataset}`.`{src_table}`
        """
        query_bq(BQ_CLIENT, query)
        return "dim-places created."

    l_dim_places(
        src_dataset=BQ_ODS_DATASET,
        src_table=TABLE_NAME,
        dest_dataset=BQ_DIM_DATASET,
        dest_table="dim-reviews_places",
    )


d_gmaps_dim_places()
