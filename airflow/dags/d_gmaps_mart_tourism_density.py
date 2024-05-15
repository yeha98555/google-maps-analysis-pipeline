import os
from datetime import datetime, timedelta

from google.cloud import bigquery
from utils.gcp import query_bq

from airflow.decorators import dag, task

BQ_DIM_DATASET = os.environ.get("BIGQUERY_DIM_DATASET")
BQ_FACT_DATASET = os.environ.get("BIGQUERY_FACT_DATASET")
BQ_MART_DATASET = os.environ.get("BIGQUERY_MART_DATASET")
TABLE_NAME = "mart-tourism_density"
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
def d_gmaps_mart_tourism_density():
    @task
    def l_mart_tourism_density(dest_dataset: str, dest_table: str):
        query = f"""
        CREATE OR REPLACE TABLE `{dest_dataset}`.`{dest_table}` AS
        SELECT
          p.`city`,
          p.`region`,
          COUNT(DISTINCT p.`place_id`) AS `total_places`,
          COUNT(r.`review_id`) AS `total_reviews`,
          ROUND(AVG(r.`rating`), 2) AS `avg_rating`,
        FROM
          `{BQ_FACT_DATASET}`.`fact-reviews` r
        JOIN
          `{BQ_DIM_DATASET}`.`dim-reviews_places` p
          ON r.`place_id` = p.`place_id`
        GROUP BY
          p.`city`,
          p.`region`
        """
        query_bq(BQ_CLIENT, query)
        return f"{dest_table} created."

    l_mart_tourism_density(
        dest_dataset=BQ_MART_DATASET,
        dest_table=TABLE_NAME,
    )


d_gmaps_mart_tourism_density()
