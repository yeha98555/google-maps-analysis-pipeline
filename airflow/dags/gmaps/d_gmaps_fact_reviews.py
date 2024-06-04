import os
from datetime import datetime, timedelta

import pandas as pd
from google.cloud import bigquery
from utils.common import load_config, table_name_with_env
from utils.gcp import query_bq

from airflow.decorators import dag, task

config = load_config()
BQ_ODS_DATASET = config["gcp"]["bigquery"]["ods_dataset"]
BQ_FACT_DATASET = config["gcp"]["bigquery"]["fact_dataset"]
ODS_TABLE_NAME = "ods-" + config["gcp"]["table"]["gmaps-reviews"]
FACT_TABLE_NAME = table_name_with_env("fact-gmaps-reviews", config["env"])

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
    def create_remote_udf() -> str:
        function_name = "analyzer"
        cloudfunctions_url = f"https://us-west1-{os.environ['GCP_PROJECT_ID']}.cloudfunctions.net/{function_name}"
        bq_connection_name = f"{os.environ['GCP_PROJECT_ID']}.us-west1.{function_name}"
        query = f"""
        CREATE OR REPLACE FUNCTION `{BQ_FACT_DATASET}.analyze_sentiment`(text STRING)
        RETURNS STRING
        REMOTE WITH CONNECTION `{bq_connection_name}`
        OPTIONS (
          endpoint = '{cloudfunctions_url}'
        );
        """
        query_bq(client=BQ_CLIENT, sql_query=query)
        return "Remote UDF created."
    
    @task
    def etl_add_emtion_and_create_fact_reviews() -> pd.DataFrame:
        # Step 1: Execute the WITH clause query and store results in a temporary table
        temp_table_name = f"{BQ_FACT_DATASET}.temp_sentiment_analysis"
        query_step_1 = f"""
        CREATE OR REPLACE TABLE `{temp_table_name}` AS
        WITH sentiment_analysis AS (
          SELECT
            `review_id`,
            `place_name`,
            `user_name`,
            `rating`,
            `published_at`,
            `review_text`,
            `{BQ_FACT_DATASET}.analyze_sentiment`(`review_text`) AS sentiment_json
          FROM
            `{BQ_ODS_DATASET}.{ODS_TABLE_NAME}`
        )
        SELECT * FROM `sentiment_analysis`
        """
        query_bq(client=BQ_CLIENT, sql_query=query_step_1)

        # Step 2: Create or replace the target table using the temporary table
        query_step_2 = f"""
        CREATE OR REPLACE TABLE `{BQ_FACT_DATASET}.{FACT_TABLE_NAME}`
        PARTITION BY `published_at`
        AS
        SELECT DISTINCT
            `review_id`,
            `place_name`,
            `user_name`,
            `rating`,
            `published_at`,
            `review_text`,
            ROUND(CAST(JSON_EXTRACT(`sentiment_json`, '$.score') AS FLOAT64), 2) AS emotion_score
        FROM
          `{temp_table_name}`
        """
        query_bq(client=BQ_CLIENT, sql_query=query_step_2)

        return f"{FACT_TABLE_NAME} created."

    create_remote_udf() >> etl_add_emtion_and_create_fact_reviews()


d_gmaps_fact_reviews()
