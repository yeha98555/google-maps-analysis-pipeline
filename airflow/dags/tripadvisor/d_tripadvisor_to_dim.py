# processed_tripadvisor_info.parquet -> google sheet
# google sheet -> external table to ods-tripadvisor


import os
from datetime import datetime, timedelta

import pandas as pd
from google.cloud import bigquery
from google.oauth2 import service_account
from utils.common import rename_place_id
from utils.email_callback import failure_callback
from utils.gcp import query_bq_to_df, upload_df_to_bq

from airflow.decorators import dag, task

BQ_ODS_DATASET = os.environ.get("BIGQUERY_ODS_DATASET")
BQ_DIM_DATASET = os.environ.get("BIGQUERY_DIM_DATASET")
ODS_TABLE_NAME = "ods-tripadvisor"
DIM_TABLE_NAME = "dim-tripadvisor"
SERVICE_ACCOUNT_FILE = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")
scopes = [
    "https://www.googleapis.com/auth/drive",
    "https://www.googleapis.com/auth/bigquery",
]
credentials = service_account.Credentials.from_service_account_file(
    SERVICE_ACCOUNT_FILE, scopes=scopes
)
BQ_CLIENT = bigquery.Client(credentials=credentials)

default_args = {
    "owner": "airflow",
    "start_date": datetime(2024, 5, 1),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "on_failure_callback": failure_callback,
}


@dag(
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=["tripadvisor"],
)
def d_tripadvisor_to_dim():
    @task
    def e_download_tripadvisor_from_bq() -> pd.DataFrame:
        query = f"""
        SELECT * FROM `{BQ_ODS_DATASET}.{ODS_TABLE_NAME}`
        """
        df = query_bq_to_df(BQ_CLIENT, query)
        return df
    
    @task
    def t_remove_tripadvisor_unused_columns(df: pd.DataFrame) -> pd.DataFrame:
        df = df.drop(columns=["info", "photo", "rating", "categories"])
        return df
    
    @task
    def t_add_attraction_id(df: pd.DataFrame) -> pd.DataFrame:
        df["attraction_id"] = df["attraction_name"].apply(
            lambda x: rename_place_id(str(x))
        )
        return df

    @task
    def l_upload_df_to_dim_tripadvisor(df: pd.DataFrame):
        upload_df_to_bq(
            client=BQ_CLIENT,
            df=df,
            dataset_name=BQ_DIM_DATASET,
            table_name=DIM_TABLE_NAME,
            schema=[
                bigquery.SchemaField("attraction_id", "STRING", mode="REQUIRED"),
                bigquery.SchemaField("attraction_name", "STRING", mode="REQUIRED"),
                bigquery.SchemaField("total_reviews", "INTEGER"),
            ],
        )
        return f"{DIM_TABLE_NAME} created."


    df = e_download_tripadvisor_from_bq()
    df_trimed = t_remove_tripadvisor_unused_columns(df)
    df_with_id = t_add_attraction_id(df_trimed)
    l_upload_df_to_dim_tripadvisor(df_with_id)

d_tripadvisor_to_dim()
