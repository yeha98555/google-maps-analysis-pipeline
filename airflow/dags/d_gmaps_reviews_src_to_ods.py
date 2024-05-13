import os
from datetime import datetime, timedelta

import pandas as pd
from dateutil.relativedelta import relativedelta
from google.cloud import bigquery, storage
from utils.common import rename_place_id
from utils.gcp import build_bq_from_gcs, query_bq_to_df, upload_df_to_bq

from airflow.decorators import dag, task

RAW_BUCKET = os.environ.get("GCP_GCS_RAW_BUCKET")
SRC_BLOB_NAME = "gmaps/detailed-reviews/*.parquet"
BQ_SRC_DATASET = os.environ.get("BIGQUERY_SRC_DATASET")
BQ_ODS_DATASET = os.environ.get("BIGQUERY_ODS_DATASET")
TABLE_NAME = "ods-gmaps_reviews"
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
    tags=["gmaps"],
)
def d_gmaps_reviews_src_to_ods():
    @task
    def e_create_external_table_with_partition(
        bucket_name: str, blob_name: str, dataset_name: str, table_name: str
    ):
        build_bq_from_gcs(
            client=BQ_CLIENT,
            dataset_name=dataset_name,
            table_name=table_name,
            bucket_name=bucket_name,
            blob_name=blob_name,
        )

    @task
    def t_remove_unused_columns(bq_src_dataset: str, table_name: str) -> pd.DataFrame:
        query = f"""
        SELECT
          `place_id`,
          `place_name`,
          `review_id`,
          `rating`,
          `review_text`,
          `published_at`,
          `user_name`,
          `user_is_local_guide`,
          `user_url`,
          `extracted_at`
        FROM `{bq_src_dataset}`.`{table_name}`
        """
        return query_bq_to_df(BQ_CLIENT, query)

    @task
    def t_remove_duplicate_reviews(df: pd.DataFrame) -> pd.DataFrame:
        return df.drop_duplicates(subset=["review_id"])

    @task
    def t_convert_reveiws_published_datetime(df: pd.DataFrame) -> pd.DataFrame:
        def convert_datetime(date_str: str, extract_datetime: datetime) -> str:
            """
            Convert the date format from relative date strings to absolute date strings.
            """
            if "小時前" in date_str:
                hours = int(date_str.split("小時前")[0])
                new_date = extract_datetime - relativedelta(hours=hours)
            elif "天前" in date_str:
                days = int(date_str.split("天前")[0])
                new_date = extract_datetime - relativedelta(days=days)
            elif "週前" in date_str:
                weeks = int(date_str.split("週前")[0])
                new_date = extract_datetime - relativedelta(weeks=weeks)
            elif "個月前" in date_str:
                months = int(date_str.split("個月前")[0])
                new_date = extract_datetime - relativedelta(months=months)
            elif "年前" in date_str:
                years = int(date_str.split("年前")[0])
                new_date = extract_datetime - relativedelta(years=years)
            else:  # Unknown
                return pd.NaT
            return new_date.date()

        df["extracted_at"] = pd.to_datetime(df["extracted_at"])
        df["published_at"] = df.apply(
            lambda x: convert_datetime(x["published_at"], x["extracted_at"]), axis=1
        )
        return df

    @task
    def t_convert_place_id(df: pd.DataFrame) -> pd.DataFrame:
        df["place_id"] = df["place_name"].apply(lambda x: rename_place_id(x))
        return df

    @task
    def l_upload_transformed_reviews_to_bq(
        df: pd.DataFrame, dataset_name: str, table_name: str
    ):
        upload_df_to_bq(
            client=BQ_CLIENT,
            dataset_name=dataset_name,
            table_name=table_name,
            df=df,
            partition_by="published_at",
            schema=[
                bigquery.SchemaField("place_id", "STRING", mode="REQUIRED"),
                bigquery.SchemaField("place_name", "STRING", mode="REQUIRED"),
                bigquery.SchemaField("review_id", "STRING", mode="REQUIRED"),
                bigquery.SchemaField("rating", "INTEGER"),
                bigquery.SchemaField("review_text", "STRING"),
                bigquery.SchemaField("published_at", "DATE"),
                bigquery.SchemaField("user_name", "STRING"),
                bigquery.SchemaField("user_is_local_guide", "BOOLEAN"),
                bigquery.SchemaField("user_url", "STRING"),
                bigquery.SchemaField("extracted_at", "TIMESTAMP"),
            ],
        )

    t1 = e_create_external_table_with_partition(
        RAW_BUCKET, SRC_BLOB_NAME, BQ_SRC_DATASET, TABLE_NAME
    )
    t2 = t_remove_unused_columns(BQ_SRC_DATASET, TABLE_NAME)
    t3 = t_remove_duplicate_reviews(t2)
    t4 = t_convert_reveiws_published_datetime(t3)
    t5 = t_convert_place_id(t4)
    t6 = l_upload_transformed_reviews_to_bq(t5, BQ_ODS_DATASET, TABLE_NAME)

    t2.set_upstream(t1)
    t3.set_upstream(t2)
    t4.set_upstream(t3)
    t5.set_upstream(t4)
    t6.set_upstream(t5)


d_gmaps_reviews_src_to_ods()
