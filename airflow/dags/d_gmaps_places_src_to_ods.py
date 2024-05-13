import hashlib
import os
from datetime import datetime, timedelta

import pandas as pd
from google.cloud import bigquery, storage
from utils.gcp import build_bq_from_gcs, download_df_from_gcs, upload_df_to_gcs

from airflow.decorators import dag, task

RAW_BUCKET = os.environ.get("GCP_GCS_RAW_BUCKET")
PROCESSED_BUCKET = os.environ.get("GCP_GCS_PROCESSED_BUCKET")
ARCHIVE_BUCKET = os.environ.get("GCP_GCS_ARCHIVE_BUCKET")
BLOB_NAME = "gmaps/places"
BQ_ODS_DATASET = os.environ.get("BIGQUERY_ODS_DATASET")
ODS_TABLE_NAME = "ods-gmaps_places"
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
def d_gmaps_places_src_to_ods():
    @task
    def t_get_latest_places_blobname(bucket_name: str, blob_prefix: str) -> str:
        blobs = storage.Client().list_blobs(bucket_name, prefix=blob_prefix)
        blobs = sorted(blobs, key=lambda x: x.name)
        print(f"blob name: {blobs[-1].name}")
        return blobs[-1].name

    @task
    def t_get_places_df_from_gcs(bucket_name: str, blob_name: str) -> pd.DataFrame:
        return download_df_from_gcs(
            client=GCS_CLIENT, bucket_name=bucket_name, blob_name=blob_name
        )

    # @task
    # def t_get_latset_and_second_latest_places_file(
    #     bucket_name: str, blob_prefix: str
    # ) -> Tuple[str, str]:
    #     bucket = GCS_CLIENT.get_bucket(bucket_name)
    #     blobs = bucket.list_blobs(prefix=blob_prefix)

    #     blobs = sorted(blobs, key=lambda x: x.name)
    #     return blobs[-1], blobs[-2]

    # @task
    # def t_compare_places_row_number(
    #     filename_latest: str, filename_second_latest: str
    # ) -> pd.DataFrame:
    #     print(filename_latest, filename_second_latest)
    #     df_latest = pd.read_csv(filename_latest)
    #     df_second_latest = pd.read_csv(filename_second_latest)

    #     # 確認最新檔案和第二新的檔案筆數是否相同，如果相同，刪除舊的，如果不相同，保留兩個，回傳筆數多的
    #     if df_latest.shape[0] == df_second_latest.shape[0]:
    #         # 搬移舊有的檔案到archive bucket
    #         t_move_old_places_file_to_archive_bucket(filename_second_latest)
    #         return df_latest
    #     else:
    #         # 如果不相同，保留兩個，回傳筆數多的
    #         print(
    #             f"number of rows in latest and second latest are not the same: {df_latest.shape[0]} and {df_second_latest.shape[0]}"
    #         )
    #         if df_latest.shape[0] > df_second_latest.shape[0]:
    #             return df_latest
    #         else:
    #             return df_second_latest

    # def t_move_old_places_file_to_archive_bucket(
    #     bucket_name: str, source_blob_name: str
    # ):
    #     # 若相同將除了最新以外的該層資料搬移到archive bucket
    #     bucket = GCS_CLIENT.get_bucket(bucket_name)

    #     blob = bucket.blob(source_blob_name)

    #     blob.copy_to(f"{ARCHIVE_BUCKET}/{source_blob_name}")
    #     blob.delete()

    @task
    def t_rename_places_columns(df: pd.DataFrame):
        df.rename(
            columns={
                "place_id": "place_id_raw",
                "name": "place_name",
                "link": "google_place_url",
                "reviews": "total_reviews",
                "rating": "avg_rating",
            },
            inplace=True,
        )
        return df

    @task
    def t_convert_place_id(df: pd.DataFrame):
        # place_id 使用自己設定的編碼規則, 暫時使用md5 hash
        df["place_id"] = df["place_id_raw"].apply(
            lambda x: "place_" + hashlib.md5(x.encode("utf-8")).hexdigest()
        )
        return df

    @task
    def l_upload_transformed_places_to_gcs(
        df: pd.DataFrame, bucket_name: str, blob_name: str
    ):
        upload_df_to_gcs(
            client=GCS_CLIENT,
            bucket_name=bucket_name,
            blob_name=blob_name,
            df=df,
        )

    @task
    def create_bq_external_table_with_partition(
        bucket_name: str, blob_name: str, dataset_name: str, table_name: str
    ):
        build_bq_from_gcs(
            client=BQ_CLIENT,
            bucket_name=bucket_name,
            blob_name=blob_name,
            dataset_name=dataset_name,
            table_name=table_name,
            schema=[
                bigquery.SchemaField("place_id", "STRING", mode="REQUIRED"),
                bigquery.SchemaField("place_id_raw", "STRING", mode="REQUIRED"),
                bigquery.SchemaField("place_name", "STRING", mode="REQUIRED"),
                bigquery.SchemaField("main_category", "STRING", mode="NULLABLE"),
                # bigquery.SchemaField("categories", "STRING", mode="REPEATED"),
                bigquery.SchemaField("google_place_url", "STRING", mode="REQUIRED"),
                bigquery.SchemaField("total_reviews", "INTEGER", mode="NULLABLE"),
                bigquery.SchemaField("avg_rating", "FLOAT", mode="NULLABLE"),
                bigquery.SchemaField("address", "STRING", mode="NULLABLE"),
            ],
        )

    latest_blob_name = t_get_latest_places_blobname(RAW_BUCKET, BLOB_NAME)
    df = t_get_places_df_from_gcs(RAW_BUCKET, latest_blob_name)
    df = t_rename_places_columns(df)
    df = t_convert_place_id(df)
    (
        l_upload_transformed_places_to_gcs(df, PROCESSED_BUCKET, f"{BLOB_NAME}.parquet")
        >> create_bq_external_table_with_partition(
            PROCESSED_BUCKET, f"{BLOB_NAME}.parquet", BQ_ODS_DATASET, ODS_TABLE_NAME
        )
    )


d_gmaps_places_src_to_ods()
