import os
from datetime import datetime, timedelta
from functools import partial

import pandas as pd
from google.cloud import bigquery, storage
from google.oauth2 import service_account
from googleapiclient.discovery import build
from utils.email_callback import failure_callback, info_gsheet_callback
from utils.gcp import download_df_from_gcs, upload_df_to_gcs

from airflow.decorators import dag, task

RAW_BUCKET = os.environ.get("GCP_GCS_RAW_BUCKET")
PROCESSED_BUCKET = os.environ.get("GCP_GCS_PROCESSED_BUCKET")
BQ_ODS_DATASET = os.environ.get("BIGQUERY_ODS_DATASET")
SRC_BLOB_NAME = "tripadvisor/src_tripadvisor.csv"
PROCESSED_BLOB_NAME = "tripadvisor/processed_tripadvisor_info.parquet"
GCS_CLIENT = storage.Client()
BQ_CLIENT = bigquery.Client()

SERVICE_ACCOUNT_FILE = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")
scopes = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]
credentials = service_account.Credentials.from_service_account_file(
    SERVICE_ACCOUNT_FILE, scopes=scopes
)
service = build("sheets", "v4", credentials=credentials)

spreadsheet_id = os.environ.get("GSHEET_TRIPADVISOR_SPREADSHEET_ID")
info_gsheet_callback_with_url = partial(
    info_gsheet_callback,
    url=f"https://docs.google.com/spreadsheets/d/{spreadsheet_id}/edit#gid=0",
)

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
def d_tripadvisor_src_to_ods():
    @task
    def e_download_tripadvisor_from_gcs(
        bucket_name: str, blob_name: str
    ) -> pd.DataFrame:
        return download_df_from_gcs(
            client=GCS_CLIENT,
            bucket_name=bucket_name,
            blob_name=blob_name,
            filetype="csv",
        )

    @task
    def t_remove_tripadvisor_unnamed_column(df: pd.DataFrame) -> pd.DataFrame:
        # Assuming the unnamed column follows the pattern 'Unnamed: X' where X is a number
        df = df.loc[:, ~df.columns.str.contains("^Unnamed")]
        return df

    @task
    def t_rename_tripadvisor_columns(df: pd.DataFrame) -> pd.DataFrame:
        df.rename(
            columns={
                "景點介紹": "info",
                "景點照": "photo",
                "景點名": "attraction_name",
                "評分": "rating",
                "評論數": "total_reviews",
                "類型": "categories",
            },
            inplace=True,
        )
        return df

    @task
    def t_remove_tripadvisor_null_rows(df: pd.DataFrame) -> pd.DataFrame:
        return df.dropna(how="all")

    @task
    def t_remove_tripadvisor_duplicate_rows(df: pd.DataFrame) -> pd.DataFrame:
        return df.drop_duplicates(subset=["attraction_name", "rating"], keep="first")

    @task
    def t_rename_tripadvisor_rating_values(df: pd.DataFrame) -> pd.DataFrame:
        # rating to float, if can't convert, set NaN
        df["rating"] = pd.to_numeric(
            df["rating"].str.split("分 ").str[0], errors="coerce"
        )
        # remove rows with NaN rating
        df = df.dropna(subset=["rating"])
        return df

    @task
    def t_remove_tripadvisor_total_reviews_values(df: pd.DataFrame) -> pd.DataFrame:
        df["total_reviews"] = pd.to_numeric(df["total_reviews"], errors="coerce")
        df = df.dropna(subset=["total_reviews"])
        return df

    @task
    def t_convert_tripadvisor_categories_to_list(df: pd.DataFrame) -> pd.DataFrame:
        # if type is not str, drop the row
        df = df[df["categories"].apply(lambda x: isinstance(x, str))]

        df["categories"] = df["categories"].apply(
            lambda x: x.replace(" • ", ",") if " • " in x else x
        )
        return df

    @task
    def l_upload_tripadvisor_to_gcs(df: pd.DataFrame, bucket_name: str, blob_name: str):
        upload_df_to_gcs(
            client=GCS_CLIENT,
            bucket_name=bucket_name,
            blob_name=blob_name,
            df=df,
        )

    @task(on_success_callback=info_gsheet_callback_with_url)
    def l_df_to_gsheet(service, spreadsheet_id: str, df: pd.DataFrame):
        df.sort_values(by="total_reviews", ascending=False, inplace=True)
        df = df.fillna("")  # NaN is invalid for GSheet

        body = {"values": [df.columns.tolist()] + df.values.tolist()}
        range_name = "Sheet1!A1"
        result = (
            service.spreadsheets()
            .values()
            .update(
                spreadsheetId=spreadsheet_id,
                range=range_name,
                valueInputOption="RAW",
                body=body,
            )
            .execute()
        )

        print(f"{result.get('updatedCells')} cells updated.")

    @task
    def l_create_tripadvisor_bq_external_table(
        dataset_name: str, table_name: str, bucket_name: str, blob_name: str
    ):
        # FIXME: build_bq_from_gsheet
        pass

    t1 = e_download_tripadvisor_from_gcs(
        bucket_name=RAW_BUCKET,
        blob_name=SRC_BLOB_NAME,
    )
    t2 = t_remove_tripadvisor_unnamed_column(t1)
    t3 = t_rename_tripadvisor_columns(t2)
    t4 = t_remove_tripadvisor_null_rows(t3)
    t5 = t_remove_tripadvisor_duplicate_rows(t4)
    t6 = t_rename_tripadvisor_rating_values(t5)
    t7 = t_remove_tripadvisor_total_reviews_values(t6)
    t8 = t_convert_tripadvisor_categories_to_list(t7)

    l_df_to_gsheet(
        service=service,
        spreadsheet_id=spreadsheet_id,
        df=t8,
    ) >> l_create_tripadvisor_bq_external_table(
        dataset_name=BQ_ODS_DATASET,
        table_name="ods-tripadvisor",
        bucket_name=PROCESSED_BUCKET,
        blob_name=PROCESSED_BLOB_NAME,
    )
    l_upload_tripadvisor_to_gcs(
        t8,
        PROCESSED_BUCKET,
        PROCESSED_BLOB_NAME,
    )


d_tripadvisor_src_to_ods()
