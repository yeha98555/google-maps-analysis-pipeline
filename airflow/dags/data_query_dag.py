import os

import pandas as pd
from utils.gcp import (
    build_bq_from_gcs,
    download_df_from_gcs,
    upload_df_to_gcs,
)

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago

RAW_BUCKET = os.environ.get("GCP_GCS_RAW_BUCKET")
PROCESSED_BUCKET = os.environ.get("GCP_GCS_PROCESSED_BUCKET")
TABLE_NAME = "yellow_taxi"
BLOB_NAME = f"{TABLE_NAME}/yellow_tripdata_2021-05.parquet"
BQ_SRC_DATASET = os.environ.get("BIGQUERY_SRC_DATASET")
BQ_ODS_DATASET = os.environ.get("BIGQUERY_ODS_DATASET")
BQ_DIM_DATASET = os.environ.get("BIGQUERY_DIM_DATASET")

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": days_ago(1),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
}


def transform_data(df: pd.DataFrame) -> pd.DataFrame:
    return df


dag = DAG(
    "data_query_dag",
    default_args=default_args,
    description="A DAG to process data, create BigQuery table, and perform table operations",
    schedule_interval="@daily",
)

with dag:
    download_data_task = PythonOperator(
        task_id="download_data_task",
        python_callable=download_df_from_gcs,
        op_kwargs={
            "bucket_name": RAW_BUCKET,
            "blob_name": BLOB_NAME,
        },
    )

    transform_data_task = PythonOperator(
        task_id="transform_data_task",
        python_callable=transform_data,
        op_args=[download_data_task.output],
    )

    upload_transformed_data_task = PythonOperator(
        task_id="upload_transformed_data_task",
        python_callable=upload_df_to_gcs,
        op_kwargs={
            "bucket_name": PROCESSED_BUCKET,
            "blob_name": f"{TABLE_NAME}_processed",
            "df": transform_data_task.output,
        },
    )

    create_bq_external_table_task = PythonOperator(
        task_id="create_bq_external_table_task",
        python_callable=build_bq_from_gcs,
        op_kwargs={
            "dataset_name": BQ_ODS_DATASET,
            "table_name": TABLE_NAME,
            "bucket_name": PROCESSED_BUCKET,
            "blob_name": f"{TABLE_NAME}_processed",
        },
    )

    # 從GCS下載成pd.DataFrame，使用pandas做一些資料處理，再次上傳到GCS，最後建立BigQuery的Exteral Table
    (
        download_data_task
        >> transform_data_task
        >> upload_transformed_data_task
        >> create_bq_external_table_task
    )
