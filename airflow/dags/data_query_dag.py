import os

import pandas as pd
from utils.gcp import (
    build_bq_from_gcs,
    download_df_from_gcs,
    query_bq,
    query_bq_to_df,
    upload_df_to_bq,
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
    """
    Transform data (drop duplicates, drop na, drop columns)

    Args:
        df (pd.DataFrame): input dataframe.

    Returns:
        pd.DataFrame: transformed dataframe.
    """
    # 使用python做一些轉換
    df.drop_duplicates(inplace=True)
    df.dropna(inplace=True)
    df.drop(
        columns=[
            "airport_fee",
            "congestion_surcharge",
            "improvement_surcharge",
            "extra",
            "mta_tax",
            "store_and_fwd_flag",
        ],
        inplace=True,
    )
    df.reset_index(drop=True, inplace=True)
    return df[df["fare_amount"] < 10]


def lookup_data(dataset_name, table_name):
    """
    Lookup data from bigquery.

    Args:
        dataset_name (str): dataset name.
        table_name (str): table name.
    """
    query = f"""
    SELECT *
    FROM `{dataset_name}.{table_name}` WHERE fare_amount = 1
    """
    results = query_bq(query)
    for row in results:
        print(row)


def join_data(dataset_name: str) -> pd.DataFrame:
    """
    Join data from bigquery.

    Args:
        dataset_name (str): dataset name.

    Returns:
        pd.DataFrame: joined dataframe.
    """
    # 這裡使用兩個相同的資料表名稱, 只是示範，應該是要兩個不同的資料表名稱操作才有意義
    table1_name = TABLE_NAME
    table2_name = TABLE_NAME
    common_column = "VendorID"

    query = f"""
    SELECT t1.tpep_pickup_datetime, t1.trip_distance, t1.fare_amount,
    FROM `{dataset_name}.{table1_name}` AS t1
    INNER JOIN `{dataset_name}.{table2_name}` AS t2
    ON t1.{common_column} = t2.{common_column}
    WHERE t1.fare_amount = 1
    """
    df = query_bq_to_df(query)
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

    lookup_bq_table_task = PythonOperator(
        task_id="lookup_bq_table_task",
        python_callable=lookup_data,
        op_kwargs={
            "dataset_name": BQ_ODS_DATASET,
            "table_name": TABLE_NAME,
        },
    )

    join_data_task = PythonOperator(
        task_id="join_data_task",
        python_callable=join_data,
        op_kwargs={
            "dataset_name": BQ_ODS_DATASET,
        },
    )

    upload_to_bigquery_task = PythonOperator(
        task_id="upload_to_bigquery_task",
        python_callable=upload_df_to_bq,
        op_kwargs={
            "dataset_name": BQ_DIM_DATASET,
            "table_name": TABLE_NAME,
            "df": join_data_task.output,
        },
    )

    # 從GCS下載成pd.DataFrame，使用pandas做一些資料處理，再次上傳到GCS，最後建立BigQuery的Exteral Table
    (
        download_data_task
        >> transform_data_task
        >> upload_transformed_data_task
        >> create_bq_external_table_task
    )

    # 查詢BigQuery的Exteral Table
    create_bq_external_table_task >> lookup_bq_table_task

    # 對兩個BigQuery的Exteral Table做join，最後上傳到BigQuery
    create_bq_external_table_task >> join_data_task >> upload_to_bigquery_task
