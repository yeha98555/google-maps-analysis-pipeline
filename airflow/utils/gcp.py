from io import BytesIO
from typing import List

import pandas as pd
from google.cloud import bigquery, storage
from google.cloud.exceptions import NotFound

# WORKAROUND to prevent timeout for files > 6 MB on 800 kbps upload speed.
# (Ref: https://github.com/googleapis/python-storage/issues/74)
storage.blob._MAX_MULTIPART_SIZE = 1024 * 1024  # 1 MB
storage.blob._DEFAULT_CHUNKSIZE = 1024 * 1024  # 1 MB
# End of Workaround


def upload_df_to_gcs(bucket_name: str, blob_name: str, df: pd.DataFrame) -> bool:
    """
    Upload a pandas dataframe to GCS

    Args:
        bucket_name (str): The name of the bucket to upload to
        blob_name (str): The name of the blob to upload to
        df (pd.DataFrame): The dataframe to upload

    Returns:
        bool: True if the upload was successful, False otherwise
    """
    client = storage.Client()
    bucket = client.bucket(bucket_name)

    blob = bucket.blob(blob_name)
    if blob.exists():
        print("File already exists in GCP.")
        return False
    try:
        blob.upload_from_string(
            df.to_parquet(), content_type="application/octet-stream"
        )
        print("Upload successful.")
        return True
    except Exception as e:
        raise Exception(f"Failed to upload pd.DataFrame to GCS, reason: {e}")


def upload_file_to_gcs(bucket_name: str, blob_name: str, source_filepath: str) -> bool:
    """
    Upload a file to GCS

    Args:
        bucket_name (str): The name of the bucket to upload to
        blob_name (str): The name of the blob to upload to
        source_filepath (str): The path to the file to upload

    Returns:
        bool: True if the upload was successful, False otherwise
    """
    client = storage.Client()
    bucket = client.bucket(bucket_name)

    blob = bucket.blob(blob_name)
    if blob.exists():
        print("File already exists.")
        return False
    try:
        blob.upload_from_filename(source_filepath)
        print("Upload successful.")
        return True
    except Exception as e:
        raise Exception(f"Failed to upload file to GCS, reason: {e}")


def download_df_from_gcs(bucket_name: str, blob_name: str) -> pd.DataFrame:
    """
    Download a pandas dataframe from GCS

    Args:
        bucket_name (str): The name of the bucket to download from
        blob_name (str): The name of the blob to download from

    Returns:
        pd.DataFrame: The dataframe downloaded from GCS
    """
    client = storage.Client()
    bucket = client.bucket(bucket_name)

    blob = bucket.blob(blob_name)
    if not blob.exists():
        raise FileNotFoundError(f"file {blob_name} not found in bucket {bucket_name}")

    bytes_data = blob.download_as_bytes()
    return pd.read_parquet(BytesIO(bytes_data))


def build_bq_from_gcs(
    dataset_name: str,
    table_name: str,
    bucket_name: str,
    blob_name: str,
    schema: List[bigquery.SchemaField] = None,
) -> bool:
    """
    Build a bigquery external table from a parquet file in GCS.

    Args:
        dataset_name (str): The name of the dataset to create.
        table_name (str): The name of the table to create.
        bucket_name (str): The name of the bucket to upload to.
        blob_name (str): The name of the blob to upload to.
        schema (List[bigquery.SchemaField], optional): The schema of the table to upload to. Default is None.
                                                        If None, use the default schema (automatic-detect).

    Returns:
        bool: True if the upload was successful, False otherwise.
    """
    client = bigquery.Client()

    # Construct the fully-qualified BigQuery table ID
    table_id = f"{client.project}.{dataset_name}.{table_name}"

    try:
        client.get_table(table_id)  # Attempt to get the table
        print(f"Table {table_id} already exists.")
        return False
    except NotFound:
        # Define the external data source configuration
        external_config = bigquery.ExternalConfig("PARQUET")
        external_config.source_uris = [f"gs://{bucket_name}/{blob_name}"]
        if schema:
            external_config.schema = schema
        # Create a table with the external data source configuration
        table = bigquery.Table(table_id)
        table.external_data_configuration = external_config

        try:
            client.create_table(table)  # API request to create the external table
            print(f"External table {table.table_id} created.")
            return True
        except Exception as e:
            raise Exception(f"Failed to create external table, reason: {e}")
    except Exception as e:
        raise Exception(f"An error occurred while checking if the table exists: {e}")


def query_bq(sql_query: str) -> bigquery.QueryJob:
    """
    Query bigquery and return results. (可以用在bigquery指令，例如Insert、Update，但沒有要取得資料表的資料)

    Args:
        sql_query (str): The SQL query to execute.

    Returns:
        bigquery.QueryJob: The result of the query.
    """
    client = bigquery.Client()

    try:
        query_job = client.query(sql_query)
        return query_job.result()  # Return the results for further processing
    except Exception as e:
        raise Exception(f"Failed to query bigquery table, reason: {e}")


def query_bq_to_df(sql_query: str) -> pd.DataFrame():
    """
    Executes a BigQuery SQL query and directly loads the results into a DataFrame
    using the BigQuery Storage API.  (可以用在bigquery指令，然後取得資料表的資料成為DataFrame)

    Args:
        query (str): SQL query string.

    Returns:
        pd.DataFrame: The query results as a Pandas DataFrame.
    """
    client = bigquery.Client()

    df = pd.read_gbq(
        sql_query, project_id=client.project, dialect="standard", use_bqstorage_api=True
    )
    return df


def upload_df_to_bq(
    df: pd.DataFrame,
    dataset_name: str,
    table_name: str,
    schema: List[bigquery.SchemaField] = None,
) -> bool:
    """
    Upload a pandas dataframe to bigquery

    Args:
        df (pd.DataFrame): The dataframe to upload.
        dataset_name (str): The name of the dataset to upload to.
        table_name (str): The name of the table to upload to.
        schema (List[bigquery.SchemaField], optional): The schema of the table to upload to. Default is None.
                                                        If None, use the default schema (automatic-detect).

    Returns:
        bool: True if the upload was successful, False otherwise.
    """
    client = bigquery.Client()

    dataset_id = client.dataset(dataset_name)
    table_id = dataset_id.table(table_name)

    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.PARQUET,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
    )
    if schema:
        job_config.schema = schema

    try:
        job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
        job.result()  # Wait for the job to complete
        table = client.get_table(table_id)
        print(f"Table {table.table_id} created with {table.num_rows} rows.")
        return True
    except Exception as e:
        raise Exception(f"Failed to upload df to bigquery, reason: {e}")
