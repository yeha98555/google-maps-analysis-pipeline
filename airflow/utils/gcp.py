from io import BytesIO

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
    dataset_name: str, table_name: str, bucket_name: str, blob_name: str
) -> bool:
    """
    Build a bigquery external table from a parquet file in GCS

    Args:
        dataset_name (str): The name of the dataset to create
        table_name (str): The name of the table to create
        bucket_name (str): The name of the bucket to upload to
        blob_name (str): The name of the blob to upload to

    Returns:
        bool: True if the upload was successful, False otherwise
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
