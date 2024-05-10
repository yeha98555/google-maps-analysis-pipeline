from google.cloud import bigquery, storage

# WORKAROUND to prevent timeout for files > 6 MB on 800 kbps upload speed.
# (Ref: https://github.com/googleapis/python-storage/issues/74)
storage.blob._MAX_MULTIPART_SIZE = 1024 * 1024  # 1 MB
storage.blob._DEFAULT_CHUNKSIZE = 1024 * 1024  # 1 MB
# End of Workaround


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

    # bigquery
    dataset_id = client.dataset(dataset_name)
    table_id = dataset_id.table(table_name)
    # gcs
    source_uri = f"gs://{bucket_name}/{blob_name}"

    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.PARQUET,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
    )

    try:
        job = client.load_table_from_uri(source_uri, table_id, job_config=job_config)
        job.result()  # Wait for the job to complete
        table = client.get_table(table_id)
        print(f"Table {table.table_id} created with {table.num_rows} rows.")
        return True
    except Exception as e:
        raise Exception(f"Failed to build bigquery external table, reason: {e}")
