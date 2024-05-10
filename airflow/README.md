# Airflow


## Usage
1. Copy `gcp_keyfile.json` to `airflow/`
2. Copy `.env` to `airflow/`
3. Run
```sh
docker-compose build
docker-compose up
```
4. Navigate to `localhost:8080`


## Example

1. [data_ingestion_gcs_dag.py](./dags/data_ingestion_gcs_dag.py): upload file to GCS, and create bigquery external table.

2. [data_query_dag.py](./dags/data_query_dag.py): simple data pipeline.
- download data from gcs, and transform data, then upload to gcs and create external table.
- query bigquery and create new table.
