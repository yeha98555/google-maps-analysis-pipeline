# Airflow


## Usage
1. Copy `gcp_keyfile.json` to `airflow/`
2. Copy `.env` to `airflow/`
3. Run `docker-compose up`
4. Navigate to `localhost:8080`


## Example

1. Upload file to GCS, and create bigquery external table, see [data_ingestion_gcs_dag.py](./dags/data_ingestion_gcs_dag.py)
