# Airflow


## Usage

### Setup Service Account Key

1. Generate airflow service account credentials in GCP(Google Cloud Console) and save it to `airflow/gcp_keyfile.json` and `airflow/crawler_gcp_keyfile.json`.

2. Copy `.env.example` to `.env`.

3. Update the values of project id, bucket name, dataset name and credentials path in `.env`.


### Setup smtp Server

1. Create the Google app passwords for `smtp_password` in next step. (following by [Sign in with app passwords](https://support.google.com/accounts/answer/185833?hl=en))

2. Update the values of `smtp_user`, `smtp_password` and `smtp_mail_from` in `airflow/config/airflow.cfg`.

3. Update the values of `alert_email` and `pm_email` in `airflow/variables/email_variables.json`.


### Setup Airflow

1. Generate `AIRFLOW_SECRET_KEY` in `.env`

Use below command to generate a random secret key.
```sh
openssl rand -hex 24
```
Paste the result to `AIRFLOW_SECRET_KEY` in `.env`.

2. Run
```sh
docker-compose build
docker-compose up
```

3. Navigate to `localhost:8080`


## Example

1. [d_data_ingestion_gcs.py](./dags/d_data_ingestion_gcs.py): upload file to GCS, and create bigquery external table.

2. [d_example_data_pipeline.py](./dags/d_example_data_pipeline.py): simple data pipeline.
- download data from gcs, and transform data, then upload to gcs and create external table.
- query bigquery and create new table.

## Reference

- [TIR101 Group 2](https://github.com/harryhowiefish/TIR101_Group2): Refer to Airflow settings and Upload to GCS.
