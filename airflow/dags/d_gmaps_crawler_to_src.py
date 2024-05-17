import os
from datetime import datetime, timedelta

from docker.types import Mount
from google.cloud import bigquery

from airflow.decorators import dag, task
from airflow.providers.docker.operators.docker import DockerOperator

RAW_BUCKET = os.environ.get("GCP_GCS_RAW_BUCKET")
BQ_ODS_DATASET = os.environ.get("BIGQUERY_ODS_DATASET")
CRAWER_GOOGLE_CREDENTIALS_LOCAL_PATH = os.environ.get(
    "CRAWER_GOOGLE_CREDENTIALS_LOCAL_PATH"
)

BQ_CLIENT = bigquery.Client()

default_args = {
    "owner": "airflow",
    "start_date": datetime(2024, 5, 1),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


@dag(
    default_args=default_args,
    schedule_interval="@daily",
    catchup=False,
    tags=["gmaps"],
)
def d_gmaps_crawler_to_src():
    @task
    def get_attraction_list() -> list[dict]:
        query = f"""
            SELECT DISTINCT
                attraction_id,
                attraction_name
            FROM
                `{BQ_ODS_DATASET}`.`ods_tripadvisor_info`
        """
        df = BQ_CLIENT.query(query).to_dataframe()
        return df.to_dict(orient="records")

    @task
    def run_gmaps_crawler(attraction: dict):
        print(f"crawling attraction: {attraction}")
        attraction_name = attraction["attraction_name"]
        attraction_id = attraction["attraction_id"]
        crawler_task = DockerOperator(
            task_id=f"crawl_{attraction_name}",
            image="gmaps-scraper",
            api_version="auto",
            auto_remove=True,
            environment={
                "ATTRACTION_ID": attraction_id,
                "ATTRACTION_NAME": attraction_name,
                "GCS_BUCKET_NAME": RAW_BUCKET,
                "GCS_BLOB_NAME": "gmaps-taiwan",
            },
            command="make run",
            mounts=[
                Mount(
                    source=CRAWER_GOOGLE_CREDENTIALS_LOCAL_PATH,
                    target="/app/crawler_gcp_keyfile.json",
                    type="bind",
                    read_only=True,
                ),
            ],
            mount_tmp_dir=False,
            mem_limit="24g",
            shm_size="2g",
            docker_url="tcp://docker-proxy:2375",
            network_mode="bridge",
        )
        return crawler_task.execute({})

    attractions = get_attraction_list()
    run_gmaps_crawler.expand(attraction=attractions)


d_gmaps_crawler_to_src()
