import os
from datetime import datetime, timedelta

from docker.types import Mount
from google.cloud import bigquery, storage

from airflow.decorators import dag
from airflow.operators.dagrun_operator import TriggerDagRunOperator
from airflow.providers.docker.operators.docker import DockerOperator

RAW_BUCKET = os.environ.get("GCP_GCS_RAW_BUCKET")
BLOB_NAME = "gmaps"
GCS_CLIENT = storage.Client()
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
    el_gmaps_crawler = DockerOperator(
        task_id="el_gmaps_reviews_crawler",
        # use gmaps-scraper image on https://github.com/yeha98552/google-maps-reviews-scraper
        image="gmaps-scraper",
        api_version="auto",
        auto_remove=True,
        environment={
            "GCS_BUCKET_NAME": RAW_BUCKET,
            "GCS_BLOB_NAME": BLOB_NAME,
        },
        command="make run",
        mounts=[
            Mount(
                source=os.environ.get("CRAWER_GOOGLE_CREDENTIALS_LOCAL_PATH"),
                target="/app/crawler_gcp_keyfile.json",
                type="bind",
                read_only=True,
            ),
        ],
        mount_tmp_dir=False,
        mem_limit="24g",  # 50%-75% of local memory size
        shm_size="2g",
        docker_url="tcp://docker-proxy:2375",
        network_mode="bridge",
    )

    trigger_d_gmaps_places_src_to_ods = TriggerDagRunOperator(
        task_id="trigger_d_gmaps_places_src_to_ods",
        trigger_dag_id="d_gmaps_places_src_to_ods",
    )

    trigger_d_gmaps_reviews_src_to_ods = TriggerDagRunOperator(
        task_id="trigger_d_gmaps_reviews_src_to_ods",
        trigger_dag_id="d_gmaps_reviews_src_to_ods",
    )

    el_gmaps_crawler
    trigger_d_gmaps_places_src_to_ods.set_upstream(el_gmaps_crawler)
    trigger_d_gmaps_reviews_src_to_ods.set_upstream(el_gmaps_crawler)


d_gmaps_crawler_to_src()
