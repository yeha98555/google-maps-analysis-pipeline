import os
from datetime import datetime, timedelta

import numpy as np
import pandas as pd
from google.cloud import bigquery, storage
from utils.common import rename_place_id
from utils.gcp import build_bq_from_gcs, download_df_from_gcs, upload_df_to_gcs

from airflow.decorators import dag, task

RAW_BUCKET = os.environ.get("GCP_GCS_RAW_BUCKET")
PROCESSED_BUCKET = os.environ.get("GCP_GCS_PROCESSED_BUCKET")
ARCHIVE_BUCKET = os.environ.get("GCP_GCS_ARCHIVE_BUCKET")
BLOB_NAME = "gmaps/places"
BQ_ODS_DATASET = os.environ.get("BIGQUERY_ODS_DATASET")
ODS_TABLE_NAME = "ods-gmaps_places"
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
    schedule_interval=None,  # Because this is triggered by another DAG
    catchup=False,
    tags=["gmaps"],
)
def d_gmaps_places_src_to_ods():
    @task
    def t_get_latest_places_blobname(bucket_name: str, blob_prefix: str) -> str:
        blobs = storage.Client().list_blobs(bucket_name, prefix=blob_prefix)
        blobs = sorted(blobs, key=lambda x: x.name)
        print(f"blob name: {blobs[-1].name}")
        return blobs[-1].name

    @task
    def t_get_places_df_from_gcs(bucket_name: str, blob_name: str) -> pd.DataFrame:
        return download_df_from_gcs(
            client=GCS_CLIENT,
            bucket_name=bucket_name,
            blob_name=blob_name,
            filetype="jsonl",
        )

    # @task
    # def t_get_latset_and_second_latest_places_file(
    #     bucket_name: str, blob_prefix: str
    # ) -> Tuple[str, str]:
    #     bucket = GCS_CLIENT.get_bucket(bucket_name)
    #     blobs = bucket.list_blobs(prefix=blob_prefix)

    #     blobs = sorted(blobs, key=lambda x: x.name)
    #     return blobs[-1], blobs[-2]

    # @task
    # def t_compare_places_row_number(
    #     filename_latest: str, filename_second_latest: str
    # ) -> pd.DataFrame:
    #     print(filename_latest, filename_second_latest)
    #     df_latest = pd.read_csv(filename_latest)
    #     df_second_latest = pd.read_csv(filename_second_latest)

    #     # 確認最新檔案和第二新的檔案筆數是否相同，如果相同，刪除舊的，如果不相同，保留兩個，回傳筆數多的
    #     if df_latest.shape[0] == df_second_latest.shape[0]:
    #         # 搬移舊有的檔案到archive bucket
    #         t_move_old_places_file_to_archive_bucket(filename_second_latest)
    #         return df_latest
    #     else:
    #         # 如果不相同，保留兩個，回傳筆數多的
    #         print(
    #             f"number of rows in latest and second latest are not the same: {df_latest.shape[0]} and {df_second_latest.shape[0]}"
    #         )
    #         if df_latest.shape[0] > df_second_latest.shape[0]:
    #             return df_latest
    #         else:
    #             return df_second_latest

    # def t_move_old_places_file_to_archive_bucket(
    #     bucket_name: str, source_blob_name: str
    # ):
    #     # 若相同將除了最新以外的該層資料搬移到archive bucket
    #     bucket = GCS_CLIENT.get_bucket(bucket_name)

    #     blob = bucket.blob(source_blob_name)

    #     blob.copy_to(f"{ARCHIVE_BUCKET}/{source_blob_name}")
    #     blob.delete()

    @task
    def t_remove_places_columns(df: pd.DataFrame) -> pd.DataFrame:
        df.drop(
            columns=[
                "status",
                "featured_question",
                "cid",
                "phone",
                "owner",
                "plus_code",
                "data_id",
                "reviews_per_rating",
            ],
            inplace=True,
        )
        return df

    @task
    def t_rename_places_columns(df: pd.DataFrame) -> pd.DataFrame:
        df.rename(
            columns={
                "place_id": "place_id_raw",
                "name": "place_name",
                "link": "google_place_url",
                "reviews": "total_reviews",
                "rating": "avg_rating",
            },
            inplace=True,
        )
        return df

    @task
    def t_convert_place_id(df: pd.DataFrame) -> pd.DataFrame:
        df["place_id"] = df["place_name"].apply(lambda x: rename_place_id(x))
        return df

    @task
    def t_convert_closed_on_to_list(df: pd.DataFrame) -> pd.DataFrame:
        df["closed_on"] = df["closed_on"].apply(
            lambda x: x if isinstance(x, list) else [x]
        )
        return df

    @task
    def t_convert_categories_to_list(df: pd.DataFrame) -> pd.DataFrame:
        df["categories"] = df["categories"].apply(
            lambda x: x if isinstance(x, list) else [x]
        )
        return df

    @task
    def t_convert_most_popular_times_null(df: pd.DataFrame) -> pd.DataFrame:
        # if "Not Present" in most_popular_times, replace it with NaN
        df["most_popular_times"] = df["most_popular_times"].apply(
            lambda x: x if x != "Not Present" else np.nan
        )
        return df

    @task
    def t_convert_popular_times_null(df: pd.DataFrame) -> pd.DataFrame:
        # if "Not Present" in popular_times, replace it with NaN
        df["popular_times"] = df["popular_times"].apply(
            lambda x: x if x != "Not Present" else np.nan
        )
        return df

    @task
    def l_upload_transformed_places_to_gcs(
        df: pd.DataFrame, bucket_name: str, blob_name: str
    ) -> bool:
        upload_df_to_gcs(
            client=GCS_CLIENT,
            bucket_name=bucket_name,
            blob_name=blob_name,
            df=df,
            filetype="jsonl",
        )

    @task
    def l_create_bq_external_table(
        bucket_name: str, blob_name: str, dataset_name: str, table_name: str
    ) -> bool:
        build_bq_from_gcs(
            client=BQ_CLIENT,
            bucket_name=bucket_name,
            blob_name=blob_name,
            dataset_name=dataset_name,
            table_name=table_name,
            schema=[
                bigquery.SchemaField("place_id", "STRING", mode="REQUIRED"),
                bigquery.SchemaField("place_id_raw", "STRING", mode="REQUIRED"),
                bigquery.SchemaField("place_name", "STRING", mode="REQUIRED"),
                bigquery.SchemaField("description", "STRING"),
                bigquery.SchemaField("total_reviews", "INTEGER"),
                bigquery.SchemaField("avg_rating", "FLOAT"),
                bigquery.SchemaField("can_claim", "BOOLEAN"),
                bigquery.SchemaField("featured_image", "STRING"),
                bigquery.SchemaField("main_category", "STRING"),
                bigquery.SchemaField("categories", "STRING", mode="REPEATED"),
                bigquery.SchemaField("google_place_url", "STRING", mode="REQUIRED"),
                bigquery.SchemaField("workday_timing", "STRING"),
                bigquery.SchemaField("closed_on", "STRING", mode="REPEATED"),
                bigquery.SchemaField("address", "STRING"),
                bigquery.SchemaField(
                    "review_keywords",
                    "RECORD",
                    mode="REPEATED",
                    fields=[
                        bigquery.SchemaField("keyword", "STRING"),
                        bigquery.SchemaField("count", "INTEGER"),
                    ],
                ),
                bigquery.SchemaField("link", "STRING"),
                bigquery.SchemaField("status", "STRING"),
                bigquery.SchemaField("price_range", "STRING"),
                bigquery.SchemaField("reviews_link", "STRING"),
                # bigquery.SchemaField(
                #     "reviews_per_rating",
                #     "RECORD",
                #     fields=[
                #         bigquery.SchemaField("1", "INTEGER"),
                #         bigquery.SchemaField("2", "INTEGER"),
                #         bigquery.SchemaField("3", "INTEGER"),
                #         bigquery.SchemaField("4", "INTEGER"),
                #         bigquery.SchemaField("5", "INTEGER"),
                #     ],
                # ),
                bigquery.SchemaField(
                    "coordinates",
                    "RECORD",
                    fields=[
                        bigquery.SchemaField("latitude", "FLOAT"),
                        bigquery.SchemaField("longitude", "FLOAT"),
                    ],
                ),
                bigquery.SchemaField(
                    "detailed_address",
                    "RECORD",
                    fields=[
                        bigquery.SchemaField("ward", "STRING"),
                        bigquery.SchemaField("street", "STRING"),
                        bigquery.SchemaField("city", "STRING"),
                        bigquery.SchemaField("postal_code", "STRING"),
                        bigquery.SchemaField("state", "STRING"),
                        bigquery.SchemaField("country_code", "STRING"),
                    ],
                ),
                bigquery.SchemaField("time_zone", "STRING"),
                bigquery.SchemaField(
                    "menu",
                    "RECORD",
                    fields=[
                        bigquery.SchemaField("link", "STRING"),
                        bigquery.SchemaField("source", "STRING"),
                    ],
                ),
                bigquery.SchemaField("reservations", "STRING", mode="REPEATED"),
                bigquery.SchemaField("order_online_links", "STRING", mode="REPEATED"),
                bigquery.SchemaField(
                    "about",
                    "RECORD",
                    mode="REPEATED",
                    fields=[
                        bigquery.SchemaField("id", "STRING"),
                        bigquery.SchemaField("name", "STRING"),
                        bigquery.SchemaField(
                            "options",
                            "RECORD",
                            mode="REPEATED",
                            fields=[
                                bigquery.SchemaField("name", "STRING"),
                                bigquery.SchemaField("enabled", "BOOLEAN"),
                            ],
                        ),
                    ],
                ),
                bigquery.SchemaField(
                    "images",
                    "RECORD",
                    mode="REPEATED",
                    fields=[
                        bigquery.SchemaField("about", "STRING"),
                        bigquery.SchemaField("link", "STRING"),
                    ],
                ),
                bigquery.SchemaField(
                    "hours",
                    "RECORD",
                    mode="REPEATED",
                    fields=[
                        bigquery.SchemaField("day", "STRING"),
                        bigquery.SchemaField("times", "STRING", mode="REPEATED"),
                    ],
                ),
                bigquery.SchemaField(
                    "most_popular_times",
                    "RECORD",
                    mode="REPEATED",
                    fields=[
                        bigquery.SchemaField("hour_of_day", "INTEGER"),
                        bigquery.SchemaField("average_popularity", "FLOAT"),
                        bigquery.SchemaField("time_label", "STRING"),
                    ],
                ),
                bigquery.SchemaField(
                    "popular_times",
                    "RECORD",
                    fields=[
                        bigquery.SchemaField(
                            "Monday",
                            "RECORD",
                            mode="REPEATED",
                            fields=[
                                bigquery.SchemaField("hour_of_day", "INTEGER"),
                                bigquery.SchemaField("time_label", "STRING"),
                                bigquery.SchemaField(
                                    "popularity_percentage", "INTEGER"
                                ),
                                bigquery.SchemaField(
                                    "popularity_description", "STRING"
                                ),
                            ],
                        ),
                        bigquery.SchemaField(
                            "Tuesday",
                            "RECORD",
                            mode="REPEATED",
                            fields=[
                                bigquery.SchemaField("hour_of_day", "INTEGER"),
                                bigquery.SchemaField("time_label", "STRING"),
                                bigquery.SchemaField(
                                    "popularity_percentage", "INTEGER"
                                ),
                                bigquery.SchemaField(
                                    "popularity_description", "STRING"
                                ),
                            ],
                        ),
                        bigquery.SchemaField(
                            "Wednesday",
                            "RECORD",
                            mode="REPEATED",
                            fields=[
                                bigquery.SchemaField("hour_of_day", "INTEGER"),
                                bigquery.SchemaField("time_label", "STRING"),
                                bigquery.SchemaField(
                                    "popularity_percentage", "INTEGER"
                                ),
                                bigquery.SchemaField(
                                    "popularity_description", "STRING"
                                ),
                            ],
                        ),
                        bigquery.SchemaField(
                            "Thursday",
                            "RECORD",
                            mode="REPEATED",
                            fields=[
                                bigquery.SchemaField("hour_of_day", "INTEGER"),
                                bigquery.SchemaField("time_label", "STRING"),
                                bigquery.SchemaField(
                                    "popularity_percentage", "INTEGER"
                                ),
                                bigquery.SchemaField(
                                    "popularity_description", "STRING"
                                ),
                            ],
                        ),
                        bigquery.SchemaField(
                            "Friday",
                            "RECORD",
                            mode="REPEATED",
                            fields=[
                                bigquery.SchemaField("hour_of_day", "INTEGER"),
                                bigquery.SchemaField("time_label", "STRING"),
                                bigquery.SchemaField(
                                    "popularity_percentage", "INTEGER"
                                ),
                                bigquery.SchemaField(
                                    "popularity_description", "STRING"
                                ),
                            ],
                        ),
                        bigquery.SchemaField(
                            "Saturday",
                            "RECORD",
                            mode="REPEATED",
                            fields=[
                                bigquery.SchemaField("hour_of_day", "INTEGER"),
                                bigquery.SchemaField("time_label", "STRING"),
                                bigquery.SchemaField(
                                    "popularity_percentage", "INTEGER"
                                ),
                                bigquery.SchemaField(
                                    "popularity_description", "STRING"
                                ),
                            ],
                        ),
                        bigquery.SchemaField(
                            "Sunday",
                            "RECORD",
                            mode="REPEATED",
                            fields=[
                                bigquery.SchemaField("hour_of_day", "INTEGER"),
                                bigquery.SchemaField("time_label", "STRING"),
                                bigquery.SchemaField(
                                    "popularity_percentage", "INTEGER"
                                ),
                                bigquery.SchemaField(
                                    "popularity_description", "STRING"
                                ),
                            ],
                        ),
                    ],
                ),
                bigquery.SchemaField("is_spending_on_ads", "BOOLEAN"),
            ],
            filetype="jsonl",
        )

    t1 = t_get_latest_places_blobname(RAW_BUCKET, BLOB_NAME)
    t2 = t_get_places_df_from_gcs(RAW_BUCKET, t1)
    t3 = t_remove_places_columns(t2)
    t4 = t_rename_places_columns(t3)
    t5 = t_convert_place_id(t4)
    t6 = t_convert_closed_on_to_list(t5)
    t7 = t_convert_categories_to_list(t6)
    t8 = t_convert_most_popular_times_null(t7)
    t9 = t_convert_popular_times_null(t8)
    t10 = l_upload_transformed_places_to_gcs(t9, PROCESSED_BUCKET, f"{BLOB_NAME}.jsonl")
    t11 = l_create_bq_external_table(
        PROCESSED_BUCKET, f"{BLOB_NAME}.jsonl", BQ_ODS_DATASET, ODS_TABLE_NAME
    )

    t10 >> t11


d_gmaps_places_src_to_ods()
