import os
from datetime import datetime, timedelta

import jieba
import pandas as pd
from google.cloud import bigquery
from snownlp import SnowNLP
from utils.gcp import query_bq_to_df, upload_df_to_bq

from airflow.decorators import dag, task

BQ_ODS_DATASET = os.environ.get("BIGQUERY_ODS_DATASET")
BQ_FACT_DATASET = os.environ.get("BIGQUERY_FACT_DATASET")
TABLE_NAME = "ods-gmaps_reviews"
BQ_CLIENT = bigquery.Client()

default_args = {
    "owner": "airflow",
    "start_date": datetime(2024, 5, 1),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

stopwords = set(["的", "是", "在", "我"])


def preprocess_text(text: str) -> str:
    """
    Preprocess text: cut words and remove stopwords

    Args:
        text (str): The text to be processed
    Returns:
        str: The processed text
    """
    words = jieba.cut(text)
    filtered_words = [word for word in words if word not in stopwords]
    return " ".join(filtered_words)


def analyze_sentiment(text: str) -> float:
    """
    Analyze sentiment of the text

    Args:
        text (str): The text to be analyzed
    Returns:
        float: The sentiment score
    """
    text = preprocess_text(text)
    if not text.strip():
        return 0
    s = SnowNLP(text)
    # close to 1 is positive, close to 0 is negative
    return round(s.sentiments, 2)


@dag(
    default_args=default_args,
    schedule_interval=None,  # Because this is triggered by another DAG
    catchup=False,
    tags=["gmaps"],
)
def d_gmaps_fact_reviews():
    @task
    def e_load_reviews(
        src_dataset: str, src_table: str, dest_dataset: str, dest_table: str
    ) -> pd.DataFrame:
        query = f"""
        SELECT DISTINCT
          `review_id`,
          `place_name`,
          `user_name`,
          `rating`,
          `published_at`,
          `review_text`,
        FROM
          `{src_dataset}`.`{src_table}`
        """
        return query_bq_to_df(client=BQ_CLIENT, sql_query=query)

    @task
    def t_reviews_emotion_score(df: pd.DataFrame) -> pd.DataFrame:
        # if review_text null emotion is 0, else process analyze_sentiment
        df["emotion_score"] = df["review_text"].apply(
            lambda x: 0 if pd.isnull(x) else analyze_sentiment(x)
        )
        return df

    @task
    def l_fact_reviews(df: pd.DataFrame, dest_dataset: str, dest_table: str):
        upload_df_to_bq(
            BQ_CLIENT,
            df,
            dest_dataset,
            dest_table,
            partition_by="published_at",
            schema=[
                bigquery.SchemaField("review_id", "STRING", mode="REQUIRED"),
                bigquery.SchemaField("place_name", "STRING", mode="REQUIRED"),
                bigquery.SchemaField("user_name", "STRING", mode="REQUIRED"),
                bigquery.SchemaField("rating", "INTEGER", mode="REQUIRED"),
                bigquery.SchemaField("published_at", "DATE", mode="REQUIRED"),
                bigquery.SchemaField("emotion_score", "FLOAT", mode="REQUIRED"),
            ],
        )

    df = e_load_reviews(BQ_ODS_DATASET, TABLE_NAME, BQ_FACT_DATASET, "fact-reviews")
    df_transformed = t_reviews_emotion_score(df)
    l_fact_reviews(df_transformed, BQ_FACT_DATASET, "fact-reviews")


d_gmaps_fact_reviews()
