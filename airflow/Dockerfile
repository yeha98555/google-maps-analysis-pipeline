FROM apache/airflow:2.9.1

COPY requirements.txt .
COPY gcp_keyfile.json .

ENV GOOGLE_APPLICATION_CREDENTIALS = "/opt/airflow/gcp_keyfile.json"

RUN pip install requirements.txt
