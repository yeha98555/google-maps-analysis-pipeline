FROM apache/airflow:2.9.1

COPY requirements.txt .
COPY gcp_keyfile.json .

RUN pip install -r requirements.txt