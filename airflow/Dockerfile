FROM apache/airflow:2.5.0-python3.9

USER root

RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    g++ \
    libsasl2-dev \
    && rm -rf /var/lib/apt/lists/*

USER airflow
RUN pip install apache-airflow[async] airflow-exporter
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY airflow.cfg /usr/local/airflow/airflow.cfg