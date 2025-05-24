FROM apache/airflow:2.5.0

USER root

RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    g++ \
    libsasl2-dev \
    && rm -rf /var/lib/apt/lists/*

USER airflow

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY airflow.cfg /usr/local/airflow/airflow.cfg