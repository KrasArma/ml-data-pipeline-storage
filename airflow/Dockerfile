FROM apache/airflow:2.5.0

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY airflow.cfg /usr/local/airflow/airflow.cfg