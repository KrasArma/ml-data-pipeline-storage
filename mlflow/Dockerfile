FROM python:3.9-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    libsasl2-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

COPY model/ /app/model/

RUN pip install mlflow

CMD ["mlflow", "ui", "--host", "0.0.0.0", "--port", "5000"]