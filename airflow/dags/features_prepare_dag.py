from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd
from modules.extract_data import extract_products_list_cian, extract_stats_wiki
from modules.prepare_data import prepare_wiki, prepare_cian
from pyhive import hive
import redis
import logging
import os

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 5, 23),
    'retries': 1,
}

TEMP_DIR = "/tmp/airflow_data"
os.makedirs(TEMP_DIR, exist_ok=True)

def extract_data(**context):
    logging.info("Starting data extraction")
    wiki_df = extract_stats_wiki()
    cian_df = extract_products_list_cian()
    wiki_path = os.path.join(TEMP_DIR, "wiki.csv")
    cian_path = os.path.join(TEMP_DIR, "cian.csv")
    wiki_df.to_csv(wiki_path, index=False)
    cian_df.to_csv(cian_path, index=False)
    context['ti'].xcom_push(key='wiki_path', value=wiki_path)
    context['ti'].xcom_push(key='cian_path', value=cian_path)
    logging.info("Data extraction completed")

def preprocess_data(**context):
    ti = context['ti']
    wiki_path = ti.xcom_pull(key='wiki_path', task_ids='extract_data')
    cian_path = ti.xcom_pull(key='cian_path', task_ids='extract_data')
    wiki_df = pd.read_csv(wiki_path)
    cian_df = pd.read_csv(cian_path)
    wiki_df = prepare_wiki(wiki_df)
    cian_df = prepare_cian(cian_df)
    wiki_prep = os.path.join(TEMP_DIR, "wiki_preprocessed.csv")
    cian_prep = os.path.join(TEMP_DIR, "cian_preprocessed.csv")
    wiki_df.to_csv(wiki_prep, index=False)
    cian_df.to_csv(cian_prep, index=False)
    ti.xcom_push(key='wiki_prep_path', value=wiki_prep)
    ti.xcom_push(key='cian_prep_path', value=cian_prep)
    logging.info("Data preprocessing completed")

def load_to_hive(**context):
    ti = context['ti']
    wiki_prep = ti.xcom_pull(key='wiki_prep_path', task_ids='preprocess_data')
    cian_prep = ti.xcom_pull(key='cian_prep_path', task_ids='preprocess_data')

    conn = hive.Connection(host='hive', port=10000, username='hive')
    cursor = conn.cursor()

    logging.info("Creating tables in Hive if they do not exist")
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS wiki_table (
        Area STRING,
        Area_km2 FLOAT,
        Area_percentage FLOAT,
        Rank_Area INT,
        Population_2024 INT,
        Population_percentage FLOAT,
        Rank_Population INT,
        Density_2024 FLOAT,
        Rank_Density INT
    )
    ROW FORMAT DELIMITED
    FIELDS TERMINATED BY ','
    STORED AS TEXTFILE
    """)
    cursor.execute("TRUNCATE TABLE wiki_table")

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS cian_table (
        id INT,
        price FLOAT,
        roomsCount INT,
        ceilingHeight FLOAT,
        totalArea FLOAT,
        floorNumber INT,
        floorsCount INT,
        cargoLiftsCount INT,
        okrug STRING,
        houseMaterialType_brick BOOLEAN,
        houseMaterialType_monolith BOOLEAN,
        houseMaterialType_monolithBrick BOOLEAN,
        houseMaterialType_none BOOLEAN,
        houseMaterialType_panel BOOLEAN
    )
    ROW FORMAT DELIMITED
    FIELDS TERMINATED BY ','
    STORED AS TEXTFILE
    """)
    cursor.execute("TRUNCATE TABLE cian_table")
    cursor.close()
    conn.close()

    os.system(f"hive -e \"LOAD DATA LOCAL INPATH '{wiki_prep}' OVERWRITE INTO TABLE wiki_table\"")
    os.system(f"hive -e \"LOAD DATA LOCAL INPATH '{cian_prep}' OVERWRITE INTO TABLE cian_table\"")

    logging.info("Data loading to Hive completed")

def load_to_redis():
    logging.info("Connecting to Hive to load data for Redis")
    conn = hive.Connection(host='hive', port=10000, username='hive')
    wiki_df = pd.read_sql("SELECT * FROM wiki_table", conn)
    cian_df = pd.read_sql("SELECT * FROM cian_table", conn)
    conn.close()

    r = redis.Redis(host='redis', port=6379, db=0)
    for _, row in wiki_df.iterrows():
        r.hset(f"wiki:{row.Area}", mapping=row.to_dict())
    for _, row in cian_df.iterrows():
        r.hset(f"cian:{row.id}", mapping=row.to_dict())
        r.sadd(f"area:{row.okrug}:cian_ids", row.id)
    logging.info("Data loading to Redis completed")

with DAG('feature_prepare_dag',
         default_args=default_args,
         schedule_interval='@daily',
         catchup=False) as dag:

    extract = PythonOperator(
        task_id='extract_data',
        python_callable=extract_data,
        provide_context=True,
    )

    preprocess = PythonOperator(
        task_id='preprocess_data',
        python_callable=preprocess_data,
        provide_context=True,
    )

    load_hive = PythonOperator(
        task_id='load_to_hive',
        python_callable=load_to_hive,
        provide_context=True,
    )

    load_redis = PythonOperator(
        task_id='load_to_redis',
        python_callable=load_to_redis,
    )

    extract >> preprocess >> load_hive >> load_redis
