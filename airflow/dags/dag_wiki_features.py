from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd
from modules.extract_data import extract_stats_wiki
from modules.prepare_data import prepare_wiki
import psycopg2
import logging
import redis

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 5, 23),
    'retries': 1,
}

def extract_data(**context):
    logging.info("Starting data extraction for wiki data")
    wiki_df = extract_stats_wiki()
    
    if wiki_df.empty:
        logging.error("Wiki DataFrame is empty, not proceeding.")
        return

    context['ti'].xcom_push(key='wiki_df', value=wiki_df.to_dict())
    logging.info("Data extraction completed")

def preprocess_data(**context):
    ti = context['ti']
    wiki_df = pd.DataFrame(ti.xcom_pull(key='wiki_df', task_ids='extract_data'))
    wiki_df = prepare_wiki(wiki_df)
    
    areas = {
        'ЦАО': 'CAO',
        'САО': 'SAO',
        'СВАО': 'SVAO',
        'ВАО': 'VAO',
        'ЮВАО': 'UVAO',
        'ЮАО': 'UAO',
        'ЮЗАО': 'UZAO',
        'ЗАО': 'ZAO',
        'СЗАО': 'SZAO',
        'ЗелАО': 'ZelAO',
        'ТАО': 'TAO',
        'НАО': 'NAO'
    }

    wiki_df['lat_area'] = wiki_df['Area'].map(areas)

    context['ti'].xcom_push(key='wiki_df', value=wiki_df.to_dict())
    logging.info("Data preprocessing completed")

def load_to_db(**context):
    ti = context['ti']
    wiki_df = pd.DataFrame(ti.xcom_pull(key='wiki_df', task_ids='preprocess_data'))

    conn = psycopg2.connect(
        dbname='metastore',
        user='hive',
        password='hive',
        host='postgres',
        port='5432'
    )
    cursor = conn.cursor()

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS wiki_table (
        Area VARCHAR,
        Area_km2 FLOAT,
        Area_percentage FLOAT,
        Rank_Area INT,
        Population_2024 INT,
        Population_percentage FLOAT,
        Rank_Population INT,
        Density_2024 FLOAT,
        Rank_Density INT,
        lat_area VARCHAR
    )
    """)
    cursor.execute("TRUNCATE TABLE wiki_table")

    for index, row in wiki_df.iterrows():
        cursor.execute("""
        INSERT INTO wiki_table (
                       Area, 
                       Area_km2, 
                       Area_percentage, 
                       Rank_Area, 
                       Population_2024, 
                       Population_percentage, 
                       Rank_Population, 
                       Density_2024, 
                       Rank_Density,
                       lat_area
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, tuple(row))

    conn.commit()
    cursor.close()
    conn.close()
    logging.info("Data loaded to PostgreSQL successfully.")
def load_to_redis(**context):
    ti = context['ti']
    wiki_df = pd.DataFrame(ti.xcom_pull(key='wiki_df', task_ids='preprocess_data'))

    r = redis.Redis(host='redis', port=6379, db=0)

    for _, row in wiki_df.iterrows():
        try:
            area_key = f"wiki:{row['Area']}" 
            r.hset(area_key, mapping=row.to_dict())
            logging.info(f"Loaded data to Redis under key: {area_key}")
        except Exception as e:
            logging.error(f"Error loading data for wiki area {row['Area']}: {e}")

    logging.info("Data loading to Redis completed")
    
with DAG('wiki_data_processing_dag',
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

    load_db = PythonOperator(
        task_id='load_to_db',
        python_callable=load_to_db,
        provide_context=True,
    )
    
    load_redis = PythonOperator(
        task_id='load_to_redis',
        python_callable=load_to_redis,
        provide_context=True,
    )
    

    extract >> preprocess >> load_db >> load_redis
