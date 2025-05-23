from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from modules.extract_data import extract_products_list_cian, extract_stats_wiki
from modules.prepare_data import prepare_wiki, prepare_cian
from pyhive import hive
import pandas as pd
import redis


default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 5, 23),
    'retries': 1,
}

def extract_data():
   wiki_df = extract_stats_wiki()
   cian_df = extract_products_list_cian()
   return wiki_df, cian_df


def preprocess_data(wiki_df, cian_df):
    wiki_df = prepare_wiki(wiki_df)
    cian_df = prepare_cian(cian_df)
    return wiki_df, cian_df


def load_to_hive(wiki_df, cian_df):

    conn = hive.Connection(host='hive', port=10000, username='hive') 
    cursor = conn.cursor()

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
    """)

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
    """)

    cursor.close()
    for start in range(0, len(wiki_df), 50):  
        end = start + 50
        batch = wiki_df[start:end]
        batch.to_sql('wiki_table', conn, if_exists='append', index=False)

    for start in range(0, len(cian_df), 50):  
        end = start + 50
        batch = cian_df[start:end]
        batch.to_sql('cian_table', conn, if_exists='append', index=False)
    conn.close()


def load_to_redis():

    hive_conn = hive.Connection(host='hive', port=10000, username='hive')

    wiki_query = "SELECT * FROM wiki_table"
    wiki_df = pd.read_sql(wiki_query, hive_conn)

    cian_query = "SELECT * FROM cian_table"
    cian_df = pd.read_sql(cian_query, hive_conn)

    hive_conn.close()
    r = redis.Redis(host='redis', port=6379, db=0)

    for start in range(0, len(wiki_df), 50): 
        end = start + 50
        batch = wiki_df[start:end]
        for index, row in batch.iterrows():
            r.hmset(f"wiki:{row['Area']}", row.to_dict()) 

    for start in range(0, len(cian_df), 50):  
        end = start + 50
        batch = cian_df[start:end]
        for index, row in batch.iterrows():
            cian_key = f"cian:{row['id']}"
            r.hmset(cian_key, row.to_dict())  
            r.sadd(f"area:{row['okrug']}:cian_ids", row['id']) 

with DAG('feature_prepare_dag',
         default_args=default_args,
         schedule_interval='@daily',
         catchup=False) as dag:

    extract = PythonOperator(
        task_id='extract_data',
        python_callable=extract_data,
    )

    preprocess = PythonOperator(
        task_id='preprocess_data',
        python_callable=preprocess_data,
        op_kwargs={'wiki_df': '{{ task_instance.xcom_pull(task_ids="extract_data")[0] }}',
                   'cian_df': '{{ task_instance.xcom_pull(task_ids="extract_data")[1] }}'},
    )

    load_hive = PythonOperator(
        task_id='load_to_hive',
        python_callable=load_to_hive,
        op_kwargs={'wiki_df': '{{ task_instance.xcom_pull(task_ids="preprocess_data")[0] }}',
                   'cian_df': '{{ task_instance.xcom_pull(task_ids="preprocess_data")[1] }}'},
    )

    load_redis = PythonOperator(
        task_id='load_to_redis',
        python_callable=load_to_redis,
    )

    extract >> preprocess >> load_hive >> load_redis