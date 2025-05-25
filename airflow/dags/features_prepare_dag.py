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
import psycopg2
import numpy as np


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
    
    logging.info(f"Wiki path: {wiki_path}")
    logging.info(f"Cian path: {cian_path}")

    if not wiki_df.empty:
        wiki_df.to_csv(wiki_path)
        logging.info("Wiki file created successfully.")
    else:
        logging.error("Wiki DataFrame is empty, not saving to CSV.")

    if not cian_df.empty:
        cian_df.to_csv(cian_path)
        logging.info("Cian file created successfully.")
    else:
        logging.error("Cian DataFrame is empty, not saving to CSV.")

    context['ti'].xcom_push(key='wiki_path', value=wiki_path)
    context['ti'].xcom_push(key='cian_path', value=cian_path)
    logging.info("Data extraction completed")

def preprocess_data(**context):
    ti = context['ti']
    wiki_path = ti.xcom_pull(key='wiki_path', task_ids='extract_data')
    cian_path = ti.xcom_pull(key='cian_path', task_ids='extract_data')

    logging.info(f"Reading Wiki data from: {wiki_path}")
    logging.info(f"Reading Cian data from: {cian_path}")


    """ Из горячего хранилища авто в модель попадают 
        только данные из wiki. Данные по циану передаются в 
        запросе, но тут можно пособирать свежих при желании."""

    if os.path.exists(cian_path):
        cian_df = pd.read_csv(cian_df)
        cian_df = prepare_cian(cian_df)
    else:
        columns = [
        "id",
        "price",
        "roomsCount",
        "ceilingHeight",
        "totalArea",
        "floorNumber",
        "floorsCount",
        "cargoLiftsCount",
        "houseMaterialType"
        "okrug", 
        "houseMaterialType_brick", 
        "houseMaterialType_monolith", 
        "houseMaterialType_monolithBrick", 
        "houseMaterialType_none", 
        "houseMaterialType_panel"
    ]

        cian_df = pd.DataFrame(columns=columns)
        cian_df.loc[0] = [0] * len(columns)  

    wiki_df = pd.read_csv(wiki_path)
    wiki_df = prepare_wiki(wiki_df)

    wiki_prep = os.path.join(TEMP_DIR, "wiki_preprocessed.csv")
    cian_prep = os.path.join(TEMP_DIR, "cian_preprocessed.csv")

    wiki_df.to_csv(wiki_prep)
    cian_df.to_csv(cian_prep)

    ti.xcom_push(key='wiki_prep_path', value=wiki_prep)
    ti.xcom_push(key='cian_prep_path', value=cian_prep)
    logging.info("Data preprocessing completed")

def load_to_db(**context):
    ti = context['ti']
    wiki_prep = ti.xcom_pull(key='wiki_prep_path', task_ids='preprocess_data')
    cian_prep = ti.xcom_pull(key='cian_prep_path', task_ids='preprocess_data')

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
        Rank_Density INT
    )
    """)
    cursor.execute("TRUNCATE TABLE wiki_table")

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS cian_table (
        id SERIAL PRIMARY KEY,
        price FLOAT,
        roomsCount INT,
        ceilingHeight FLOAT,
        totalArea FLOAT,
        floorNumber INT,
        floorsCount INT,
        cargoLiftsCount INT,
        okrug VARCHAR,
        houseMaterialType_brick BOOLEAN,
        houseMaterialType_monolith BOOLEAN,
        houseMaterialType_monolithBrick BOOLEAN,
        houseMaterialType_none BOOLEAN,
        houseMaterialType_panel BOOLEAN
    )
    """)
    cursor.execute("TRUNCATE TABLE cian_table")

    wiki_df = pd.read_csv(wiki_prep)
    cian_df = pd.read_csv(cian_prep)
    logging.info(f"Columns in cian_df: {cian_df.columns.tolist()}")
    logging.info(f"Columns in wiki_df: {wiki_df.columns.tolist()}")

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
                       Rank_Density
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (
            row['Area'],
            row['Area_km2'], 
            row['Area_percentage'], 
            row['Rank_Area'], 
            row['Population_2024'], 
            row['Population_percentage'], 
            row['Rank_Population'], 
            row['Density_2024'], 
            row['Rank_Density']
            )
        )

    for index, row in cian_df.iterrows():
        cursor.execute("""
            INSERT INTO cian_table (
                price, 
                roomsCount, 
                ceilingHeight, 
                totalArea, 
                floorNumber, 
                floorsCount, 
                cargoLiftsCount, 
                okrug, 
                houseMaterialType_brick, 
                houseMaterialType_monolith, 
                houseMaterialType_monolithBrick, 
                houseMaterialType_none, 
                houseMaterialType_panel
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (
            row.get('price', 0).item() 
                if isinstance(row.get('price', 0), (np.integer, np.floating)) 
                else row.get('price', 0),
            
            row.get('roomsCount', 0).item() 
                if isinstance(row.get('roomsCount', 0), (np.integer, np.floating)) 
                else row.get('roomsCount', 0),
            
            row.get('ceilingHeight', 0).item() 
                if isinstance(row.get('ceilingHeight', 0), (np.integer, np.floating)) 
                else row.get('ceilingHeight', 0),
            
            row.get('totalArea', 0).item() 
                if isinstance(row.get('totalArea', 0), (np.integer, np.floating)) 
                else row.get('totalArea', 0),
            
            row.get('floorNumber', 0).item() 
                if isinstance(row.get('floorNumber', 0), (np.integer, np.floating)) 
                else row.get('floorNumber', 0),
            
            row.get('floorsCount', 0).item() 
                if isinstance(row.get('floorsCount', 0), (np.integer, np.floating)) 
                else row.get('floorsCount', 0),
            
            row.get('cargoLiftsCount', 0).item() 
                if isinstance(row.get('cargoLiftsCount', 0), (np.integer, np.floating)) 
                else row.get('cargoLiftsCount', 0),
            
            str(row.get('okrug', '')),
            
            bool(row.get('houseMaterialType_brick', 0)),  
            bool(row.get('houseMaterialType_monolith', 0)),  
            bool(row.get('houseMaterialType_monolithBrick', 0)),  
            bool(row.get('houseMaterialType_none', 0)),  
            bool(row.get('houseMaterialType_panel', 0)) 
        )
    )

def load_to_redis():
    logging.info("Connecting to PostgreSQL to load data for Redis")
    try:
        conn = psycopg2.connect(
            dbname='metastore',
            user='hive',
            password='hive',
            host='postgres',
            port='5432'
        )
        wiki_df = pd.read_sql("SELECT * FROM wiki_table", conn)
        cian_df = pd.read_sql("SELECT * FROM cian_table", conn)

        logging.info(
            f"Loaded {len(wiki_df)} rows from wiki_table with columns: {list(wiki_df.columns)}"
        )
        logging.info(
            f"Loaded {len(cian_df)} rows from cian_table with columns: {list(cian_df.columns)}"
        )
    except Exception as e:
        logging.error(f"Error connecting to PostgreSQL: {e}")
        return
    finally:
        conn.close()

    r = redis.Redis(host='redis', port=6379, db=0)

    for _, row in wiki_df.iterrows():
        try:
            area_key = f"wiki:{row['area']}" 
            r.hset(area_key, mapping=row.to_dict())
            logging.info(f"Loaded data to Redis under key: {area_key}")
        except Exception as e:
            logging.error(f"Error loading data for wiki area {row['area']}: {e}")

    for _, row in cian_df.iterrows():
        try:
            cian_key = f"cian:{row['id']}"  
            r.hset(cian_key, mapping=row.to_dict())
            r.sadd(f"area:{row['okrug']}:cian_ids", row['id'])  
            logging.info(
                f"Loaded data to Redis under key: {cian_key} and added to area set: area:{row['okrug']}:cian_ids"
            )
        except Exception as e:
            logging.error(f"Error loading data for cian id {row['id']}: {e}")

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

    load_db = PythonOperator(
        task_id='load_to_db',
        python_callable=load_to_db,
        provide_context=True,
    )

    load_redis = PythonOperator(
        task_id='load_to_redis',
        python_callable=load_to_redis,
    )

    extract >> preprocess >> load_db >> load_redis
                       