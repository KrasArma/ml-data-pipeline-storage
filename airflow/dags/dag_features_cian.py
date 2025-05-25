from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.dummy import DummyOperator
from datetime import datetime
import pandas as pd
from modules.extract_data import extract_products_list_cian
from modules.prepare_data import prepare_cian
import logging
import psycopg2
import redis

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 5, 23),
    'retries': 1,
}

def extract_cian_data(**context):
    logging.info("Starting Cian data extraction")
    cian_df = extract_products_list_cian()
    context['ti'].xcom_push(key='cian_df', value=cian_df.to_dict())
    logging.info("Cian data extraction completed")
    return cian_df.empty  

def branch_task(**context):
    ti = context['ti']
    cian_empty = ti.xcom_pull(task_ids='extract_cian_data') 
    if cian_empty:
        return 'end_dag'  
    else:
        return 'preprocess_cian_data'  
    
def preprocess_cian_data(**context):
    ti = context['ti']
    cian_df = pd.DataFrame(ti.xcom_pull(key='cian_df', task_ids='extract_cian_data'))
    cian_df = prepare_cian(cian_df)
    context['ti'].xcom_push(key='cian_prepared_df', value=cian_df.to_dict())
    logging.info("Cian data preprocessing completed")

def load_cian_to_db(**context):
    ti = context['ti']
    cian_df = pd.DataFrame(ti.xcom_pull(key='cian_prepared_df', task_ids='preprocess_cian_data'))

    conn = psycopg2.connect(
        dbname='metastore',
        user='hive',
        password='hive',
        host='postgres',
        port='5432'
    )
    cursor = conn.cursor()

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
            row['price'],
            row['roomsCount'],
            row['ceilingHeight'],
            row['totalArea'],
            row['floorNumber'],
            row['floorsCount'],
            row['cargoLiftsCount'],
            str(row['okrug']),
            bool(row['houseMaterialType_brick']),
            bool(row['houseMaterialType_monolith']),
            bool(row['houseMaterialType_monolithBrick']),
            bool(row['houseMaterialType_none']),
            bool(row['houseMaterialType_panel'])
        ))

    conn.commit()
    cursor.close()
    conn.close()
    logging.info("Cian data loaded to database successfully")

def load_cian_to_redis(**context):
    logging.info("Connecting to PostgreSQL to load data for Redis")
    try:
        conn = psycopg2.connect(
            dbname='metastore',
            user='hive',
            password='hive',
            host='postgres',
            port='5432'
        )
        cian_df = pd.read_sql("SELECT * FROM cian_table", conn)
        logging.info(f"Loaded {len(cian_df)} rows from cian_table with columns: {list(cian_df.columns)}")
    except Exception as e:
        logging.error(f"Error connecting to PostgreSQL: {e}")
        return
    finally:
        conn.close()

    r = redis.Redis(host='redis', port=6379, db=0)

    for _, row in cian_df.iterrows():
        try:
            cian_key = f"cian:{row['id']}"
            r.hset(cian_key, mapping=row.to_dict())
            r.sadd(f"area:{row['okrug']}:cian_ids", row['id'])
            logging.info(f"Loaded data to Redis under key: {cian_key} and added to area set: area:{row['okrug']}:cian_ids")
        except Exception as e:
            logging.error(f"Error loading data for cian id {row['id']}: {e}")

    logging.info("Data loading to Redis completed")
    

with DAG('cian_data_processing_dag',
         default_args=default_args,
         schedule_interval='@daily',
         catchup=False) as dag:

    extract_cian = PythonOperator(
        task_id='extract_cian_data',
        python_callable=extract_cian_data,
        provide_context=True,
    )

    branch = BranchPythonOperator(
        task_id='branch_task',
        python_callable=branch_task,
        provide_context=True,
    )

    preprocess_cian = PythonOperator(
        task_id='preprocess_cian_data',
        python_callable=preprocess_cian_data,
        provide_context=True,
    )

    load_cian_db = PythonOperator(
        task_id='load_cian_to_db',
        python_callable=load_cian_to_db,
        provide_context=True,
    )

    load_cian_redis = PythonOperator(
        task_id='load_cian_to_redis',
        python_callable=load_cian_to_redis,
        provide_context=True,
    )

    end_dag = DummyOperator(
        task_id='end_dag',
    )

    extract_cian >> branch
    branch >> preprocess_cian >> load_cian_db >> load_cian_redis
    branch >> end_dag