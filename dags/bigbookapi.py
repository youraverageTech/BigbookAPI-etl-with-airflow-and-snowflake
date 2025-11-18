from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from scripts.extract import fetch_api_data
from scripts.transform import transform_data
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from sqlalchemy.dialects.postgresql import ARRAY
from sqlalchemy import Text
import logging
import os
import time
import pandas as pd
import json


logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# importing variable from environment variable
api_key = os.getenv('API_KEY')


# Variables
url = "https://api.bigbookapi.com/search-books"
headers = {
    "Accept": "application/json",
    'x-api-key': api_key
}
params = {
    'number': 100,
    'offset': 0,
    'genres': 'action'
}


# 1. DAG Configuration
default_args = {
    'owner': 'airflow',
    'email': ['alfarelahmad99@gmail.com'],
    'depends_on_past': False,
    'email_on_failure': True,
    'email_on_success': True,
    'start_date': datetime(2025, 11, 15)
}

dag = DAG(
    dag_id='bigbookapi_etl',
    default_args=default_args,
    description='ETL pipeline for BigBookAPI data',
    schedule="@daily",
    catchup=False,
    tags=['etl', 'bigbookapi', 'postgres', 'snowflake'],
)

# 2. Extract Function
def running_extract(url, headers, params, **kwargs):
    """
    Extract phase: Pull raw data from BigBookAPI
    """
    raw = fetch_api_data(url, headers, params)

    raw_path = '/opt/airflow/output/raw_data.json'

    # Push the raw models to XCom for downstream tasks
    kwargs['ti'].xcom_push(key='raw_path', value=raw_path)

    print(f'Extract Complete: Retrieved {len(raw)} records')

    return "extract complated successfully"

# creating extract task
extract_task = PythonOperator(
    task_id='extract_bigbookapi_data',
    python_callable=running_extract,
    op_kwargs = {'url': url, 'headers': headers, 'params': params},
    execution_timeout=timedelta(minutes=5),
    dag=dag
)

# 3. Transform Function
def running_transform(**kwargs):
    """
    Transform phase: Process raw data into structured format
    """
    # Pull raw data from XCom
    ti = kwargs['ti']
    raw_path = ti.xcom_pull(key='raw_path', task_ids='extract_bigbookapi_data')

    with open(raw_path, "r") as f:
        raw_data_json = json.load(f)

    # Transform the data
    transformed_df = transform_data(raw_data_json)

    # output path
    output_path = '/opt/airflow/output/transform_data.parquet'
    
    try: 
        transformed_df.to_parquet(output_path, index=False)
        logger.info(f"Data saved to {'/opt/airflow/output/transform_data.parquet'}")
    except Exception as e:
        logger.error(f"Error saving JSON file: {e}")

    # validate transform data
    if not os.path.exists(output_path):
        logger.error("FILE PARQUET TIDAK ADA !!!")
    else:
        logger.info("FILE PARQUET BERHASIL DIBUAT")

    # Push the transformed data to XCom for downstream tasks
    ti.xcom_push(key='transformed_data', value=output_path)

    print(f'Transform Complete: Transformed {len(transformed_df)} records')
    return "transform complated successfully"

# creating transform task
transform_task = PythonOperator(
    task_id='transform_bigbookapi_data',
    python_callable=running_transform,
    execution_timeout=timedelta(minutes=5),
    dag=dag
)

# 4. Loading data into postgresql database
def load_data_to_db(**kwargs):
    """
    Load data to database postgresql
    """

    logger.info('Starting Loading data into database PostgreSQL...')
    starttime = time.time()
    # retrieves transform data from xcom
    ti = kwargs['ti']
    data = ti.xcom_pull(key='transformed_data', task_ids='transform_bigbookapi_data')

    # loading data into dataframe
    data_df = pd.read_parquet(data)

    # validate data is available
    # if not data_df.empty:
    #     print("Load Error : No transformed data available to load")
    #     return "No data to load"
    
    # make connection to postgres using postgres.hook
    postgres_hook = PostgresHook(postgres_conn_id = 'bigbookapi_connections')

    # creating table if not exists
    query_create_table = """
        CREATE TABLE IF NOT EXISTS books (
            id INTEGER,
            title VARCHAR(255),
            image VARCHAR(255),
            genres TEXT[],
            rating FLOAT,
            author_id TEXT[],
            author_name TEXT[]
        );
        """
    postgres_hook.run(query_create_table)
    logger.info('Table has created or exists')

    # inserting data into table books
    try:
        engine = postgres_hook.get_sqlalchemy_engine()

        data_df.to_sql(
            name="books",
            con=engine,
            if_exists="append",
            index=False,
            dtype={
                "genres": ARRAY(Text),
                "author_id": ARRAY(Text),
                "author_name": ARRAY(Text)
            }
        )

        endtime = time.time()
        logger.info(f'Completed Loading data : {endtime - starttime} seconds')

    except Exception as e:
        print(f"Error during loading data: {e}")
        raise

# creating load task
load_task = PythonOperator(
    task_id = 'load_bigbookapi_data',
    python_callable=load_data_to_db,
    execution_timeout=timedelta(minutes=5),
    dag=dag
)

# creating dependency task
extract_task >> transform_task >> load_task