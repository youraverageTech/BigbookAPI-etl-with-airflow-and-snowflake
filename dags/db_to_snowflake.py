from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
import logging
import os
import pandas as pd


logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

default_args = {
    'owner' : 'airflow',
    'email' : 'alfarelahmad99@gmail.com',
    'start_date' : datetime(2025, 11, 15),
}

dag = DAG(
    dag_id= 'DataDB_to_snowflake',
    default_args=default_args,
    schedule= '@daily',
    description= 'Data BigbookAPI from postgresql into Snowflake',
    catchup=False,
    tags= ['posgresql', 'snowflake']
)

def extract_from_postgres(**kwargs):
    logger.info('Starting extracting data from postgresql...')

    # wrapper connection
    db_hook = PostgresHook(postgres_conn_id='bigbookapi_connections')

    # connection engine
    conn = db_hook.get_conn()

    # extract data into dataframe
    data_df = pd.read_sql('select * from books', con=conn)

    # extracting into parquet file
    try:
        output_path = '/opt/airflow/output/extractdatadb.parquet'
        data_df.to_parquet(output_path, index=False)
    except Exception as e:
        logger.error(f'Error extracting data into parquet file : {e}')

    # validate file exists
    if os.path.exists(output_path):
        logger.info('extracted data succesfully save into parquet file')
    else:
        logger.error('there are no file extractdatadb.parquet')

    # push path file into xcom
    kwargs['ti'].xcom_push(key='extract_data', value=output_path)

    return logger.info('Succesfully extract data from Postgresql')

extract_task = PythonOperator(
    task_id = 'extract_data_postgres',
    python_callable = extract_from_postgres,
    execution_timeout = timedelta(minutes=3),
    dag=dag
)

def load_data_into_snowflake(**kwargs):
    logger.info('Starting loading data into Snowflake...')

    sf_hook = SnowflakeHook(snowflake_conn_id='db_to_snowflake_connections')

    # file_path data extracted from postgres
    ti = kwargs['ti']
    file_path = ti.xcom_pull(key='extract_data', task_ids='extract_data_postgres')

    # creating table if not exists
    try:
        create_table = """
            CREATE TABLE IF NOT EXISTS books (
                id INTEGER,
                title VARCHAR(255),
                image VARCHAR(255),
                genres VARIANT,
                rating FLOAT,
                author_id VARIANT,
                author_name VARIANT
            );
            """
        
        sf_hook.run(create_table)
    except Exception as e:
        logging.error(f'Error creating table : {e}')

    # creating staging for insert data
    create_stage = """
        CREATE STAGE IF NOT EXISTS books_stage
            COMMENT = 'This is a staging for bigbookapi data.'
            
    """
    sf_hook.run(create_stage)

    try:
        # uploading file into staging area
        sf_hook.run(f'PUT file://{file_path} @books_stage AUTO_COMPRESS=TRUE')

        # inserting data from staging area into snowflake table
        sf_hook.run("""
            COPY INTO books
            FROM @books_stage
            FILE_FORMAT = (TYPE = PARQUET)
            ON_ERROR = 'CONTINUE'
            MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE;
            """)
    except Exception as e:
        logging.error(f'Error loading data into Snowflake table : {e}')
        raise
    
    # validating data
    conn = sf_hook.get_conn()

    data_df = pd.read_sql('select * from books', con=conn)

    if len(data_df) == 0:
        logging.error('Error loading data into snowflake : No data from snowflake table')
    else:
        logging.info('There are data in snowflake table')
    
    logging.info('Succesfully Loading data into Snowflake')

load_task = PythonOperator(
    task_id = 'load_data_into_snowflake',
    python_callable = load_data_into_snowflake,
    execution_timeout = timedelta(minutes=3),
    dag=dag
)

# creating dependency task
extract_task >> load_task


