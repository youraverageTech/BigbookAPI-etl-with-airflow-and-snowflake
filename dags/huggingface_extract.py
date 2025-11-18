from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from typing import List, Dict, Any
from huggingface_hub import list_models

# 1. DAG Configuration
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 11, 10),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}
dag = DAG(
    default_args=default_args,
    dag_id = 'huggingface_model_etl',
    description='ETL pipeline for Hugging Face models',
    schedule= '@daily',
    catchup=False,
    tags=['etl', 'huggingface', 'postgres'],
)

# 2. Extract Function
def extract_model_data(**kwargs):
    """
    extract phase : pull raw data from huggingface API
    returns the top 50 models by download count

    print("Extract Phase : Fetching  model data from Hugging Face Hub")
    """

    try:
        # fetch top 50 models sorted by last modified date
        models = list_models(sort='lastModified', direction=-1, limit=50, cardData=True)

        raw_models = [{
            'model_id': m.Id,
            'author': m.author or None,
            'pipeline_tag': m.pipeline_tag or None,
            'tags': m.tags or [],
            'last_modified': m.lastModified,

        } for m in models]

        # Push the raw models to XCom for downstream tasks
        kwargs['ti'].xcom_push(key='raw_models', value=raw_models)

        print(f'Extract Complete: Retrieved {len(raw_models)} models')
        return "extract complated successfully"
    
    except Exception as e:
        print(f"Error during extraction: {e}")
        kwargs['ti'].xcom_push(key='raw_models', value=[])
        return "extract failed"
    
# creating extract task
extract_task = PythonOperator(
    task_id = 'extract_huggingface_models',
    python_callable=extract_model_data,
    dag=dag
)

# 3. Transform Function
def transform_model_data(**kwargs):
    """
    transform phase : clean and transform raw data
    returns cleaned model data ready for loading
    - remove duplicates
    - Handle missing values
    - Standardize fields
    """

    # Retrieve raw models from XCom
    ti = kwargs['ti']
    raw_models = ti.xcom_pull(key='raw_models', task_ids='extract_huggingface_models') or []

    transformed_models = []
    seen = set()
    for m in raw_models:
        # Clean and transform data
        mid = m.get('model_id')
        if not mid or mid in seen:
            continue
        seen.add(mid)

        transformed_models.append({
            'model_id': mid,
            'author': m.get('author'),
            'pipeline_tag': m.get('pipeline_tag'),
            'tags': m.get('tags'),
            'last_modified': m.get('last_modified'),
        })

    # Push the transformed models to XCom for downstream tasks
    ti.xcom_push(key='transformed_models', value=transformed_models)

    print(f'Transform Complete: Produced {len(transformed_models)} models')

    return "transform complated successfully"

# creating transform task
transform_task = PythonOperator(
    task_id='transform_huggingface_models',
    python_callable=transform_model_data,
    dag=dag
)

# 4. Load Function - insert into postgresql
def load_model_data(**kwargs):
    """
    load phase : load transformed data into Postgres
    """

    # Retrieve transformed models from XCom
    ti = kwargs['ti']
    transformed_models = ti.xcom_pull(key='transformed_models', task_ids='transform_huggingface_models') or []

    if not transformed_models:
        print("Load Error : No transformed data available to load")
        return "No data to load"
    
    postgres_hook = PostgresHook(postgres_conn_id = 'models_connection')

    # creating table if not exists
    create_table_sql = """
    CREATE TABLE IF NOT EXISTS ai_models (
        model_id VARCHAR(255) PRIMARY KEY,
        author VARCHAR(255),
        pipeline_tag VARCHAR(255),
        tags TEXT[],
        last_modified TIMESTAMP
    );
    """
    postgres_hook.run(create_table_sql)

    # inserting data into table
    insert_sql = """
    INSERT INTO ai_models (model_id, author, pipeline_tag, tags, last_modified)
    VALUES (%s, %s, %s, %s, %s)
    ON CONFLICT (model_id) DO UPDATE SET
        author = EXCLUDED.author,
        pipeline_tag = EXCLUDED.pipeline_tag,
        tags = EXCLUDED.tags,
        last_modified = EXCLUDED.last_modified;
    """
    try:
        for m in transformed_models:
            postgres_hook.run(insert_sql, parameters=(
                m['model_id'],
                m['author'],
                m['pipeline_tag'],
                m['tags'],
                m['last_modified']
            ))
        print(f'Load Complete: Loaded {len(transformed_models)} models into Postgres')
        return "load complated successfully"
    
    except Exception as e:
        print(f"Error during loading data: {e}")
        raise

# creating load task
load_task = PythonOperator(
    task_id='load_huggingface_models',
    python_callable=load_model_data,
    dag=dag
)

# 5. Defining Task Dependencies
extract_task >> transform_task >> load_task
