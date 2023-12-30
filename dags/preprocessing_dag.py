from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from sql_scripts.sql_generators import create_deduplication_sql, append_to_schema, truncate_tables
from dag_functions.preprocessing import load_and_preprocess
from datetime import datetime
from datetime import timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    
}

RAW_DATA_INPUT = '/tmp/data/raw_data/input'
RAW_DATA_SUCCESS = '/tmp/data/raw_data/success'
RAW_DATA_FAIL = '/tmp/data/raw_data/fail'

NORM_JSON_INPUT = '/tmp/data/preprocessed_data/input'
NORM_JSON_SUCCESS = '/tmp/data/preprocessed_data/success'
NORM_JSON_FAIL = '/tmp/data/preprocessed_data/fail'
NORM_JSON_PROCESSING = '/tmp/data/preprocessed_data/processing'

BATCH_SIZE = 500
#N_BATCHES = 2

schema = 'staging'
preprocessing_dag = DAG(
    dag_id='preprocess',
    default_args=default_args,
    catchup=False,
    description='DAG for preprocessing',
    schedule_interval='*/1 * * * *',  # Run every minute
)

preprocess_data_task = PythonOperator(
    task_id='preprocess_data_task',
    dag=preprocessing_dag,
    python_callable=load_and_preprocess,
    op_kwargs={
        'data_path': f'{RAW_DATA_INPUT}/*.json',
        'output_path': NORM_JSON_INPUT,
        'success_path': RAW_DATA_SUCCESS,
        'fail_path': RAW_DATA_FAIL,
        'batch_size': BATCH_SIZE,
        #'n_batches': N_BATCHES
    },
)
preprocess_data_task
