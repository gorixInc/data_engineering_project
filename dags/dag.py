import requests
import math
import json
from datetime import datetime, timedelta
from glob import glob
import pandas as pd
import json
import uuid
import os

from airflow import DAG 
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.operators.bash_operator import BashOperator 
from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.contrib.sensors.file_sensor import FileSensor

DEFAULT_ARGS = {
    'owner': 'Tartu',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

API_URL = 'http://api.open-notify.org/iss-now.json'

DATA_FOLDER = '/tmp/data'
RAW_DATA_FOLDER = '/tmp/data/raw_data'
LVL1_DATA_FOLDER = '/tmp/data/level1'

convert_to_csv = DAG(
    dag_id='convert_to_csv', # name of dag
    schedule_interval='* * * * 10', # execute every 10 minutes
    start_date=datetime(2022,9,14,9,15,0),
    catchup=False, # in case execution has been paused, should it execute everything in between
    template_searchpath=DATA_FOLDER, # the PostgresOperator will look for files in this folder
    default_args=DEFAULT_ARGS, # args assigned to all operators
    retries = 1,
    depends_on_past=False,

)

# Task 1 - fetch the current ISS location and save it as a file.
# Task 2 - find the closest country, save it to a file
# Task 3 - save the closest country information to the database
# Task 4 - output the continent

file_sensor_task = FileSensor(
    task_id='file_sensor_task',
    filepath=f'{RAW_DATA_FOLDER}/*.json',
    fs_conn_id='fs_default',
    poke_interval=300,
    dag=convert_to_csv,
)

def convert_to_csv_func(data_path, output_path):
    data = []
    for path in glob(data_path):
        with open(path, 'r') as f: 
            for line in f:
                data.append(json.loads(line.strip()))
        os.remove(path)

    df = pd.DataFrame(data)
    df = df.drop(['abstract'], axis=1)
    df.to_csv(f'{output_path}/{uuid.uuid1()}.csv')

process_file_task = PythonOperator(
    task_id='convert_to_csv_task',
    dag=convert_to_csv,
    trigger_rule='none_failed',
    python_callable=convert_to_csv_func,
    op_kwargs={
        'data_path': f'{RAW_DATA_FOLDER}/*.json',
        'output_path': LVL1_DATA_FOLDER,
    },
)

file_sensor_task >> process_file_task

