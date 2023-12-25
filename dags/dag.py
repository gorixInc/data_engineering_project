import sys
sys.path.append('/opt/airflow/')
sys.path.append('/opt/airflow/sqlalchemy_orm')
import json
from datetime import datetime, timedelta
from glob import glob
import pandas as pd
import json
import uuid
import os
import shutil
from pathlib import Path
from copy import deepcopy
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy_orm.snowflake_dirty import (Person, Submitter, Category, 
                              SubCategory, Journal, Publication, License,
                              JournalSpecifics, Authorship, PublicationCategory)

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

DATABASE_URL = "postgresql://airflow:airflow@localhost:5432/airflow"
DATA_FOLDER = '/tmp/data'
RAW_DATA_FOLDER = '/tmp/data/raw_data'
RAW_DATA_SUCCESS_FOLDER = '/tmp/data/raw_data_processed'
NORM_DATA_FOLDER = '/tmp/data/norm_jsons'
NORM_SUCCESS_FOLDER = '/tmp/data/norm_jsons_processed'

convert_to_csv = DAG(
    dag_id='convert_to_csv', # name of dag
    schedule_interval='* * * * *', # execute every 10 minutes
    start_date=datetime(2022,9,14,9,15,0),
    catchup=False, # in case execution has been paused, should it execute everything in between
    template_searchpath=DATA_FOLDER, # the PostgresOperator will look for files in this folder
    default_args=DEFAULT_ARGS, # args assigned to all operators
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


def normalize_categories(publication_data):
    categories_str = publication_data['categories']
    categories_split = categories_str.split(' ')
    category_data = []
    for category_str in categories_split:
        if '.' in category_str:
            category_name = category_str.split('.')[0]
            subcategory_name = category_str.split('.')[1]
        else: 
            category_name = categories_str
            subcategory_name = None
        category_data.append({'category_name': category_name,
                              'subcategory_name': subcategory_name})
    #publication_data['categories_parsed'] = category_data
    return category_data

def normalize_authors(publication_data):
    author_objs = []
    author_lists = publication_data['authors_parsed']
    for author_list in author_lists:
        first_name = author_list[1]
        last_name = author_list[0]
        third_name = author_list[2]
        author_obj = {
                        'first_name': first_name,
                        'last_name': last_name,
                        'third_name': third_name
                    }
        author_objs.append(author_obj)
    return author_objs
        
def normalize_submitter(publication_data):
    submitter_str = publication_data['submitter']
    split_str = submitter_str.split(' ')
    first_name, last_name, third_name = None, None, None
    first_name = split_str[0]
    if len(split_str) > 1:
        last_name = split_str[1]
    if len(split_str) > 2:
        third_name = split_str[2]
    submitter_obj = {
        'first_name': first_name,
        'last_name': last_name,
        'third_name': third_name
    }
    return submitter_obj

def normalize_json(publication_data):
    data_norm = deepcopy(publication_data)
    del data_norm['abstract'] 
    del data_norm['versions']
    del data_norm['report-no']
    data_norm['norm_authors'] = normalize_authors(publication_data)
    del data_norm['authors']
    del data_norm['authors_parsed']
    data_norm['norm_categories'] = normalize_categories(publication_data)
    del data_norm['categories']
    data_norm['arxiv_id'] = data_norm['id']
    del data_norm['id']
    data_norm['submitter_norm'] = normalize_submitter(publication_data)
    del data_norm['submitter']

    return data_norm

def load_and_normalize(data_path, output_path, success_path):
    json_data = []
    for path in glob(data_path):
        with open(path, 'r') as f: 
            for line in f:
                json_data.append(json.loads(line.strip()))
        shutil.move(path, Path(success_path)/Path(path).name)

    norm_datas = []
    for data in json_data:
        norm_data = normalize_json(data)
        norm_datas.append(norm_data)

    with open(f'{output_path}/{uuid.uuid1()}.json', 'w') as f:
        json.dump(norm_datas, f, indent=4)

process_file_task = PythonOperator(
    task_id='convert_to_csv_task',
    dag=convert_to_csv,
    trigger_rule='none_failed',
    python_callable=load_and_normalize,
    op_kwargs={
        'data_path': f'{RAW_DATA_FOLDER}/*.json',
        'output_path': NORM_DATA_FOLDER,
        'success_path': RAW_DATA_SUCCESS_FOLDER
    },
)

file_sensor_task >> process_file_task


def insert_publication(publication_data, session):
    person_objs = []
    sub_category_objs = []
    category_objs = []

    for author in publication_data.pop('norm_authors'):
        person_obj = Person(**author)
        person_objs.append(person_obj)
        session.add(person_obj)

    for category_data in publication_data.pop('norm_categories'):
        category_obj = Category(name = category_data['category_name'])
        category_objs.append(category_obj)
        session.add(category_obj)
        subcategory_name = category_data['subcategory_name']

        if subcategory_name is not None and not subcategory_name == '':
            sub_category_obj = SubCategory(name = subcategory_name)
            sub_category_objs.append(sub_category_obj)
            session.add(sub_category_obj)
        else: 
            sub_category_objs.append(None)
        
    
    journal_ref = publication_data.pop('journal-ref')
    journal_obj = None
    if journal_ref is not None:
        journal_obj = Journal(journal_ref=journal_ref)
        session.add(journal_obj)

    licence_name = publication_data.pop('license')
    license_obj = None
    if licence_name is not None:
        license_obj = License(name=licence_name)
        session.add(license_obj)
    
    submitter_obj = Submitter(** publication_data.pop('submitter_norm'))
    session.add(submitter_obj)

    

    publication_obj = Publication(**publication_data,
                                  submitter=submitter_obj,
                                  license=license_obj
                                  )
    session.add(publication_obj)

    for person_obj in person_objs:
        authorship_obj = Authorship(author=person_obj, publication=publication_obj)
        session.add(authorship_obj)

    for i, category_obj in enumerate(category_objs):
        sub_category_obj = sub_category_objs[i]
        pub_cat_obj = PublicationCategory(publication=publication_obj, category=category_obj, 
                                          subcategory=sub_category_obj)
        session.add(pub_cat_obj)
    
    pub_journ_obj = JournalSpecifics(journal = journal_obj, publication=publication_obj)
    session.add(pub_journ_obj)  
    session.commit()


def insert_normalized_json(data_path, success_path):
    f_paths = glob(f'{data_path}/*.json')
    engine = create_engine(DATABASE_URL)
    Session = sessionmaker(bind=engine)
    session = Session()
    for path in f_paths:
        with open(path, 'r') as f:
            publication_data_list = json.load(f)
        for publication_data in publication_data_list:
            insert_publication(publication_data)
        shutil.move(path, Path(success_path)/Path(path).name)
    session.close()


file_sensor_task_norm = FileSensor(
    task_id='file_sensor_task_norm',
    filepath=f'{NORM_DATA_FOLDER}/*.json',
    fs_conn_id='fs_default',
    poke_interval=300,
    dag=convert_to_csv,
)


insert_to_db_task = PythonOperator(
    task_id='insert_to_db_task',
    dag=convert_to_csv,
    trigger_rule='none_failed',
    python_callable=insert_normalized_json,
    op_kwargs={
        'data_path': NORM_DATA_FOLDER,
        'success_path': NORM_SUCCESS_FOLDER,
    },
)

file_sensor_task_norm >> insert_to_db_task