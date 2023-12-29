import sys
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
from datetime import datetime
from sqlalchemy_orm.staging import (Person, Category, 
                              SubCategory, Journal, Publication, License,
                              PublicationJournal, Authorship, PublicationCategory,
                              Version)

from airflow import DAG 
from airflow.operators.python_operator import PythonOperator , BranchPythonOperator
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

DATABASE_URL = "postgresql+psycopg2://airflow:airflow@postgres/main"

RAW_DATA_INPUT = '/tmp/data/raw_data/input'
RAW_DATA_SUCCESS = '/tmp/data/raw_data/success'
RAW_DATA_FAIL = '/tmp/data/raw_data/fail'

NORM_JSON_INPUT = '/tmp/data/norm_jsons/input'
NORM_JSON_SUCCESS = '/tmp/data/norm_jsons/success'
NORM_JSON_FAIL = '/tmp/data/norm_jsons/fail'

upload_to_staging_db = DAG(
    dag_id='process_raw_and_upload_staging', # name of dag
    schedule_interval='* * * * *', 
    start_date=datetime(2022,9,14,9,15,0),
    catchup=False, # in case execution has been paused, should it execute everything in between
    default_args=DEFAULT_ARGS, # args assigned to all operators
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

def normalize_versions(publication_data):
    versions = publication_data['versions']
    versions_norm = []
    for version in versions:
        version_norm = {'version_no': int(version['version'].split('v')[1]),
                        'create_date': datetime.strptime(version['created'], 
                                                         "%a, %d %b %Y %H:%M:%S GMT").isoformat()}
        versions_norm.append(version_norm)
    return versions_norm
    

def normalize_json(publication_data):
    data_norm = deepcopy(publication_data)
    del data_norm['abstract'] 
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
    data_norm['versions_norm'] = normalize_versions(publication_data)
    del data_norm['versions']

    return data_norm

def set_dict(d, key, item):
    if key in d:
        d[key].add(item)
    else:
        d[key] = set([item]) 

def process_file(path, output_path, failed_lines, norm_datas, n_lines, batch_size):
    with open(path, 'rb') as f:
        for line in f:
            try:
                data = json.loads(line.strip())
            except:
                failed_lines.append(line)
                continue                    
            try:
                norm_data = normalize_json(data)
                norm_datas.append(norm_data)
            except:
                failed_lines.append(line)
                continue
            
            n_lines += 1
            if n_lines > batch_size:
                # Not handling exceptions here as it's perferrable to crash as errors here are not related to input data
                with open(f'{output_path}/{uuid.uuid1()}.json', 'w') as f:
                    json.dump(norm_datas, f, indent=4)
                n_lines = 0
                norm_datas = []
            
    return n_lines, norm_datas

def load_and_normalize(data_path, output_path, success_path, fail_path, batch_size=100):
    norm_datas = []
    n_lines = 0
    for path in glob(data_path):
        failed_lines = []
        try:
            n_lines, norm_datas = process_file(path, output_path, failed_lines, norm_datas, n_lines, batch_size)
            if len(norm_datas) > 0:
                with open(f'{output_path}/{uuid.uuid1()}.json', 'w') as f:
                    json.dump(norm_datas, f, indent=4)
            shutil.move(path, Path(success_path)/Path(path).name)
        except:
            shutil.move(path, Path(fail_path)/Path(path).name)
        current_time = datetime.now().strftime("%Y%m%d_%H%M%S")
        if len(failed_lines) > 0:
            with open(Path(fail_path)/f'{Path(path).name}_{current_time}_failed_lines.txt', 'w') as file:
                for item in failed_lines:
                    file.write("%s" % item)
    return

normalize_json_task = PythonOperator(
    task_id='normalize_json_task',
    dag=upload_to_staging_db,
    trigger_rule='none_failed',
    python_callable=load_and_normalize,
    op_kwargs={
        'data_path': f'{RAW_DATA_INPUT}/*.json',
        'output_path': NORM_JSON_INPUT,
        'success_path': RAW_DATA_SUCCESS,
        'fail_path': RAW_DATA_FAIL,
        'batch_size': 100
    },
)


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
    
    submitter_obj = Person(** publication_data.pop('submitter_norm'))
    session.add(submitter_obj)

    
    versions_norm = publication_data.pop('versions_norm')

    publication_obj = Publication(**publication_data,
                                  submitter=submitter_obj,
                                  license=license_obj
                                  )
    session.add(publication_obj)

    for version_norm in versions_norm:
        version_obj = Version(version_no=version_norm['version_no'], 
                              create_date=datetime.fromisoformat(version_norm['create_date']),
                              publication=publication_obj)
        session.add(version_obj)
    
    for person_obj in person_objs:
        authorship_obj = Authorship(author=person_obj, publication=publication_obj)
        session.add(authorship_obj)

    for i, category_obj in enumerate(category_objs):
        sub_category_obj = sub_category_objs[i]
        pub_cat_obj = PublicationCategory(publication=publication_obj, category=category_obj, 
                                          subcategory=sub_category_obj)
        session.add(pub_cat_obj)
    
    pub_journ_obj = PublicationJournal(journal = journal_obj, publication=publication_obj)
    session.add(pub_journ_obj)  
    session.commit()


def insert_normalized_json(data_path, success_path, fail_path):
    f_paths = glob(f'{data_path}/*.json')
    engine = create_engine(DATABASE_URL)
    Session = sessionmaker(bind=engine)
    session = Session()
    for path in f_paths:
        #try:
        with open(path, 'r') as f:
            publication_data_list = json.load(f)
        for publication_data in publication_data_list:
            insert_publication(publication_data, session)
        shutil.move(path, Path(success_path)/Path(path).name)
        # except:
        #     shutil.move(path, Path(fail_path)/Path(path).name)

    session.close()


upload_to_staging_db_task = PythonOperator(
    task_id='upload_to_staging_db_task',
    dag=upload_to_staging_db,
    trigger_rule='none_failed',
    python_callable=insert_normalized_json,
    op_kwargs={
        'data_path': NORM_JSON_INPUT,
        'success_path': NORM_JSON_SUCCESS,
        'fail_path': NORM_JSON_FAIL
    },
)

normalize_json_task >> upload_to_staging_db_task


