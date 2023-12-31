
import json
from datetime import datetime, timedelta
from glob import glob
import pandas as pd
import os
import shutil
from pathlib import Path
from sqlalchemy import create_engine, select
from sqlalchemy.orm import sessionmaker
from datetime import datetime
from sql_scripts.sql_generators import create_deduplication_sql
from sqlalchemy_orm.staging import (Person, Category, 
                              SubCategory, Journal, Publication, License,
                              PublicationJournal, Authorship, PublicationCategory,
                              Version)
from dag_functions.preprocessing import load_and_preprocess
from dag_functions.insert_to_staging import insert_preprocessed_to_staging
from dag_functions.insert_to_neo4j import mark_records_as_processed, create_nodes, create_edges
from airflow import DAG 
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.contrib.sensors.file_sensor import FileSensor


DEFAULT_ARGS = {
    'owner': 'Tartu',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=1)
}

tables_staging = [Authorship, PublicationJournal, PublicationCategory, Version, 
                           Publication, Person, Journal, Category, SubCategory, License]

DATABASE_URL = "postgresql+psycopg2://airflow:airflow@postgres/main"
GRAPH_URL = "bolt://neo4j:7687"
GRAPH_AUTH = ("neo4j", "airflow")

RAW_DATA_INPUT = '/tmp/data/raw_data/input'
RAW_DATA_SUCCESS = '/tmp/data/raw_data/success'
RAW_DATA_FAIL = '/tmp/data/raw_data/fail'

NORM_JSON_INPUT = '/tmp/data/preprocessed_data/input'
NORM_JSON_SUCCESS = '/tmp/data/preprocessed_data/success'
NORM_JSON_FAIL = '/tmp/data/preprocessed_data/fail'
NORM_JSON_PROCESSING = '/tmp/data/preprocessed_data/processing'

upload_to_staging_db = DAG(
    dag_id='ingest_data', # name of dag
    schedule_interval='*/1 * * * *', 
    start_date=datetime(2022,9,14,9,15,0),
    catchup=False, # in case execution has been paused, should it execute everything in between
    default_args=DEFAULT_ARGS, # args assigned to all operators
)



upload_to_staging_db_task = PythonOperator(
    task_id='upload_to_staging_db_task',
    dag=upload_to_staging_db,
    trigger_rule='none_failed',
    python_callable=insert_preprocessed_to_staging,
    op_kwargs={
        'data_path': NORM_JSON_INPUT,
        'success_path': NORM_JSON_SUCCESS,
        'fail_path': NORM_JSON_FAIL,
        'processing_path': NORM_JSON_PROCESSING,
        'DATABASE_URL': DATABASE_URL,
    },
)

def choose_next_task(**kwargs):
    insert_output = kwargs['ti'].xcom_pull(task_ids='upload_to_staging_db_task')
    if insert_output:
        return 'dedupe_publication'
    else:
        return 'end_task'

branch_task = BranchPythonOperator(
    task_id='branch_task',
    python_callable=choose_next_task,
    provide_context=True,
    dag=upload_to_staging_db)

end_task = DummyOperator(
    task_id='end_task',
    dag=upload_to_staging_db)

dedupe_publication_sql = """
    DELETE FROM staging.publication
    WHERE id NOT IN (
        SELECT MIN(id)
        FROM staging.publication
        GROUP BY title, doi, arxiv_id, update_date
    );
"""

dedupe_publication = PostgresOperator(
    task_id='dedupe_publication',
    postgres_conn_id='postgres_main',
    sql=dedupe_publication_sql,
    dag=upload_to_staging_db,
)

upload_to_staging_db_task >> branch_task
branch_task >> end_task
branch_task >> dedupe_publication

# DEDUPING 
dedupe_person_sql = create_deduplication_sql('staging', 'person', ['first_name', 'last_name', 'third_name'], 
                                             ['authorship', 'publication'], ['author_id', 'submitter_id'])
dedupe_person = PostgresOperator(
    task_id='dedupe_person',
    postgres_conn_id='postgres_main',
    sql=dedupe_person_sql,
    dag=upload_to_staging_db,
)

dedupe_journal_sql = create_deduplication_sql('staging', 'journal', ['journal_ref'], 
                                              ['publication_journal'], ['journal_id'])
dedupe_journal = PostgresOperator(
    task_id='dedupe_journal',
    postgres_conn_id='postgres_main',
    sql=dedupe_journal_sql,
    dag=upload_to_staging_db,
)

dedupe_license_sql = create_deduplication_sql('staging', 'license', ['name'], ['publication'], ['license_id'])
dedupe_license = PostgresOperator(
    task_id='dedupe_license',
    postgres_conn_id='postgres_main',
    sql=dedupe_license_sql,
    dag=upload_to_staging_db,
)

dedupe_subcategory_sql = create_deduplication_sql('staging', 'sub_category', ['name'], 
                                                  ['publication_category'], ['subcategory_id'])
dedupe_subcategory = PostgresOperator(
    task_id='dedupe_subcategory',
    postgres_conn_id='postgres_main',
    sql=dedupe_subcategory_sql,
    dag=upload_to_staging_db,
)

dedupe_category_sql = create_deduplication_sql('staging', 'category', ['name'],
                                                ['publication_category'], ['category_id'])
dedupe_category = PostgresOperator(
    task_id='dedupe_category',
    postgres_conn_id='postgres_main',
    sql=dedupe_category_sql,
    dag=upload_to_staging_db,
)

upload_to_dwh = PostgresOperator(
    task_id='upload_to_dwh',
    postgres_conn_id='postgres_main',
    sql="{{ ti.xcom_pull(task_ids='begin_population_task', key='upload_to_dwh_sql') }}",
    dag=upload_to_staging_db,
)


begin_population_task = PythonOperator(
    task_id='begin_population_task',
    dag=upload_to_staging_db,
    #trigger_rule='none_failed',
    python_callable=mark_records_as_processed,
    op_kwargs={
        'DATABASE_URL': DATABASE_URL,
    },
)


create_graph_nodes = PythonOperator(
    task_id='create_graph_nodes_task',
    dag=upload_to_staging_db,
    #trigger_rule='none_failed',
    python_callable=create_nodes,
    op_kwargs={
        'batch_size': 100,
        'DATABASE_URL': DATABASE_URL,
        'GRAPH_URL': GRAPH_URL,
        'GRAPH_AUTH': GRAPH_AUTH
    },
)

create_graph_edges = PythonOperator(
    task_id='create_graph_edges_task',
    dag=upload_to_staging_db,
    #trigger_rule='none_failed',
    python_callable=create_edges,
    op_kwargs={
        'batch_size': 100,
        'DATABASE_URL': DATABASE_URL,
        'GRAPH_URL': GRAPH_URL,
        'GRAPH_AUTH': GRAPH_AUTH

    },
)

def clean_staging_db(**kwargs):
    engine = create_engine(DATABASE_URL, connect_args={'options': '-csearch_path=staging'})
    Session = sessionmaker(bind=engine)
    session = Session()

    start_time = kwargs['ti'].xcom_pull(task_ids='begin_population_task', key='start_time')

    try:
        for table in tables_staging:
            session.query(table).filter(table.processed_at == start_time).delete()

        session.commit()
    except Exception as e:
        session.rollback()
        raise e
    finally:
        session.close()

clean_staging_db_task = PythonOperator(
    task_id='clean_staging_db_task',
    python_callable=clean_staging_db,
    dag=upload_to_staging_db
)

dedupe_publication >> [dedupe_journal, dedupe_person, dedupe_category, dedupe_subcategory, dedupe_license] >> begin_population_task >> [create_graph_nodes, upload_to_dwh]
create_graph_edges.set_upstream(create_graph_nodes)
[create_graph_edges, upload_to_dwh] >> clean_staging_db_task
