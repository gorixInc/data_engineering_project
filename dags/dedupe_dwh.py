from airflow import DAG 
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.contrib.sensors.file_sensor import FileSensor
from datetime import datetime, timedelta
from sql_scripts.sql_generators import create_deduplication_sql

DEFAULT_ARGS = {
    'owner': 'Tartu',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=1)
}

dedupe_dwh_dag = DAG(
    dag_id='dedupe_dwh', # name of dag
    schedule_interval='0 0 * * *', 
    start_date=datetime(2022,9,14,9,15,0),
    catchup=False, # in case execution has been paused, should it execute everything in between
    default_args=DEFAULT_ARGS, # args assigned to all operators
)

dedupe_publication_sql = """
    DELETE FROM dwh.publication
    WHERE id NOT IN (
        SELECT MIN(id)
        FROM dwh.publication
        GROUP BY title, doi, arxiv_id, update_date
    );
"""

dedupe_publication = PostgresOperator(
    task_id='dedupe_publication',
    postgres_conn_id='postgres_main',
    sql=dedupe_publication_sql,
    dag=dedupe_dwh_dag,
)

# DEDUPING 
dedupe_person_sql = create_deduplication_sql('dwh', 'person', ['first_name', 'last_name', 'third_name'], 
                                             ['authorship', 'publication'], ['author_id', 'submitter_id'])
dedupe_person = PostgresOperator(
    task_id='dedupe_person',
    postgres_conn_id='postgres_main',
    sql=dedupe_person_sql,
    dag=dedupe_dwh_dag,
)

dedupe_journal_sql = create_deduplication_sql('dwh', 'journal', ['journal_ref'], 
                                              ['publication_journal'], ['journal_id'])
dedupe_journal = PostgresOperator(
    task_id='dedupe_journal',
    postgres_conn_id='postgres_main',
    sql=dedupe_journal_sql,
    dag=dedupe_dwh_dag,
)

dedupe_license_sql = create_deduplication_sql('dwh', 'license', ['name'], ['publication'], ['license_id'])
dedupe_license = PostgresOperator(
    task_id='dedupe_license',
    postgres_conn_id='postgres_main',
    sql=dedupe_license_sql,
    dag=dedupe_dwh_dag,
)

dedupe_subcategory_sql = create_deduplication_sql('dwh', 'sub_category', ['name'], 
                                                  ['publication_category'], ['subcategory_id'])
dedupe_subcategory = PostgresOperator(
    task_id='dedupe_subcategory',
    postgres_conn_id='postgres_main',
    sql=dedupe_subcategory_sql,
    dag=dedupe_dwh_dag,
)

dedupe_category_sql = create_deduplication_sql('dwh', 'category', ['name'],
                                                ['publication_category'], ['category_id'])
dedupe_category = PostgresOperator(
    task_id='dedupe_category',
    postgres_conn_id='postgres_main',
    sql=dedupe_category_sql,
    dag=dedupe_dwh_dag,
)

dedupe_publication >> [dedupe_journal, dedupe_person, dedupe_category, dedupe_subcategory, dedupe_license]