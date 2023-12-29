# from airflow import DAG
# from airflow.providers.postgres.operators.postgres import PostgresOperator
# from airflow.utils.dates import days_ago
# from sql_scripts.sql_generators import create_deduplication_sql, append_to_schema, truncate_tables


# default_args = {
#     'owner': 'airflow',
#     'depends_on_past': False,
#     'start_date': days_ago(1),
#     'email_on_failure': False,
#     'email_on_retry': False,
#     'retries': 1,
# }


# schema = 'staging'
# dag = DAG(
#     'dedupe_and_upload_to_dwh',
#     default_args=default_args,
#     description='An Airflow DAG for database deduplication',
#     schedule_interval=None,
# )

# dedupe_publication_sql = """
#     DELETE FROM staging.publication
#     WHERE id NOT IN (
#         SELECT MIN(id)
#         FROM staging.publication
#         GROUP BY title, doi, arxiv_id, update_date
#     );
# """

# dedupe_publication = PostgresOperator(
#     task_id='dedupe_publication',
#     postgres_conn_id='postgres_main',
#     sql=dedupe_publication_sql,
#     dag=dag,
# )


# dedupe_person_sql = create_deduplication_sql('staging', 'person', ['first_name', 'last_name', 'third_name'], 
#                                              ['authorship', 'publication'], ['author_id', 'submitter_id'])
# dedupe_person = PostgresOperator(
#     task_id='dedupe_person',
#     postgres_conn_id='postgres_main',
#     sql=dedupe_person_sql,
#     dag=dag,
# )

# dedupe_journal_sql = create_deduplication_sql('staging', 'journal', ['journal_ref'], 
#                                               ['publication_journal'], ['journal_id'])
# dedupe_journal = PostgresOperator(
#     task_id='dedupe_journal',
#     postgres_conn_id='postgres_main',
#     sql=dedupe_journal_sql,
#     dag=dag,
# )

# dedupe_license_sql = create_deduplication_sql('staging', 'license', ['name'], ['publication'], ['license_id'])
# dedupe_license = PostgresOperator(
#     task_id='dedupe_license',
#     postgres_conn_id='postgres_main',
#     sql=dedupe_license_sql,
#     dag=dag,
# )

# dedupe_subcategory_sql = create_deduplication_sql('staging', 'sub_category', ['name'], 
#                                                   ['publication_category'], ['subcategory_id'])
# dedupe_subcategory = PostgresOperator(
#     task_id='dedupe_subcategory',
#     postgres_conn_id='postgres_main',
#     sql=dedupe_subcategory_sql,
#     dag=dag,
# )

# dedupe_category_sql = create_deduplication_sql('staging', 'category', ['name'],
#                                                 ['publication_category'], ['category_id'])
# dedupe_category = PostgresOperator(
#     task_id='dedupe_category',
#     postgres_conn_id='postgres_main',
#     sql=dedupe_category_sql,
#     dag=dag,
# )

# tables = ['journal', 'version', 'license', 'publication', 
#           'publication_journal', 'person', 'authorship',
#           'sub_category', 'category', 'publication_category']
# upload_to_dwh_sql = append_to_schema('staging', 'dwh', tables)
# upload_to_dwh = PostgresOperator(
#     task_id='upload_to_dwh',
#     postgres_conn_id='postgres_main',
#     sql=upload_to_dwh_sql,
#     dag=dag,
# )

# truncate_staging_sql = truncate_tables('staging', tables)
# truncate_tables = PostgresOperator(
#     task_id='truncate_tables',
#     postgres_conn_id='postgres_main',
#     sql=truncate_staging_sql,
#     dag=dag,
# )

# dedupe_publication >> [dedupe_journal, 
#                        dedupe_person, dedupe_category, 
#                        dedupe_subcategory, dedupe_license] >>upload_to_dwh>> truncate_tables
