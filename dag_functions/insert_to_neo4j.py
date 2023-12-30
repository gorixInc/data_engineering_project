from py2neo import Graph, Node, Relationship
from sqlalchemy import create_engine, select
from sqlalchemy.orm import sessionmaker
from datetime import datetime
from sql_scripts.sql_generators import append_to_schema
from sqlalchemy_orm.staging import (Person, Category, 
                              SubCategory, Journal, Publication, License,
                              PublicationJournal, Authorship, PublicationCategory,
                              Version)

tables_staging = [Authorship, PublicationJournal, PublicationCategory, Version, 
                           Publication, Person, Journal, Category, SubCategory, License]
tables = ['journal', 'version', 'license', 'publication', 
          'publication_journal', 'person', 'authorship',
          'sub_category', 'category', 'publication_category']

def mark_records_as_processed(DATABASE_URL, **kwargs,):
    engine = create_engine(DATABASE_URL, connect_args={'options': '-csearch_path=staging'})
    Session = sessionmaker(bind=engine)
    session = Session()

    start_time = datetime.utcnow()
    upload_to_dwh_sql = append_to_schema('staging', 'dwh', tables, start_time)

    for table in tables_staging:
        session.query(table).filter(table.processed_at.is_(None)).update({"processed_at": start_time})
    session.commit()

    session.close()
    kwargs['ti'].xcom_push(key='start_time', value=start_time.isoformat())
    kwargs['ti'].xcom_push(key='upload_to_dwh_sql', value=upload_to_dwh_sql)

def create_publication_nodes(session, graph, batch_size, start_time):
    offset = 0
    while True:
        sql_query = f"""
            SELECT id, title, doi, arxiv_id, update_date, comments
            FROM publication
            WHERE processed_at = '{start_time}'
            LIMIT {batch_size} OFFSET {offset}
        """

        results = session.execute(sql_query).fetchall()

        if not results:
            break

        for publication in results:
            node = Node('Publication', 
                            id=publication.id,
                            title=publication.title,
                            doi=publication.doi,
                            arxiv_id = publication.arxiv_id,
                            update_date = publication.update_date,
                            comments = publication.comments)
            node.__primarylabel__ = 'Publication'
            node.__primarykey__ = 'id'                   
            graph.merge(node)

        offset += batch_size


def create_person_nodes(session, graph, batch_size, start_time):
    offset = 0
    while True:
        sql_query = f"""
            SELECT id, first_name, last_name, third_name
            FROM person
            WHERE processed_at = '{start_time}'
            LIMIT {batch_size} OFFSET {offset}
        """

        results = session.execute(sql_query).fetchall()

        if not results:
            break

        for person in results:
            node = Node('Person',
                            id=person.id,
                            first_name=person.first_name,
                            last_name=person.last_name,
                            third_name=person.third_name)
            node.__primarylabel__ = 'Person'
            node.__primarykey__ = 'id'
            graph.merge(node)

        offset += batch_size

def create_journal_nodes(session, graph, batch_size, start_time):
    offset = 0
    while True:
        sql_query = f"""
            SELECT id, name, journal_ref
            FROM journal
            WHERE processed_at = '{start_time}'
            LIMIT {batch_size} OFFSET {offset}
        """

        results = session.execute(sql_query).fetchall()

        if not results:
            break

        for journal in results:
            node = Node('Journal', 
                            id=journal.id,
                            name=journal.name,
                            journal_ref=journal.journal_ref)
            node.__primarylabel__ = 'Journal'
            node.__primarykey__ = 'id'
            graph.merge(node)

        offset += batch_size

def create_category_nodes(session, graph, batch_size, start_time):
    offset = 0
    while True:
        sql_query = f"""
            SELECT id, name
            FROM category
            WHERE processed_at = '{start_time}'
            LIMIT {batch_size} OFFSET {offset}
        """

        results = session.execute(sql_query).fetchall()

        if not results:
            break

        for category in results:
            node = Node('Category', 
                            id=category.id,
                            name=category.name)
            node.__primarylabel__ = 'Category'
            node.__primarykey__ = 'id'
            graph.merge(node)

        offset += batch_size

def create_nodes(batch_size, DATABASE_URL, GRAPH_URL, GRAPH_AUTH, **kwargs):
    graph = Graph(GRAPH_URL, auth=GRAPH_AUTH)

    engine = create_engine(DATABASE_URL, connect_args={'options': '-csearch_path=staging'})
    Session = sessionmaker(bind=engine)
    session = Session()

    start_time = kwargs['ti'].xcom_pull(task_ids='begin_population_task', key='start_time')

    create_publication_nodes(session, graph, batch_size, start_time)
    create_person_nodes(session, graph, batch_size, start_time)
    create_journal_nodes(session, graph, batch_size, start_time)
    create_category_nodes(session, graph, batch_size, start_time)

    session.close()


def merge_relationship(graph, node1_label, node1_id, node2_label, node2_id, relationship_type):
    node1 = graph.nodes.match(node1_label, id=node1_id).first()
    node2 = graph.nodes.match(node2_label, id=node2_id).first()

    if node1 and node2:
        node1.__primarylabel__ = node1_label
        node1.__primarykey__ = 'id'
        node2.__primarylabel__ = node2_label
        node2.__primarykey__ = 'id'

        rel = Relationship(node1, relationship_type, node2)
        graph.merge(rel)

def create_relationships(session, graph, batch_size, start_time):
    offset = 0
    while True:
        sql_query = f"""
            SELECT pub.id AS publication, jou.id AS journal, 
                ARRAY_AGG(per.id) AS persons, ARRAY_AGG(cat.id) AS categories 
            FROM publication pub
            JOIN authorship aus ON pub.id = aus.publication_id
            LEFT JOIN person per ON aus.author_id = per.id AND per.processed_at = '{start_time}'
            JOIN publication_journal pjo ON pub.id = pjo.publication_id
            LEFT JOIN journal jou ON pjo.journal_id = jou.id AND jou.processed_at = '{start_time}'
            JOIN publication_category pct ON pub.id = pct.publication_id
            LEFT JOIN category cat ON pct.category_id = cat.id AND cat.processed_at = '{start_time}'
            WHERE pub.processed_at = '{start_time}'
            GROUP BY pub.id, jou.id
            ORDER BY publication
            LIMIT {batch_size} OFFSET {offset}
        """

        results = session.execute(sql_query).fetchall()

        if not results:
            break

        for result in results:
            publication_id, journal_id, person_ids, category_ids = result

            for person_id in set(person_ids):
                if person_id is not None:
                    merge_relationship(graph, 'Person', person_id, 'Publication', publication_id, 'WRITTEN_BY')

                    for coworker_id in set(person_ids):
                        if coworker_id is not None and coworker_id != person_id:
                            merge_relationship(graph, 'Person', person_id, 'Person', coworker_id, 'COWORKED')

            if journal_id is not None:
                merge_relationship(graph, 'Publication', publication_id, 'Journal', journal_id, 'PUBLISHED_IN')

            for person_id, category_id in zip(person_ids, category_ids):
                if person_id is not None and category_id is not None:
                    merge_relationship(graph, 'Person', person_id, 'Category', category_id, 'CONTRIBUTES_TO')

        offset += batch_size

def create_edges(batch_size, DATABASE_URL, GRAPH_URL, GRAPH_AUTH, **kwargs):
    graph = Graph(GRAPH_URL, auth=GRAPH_AUTH)

    engine = create_engine(DATABASE_URL, connect_args={'options': '-csearch_path=staging'})
    Session = sessionmaker(bind=engine)
    session = Session()

    start_time = kwargs['ti'].xcom_pull(task_ids='begin_population_task', key='start_time')

    create_relationships(session, graph, batch_size, start_time)

    session.close()