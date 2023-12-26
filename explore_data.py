# %%
import json
from glob import glob
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from plugins.sqlalchemy_orm.snowflake_dirty import (Person, Submitter, Category, 
                              SubCategory, Journal, Publication, License,
                              JournalSpecifics, Authorship, PublicationCategory)

# %%
filename = "arxiv-metadata-full.json"
data = []
with open(filename, 'rb') as f:
    i = 0
    for line in f:
        # Process each line
        json_data = json.loads(line.strip())
        data.append(json_data)
        i += 1
        if i > 50:
            break
# %%
DATABASE_URL = "postgresql://airflow:airflow@localhost:5432/airflow"
def insert_publication(publication_data):
    DATABASE_URL = "postgresql://airflow:airflow@localhost:5432/airflow"
    engine = create_engine(DATABASE_URL)
    Session = sessionmaker(bind=engine)
    session = Session()

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


# %%
with open('test_data.json', 'r') as f:
    publication_data = json.load(f)

print(publication_data[0])
insert_publication(publication_data[0])
# %%
DATABASE_URL = "postgresql://airflow:airflow@localhost:5432/airflow"
engine = create_engine(DATABASE_URL)
Session = sessionmaker(bind=engine)
session = Session()
session.commit()
# %%
