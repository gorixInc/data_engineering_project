from sqlalchemy_orm.staging import (Person, Category, 
                              SubCategory, Journal, Publication, License,
                              PublicationJournal, Authorship, PublicationCategory,
                              Version)

from sqlalchemy import create_engine, select
from sqlalchemy.orm import sessionmaker
from datetime import datetime
from glob import glob
import shutil
from pathlib import Path
import json

def insert_publication(publication_data, session):
    person_objs = []
    sub_category_objs = []
    category_objs = []

    for author in publication_data.pop('norm_authors'):
        person_obj = Person(**author, processed_at=None)
        person_objs.append(person_obj)
        session.add(person_obj)

    for category_data in publication_data.pop('norm_categories'):
        category_obj = Category(name = category_data['category_name'], processed_at=None)
        category_objs.append(category_obj)
        session.add(category_obj)
        subcategory_name = category_data['subcategory_name']

        if subcategory_name is not None and not subcategory_name == '':
            sub_category_obj = SubCategory(name = subcategory_name, processed_at=None)
            sub_category_objs.append(sub_category_obj)
            session.add(sub_category_obj)
        else: 
            sub_category_objs.append(None)
        
    
    journal_ref = publication_data.pop('journal-ref')
    journal_obj = None
    if journal_ref is not None:
        journal_obj = Journal(journal_ref=journal_ref, processed_at=None)
        session.add(journal_obj)

    licence_name = publication_data.pop('license')
    license_obj = None
    if licence_name is not None:
        license_obj = License(name=licence_name, processed_at=None)
        session.add(license_obj)
    
    submitter_obj = Person(** publication_data.pop('submitter_norm'), processed_at=None)

    session.add(submitter_obj)

    versions_norm = publication_data.pop('versions_norm')

    publication_obj = Publication(**publication_data,
                                  submitter=submitter_obj,
                                  license=license_obj, 
                                  processed_at=None
                                  )
    session.add(publication_obj)

    for version_norm in versions_norm:
        version_obj = Version(version_no=version_norm['version_no'], 
                              create_date=datetime.fromisoformat(version_norm['create_date']),
                              publication=publication_obj, 
                              processed_at=None)
        session.add(version_obj)
    
    for person_obj in person_objs:
        authorship_obj = Authorship(author=person_obj, publication=publication_obj, processed_at=None)
        session.add(authorship_obj)

    for i, category_obj in enumerate(category_objs):
        sub_category_obj = sub_category_objs[i]
        pub_cat_obj = PublicationCategory(publication=publication_obj, category=category_obj, 
                                          subcategory=sub_category_obj, processed_at=None)
        session.add(pub_cat_obj)
    
    pub_journ_obj = PublicationJournal(journal = journal_obj, publication=publication_obj, processed_at=None)
    session.add(pub_journ_obj)  
    session.commit()


def insert_preprocessed_to_staging(data_path, success_path, fail_path, DATABASE_URL):
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
