from sqlalchemy import create_engine, Column, Integer, Text, Date, DateTime, ForeignKey, String

from sqlalchemy.orm import relationship
#from sqlalchemy_orm.common_base import Base
from sqlalchemy.ext.declarative import declarative_base

DwhBase = declarative_base()

schema = 'dwh'

class Journal(DwhBase):
    __tablename__ = 'journal'
    __table_args__ = {'schema': schema, 'extend_existing': True}
    id = Column(Integer, primary_key=True)
    name = Column(String(256))
    journal_ref = Column(String(256))
    processed_at = Column(DateTime)

class Version(DwhBase):
    __tablename__ = 'version'
    __table_args__ = {'schema': schema, 'extend_existing': True}
    id = Column(Integer, primary_key=True)
    publication_id = Column(Integer, ForeignKey('dwh.publication.id', ondelete="CASCADE"))
    version_no = Column(Integer)
    create_date = Column(Date)
    processed_at = Column(DateTime)

    publication = relationship("sqlalchemy_orm.dwh.Publication", cascade="all,delete")
    
class License(DwhBase):
    __tablename__ = 'license'
    __table_args__ = {'schema': schema, 'extend_existing': True}
    id = Column(Integer, primary_key=True)
    name = Column(String(256))
    processed_at = Column(DateTime)

class Publication(DwhBase):
    __tablename__ = 'publication'
    __table_args__ = {'schema': schema, 'extend_existing': True}
    id = Column(Integer, primary_key=True)
    title = Column(String(1024))
    doi = Column(String(256))
    arxiv_id = Column(String(32))
    update_date = Column(Date)
    comments = Column(String(1024))
    submitter_id = Column(Integer, ForeignKey('dwh.person.id'))
    license_id = Column(Integer, ForeignKey('dwh.license.id'))
    processed_at = Column(DateTime)

    submitter = relationship("sqlalchemy_orm.dwh.Person")
    license = relationship("sqlalchemy_orm.dwh.License")

class PublicationJournal(DwhBase):
    __tablename__ = 'publication_journal'
    __table_args__ = {'schema': schema, 'extend_existing': True}
    id = Column(Integer, primary_key=True)
    journal_id = Column(Integer, ForeignKey('dwh.journal.id'))
    publication_id = Column(Integer, ForeignKey('dwh.publication.id',  ondelete="CASCADE"))
    processed_at = Column(DateTime)

    journal = relationship("sqlalchemy_orm.dwh.Journal")
    publication = relationship("sqlalchemy_orm.dwh.Publication", cascade="all,delete")

class Person(DwhBase):
    __tablename__ = 'person'
    __table_args__ = {'schema': schema, 'extend_existing': True}
    id = Column(Integer, primary_key=True)
    first_name = Column(String(128))
    last_name = Column(String(128))
    third_name = Column(String(128))
    processed_at = Column(DateTime)

class Authorship(DwhBase):
    __tablename__ = 'authorship'
    __table_args__ = {'schema': schema, 'extend_existing': True}
    id = Column(Integer, primary_key=True)
    author_id = Column(Integer, ForeignKey('dwh.person.id'))
    publication_id = Column(Integer, ForeignKey('dwh.publication.id', ondelete="CASCADE"))
    processed_at = Column(DateTime)

    author = relationship("sqlalchemy_orm.dwh.Person")
    publication = relationship("sqlalchemy_orm.dwh.Publication", cascade="all,delete")

class SubCategory(DwhBase):
    __tablename__ = 'sub_category'
    __table_args__ = {'schema': schema, 'extend_existing': True}
    id = Column(Integer, primary_key=True)
    name = Column(String(128))

class Category(DwhBase):
    __tablename__ = 'category'
    __table_args__ = {'schema': schema, 'extend_existing': True}
    id = Column(Integer, primary_key=True)
    name = Column(String(128))
    processed_at = Column(DateTime)

class PublicationCategory(DwhBase):
    __tablename__ = 'publication_category'
    __table_args__ = {'schema': schema, 'extend_existing': True}
    id = Column(Integer, primary_key=True)
    category_id = Column(Integer, ForeignKey('dwh.category.id'))
    subcategory_id = Column(Integer, ForeignKey('dwh.sub_category.id'))
    publication_id = Column(Integer, ForeignKey('dwh.publication.id',  ondelete="CASCADE"))
    processed_at = Column(DateTime)

    category = relationship("sqlalchemy_orm.dwh.Category")
    subcategory = relationship("sqlalchemy_orm.dwh.SubCategory")
    publication = relationship("sqlalchemy_orm.dwh.Publication", cascade="all,delete")