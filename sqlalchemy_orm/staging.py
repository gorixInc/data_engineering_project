from sqlalchemy import create_engine, Column, Integer, Text, Date, DateTime, ForeignKey, String
    
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship
#from sqlalchemy_orm.common_base import Base
from sqlalchemy.ext.declarative import declarative_base
StagingBase = declarative_base()

schema = 'staging'

class Journal(StagingBase):
    __tablename__ = 'journal'
    __table_args__ = {'schema': schema, 'extend_existing': True}
    id = Column(Integer, primary_key=True)
    name = Column(String(256))
    journal_ref = Column(String(256))
    processed_at = Column(DateTime)

class Version(StagingBase):
    __tablename__ = 'version'
    __table_args__ = {'schema': schema, 'extend_existing': True}
    id = Column(Integer, primary_key=True)
    publication_id = Column(Integer, ForeignKey('staging.publication.id', ondelete="CASCADE"))
    version_no = Column(Integer)
    create_date = Column(Date)
    publication = relationship("sqlalchemy_orm.staging.Publication", cascade="all,delete")
    processed_at = Column(DateTime)
    
class License(StagingBase):
    __tablename__ = 'license'
    __table_args__ = {'schema': schema, 'extend_existing': True}
    id = Column(Integer, primary_key=True)
    name = Column(String(256))
    processed_at = Column(DateTime)

class Publication(StagingBase):
    __tablename__ = 'publication'
    __table_args__ = {'schema': schema, 'extend_existing': True}
    id = Column(Integer, primary_key=True)
    title = Column(String(1024))
    doi = Column(String(256))
    arxiv_id = Column(String(32))
    update_date = Column(Date)
    comments = Column(String(1024))
    submitter_id = Column(Integer, ForeignKey('staging.person.id'))
    license_id = Column(Integer, ForeignKey('staging.license.id'))
    processed_at = Column(DateTime)

    submitter = relationship("sqlalchemy_orm.staging.Person")
    license = relationship("sqlalchemy_orm.staging.License")

class PublicationJournal(StagingBase):
    __tablename__ = 'publication_journal'
    __table_args__ = {'schema': schema, 'extend_existing': True}
    id = Column(Integer, primary_key=True)
    journal_id = Column(Integer, ForeignKey('staging.journal.id'))
    publication_id = Column(Integer, ForeignKey('staging.publication.id',  ondelete="CASCADE"))
    processed_at = Column(DateTime)

    journal = relationship("sqlalchemy_orm.staging.Journal")
    publication = relationship("sqlalchemy_orm.staging.Publication", cascade="all,delete")

class Person(StagingBase):
    __tablename__ = 'person'
    __table_args__ = {'schema': schema, 'extend_existing': True}
    id = Column(Integer, primary_key=True)
    first_name = Column(String(128))
    last_name = Column(String(128))
    third_name = Column(String(128))
    processed_at = Column(DateTime)

class Authorship(StagingBase):
    __tablename__ = 'authorship'
    __table_args__ = {'schema': schema, 'extend_existing': True}
    id = Column(Integer, primary_key=True)
    author_id = Column(Integer, ForeignKey('staging.person.id'))
    publication_id = Column(Integer, ForeignKey('staging.publication.id', ondelete="CASCADE"))
    processed_at = Column(DateTime)

    author = relationship("sqlalchemy_orm.staging.Person")
    publication = relationship("sqlalchemy_orm.staging.Publication", cascade="all,delete")

class SubCategory(StagingBase):
    __tablename__ = 'sub_category'
    __table_args__ = {'schema': schema, 'extend_existing': True}
    id = Column(Integer, primary_key=True)
    name = Column(String(128))
    processed_at = Column(DateTime)

class Category(StagingBase):
    __tablename__ = 'category'
    __table_args__ = {'schema': schema, 'extend_existing': True}
    id = Column(Integer, primary_key=True)
    name = Column(String(128))
    processed_at = Column(DateTime)

class PublicationCategory(StagingBase):
    __tablename__ = 'publication_category'
    __table_args__ = {'schema': schema, 'extend_existing': True}
    id = Column(Integer, primary_key=True)
    category_id = Column(Integer, ForeignKey('staging.category.id'))
    subcategory_id = Column(Integer, ForeignKey('staging.sub_category.id'))
    publication_id = Column(Integer, ForeignKey('staging.publication.id',  ondelete="CASCADE"))
    processed_at = Column(DateTime)

    category = relationship("sqlalchemy_orm.staging.Category")
    subcategory = relationship("sqlalchemy_orm.staging.SubCategory")
    publication = relationship("sqlalchemy_orm.staging.Publication", cascade="all,delete")