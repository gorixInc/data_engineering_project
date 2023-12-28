from sqlalchemy import create_engine, Column, Integer, Text, Date, ForeignKey, DateTime
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship
from sqlalchemy_orm.common_base import Base

schema = 'staging'

class Journal(Base):
    __tablename__ = 'journal'
    __table_args__ = {'schema': schema, 'extend_existing': True}
    id = Column(Integer, primary_key=True)
    name = Column(Text)
    journal_ref = Column(Text)
    processed_at = Column(DateTime)

class Submitter(Base):
    __tablename__ = 'submitter'
    __table_args__ = {'schema': schema, 'extend_existing': True}
    id = Column(Integer, primary_key=True)
    first_name = Column(Text)
    last_name = Column(Text)
    third_name = Column(Text)
    processed_at = Column(DateTime)

class Version(Base):
    __tablename__ = 'version'
    __table_args__ = {'schema': schema, 'extend_existing': True}
    id = Column(Integer, primary_key=True)
    publication_id = Column(Integer, ForeignKey('staging.publication.id'))
    name = Column(Text)
    create_date = Column(Date)
    processed_at = Column(DateTime)

    publication = relationship("sqlalchemy_orm.staging.Publication")
    
class License(Base):
    __tablename__ = 'license'
    __table_args__ = {'schema': schema, 'extend_existing': True}
    id = Column(Integer, primary_key=True)
    name = Column(Text)
    processed_at = Column(DateTime)

class Publication(Base):
    __tablename__ = 'publication'
    __table_args__ = {'schema': schema, 'extend_existing': True}
    id = Column(Integer, primary_key=True)
    title = Column(Text)
    doi = Column(Text)
    arxiv_id = Column(Text)
    update_date = Column(Date)
    comments = Column(Text)
    submitter_id = Column(Integer, ForeignKey('staging.submitter.id'))
    license_id = Column(Integer, ForeignKey('staging.license.id'))
    processed_at = Column(DateTime)

    submitter = relationship("sqlalchemy_orm.staging.Submitter")
    license = relationship("sqlalchemy_orm.staging.License")

class JournalSpecifics(Base):
    __tablename__ = 'journal_specifics'
    __table_args__ = {'schema': schema, 'extend_existing': True}
    id = Column(Integer, primary_key=True)
    journal_id = Column(Integer, ForeignKey('staging.journal.id'))
    publication_id = Column(Integer, ForeignKey('staging.publication.id'))
    processed_at = Column(DateTime)

    journal = relationship("sqlalchemy_orm.staging.Journal")
    publication = relationship("sqlalchemy_orm.staging.Publication")

class Person(Base):
    __tablename__ = 'person'
    __table_args__ = {'schema': schema, 'extend_existing': True}
    id = Column(Integer, primary_key=True)
    first_name = Column(Text)
    last_name = Column(Text)
    third_name = Column(Text)
    processed_at = Column(DateTime)

class Authorship(Base):
    __tablename__ = 'authorship'
    __table_args__ = {'schema': schema, 'extend_existing': True}
    id = Column(Integer, primary_key=True)
    author_id = Column(Integer, ForeignKey('staging.person.id'))
    publication_id = Column(Integer, ForeignKey('staging.publication.id'))
    processed_at = Column(DateTime)

    author = relationship("sqlalchemy_orm.staging.Person")
    publication = relationship("sqlalchemy_orm.staging.Publication")

class SubCategory(Base):
    __tablename__ = 'sub_category'
    __table_args__ = {'schema': schema, 'extend_existing': True}
    id = Column(Integer, primary_key=True)
    name = Column(Text)
    processed_at = Column(DateTime)

class Category(Base):
    __tablename__ = 'category'
    __table_args__ = {'schema': schema, 'extend_existing': True}
    id = Column(Integer, primary_key=True)
    name = Column(Text)
    processed_at = Column(DateTime)

class PublicationCategory(Base):
    __tablename__ = 'publication_category'
    __table_args__ = {'schema': schema, 'extend_existing': True}
    id = Column(Integer, primary_key=True)
    category_id = Column(Integer, ForeignKey('staging.category.id'))
    subcategory_id = Column(Integer, ForeignKey('staging.sub_category.id'))
    publication_id = Column(Integer, ForeignKey('staging.publication.id'))
    processed_at = Column(DateTime)

    category = relationship("sqlalchemy_orm.staging.Category")
    subcategory = relationship("sqlalchemy_orm.staging.SubCategory")
    publication = relationship("sqlalchemy_orm.staging.Publication")