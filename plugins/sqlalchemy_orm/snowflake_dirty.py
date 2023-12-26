from sqlalchemy import create_engine, Column, Integer, Text, Date, ForeignKey
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship

Base = declarative_base()

class Journal(Base):
    __tablename__ = 'journal'
    __table_args__ = {'schema': 'raw_data_snowflake'}
    id = Column(Integer, primary_key=True)
    name = Column(Text)
    journal_ref = Column(Text)

class Submitter(Base):
    __tablename__ = 'submitter'
    __table_args__ = {'schema': 'raw_data_snowflake'}
    id = Column(Integer, primary_key=True)
    first_name = Column(Text)
    last_name = Column(Text)
    third_name = Column(Text)

class Version(Base):
    __tablename__ = 'version'
    __table_args__ = {'schema': 'raw_data_snowflake'}
    id = Column(Integer, primary_key=True)
    publication_id = Column(Integer, ForeignKey('raw_data_snowflake.publication.id'))
    name = Column(Text)
    create_date = Column(Date)
    publication = relationship("Publication")
    
class License(Base):
    __tablename__ = 'license'
    __table_args__ = {'schema': 'raw_data_snowflake'}
    id = Column(Integer, primary_key=True)
    name = Column(Text)

class Publication(Base):
    __tablename__ = 'publication'
    __table_args__ = {'schema': 'raw_data_snowflake'}
    id = Column(Integer, primary_key=True)
    title = Column(Text)
    doi = Column(Text)
    arxiv_id = Column(Text)
    update_date = Column(Date)
    comments = Column(Text)
    submitter_id = Column(Integer, ForeignKey('raw_data_snowflake.submitter.id'))
    license_id = Column(Integer, ForeignKey('raw_data_snowflake.license.id'))

    submitter = relationship("Submitter")
    license = relationship("License")

class JournalSpecifics(Base):
    __tablename__ = 'journal_specifics'
    __table_args__ = {'schema': 'raw_data_snowflake'}
    id = Column(Integer, primary_key=True)
    journal_id = Column(Integer, ForeignKey('raw_data_snowflake.journal.id'))
    publication_id = Column(Integer, ForeignKey('raw_data_snowflake.publication.id'))

    journal = relationship("Journal")
    publication = relationship("Publication")

class Person(Base):
    __tablename__ = 'person'
    __table_args__ = {'schema': 'raw_data_snowflake'}
    id = Column(Integer, primary_key=True)
    first_name = Column(Text)
    last_name = Column(Text)
    third_name = Column(Text)

class Authorship(Base):
    __tablename__ = 'authorship'
    __table_args__ = {'schema': 'raw_data_snowflake'}
    id = Column(Integer, primary_key=True)
    author_id = Column(Integer, ForeignKey('raw_data_snowflake.person.id'))
    publication_id = Column(Integer, ForeignKey('raw_data_snowflake.publication.id'))

    author = relationship("Person")
    publication = relationship("Publication")

class SubCategory(Base):
    __tablename__ = 'sub_category'
    __table_args__ = {'schema': 'raw_data_snowflake'}
    id = Column(Integer, primary_key=True)
    name = Column(Text)

class Category(Base):
    __tablename__ = 'category'
    __table_args__ = {'schema': 'raw_data_snowflake'}
    id = Column(Integer, primary_key=True)
    name = Column(Text)

class PublicationCategory(Base):
    __tablename__ = 'publication_category'
    __table_args__ = {'schema': 'raw_data_snowflake'}
    id = Column(Integer, primary_key=True)
    category_id = Column(Integer, ForeignKey('raw_data_snowflake.category.id'))
    subcategory_id = Column(Integer, ForeignKey('raw_data_snowflake.sub_category.id'))
    publication_id = Column(Integer, ForeignKey('raw_data_snowflake.publication.id'))

    category = relationship("Category")
    subcategory = relationship("SubCategory")
    publication = relationship("Publication")