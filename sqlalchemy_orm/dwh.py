from sqlalchemy import create_engine, Column, Integer, Text, Date, ForeignKey
from sqlalchemy.orm import relationship
from sqlalchemy_orm.common_base import Base

class test_table(Base):
    __tablename__ = 'test_table'
    __table_args__ = {'schema': 'dwh', 'extend_existing': True}
    id = Column(Integer, primary_key=True)
    name = Column(Text)
    journal_ref = Column(Text)