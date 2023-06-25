from typing import Optional

from sqlalchemy import Column, Integer, DateTime, func
from sqlalchemy import String

from db.database import db


class LawElemModel(db.Model):
    __tablename__ = 'law_elem'

    id = Column(Integer, primary_key=True, autoincrement=True)
    created_at = Column(DateTime, default=func.now())
    updated_at = Column(DateTime, onupdate=func.now())
    law_name = Column(String)
    jurisdiction = Column(String)
    category = Column(String)
    sub_category = Column(String)
    url = Column(String)
    file_name = Column(String)
    title = Column(String)

    def __init__(self, law_name: Optional[str] = None, jurisdiction: Optional[str] = None,
                 category: Optional[str] = None, sub_category: Optional[str] = None,
                 url: Optional[str] = None, file_name: Optional[str] = None, title: Optional[str] = None):
        self.law_name = law_name
        self.jurisdiction = jurisdiction
        self.category = category
        self.sub_category = sub_category
        self.url = url
        self.file_name = file_name
        self.title = title
        self.created_at = func.now()
        self.updated_at = func.now()

    def __str__(self):
        return f'LawElement({self.law_name}, {self.jurisdiction}, {self.category}, {self.sub_category}), {self.url}, {self.file_name}, {self.title}'
