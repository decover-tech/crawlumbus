from typing import Optional


class LawElem:
    """
    Represents each row of the law information.
    """

    def __init__(self, law_name: Optional[str] = None, jurisdiction: Optional[str] = None,
                 category: Optional[str] = None, sub_category: Optional[str] = None):
        self._law_name = law_name
        self._jurisdiction = jurisdiction
        self._category = category
        self._sub_category = sub_category
        self._url = None
        self._file_name = None
        self._title = None

    @property
    def title(self):
        return self._title

    @title.setter
    def title(self, value: str):
        self._title = value

    @property
    def file_name(self):
        return self._file_name

    @file_name.setter
    def file_name(self, value: str):
        self._file_name = value

    @property
    def url(self):
        return self._url

    @url.setter
    def url(self, value: str):
        self._url = value

    @property
    def law_name(self):
        return self._law_name

    @law_name.setter
    def law_name(self, value: str):
        self._law_name = value

    @property
    def jurisdiction(self):
        return self._jurisdiction

    @jurisdiction.setter
    def jurisdiction(self, value: str):
        self._jurisdiction = value

    @property
    def category(self):
        return self._category

    @category.setter
    def category(self, value: str):
        self._category = value

    @property
    def sub_category(self):
        return self._sub_category

    @sub_category.setter
    def sub_category(self, value: str):
        self._sub_category = value

    def __str__(self):
        return f'LawElement({self._law_name}, {self._jurisdiction}, {self._category}, {self._sub_category}), {self._url}, {self._file_name}, {self._title}'

    def to_dict(self):
        return {
            'law_name': self._law_name,
            'jurisdiction': self._jurisdiction,
            'category': self._category,
            'sub_category': self._sub_category,
            'url': self._url,
            'file_name': self._file_name,
            'title': self._title
        }