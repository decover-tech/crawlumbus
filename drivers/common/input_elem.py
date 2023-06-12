from typing import Optional


class InputElem:
    """
    Represents of each row of the CSV that is read by Crawlumbus.
    """

    def __init__(self, title: Optional[str] = None, url: Optional[str] = None, allowed_domains: Optional[str] = None,
                 frequency: str = 'once', jurisdiction: Optional[str] = None, category: Optional[str] = None):
        self._title = title
        self._url = url
        self._allowed_domains = allowed_domains if allowed_domains else self.get_root_domain(url)
        self._frequency = frequency
        self._jurisdiction = jurisdiction
        self._category = category

    @property
    def category(self):
        return self._category

    @category.setter
    def category(self, value: str):
        self._category = value

    @property
    def title(self):
        return self._title

    @title.setter
    def title(self, value: str):
        self._title = value

    @property
    def url(self):
        return self._url

    @url.setter
    def url(self, value: str):
        self._url = value
        self._allowed_domains = self.get_root_domain(value)

    @property
    def allowed_domains(self):
        return self._allowed_domains

    @allowed_domains.setter
    def allowed_domains(self, value: Optional[str]):
        self._allowed_domains = value if value else self.get_root_domain(self._url)

    @property
    def frequency(self):
        return self._frequency

    @frequency.setter
    def frequency(self, value: str):
        self._frequency = value

    @property
    def jurisdiction(self):
        return self._jurisdiction

    @jurisdiction.setter
    def jurisdiction(self, value: Optional[str]):
        self._jurisdiction = value

    @staticmethod
    def get_root_domain(url: str):
        from urllib.parse import urlparse
        netloc = urlparse(url).netloc
        return '.'.join(netloc.split('.')[-2:])

    def __str__(self):
        return f'InputElem({self._title}, {self._url}, {self._allowed_domains}, {self._frequency}, {self._jurisdiction})'

    def __dict__(self):
        return {
            'title': self._title,
            'url': self._url,
            'allowed_domains': self._allowed_domains,
            'frequency': self._frequency,
            'jurisdiction': self._jurisdiction,
        }
