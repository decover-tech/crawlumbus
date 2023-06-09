class WebPageInfo:
    """
    This class is used to store the information of a webpage.
    """

    def __init__(self, name, url, snippet):
        self.name = name
        self.url = url
        self.snippet = snippet

    def __str__(self):
        return f'Name: {self.name}\n Url: {self.url}\n Snippet: {self.snippet}\n'

    def __dict__(self):
        return {'name': self.name, 'url': self.url, 'snippet': self.snippet}

    def get_url(self):
        return self.url

    def get_name(self):
        return self.name

    def get_snippet(self):
        return self.snippet
