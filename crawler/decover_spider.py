import scrapy
from bs4 import BeautifulSoup


# TODO(Ravi): Move this to a separate file.
def get_text_from_html(html_content):
    soup = BeautifulSoup(html_content, features="html.parser")

    # kill all script and style elements.
    for script in soup(["script", "style"]):
        script.extract()  # rip it out

    # get text
    text = soup.get_text()

    # break into lines and remove leading and trailing space on each
    lines = (line.strip() for line in text.splitlines())
    # break multi-headlines into a line each
    chunks = (phrase.strip() for line in lines for phrase in line.split("  "))
    # drop blank lines
    text = ' '.join(chunk for chunk in chunks if chunk)
    return text


class DecoverSpider(scrapy.Spider):
    name = 'decover_spider'

    def __init__(self, allowed_domains=None, start_urls=None, should_recurse=True, *args, **kwargs):
        super(DecoverSpider, self).__init__(*args, **kwargs)
        self.allowed_domains = allowed_domains
        self.start_urls = start_urls
        self.should_recurse = should_recurse
        self.page_limit = 5

    def parse(self, response):
        # Step I: Extract the text from the webpage
        text_html = response.xpath('//body').get()

        # Step II: Create a dictionary to store the results for this page. It will be a key-value pair of {url: text}.
        text = get_text_from_html(text_html)
        result = {response.url: text}
        self.page_limit -= 1

        # Step III: Follow all the hyperlinks in the same domain including pdfs as well.
        if self.should_recurse and self.page_limit > 0:
            for link in response.xpath('//a/@href'):
                url = response.urljoin(link.extract())
                # Check if the domain is allowed.
                if any(domain in url for domain in self.allowed_domains):
                    yield scrapy.Request(url, callback=self.parse)
        yield result
