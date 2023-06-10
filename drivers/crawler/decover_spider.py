import scrapy

from drivers.crawler.utils.helper_methods import get_text_from_html, get_pdf_links, download_pdf


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
        pdf_links = get_pdf_links(text_html)
        for pdf_link in pdf_links:
            download_pdf(pdf_link['href'])
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
