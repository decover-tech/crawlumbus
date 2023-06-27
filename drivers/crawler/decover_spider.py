import logging

import scrapy

from drivers.crawler.utils.helper_methods import get_text_from_html, get_pdf_links, download_pdf


class DecoverSpider(scrapy.Spider):
    name = 'decover_spider'

    def __init__(self,
                 allowed_domains=None,
                 start_urls=None,
                 should_recurse=True,
                 max_links=10,
                 download_pdfs=False,
                 file_name='items.jsonl',
                 filter=None,
                 *args, **kwargs):
        super(DecoverSpider, self).__init__(*args, **kwargs)
        self.allowed_domains = allowed_domains
        self.start_urls = start_urls
        self.should_recurse = should_recurse
        self.max_links = max_links
        self.should_download_pdf = download_pdfs
        self.file = file_name
        self.filter = filter

    @property
    def file_name(self):
        return self.file

    def parse(self, response):  # noqa
        # Bail out if the page limit is reached.
        if self.max_links <= 0:
            return

        logging.debug(f"Processing {response.url}")
        # Step I: Extract the text from the webpage
        text_html = response.xpath('//body').get()

        # Step II: Create a dictionary to store the results for this page.
        #          It will be a key-value pair of {url: text}.
        text = get_text_from_html(text_html)
        if self.should_download_pdf:
            pdf_links = get_pdf_links(text_html, self.filter)
            for pdf_link in pdf_links:
                download_pdf(pdf_link['href'])
        result = {response.url: text}
        self.max_links -= 1

        # Step III: Follow all the hyperlinks in the same domain including pdfs as well.
        if self.should_recurse and self.max_links > 0:
            for link in response.xpath('//a/@href'):
                url = response.urljoin(link.extract())
                # Check if the domain is allowed.
                if any(domain in url for domain in self.allowed_domains):
                    # Check if the filter is a prefix of the url.
                    if self.filter in url:
                        yield scrapy.Request(url, callback=self.parse)
        yield result
