# Use WebSiteCrawlerScrapy to crawl the website.
# Assume that the input is a list of URLs that are read from a CSV file.
import csv
import os
from typing import Tuple
from logging.config import dictConfig

from crawler.website_crawler_scrapy import WebSiteCrawlerScrapy
from utilities.file_reader import FileReader

dictConfig({
    'version': 1,
    'formatters': {'default': {
        'format': '[%(asctime)s] %(levelname)s in %(module)s: %(message)s',
    }},
    'handlers': {'wsgi': {
        'class': 'logging.StreamHandler',
        'stream': 'ext://sys.stdout',
        'formatter': 'default'
    }},
    'root': {
        'level': 'INFO',
        'handlers': ['wsgi']
    }
})


# In[ ]:
class SiteScraperDriver:
    def __init__(self, csv_path: str):
        self.csv_path = csv_path
        self.file_reader = FileReader()
        self.scrapy_crawler = WebSiteCrawlerScrapy()

    def run(self) -> None:
        self.__validate_csv_path()
        urls, allowed_domains = self.__read_urls_from_csv()
        self.__crawl_website(urls, allowed_domains)

    def __validate_csv_path(self):
        # Check if the CSV file is defined and exists.
        if self.csv_path is None or len(self.csv_path) == 0:
            raise Exception('CSV file path is not defined.')
        if not self.file_reader.exists(self.csv_path):
            raise Exception(f'CSV file {self.csv_path} does not exist.')

    def __read_urls_from_csv(self) -> Tuple[list, list]:
        # Read the CSV file and return a list of laws.
        urls = []
        allowed_domains = []

        # Use a CSV reader to read the CSV file.
        csv_file_contents = self.file_reader.read(self.csv_path)

        # Write to a tmp file and read it using the CSV reader.
        tmp_file_path = os.path.join(os.getcwd(), 'tmp', 'urls_input.csv')

        # Create tmp directory if it doesn't exist.
        if not os.path.exists(os.path.join(os.getcwd(), 'tmp')):
            os.makedirs(os.path.join(os.getcwd(), 'tmp'))

        with open(tmp_file_path, 'w') as f:
            f.write(csv_file_contents)

        with open(tmp_file_path, 'r') as f:
            reader = csv.reader(f)
            for line in reader:
                # Skip the header
                if line[0] == 'url':
                    continue
                urls.append(line[0])
                # Get root domain from URL
                allowed_domains.append(line[0].split('/')[2])

        # Delete the tmp file
        os.remove(tmp_file_path)

        return urls, allowed_domains

    def __crawl_website(self, urls: list, allowed_domains: list):
        # Crawl the website.
        self.scrapy_crawler.crawl(urls, allowed_domains, True, 10)


if __name__ == "__main__":
    driver = SiteScraperDriver('/Users/ravidecover/Desktop/site_scraper_input.csv')
    driver.run()
