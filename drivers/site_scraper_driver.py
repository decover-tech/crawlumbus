# Use WebSiteCrawlerScrapy to crawl the website.
# Assume that the input is a list of URLs that are read from a CSV file.
import csv
import hashlib
import os
from typing import Tuple
from logging.config import dictConfig

from crawler.utils.helper_methods import normalize_string
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


# Take the last part of the URL and guess the closest file name.
# Keep it normalized, subject to the following rules:
# 1. Remove punctuation
# 2. Convert to lowercase
# 3. Capitalize the first letter of each word
# 4. Remove multiple spaces
# 5. Retain hyphens between consecutive words
def extract_file_name_from_url(url: str) -> str:
    # Remove the protocol and domain name from the URL.
    url = url.replace('https://', '').replace('http://', '')
    # Remove the trailing slash.
    url = url.rstrip('/')
    # Split the URL by slashes.
    url_parts = url.split('/')
    # Take the last part of the URL.
    file_name = url_parts[-1]
    # Take MD5 hash of the URL
    return hashlib.md5(file_name.encode()).hexdigest() + '.txt'


# Does the following:-
# 1. Writes the content of the downloaded pages to separate .txt files.
# 2. Writes a csv file with the metadata of the downloaded laws.
#    Note: CSV Format is: url, file_name
#
# @url_content_map: A dictionary with the URL as the key and the content of the page as the value.
# @return: None
def write_metadata_to_file(url_content_map: dict) -> None:
    tmp_dir = os.path.join(os.getcwd(), 'tmp')
    if not os.path.exists(tmp_dir):
        os.makedirs(tmp_dir)
    with open('metadata.csv', 'w') as f:
        writer = csv.writer(f)
        writer.writerow(['url', 'file_name'])
        for url, content in url_content_map.items():
            # Normalize the URL to get the file name.
            file_name = extract_file_name_from_url(url)
            writer.writerow([url, file_name])
            with open(f'{tmp_dir}/{file_name}', 'w') as f:
                f.write(content)


class SiteScraperDriver:
    def __init__(self, csv_path: str):
        self.csv_path = csv_path
        self.file_reader = FileReader()
        self.scrapy_crawler = WebSiteCrawlerScrapy()
        self.max_pages_per_domain = 2
        self.should_recurse = True

    def run(self) -> dict:
        self.__validate_csv_path()
        urls, allowed_domains = self.__read_urls_from_csv()
        url_content_map = self.__crawl_website(urls, allowed_domains)
        write_metadata_to_file(url_content_map)
        return url_content_map

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
            next(reader, None)
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

    def __crawl_website(self, urls: list, allowed_domains: list) -> dict:
        return self.scrapy_crawler.crawl(urls, allowed_domains, self.should_recurse, self.max_pages_per_domain)


if __name__ == "__main__":
    driver = SiteScraperDriver('/Users/ravidecover/Desktop/site_scraper_input.csv')
    results = driver.run()
    for k, v in results.items():
        print(f'Link downloaded: {k} with number of words: {len(v.split())}')
