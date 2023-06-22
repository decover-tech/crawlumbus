# Use WebSiteCrawlerScrapy to crawl the website.
# Assume that the input is a list of URLs that are read from a CSV file.
import concurrent
import csv
import logging
import os

from drivers.common.input_elem import InputElem
from drivers.crawler.utils.helper_methods import extract_domain, extract_file_name_from_url, unify_csv_format
from drivers.crawler.website_crawler_scrapy import WebSiteCrawlerScrapy
from drivers.utilities.file import File
from typing import List, Tuple

RUN_PARALLEL = True

METADATA_FILE_NAME = 'metadata.csv'


class SiteScraperDriver:
    def __init__(self, csv_path: str,
                 max_pages_per_domain: int,
                 should_recurse: bool,
                 should_download_pdf: bool,
                 base_dir: str,
                 max_parallelism: int,
                 max_websites: int):
        self.file = File()
        self.scrapy_crawler = WebSiteCrawlerScrapy()
        self.csv_path = csv_path
        self.max_pages_per_domain = max_pages_per_domain
        self.should_recurse = should_recurse
        self.should_download_pdf = should_download_pdf
        self.target_base_dir = base_dir
        self.max_parallelism = max_parallelism
        self.is_s3_file = base_dir.startswith('s3://')
        self.max_websites = max_websites

    def ping(self) -> str:
        logging.info('Pinging SiteScraperDriver...')
        return "Pong!"

    def run(self) -> Tuple[int, int]:
        self.__validate_csv_path()
        # Each in_element is a website to crawl.
        in_elements = self.__read_urls_from_csv()
        if self.max_websites > 0:
            in_elements = in_elements[:self.max_websites]
        num_pages_crawled = 0
        # Find length of in_elements it is a list
        num_websites_crawled = in_elements.__len__()

        with concurrent.futures.ThreadPoolExecutor(max_workers=self.max_parallelism) as executor:
            # map the crawling function to each url, returns immediately with future objects
            future_to_url = {executor.submit(
                self.__crawl_website, in_element): in_element for in_element in in_elements}

            for future in concurrent.futures.as_completed(future_to_url):
                in_element = future_to_url[future]
                try:
                    url_content_map = future.result()  # get the result (or exception) of the future
                except Exception as exc:
                    logging.error(
                        f'An error occurred while crawling {in_element.site_name}: {exc}')
                else:
                    self.__write_content_metadata_to_files(in_element, url_content_map)
                    logging.info(f'Finished crawling {in_element.site_name} with {len(url_content_map)} pages.')
                    num_pages_crawled += len(url_content_map)
        return num_pages_crawled, num_websites_crawled

    def __validate_csv_path(self):
        # Check if the CSV file is defined and exists.
        if self.csv_path is None or len(self.csv_path) == 0:
            raise Exception('CSV file path is not defined.')
        if not self.file.exists(self.csv_path):
            raise Exception(f'CSV file {self.csv_path} does not exist.')

    def __read_urls_from_csv(self) -> List[InputElem]:
        # Read the CSV file and return a list of laws.
        in_elements = []

        # Use a CSV reader to read the CSV file.
        csv_file_contents = self.file.read(self.csv_path)

        # Write to a tmp file and read it using the CSV reader.
        tmp_file_path = os.path.join(os.getcwd(), 'tmp', 'urls_input.csv')

        # Create tmp directory if it doesn't exist.
        if not os.path.exists(os.path.join(os.getcwd(), 'tmp')):
            os.makedirs(os.path.join(os.getcwd(), 'tmp'))

        with open(tmp_file_path, 'w') as f:
            f.write(csv_file_contents)

        with open(tmp_file_path, 'r') as f:
            reader = csv.reader(f)
            # Skip the header
            next(reader, None)
            # Headers are url, jurisdiction, category
            for line in reader:
                url = line[0]
                if not url.startswith("http://") and not url.startswith("https://"):
                    url = "http://" + url
                allowed_domains = extract_domain(url)
                in_elements.append(InputElem(url=url, allowed_domains=allowed_domains,
                                             jurisdiction=line[1], category=line[2]))

        # Delete the tmp file
        os.remove(tmp_file_path)

        return in_elements

    def __crawl_website(self, in_element: InputElem) -> dict:
        return self.scrapy_crawler.crawl([in_element.url],
                                         [in_element.allowed_domains],
                                         self.should_recurse,
                                         self.max_pages_per_domain,
                                         self.should_download_pdf)

    # Does the following:-
    # 1. Writes the content of the downloaded pages to separate .txt files.
    # 2. Writes a csv file with the metadata of the downloaded laws.
    #    Note: CSV Format is: url, file_name, jurisdiction, category
    #
    # @url_content_map: A dictionary with the URL as the key and the content of the page as the value.
    # @return: None
    def __write_content_metadata_to_files(self, in_element: InputElem, url_content_map: dict) -> None:
        jurisdiction = in_element.jurisdiction
        category = in_element.category
        site_name = in_element.site_name

        target_directory = f'{self.target_base_dir}/{jurisdiction}/{category}/{site_name}'

        data_to_write = []
        for url, content in url_content_map.items():
            file_name = extract_file_name_from_url(url)
            self.file.write(content, f'{target_directory}/{file_name}')
            data_to_write.append({
                "title": site_name,
                "jurisdiction": jurisdiction,
                "category": category,
                "url": url,
                "file_name": file_name
            })

        with open(METADATA_FILE_NAME, 'w') as f:
            unify_csv_format(f, data_to_write)

        # Put the metadata file in S3
        target_file_path = f'{target_directory}/{METADATA_FILE_NAME}'
        self.file.write_file(f, target_file_path)
        logging.info(f'Uploading metadata file to {target_file_path}')
        os.remove(METADATA_FILE_NAME)
