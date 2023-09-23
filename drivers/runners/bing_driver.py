import csv
import os
import re
import logging
import requests
from typing import List

from drivers.common.law_elem import LawElem
from drivers.crawler.utils.helper_methods import get_target_file_path, unify_csv_format, normalize_string
from drivers.utilities.bing_client import BingClient
from drivers.utilities.file import File

METADATA_FILE_NAME = 'metadata.csv'


class BingDriver:
    def __init__(self, csv_path: str, base_dir: str, max_laws: int):
        self.csv_path = csv_path
        self.bing_client = BingClient()
        self.target_base_dir = base_dir
        self.max_laws = max_laws
        self.file = File()

    def ping(self) -> str:
        logging.info('Pinging BingDriver...')
        return "Pong!"

    def run(self) -> tuple[int, List[LawElem]]:
        # Check for early return
        if self.max_laws == 0:
            return 0, []
        self.__validate_csv_path()
        laws = self.__read_laws_from_csv()
        output_laws = self.__search_laws(laws)
        num_laws_downloaded = self.__download_laws(output_laws)
        self.__write_metadata(output_laws)
        return num_laws_downloaded, output_laws

    def __write_metadata(self, output_laws: List[LawElem]):
        # Write the laws with their additional information to a CSV file
        tmp_dir = os.path.join(os.getcwd(), 'tmp')
        delete_dir = False
        category = 'law'
        if not os.path.exists(tmp_dir):
            delete_dir = True
            os.mkdir(tmp_dir)

        # Create separate buckets for each jurisdiction
        jurisdiction_to_laws = {}
        for law in output_laws:
            if law.jurisdiction not in jurisdiction_to_laws:
                jurisdiction_to_laws[law.jurisdiction] = []
            jurisdiction_to_laws[law.jurisdiction].append(law)

        # For each jurisdiction, write the laws to a separate CSV file
        for jurisdiction, laws in jurisdiction_to_laws.items():
            target_directory = f'{self.target_base_dir}/{jurisdiction}/{category}'
            data_to_write = []
            for law in laws:
                data_to_write.append({
                    "title": law.title,
                    "jurisdiction": jurisdiction,
                    "category": category,
                    "url": law.url,
                    "file_name": law.file_name
                })
            with open(METADATA_FILE_NAME, 'w') as f:
                unify_csv_format(f, data_to_write)
            # Put the metadata file in S3
            target_file_path = f'{target_directory}/{METADATA_FILE_NAME}'
            self.file.write_file(f, target_file_path)
            logging.info(f'Uploading metadata file to {target_file_path}')
            os.remove(METADATA_FILE_NAME)

        if delete_dir:
            os.rmdir(tmp_dir)

    def __download_laws(self, output_laws: List[LawElem]) -> int:
        # Download the PDFs
        count = 0
        for law in output_laws:
            # Download the PDF using requests
            try:
                response = requests.get(law.url, verify=False)
                jurisdiction = law.jurisdiction
                category = law.category
                file_name = law.file_name
                # if the directory doesn't exist create it and do not bail out
                target_file_path = get_target_file_path(
                    self.target_base_dir, file_name, jurisdiction, category)
                self.file.write(response.content, target_file_path)
                logging.info(
                    f'Downloaded {file_name} to {target_file_path}')
                count += 1
            except Exception as e:
                logging.error(f'Failed to download {law.url}.')
                logging.error(e)
        return count

    def __validate_csv_path(self):
        # Check if the CSV file is defined and exists.
        if self.csv_path is None or len(self.csv_path) == 0:
            raise Exception('CSV file path is not defined.')
        if not os.path.exists(self.csv_path) and not self.csv_path.startswith('s3://'):
            raise Exception(f'CSV file {self.csv_path} does not exist.')

    def __read_laws_from_csv(self) -> List[LawElem]:
        # Read the CSV file and return a list of laws.
        laws = []

        # Use a CSV reader to read the CSV file.
        csv_file_contents = self.file.read(self.csv_path)

        # Write to a tmp file and read it using the CSV reader.
        tmp_file_path = os.path.join(os.getcwd(), 'tmp', 'laws_input.csv')

        # Create tmp directory if it doesn't exist.
        if not os.path.exists(os.path.join(os.getcwd(), 'tmp')):
            os.makedirs(os.path.join(os.getcwd(), 'tmp'))

        with open(tmp_file_path, 'w') as f:
            f.write(csv_file_contents)

        with open(tmp_file_path, 'r') as f:
            reader = csv.reader(f)
            # Skip the header
            next(reader, None)
            for line in reader:
                law_elem = LawElem(
                    law_name=line[0],
                    jurisdiction=line[1],
                    category=line[2],
                    sub_category=line[3]
                )
                laws.append(law_elem)
                if len(laws) >= self.max_laws != -1:
                    break

        # Delete the tmp file
        os.remove(tmp_file_path)
        return laws

    def __search_laws(self, laws: List[LawElem]) -> List[LawElem]:
        # Use BingClient to search for the law. Return a list of laws with additional information
        output_laws = []
        for law in laws:
            query = f'{law.law_name} filetype:pdf'
            try:
                results = self.bing_client.search(query, law.jurisdiction)
            except Exception as e:
                logging.error(f'Failed to search for {query}.')
                logging.error(e)
                continue
            # Read the first result and extract the title and url and update the JSON
            if len(results) > 0:
                # Get the first result that ends with .pdf from the list of results.
                pdf_result = next(
                    (x for x in results if x.url.endswith('.pdf')), None)
                if pdf_result is None:
                    continue
                first_result = pdf_result
                law_name = law.law_name.replace(' ', '_')
                tmp_file_name = re.sub(
                    r'[^A-Za-z0-9_.]', '', f'{law_name}.pdf')
                jurisdiction, category = law.jurisdiction, law.category
                target_file_path = get_target_file_path(
                    self.target_base_dir, tmp_file_name, jurisdiction, category)
                if self.file.exists(target_file_path):
                    continue
                law.title = normalize_string(first_result.name)
                law.url = first_result.url
                law.file_name = tmp_file_name
                output_laws.append(law)
        return output_laws
