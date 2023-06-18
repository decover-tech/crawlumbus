import csv
import os
import re
import logging
from logging.config import dictConfig
import requests
import urllib3

from crawler.utils.helper_methods import normalize_string, unify_csv_format

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)  # noqa
from utilities.bing_client import BingClient
from utilities.file import File

MAX_LAWS = 1  # Set to -1 to download all laws
METADATA_FILE_NAME = 'metadata.csv'

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


class BingDriver:
    def __init__(self, csv_path: str, base_dir: str):
        self.csv_path = csv_path
        self.file_reader = File()
        self.bing_client = BingClient()
        self.target_base_dir = base_dir
        self.file = File()

    def run(self) -> None:
        self.__validate_csv_path()
        laws = self.__read_laws_from_csv()
        output_laws = self.__search_laws(laws)
        self.__download_laws(output_laws)
        self.__write_metadata_to_S3(output_laws)

    def __write_metadata_to_S3(self, output_laws: list[dict[str, str]]):
        # Write the laws with their additional information to a CSV file
        tmp_dir = os.path.join(os.getcwd(), 'tmp')
        delete_dir = False
        if not os.path.exists(tmp_dir):
            delete_dir = True
            os.mkdir(tmp_dir)

        for index, law in enumerate(output_laws):
            # Ensure that the file was downloaded successfully.
            target_file_path = self.get_target_file_path(
                law['category'], law['file_name'], law['jurisdiction'])
            if not self.file.exists(target_file_path):
                output_laws.pop(index)

        with open(METADATA_FILE_NAME, 'w') as f:
            unify_csv_format(f, output_laws)

        self.file.write_file(f, self.get_target_file_path(
            law['category'], METADATA_FILE_NAME, law['jurisdiction']))
        logging.info(f'Wrote {METADATA_FILE_NAME} to current directory.')
        # Delete the file from tmp directory
        os.remove(METADATA_FILE_NAME)
        if delete_dir:
            os.rmdir(tmp_dir)

    def __download_laws(self, output_laws: list):
        # Download the PDFs
        for law in output_laws:
            # Download the PDF using requests
            try:
                tmp_file_name = law['file_name']
                response = requests.get(law['url'], verify=False)
                jurisdiction = law['jurisdiction']
                category = law['category']
                file_name = law['file_name']
                target_file_path = self.get_target_file_path(
                    category, file_name, jurisdiction)
                self.file.write(response.content, target_file_path)
                logging.info(
                    f'Downloaded {tmp_file_name} to {target_file_path}')
            except Exception as e:
                logging.error(f'Failed to download {law["url"]}')
                logging.error(e)

    def get_target_file_path(self, category, file_name, jurisdiction):
        target_file_path = f'{self.target_base_dir}/{jurisdiction}/{category}/{file_name}'
        return target_file_path

    def __validate_csv_path(self):
        # Check if the CSV file is defined and exists.
        if self.csv_path is None or len(self.csv_path) == 0:
            raise Exception('CSV file path is not defined.')
        if not os.path.exists(self.csv_path) and not self.csv_path.startswith('s3://'):
            raise Exception(f'CSV file {self.csv_path} does not exist.')

    def __read_laws_from_csv(self):
        # Read the CSV file and return a list of laws.
        laws = []

        # Use a CSV reader to read the CSV file.
        csv_file_contents = self.file_reader.read(self.csv_path)

        # Write to a tmp file and read it using the CSV reader.
        tmp_file_path = os.path.join(os.getcwd(), 'tmp', 'laws_input.csv')

        # Create tmp directory if it doesn't exist.
        if not os.path.exists(os.path.join(os.getcwd(), 'tmp')):
            os.makedirs(os.path.join(os.getcwd(), 'tmp'))

        with open(tmp_file_path, 'w') as f:
            f.write(csv_file_contents)

        with open(tmp_file_path, 'r') as f:
            reader = csv.reader(f)
            for line in reader:
                # Skip the header
                if line[0] == 'law_name':
                    continue
                law_elem = {
                    'law_name': line[0],
                    'jurisdiction': line[1],
                    'category': line[2],
                    'sub_category': line[3]
                }
                laws.append(law_elem)
                if len(laws) >= MAX_LAWS > 0:
                    break

        # Delete the tmp file
        os.remove(tmp_file_path)

        return laws

    def __search_laws(self, laws):
        # Use BingClient to search for the law. Return a list of laws with additional information
        output_laws = []
        for law in laws:
            query = f'{law["law_name"]} type:pdf'
            results = self.bing_client.search(query, law['jurisdiction'])
            # Read the first result and extract the title and url and update the JSON
            if len(results) > 0:
                # Get the first result that ends with .pdf from the list of results.
                pdf_result = next(
                    (x for x in results if x.url.endswith('.pdf')), None)
                if pdf_result is None:
                    continue
                first_result = pdf_result
                law['title'] = normalize_string(first_result.name)
                law['url'] = first_result.url
                law_name = law['law_name'].replace(' ', '_')
                tmp_file_name = f'{law_name}.pdf'
                tmp_file_name = re.sub(r'[^A-Za-z0-9_.]', '', tmp_file_name)
                law['file_name'] = tmp_file_name
                output_laws.append(law)
        return output_laws


if __name__ == '__main__':
    bing_driver = BingDriver(
        's3://decoverlaws/metadata/laws_input.csv', 's3://decoverlaws')
    bing_driver.run()
