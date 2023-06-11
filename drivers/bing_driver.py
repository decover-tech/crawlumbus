import csv
import os
import re
import logging
from logging.config import dictConfig
import requests
import urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)  # noqa
from utilities.bing_client import BingClient
from utilities.file_reader import FileReader

MAX_LAWS = -1  # Set to -1 to download all laws

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


def normalize_string(input_string: str) -> str:
    # Remove punctuation
    normalized_string = re.sub(r'[^\w\s-]', '', input_string)
    # Convert to lowercase
    normalized_string = normalized_string.lower()
    # Capitalize the first letter of each word
    normalized_string = normalized_string.title()
    # Remove multiple spaces
    normalized_string = re.sub(r'\s+', ' ', normalized_string).strip()
    # Retain hyphens between consecutive words
    return re.sub(r'(\b\w)-(\w\b)', r'\1\2', normalized_string)


def write_laws_to_csv(output_laws):
    # Write the laws with their additional information to a CSV file
    tmp_dir = os.path.join(os.getcwd(), 'tmp')
    with open('output.csv', 'w') as f:
        writer = csv.writer(f)
        writer.writerow(['law_name', 'jurisdiction', 'category', 'sub_category', 'title', 'url', 'file_name'])
        for law in output_laws:
            # Ensure that the file was downloaded successfully.
            if not os.path.exists(f'{tmp_dir}/{law["file_name"]}'):
                continue
            writer.writerow([law['law_name'], law['jurisdiction'], law['category'], law['sub_category'],
                             law['title'], law['url'], law['file_name']])
    logging.info('Wrote output.csv to current directory.')


def download_laws(output_laws):
    # Download the PDFs for the laws and store them in a temp directory
    tmp_dir = os.path.join(os.getcwd(), 'tmp')

    # Create the tmp directory if it doesn't exist.
    if not os.path.exists(tmp_dir):
        os.makedirs(tmp_dir)

    # Download the PDFs
    for law in output_laws:
        # Download the PDF using requests
        try:
            tmp_file_name = law['file_name']
            with open(f'{tmp_dir}/{tmp_file_name}', 'wb') as f:
                response = requests.get(law['url'], verify=False)
                f.write(response.content)
            # Print the file name and the directory where it was downloaded.
            logging.info(f'Downloaded {tmp_file_name} to {tmp_dir}')
        except Exception as e:
            logging.error(f'Failed to download {law["url"]}')
            logging.error(e)


class BingDriver:
    def __init__(self, csv_path: str):
        self.csv_path = csv_path
        self.file_reader = FileReader()
        self.bing_client = BingClient()

    def run(self) -> None:
        self.__validate_csv_path()
        laws = self.__read_laws_from_csv()
        output_laws = self.__search_laws(laws)
        download_laws(output_laws)
        write_laws_to_csv(output_laws)

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
                pdf_result = next((x for x in results if x.url.endswith('.pdf')), None)
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
    bing_driver = BingDriver('s3://decoverlaws/laws_input.csv')
    bing_driver.run()
