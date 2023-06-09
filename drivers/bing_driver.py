import csv
import os
from logging.config import dictConfig

import requests

from utilities.bing_client import BingClient
from utilities.file_reader import FileReader

MAX_LAWS = -1

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
    def __init__(self):
        # TODO: Update this to read from S3.
        self.csv_path = 's3://decoverlaws/laws_input.csv'
        self.file_reader = FileReader()
        self.bing_client = BingClient()

    def run(self):
        # Check if the CSV file is defined and exists.
        if self.csv_path is None or len(self.csv_path) == 0:
            raise Exception('CSV file path is not defined.')
        if not os.path.exists(self.csv_path) and not self.csv_path.startswith('s3://'):
            raise Exception(f'CSV file {self.csv_path} does not exist.')

        # Step I: Read the CSV file from S3.
        # File format is <law_name>,<jurisdiction>,<category>,<sub_category>
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

        # Step III: Use BingClient to search for the law. Search Query: '<law_name> type:pdf'
        for law in laws:
            query = f'{law["law_name"]} type:pdf'
            results = self.bing_client.search(query, law['jurisdiction'])
            # Read the first result and extract the title and url and update the JSON
            if len(results) > 0:
                first_result = results[0]
                law['title'] = first_result.name
                law['url'] = first_result.url

        # Print the laws
        for law in laws:
            print(law)

        # Step IV: Write to a tmp directory all the files with file name being <law_name>.pdf
        tmp_dir = os.path.join(os.getcwd(), 'tmp')

        # Create the tmp directory if it doesn't exist.
        if not os.path.exists(tmp_dir):
            os.makedirs(tmp_dir)

        # Download the PDFs
        for law in laws:
            # Download the PDF using requests
            # Replace all the spaces with underscores in law_name
            law_name = law['law_name'].replace(' ', '_')
            tmp_file_name = f'{law_name}.pdf'
            # Download the PDF
            with open(f'{tmp_dir}/{tmp_file_name}', 'wb') as f:
                f.write(requests.get(law['url']).content)

            # Update the JSON with the tmp file name
            law['tmp_file_name'] = tmp_file_name
            # Write the PDF to tmp directory
            print(f'Wrote {tmp_file_name} to {tmp_dir}')

        # Delete the tmp file
        os.remove(tmp_file_path)


if __name__ == '__main__':
    bing_driver = BingDriver()
    bing_driver.run()
