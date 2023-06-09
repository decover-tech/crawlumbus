import csv
import os

import requests

from utilities.bing_client import BingClient

MAX_LAWS = -1


class BingDriver:
    def __init__(self):
        # TODO: Update this to read from S3.
        self.csv_path = None
        self.bing_client = BingClient()

    def run(self):
        # Check if the CSV file is defined and exists.
        if self.csv_path is None or len(self.csv_path) == 0:
            raise Exception('CSV file path is not defined.')
        if not os.path.exists(self.csv_path):
            raise Exception(f'CSV file {self.csv_path} does not exist.')

        # Step I: Read the CSV file from S3.
        # File format is <law_name>,<jurisdiction>,<category>,<sub_category>
        laws = []

        # Use a CSV reader to read the CSV file.
        with open(self.csv_path, 'r') as f:
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


if __name__ == '__main__':
    bing_driver = BingDriver()
    bing_driver.run()
