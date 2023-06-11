# Indexer reads data from a database and indexes it into the search engine.
# To start with, we will assume that there is a single directory in S3 where all the PDFs are stored.
# These PDFs will then be sent to our Search Index in Elastic.
from logging.config import dictConfig

from utilities.file import File
from utilities.s3_client import S3Client

MAX_DOCUMENTS_TO_INDEX = 1

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


class Indexer:
    """
    Indexer reads data from a database and indexes it into the search engine.
    """
    def __init__(self, src_dir: str, es_end_point: str):
        self.num_docs_indexed = 0
        self.source_dir = src_dir
        # For now, we will hit the POST API on Decover Master.
        # @app.route('/api/v1/documents', methods=['POST'])
        self.es_end_point = es_end_point
        self.file_reader = File()
        self.s3_client = S3Client()
        self.file_to_contents_map = {}

    def run(self):
        """
        Run the indexer.
        :return:
        """
        self.__read_documents()
        self.__index_to_elastic()
        pass

    def file_to_contents(self) -> dict:
        """
        Returns a map of file name to contents of the file.
        :return:
        """
        return self.file_to_contents_map

    def __read_documents(self):
        # Read all the files from the source directory in S3.
        files = self.s3_client.list_files(self.source_dir)
        # Iterate over the files and index them.
        for file in files:
            file_path = f'{self.source_dir}/{file}'
            contents = self.file_reader.read(file_path)
            self.file_to_contents_map[file] = contents
            self.num_docs_indexed += 1
            if self.num_docs_indexed == MAX_DOCUMENTS_TO_INDEX:
                break


    def __index_to_elastic(self):
        pass


if __name__ == '__main__':
    indexer = Indexer('s3://decoverlaws/india')
    indexer.run()
    print(indexer.file_to_contents())
