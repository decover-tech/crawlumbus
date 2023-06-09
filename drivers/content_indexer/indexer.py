# Indexer reads data from a database and indexes it into the search engine.
# To start with, we will assume that there is a single directory in S3 where all the PDFs are stored.
# These PDFs will then be sent to our Search Index in Elastic.
from utilities.file_reader import FileReader

MAX_DOCUMENTS_TO_INDEX = 1

class Indexer:
    """
    Indexer reads data from a database and indexes it into the search engine.
    """
    def __init__(self):
        self.num_docs_indexed = 0
        self.source_dir = None
        self.file_reader = FileReader()

    def run(self):
        """
        Run the indexer.
        :return:
        """
        self.__read_documents()
        self.__index_to_elastic()
        pass

    def __read_documents(self):
        # Read all the files from the source directory in S3.

    def __index_to_elastic(self):
        pass