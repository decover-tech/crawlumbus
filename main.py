import datetime
import time
from logging.config import dictConfig

from drivers.runners.root_driver import RootDriver

# CONFIGURATION PARAMETERS

# The maximum number of pages to crawl per domain.
MAX_PAGES_PER_DOMAIN = 10
# Set to -1 to download all laws
MAX_LAWS = 1
# The base directory where all the files will be stored.
BASE_DIR = 's3://decoverlaws'
# Number of threads to use for the site scraper
MAX_PARALLELISM_SITE_SCRAPER = 1
# The path to the metadata file for the laws
LAWS_METADATA_FILE_PATH = f'{BASE_DIR}/metadata/laws_input.csv'
# The path to the metadata file for the site scraper
SITE_SCRAPER_METADATA_FILE_PATH = f'{BASE_DIR}/metadata/site_scraper_input.csv'
################################################################################

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

def run_root_driver():
    RootDriver(
        base_dir=BASE_DIR,
        max_pages_per_domain=MAX_PAGES_PER_DOMAIN,
        max_laws=MAX_LAWS,
        site_scraper_parallelism=MAX_PARALLELISM_SITE_SCRAPER,
        laws_metadata_file_path=LAWS_METADATA_FILE_PATH,
        site_scraper_metadata_file_path=SITE_SCRAPER_METADATA_FILE_PATH
    ).run()


if __name__ == '__main__':
    while True:
        time.sleep(60 * 60 * 1)
        if datetime.datetime.now().hour == 1:
            run_root_driver()
