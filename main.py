from logging.config import dictConfig

from drivers.runners.root_driver import RootDriver

# The maximum number of pages to crawl per domain.
MAX_PAGES_PER_DOMAIN = 10
# Set to -1 to download all laws
MAX_LAWS = 1
# The base directory where all the files will be stored.
BASE_DIR = 's3://decoverlaws'

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


if __name__ == '__main__':
    base_directory = 's3://decoverlaws'
    parent_driver = RootDriver(
        base_dir=BASE_DIR,
        max_pages_per_domain=MAX_PAGES_PER_DOMAIN,
        max_laws=MAX_LAWS
    )
    parent_driver.run()
