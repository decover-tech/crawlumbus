import datetime
import logging
import os
import threading
import time
from logging.config import dictConfig

from flask import jsonify, render_template, Flask

from db.crawler_run import CrawlerRun, db
from db.crawler_run_driver import CrawlerRunDriver
from drivers.runners.root_driver import RootDriver
from drivers.utilities.remove_prefix_middleware import RemovePrefixMiddleware

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
# The time to sleep between runs of the root driver
TIME_SLEEP_MINUTES = 60 * 60 * 1
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

app = Flask(__name__)
app.config['SQLALCHEMY_DATABASE_URI'] = os.environ.get('SQLALCHEMY_DATABASE_URI')
db.init_app(app)
app.wsgi_app = RemovePrefixMiddleware(app.wsgi_app)

with app.app_context():
    db.create_all()


def run_root_driver():
    """
    Triggers the root driver in a background thread.
    :return:
    """
    count_laws, count_pages = 0, 0
    while True:
        time.sleep(TIME_SLEEP_MINUTES)
        if datetime.datetime.now().hour == 1:
            count_laws, count_pages = RootDriver(
                base_dir=BASE_DIR,
                max_pages_per_domain=MAX_PAGES_PER_DOMAIN,
                max_laws=MAX_LAWS,
                site_scraper_parallelism=MAX_PARALLELISM_SITE_SCRAPER,
                laws_metadata_file_path=LAWS_METADATA_FILE_PATH,
                site_scraper_metadata_file_path=SITE_SCRAPER_METADATA_FILE_PATH
            ).run()
        logging.info(f'Finished running root driver. Found {count_laws} laws and crawled {count_pages} pages.')
        CrawlerRunDriver.add_run(run_number=1, num_pages_crawled=100, num_laws_crawled=50, num_websites_crawled=10)


@app.route('/')
def ping():
    return jsonify({'status': 'ok'})


@app.route('/health')
def handle_health():
    return jsonify({'status': 'ok'})


@app.route('/status')
def handle_status():
    # Renders the status page to indicate the build-status of the laws and websites
    crawler_runs = CrawlerRun.query.all()
    return render_template('status.html', crawler_runs=crawler_runs)


if __name__ == '__main__':
    # Trigger this in a background thread
    t1 = threading.Thread(target=run_root_driver)
    t1.start()

    # Start the server
    app.run(threaded=True, host='0.0.0.0', port=80)
