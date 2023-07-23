import datetime
import logging
import os
import threading
import time
from logging.config import dictConfig

import requests
from flask import jsonify, render_template, Flask, request

from db.crawler_run import CrawlerRun
from db.crawler_run_driver import CrawlerRunDriver
from db.database import db
from db.law_elem import LawElemModel
from db.law_elem_driver import LawElemDriver
from drivers.runners.root_driver import RootDriver
from drivers.utilities.remove_prefix_middleware import RemovePrefixMiddleware

# CONFIGURATION PARAMETERS
# The maximum number of pages to crawl per domain.
MAX_PAGES_PER_DOMAIN = 1000
# Set to -1 to download all laws
MAX_LAWS = -1
# Maximum number of websites to crawl
MAX_WEBSITES = -1
# Number of threads to use for the site scraper
MAX_PARALLELISM_SITE_SCRAPER = 10
# The time to sleep between runs of the root driver in seconds. Currently set to 1 hour (i.e. 3600 seconds).
TIME_SLEEP_SECONDS = 3600
# The base directory where all the files will be stored.
BASE_DIR = os.environ.get('BASE_DIR', "/Users/ravidecover/Desktop/decoverlaws")
# The path to the metadata file for the laws
LAWS_METADATA_FILE_PATH = f'{BASE_DIR}/metadata/laws_input.csv'
SITE_SCRAPER_METADATA_FILE_PATH = f'{BASE_DIR}/metadata/site_scraper_input.csv'
# If SQLALCHEMY_DATABASE_URI is not set, then use sqlite in memory
if not os.environ.get('SQLALCHEMY_DATABASE_URI'):
    os.environ['SQLALCHEMY_DATABASE_URI'] = 'sqlite:///:memory:'
# If TRIGGER_BUILD is set to True, then trigger a build
TRIGGER_BUILD = False
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
app.config['SQLALCHEMY_DATABASE_URI'] = os.environ.get(
    'SQLALCHEMY_DATABASE_URI')
db.init_app(app)
app.wsgi_app = RemovePrefixMiddleware(app.wsgi_app)

with app.app_context():
    db.create_all()


def trigger_run(scrape_laws: bool = True, scrape_websites: bool = True):
    """
    Triggers the root driver.
    :return:
    """
    logging.info("Starting root driver.")
    max_laws = MAX_LAWS if scrape_laws else 0
    max_websites = MAX_WEBSITES if scrape_websites else 0
    count_laws, count_pages, count_websites, laws_indexed = RootDriver(
        base_dir=BASE_DIR,
        max_pages_per_domain=MAX_PAGES_PER_DOMAIN,
        max_laws=max_laws,
        max_websites=max_websites,
        site_scraper_parallelism=MAX_PARALLELISM_SITE_SCRAPER,
        laws_metadata_file_path=LAWS_METADATA_FILE_PATH,
        site_scraper_metadata_file_path=SITE_SCRAPER_METADATA_FILE_PATH).run()
    logging.info(
        f'Finished running root driver. Found {count_laws} laws and crawled {count_pages} pages from {count_websites} websites.')
    with app.app_context():
        CrawlerRunDriver.add_run(db=db,
                                 num_pages_crawled=count_pages,
                                 num_laws_crawled=count_laws,
                                 num_websites_crawled=count_websites)
        add_law_to_status_page(laws_indexed)
    if TRIGGER_BUILD:
        url = "https://app-api.decoverapp.com/index/api/v1/build_index?laws=true"
        response = requests.get(url)
        if response.status_code == 200:
            logging.info("Build request successful.")
        else:
            logging.error(
                f"Build request failed with status code: {response.status_code}")


def add_law_to_status_page(laws_indexed):
    for law in laws_indexed:
        law_name = law.law_name
        jurisdiction = law.jurisdiction
        category = law.category
        sub_category = law.sub_category
        url = law.url
        file_name = law.file_name
        title = law.title
        LawElemDriver.add_law(
            db=db, law_name=law_name, jurisdiction=jurisdiction,
            category=category, sub_category=sub_category, url=url,
            file_name=file_name, title=title)


def run_root_driver():
    """
    Triggers the root driver in a background thread.
    :return:
    """
    while True:
        logging.info("Running the root driver.")
        time.sleep(TIME_SLEEP_SECONDS)
        logging.info(f"Checking if root driver should be triggered. Current hour: {datetime.datetime.now().hour}")
        if datetime.datetime.now().hour == 20:
            trigger_run(True, True)


@app.route('/')
def ping():
    return jsonify({'status': 'ok'})


@app.route('/health')
def handle_health():
    return jsonify({'status': 'ok'})


@app.route('/laws')
def handle_laws_status():
    laws = LawElemModel.query.order_by(LawElemModel.created_at.desc()).all()
    return render_template('status_laws.html', laws=laws)


@app.route('/status')
def handle_status():
    # Renders the status page to indicate the build-status of the laws and websites
    crawler_runs = CrawlerRun.query.order_by(CrawlerRun.id.desc()).all()
    return render_template('status.html', crawler_runs=crawler_runs)


@app.route('/api/v1/index')
def trigger_run_manually():
    # Get request parameter
    scrape_laws_param = request.args.get('scrape_laws')
    scrape_websites_param = request.args.get('scrape_websites')

    scrape_laws = True if scrape_laws_param == 'true' else False
    scrape_websites = True if scrape_websites_param == 'true' else False

    run_thread = threading.Thread(
        target=trigger_run, args=(scrape_laws, scrape_websites))
    run_thread.start()
    return jsonify({'status': 'ok'})


if __name__ == '__main__':
    # Trigger this in a background thread
    t1 = threading.Thread(target=run_root_driver)
    t1.start()

    # Start the server
    app.run(threaded=True, host='0.0.0.0', port=80)
