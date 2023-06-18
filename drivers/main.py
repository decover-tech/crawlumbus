import concurrent
from logging.config import dictConfig

from bing_driver import BingDriver
from site_scraper_driver import SiteScraperDriver

# The maximum number of pages to crawl per domain.
MAX_PAGES_PER_DOMAIN = 10

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


class ParentDriver:
    """
    This class is responsible for running all the child drivers.

    @base_dir: The base directory where all the files will be stored.
    @max_pages_per_domain: The maximum number of pages to crawl per domain.
    """
    def __init_(self, base_dir: str, max_pages_per_domain: int = 10):
        self.base_directory = base_dir
        laws_metadata_file_path = f'{base_dir}/metadata/laws_input.csv'
        site_scraper_metadata_file_path = f'{base_dir}/metadata/site_scraper_input.csv'
        self.bing_driver = BingDriver(csv_path=laws_metadata_file_path, base_dir=base_dir)
        self.site_scraper_driver = SiteScraperDriver(
            csv_path=site_scraper_metadata_file_path,
            max_pages_per_domain=max_pages_per_domain,
            should_recurse=True,
            should_download_pdf=False,
            base_dir=base_dir
        )

    def run(self):
        with concurrent.futures.ThreadPoolExecutor() as executor:
            future_to_url = {
                executor.submit(self.bing_driver.run): self.bing_driver,
                executor.submit(self.site_scraper_driver.run): self.site_scraper_driver
            }

            for future in concurrent.futures.as_completed(future_to_url):
                url = future_to_url[future]
                try:
                    future.result()  # get the result (or exception) of the future
                except Exception as exc:
                    print(f'An error occurred while crawling {url}: {exc}')


if __name__ == '__main__':
    base_directory = 's3://decoverlaws'
    parent_driver = ParentDriver(base_directory, MAX_PAGES_PER_DOMAIN)
    parent_driver.run()
