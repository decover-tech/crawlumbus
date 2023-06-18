import concurrent
import logging

from drivers.runners.bing_driver import BingDriver
from drivers.runners.site_scraper_driver import SiteScraperDriver


class RootDriver:
    """
    This class is responsible for running all the child drivers.

    @base_dir: The base directory where all the files will be stored.
    @max_pages_per_domain: The maximum number of pages to crawl per domain.
    """
    def __init__(self, base_dir: str, max_pages_per_domain: int = 10, max_laws: int = -1):
        self.base_directory = base_dir
        laws_metadata_file_path = f'{base_dir}/metadata/laws_input.csv'
        site_scraper_metadata_file_path = f'{base_dir}/metadata/site_scraper_input.csv'
        self.bing_driver = BingDriver(csv_path=laws_metadata_file_path, base_dir=base_dir, max_laws=max_laws)
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
                executor.submit(self.bing_driver.ping): self.bing_driver,
                executor.submit(self.site_scraper_driver.ping): self.site_scraper_driver
            }

            for future in concurrent.futures.as_completed(future_to_url):
                url = future_to_url[future]
                try:
                    response = future.result()  # get the result (or exception) of the future
                except Exception as exc:
                    logging.error(f'An error occurred while crawling {url}: {exc}')
                else:
                    logging.info(response)


if __name__ == '__main__':
    base_directory = 's3://decoverlaws'
    parent_driver = RootDriver(
        base_dir=base_directory,
        max_pages_per_domain=10,
        max_laws=1
    )
    parent_driver.run()