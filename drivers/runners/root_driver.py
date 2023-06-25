import concurrent
import logging
from typing import Tuple

from drivers.runners.bing_driver import BingDriver
from drivers.runners.site_scraper_driver import SiteScraperDriver


class RootDriver:
    """
    This class is responsible for running all the child drivers.

    @base_dir: The base directory where all the files will be stored.
    @max_pages_per_domain: The maximum number of pages to crawl per domain.
    """

    def __init__(self,
                 base_dir: str,
                 laws_metadata_file_path: str,
                 site_scraper_metadata_file_path: str,
                 max_pages_per_domain: int = 10,
                 max_laws: int = -1,
                 max_websites: int = -1,
                 site_scraper_parallelism: int = 10):
        self.site_scraper_parallelism = site_scraper_parallelism
        self.bing_driver = BingDriver(
            csv_path=laws_metadata_file_path, base_dir=base_dir, max_laws=max_laws)
        self.site_scraper_driver = SiteScraperDriver(
            csv_path=site_scraper_metadata_file_path,
            max_pages_per_domain=max_pages_per_domain,
            should_recurse=True,
            should_download_pdf=False,
            base_dir=base_dir,
            max_parallelism=site_scraper_parallelism,
            max_websites=max_websites)

    def run(self) -> Tuple[int, int, int]:
        """
        Runs the root driver.
        :return: A tuple indicating the response of each driver.
        """
        count_laws, count_pages, count_websites = 0, 0, 0
        with concurrent.futures.ThreadPoolExecutor() as executor:
            future_to_result = {
                executor.submit(self.bing_driver.run): self.bing_driver,
                executor.submit(self.site_scraper_driver.run): self.site_scraper_driver
            }

            # Return a tuple indicating the response of each driver.
            # Bing driver returns the number of laws found.
            # Site scraper driver returns the number of pages crawled.
            for future in concurrent.futures.as_completed(future_to_result):
                driver = future_to_result[future]
                try:
                    result = future.result()
                except Exception as exc:
                    logging.error(
                        f'An error occurred while running {driver}: {exc}')
                else:
                    if isinstance(driver, BingDriver):
                        count_laws = result
                    elif isinstance(driver, SiteScraperDriver):
                        count_pages, count_websites = result
        return count_laws, count_pages, count_websites
