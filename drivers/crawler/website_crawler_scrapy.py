import json
import logging

import os
import random
import string
from logging.config import dictConfig

import scrapy.crawler as crawler
from multiprocessing import Process, Queue
from twisted.internet import reactor

from drivers.crawler.decover_spider import DecoverSpider

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


def f(q, start_urls, allowed_domains, should_recurse, max_links, download_pdfs, file_name):
    """

    :param file_name:
    :param download_pdfs:
    :param max_links:
    :param q:
    :param start_urls:
    :param allowed_domains:
    :param should_recurse:
    :return:
    """
    try:
        runner = crawler.CrawlerRunner(
            settings={
                'ITEM_PIPELINES': {
                    'drivers.crawler.json_writer_pipeline.JsonWriterPipeline': 1,
                },
                'LOG_LEVEL': 'INFO',
                'USER_AGENT': 'Mozilla/5.0 (iPad; CPU OS 12_2 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) '
                              'Mobile/15E148',
            }
        )
        deferred = runner.crawl(DecoverSpider,
                                start_urls=start_urls,
                                allowed_domains=allowed_domains,
                                should_recurse=should_recurse,
                                max_links=max_links,
                                download_pdfs=download_pdfs,
                                file_name=file_name)
        deferred.addBoth(lambda _: reactor.stop())
        reactor.run(0)
        q.put(None)
    except Exception as e:
        q.put(e)


def get_random_file_name(prefix='items', suffix='jsonl', length=10):
    """
    This method is used to generate a random file name.
    :param prefix:
    :param suffix:
    :return:
    """
    random_number = random.randint(1, 100000)
    random_string = ''.join(random.choice(string.ascii_letters + string.digits) for _ in range(length))
    file_name = f'{prefix}_{random_number}_{random_string}.{suffix}'
    return file_name


class WebSiteCrawlerScrapy:
    """
    This class is used to crawl a website using Scrapy.
    """

    def __init__(self):
        pass

    # The wrapper to make it run more times.
    def crawl(self, start_urls, allowed_domains, should_recurse, max_links, download_pdfs) -> dict:
        # Preprocess the inputs. start_urls should begin with https
        for i in range(len(start_urls)):
            if not start_urls[i].startswith('https'):
                start_urls[i] = 'https://' + start_urls[i]


        logging.info('Crawling the website using Scrapy.')
        feed_export_file_name = get_random_file_name()
        q = Queue()
        p = Process(target=f, args=(q, start_urls,
                                    allowed_domains, should_recurse, max_links, download_pdfs, feed_export_file_name))
        p.start()
        result = q.get()
        p.join()

        # Read the results from the JSON file.
        results = {}

        # Step III: Go through and read the results from the JSON file.
        with open(feed_export_file_name, 'r') as file:
            for line in file:
                item = json.loads(line)
                for url, content in item.items():
                    results[url] = content

        # Step IV: Clean up (Delete the JSON file).
        if os.path.exists(feed_export_file_name):
            os.remove(feed_export_file_name)

        if result is not None:
            raise result
        return results


if __name__ == "__main__":
    # Test the crawler.
    site = "www.texastribune.org"
    domain = "texastribune.org"
    crawler = WebSiteCrawlerScrapy()
    results = crawler.crawl([site], [domain], True, 25, False)
    # Print the set of urls that were crawled.
    print(results.keys())

