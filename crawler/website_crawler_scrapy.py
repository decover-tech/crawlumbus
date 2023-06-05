import json

import os
import scrapy.crawler as crawler
from multiprocessing import Process, Queue
from twisted.internet import reactor

from crawler.decover_spider import DecoverSpider


def f(q, start_urls, allowed_domains, should_recurse):
    """

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
                    'crawler.json_writer_pipeline.JsonWriterPipeline': 1,
                },
                'LOG_LEVEL': 'INFO',
                'USER_AGENT': 'Mozilla/5.0 (iPad; CPU OS 12_2 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Mobile/15E148'
            }
        )
        deferred = runner.crawl(DecoverSpider,
                                start_urls=start_urls,
                                allowed_domains=allowed_domains,
                                should_recurse=should_recurse)
        deferred.addBoth(lambda _: reactor.stop())
        reactor.run(0)
        q.put(None)
    except Exception as e:
        q.put(e)


class WebSiteCrawlerScrapy:
    """
    This class is used to crawl a website using Scrapy.
    """

    def __init__(self):
        pass

    # The wrapper to make it run more times.
    def crawl(self, start_urls, allowed_domains, should_recurse=True):
        q = Queue()
        p = Process(target=f, args=(q, start_urls, allowed_domains, should_recurse))
        p.start()
        result = q.get()
        p.join()

        # Read the results from the JSON file.
        results = {}
        # Step III: Go through and read the results from the JSON file.
        with open('items.jsonl', 'r') as file:
            for line in file:
                item = json.loads(line)
                for url, content in item.items():
                    results[url] = content

        # Step IV: Clean up (Delete the JSON file).
        os.remove('items.jsonl')

        if result is not None:
            raise result
        return results


if __name__ == '__main__':
    start_urls = ["https://www.supremecourt.gov/"]
    allowed_domains = ['supremecourt.gov']
    scrapy_crawler = WebSiteCrawlerScrapy()
    for i in range(1):
        print('run {}...'.format(i))
        spider_results = scrapy_crawler.crawl(start_urls, allowed_domains, should_recurse=True)
        print('spider_results: {}'.format(spider_results))
