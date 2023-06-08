# Use the WebSiteCrawlerScrapy class to crawl an input website.
from crawler.website_crawler_scrapy import WebSiteCrawlerScrapy

if __name__ == '__main__':
    # Step I: Get the input website.
    website = 'https://doj.gov.in/'
    domain = 'gov.in'

    # Step II: Crawl the website.
    crawler = WebSiteCrawlerScrapy()
    results = crawler.crawl([website], [domain], should_recurse=False)

    # Step III: Print the results.
    print('Number of pages crawled: ', len(results))
    for url, content in results.items():
        print("URL: ", url)
        print("Content: ", content)
        print()
