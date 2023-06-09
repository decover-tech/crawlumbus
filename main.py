# Use the WebSiteCrawlerScrapy class to crawl an input website.
from drivers.crawler.website_crawler_scrapy import WebSiteCrawlerScrapy

# This is the main driver file. It should be broken down into the following steps:-
# Step I: Read CSV from an S3 file.
# Step 2: For each row in the CSV, crawl the website. This should be done in parallel.
# Step 3: Emit out WebPageInfo objects to an S3 bucket.
# Step 4: Write the updated state to a Postgres table.
# Step 5: Read the contents and send them to Decover index. This should be done in parallel.

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
