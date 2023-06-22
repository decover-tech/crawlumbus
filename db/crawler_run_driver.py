from db.crawler_run import CrawlerRun


class CrawlerRunDriver:
    @staticmethod
    def add_run(db, num_pages_crawled, num_laws_crawled, num_websites_crawled):
        new_run = CrawlerRun(num_pages_crawled, num_laws_crawled, num_websites_crawled)
        db.session.add(new_run)
        db.session.commit()
