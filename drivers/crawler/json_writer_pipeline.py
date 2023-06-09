import json

from itemadapter import ItemAdapter

# TODO: Move it to S3 pipeline using Scrapy's built-in feed exports.
# Refer: https://docs.scrapy.org/en/1.8/topics/feed-exports.html#topics-feed-exports


class JsonWriterPipeline:
    """
    A pipeline to write the scraped data to a JSON file.
    Refer: https://docs.scrapy.org/en/1.8/topics/item-pipeline.html
    """
    def __init__(self):
        self.file = None

    def open_spider(self, spider):
        self.file = open('items.jsonl', 'w')

    def close_spider(self, spider):
        self.file.close()

    def process_item(self, item, spider):
        line = json.dumps(ItemAdapter(item).asdict()) + "\n"
        self.file.write(line)
        return item
