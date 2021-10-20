import logging

import scrapy

from ..items import TwitterKeywordItem

logging.basicConfig(level=logging.DEBUG,
                    format='[%(asctime)-15s] [%(levelname)8s] [%(name)10s ] - %(message)s (%(filename)s:%(lineno)s)',
                    datefmt='%Y-%m-%d %T'
                    )
logger = logging.getLogger(__name__)


class TwitterSpider(scrapy.Spider):
    name = 'twitter'
    custom_settings = {
        'ITEM_PIPELINES': {'TextProcessorScrapy.pipelines.TwitterPipeline': 500},
        'CONCURRENT_REQUESTS': 1,
        "HTTPERROR_ALLOWED_CODES": [400]
    }

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.allowed_domains = ['twitter.com']
        self.start_urls = ['https://twitter.com/explore']
        if 'crawl_id' in kwargs['crawl_id']:
            self.crawl_id = kwargs['crawl_id']
        if 'keyword' in kwargs:
            self.keyword = kwargs['keyword']
        if 'database' in kwargs:
            self.database = kwargs['database']

    def parse(self, response):
        logger.info("Twitter Spider starting!")
        item = TwitterKeywordItem()
        item['keyword'] = self.keyword
        yield item

