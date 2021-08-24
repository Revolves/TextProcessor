import logging

import scrapy
from ..utils import twitter_utils as tu
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
        'CONCURRENT_REQUESTS': 2,
        "HTTPERROR_ALLOWED_CODES": [400]
    }

    def __init__(self, *args, **kwargs):
        super().__init__()
        self.name = 'twitter'
        self.allowed_domains = ['twitter.com']
        self.start_urls = ['https://twitter.com/explore']
        if args:
            self.keywords = args

    def parse(self, response):
        logger.info("Twitter Spider starting!")
        for keyword in self.keywords:
            item = TwitterKeywordItem()
            item['keyword'] = keyword
            yield item

