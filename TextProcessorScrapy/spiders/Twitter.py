import logging

import scrapy
from utils import twitter_utils as tu
from items import TwitterKeywordItem
logging.basicConfig(level=logging.DEBUG,
                    format='[%(asctime)-15s] [%(levelname)8s] [%(name)10s ] - %(message)s (%(filename)s:%(lineno)s)',
                    datefmt='%Y-%m-%d %T'
                    )
logger = logging.getLogger(__name__)

class TwitterSpider(scrapy.Spider):
    name = 'twitter'
    custom_settings = {
        'ITEM_PIPELINES': {'TextProcessorScrapy.pipelines.TwitterPipeline': 500},
        'CONCURRENT_REQUESTS': 2
    }

    def __init__(self, **kwargs):
        super().__init__()
        self.name = 'twitter'
        self.allowed_domains = ['twitter.com']
        self.start_urls = ['https://twitter.com/explore']
        self.keywords = 't'
        if 'keyword' in kwargs:
            self.keywords = kwargs['keyword']

    def parse(self, response):
        logger.info("Twitter Spider starting!")
        item = TwitterKeywordItem()
        for keyword in self.keywords:
            item['keyword'] = keyword
            yield item
        #     results = tu.dataget(self.api, keyword)
        #     for result in results:
        #         item = result
        #         yield item


class Twitter:
    def __init__(self, keywords=None):
        if keywords is not None:
            self.keywords = keywords
        consumer_key = "ioTGfhxK3Fylub82QJmLMB6mB"
        consumer_secret = "fg19r72exdPQfNa0HzBRNPzUrPKZI4YvU4FVDEmlWxkaVcFuKs"
        access_key = "1372722615991738368-bQ4nwuK0vKY95zAIoIAaeqP44DYg56"
        access_secret = "qjZnegLBlk7OvBKGrjFvfjRV6rPu0descZhLdtaILLVlH"

        self.api = tu.get_api(consumer_key, consumer_secret, access_key, access_secret)
