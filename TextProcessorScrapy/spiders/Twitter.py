import scrapy
from ..utils import twitter_utils as tu
from ..items import TwitterKeywordItem


class TwitterSpider(scrapy.Spider):
    name = 'twitter'
    custom_settings = {
        'ITEM_PIPELINES': {'TextProcessorScrapy.pipelines.TwitterPipeline': 500},
    }

    def __init__(self):
        super().__init__()
        self.name = 'twitter'
        self.allowed_domains = ['twitter.com']
        self.start_urls = ['https://twitter.com/explore']
        self.keywords = ['target']
        consumer_key = "ioTGfhxK3Fylub82QJmLMB6mB"
        consumer_secret = "fg19r72exdPQfNa0HzBRNPzUrPKZI4YvU4FVDEmlWxkaVcFuKs"
        access_key = "1372722615991738368-bQ4nwuK0vKY95zAIoIAaeqP44DYg56"
        access_secret = "qjZnegLBlk7OvBKGrjFvfjRV6rPu0descZhLdtaILLVlH"

        self.api = tu.get_api(consumer_key, consumer_secret, access_key, access_secret)

    def data_get(self):
        for keyword in self.keywords:
            results = tu.dataget(self.api, keyword)
            for result in results:
                item = result
                yield item

    def parse(self, response):
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
