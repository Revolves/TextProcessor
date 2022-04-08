from hashlib import md5
import logging
from datetime import datetime
import time

import requests

from ..items import DataItem
import scrapy

logging.basicConfig(level=logging.DEBUG,
                    format='[%(asctime)-15s] [%(levelname)8s] [%(name)10s ] - %(message)s (%(filename)s:%(lineno)s)',
                    datefmt='%Y-%m-%d %T'
                    )
logger = logging.getLogger(__name__)


class AiaaSpider(scrapy.Spider):
    name = "aiaa"
    custom_settings = {
        'ITEM_PIPELINES': {'TextProcessorScrapy.pipelines.AiaaPipeline': 500},
        'HTTPERROR_ALLOWED_CODES': [302, 301]
    }

    def __init__(self, *args, **kwargs):
        super().__init__(**kwargs)
        self.allowed_domains = ["www.aiaa.org"]
        if 'crawl_id' in kwargs['crawl_id']:
            self.crawl_id = kwargs['crawl_id']
        if 'keyword' in kwargs:
            self.keyword = kwargs['keyword'].split('_')[-1]
            self.keyword_type = kwargs['keyword'].split('_')[0]
        if 'database' in kwargs:
            self.database = kwargs['database']

    def start_requests(self):
        url_head = "http://arc.aiaa.org/action/doSearch?AllField="
        url_end = "&sortBy=Earliest&startPage=0&pageSize=20&"
        logger.info("Aiaa Spider Starting!")
        yield scrapy.Request(url_head + self.keyword + url_end, callback=self.parse, dont_filter=True)

    def parse(self, response):
        if "The URL has moved " in response.text or response.text == '':
            part_url = requests.get(response.url).xpath("//h4[@class='search-item__title']")
        else:
            part_url = response.xpath("//h4[@class='search-item__title']")
        for url in part_url:
            url = "https://arc.aiaa.org" + str(url.xpath("./a/@href").get())
            yield scrapy.Request(url, callback=self.parse_crawl, dont_filter=True)

    def parse_crawl(self, response):
        title = response.xpath("//h1[@class='citation__title']/text()").get()
        content_time = response.xpath("//span[@class='epub-section__date']/text()").get()
        gmt_format = '%d %b %Y'
        publish_time = str(datetime.strptime(content_time, gmt_format))[0:10].replace(' ', '')
        publish_time = publish_time.replace('-', '')
        # 摘要abstractSection abstractInFull
        # content = response.xpath("//div[@class='article__body']//p//text()").extract()
        # content = response.xpath("//div[@class='hlFld-Abstract']//text()").extract()
        try:
            content1 = response.xpath("//div[@class='article__body ']//text()").extract()
            content = ""
            for con in content1:
                if str(con).find('window.figureViewer=') >= 0:
                    break
                content = content + con
        except:
            content = ""
        item = DataItem()
        item['keyword'] = self.keyword
        item['source'] = "AIAA"
        item['title'] = str(title)
        item['date'] = str(publish_time)
        item['rowkey_id'] = md5(str(time.time()).encode()).hexdigest()
        item['url'] = response.url
        item['content'] = str(content).replace("\n", "").replace("\\", "/").strip().replace("\xa0", '').replace('\\u',
                                                                                                                ',')
        # yield scrapy.Request(url=href, callback=self.new_parse), item
        if len(item['content'].replace(' ', '').replace("\n", '')) <= 20 or item['content'] == '':
            return
        yield item
