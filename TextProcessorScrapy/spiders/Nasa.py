import logging

import scrapy

from ..items import DataItem

logging.basicConfig(level=logging.DEBUG,
                    format='[%(asctime)-15s] [%(levelname)8s] [%(name)10s ] - %(message)s (%(filename)s:%(lineno)s)',
                    datefmt='%Y-%m-%d %T'
                    )
logger = logging.getLogger(__name__)


class nasaSpider(scrapy.Spider):
    name = "nasa"
    custom_settings = {
        'ITEM_PIPELINES': {'TextProcessorScrapy.pipelines.HsNasaPipeline': 400},
        'DOWNLOADER_MIDDLEWARES': { 
                            'TextProcessorScrapy.middlewares.ProxyMiddleware': 90,
                                    }
    }

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.index_url = "https://nasasearch.nasa.gov"
        self.allowed_domains = ['nasa.gov']
        url_header = 'https://nasasearch.nasa.gov/search?query='
        url_late = '&affiliate=nasa&utf8=%E2%9C%93'
        if 'crawl_id' in kwargs['crawl_id']:
            self.crawl_id = kwargs['crawl_id']
        if 'keyword' in kwargs:
            self.keyword = kwargs['keyword'].split('_')[-1]
            self.keyword_type = kwargs['keyword'].split('_')[0]
        if 'database' in kwargs:
            self.database = kwargs['database']
        self.start_urls.append(url_header + self.keyword + url_late)
        self.count = -1

    def parse_detail(self, response):
        """
        获取正文内容
        :param response:
        :return:
        """
        content_list = response.xpath('//p/text()|//ol/text()').extract()
        content = ''.join(content_list).replace('\r', '').replace('\t', '').replace('\n', ' ').replace('\xa0',
                  ' ').replace("'","''")
        item = response.meta['item']
        item['content'] = content
        if len(item['content'].replace(' ', '').replace("\n", '')) <= 20 or item['content'] == '':
            return
        yield item


    def parse(self, response):
        logger.info('NASA Spider Starting!')
        self.count += 1
        results = response.xpath('//div[@class="content-block-item result"]')
        for result in results:
            """
            提取字段
            """
            item = DataItem()
            item["keyword"] = self.keyword
            item["source"] = "NASA"
            item["title"] = result.xpath('./h4/a/text()').extract_first().replace("'", "''")
            item["url"] = result.xpath('./span[1]/text()').extract_first()
            item["date"] = 0
            # description = result.xpath('string(./span[@class="description"])').extract_first()
            # item["description"] = description.replace('\r', '').replace('\t', '').replace('\n', ' ').replace('\xa0',' ')
            if item["url"][-3:] == "pdf":
                continue
            else:
                yield scrapy.Request(url=item["url"], callback=self.parse_detail, meta={'item': item})

        """
        获取下一页链接
        """
        # next_page = response.xpath('//a[@class="next_page"]/@href').extract_first()
        # if next_page is not None:
        #     next_page = response.urljoin(next_page)
        #     yield scrapy.Request(next_page, callback=self.parse)
