import logging

import scrapy

from ..items import HsNasaItem
from ..utils.utils import parse_pdf

logging.basicConfig(level=logging.DEBUG,
                    format='[%(asctime)-15s] [%(levelname)8s] [%(name)10s ] - %(message)s (%(filename)s:%(lineno)s)',
                    datefmt='%Y-%m-%d %T'
                    )
logger = logging.getLogger(__name__)


class nasaSpider(scrapy.Spider):
    name = "nasa"
    custom_settings = {
        'ITEM_PIPELINES': {'TextProcessorScrapy.pipelines.HsNasaPipeline': 400},
    }

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

        self.index_url = "https://nasasearch.nasa.gov"
        self.allowed_domains = ['nasa.gov']
        url_header = 'https://nasasearch.nasa.gov/search?query='
        url_late = '&affiliate=nasa&utf8=%E2%9C%93'
        self.keywords = 'target'
        if 'keyword' in kwargs:
            self.keywords = kwargs['keyword']
        # for keyword in self.keywords:
        self.start_urls.append(url_header + self.keywords + url_late)

    def parse_detail(self, response):
        # content = response.xpath('string(//p|//ol)').extract_first().replace('\r', '').replace('\t', '').replace('\n',
        #                                                                                                     ' ').replace(
        #     '\xa0', ' ')
        content_list = response.xpath('//p/text()|//ol/text()').extract()
        content = ''.join(content_list).replace('\r', '').replace('\t', '').replace('\n', ' ').replace('\xa0',
                                                                                                       ' ').replace("'",
                                                                                                                    "''")
        item = response.meta['item']
        item['content'] = content
        yield item

    def parse_detail_pdf(self, response):
        content = parse_pdf(response.url)
        content.replace('\n', ' ').replace("'", "")
        item = response.meta['item']
        item['content'] = content
        yield item

    def parse(self, response):
        logger.info('NASA Spider starting!')
        results = response.xpath('//div[@class="content-block-item result"]')
        for result in results:
            """
            提取字段
            """
            item = HsNasaItem()
            item["keyword"] = self.keywords
            item["source"] = "NASA"
            item["title"] = result.xpath('./h4/a/text()').extract_first().replace("'", "''")
            item["url"] = result.xpath('./span[1]/text()').extract_first()
            item["date"] = 0
            # description = result.xpath('string(./span[@class="description"])').extract_first()
            # item["description"] = description.replace('\r', '').replace('\t', '').replace('\n', ' ').replace('\xa0',' ')
            if item["url"][-3:] == "pdf":
                continue
                # item["content"] = "Cannot get"
                yield scrapy.Request(url=item["url"], callback=self.parse_detail_pdf, meta={'item': item})
            else:
                yield scrapy.Request(url=item["url"], callback=self.parse_detail, meta={'item': item})

        """
        获取下一页链接
        """
        # next_page = response.xpath('//a[@class="next_page"]/@href').extract_first()
        # if next_page is not None:
        #     next_page = response.urljoin(next_page)
        #     yield scrapy.Request(next_page, callback=self.parse)
