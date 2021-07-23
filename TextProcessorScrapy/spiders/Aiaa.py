import logging
from datetime import datetime
from items import AiaaItem

import scrapy

span1 = '-' * 100 + '\n'
span2 = '\n' + '-' * 100
logging.basicConfig(level=logging.DEBUG,
                    format='[%(asctime)-15s] [%(levelname)8s] [%(name)10s ] - %(message)s (%(filename)s:%(lineno)s)',
                    datefmt='%Y-%m-%d %T'
                    )
logger = logging.getLogger(__name__)

class AiaaSpider(scrapy.Spider):
    name = "aiaa"
    custom_settings = {
        'ITEM_PIPELINES': {'TextProcessorScrapy.pipelines.AiaaPipeline': 500},
    }

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.allowed_domains = ["www.aiaa.org"]
        url_head = 'https://arc.aiaa.org/action/doSearch?AllField='
        url_end = "&sortBy=Earliest&startPage=0"
        self.keyword = 't'
        if 'keyword' in kwargs:
            self.keyword = kwargs['keyword']
        self.start_urls.append(url_head + self.keyword + url_end)
        print(span1 + self.start_urls[0] + span2)

    def start_requests(self):
        yield scrapy.Request(self.start_urls[0], callback=self.parse, dont_filter=True)

    def parse(self, response):
        logger.info("Aiaa Spider Starting!")
        print(span1 + 'response.text:\n' + response.text + span2)
        part_url = response.xpath("//h4[@class='search-item__title']")
        print(span1 + 'part_url:' + str(part_url) + span2)
        for url in part_url:
            url = "https://arc.aiaa.org" + str(url.xpath("./a/@href").get())
            print("报告地址" + url)
            yield scrapy.Request(url, callback=self.parse_crawl, dont_filter=True)
            # 每个关键词只读一条
            # break

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
            print(content1)
            content = ""
            for con in content1:
                if str(con).find('window.figureViewer=') >= 0:
                    break
                content = content + con
        except:
            print("正文是图片")
            content = ""
        print("发布时间为：" + str(publish_time))
        item = AiaaItem()
        item['keyword'] = self.keyword
        item['source'] = "AIAA"
        item['title'] = str(title)
        item['date'] = str(publish_time)
        item['url'] = response.url
        item['content'] = str(content)
        # yield scrapy.Request(url=href, callback=self.new_parse), item
        yield item
