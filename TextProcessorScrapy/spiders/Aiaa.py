import logging
from datetime import datetime
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
        'REDIRECT_ENABLED': False
    }

    def __init__(self, *args, **kwargs):
        super().__init__(**kwargs)
        self.allowed_domains = ["www.aiaa.org"]
        if args:
            self.keywords = args
        url_head = "https://arc.aiaa.org/action/doSearch?AllField="
        url_end = "&sortBy=Earliest&startPage=0&pageSize=20&"
        for keyword in self.keywords:
            self.start_urls.append((url_head + keyword + url_end))
        self.count = -1

    def parse(self, response):
        logger.info("Aiaa Spider Starting!")
        self.count += 1
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
        item['keyword'] = self.keywords[self.count]
        item['source'] = "AIAA"
        item['title'] = str(title)
        item['date'] = str(publish_time)
        item['url'] = response.url
        item['content'] = str(content)
        # yield scrapy.Request(url=href, callback=self.new_parse), item
        yield item
