import scrapy

from ..items import HsNasaItem
from ..utils.utils import parse_pdf

global count


class nasaSpider(scrapy.Spider):
    name = "NASA"
    custom_settings = {
        'ITEM_PIPELINES': {'TextProcessorScrapy.pipelines.HsNasaPipeline': 400},
    }
    index_url = "https://nasasearch.nasa.gov"
    allowed_domains = ['nasa.gov']
    url_header = 'https://nasasearch.nasa.gov/search?query='
    url_late = '&affiliate=nasa&utf8=%E2%9C%93'
    keyword_file = open("../file/keywords.txt")
    # keywords = 'time sensitive target'
    global count
    url_list = []
    keyword_list = []
    keyword = keyword_file.readline()
    while keyword:
        keyword_list.append(keyword)
        url_list.append(url_header + keyword.replace(' ', '+') + url_late)
        keyword = keyword_file.readline()
    start_urls = url_list
    # start_urls = ['https://nasasearch.nasa.gov/search?query=cyberspace+target&affiliate=nasa&utf8=%E2%9C%93']
    count = 0

    def parse_detail(self, response):
        # content = response.xpath('string(//p|//ol)').extract_first().replace('\r', '').replace('\t', '').replace('\n',
        #                                                                                                     ' ').replace(
        #     '\xa0', ' ')
        content_list = response.xpath('//p/text()|//ol/text()').extract()
        content = ''.join(content_list).replace('\r', '').replace('\t', '').replace('\n', ' ').replace('\xa0', ' ').replace("'", "''")
        item = response.meta['item']
        item['content'] = content
        yield item

    def parse_detail_pdf(self, response):
        content = parse_pdf(response.url)
        content.replace('\n', ' ').replace("'", "")
        item = response.meta['item']
        item['content'] = content
        yield item

    def parse(self, response, keyword_list=keyword_list):
        results = response.xpath('//div[@class="content-block-item result"]')
        global count
        count += 1
        for result in results:
            """
            提取字段
            """
            item = HsNasaItem()
            item["keyword"] = keyword_list[count - 1].replace('\n', '')
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

        next_page = response.xpath('//a[@class="next_page"]/@href').extract_first()
        if next_page is not None:
            next_page = response.urljoin(next_page)
            yield scrapy.Request(next_page, callback=self.parse)
