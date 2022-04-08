from hashlib import md5
import logging
from datetime import datetime, date, timedelta
import time

import scrapy

from ..items import DataItem

logging.basicConfig(level=logging.DEBUG,
                    format='[%(asctime)-15s] [%(levelname)8s] [%(name)10s ] - %(message)s (%(filename)s:%(lineno)s)',
                    datefmt='%Y-%m-%d %T'
                    )
logger = logging.getLogger(__name__)


class JanesSpider(scrapy.Spider):
    task_id = 0
    name = 'janes'
    custom_settings = {
        'ITEM_PIPELINES': {'TextProcessorScrapy.pipelines.JanesPipeline': 400},
    }

    def __init__(self, *args, **kwargs):
        super(JanesSpider, self).__init__(*args, **kwargs)
        self.allowed_domains = ['janes.com']
        if 'crawl_id' in kwargs['crawl_id']:
            self.crawl_id = kwargs['crawl_id']
        if 'keyword' in kwargs:
            self.keyword = kwargs['keyword'].split('_')[-1]
            self.keyword_type = kwargs['keyword'].split('_')[0]
        if 'database' in kwargs:
            self.database = kwargs['database']
        url = 'https://www.janes.com/search-results?indexCatalogue=all---production&searchQuery=' + self.keyword + \
              '&wordsMode=AllWords&orderBy=Newest'
        self.start_urls.append(url)
        self.num = 0
        self.count = -1

    def parse(self, response):
        logger.info("Jane's Spider Starting!")
        self.count += 1
        news_urls = []
        news_list = response.xpath("//div[@class = 'sf-search-results media-list extra-small-list']")
        # 如果没有找到信息
        item = DataItem()
        if len(news_list) == 0:
            item['title'] = ''
            item['date'] = '0'
            item['keyword'] = self.keyword
            item['url'] = ''
            item['content'] = ''
            if len(item['content'].replace(' ', '').replace("\n", '')) <= 20 or item['content'] == '':
                return
            yield item
            return
        for news in news_list:
            new = news.xpath("./div")
            for n in new:
                href = n.xpath("./a/@href").get()
                self.num += 1
                if self.num > 20:
                    break
                news_urls.append(href)
        for news_url in news_urls:
            yield scrapy.Request(news_url, callback=self.parse_dir_contents)

    def parse_more_page(self, response):
        news_urls = []
        news_list = response.xpath("//div[@class = 'sf-search-results media-list extra-small-list']")
        # 如果没有找到信息
        if len(news_list) == 0:
            item = DataItem()
            item['title'] = ''
            item['date'] = '0'
            item['keyword'] = self.keyword
            item['url'] = ''
            item['content'] = ''
            if len(item['content'].replace(' ', '').replace("\n", '')) <= 20 or item['content'] == '':
                return
            return
        for news in news_list:
            new = news.xpath("./div")
            for n in new:
                href = n.xpath("./a/@href").get()
                self.num += 1
                if self.num > 20:
                    break
            if self.num > 20:
                break
            news_urls.append(href)
        for news_url in news_urls:
            yield scrapy.Request(news_url, callback=self.parse_dir_contents)
            # break

    def parse_dir_contents(self, response):
        # 爬取标题
        news_title = response.xpath("//h1/text()").get()
        # 爬取时间
        try:
            news_time = response.xpath("//p[@itemprop = 'dateModified']/span/text()").get()
            # 对时间进行格式化
            gmt_format = '%d %B %Y'
            publish_time = str(datetime.strptime(news_time, gmt_format))[0:10].replace(' ', '') \
                .replace(':', '').replace('-', '')
        except:
            publish_time = '0'

        # 获取昨天时间
        now_time = (date.today() + timedelta(days=-7)).strftime("%Y%m%d")
        # 判断是否是近两天发表
        # if int(publish_time) < int(now_time):
        #     return
        # 爬取正文
        content_pre = response.selector.xpath("//h1/../p[not(@class)]")
        news_content = content_pre.xpath('string(.)').extract()
        news_text = ''
        if news_content is not None:
            for i in news_content:
                temp = i
                if i == '\n':
                    continue
                # if i == news_content[1]:
                #     i = str(i).replace('\n', '')
                # else:
                #     i = '' + str(i).replace('\n', '')
                news_text = news_text + str(i).replace('\n', '')
                # if temp != news_content[-1]:
                #     print("不是最后一句")
                #     print(str(i))
                #     news_text = news_text + '\n'

        # 爬取作者
        try:
            news_author = response.xpath("//h1/../p[2]/text()").extract()[1]
        except:
            news_author = ''
        news_author = str(news_author).replace("\n", '').replace(" ", '')

        item = DataItem()
        item['source'] = 'janes'
        item['title'] = str(news_title)
        item['date'] = str(publish_time)
        item['keyword'] = self.keyword
        item['url'] = response.url
        item['rowkey_id'] = md5(str(time.time()).encode()).hexdigest()
        item['content'] = str(news_text)
        if len(item['content'].replace(' ', '').replace("\n", '')) <= 20 or item['content'] == '':
            return
        yield item

