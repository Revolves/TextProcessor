# -*- coding: utf-8 -*-
import logging
import re

import scrapy

from ..items import DataItem

logging.basicConfig(level=logging.DEBUG,
                    format='[%(asctime)-15s] [%(levelname)8s] [%(name)10s ] - %(message)s (%(filename)s:%(lineno)s)',
                    datefmt='%Y-%m-%d %T'
                    )
logger = logging.getLogger(__name__)


class TiexueSpider(scrapy.Spider):
    name = 'tiexue'
    custom_settings = {
        'ITEM_PIPELINES': {'TextProcessorScrapy.pipelines.TiexuePipeline': 400},
    }

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.allowed_domains = ['bbs.tiexue.net', 'baidu.com']
        if 'crawl_id' in kwargs['crawl_id']:
            self.crawl_id = kwargs['crawl_id']
        if 'keyword' in kwargs:
            self.keyword = kwargs['keyword']
        if 'database' in kwargs:
            self.database = kwargs['database']
        url = "https://baidu.com/baidu?word=" + self.keyword + "&tn=bds&cl=3&ct=2097152&si=tiexue.net&s=on"
        self.start_urls.append(url)
        self.num = 0
        self.count = 0

    def parse(self, response):
        logger.info("Tiexue Spider Starting!")
        hreflist = []
        # 只爬一条
        # web_node_list = response.xpath('//div[@id="content_left"]//div [@class="result c-container new-pmd"][1]//h3/a/@href').extract()
        # hreflist.append(web_node_list[0])

        # 获取取下一页url
        nextpagehref = response.xpath('//div [@class="page-inner"]/a[last()]/@href').extract()
        nextpageurl = None
        if nextpagehref:
            temp = nextpagehref[0]
            nextpageurl = "https://www.baidu.com" + temp

        # 一页全爬
        web_node_list = response.xpath('//div[@id="content_left"]//h3/a/@href').extract()
        for i in range(len(web_node_list)):
            hreflist.append(web_node_list[i])

        for href in hreflist:
            # //div [@class="s_form"]//span [@class="bg s_ipt_wr quickdelete-wrap"]//input/@value
            searchworditem = response.xpath('//title/text()').get()
            findsearchword = re.compile(r"(.*)_百度搜索")
            searchworditem = str(searchworditem)
            searchkeyword = re.findall(findsearchword, searchworditem)
            searchword = searchkeyword[0]

            yield scrapy.Request(url=href, callback=self.new_parse)
        self.num += 1
        if self.num > 5:
            return
        # 迭代下一页
        if nextpageurl:
            yield scrapy.Request(url=nextpageurl, callback=self.parse)
        self.count += 1

    def new_parse(self, response):
        Source = '铁血社区'
        # Title = response.xpath('//div [@class="postContent border"]//div [@class="contents"]//div [@class="contRow_2"]//div [@class="bbsPosTit"]/h1/text()').get().strip()
        Title = response.xpath(
            '//div [@class="postContent border"]//div [@class="contents"]//div [@class="contRow_2"]//div [@class="bbsPosTit"]/h1/text()').extract_first()
        Website = response.request.url

        Date = response.xpath('//div [@class="postContent border"]//div [@class="date"]/text()').extract_first()
        # Date = "".join(Date).strip()
        Date = str(Date).replace(":", "").replace("/", "").replace(" ", "")
        # Date = int(Date)

        Content = response.xpath(
            '//div [@class="postContent border"]//div [@class="contents"]//div [@class="contRow_2"]//div [@id="postContent"]//text()').getall()
        Content = "".join(Content).replace("\"", "\'").replace("\\", "/").strip()

        # Name = response.xpath('//div [@class="postContent border"]//div [@class="postStart"]//ul//li [@class= "userName"]//div [@class="user_01"]//strong//a/text()').get()

        Keyword = self.keyword
        item = DataItem()
        item = {"keyword": Keyword, "source": Source, "title": Title, "url": Website, "date": Date,
                "content": Content}
        if len(item['content'].replace(' ', '').replace("\n", '')) <= 20 or item['content'] == '':
            return
        yield item
