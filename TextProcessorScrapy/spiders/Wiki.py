# -*- coding: utf-8 -*-
import re
import time

import scrapy
from ..items import DataItem
count = 1

class WikiSpider(scrapy.Spider):
    name = 'Wiki'
    custom_settings = {
        'ITEM_PIPELINES': {'TextProcessorScrapy.pipelines.WikiPipeline': 400},
    }
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.allowed_domains = ['en.wikipedia.org']
    # 获取每个关键词的wiki搜索网址（含不同页的网址）
        if 'keyword' in kwargs:
            self.keyword = kwargs['keyword']
        # 多页爬取，一页20个词条
        for j in range(0, count * 20, 20):
            url = "https://en.wikipedia.org/w/index.php?title=Special:Search&limit=20&offset=" + str(
                j) + "&profile=default&search=" + self.keyword + "&ns0=1"
            self.start_urls.append(url)

    # 进入一级解析，starturl解析
    def parse(self, response):
        hreflist = []
        time.sleep(3)
        # 只爬一条
        # web_node_list = response.xpath('//div[@id="content_left"]//div [@class="result c-container new-pmd"][1]//h3/a/@href').extract()
        # hreflist.append(web_node_list[0])

        # 获取关键字
        currenturl = response.request.url
        findsearchword = re.compile(r".*&search=(.*)&ns0=1")
        searchworditem = str(currenturl).replace("%20", " ")
        searchkeyword = re.findall(findsearchword, searchworditem)
        searchword = searchkeyword[0]
        #
        # print("****************************************************\n")
        # print(searchkeyword)
        # print("****************************************************\n")

        # 一页全爬,此时获取的是域名之后的那段网址
        web_node_list_href = response.xpath(
            '//ul [@class="mw-search-results"]//li [@class="mw-search-result"]//div [@class="mw-search-result-heading"]/a/@href').extract()
        for i in range(len(web_node_list_href)):
            web_node_url = "https://en.wikipedia.org" + str(web_node_list_href[i])
            hreflist.append(web_node_url)

            # print("****************************************************\n")
            # print(web_node_url)
            # print("****************************************************\n")

        # 进入二级解析，具体网址解析
        for href in hreflist:
            yield scrapy.Request(url=href, callback=self.new_parse)

    def new_parse(self, response):
        item = DataItem()
        Source = 'wiki'
        Title = response.xpath('//div [@id="content"]//h1 [@id="firstHeading"]/text()').extract_first()
        Website = response.request.url
        UTCDate = response.xpath('//li [@id="footer-info-lastmod"]/text()').extract_first().strip()
        strlist = UTCDate.split(" ")
        monthlist = ["January", "February", "March", "April", "May", "June", "July", "August", "September", "October",
                     "November", "December"]
        for i in range(len(monthlist)):
            if strlist[7] == monthlist[i]:
                month = i + 1
                if month < 10:
                    month = "0" + str(month)
                else:
                    month = str(month)
                if int(strlist[6]) < 10:
                    strlist[6] = "0" + strlist[6]
                date = strlist[8] + month + strlist[6] + strlist[10]
                Date = date.replace(",", "").replace(":", "")
                break

        # 该时间是UTC时间
        Date = int(Date)

        # 内容出现{/displaystyle }字样，大多是数学公式或是其他的图片（数字或字母）的alt属性值
        Content = response.xpath('//div [@id="mw-content-text"]//p//text()').getall()
        Content = "".join(Content).replace("\n", "").replace("\"", "\'").replace("\\", "/").strip()


        item = {"keyword": self.keyword, "source": Source, "title": Title, "url": Website, "date": Date,
                "content": Content}
        #
        # print("****************************************************\n")
        # print(item)
        # print("****************************************************\n")

        yield item


