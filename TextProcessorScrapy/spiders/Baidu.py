# -*- coding: utf-8 -*-
import scrapy
from ..items import DataItem


class BaidubaikeSpider(scrapy.Spider):
    name = 'baidu'
    custom_settings = {
        'ITEM_PIPELINES': {'TextProcessorScrapy.pipelines.BaiduPipeline': 400},
    }

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.allowed_domains = ['www.baike.baidu.com']
        if 'keyword' in kwargs:
            self.keyword = kwargs['keyword']
        self.start_urls.append("https://baike.baidu.com/item/" + self.keyword)

    '''回调函数，子类必须重写这个方法，否侧抛出异常'''

    def parse(self, response):
        title = response.xpath("//dl/dd/h1/text()").get()
        print(title)
        try:
            content = ""
            content1 = response.xpath("//div[@label-module='para']//text()").extract()
            for con in content1:
                content = content + con
            content1 = content.replace('\n', '')
            content = content1
            """
            content2 = response.xpath("//div[@class='basic-info J-basic-info cmn-clearfix']//text()").extract()
            print(content2)
            content1 = content.replace(' ', '')
            content = content1.replace('\n', '')
            content1 = content.replace('\r\n', '')
            content = content1
            cnt = 0
            for con in content2:
                # if cnt == 2:
                    # content = content + "。"
                    # cnt = 0
                content = content + con
                cnt = cnt + 1
            """
            print(content)
        except:
            print("无正文")
            content = ""
        item = DataItem()
        item['keyword'] = self.keyword
        item['source'] = "baidu"
        item['title'] = str(title)
        item['date'] = "\"0\""
        item['url'] = response.url
        item['content'] = str(content)
        yield item
