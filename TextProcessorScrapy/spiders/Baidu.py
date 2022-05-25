# -*- coding: utf-8 -*-
from hashlib import md5
import time

import scrapy
from selenium import webdriver
from scrapy import signals

from ..items import BaiduWikiItem
from scrapy.spidermiddlewares.httperror import HttpError
from twisted.internet.error import DNSLookupError
from twisted.internet.error import TimeoutError, TCPTimedOutError

class BaidubaikeSpider(scrapy.Spider):
    name = 'baidu'
    custom_settings = {
        'ITEM_PIPELINES': {'TextProcessorScrapy.pipelines.BaiduPipeline': 400,
                            'TextProcessorScrapy.pipelines.ImagePipeline': 300},
    }

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.allowed_domains = ['baidu.com']
        # if 'keywords' in kwargs:
        #     self.keywords = kwargs['keywords']
        if 'crawl_id' in kwargs['crawl_id']:
            self.crawl_id = kwargs['crawl_id']
        if 'keyword' in kwargs:
            self.keyword = kwargs['keyword'].split('_')[-1]
            self.keyword_type = kwargs['keyword'].split('_')[0]
        if 'database' in kwargs:
            self.database = kwargs['database']
        self.start_urls.append("https://baike.baidu.com/item/" + self.keyword)
        self.number = -1

        options = webdriver.ChromeOptions()
        prefs = {
            'profile.default_content_setting_values':
                {'notifications': 2,
                 'images': 2}  # 禁止谷歌浏览器弹出通知消息
        }
        options.add_experimental_option('prefs', prefs)
        # 设置chrome浏览器无界面模式
        options.add_argument('--no-sandbox') #让Chrome在root权限下运行
        options.add_argument('--disable-dev-shm-usage') 
        options.add_argument('--headless') # 无界面模式
        options.add_argument('--ignore-certificate-errors') # 忽略证书错误
        options.add_argument('-ignore -ssl-errors')     
        options.add_argument('--disable-software-rasterizer')    
        options.add_argument('--log-level=3')
        options.add_experimental_option('excludeSwitches', ['enable-logging'])
        self.driver = webdriver.Chrome(options=options)
        # driver.maximize_window()  # 浏览器窗口最大化
        self.driver.implicitly_wait(1)  # 隐形等待10秒

    # @classmethod
    # def from_crawler(cls, crawler, *args, **kwargs):
    #     spider = super(BaidubaikeSpider, cls).from_crawler(crawler, *args, **kwargs)
    #     crawler.signals.connect(spider.item_scraped, signal=signals.item_scraped)
    #     return spider

    '''回调函数，子类必须重写这个方法，否侧抛出异常'''

    def start_requests(self):
        for u in self.start_urls:
            yield scrapy.Request(u, callback=self.parse,
                                    errback=self.errback_httpbin,
                                    dont_filter=True)

    def parse(self, response):
        self.number += 1
        # self.keyword = self.keywords[self.number]
        name = str(response.xpath('//*[@class="lemmaWgt-lemmaTitle-title J-lemma-title"]/span//text()').get()).strip()
        item = BaiduWikiItem()
        # # 简介
        content = ''
        try:
            cont = ""
            content1 = response.xpath("//div[@label-module='para']//text()").extract()
            for con in content1:
                cont = cont + con
            if cont:
                content1 = cont.replace('\n', '')
                cont = content1.replace('\r', '')
                content1 = cont.replace(chr(10), ' ')
                cont = content1.replace('\"', '\'')
                content = cont.replace('\\', '')
            else:
                yield item
        except:
            content = ""
        # 属性
        # if content == '':
        #     return
        attr = dict()
        attr_names = response.xpath("//dt[@class='basicInfo-item name']//text()").extract()
        attr_values = response.xpath("//dd[@class='basicInfo-item value']//text()").extract()
        for i, attr_n in enumerate(attr_names):
            attr_n = attr_n.replace(' ', '')
            attr_n = attr_n.replace(chr(10), '')
            attr_n = attr_n.replace('\"', '\'')
            attr_names[i] = "".join(str(attr_n).split())
            attr_values[i] = attr_values[i].replace(chr(10), '')
            attr_values[i] = attr_values[i].replace('\\xa0', '')
            attr_values[i] = attr_values[i].replace('\"', '\'')
            # attr = attr + ", \"" + str(attr_names[i]) + "\" : \"" + str(attr_values[i]) + "\""
            attr[str(attr_names[i])] = str(attr_values[i])

        # 加载所有图片
        cnt = 0
        # 设置浏览器

        self.driver.get(response.url)

        # 下拉滑动条至底部，加载出所有帖子信息
        t = True
        start = time.time()
        end1 = time.time()
        while t and (end1 - start) < 5:
            check_height = self.driver.execute_script("return document.body.scrollHeight;")
            for r in range(4):
                time.sleep(2)
                self.driver.execute_script("window.scrollBy(0,1500)")
            check_height1 = self.driver.execute_script("return document.body.scrollHeight;")
            end1 = time.time()
            if check_height == check_height1:
                t = False
        image_urls = self.driver.find_elements_by_xpath("//img[contains(@alt, '" + str(self.keyword) + "')]")
        img_url = dict()
        # 得到图片url
        for image_url in image_urls:
            image_url = image_url.get_attribute('src')
            cnt = cnt + 1
            # img_url = img_url + ", \"图片" + str(cnt) + "\" : \"" + str(image_url) + "\""
            img_url["图片" + str(cnt)] = str(image_url)
            time.sleep(2)
        attr['img_url'] = img_url
        item['source'] = 'baidu'
        item['keyword'] = self.keyword
        item['title'] = item['title'] = name if name is not None else self.keyword
        item['content'] = str(content)
        item['attributes'] = attr
        item['url'] = response.url
        item['date'] = ''
        item['crawl_id'] = self.crawl_id
        item['label_type'] = self.keyword_type
        yield item

    def errback_httpbin(self, failure):
        # log all failures
        self.logger.error(repr(failure))

        # in case you want to do something special for some errors,
        # you may need the failure's type:

        if failure.check(HttpError):
            # these exceptions come from HttpError spider middleware
            # you can get the non-200 response
            response = failure.value.response
            self.logger.error('HttpError on %s', response.url)

        elif failure.check(DNSLookupError):
            # this is the original request
            request = failure.request
            self.logger.error('DNSLookupError on %s', request.url)

        elif failure.check(TimeoutError, TCPTimedOutError):
            request = failure.request
            self.logger.error('TimeoutError on %s', request.url)