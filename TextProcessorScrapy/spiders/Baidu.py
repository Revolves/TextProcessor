# -*- coding: utf-8 -*-
import scrapy
from ..items import BaiduWikiItem
import json
import time

import scrapy
from datetime import datetime

from selenium import webdriver


class BaidubaikeSpider(scrapy.Spider):
    name = 'baidu'
    custom_settings = {
        'ITEM_PIPELINES': {'TextProcessorScrapy.pipelines.BaiduPipeline': 400},
    }

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.allowed_domains = ['www.baike.baidu.com']
        # if 'keywords' in kwargs:
        #     self.keywords = kwargs['keywords']
        if 'keyword' in kwargs:
            self.keyword = kwargs['keyword']
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
        options.add_argument('--headless')
        self.browser = webdriver.Chrome(options=options)
        # browser.maximize_window()  # 浏览器窗口最大化
        self.browser.implicitly_wait(1)  # 隐形等待10秒

    '''回调函数，子类必须重写这个方法，否侧抛出异常'''

    def parse(self, response):
        self.number += 1
        # self.keyword = self.keywords[self.number]
        name = response.xpath("//dl/dd/h1/text()").get()
        item = BaiduWikiItem()
        # 简介
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
                return item
        except:
            content = ""
        # 属性
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

        self.browser.get(response.url)

        # 下拉滑动条至底部，加载出所有帖子信息
        t = True
        start = time.time()
        end1 = time.time()
        while t and (end1 - start) < 5:
            check_height = self.browser.execute_script("return document.body.scrollHeight;")
            for r in range(4):
                time.sleep(2)
                self.browser.execute_script("window.scrollBy(0,1500)")
            check_height1 = self.browser.execute_script("return document.body.scrollHeight;")
            end1 = time.time()
            if check_height == check_height1:
                t = False
        image_urls = self.browser.find_elements_by_xpath("//img[contains(@alt, '" + str(self.keyword) + "')]")
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
        item['title'] = str(name)
        item['content'] = str(content)
        item['attributes'] = attr
        item['url'] = response.url
        item['date'] = ''
        if len(item['content'].replace(' ', '').replace("\n", '')) <= 20 or item['content'] == '':
            return
        yield item
