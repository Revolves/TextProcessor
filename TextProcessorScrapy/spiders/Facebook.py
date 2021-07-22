import scrapy
import xlrd
from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.by import By
from selenium.webdriver.support.wait import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import time
import hashlib
from selenium.webdriver import ActionChains
from ..utils.facebook_utiles import options
from ..items import FacebookItem



class FacebookSpider(scrapy.Spider):
    name = 'facebook'
    allowed_domains = ['facebook.com']
    start_urls = ['https://facebook.com/']

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.allowed_domains = ['facebook.com']
        head_url = "https://www.facebook.com/search/posts/?q="
        self.driver = webdriver.Chrome(options= options)
        self.driver.implicitly_wait(10)
        if 'keyword' in kwargs:
            self.keyword = kwargs['keyword']
        self.start_urls.append(head_url + self.keyword)
        #

    def parse(self, response):
        # results = response.xpath(
        #     '//div[@role="main"]//div[@class="jb3vyjys hv4rvrfc ihqw7lf3 dati1w0a"]')
        # for result in results:
        #     item = FacebookItem()
        #     item['keyword'] = self.keyword
        #     item['source'] = 'facebook'
        #     item['url'] = result.xpath('/a/text()').extract_first()
        #     item['content'] = result.xpath(
        #         '//span[@class="a8c37x1j ni8dbmo4 stjgntxs l9j0dhe7"]/text()').extract_first()
        #     item['date'] = result.xpath(
        #         '//span[@class="a8c37x1j ni8dbmo4 stjgntxs l9j0dhe7"]/span').extract_first()
        #     item['title'] = item['content'][0:30]
        #     yield item
        try:
            try:
                self.driver.get('https://www.facebook.com/')
            except:
                self.driver.find_element_by_id('reload-button').click()
                print('重新刷新页面~')
            # 输入账户密码
            self.driver.find_element_by_id('email').clear()
            self.driver.find_element_by_id('email').send_keys('liuleyuan0114@qq.com')
            self.driver.find_element_by_id('pass').clear()
            self.driver.find_element_by_id('pass').send_keys('zxcvbnm1234')

            # 模拟点击登录按钮，两种不同的点击方法
            try:
                self.driver.find_element_by_xpath("//button[contains(@id,'u_0_d_')]").send_keys(Keys.ENTER)
            except:
                self.driver.find_element_by_xpath('//a[@href="https://www.facebook.com/?ref=logo"]').send_keys(
                    Keys.ENTER)
            time.sleep(5)
            self.driver.get(response.url)
            self.scroll_to_bottom()
            results = self.driver.find_elements_by_xpath('//div[@role="main"]//div[@class="jb3vyjys hv4rvrfc ihqw7lf3 dati1w0a"]')
            for result in results:
                item = FacebookItem()
                item['keyword'] = self.keyword
                item['source'] = 'facebook'
                item['url'] = result.find_element_by_xpath('/a').get_attribute("href")
                item['content'] = result.find_element_by_xpath('//span[@class="a8c37x1j ni8dbmo4 stjgntxs l9j0dhe7"]').get_attribute("textContent")
                item['date'] = result.find_element_by_xpath('//span[@class="a8c37x1j ni8dbmo4 stjgntxs l9j0dhe7"]/span').get_attribute("textContent")
                item['title'] = item['content'][0:30]
                yield item
        except:
            print('url parse error!')


    def scroll_to_bottom(self):
        """
        滚动到页面底部
        :return:
        """
        js = "return action=document.body.scrollHeight"
        # 初始化现在滚动条所在高度为0
        height = 0
        # 当前窗口总高度
        new_height = self.driver.execute_script(js)

        while height < new_height:
            # 将滚动条调整至页面底部
            for i in range(height, new_height, 100):
                self.driver.execute_script('window.scrollTo(0, {})'.format(i))
            time.sleep(0.5)
            height = new_height
            time.sleep(2)
            new_height = self.driver.execute_script(js)

