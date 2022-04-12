from hashlib import md5
import logging
import time

import scrapy
from selenium import webdriver
from selenium.webdriver.common.keys import Keys

from ..items import DataItem
from ..utils.facebook_utiles import options

logging.basicConfig(level=logging.DEBUG,
                    format='[%(asctime)-15s] [%(levelname)8s] [%(name)10s ] - %(message)s (%(filename)s:%(lineno)s)',
                    datefmt='%Y-%m-%d %T'
                    )
logger = logging.getLogger(__name__)


class FacebookSpider(scrapy.Spider):
    name = 'facebook'
    custom_settings = {
        'ITEM_PIPELINES': {'TextProcessorScrapy.pipelines.FacebookPipeline': 300},
        'REDIRECT_ENABLED': False,
        'HTTPERROR_ALLOWED_CODES': [302, 301],
        'CONCURRENT_REQUESTS': 8,
        'COOKIES_ENABLED': True
    }

    def __init__(self, *args, **kwargs):
        super().__init__(**kwargs)
        self.allowed_domains = ['facebook.com']
        self.driver = webdriver.Chrome(options=options)
        self.driver.implicitly_wait(3)
        if 'crawl_id' in kwargs['crawl_id']:
            self.crawl_id = kwargs['crawl_id']
        if 'keyword' in kwargs:
            self.keyword = kwargs['keyword'].split('_')[-1]
            self.keyword_type = kwargs['keyword'].split('_')[0]
        if 'database' in kwargs:
            self.database = kwargs['database']

    def start_requests(self):
        # 登录账户
        try:
            try:
                self.driver.get(
                    'https://facebook.com/?stype=lo&jlou=AfcFaK2ov8XLonPVvzBvlW-hIShxrA1nbAwDVg1CPMo5TSXJNtdG7Xb_KE6SOxa2sv2Gnk4o43Lvgt9NoSXSq-tsR0H1-eUxIGf-rgp1P5hTAg&smuh=47658&lh=Ac8jNUuHrt8XoyfC-hU')
            except:
                self.driver.find_element_by_id('reload-button').click()
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
            time.sleep(1)
        except:
            pass
        head_url = "https://facebook.com/search/posts/?q="
        yield scrapy.Request(head_url + self.keyword, callback=self.parse, dont_filter=True)

    def parse(self, response):
        print(response.url)
        logger.info("Facebook Spider starting!")
        try:
            self.driver.get(response.url)
            self.scroll_to_bottom()
            results = self.driver.find_elements_by_xpath('//div[@role="feed"]/div/div') # //div[@role="main"]/div/div[1]/div/div/div/div/div
            print(results.text)
            for result in results:
                print(result.text)
                item = DataItem()
                item['keyword'] = self.keyword
                item['source'] = 'facebook'
                item['url'] = result.find_element_by_xpath('//a[@role="link"]').get_attribute("href")
                item['content'] = result.find_element_by_xpath(
                    '//div[@class="kvgmc6g5 cxmmr5t8 oygrvhab hcukyx3x c1et5uql ii04i59q"]').get_attribute("textContent").replace('\n','')
                item['date'] = result.find_element_by_xpath(
                    '//span/b').get_attribute("textContent").replace('=', '')  #    //div[@role="main"]/div/div[1]/div/div/div/div/div//span/b
                item['title'] = item['content'][0:30]
                print(item)
                # if len(item['content'].replace(' ', '').replace("\n", '')) <= 20 or item['content'] == '':
                #     continue
                yield item
        except Exception as e:
            print(e)
            pass

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
