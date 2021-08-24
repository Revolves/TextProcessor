import scrapy
import xlrd
import logging
from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.by import By
from selenium.webdriver.support.wait import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import time
import hashlib
from selenium.webdriver import ActionChains
from ..utils.facebook_utiles import options
from ..items import DataItem

logging.basicConfig(level=logging.DEBUG,
                    format='[%(asctime)-15s] [%(levelname)8s] [%(name)10s ] - %(message)s (%(filename)s:%(lineno)s)',
                    datefmt='%Y-%m-%d %T'
                    )
logger = logging.getLogger(__name__)

class FacebookSpider(scrapy.Spider):
    name = 'facebook'
    custom_settings = {
        'ITEM_PIPELINES': {'TextProcessorScrapy.pipelines.FacebookPipeline': 300},
    }
    def __init__(self, *args, **kwargs):
        super().__init__(**kwargs)
        self.allowed_domains = ['facebook.com']
        head_url = "https://www.facebook.com/search/posts/?q="
        self.driver = webdriver.Chrome(options= options)
        self.driver.implicitly_wait(10)
        if args:
            self.keywords = args
        for keyword in self.keywords:
            self.start_urls.append(head_url + keyword)
        self.count = -1
         # 登录账户
        try:
            self.driver.get('https://www.facebook.com/')
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
        time.sleep(5)

    def parse(self, response):
        logger.info("Facebook Spider starting!")
        self.count += 1
        try:
            self.driver.get(response.url)
            self.scroll_to_bottom()
            results = self.driver.find_elements_by_xpath('//div[@role="main"]//div[@class="jb3vyjys hv4rvrfc ihqw7lf3 dati1w0a"]')
            for result in results:
                item = DataItem()
                item['keyword'] = self.keywords[self.count]
                item['source'] = 'facebook'
                item['url'] = result.find_element_by_xpath('/a').get_attribute("href")
                item['content'] = result.find_element_by_xpath('//span[@class="a8c37x1j ni8dbmo4 stjgntxs l9j0dhe7"]').get_attribute("textContent")
                item['date'] = result.find_element_by_xpath('//span[@class="a8c37x1j ni8dbmo4 stjgntxs l9j0dhe7"]/span').get_attribute("textContent")
                item['title'] = item['content'][0:30]
                yield item
        except:
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

