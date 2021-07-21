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
        self.driver = webdriver.Chrome(options)
        if 'keyword' in kwargs:
            self.keyword = kwargs['keyword']

    def parse(self, response):
        pass
