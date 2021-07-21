# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

import scrapy
import re
import sys, os

sys.path.append(os.path.dirname(__file__) + os.sep + '../')
from TextProcessor.settings import PROXY_URL_FORMATTER

schema_pattern = re.compile(r'http|https$', re.I)
ip_pattern = re.compile(r'^([0-9]{1,3}.){3}[0-9]{1,3}$', re.I)
port_pattern = re.compile(r'^[0-9]{2,5}$', re.I)


class HsNasaItem(scrapy.Item):
    # define the fields for your item here like:
    # name = scrapy.Field()
    keyword = scrapy.Field()  # 标签
    source = scrapy.Field()  # 来源
    title = scrapy.Field()  # 标题
    url = scrapy.Field()  # 网址
    date = scrapy.Field()  #
    # description = scrapy.Field()
    content = scrapy.Field()
    pass


class TwitterItem(scrapy.Item):
    keyword = scrapy.Field()  # 标签
    source = scrapy.Field()  # 来源
    title = scrapy.Field()  # 标题
    url = scrapy.Field()  # 网址
    date = scrapy.Field()  #
    # description = scrapy.Field()
    content = scrapy.Field()


class FacebookItem(scrapy.Item):
    keyword = scrapy.Field()  # 标签
    source = scrapy.Field()  # 来源
    title = scrapy.Field()  # 标题
    url = scrapy.Field()  # 网址
    date = scrapy.Field()  #
    # description = scrapy.Field()
    content = scrapy.Field()


class VideoItem(scrapy.Item):
    title = scrapy.Field()
    video = scrapy.Field()


class ProxyPoolItem(scrapy.Item):
    # define the fields for your item here like:
    # name = scrapy.Field()
    """
    "schema": "http", # 代理的类型
    "ip": "127.0.0.1", # 代理的IP地址
    "port": "8050", # 代理的端口号
    "original":"西刺代理",
    "used_total": 11, # 代理的使用次数
    "success_times": 5, # 代理请求成功的次数
    "continuous_failed": 3, # 使用代理发送请求，连续失败的次数
    "created_time": "2018-05-02" # 代理的爬取时间
    """
    schema = scrapy.Field()
    ip = scrapy.Field()
    port = scrapy.Field()
    original = scrapy.Field()
    used_total = scrapy.Field()
    success_times = scrapy.Field()
    continuous_failed = scrapy.Field()
    created_time = scrapy.Field()

    # 检查IP代理的格式是否正确
    def _check_format(self):
        if self['schema'] is not None and self['ip'] is not None and self['port'] is not None:
            if schema_pattern.match(self['schema']) and ip_pattern.match(self['ip']) and port_pattern.match(
                    self['port']):
                return True
        return False

    # 获取IP代理的url
    def _get_url(self):
        return PROXY_URL_FORMATTER % {'schema': self['schema'], 'ip': self['ip'], 'port': self['port']}


class Tweet(scrapy.Item):
    id_ = scrapy.Field()
    raw_data = scrapy.Field()


class User(scrapy.Item):
    id_ = scrapy.Field()
    raw_data = scrapy.Field()


class TwitterKeywordItem(scrapy.Item):
    keyword = scrapy.Field()
