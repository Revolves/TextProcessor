"""
user: adm
date: 2021/8/13
time: 18:31
IDE: PyCharm  
"""
from scrapy.cmdline import execute

execute(['scrapy', 'crawl', 'aiaa', '-a', 'keyword=target'])
