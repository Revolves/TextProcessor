"""
user: adm
date: 2021/8/13
time: 18:31
IDE: PyCharm  
"""
import logging

from scrapy.crawler import CrawlerRunner
from scrapy.utils.log import configure_logging
from scrapy.utils.project import get_project_settings
from twisted.internet import reactor

logging.basicConfig(level=logging.DEBUG,
                    format='[%(asctime)-15s] [%(levelname)8s] [%(name)10s ] - %(message)s (%(filename)s:%(lineno)s)',
                    datefmt='%Y-%m-%d %T'
                    )
logger = logging.getLogger(__name__)
keywords = []
f = open('file/keys.txt', 'r')
_f = f.readline()
while _f:
    keywords.append(_f.replace('\n', ''))
    _f = f.readline()
print(keywords)
configure_logging()
runner = CrawlerRunner(get_project_settings())
runner.crawl('wiki', keywords=keywords)
d = runner.join()
d.addBoth(lambda _: reactor.stop())
reactor.run()
