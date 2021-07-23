"""
user: adm
date: 2021/7/23
time: 15:21
IDE: PyCharm  
"""
import logging
import threading
import time

import scrapy

from multiprocessing import Process
from scrapy.crawler import CrawlerProcess, CrawlerRunner
from threading import Thread

from twisted.internet import reactor
from scrapy.utils.project import get_project_settings
from scrapy.utils.log import configure_logging
from scrapy.cmdline import execute
from spiders.Twitter import TwitterSpider
from spiders.NASA import nasaSpider

logging.basicConfig(level=logging.DEBUG,
                    format='[%(asctime)-15s] [%(levelname)8s] [%(name)10s ] - %(message)s (%(filename)s:%(lineno)s)',
                    datefmt='%Y-%m-%d %T'
                    )
logger = logging.getLogger(__name__)


def ControlInfo():
    Info = {
        "sites": ['NASA', 'twitter'],
        "keywords": ['target']
    }
    return Info


def SpiderStart(site, keyword):
    def run():
        logger.info('Spider {} is starting!'.format(site))
        # execute(['scrapy', 'crawl', site, '--nolog', '-a', 'keyword={}'.format(keyword)])
        configure_logging()
        runner = CrawlerRunner(get_project_settings())
        runner.crawl(site, keyword=keyword)
        d = runner.join()
        d.addBoth(lambda _: reactor.stop())
        reactor.run()
    r = threading.Thread(target=run)
    r.start()


if __name__ == '__main__':
    info = ControlInfo()
    Crawler_list = {'NASA': nasaSpider, 'twitter': TwitterSpider}
    for site in info['sites']:
        # SpiderStart(Crawler_list[site], info['keywords'])
        logger.info('Spider {} is starting!'.format(site))
        execute(['scrapy', 'crawl', site, '--nolog', '-a', 'keyword={}'.format(info['keywords'])])
        time.sleep(1)
    # t = testThread()
    # t.start()
