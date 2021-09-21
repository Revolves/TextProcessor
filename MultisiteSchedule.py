"""
user: adm
date: 2021/7/23
time: 15:21
IDE: PyCharm  
"""
import json
import logging
import os

import requests
from scrapy import signals
from scrapy.crawler import CrawlerRunner, Crawler
from scrapy.utils.log import configure_logging
from scrapy.utils.project import get_project_settings
from twisted.internet import reactor, defer
from TextProcessorScrapy.spiders.Baidu import BaidubaikeSpider

logging.basicConfig(level=logging.DEBUG,
                    format='[%(asctime)-15s] [%(levelname)8s] [%(name)10s ] - %(message)s (%(filename)s:%(lineno)s)',
                    datefmt='%Y-%m-%d %T'
                    )
logger = logging.getLogger(__name__)
WEB_MAP = {"百度百科": 'baidu',
           "维基百科": "wiki",
           "NASA": "nasa",
           "AIAA": "aiaa",
           "铁血论坛": "tiexue",
           'twitter': 'twitter',
           "facebook": "facebook",
           "jane's": "janes"
           }
SITE_to_SPIDER = {"百度百科": BaidubaikeSpider}


class CrawlRunner:

    def __init__(self):
        self.running_crawlers = []

    def spider_closing(self, spider):
        logger.info("Spider closed: %s" % spider)
        self.running_crawlers.remove(spider)
        if not self.running_crawlers:
            reactor.stop()

    def run(self):
        settings = get_project_settings()
        logging.basicConfig(level=logging.DEBUG,
                            format='[%(asctime)-15s] [%(levelname)8s] [%(name)10s ] - %(message)s (%(filename)s:%(lineno)s)',
                            datefmt='%Y-%m-%d %T'
                            )
        logger = logging.getLogger(__name__)
        to_crawl = [SITE_to_SPIDER["百度百科"]]

        for spider in to_crawl:
            crawler = Crawler(settings)
            crawler_obj = spider()
            self.running_crawlers.append(crawler_obj)

            crawler.signals.connect(self.spider_closing, signal=signals.spider_closed)
            crawler.configure()
            crawler.crawl(crawler_obj, keywords=['手榴弹'])
            crawler.start()

        reactor.run()


def control_status(url):
    """
    获取当前爬取启动状态,开始运行则返回True
    :param url:启动接口
    :return:
    """
    response = requests.get(url).text
    status = json.loads(response)[0]["status"]
    if status == 1:
        return True
    return False


def control_info(url):
    """
    解析控制信息
    :param url:
    :return: 爬取站点列表、关键词
    """
    response = requests.get(url).text
    info_json = json.loads(response)[0]
    info = None
    if "文本" in info_json['data_type']:
        info = {"sites": [], "keywords": [], "crawlerID": info_json['crawl_id']}
        for _website in info_json['website']:
            if _website in WEB_MAP:
                info["sites"].append(WEB_MAP[_website])
        for keyword in info_json['keywords']:
            info['keywords'].append(keyword.replace('\t', ''))
    return info


def get_count():
    """
    获取本轮爬取数量信息
    :return:
    """
    count = 0
    count_path = './result/count/'
    for file in os.listdir(count_path):
        with open(count_path + file, 'r') as f:
            count += int(f.readline())
    return count


def crawl_file_list(root):
    """
    获取本次爬取文件路径列表
    :param root:
    :return: 文件列表
    """
    file_list = []
    for _dir in root:
        for root, dirs, files in os.walk(_dir):
            for file in files:
                if file.endswith(".json"):  # 过滤得到json文件
                    file_list.append(os.path.join(root, file))
    return file_list
    pass


def upload_crawl_file(path_list):
    for file in path_list:
        pass
    pass


def stop(*args, **kwargs):
    reactor.stop()


if __name__ == '__main__':
    status_url = "http://localhost:8080/text/textCrawler"
    info_url = "http://localhost:8080/site/siteJobManage"
    while True:
        # while control_status(status_url):
        while True:
            logger.info('TextCrawler On!')
            # info = control_info(info_url)  # 控制信息
            info = {"sites": ['nasa', 'wiki'], "keywords": ['target', 'under water']}
            if info is not None:
                stats_file = open('stats.txt', 'a')
                keywords = info['keywords']
                sites = info['sites']
                configure_logging()
                runner = CrawlerRunner(get_project_settings())
                for keyword in keywords:
                    for site in sites:
                        runner.crawl(site, keyword=keyword)
                        d = runner.join()
                        d.addBoth(stop)
                    # defer.DeferredList(set()).addBoth(stop)
                try:
                    reactor.run()
                except:
                    stop()
                # reactor.run()
            crawl_numbers = get_count()
            print("crawl_numbers:{}".format(crawl_numbers))
            break
