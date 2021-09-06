"""
user: adm
date: 2021/7/23
time: 15:21
IDE: PyCharm  
"""
import json
import logging
import threading

import requests
from scrapy.cmdline import execute
from scrapy.crawler import CrawlerRunner
from scrapy.utils.log import configure_logging
from scrapy.utils.project import get_project_settings
from twisted.internet import reactor

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
        info = {"sites": [], "keywords": []}
        for _website in info_json['website']:
            if _website in WEB_MAP:
                info["sites"].append(WEB_MAP[_website])
        for keyword in info_json['keywords']:
            info['keywords'].append(keyword.replace('\t', ''))
    return info


if __name__ == '__main__':
    status_url = "http://localhost:8080/text/textCrawler"
    info_url = "http://localhost:8080/site/siteJobManage"
    while control_status(status_url):
        logger.info('TextCrawler On!')
        info = control_info()  # 控制信息
        if info is not None:
            keywords = info['keywords']
            sites = info['sites']
            configure_logging()
            runner = CrawlerRunner(get_project_settings())
            for site in sites:
                runner.crawl(site, keywords=keywords)
                d = runner.join()
                d.addBoth(lambda _: reactor.addSystemEventTrigger('before', 'shutdown'))
            reactor.run()
        break
