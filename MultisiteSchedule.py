"""
user: adm
date: 2021/7/23
time: 15:21
IDE: PyCharm  
"""
import datetime
import json
import logging
import os

import requests
from scrapy import signals
from scrapy.crawler import CrawlerRunner, Crawler
from scrapy.utils.log import configure_logging
from scrapy.utils.project import get_project_settings
from twisted.internet import reactor, error

from TextProcessorScrapy.spiders.Baidu import BaidubaikeSpider
from Transwarp import Transwarp
from DataCleaning import DataCleaning

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
NUM = 0


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

    :param url: 启动接口
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
    if int(info_json['status']) == 0:
        return False
    info = None
    if "文本" in info_json['data_type']:
        info = {"sites": [], "keywords": [], "crawl_id": info_json['crawl_id']}
        for _website in info_json['website']:
            if _website in WEB_MAP:
                info["sites"].append(WEB_MAP[_website])
        for keyword in info_json['keywords']:
            info['keywords'].append(keyword.replace('\t', ''))
    return info


def get_count(keyword):
    """
    获取本轮爬取数量信息

    :return: 爬取数量(str类型）
    """
    count = 0
    count_path = './result/count/'
    for file in os.listdir(count_path):
        if file.endswith(keyword):
            with open(count_path + file, 'r') as f:
                count += int(f.readline())
                f.close()
            os.remove(count_path + file)
    return str(count)


def crawl_file_list(root):
    """
    获取本次爬取文件路径列表

    :param root: 爬取文件的保存主目录
    :return: 文件列表
    """
    # 进行数据清洗
    DC = DataCleaning(root)
    DC.data_clean()

    file_list = {}
    for root, dirs, files in os.walk(root):
        for file in files:
            if file.endswith(".json") or file.endswith(".pdf"):  # 过滤得到json文件
                file_path = os.path.join(os.path.abspath("."), os.path.join(root, file))
                file_size = os.stat(file_path).st_size  # 文件大小
                if file_size <= 10:
                    continue
                # print("file_path:{}, file_size:{}".format(file_path, file_size))
                file_size = round(file_size / 1024, 2)
                file_list[file_path] = str(file_size) + 'KB'
    return file_list
    pass


def upload_crawl_file(path_list, connect):
    """
    上传本次爬取保存文件

    :param path_list: 爬取文件路径列表
    :param connect: Transwarp连接（包括hdfs和Inceptor）
    :return: 上传状态
    """
    try:
        for file in path_list:
            size_ = path_list[file]
            # print(file)
            # connect.upload_file(file, "\\text_crawl_file\\")
            # os.remove(file)
            sql_ = "INSERT INTO hs.text_crawl_file_temp  VALUES (?, ?, ?, ?, ?)"
            date_ = str(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
            filename_ = file.split("\\")[-1]
            format_ = file.split('.')[-1]
            path_ = "/hs/text_crawl_file/"
            pram_ = [date_, filename_, size_, format_, path_]
            connect.execute_sql(sql_, pram_)
        return "upload crawl file success"
    except:
        return "upload crawl file failure"
    pass


def insert_crawl_stats(crawl_id, keyword, connect):
    """
    向数据库表中插入当前轮次爬取数量状态

    :param connect: Transwarp连接（包括hdfs和Inceptor）
    :param crawl_id: 爬虫id，由前端接口获取
    :param keyword: 本轮爬取关键字
    :return: 执行状态
    """
    try:
        sql_ = "INSERT INTO  hs.text_crawl_stats  VALUES (?, ?, ?)"
        pram_ = [crawl_id, keyword, get_count(keyword)]
        connect.execute_sql(sql_, pram_)
        return "insert crawl stats success"
    except Exception as e:
        print(e)
        return "insert crawl stats failure"


def stop(*args, **kwargs):
    if reactor.running:
        try:
            reactor.stop()
        except error.ReactorNotRunning:
            pass


def start_spiders(transwarp=None, info=None):
    """
    启动爬虫

    :return:
    """
    # transwarp = Transwarp("Transwarp/JavaJar/DB.jar", "Transwarp/libs")
    # transwarp.connect_inceptor()
    # transwarp.connect_hdfs()

    status_url = "http://localhost:8080/text/textCrawler"
    info_url = "http://localhost:8080/site/siteJobManage"
    while True:
        # info = control_info(info_url)  # 控制信息
        keywords = []
        f = open('file/keys.txt', 'r')
        _f = f.readline()
        while _f:
            keywords.append(_f.replace('\n', ''))
            _f = f.readline()
        if info is None:
            info = {"sites": ['nasa', 'wiki', 'baidu', 'tiexue', 'aiaa'],
                    "keywords": ['枪榴弹', '航空母舰'], "crawl_id": "crawler1"}
        # while control_status(status_url):
        if info is not False:
            logger.info('TextCrawler On!')
            if info is not None:
                keywords = info['keywords']
                sites = info['sites']
                spiders_count = len(keywords) * len(sites)
                configure_logging()
                runner = CrawlerRunner(get_project_settings())
                for keyword in keywords:
                    for site in sites:
                        runner.crawl(site, keyword=keyword, crawl_id=info['crawl_id'], database=transwarp, spiders_count=spiders_count)
                        d = runner.join()
                        d.addBoth(stop)
                reactor.run()
                for keyword in keywords:
                    logger.info(insert_crawl_stats(keyword=keyword, crawl_id=info['crawl_id'], connect=transwarp))
                logger.info(upload_crawl_file(crawl_file_list("result\\"), transwarp))
                sql_ = "DELETE FROM hs.text_crawl_http_interact WHERE crawl_id = ? "
                pram_ = [info['crawl_id']]
                transwarp.execute_sql(sql_, pram_)
            break
        break
    pass


if __name__ == '__main__':
    'TODO:'
    # start_spiders()
    transwarp = Transwarp("Transwarp/JavaJar/Util.jar", "Transwarp/libs")
    transwarp.connect_inceptor()
    start_spiders(transwarp)
    # transwarp.connect_hdfs()
