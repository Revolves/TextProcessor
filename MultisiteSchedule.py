"""
user: adm
date: 2021/7/23
time: 15:21
IDE: PyCharm  
"""
from cProfile import label
from dataclasses import dataclass
import datetime
import json
import logging
import os
import sys
import time
from unicodedata import name

import requests
from scrapy import signals
from scrapy.crawler import CrawlerRunner, Crawler
from scrapy.utils.log import configure_logging
from scrapy.utils.project import get_project_settings
from twisted.internet import reactor, error
from hashlib import md5

from TextProcessorScrapy.spiders.Baidu import BaidubaikeSpider
from Transwarp import Transwarp
from DataCleaning import DataCleaning

logging.basicConfig(level=logging.DEBUG,
                    format='[%(asctime)-15s] [%(levelname)8s] [%(name)10s ] - %(message)s (%(filename)s:%(lineno)s)',
                    datefmt='%Y-%m-%d %T'
                    )
logger = logging.getLogger(__name__)
WEB_MAP = {"baidu": 'baidu',
           "wiki": "wiki",
           "NASA": "nasa",
           "AIAA": "aiaa",
           "铁血论坛": "tiexue",
           'twitter': 'twitter',
           "facebook": "facebook",
           "jane's": "janes"
           }
SITE_to_SPIDER = {"百度百科": BaidubaikeSpider}
NUM = 0

def rowkey_id_gen():
    return md5(str(time.time()).encode()).hexdigest()

def is_chinese(string):
    """
    检查整个字符串是否包含中文
    :param string: 需要检查的字符串
    :return: bool
    """
    for ch in string:
        if u'\u4e00' <= ch <= u'\u9fff':
            return True

    return False

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
    count = int(0)
    count_path = 'result/count/'
    for file in os.listdir(count_path):
        if file.endswith(keyword):
            with open(count_path + file, 'r') as f:
                r = f.readline()
                if r != '':
                    c = int(r)
                    count = count + c
                f.close()
            os.remove(count_path + file)
    return str(count)


def crawl_file_list(root):
    """
    获取本次爬取文本文件路径列表

    :param root: 爬取文件的保存主目录
    :return: 文件列表
    """
    # 进行数据清洗
    # DC = DataCleaning(root)
    # DC.data_clean()

    file_list = {}
    for root, dirs, files in os.walk(root):
        for file in files:
            if file.endswith(".json") or file.endswith(".pdf") or file.endswith(".csv"):  # 过滤得到json文件
                file_path = os.path.join(os.path.abspath("."), os.path.join(root, file))
                file_size = os.stat(file_path).st_size  # 文件大小
                if file_size <= 10:
                    continue
                # print("file_path:{}, file_size:{}".format(file_path, file_size))
                file_size = round(file_size / 1024, 2)
                file_list[file_path] = str(file_size) + 'KB'
    return file_list
    pass

def crawl_img_list(root):
    """
    获取本次爬取图片文件路径列表

    :param root: 爬取文件的保存主目录
    :return: 文件列表
    """
    file_list = {}
    for root, dirs, files in os.walk(root):
        for file in files:
            if file.endswith(".jpg") or file.endswith(".png"):  # 过滤得到json文件
                file_path = os.path.join(os.path.abspath("."), os.path.join(root, file))
                file_size = os.stat(file_path).st_size  # 文件大小
                if file_size <= 10:
                    continue
                # print("file_path:{}, file_size:{}".format(file_path, file_size))
                file_size = round(file_size / 1024, 2)
                file_list[file_path] = str(file_size) + 'KB'
    return file_list
    pass

def upload_crawl_img(path_list, connect):
    """
    上传本次爬取保存图片

    :param path_list: 爬取图片路径列表
    :param connect: Transwarp连接（包括hdfs和Inceptor）
    :return: 上传状态
    """
    try:
        for file in path_list:
            size_ = path_list[file]
            logger.info("上传文件：{}".format(file))
            connect.upload_file(file, "/text_crawl_file/")
            os.remove(file)
            sql_ = "INSERT INTO hsold.metadata_imagesearch VALUES (?, ?, ?, ?, ?, ?, ?, ?, ? ,? ,?, ?)"
            rowkey_id = rowkey_id_gen()
            date_ = str(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
            name_ = file.split("/")[-1]
            format_ = file.split('.')[-1]
            path_ = "/hs/imageSearch/" + name_
            url = ''
            label_ = name_.split('_')[0]
            label_type = ''
            pram_ = [rowkey_id, name_, size_, format_, path_, url, date_, label_, label_type, '', '', '']
            connect.execute_sql(sql_, pram_)
        return "upload crawl file success"
    except Exception as e:
        logger.exception(e)
        return "upload crawl file failure"
    pass

def upload_crawl_img_new(file_index, image_index, connect):
    """
    上传本次爬取保存图片

    :param file_index: 文件根目录
    :param connect: Transwarp连接（包括hdfs和Inceptor）
    :return: 上传状态
    """
    try:
        img_file_list = os.listdir(file_index)
        for img_file in img_file_list:
            with open(file_index + '/' + img_file) as f:
                data_list = json.load(f)
            os.remove(file_index + '/' + img_file)
            for file in data_list.keys():
                file_path = image_index + '/' + file
                file_size = os.stat(file_path ).st_size  # 文件大小
                file_size = round(file_size / 1024, 2)
                size_ = str(file_size) + 'KB'
                logger.info("上传文件：{}".format(file_path))
                connect.upload_file(file_path, "/imageSearch")
                os.remove(file_path)
                sql_ = "INSERT INTO hsold.metadata_imagesearch VALUES (?, ?, ?, ?, ?, ?, ?, ?, ? ,? ,?, ?, ?)"
                rowkey_id = rowkey_id_gen()
                date_ = str(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
                name_ = file.split("/")[-1]
                format_ = file.split('.')[-1]
                path_ = "/hs/imageSearch/" + name_
                url = data_list[file]
                label_ = name_.split('_')[0]
                label_type = name_.split('_')[0]
                neo4j_id = ''
                pram_ = [rowkey_id, name_, size_, format_, path_, url, date_, label_, label_type, '', '', '', neo4j_id]
                connect.execute_sql(sql_, pram_)
        return "upload crawl file success"
    except Exception as e:
        logger.exception(e)
        return "upload crawl file failure"
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
            logger.info("上传文件：{}".format(file))
            format_ = file.split('.')[-1]
            if format_ == 'csv':
                path_ = "/hs/text_crawl_csv/"
                connect.upload_file(file, "/text_crawl_csv/")
            else:
                path_ = "/hs/text_crawl_file/"
                connect.upload_file(file, "/text_crawl_file/")
            os.remove(file)
            sql_ = "INSERT INTO hsold.metadata_text_crawl VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)"
            rowkey_id = rowkey_id_gen()
            date_ = str(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
            name_ = file.split("\\")[-1]
            label_ = name_.split('_')[1]
            label_type_ = name_.split('_')[2]
            neo4j_id = ''
            pram_ = [rowkey_id, name_, size_, format_, path_, date_, label_, label_type_, neo4j_id]
            connect.execute_sql(sql_, pram_)
        return "upload crawl file success"
    except Exception as e:
        logger.exception(e)
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
        sql_ = "INSERT INTO  hsold.text_crawl_stats  VALUES (?, ?, ?, ?)"
        pram_ = [rowkey_id_gen(), crawl_id, keyword, get_count(keyword)]
        connect.execute_sql(sql_, pram_)
        return "insert crawl stats success"
    except Exception as e:
        logger.exception(e)
        return "insert crawl stats failure"


def stop(*args, **kwargs):
    if reactor.running:
        try:
            reactor.stop()
        except error.ReactorNotRunning:
            pass

def crawling(crawl_id, connect):
    sql_ = "SELECT crawl_id FROM hsold.text_crawler_running"
    crawling_list = connect.execute_sql(sql_, [])
    if(len(crawling_list) == 0):
        clear_http_table_sql_ = "DELETE FROM hsold.show_text_keyword"
        print("No crawler")
    insert_crawl_id_sql_ = "INSERT INTO hsold.text_crawler_running VALUES (?, ?)"
    pram_ = [rowkey_id_gen(), crawl_id]
    connect.execute_sql(insert_crawl_id_sql_, pram_)


def start_spiders(transwarp=None, info=None, local = None):
    """
    启动爬虫

    :return:
    """ 
    if local is None:
        if info is None:
            info = {"sites": ['baidu'],
                    "keywords": ['earth fortification', '马公机场', '澎湖机场', '西屿雷达站', '花莲机场', '直升机', '装甲防护车', '装甲扫雷车', '装甲运输车', '装甲救护车', '装甲侦察车'], "crawl_id": "test"}
        else:    
            if info['status'] == '2':
                # info = control_info(info_url)  # 控制信息
                keywords = []
                f = open('file/keys.txt', 'r', encoding='gbk')
                _f = f.readline()
                cnt = 1
                while _f:
                    if cnt == 250:
                        break
                    keywords.append(_f.replace('\n', ''))
                    _f = f.readline()
                    cnt += 1
                #if info is None:
                #  info = {"sites": ['nasa', 'wiki', 'baidu', 'tiexue', 'aiaa', 'janes', 'twitter'],
                #          "keywords": ['earth fortification', '马公机场', '澎湖机场', '西屿雷达站', '花莲机场', '直升机', '装甲防护车', '装甲扫雷车', '装甲运输车', '装甲救护车', '装甲侦察车'], "crawl_id": "crawler1"}
                info["keywords"] = keywords
                info['sites'] = ['nasa', 'wiki', 'baidu', 'tiexue', 'aiaa', 'janes', 'twitter', 'facebook']
            elif info['status'] == '1':
                if type(info['sites']) == type(''):
                    info['sites'] = info['sites'].split(',')
                if type(info['keywords']) == type(''):
                    info['keywords'] = info['keywords'].replace('\t','').split(',')
        
        for idx in range(len(info['sites'])):
            if info['sites'][idx] in WEB_MAP:
                info['sites'][idx] = WEB_MAP[info['sites'][idx]]
        if os.path.isfile('stop_signal/{}'.format(info['crawl_id'])):
                os.remove('stop_signal/{}'.format(info['crawl_id']))
    else:
        local_file = open(local, 'r', encoding='utf-8-sig')
        local_data = json.load(local_file)
        keywords = []
        for label_type in local_data:
            for label in local_data[label_type]:
                keywords.append(label_type + "_" + label)
        info = {"sites": ['wiki_zh', 'baidu'],
                         "keywords": keywords, "crawl_id": 'local'}

    print(info)
    # return
    #   run spiders
    try:
        logger.info('TextCrawler On!')
        if info is not None:
            keywords = info['keywords']
            sites = info['sites']
            spiders_count = len(keywords) * len(sites)
            configure_logging()
            runner = CrawlerRunner(get_project_settings())
            for keyword in keywords:
                for site in sites:
                    if site == 'wiki' and is_chinese(keyword):
                        runner.crawl('wiki_zh', keyword=keyword, crawl_id=info['crawl_id'], database=transwarp, spiders_count=spiders_count)
                    else:
                        runner.crawl(site, keyword=keyword, crawl_id=info['crawl_id'], database=transwarp, spiders_count=spiders_count)
                    d = runner.join()
                    d.addBoth(stop)
            try:
                reactor.run()
            except error.ReactorAlreadyRunning:
                pass
            
    except Exception as e:
        print("scrapy finished, INFO: {}".format(e))
    finally:
        # 完成任务结束后续任务
        # 上传关键字爬取状态（数量）
        # for keyword in keywords:
        #     logger.info(insert_crawl_stats(keyword=keyword, crawl_id=info['crawl_id'], connect=transwarp))
        
        # 上传hdfs和文件信息到
        logger.info(upload_crawl_file(crawl_file_list("./result"), transwarp))
        
        # 上传爬取图片
        logger.info(upload_crawl_img_new('result/url', 'result/Images', transwarp))

        # 删除本次http交互量
        sql_ = "DELETE FROM hsold.text_crawl_http_interact WHERE crawl_id = ?"
        pram_ = [info['crawl_id']]
        transwarp.execute_sql(sql_, pram_)
        # 删除程序运行状态
        sql_ = "DELETE FROM hsold.text_crawler_running WHERE crawl_id = ?"
        transwarp.execute_sql(sql_, pram_)
        logger.info('scrapy finished')
    # stop()
    pass


if __name__ == '__main__':
    # start_spiders()
    transwarp = Transwarp("Transwarp/JavaJar/InceptorUtil.jar", "Transwarp/libs")
    # transwarp.connect_hdfs()
    # transwarp.download_file('.','/hs/text_crawl_file/nasa_20210913223012944432.json')
    transwarp.connect_inceptor()
    # crawling('local', transwarp)
    # transwarp.upload_file(r"E:\workspace\ConnectDB\out\artifacts\InceptorUtil\Util.jar", "/text_crawl")
    info = {'crawl_id':'local', 'sites':['wiki_zh', 'baidu', 'twitter'], 'keywords': ['目标_花莲机场', '武器弹药_穿甲弹', '装备_M1A2坦克'], 'status': "1"}
    start_spiders(info=info, transwarp=None)
    # 上传关键字爬取状态（数量）
    # for keyword in keywords:
    #     logger.info(insert_crawl_stats(keyword=keyword, crawl_id=info['crawl_id'], connect=transwarp))
    
    # # 上传hdfs和文件信息到
    # logger.info(upload_crawl_file(crawl_file_list("result"), transwarp))
    
    # # 上传爬取图片
    # logger.info(upload_crawl_img_new('result/url', 'result/Images', transwarp))
    # # start_spiders(local='file/taiwan.json')