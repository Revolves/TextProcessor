"""
user: adm
date: 2021/7/23
time: 15:21
IDE: PyCharm  
"""
import json
import logging
from multiprocessing import Process
import os, sys
from threading import Thread

from scrapy.crawler import CrawlerRunner, Crawler
from scrapy.utils.log import configure_logging
from scrapy.utils.project import get_project_settings
from twisted.internet import reactor, error
from hashlib import md5
from crochet import setup, run_in_reactor
setup()
# 自定义工具·
from Transwarp import Transwarp
from DataCleaning import DataCleaning
from utils import *

logging.basicConfig(level=logging.DEBUG,
                    format='[%(asctime)-15s] [%(levelname)8s] [%(name)10s ] - %(message)s (%(filename)s:%(lineno)s)',
                    datefmt='%Y-%m-%d %T',
                    )

logger = logging.getLogger(__name__)

# @run_in_reactor
def start_spiders(transwarp=None, info=None):
    """
    启动爬虫

    :return:
    """ 
    def run():
        try:
            logger.info('TextCrawler On!')
            if info['status'] == '2':
                crawling(info['crawl_id'], info['data_id'], transwarp, '正在采集', '交互量测试爬虫')
            else:
                crawling(info['crawl_id'], info['data_id'], transwarp, '正在采集', ','.join(info['keywords']))
            if info is not None:
                keywords = info['keywords']
                sites = info['sites']
                spiders_count = len(keywords) * len(sites)
                configure_logging()
                runner = CrawlerRunner(get_project_settings())
                for keyword in keywords:
                    for site in sites:
                        if site == 'wiki' and is_chinese(keyword):
                            runner.crawl('wiki_zh', keyword=keyword, crawl_id=info['crawl_id'], data_id = info['data_id'], database=transwarp, spiders_count=spiders_count)
                        else:
                            runner.crawl(site, keyword=keyword, crawl_id=info['crawl_id'], data_id = info['data_id'], database=transwarp, spiders_count=spiders_count)
        except Exception as e:
            logger.exception("scrapy finished, Exception: {}".format(e))
            crawling(info['crawl_id'], info['data_id'],transwarp, '采集异常中断')
            err = True
        finally:
            logger.info('start_spider: scrapy finished!')
            pass
    run()
    pass

def start(transwarp=None, info=None, local = None):
    # 运行前参数数据处理
    if local is None:
        info = handle_info(info)
        # 网站名称转换
        for idx in range(len(info['sites'])):
            if info['sites'][idx] in WEB_MAP:
                info['sites'][idx] = WEB_MAP[info['sites'][idx]]
        
        # 删除停止文件
        if os.path.isfile('stop_signal/{}'.format(info['crawl_id'])):
                os.remove('stop_signal/{}'.format(info['crawl_id']))
    else:
        # 使用关键词文件本地文件
        local_file = open(local, 'r', encoding='utf-8-sig')
        local_data = json.load(local_file)
        keywords = []
        for label_type in local_data:
            for label in local_data[label_type]:
                keywords.append(label_type + "_" + label)
        info = {"sites": ['wiki_zh', 'baidu'],
                         "keywords": keywords, "crawl_id": 'local'}
    print(info)

    # 运行
    start_spiders(transwarp=transwarp, info=info)


if __name__ == '__main__':
    # start_spiders()
    transwarp = Transwarp("Transwarp/JavaJar/InceptorUtil.jar", "Transwarp/libs")
    transwarp.connect_inceptor()
    transwarp.connect_hdfs()
    upload_crawl_file(crawl_file_list("E:\workspace\保存文件\补充"), '20220509153412', transwarp)
    # info = {'crawl_id': 'admin_20220518102345', 'data_id': '20220518143048', 'sites': ['baidu'], 'keywords': ['武器弹药_穿甲弹'], 'status': '1', 'data_type': '文本'}#{'crawl_id':rowkey_id_gen(), 'sites':['baidu'], 'keywords': ['目标_花莲机场'], 'status': "1", 'data_id':rowkey_id_gen()}
    # # print(','.join(info['keywords']))
    # # crawling(info['crawl_id'], transwarp, '采集完成', ) #, ','.join(info['keywords']))
    # start(info=info, transwarp=transwarp)
    # post_processing(transwarp, 'admin_20220509153412', '20220509153412', '采集正常结束')
    # crawling('admin_20220509230413', 'admin_20220509230413', transwarp, '采集异常结束', 'mmxx')

    # print(os.getcwd())
