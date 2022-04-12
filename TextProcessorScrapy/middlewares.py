# coding:utf-8
# Define here the models for your spider middleware
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/spider-middleware.html
from asyncio.log import logger
import imp
import logging
import os
import random
import sys
import threading
import time
import schedule
from scrapy import signals
# from TextProcessorScrapy.utils.utils import get_random_ip
from twisted.internet import task
from hashlib import md5

from TextProcessorScrapy.settings import USER_AGENTS_LIST
# from utils.UpdateShowTable import *
# useful for handling different item types with a single interface

CRAWL_ID = None
DATABASE = None
COUNT = 0
FIRST = True
INTERACT = 0
SPIDERSNUMBER = 1
DB = None

def rowkey_id_gen():
    return md5(str(time.time()).encode()).hexdigest()


def insert_http_interact():
    global SPIDERSNUMBER
    while SPIDERSNUMBER > 0:
        time.sleep(10)
        global CRAWL_ID, INTERACT, COUNT
        if DB is not None:
            try:
                sql_ = "INSERT INTO  hsold.text_crawl_http_interact  VALUES (?, hsold.sequence_get_id.NEXTVAL,?,?)"
                pram_ = [rowkey_id_gen(),CRAWL_ID, str(INTERACT)]
                DB.execute_sql(sql_, pram_)
            except:
                logging.error("http interact insert failure")
            logging.info("{} ten seconds http interact :{}".format(CRAWL_ID, str(INTERACT * SPIDERSNUMBER)))
            INTERACT = 0

# def updateShowTable():
#     global CRAWL_ID, COUNT
#     if DB is not None:
#         keywords_count_list, sites_count_list = getCrawlStatus()
#         updateTextCrawlStatus(keywords_count_list, CRAWL_ID, DB)
#         updateShowHttpInteract(DB)
#         updateShowTextKeyword(DB)
#         updateShowTextStatus(sites_count_list, DB)

insert_th = threading.Thread(target=insert_http_interact)
insert_th.start()


class TextCrawlSpiderMiddleware:
    # Not all methods need to be defined. If a method is not defined,
    # scrapy acts as if the spider middleware does not modify the
    # passed objects.

    def __init__(self, stats):
        self.stats = stats
        # 每隔多少秒监控一次已抓取数量
        self.time = 10.0
        self.last_count = 0

    @classmethod
    def from_crawler(cls, crawler, *args, **kwargs):
        instance = cls(crawler.stats)
        crawler.signals.connect(instance.spider_opened, signal=signals.spider_opened)
        crawler.signals.connect(instance.spider_closed, signal=signals.spider_closed)
        return instance

    def spider_opened(self):
        pass
        # self.tsk = task.LoopingCall(self.collect)
        # self.tsk.start(self.time, now=True)

    def spider_closed(self):
        if CRAWL_ID:
            http_interact = self.stats.get_value('log_count/DEBUG')
            this_time_http_interact = http_interact - self.last_count
            self.last_count = http_interact
            logging.info("{} ten seconds http interact :{}".format(CRAWL_ID, this_time_http_interact))
        if self.tsk.running:
            self.tsk.stop()

    def collect(self):
        # 这里收集stats并写入相关的储存。
        # 目前展示是输出到终端
        http_interact = self.stats.get_value('log_count/DEBUG')
        this_time_http_interact = http_interact - self.last_count
        self.last_count = http_interact


class TextCrawlDownloaderMiddleware:
    # Not all methods need to be defined. If a method is not defined,
    # scrapy acts as if the downloader middleware does not modify the
    # passed objects.
    def __init__(self, stats):
        self.stats = stats
        # 每隔多少秒监控一次已抓取数量
        self.time = 10.0
        self.last_count = 0

    @classmethod
    def from_crawler(cls, crawler, *args, **kwargs):
        instance = cls(crawler.stats)
        crawler.signals.connect(instance.spider_opened, signal=signals.spider_opened)
        crawler.signals.connect(instance.spider_closed, signal=signals.spider_closed)
        return instance

    def process_request(self, request, spider):
        # Called for each request that goes through the downloader
        # middleware.

        # Must either:
        # - return None: continue processing this request
        # - or return a Response object
        # - or return a Request object
        # - or raise IgnoreRequest: process_exception() methods of
        #   installed downloader middleware will be called
        return None

    def process_response(self, request, response, spider):
        # Called with the response returned from the downloader.

        # Must either;
        # - return a Response object
        # - return a Request object
        # - or raise IgnoreRequest
        return response

    def process_exception(self, request, exception, spider):
        # Called when a download handler or a process_request()
        # (from other downloader middleware) raises an exception.

        # Must either:
        # - return None: continue processing this exception
        # - return a Response object: stops process_exception() chain
        # - return a Request object: stops process_exception() chain
        pass

    def spider_opened(self, spider):
        spider.logger.info('Spider opened: %s' % spider.name)
        global FIRST, CRAWL_ID, DB, SPIDERSNUMBER
        self.tsk = task.LoopingCall(self.collect, spider=spider)
        self.tsk.start(self.time, now=True)
        self.tsk1 = task.LoopingCall(self.check_stop, spider=spider)
        self.tsk1.start(self.time, now=True)
        if FIRST:
            CRAWL_ID = spider.crawl_id
            DB = spider.database
            SPIDERSNUMBER = spider.spiders_count
            FIRST = False

    def spider_closed(self, spider):
        try:
            if self.tsk.running:
                self.tsk.stop()
            self.collect(spider)
        except:
            pass
        global SPIDERSNUMBER
        SPIDERSNUMBER -= 1

    def collect(self, spider):
        global COUNT, CRAWL_ID, INTERACT, SPIDERSNUMBER
        if CRAWL_ID:
            http_interact = self.stats.get_value('log_count/DEBUG')
            if http_interact:
                this_time_http_interact = http_interact - self.last_count
                self.last_count = http_interact
                INTERACT += this_time_http_interact
                # t = threading.Thread(target=pushData, args=(this_time_http_interact,))
                # t = threading.Thread(target=insert, args=(CRAWL_ID, this_time_http_interact, spider.database,))
                # t.start()
                # COUNT += 1

    def check_stop(self, spider):
        if os.path.isfile('stop_signal/{}'.format(spider.crawl_id)):
            os.remove('stop_signal/{}'.format(spider.crawl_id))
            # spider.crawler.engine.close_spider(spider, 'Actively Stop the Crawler')
            raise Exception('Actively Stop the Crawler')
            try:
                spider.crawler.engine.close_spider(spider, 'Actively Stop the Crawler')
            except Exception as e:
                logger.exception("Actively Stop Exception:{}".format(e))
                self.spider_closed(spider)

#
# class RandomProxyMiddleware(object):
#     """
#     从setting的PROXY列表随机选择一个作为代理
#     """
#
#     def process_request(self, request, spider):
#         # proxy = random.choice(spider.settings['PROXIES'])
#         proxy = get_random_ip()
#         request.meta['proxy'] = proxy
#         # return resp
#
#     def process_response(self, request, response, spider):
#         """
#         返回状态不是200，则重新生成request对象
#         :param request:
#         :param response:
#         :param spider:
#         :return:
#         """
#         if response.status != 200:
#             proxy = get_random_ip()
#             print("this is response ip:" + proxy)
#             # 对当前request加上代理
#             request.meta['proxy'] = proxy
#             return request
#         return response


# class NewRetryMiddleware(RetryMiddleware):
#     super.__init__(self=self)
#     if '10061' in str(exception) or '10060' in str(exception):
#         self.proxy_ip = fetch_proxy_ip()
#
#     if self.proxy_ip:
#         current_proxy = f'http://{self.proxy_ip}'
#         request.meta['proxy'] = current_proxy
#
#     if isinstance(exception, self.EXCEPTIONS_TO_RETRY) and not request.meta.get('dont_retry', False):
#         return self._retry(request, exception, spider)

# 随机选择 User-Agent 的下载器中间件
class RandomUserAgentMiddleware(object):
    def process_request(self, request, spider):
        # 从 settings 的 USER_AGENTS 列表中随机选择一个作为 User-Agent
        user_agent = random.choice(USER_AGENTS_LIST)
        request.headers['User-Agent'] = user_agent
        # return None

    def process_response(self, request, response, spider):
        return response

#
# class HttpProxymiddleware(object):
#     # 一些异常情况汇总
#     EXCEPTIONS_TO_CHANGE = (
#         defer.TimeoutError, TimeoutError, ConnectionRefusedError, ConnectError, ConnectionLost,
#         TCPTimedOutError, ConnectionDone)
#
#     def __init__(self):
#         # 链接数据库 decode_responses设置取出的编码为str
#         self.redis = redis.from_url('redis://:你的密码@localhost:6379/0', decode_responses=True)
#         pass
#
#     def process_request(self, request, spider):
#         # 拿出全部key，随机选取一个键值对
#         keys = self.rds.hkeys("xila_hash")
#         key = random.choice(keys)
#         # 用eval函数转换为dict
#         proxy = eval(self.rds.hget("xila_hash", key))
#         # 将代理ip 和 key存入mate
#         request.meta["proxy"] = proxy["ip"]
#         request.meta["accountText"] = key
#
#     def process_response(self, request, response, spider):
#         http_status = response.status
#         # 根据response的状态判断 ，200的话ip的times +1重新写入数据库，返回response到下一环节
#         if http_status == 200:
#             key = request.meta["accountText"]
#             proxy = eval(self.rds.hget("xila_hash", key))
#             proxy["times"] = proxy["times"] + 1
#             self.rds.hset("xila_hash", key, proxy)
#             return response
#         # 403有可能是因为user-agent不可用引起，和代理ip无关，返回请求即可
#         elif http_status == 403:
#             logging.warning("#########################403重新请求中############################")
#             return request.replace(dont_filter=True)
#         # 其他情况姑且被判定ip不可用，times小于10的，删掉，大于等于10的暂时保留
#         else:
#             ip = request.meta["proxy"]
#             key = request.meta["accountText"]
#             proxy = eval(self.rds.hget("xila_hash", key))
#             if proxy["times"] < 10:
#                 self.rds.hdel("xila_hash", key)
#             logging.warning("#################" + ip + "不可用，已经删除########################")
#             return request.replace(dont_filter=True)
#
#     def process_exception(self, request, exception, spider):
#         # 其他一些timeout之类异常判断后的处理，ip不可用删除即可
#         if isinstance(exception, self.EXCEPTIONS_TO_CHANGE) \
#                 and request.meta.get('proxy', False):
#             key = request.meta["accountText"]
#             print("+++++++++++++++++++++++++{}不可用+++将被删除++++++++++++++++++++++++".format(key))
#             proxy = eval(self.rds.hget("xila_hash", key))
#             if proxy["times"] < 10:
#                 self.rds.hdel("xila_hash", key)
#             return request.replace(dont_filter=True)
