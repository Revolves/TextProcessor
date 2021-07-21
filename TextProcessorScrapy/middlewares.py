# Define here the models for your spider middleware
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/spider-middleware.html
import logging
import random

from scrapy.downloadermiddlewares.retry import RetryMiddleware

from data_scrapy.settings import USER_AGENTS_LIST
from scrapy import signals
from twisted.internet import defer
from twisted.internet.error import TCPTimedOutError, ConnectionDone, ConnectError, ConnectionLost
from data_scrapy.utils import get_random_ip
from data_scrapy.utils import logger
import redis


# useful for handling different item types with a single interface


class HsNasaSpiderMiddleware:
    # Not all methods need to be defined. If a method is not defined,
    # scrapy acts as if the spider middleware does not modify the
    # passed objects.

    @classmethod
    def from_crawler(cls, crawler):
        # This method is used by Scrapy to create your spiders.
        s = cls()
        crawler.signals.connect(s.spider_opened, signal=signals.spider_opened)
        return s

    def process_spider_input(self, response, spider):
        # Called for each response that goes through the spider
        # middleware and into the spider.

        # Should return None or raise an exception.
        return None

    def process_spider_output(self, response, result, spider):
        # Called with the results returned from the Spider, after
        # it has processed the response.

        # Must return an iterable of Request, or item objects.
        for i in result:
            yield i

    def process_spider_exception(self, response, exception, spider):
        # Called when a spider or process_spider_input() method
        # (from other spider middleware) raises an exception.

        # Should return either None or an iterable of Request or item objects.
        pass

    def process_start_requests(self, start_requests, spider):
        # Called with the start requests of the spider, and works
        # similarly to the process_spider_output() method, except
        # that it doesn’t have a response associated.

        # Must return only requests (not items).
        for r in start_requests:
            yield r

    def spider_opened(self, spider):
        spider.logger.info('Spider opened: %s' % spider.name)


class HsNasaDownloaderMiddleware:
    # Not all methods need to be defined. If a method is not defined,
    # scrapy acts as if the downloader middleware does not modify the
    # passed objects.

    @classmethod
    def from_crawler(cls, crawler):
        # This method is used by Scrapy to create your spiders.
        s = cls()
        crawler.signals.connect(s.spider_opened, signal=signals.spider_opened)
        return s

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


class RandomProxyMiddleware(object):
    """
    从setting的PROXY列表随机选择一个作为代理
    """

    def process_request(self, request, spider):
        # proxy = random.choice(spider.settings['PROXIES'])
        proxy = get_random_ip()
        request.meta['proxy'] = proxy
        # return resp

    def process_response(self, request, response, spider):
        """
        返回状态不是200，则重新生成request对象
        :param request:
        :param response:
        :param spider:
        :return:
        """
        if response.status != 200:
            proxy = get_random_ip()
            print("this is response ip:" + proxy)
            # 对当前request加上代理
            request.meta['proxy'] = proxy
            return request
        return response


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
        # 验证 User-Agent 设置是否生效
        logger.info("headers ::> User-Agent = " + str(request.headers['User-Agent'], encoding="utf8"))
        return response


class HttpProxymiddleware(object):
    # 一些异常情况汇总
    EXCEPTIONS_TO_CHANGE = (
        defer.TimeoutError, TimeoutError, ConnectionRefusedError, ConnectError, ConnectionLost,
        TCPTimedOutError, ConnectionDone)

    def __init__(self):
        # 链接数据库 decode_responses设置取出的编码为str
        self.redis = redis.from_url('redis://:你的密码@localhost:6379/0', decode_responses=True)
        pass

    def process_request(self, request, spider):
        # 拿出全部key，随机选取一个键值对
        keys = self.rds.hkeys("xila_hash")
        key = random.choice(keys)
        # 用eval函数转换为dict
        proxy = eval(self.rds.hget("xila_hash", key))
        logger.warning("-----------------" + str(proxy) + "试用中------------------------")
        # 将代理ip 和 key存入mate
        request.meta["proxy"] = proxy["ip"]
        request.meta["accountText"] = key

    def process_response(self, request, response, spider):
        http_status = response.status
        # 根据response的状态判断 ，200的话ip的times +1重新写入数据库，返回response到下一环节
        if http_status == 200:
            key = request.meta["accountText"]
            proxy = eval(self.rds.hget("xila_hash", key))
            proxy["times"] = proxy["times"] + 1
            self.rds.hset("xila_hash", key, proxy)
            return response
        # 403有可能是因为user-agent不可用引起，和代理ip无关，返回请求即可
        elif http_status == 403:
            logging.warning("#########################403重新请求中############################")
            return request.replace(dont_filter=True)
        # 其他情况姑且被判定ip不可用，times小于10的，删掉，大于等于10的暂时保留
        else:
            ip = request.meta["proxy"]
            key = request.meta["accountText"]
            proxy = eval(self.rds.hget("xila_hash", key))
            if proxy["times"] < 10:
                self.rds.hdel("xila_hash", key)
            logging.warning("#################" + ip + "不可用，已经删除########################")
            return request.replace(dont_filter=True)

    def process_exception(self, request, exception, spider):
        # 其他一些timeout之类异常判断后的处理，ip不可用删除即可
        if isinstance(exception, self.EXCEPTIONS_TO_CHANGE) \
                and request.meta.get('proxy', False):
            key = request.meta["accountText"]
            print("+++++++++++++++++++++++++{}不可用+++将被删除++++++++++++++++++++++++".format(key))
            proxy = eval(self.rds.hget("xila_hash", key))
            if proxy["times"] < 10:
                self.rds.hdel("xila_hash", key)
            logger.debug("Proxy {}链接出错{}.".format(request.meta['proxy'], exception))
            return request.replace(dont_filter=True)
