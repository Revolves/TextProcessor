"""
user: adm
date: 2021/7/23
time: 15:21
IDE: PyCharm  
"""
import logging
import threading
import multiprocessing
import time
import scrapy
from multiprocessing import Process
from scrapy.crawler import CrawlerProcess, CrawlerRunner
from threading import Thread
from twisted.internet import reactor
from scrapy.utils.project import get_project_settings
from scrapy.utils.log import configure_logging
from scrapy.cmdline import execute

# from spiders.Twitter import TwitterSpider
# from spiders.NASA import nasaSpider

# from spiders.Facebook import FacebookSpider
# from spiders.Aiaa import AiaaSpider

logging.basicConfig(level=logging.DEBUG,
                    format='[%(asctime)-15s] [%(levelname)8s] [%(name)10s ] - %(message)s (%(filename)s:%(lineno)s)',
                    datefmt='%Y-%m-%d %T'
                    )
logger = logging.getLogger(__name__)


def StartNasa():
    def run():
        execute(['scrapy', 'crawl', 'NASA', '--nolog', '-a', 'keyword=target'])

    run()


def StartTwitter():
    def run():
        execute(['scrapy', 'crawl', 'twitter', '--nolog', '-a', 'keyword=target'])

    run()


def ControlInfo():
    """
    解析控制信息
    :return: 爬取站点列表、关键词
    """
    Info = {
        "sites": ['NASA', 'Twitter', 'Facebook', 'Aiaa'],
        "keywords": ['target']
    }
    return Info


def SpiderStart(site):
    def run():
        logger.info('Spider {} is starting!'.format(site))
        StartNasa()
        StartTwitter()

        # execute(['scrapy', 'crawl', site, '--nolog', '-a', 'keyword={}'.format(keyword)])
        # configure_logging()
        # runner = CrawlerRunner(get_project_settings())
        # runner.crawl(site, keyword=keyword)
        # d = runner.join()
        # d.addBoth(lambda _: reactor.stop())
        # reactor.run()

    r = threading.Thread(target=run)
    r.start()


# 写数据进程执行的代码
def proc_send(pipe, site, start):
    # print 'Process is write....'
    logger.info('Now NASA Spider Will Start!')
    pipe.send(start)


# 读数据进程的代码
def proc_recv(pipe):
    while True:
        if pipe.recv():
            print('running')
            StartNasa()
            logger.info('Now NASA Spider is Starting!')


if __name__ == '__main__':
    # 父进程创建pipe，并传给各个子进程
    # Nasa_pipe = multiprocessing.Pipe()
    # p1 = multiprocessing.Process(target=proc_send, args=(Nasa_pipe[0], 'NASA', True))
    # p2 = multiprocessing.Process(target=proc_recv, args=(Nasa_pipe[1],))
    # # 启动子进程，写入
    # p1.start()
    # p2.start()
    #
    # p1.join()
    # p2.terminate()
    execute(['scrapy', 'RunAll', '-a', 'keyword=LOL'])
    # info = ControlInfo()
    # Crawler_list = {'NASA': nasaSpider, 'Twitter': TwitterSpider, 'Facebook': FacebookSpider, 'Aiaa': AiaaSpider}
