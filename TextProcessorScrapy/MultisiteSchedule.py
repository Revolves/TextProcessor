"""
user: adm
date: 2021/7/23
time: 15:21
IDE: PyCharm  
"""
import logging
import threading

from scrapy.cmdline import execute

# from spiders.Twitter import TwitterSpider
# from spiders.NASA import nasaSpider

# from spiders.Facebook import FacebookSpider
# from spiders.Aiaa import AidaSpider

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
    info = ControlInfo() # 控制信息
    execute(['scrapy', 'RunAll', '-a', 'keyword=LOL'])
    # info = ControlInfo()
    # Crawler_list = {'NASA': nasaSpider, 'Twitter': TwitterSpider, 'Facebook': FacebookSpider, 'Aiaa': AiaaSpider}
