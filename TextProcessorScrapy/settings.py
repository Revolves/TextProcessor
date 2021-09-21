# Scrapy settings for data_scrapy project
#
# For simplicity, this file contains only settings considered important or
# commonly used. You can find more settings consulting the documentation:
#
#     https://docs.scrapy.org/en/latest/topics/settings.html
#     https://docs.scrapy.org/en/latest/topics/downloader-middleware.html
#     https://docs.scrapy.org/en/latest/topics/spider-middleware.html
import datetime
import os

BOT_NAME = 'TextProcessorScrapy'

# 保存未检验代理的Redis key
PROXIES_UNCHECKED_LIST = 'proxies:unchecked:list'

# 已经存在的未检验HTTP代理和HTTPS代理集合
PROXIES_UNCHECKED_SET = 'proxies:unchecked:set'

# 代理地址的格式化字符串
PROXY_URL_FORMATTER = '%(schema)s://%(ip)s:%(port)s'

# 通用请求头字段
DEFAULT_REQUEST_HEADERS = {
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8',
    'Accept-Encoding': 'gzip, deflate, br',
    'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8,zh-TW;q=0.7',
    'Connection': 'keep-alive',
    'cookie': 'xxxxxxxxx'
}

SPIDER_MODULES = ['TextProcessorScrapy.spiders']
NEWSPIDER_MODULE = 'TextProcessorScrapy.spiders'

# Crawl responsibly by identifying yourself (and your website) on the user-agent
# USER_AGENT = 'data_scrapy (+http://www.yourdomain.com)'
USER_AGENTS_LIST = [
    "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.1 (KHTML, like Gecko) Chrome/22.0.1207.1 Safari/537.1",
    "Mozilla/5.0 (X11; CrOS i686 2268.111.0) AppleWebKit/536.11 (KHTML, like Gecko) Chrome/20.0.1132.57 Safari/536.11",
    "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/536.6 (KHTML, like Gecko) Chrome/20.0.1092.0 Safari/536.6",
    "Mozilla/5.0 (Windows NT 6.2) AppleWebKit/536.6 (KHTML, like Gecko) Chrome/20.0.1090.0 Safari/536.6",
    "Mozilla/5.0 (Windows NT 6.2; WOW64) AppleWebKit/537.1 (KHTML, like Gecko) Chrome/19.77.34.5 Safari/537.1",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/536.5 (KHTML, like Gecko) Chrome/19.0.1084.9 Safari/536.5",
    "Mozilla/5.0 (Windows NT 6.0) AppleWebKit/536.5 (KHTML, like Gecko) Chrome/19.0.1084.36 Safari/536.5",
    "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/536.3 (KHTML, like Gecko) Chrome/19.0.1063.0 Safari/536.3",
    "Mozilla/5.0 (Windows NT 5.1) AppleWebKit/536.3 (KHTML, like Gecko) Chrome/19.0.1063.0 Safari/536.3",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_8_0) AppleWebKit/536.3 (KHTML, like Gecko) Chrome/19.0.1063.0 Safari/536.3",
    "Mozilla/5.0 (Windows NT 6.2) AppleWebKit/536.3 (KHTML, like Gecko) Chrome/19.0.1062.0 Safari/536.3",
    "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/536.3 (KHTML, like Gecko) Chrome/19.0.1062.0 Safari/536.3",
    "Mozilla/5.0 (Windows NT 6.2) AppleWebKit/536.3 (KHTML, like Gecko) Chrome/19.0.1061.1 Safari/536.3",
    "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/536.3 (KHTML, like Gecko) Chrome/19.0.1061.1 Safari/536.3",
    "Mozilla/5.0 (Windows NT 6.1) AppleWebKit/536.3 (KHTML, like Gecko) Chrome/19.0.1061.1 Safari/536.3",
    "Mozilla/5.0 (Windows NT 6.2) AppleWebKit/536.3 (KHTML, like Gecko) Chrome/19.0.1061.0 Safari/536.3",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/535.24 (KHTML, like Gecko) Chrome/19.0.1055.1 Safari/535.24",
    "Mozilla/5.0 (Windows NT 6.2; WOW64) AppleWebKit/535.24 (KHTML, like Gecko) Chrome/19.0.1055.1 Safari/535.24"
]
# PROXIES = [
#     "https://171.13.92.212:9797",
#     "https://164.163.234.210:8080",
#     "https://143.202.73.219:8080",
#     "https://103.75.166.15:8080"
# ]

# Obey robots.txt rules
ROBOTSTXT_OBEY = False

# Configure maximum concurrent requests performed by Scrapy (default: 16)
CONCURRENT_REQUESTS = 8
REACTOR_THREADPOOL_MAXSIZE = 20
# Configure a delay for requests for the same website (default: 0)
# See https://docs.scrapy.org/en/latest/topics/settings.html#download-delay
# See also autothrottle settings and docs
DOWNLOAD_DELAY = 2
# The download delay setting will honor only one of:
CONCURRENT_REQUESTS_PER_DOMAIN = 32
CONCURRENT_REQUESTS_PER_IP = 128

# Disable cookies (enabled by default)
COOKIES_ENABLED = False
HTTPERROR_ALLOWED_CODES = [302]
# 重定向
REDIRECT_ENABLED = True
COMMANDS_MODULE = 'TextProcessorScrapy.commands'
# Disable Telnet Console (enabled by default)
# TELNETCONSOLE_ENABLED = False

# Override the default request headers:
# DEFAULT_REQUEST_HEADERS = {
#   'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
#   'Accept-Language': 'en',
# }

# Enable or disable spider middlewares
# See https://docs.scrapy.org/en/latest/topics/spider-middleware.html
SPIDER_MIDDLEWARES = {
    'TextProcessorScrapy.middlewares.TextCrawlSpiderMiddleware': 543,
    # 'TextProcessorScrapy.middlewares.HttpProxymiddleware': 541
}

# Enable or disable downloader middlewares
# See https://docs.scrapy.org/en/latest/topics/downloader-middleware.html
DOWNLOADER_MIDDLEWARES = {
    'TextProcessorScrapy.middlewares.TextCrawlDownloaderMiddleware': 543,
    # 'TextProcessorScrapy.middlewares.RandomUserAgentMiddleware': 515,
    # 'TextProcessorScrapy.middlewares.RandomProxyMiddleware': 123,
}

# Enable or disable extensions
# See https://docs.scrapy.org/en/latest/topics/extensions.html
# EXTENSIONS = {
#    'scrapy.extensions.telnet.TelnetConsole': None,
# }

# Configure item pipelines
# See https://docs.scrapy.org/en/latest/topics/item-pipeline.html
ITEM_PIPELINES = {
    # 'TextProcessorScrapy.pipelines.HsNasaPipeline': 400,
    # # 'TextProcessor.pipelines.ProxyPoolPipeline': 300
    # 'TextProcessorScrapy.pipelines.TwitterPipeline': 500,
    # 'TextProcessorScrapy.pipelines.FacebookPipeline': 300

}

# Enable and configure the AutoThrottle extension (disabled by default)
# See https://docs.scrapy.org/en/latest/topics/autothrottle.html
# AUTOTHROTTLE_ENABLED = True
# The initial download delay
# AUTOTHROTTLE_START_DELAY = 5
# The maximum download delay to be set in case of high latencies
# AUTOTHROTTLE_MAX_DELAY = 60
# The average number of requests Scrapy should be sending in parallel to
# each remote server
# AUTOTHROTTLE_TARGET_CONCURRENCY = 1.0
# Enable showing throttling stats for every response received:
# AUTOTHROTTLE_DEBUG = False

# Enable and configure HTTP caching (disabled by default)
# See https://docs.scrapy.org/en/latest/topics/downloader-middleware.html#httpcache-middleware-settings
# HTTPCACHE_ENABLED = True
# HTTPCACHE_EXPIRATION_SECS = 0
# HTTPCACHE_DIR = 'httpcache'
# HTTPCACHE_IGNORE_HTTP_CODES = []
# HTTPCACHE_STORAGE = 'scrapy.extensions.httpcache.FilesystemCacheStorage'


# LOG_LEVEL = 'INFO'
to_day = datetime.datetime.now()
count = 0
log_file_path = 'log/scrapy_{}_{}_{}_{}.log'.format(to_day.year, to_day.month, to_day.day, count)
while os.path.isfile(log_file_path) is True:
    count += 1
    log_file_path = 'log/scrapy_{}_{}_{}_{}.log'.format(to_day.year, to_day.month, to_day.day, count)
LOG_FILE = log_file_path
#
# # 配置Scrapy-Redis
# # 指定Redis的主机名和端口
# REDIS_HOST = '127.0.0.1'
# REDIS_PORT = 6379
# REDIS_PARAMS = {'password': 'password'}
#
# # 调度器启用Redis存储Requests队列
# SCHEDULER = "scrapy_redis.scheduler.Scheduler"
#
# # 确保所有的爬虫实例使用Redis进行重复过滤
# DUPEFILTER_CLASS = "scrapy_redis.dupefilter.RFPDupeFilter"
#
# # 将Requests队列持久化到Redis，可支持暂停或重启爬虫
# SCHEDULER_PERSIST = True
#
# # Requests的调度策略，默认优先级队列
# SCHEDULER_QUEUE_CLASS = 'scrapy_redis.queue.PriorityQueue'
