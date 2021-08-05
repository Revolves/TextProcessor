import math
import re
import time
from datetime import datetime, date, timedelta
from ..items import DataItem
import scrapy
from selenium import webdriver


class JanesSpider(scrapy.Spider):
    task_id = 0
    name = 'janes'
    custom_settings = {
        'ITEM_PIPELINES': {'TextProcessorScrapy.pipelines.JanesPipeline': 400},
    }

    def __init__(self, *args, **kwargs):
        super(JanesSpider, self).__init__(*args, **kwargs)
        self.allowed_domains = ['janes.com']
        if 'keyword' in kwargs:
            self.keyword = kwargs['keyword']
        url = 'https://www.janes.com/search-results?indexCatalogue=all---production&searchQuery=' + self.keyword + \
              '&wordsMode=AllWords&orderBy=Newest'
        self.start_urls.append(url)

    def parse(self, response):
        time.sleep(5)
        news_num = response.xpath("//div[@class = 'search-results-wrp']/@data-search-count").get()
        # news_num = response.xpath("//*[@id='collapseSearch']/small").get()
        print("多少页？" + str(news_num))
        try:
            news_num = int(re.findall("\d+", str(news_num))[0])
            # 可以查询多少页,向上取整
            news_page = math.ceil(news_num / 20)
        except:
            print("未找到数据")
            return
        yield scrapy.Request(url=response.url, callback=self.parse_more_page, dont_filter=True)
        # 因为每个关键词只爬一条，所以不需要
        for i in range(2, news_page):
            url = "https://www.janes.com/search-results/" + str(i) + \
                  "?indexCatalogue=all---production&searchQuery=target&wordsMode=AllWords"
            yield scrapy.Request(url=url, callback=self.parse_more_page, dont_filter=True)

    def parse_more_page(self, response):
        news_urls = []
        news_list = response.xpath("//div[@class = 'sf-search-results media-list extra-small-list']")

        # 如果没有找到信息
        if len(news_list) == 0:
            print("无此信息")
            item = DataItem()
            item['title'] = ''
            item['date'] = '0'
            item['keyword'] = self.keyword
            item['url'] = ''
            item['poster'] = ''
            item['content'] = ''
            yield item
            return
        for news in news_list:
            new = news.xpath("./div")
            for n in new:
                href = n.xpath("./a/@href").get()
                news_urls.append(href)
                # print(href)
        print("查找到了多少消息：")
        print(len(news_urls))
        for news_url in news_urls:
            yield scrapy.Request(news_url, callback=self.parse_dir_contents)
            # break

    def parse_dir_contents(self, response):
        # 爬取标题
        news_title = response.xpath("//h1/text()").get()
        print("爬取到的标题为：")
        print(news_title)
        # 爬取时间
        try:
            news_time = response.xpath("//p[@itemprop = 'dateModified']/span/text()").get()
            # 对时间进行格式化
            gmt_format = '%d %B %Y'
            publish_time = str(datetime.strptime(news_time, gmt_format))[0:10].replace(' ', '') \
                .replace(':', '').replace('-', '')
        except:
            print("无时间")
            publish_time = '0'

        # 获取昨天时间
        now_time = (date.today() + timedelta(days=-7)).strftime("%Y%m%d")
        # 判断是否是近两天发表
        # if int(publish_time) < int(now_time):
        #     return
        # 爬取正文
        content_pre = response.selector.xpath("//h1/../p[not(@class)]")
        news_content = content_pre.xpath('string(.)').extract()
        news_text = ''
        if news_content is not None:
            for i in news_content:
                temp = i
                if i == '\n':
                    continue
                # if i == news_content[1]:
                #     i = str(i).replace('\n', '')
                # else:
                #     i = '' + str(i).replace('\n', '')
                news_text = news_text + str(i).replace('\n', '')
                # if temp != news_content[-1]:
                #     print("不是最后一句")
                #     print(str(i))
                #     news_text = news_text + '\n'

        # 爬取作者
        try:
            news_author = response.xpath("//h1/../p[2]/text()").extract()[1]
        except:
            print("无作者")
            news_author = ''
        news_author = str(news_author).replace("\n", '')
        news_author = str(news_author).replace(" ", '')
        print('作者为：')
        print(news_author)

        item = DataItem()
        item['source'] = 'baidu'
        item['title'] = str(news_title)
        item['date'] = str(publish_time)
        item['keyword'] = self.keyword
        item['url'] = response.url
        item['content'] = str(news_text)
        yield item

    @staticmethod
    def get_driver():
        chrome_opt = webdriver.ChromeOptions()
        # 禁止加载图片和css
        prefs = {"profile.managed_default_content_settings.images": 2,
                 'permissions.default.stylesheet': 2,
                 'profile.default_content_setting_values':
                     {'notifications': 2}  # 禁止谷歌浏览器弹出通知消息
                 }
        chrome_opt.add_experimental_option("prefs", prefs)
        # 禁止打印日志
        chrome_opt.add_experimental_option('excludeSwitches', ['enable-logging'])
        chrome_opt.add_argument('--disable-gpu')  # 禁用GPU
        chrome_opt.add_argument('log-level=3')
        # chrome_opt.add_argument('disable-cache')  # 禁用缓存
        # 设置无界面浏览器
        # chrome_opt.add_argument('headless')
        # 打开浏览器
        driver = webdriver.Chrome(chrome_options=chrome_opt)
        # 最大化窗口
        driver.maximize_window()
        driver.implicitly_wait(2)
        return driver
