# -*- coding: utf-8 -*-
from hashlib import md5
import re
import time

import scrapy
from ..items import BaiduWikiItem
from bs4 import BeautifulSoup as bs
HTTPS = "https:"
reg = re.compile("\\[.*]")

class WikiSpider(scrapy.Spider):
    name = 'wiki'
    custom_settings = {
        'ITEM_PIPELINES': {'TextProcessorScrapy.pipelines.WikiPipeline': 400,
                            'TextProcessorScrapy.pipelines.ImagePipeline': 300},
    }
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.allowed_domains = ['en.wikipedia.org']
    # 获取每个关键词的wiki搜索初始网址（第一页）
        if 'crawl_id' in kwargs['crawl_id']:
            self.crawl_id = kwargs['crawl_id']
        if 'keyword' in kwargs:
            self.keyword = kwargs['keyword'].split('_')[-1]
            self.keyword_type = kwargs['keyword'].split('_')[0]
        if 'database' in kwargs:
            self.database = kwargs['database']
        url = "https://en.wikipedia.org/w/index.php?title=Special:Search&limit=20&offset=0&profile=default&search=" + self.keyword + "&ns0=1"
        self.start_urls.append(url)
        self.count = 0
        self.number = 0

    # 进入一级解析，starturl解析
    def parse(self, response):
        hreflist = []


        # 一页全爬,此时获取的是域名之后的那段网址
        web_node_list_href = response.xpath('//ul [@class="mw-search-results"]//li [@class="mw-search-result"]//div [@class="mw-search-result-heading"]/a/@href').extract()
        for i in range(len(web_node_list_href)):
            web_node_url = "https://en.wikipedia.org" + str(web_node_list_href[i])
            hreflist.append(web_node_url)

        # 进入二级解析，具体网址解析
        for href in hreflist:
            self.count += 1
            yield scrapy.Request(url=href, callback=self.new_parse)
            if self.count > 10:
                return


    def new_parse(self, response):
        item = BaiduWikiItem()
        Source = 'wiki'
        Title = response.xpath('//div [@id="content"]//h1 [@id="firstHeading"]/text()').extract_first()
        Website = response.request.url
        UTCDate = response.xpath('//li [@id="footer-info-lastmod"]/text()').extract_first().strip()
        strlist = UTCDate.split(" ")
        monthlist = ["January", "February", "March", "April", "May", "June", "July", "August", "September", "October","November", "December"]
        # monthlist = ["1月", "2月", "3月", "4月", "5月", "6月", "7月", "8月", "9月", "10月",
        #              "11月", "12月"]
        for i in range(len(monthlist)):
            # break
            if strlist[7] == monthlist[i]:
                month = i + 1
                if month < 10:
                    month = "0" + str(month)
                else:
                    month = str(month)
                if int(strlist[6]) < 10:
                    strlist[6] = "0" + strlist[6]
                date = strlist[8] + month + strlist[6] + strlist[10]
                Date = date.replace(",", "").replace(":", "")
                break

        # 该时间是UTC时间
        Date =  int(Date)

        # 内容出现{/displaystyle }字样，大多是数学公式或是其他的图片（数字或字母）的alt属性值
        Content = response.xpath('//div [@id="mw-content-text"]//p//text()').getall()
        Content = "".join(Content).replace("\n", "").replace("\"", "\'").replace("\\", "/").strip()

        #获取属性
        attributes = {}
        soup = bs(response.text, "html.parser")
        info_title = soup.find("h1", {'class', 'firstHeading'}).text.split('[')[0]  # 获取主页标题
        info_tables = soup.find_all("table", {'class', 'infobox'})  # 包含信息的表格

        image_urls = []  # 页面图像连接集合 （//div[@class='thumbinner']）图像标签
        flag_name = True  # 是否获取表名
        # flag_image = True  # 是否已保存图像
        weapon_name = info_title  # 装备名
        attributes[weapon_name] = {}

        # 搜索页面中的图像
        images = soup.find_all("div", {'class', 'thumbinner'})
        for image in images:
            try:
                image_urls.append(HTTPS + image.find('a', ('class', 'image')).find('img')['src'])
            except:
                continue

        # 获取页面表格信息
        for table in info_tables:
            table = table.find('tbody')
            for tr in table.children:
                # print("tr 1 :{}".format(tr))
                # 装备名称
                if flag_name is False:
                    flag_name = True
                    if tr.find('span'):
                        weapon_name = tr.find('span').text
                    else:
                        for th in tr.find_all('th'):
                            print(th)
                            weapon_name = th.contents[0].replace("/", ',').replace("\xa0", '').strip("\n")
                            continue
                    if weapon_name == '':
                        weapon_name = info_title
                    attributes[weapon_name] = {}
                    continue
                try:
                    """
                    获取表格信息
                    """
                    # 表格内的图像
                    if weapon_name == '':
                        weapon_name = info_title
                    if tr.find('a', ('class', 'image')) is not None:
                        # flag_image = False
                        img = tr.find('a', ('class', 'image')).find('img')  # 图像标签
                        image_urls.append(HTTPS + img['src'])
                        attributes[weapon_name]['img_url'] = image_urls
                        continue

                    # 表格内容
                    if tr.find('td') and tr.find('th'):
                        th = tr.find('th').text.replace("/", ',').replace("\xa0", '').strip("\n").strip("•").strip(" ")  # 属性名
                        if th == "舰种": # 统一标签名
                            th = "类型"
                        td = tr.find('td').text.replace("\xa0", '').replace("\n", ";")  # 属性值
                        attributes[weapon_name][th] = re.sub(reg, '', td)
                except:
                    """
                    出现异常，输出当前标签
                    """
                    # print(tr)
                    pass

        item = {"keyword": self.keyword, "source": Source, "title": Title, "url": Website, "date": Date, "content": Content, "attributes": attributes}
        item['rowkey_id'] = md5(str(time.time()).encode()).hexdigest()

        yield item



