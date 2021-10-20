# -*- coding: utf-8 -*-
import re
import time

import scrapy
from ..items import BaiduWikiItem

class WikiSpider(scrapy.Spider):
    name = 'wiki'
    custom_settings = {
        'ITEM_PIPELINES': {'TextProcessorScrapy.pipelines.WikiPipeline': 400},
    }
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.allowed_domains = ['en.wikipedia.org']
    # 获取每个关键词的wiki搜索初始网址（第一页）
        if 'crawl_id' in kwargs['crawl_id']:
            self.crawl_id = kwargs['crawl_id']
        if 'keyword' in kwargs:
            self.keyword = kwargs['keyword']
        if 'database' in kwargs:
            self.database = kwargs['database']
        url = "https://en.wikipedia.org/w/index.php?title=Special:Search&limit=20&offset=0&profile=default&search=" + self.keyword + "&ns0=1"
        self.start_urls.append(url)
        self.count = 0
        self.number = 0

    # 进入一级解析，starturl解析
    def parse(self, response):
        hreflist = []
        # self.keyword = self.keywords[self.number]
        self.number += 1
        # 只爬一条
        # web_node_list = response.xpath('//div[@id="content_left"]//div [@class="result c-container new-pmd"][1]//h3/a/@href').extract()
        # hreflist.append(web_node_list[0])

        # 获取关键字
        currenturl = response.request.url
        findsearchword = re.compile(r".*&search=(.*)&ns0=1")
        searchworditem = str(currenturl).replace("%20", " ")
        searchkeyword = re.findall(findsearchword, searchworditem)
        searchword = searchkeyword[0]

        # 获取下一页url
        next_page_list = response.xpath('//p [@class="mw-search-pager-bottom"]/a')
        next_page_url = ''
        for next_link in next_page_list:
            link_href_text = next_link.xpath('./text()').extract_first()
            if link_href_text == "next 20":
                next_href = next_link.xpath('./@href').extract_first()
                if next_href != None:
                    next_page_url = "https://en.wikipedia.org/" + next_href
                    break

        # 一页全爬,此时获取的是域名之后的那段网址
        web_node_list_href = response.xpath('//ul [@class="mw-search-results"]//li [@class="mw-search-result"]//div [@class="mw-search-result-heading"]/a/@href').extract()
        for i in range(len(web_node_list_href)):
            web_node_url = "https://en.wikipedia.org" + str(web_node_list_href[i])
            hreflist.append(web_node_url)

        # 进入二级解析，具体网址解析
        for href in hreflist:
            self.count += 1
            yield scrapy.Request(url=href, callback=self.new_parse)
            if self.count > 50:
                return
        # 迭代
        if next_page_url:
            yield scrapy.Request(url=next_page_url, callback=self.parse)


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
        tablename = response.xpath('//table[@class="infobox vcard"]/tbody/tr[1]/th [@colspan="2"]//text()').extract_first()
        attributes.setdefault("name", tablename)

        if tablename == None:
            attributes = None
        else:
            tableimg = response.xpath('//table[@class="infobox vcard"]/tbody/tr/td[@class="infobox-image"]//img/@src').extract_first()
            if tableimg != None:
                tableimg = "https:" + tableimg
            attributes.setdefault("img", tableimg)

            img_des = response.xpath('//table[@class="infobox vcard"]/tbody/tr/td[@class="infobox-image"]/div [@class="infobox-caption"]//text()').extract()
            temp_img_des = ""
            for i_d in img_des:
                temp_img_des = temp_img_des + i_d
            attributes.setdefault("img_des", temp_img_des)

            tr_list = response.xpath('//table[@class="infobox vcard"]/tbody/tr/th[contains(@class,"infobox-label")]')
            for tr in tr_list:
                # 获取属性名
                attribute_str = tr.xpath('.//text()').extract()

                # 拼接属性名
                temp_at = ""
                for a_s in attribute_str:
                    temp_at = temp_at + a_s
                temp_at = " ".join(temp_at.split())  ##解码问题gbk无法识别\xa0

                # 获取属性值
                # attribute_vul = tr.xpath('../td/a [not(@class="reference")]//text()').extract()
                attribute_vul = tr.xpath('./following-sibling::td//text()').extract()

                # 拼接属性值
                temp_vul = ""
                for a_v in attribute_vul:
                    temp_vul = temp_vul + a_v
                temp_vul = " ".join(temp_vul.split()).replace("\n", " ").replace("\'", "\"")

                # 正则替换参考提示字符
                pattern = re.compile(r'\[\d]')
                temp_vul = re.sub(pattern, " ", temp_vul)

                # 加入字典
                attributes.setdefault(temp_at, temp_vul)

        item = {"keyword": self.keyword, "source": Source, "title": Title, "url": Website, "date": Date, "content": Content, "attributes": attributes}

        yield item



