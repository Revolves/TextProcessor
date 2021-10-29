# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


import datetime
import json
import logging
import os
# useful for handling different item types with a single interface
import time

from TextProcessorScrapy.utils.twitter_utils import get_api, dataget

SavePath = 'result'


def CreatePath(path, tag):
    """
    判断保存路径是否存在，如果不存在则创建
    :param tag:
    :param path:
    :return:
    """
    path = path + '/{}/'.format(tag)
    if os.path.exists(path) is False:
        os.makedirs(path)
    SaveFile = path + '{}_'.format(tag) + datetime.datetime.now().strftime("%Y%m%d%H%M%S%f") + '.json'
    if os.path.exists(SaveFile) is False:
        file = open(SaveFile, "a+", newline="", encoding="utf-8-sig")
    else:
        file = open(SaveFile, "a+", newline="", encoding="utf-8-sig")
    return file

def create_count_file(path, tag, keyword):
    index_path = path + '/count'
    if os.path.exists(index_path) is False:
        os.makedirs((index_path))
    count_file = index_path + '/{}_{}_{}'.format(str(datetime.datetime.now().strftime("%Y%m%d%H%M%S")), tag, keyword)
    try:
        return open(count_file, 'w', encoding='utf-8')
    except:
        time.sleep(1)
        return open(count_file, 'w', encoding='utf-8')

class HsNasaPipeline:
    def __init__(self):
        self.tag = 'nasa'
        self.file = CreatePath(SavePath, self.tag)
        self.data = []
        self.count = 0
        self.first = True

    def process_item(self, item, spider):
        """
        保存
        """
        if self.first:
            self.count_file = create_count_file(SavePath, self.tag, item["keyword"])
            self.first = False
        detail_ = {"keyword": item["keyword"], "source": item["source"], "title": item["title"], "url": item["url"],
                   "date": item["date"], "content": item["content"]}
        self.data.append(detail_)
        self.count += 1
        # return item

    def close_spider(self, spider):
        json.dump(self.data, self.file, indent=4, ensure_ascii=False)
        if self.first is False:
            self.count_file.write(str(self.count))
            self.count_file.close()
        self.file.close()


class TwitterPipeline:
    def __init__(self):
        self.tag = 'twitter'
        self.file = CreatePath(SavePath, self.tag)
        self.first = True
        self.data = []
        self.count = 0

        # test
        consumer_key = "ioTGfhxK3Fylub82QJmLMB6mB"
        consumer_secret = "fg19r72exdPQfNa0HzBRNPzUrPKZI4YvU4FVDEmlWxkaVcFuKs"
        access_key = "1372722615991738368-bQ4nwuK0vKY95zAIoIAaeqP44DYg56"
        access_secret = "qjZnegLBlk7OvBKGrjFvfjRV6rPu0descZhLdtaILLVlH"

        self.api = get_api(consumer_key, consumer_secret, access_key, access_secret)

    def process_item(self, item, spider):
        """
        保存
        """
        if self.first:
            self.count_file = create_count_file(SavePath, self.tag, item["keyword"])
            self.first = False
        results = dataget(self.api, item['keyword'])
        for result in results:
            detail_ = {"keyword": result["keyword"], "source": result["source"], "title": result["title"],
                       "url": result["url"],
                       "date": result["date"], "content": result["content"]}
            if len(detail_['content'].replace(' ', '').replace("\n", '')) <= 20 or detail_['content'] == '':
                continue
            self.data.append(detail_)
            self.count += 1

        # return item

    def close_spider(self, spider):
        try:
            sql_ = "INSERT INTO  hs.text_crawl_http_interact  VALUES (?, ?, hs.sequence_get_id.NEXTVAL)"
            pram_ = [spider.crawl_id, str(self.count)]
            spider.database.execute_sql(sql_, pram_)
        except:
            logging.error("http interact insert failure")
        logging.info("{} ten seconds http interact :{}".format(spider.crawl_id, str(self.count)))
        json.dump(self.data, self.file, indent=4, ensure_ascii=False)
        if self.first is False:
            self.count_file.write(str(self.count))
            self.count_file.close()
        self.file.close()

class FacebookPipeline:
    def __init__(self):
        self.tag = 'facebook'
        self.file = CreatePath(SavePath, self.tag)
        self.first = True
        self.data = []
        self.count = 0

    def process_item(self, item, spider):
        if self.first:
            self.count_file = create_count_file(SavePath, self.tag, item["keyword"])
            self.first = False

        detail_ = {"keyword": item["keyword"], "source": item["source"], "title": item["title"], "url": item["url"],
                   "date": item["date"], "content": item["content"]}
        self.data.append(detail_)
        self.count += 1
        return item

    def close_spider(self, spider):
        json.dump(self.data, self.file, indent=4, ensure_ascii=False)
        if self.first is False:
            self.count_file.write(str(self.count))
            self.count_file.close()
        self.file.close()


class ViedoPipeline:
    def __init__(self):
        pass

    def process_item(self, item, spider):
        name = item['title']


class ProxyPoolPipeline(object):
    pass
    # # 将可用的IP代理添加到代理池队列
    # def process_item(self, item, spider):
    #     if not self._is_existed(item):
    #         server.rpush(PROXIES_UNCHECKED_LIST, json.dumps(dict(item), ensure_ascii=False))
    #
    # # 检查IP代理是否已经存在
    # def _is_existed(self, item):
    #     added = server.sadd(PROXIES_UNCHECKED_SET, item._get_url())
    #     return added == 0

class AiaaPipeline:
    def __init__(self):
        self.tag = 'aiaa'
        self.file = CreatePath(SavePath, self.tag)
        self.first = True
        self.data = []
        self.count = 0

    def process_item(self, item, spider):
        if self.first:
            self.count_file = create_count_file(SavePath, self.tag, item["keyword"])
            self.first = False
        detail_ = {"keyword": item["keyword"], "source": item["source"], "title": item["title"], "url": item["url"],
                   "date": item["date"], "content": item["content"]}
        self.data.append(detail_)
        self.count += 1
        return item

    def close_spider(self, spider):
        json.dump(self.data, self.file, indent=4, ensure_ascii=False)
        if self.first is False:
            self.count_file.write(str(self.count))
            self.count_file.close()
        self.file.close()

class WikiPipeline:
    def __init__(self):
        self.tag = 'wiki'
        self.file = CreatePath(SavePath, self.tag)
        self.first = True
        self.data = []
        self.count = 0

    def process_item(self, item, spider):
        """
        保存
        """
        if self.first:
            self.count_file = create_count_file(SavePath, self.tag, item["keyword"])
            self.first = False
        detail_ = {"keyword": item["keyword"], "source": item["source"], "title": item["title"], "url": item["url"],
                   "date": item["date"], "content": item["content"], "attributes": item["attributes"]}
        self.data.append(detail_)
        self.count += 1
        return item

    def close_spider(self, spider):
        json.dump(self.data, self.file, indent=4, ensure_ascii=False)
        if self.first is False:
            self.count_file.write(str(self.count))
            self.count_file.close()
        self.file.close()

class BaiduPipeline:
    def __init__(self):
        self.tag = 'baidu'
        self.file = CreatePath(SavePath, self.tag)
        self.first = True
        self.data = []
        self.count = 0

    def process_item(self, item, spider):
        """
        保存
        """
        if self.first:
            self.count_file = create_count_file(SavePath, self.tag, item["keyword"])
            self.first = False
        detail_ = {"keyword": item["keyword"], "source": item["source"], "title": item["title"], "url": item["url"],
                   "date": item["date"], "content": item["content"], "attributes": item["attributes"]}
        self.data.append(detail_)
        return item

    def close_spider(self, spider):
        json.dump(self.data, self.file, indent=4, ensure_ascii=False)
        self.file.close()
        if self.first is False:
            self.count_file.write(str(self.count))
            self.count_file.close()

class JanesPipeline:
    def __init__(self):
        self.tag = 'janes'
        self.file = CreatePath(SavePath, self.tag)
        self.first = True
        self.data = []
        self.count = 0

    def process_item(self, item, spider):
        """
        保存
        """
        if self.first:
            self.count_file = create_count_file(SavePath, self.tag, item["keyword"])
            self.first = False
        detail_ = {"keyword": item["keyword"], "source": item["source"], "title": item["title"], "url": item["url"],
                   "date": item["date"], "content": item["content"]}
        self.data.append(detail_)
        return item

    def close_spider(self, spider):
        json.dump(self.data, self.file, indent=4, ensure_ascii=False)
        if self.first is False:
            self.count_file.write(str(self.count))
            self.count_file.close()
        self.file.close()

class TiexuePipeline:
    def __init__(self):
        self.tag = 'tiexue'
        self.file = CreatePath(SavePath, self.tag)
        self.first = True
        self.data = []
        self.count = 0

    def process_item(self, item, spider):
        """
        保存
        """
        if self.first:
            self.count_file = create_count_file(SavePath, self.tag, item["keyword"])
            self.first = False
        detail_ = {"keyword": item["keyword"], "source": item["source"], "title": item["title"], "url": item["url"],
                   "date": item["date"], "content": item["content"]}
        self.data.append(detail_)
        # return item

    def close_spider(self, spider):
        json.dump(self.data, self.file, indent=4, ensure_ascii=False)
        if self.first is False:
            self.count_file.write(str(self.count))
            self.count_file.close()
        self.file.close()