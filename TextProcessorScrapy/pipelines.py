# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


import datetime
# useful for handling different item types with a single interface
import json
import os
# from data_scrapy.settings import REDIS_HOST, REDIS_PORT, REDIS_PARAMS, PROXIES_UNCHECKED_LIST, PROXIES_UNCHECKED_SET
from data_scrapy.utils import connect_db, create_table, delete_table, insert_to_db, mkdirs
import json
import logging
import os
from data_scrapy.twitter_utils import get_api, dataget

from scrapy.utils.project import get_project_settings

from data_scrapy.items import Tweet, User

logger = logging.getLogger(__name__)
SETTINGS = get_project_settings()

SavePath = '../result'


# server = redis.StrictRedis(host=REDIS_HOST, port=REDIS_PORT, password=REDIS_PARAMS['password'])


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
        # writer = csv.writer(file)
        # writer.writerow(["标签", "来源", "标题", "网址", "时间", "内容"])
    else:
        file = open(SaveFile, "a+", newline="", encoding="utf-8-sig")
    return file


class HsNasaPipeline:
    def __init__(self):
        self.tag = 'nasa'
        self.file = CreatePath(SavePath, self.tag)
        self.data = []
        self.count = 0
        self.connect = connect_db()
        self.cursor = self.connect.cursor()
        create_table(self.cursor, self.tag)

    def process_item(self, item, spider):
        """
        保存
        """
        insert_to_db(self.cursor, self.tag, item)
        detail = {"标签": item["keyword"], "来源": item["source"], "标题": item["title"], "网址": item["url"],
                  "时间": item["date"], "内容": item["content"]}
        detail_ = {"keyword": item["keyword"], "source": item["source"], "title": item["title"], "url": item["url"],
                   "date": item["date"], "content": item["content"]}
        self.data.append(detail_)
        self.count += 1
        for data in self.data:
            insert_to_db(self.cursor, self.tag, data)
        if self.count == 50:
            json.dump(self.data, self.file, indent=4, ensure_ascii=False)
            self.count = 0
            self.data = []
            self.file = CreatePath(SavePath)
        # return item

    def close_spider(self, spider):
        json.dump(self.data, self.file, indent=4, ensure_ascii=False)
        for data in self.data:
            insert_to_db(self.cursor, data)
        self.file.close()
        self.cursor.close()
        self.connect.close()


class TwitterPipeline:
    def __init__(self):
        self.tag = 'twitter'
        self.file = CreatePath(SavePath, self.tag)
        self.data = []
        self.count = 0
        self.connect = connect_db()
        self.cursor = self.connect.cursor()
        # delete_table(self.cursor, self.tag)
        create_table(self.cursor, self.tag)

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
        results = dataget(self.api, item['keyword'])
        for result in results:
            detail_ = {"keyword": result["keyword"], "source": result["source"], "title": result["title"],
                       "url": result["url"],
                       "date": result["date"], "content": result["content"]}
            detail = {"标签": result["keyword"], "来源": result["source"], "标题": result["title"], "网址": result["url"],
                      "时间": result["date"], "内容": result["content"]}
            self.data.append(detail_)
            self.count += 1
            if self.count == 50:
                json.dump(self.data, self.file, indent=4, ensure_ascii=False)
                for data in self.data:
                    insert_to_db(self.cursor, self.tag, data)
                self.count = 0
                self.data = []
        json.dump(self.data, self.file, indent=4, ensure_ascii=False)
        for data in self.data:
            insert_to_db(self.cursor, self.tag, data)
        return item

    def close_spider(self, spider):
        json.dump(self.data, self.file, indent=4, ensure_ascii=False)
        for data in self.data:
            insert_to_db(self.cursor, self.tag, data)
        self.file.close()
        self.cursor.close()
        self.connect.close()


class FacebookPipeline:
    def __init__(self):
        self.tag = 'facebook'
        self.file = CreatePath(SavePath, self.tag)
        self.data = []
        self.count = 0
        self.connect = connect_db()
        self.cursor = self.connect.cursor()
        # delete_table(self.cursor, self.tag)
        create_table(self.cursor, self.tag)

    def process_item(self, item, spider):
        results = dataget(self.api, item['keyword'])
        for result in results:
            detail_ = {"keyword": result["keyword"], "source": result["source"], "title": result["title"],
                       "url": result["url"],
                       "date": result["date"], "content": result["content"]}
            detail = {"标签": result["keyword"], "来源": result["source"], "标题": result["title"], "网址": result["url"],
                      "时间": result["date"], "内容": result["content"]}
            self.data.append(detail_)
            self.count += 1
            if self.count == 50:
                json.dump(self.data, self.file, indent=4, ensure_ascii=False)
                for data in self.data:
                    insert_to_db(self.cursor, self.tag, data)
                self.count = 0
                self.data = []
        json.dump(self.data, self.file, indent=4, ensure_ascii=False)
        for data in self.data:
            insert_to_db(self.cursor, self.tag, data)
        return item

    def close_spider(self, spider):
        json.dump(self.data, self.file, indent=4, ensure_ascii=False)
        for data in self.data:
            insert_to_db(self.cursor, self.tag, data)
        self.file.close()
        self.cursor.close()
        self.connect.close()


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


class SaveToFilePipeline(object):
    ''' pipeline that save data to disk '''

    def __init__(self):
        self.saveTweetPath = SETTINGS['SAVE_TWEET_PATH']
        self.saveUserPath = SETTINGS['SAVE_USER_PATH']
        mkdirs(self.saveTweetPath)  # ensure the path exists
        mkdirs(self.saveUserPath)

    def process_item(self, item, spider):
        if isinstance(item, Tweet):
            savePath = os.path.join(self.saveTweetPath, item['id_'])
            if os.path.isfile(savePath):
                pass  # simply skip existing items
                # logger.debug("skip tweet:%s"%item['id_'])
                ### or you can rewrite the file, if you don't want to skip:
                # self.save_to_file(item,savePath)
                # logger.debug("Update tweet:%s"%item['id_'])
            else:
                self.save_to_file(item, savePath)
                logger.debug("Add tweet:%s" % item['id_'])

        elif isinstance(item, User):
            savePath = os.path.join(self.saveUserPath, item['id_'])
            if os.path.isfile(savePath):
                pass  # simply skip existing items
                # logger.debug("skip user:%s"%item['id_'])
                ### or you can rewrite the file, if you don't want to skip:
                # self.save_to_file(item,savePath)
                # logger.debug("Update user:%s"%item['id_'])
            else:
                self.save_to_file(item, savePath)
                logger.debug("Add user:%s" % item['id_'])

        else:
            logger.info("Item type is not recognized! type = %s" % type(item))

    def save_to_file(self, item, fname):
        """ input:
                item - a dict like object
                fname - where to save
        """
        with open(fname, 'w', encoding='utf-8') as f:
            json.dump(dict(item), f, ensure_ascii=False)
