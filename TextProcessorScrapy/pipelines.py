# coding:utf-8
# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


import csv
import datetime
from hashlib import md5
import json
import logging
import os
# useful for handling different item types with a single interface
import time
import scrapy
from scrapy.pipelines.images import ImagesPipeline
from TextProcessorScrapy.utils.twitter_utils import get_api, dataget

SavePath = 'result/'
imgSavePath = 'result/Images'
csv_header = ['rowkey_id', 'keyword', 'source', 'title', 'url', 'date', 'content', 'attributes', 'all_content']


def CreatePath(path, crawl_id, tag, keyword, keyword_type):
    """
    判断保存路径是否存在，如果不存在则创建
    :param crawl_id: 爬虫id
    :param keyword_type:关键词类别
    :param keyword: 关键词
    :param tag:网站标识
    :param path:保存主目录
    :return:
    
    """
    path = path + crawl_id + '/{}/'.format(tag) + '/'
    if os.path.exists(path) is False:
        os.makedirs(path)
    SaveFile_json = path + '{}_{}_{}_'.format(tag, keyword, keyword_type) + datetime.datetime.now().strftime(
        "%Y%m%d%H%M%S%f") + '.json'
    SaveFile_csv = path + '{}_{}_{}_'.format(tag, keyword, keyword_type) + datetime.datetime.now().strftime(
        "%Y%m%d%H%M%S%f") + '.csv'
    file_json = open(SaveFile_json, "a+", newline="", encoding="utf-8-sig")
    file_csv = open(SaveFile_csv, "a+", newline="", encoding="utf-8-sig")
    csv_writer = csv.DictWriter(file_csv, csv_header)
    csv_writer.writeheader()
    return file_json, csv_writer, file_csv


def create_count_file(path, id, tag, keyword):
    index_path = path + '/count'
    if os.path.exists(index_path) is False:
        os.makedirs(index_path)
    count_file = index_path + '/{}_{}_{}_{}'.format(str(datetime.datetime.now().strftime("%Y%m%d%H%M%S")), id, tag,
                                                    keyword)
    try:
        return open(count_file, 'w', encoding='utf-8')
    except:
        time.sleep(1)
        return open(count_file, 'w', encoding='utf-8')


def create_url_file(path, keyword):
    index_path = path + '/url'
    if os.path.exists(index_path) is False:
        os.makedirs(index_path)
    count_file = index_path + '/{}_{}'.format(str(datetime.datetime.now().strftime("%Y%m%d%H%M%S")), keyword)
    try:
        return open(count_file, 'w', encoding='utf-8')
    except:
        time.sleep(1)
        return open(count_file, 'w', encoding='utf-8')


class HsNasaPipeline:
    def __init__(self):
        self.tag = 'nasa'
        self.data = []
        self.count = 0
        self.first = True

    def process_item(self, item, spider):
        """
        保存
        """
        if self.first:
            self.count_file = create_count_file(SavePath, spider.crawl_id, self.tag, spider.keyword)
            self.first = False
        detail_ = {"keyword": item["keyword"], "source": item["source"], "title": item["title"], "url": item["url"],
                   "date": item["date"], "content": item["content"]}
        self.data.append(detail_)
        self.count += 1
        # return item

    def close_spider(self, spider):
        if self.first is False and self.count > 0:
            self.file, self.file_csv, self.csv = CreatePath(SavePath, spider.crawl_id, self.tag, spider.keyword, spider.keyword_type)
            json.dump(self.data, self.file, indent=4, ensure_ascii=False)
            for item in self.data:
                if 'attributes' in item:
                    item['attributes'] = str(item['attributes'])
                item['all_content'] = '，'.join([x for x in item.values()])
                item['rowkey_id'] = md5(str(time.time()).encode()).hexdigest()
                self.file_csv.writerow(item)
            self.count_file.write(str(self.count))
            self.count_file.close()
            self.file.close()
            self.csv.close()

class TwitterPipeline:
    def __init__(self):
        self.tag = 'twitter'
        self.first = True
        self.data = []
        self.count = 0

        # test
        consumer_key = "FSiynF6liTsZ5vnNzdKRH4vlA"
        consumer_secret = "kDStWMyzjR7op26DhuWeTB4Q6efRSvrjC4a4zX3PWXiTjN7Lt4"
        access_key = "1372722615991738368-5OFRZJkDCHLujlW0GUpqAdXHiKETXF"
        access_secret = "FfF6dNyZcOBwx1IZGhWQHQWMb1iQ2XLa73Wvk8IJlVfzI"

        self.api = get_api(consumer_key, consumer_secret, access_key, access_secret)

    def process_item(self, item, spider):
        """
        保存
        """
        if self.first:
            self.count_file = create_count_file(SavePath, spider.crawl_id, self.tag, spider.keyword)
            self.first = False
        results = dataget(self.api, item['keyword'])
        for result in results:
            detail_ = {"keyword": result["keyword"], "source": result["source"], "title": result["title"],
                       "url": result["url"],
                       "date": result["date"], "content": result["content"], }
            if len(detail_['content'].replace(' ', '').replace("\n", '')) <= 20 or detail_['content'] == '':
                continue
            self.data.append(detail_)
            self.count += 1

        # return item

    def close_spider(self, spider):
        if self.first is False and self.count > 0:
            try:
                sql_ = "INSERT INTO  hsold.text_crawl_http_interact  VALUES (?, hs.sequence_get_id.NEXTVAL,?,?)"
                pram_ = [md5(str(time.time()).encode()).hexdigest(), spider.crawl_id, str(self.count)]
                spider.database.execute_sql(sql_, pram_)
            except:
                logging.error("http interact insert failure")
            logging.info("{} ten seconds http interact :{}".format(spider.crawl_id, str(self.count)))
            self.file, self.file_csv, self.csv = CreatePath(SavePath, spider.crawl_id, self.tag, spider.keyword, spider.keyword_type)
            json.dump(self.data, self.file, indent=4, ensure_ascii=False)
            for item in self.data:
                if 'attributes' in item:
                    item['attributes'] = str(item['attributes'])
                item['all_content'] = '，'.join([x for x in item.values()])
                item['rowkey_id'] = md5(str(time.time()).encode()).hexdigest()
                self.file_csv.writerow(item)
            
            self.count_file.write(str(self.count))
            self.count_file.close()
            self.csv.close()
            self.file.close()


class FacebookPipeline:
    def __init__(self):
        self.tag = 'facebook'
        self.first = True
        self.data = []
        self.count = 0

    def process_item(self, item, spider):
        if self.first:
            self.count_file = create_count_file(SavePath, spider.crawl_id, self.tag, spider.keyword)
            self.first = False

        detail_ = {"keyword": item["keyword"], "source": item["source"], "title": item["title"], "url": item["url"],
                   "date": item["date"], "content": item["content"]}
        self.data.append(detail_)
        self.count += 1
        return item

    def close_spider(self, spider):
        if self.first is False and self.count > 0:
            self.file, self.file_csv, self.csv = CreatePath(SavePath, spider.crawl_id, self.tag, spider.keyword, spider.keyword_type)
            json.dump(self.data, self.file, indent=4, ensure_ascii=False)
            for item in self.data:
                if 'attributes' in item:
                    item['attributes'] = str(item['attributes'])
                item['all_content'] = '，'.join([x for x in item.values()])
                item['rowkey_id'] = md5(str(time.time()).encode()).hexdigest()
                self.file_csv.writerow(item)
            self.count_file.write(str(self.count))
            self.count_file.close()
            self.csv.close()
            self.file.close()
        # if hasattr(spider, 'driver'):
        try:
            spider.driver.quit()
        except Exception:
            pass

class AiaaPipeline:
    def __init__(self):
        self.tag = 'aiaa'
        self.first = True
        self.data = []
        self.count = 0

    def process_item(self, item, spider):
        if self.first:
            self.count_file = create_count_file(SavePath, spider.crawl_id, self.tag, spider.keyword)
            self.first = False
        detail_ = {"keyword": item["keyword"], "source": item["source"], "title": item["title"], "url": item["url"],
                   "date": item["date"], "content": item["content"]}
        self.data.append(detail_)
        self.count += 1
        return item

    def close_spider(self, spider):
        if self.first is False and self.count > 0:
            self.file, self.file_csv, self.csv = CreatePath(SavePath, spider.crawl_id, self.tag, spider.keyword, spider.keyword_type)
            json.dump(self.data, self.file, indent=4, ensure_ascii=False)
            for item in self.data:
                if 'attributes' in item:
                    item['attributes'] = str(item['attributes'])
                item['all_content'] = '，'.join([x for x in item.values()])
                item['rowkey_id'] = md5(str(time.time()).encode()).hexdigest()
                self.file_csv.writerow(item)
            self.count_file.write(str(self.count))
            self.count_file.close()
            self.csv.close()
            self.file.close()


class WikiPipeline:
    def __init__(self):
        self.tag = 'wiki'
        self.first = True
        self.data = []
        self.count = 0

    def process_item(self, item, spider):
        """
        保存
        """
        if self.first:
            self.count_file = create_count_file(SavePath, spider.crawl_id, self.tag, spider.keyword)
            self.first = False

        detail_ = {"keyword": item["keyword"], "source": item["source"], "title": item["title"], "url": item["url"],
                   "date": item["date"], "content": item["content"], "attributes": item["attributes"]}
        self.data.append(detail_)
        self.count += 1
        return item

    def close_spider(self, spider):
        if self.first is False and self.count > 0:
            self.file, self.file_csv, self.csv = CreatePath(SavePath, spider.crawl_id, self.tag, spider.keyword, spider.keyword_type)
            json.dump(self.data, self.file, indent=4, ensure_ascii=False)
            for item in self.data:
                if 'attributes' in item:
                    item['attributes'] = str(item['attributes'])
                item['all_content'] = '，'.join([x for x in item.values()])
                item['rowkey_id'] = md5(str(time.time()).encode()).hexdigest()
                self.file_csv.writerow(item)
            self.count_file.write(str(self.count))
            self.count_file.close()
            self.csv.close()
            self.file.close()


class BaiduPipeline(object):
    def __init__(self):
        self.tag = 'baidu'
        self.first = True
        self.data = []
        self.count = 0

    def open_spider(self, spider):
        pass
        # print("BaiduPipeline.open_spider")
    
    def process_item(self, item, spider):
        """
        保存
        """
        if self.first:
            self.count_file = create_count_file(SavePath, spider.crawl_id, self.tag, spider.keyword)
            self.first = False
        if item is not None and len(item['content']) > 10:
            detail_ = {"keyword": item["keyword"], "source": item["source"], "title": item["title"], "url": item["url"],
                    "date": item["date"], "content": item["content"], "attributes": item["attributes"]}
            self.data.append(detail_)
        return item

    def close_spider(self, spider):
        if self.first is False and self.count > 0:
            self.file, self.file_csv, self.csv = CreatePath(SavePath, spider.crawl_id, self.tag, spider.keyword, spider.keyword_type)
            json.dump(self.data, self.file, indent=4, ensure_ascii=False)
            for item in self.data:
                if 'attributes' in item:
                    item['attributes'] = str(item['attributes'])
                item['all_content'] = '，'.join([x for x in item.values()])
                item['rowkey_id'] = md5(str(time.time()).encode()).hexdigest()
                self.file_csv.writerow(item)
            self.count_file.write(str(len(self.data)))
            self.count_file.close()
            self.csv.close()
            self.file.close()
        # if hasattr(spider, 'driver'):
        try:
            spider.driver.quit()
        except Exception as e:
            print(e)

class JanesPipeline:
    def __init__(self):
        self.tag = 'janes'
        self.first = True
        self.data = []
        self.count = 0

    def process_item(self, item, spider):
        """
        保存
        """
        if self.first:
            self.count_file = create_count_file(SavePath, spider.crawl_id, self.tag, item["keyword"])
            self.first = False

        detail_ = {"keyword": item["keyword"], "source": item["source"], "title": item["title"], "url": item["url"],
                   "date": item["date"], "content": item["content"]}
        self.data.append(detail_)
        return item

    def close_spider(self, spider):
        if self.first is False and self.count > 0:
            self.file, self.file_csv, self.csv = CreatePath(SavePath, spider.crawl_id, self.tag, spider.keyword, spider.keyword_type)
            json.dump(self.data, self.file, indent=4, ensure_ascii=False)
            for item in self.data:
                if 'attributes' in item:
                    item['attributes'] = str(item['attributes'])
                item['all_content'] = '，'.join([x for x in item.values()])
                item['rowkey_id'] = md5(str(time.time()).encode()).hexdigest()
                self.file_csv.writerow(item)
            self.count_file.write(str(self.count))
            self.count_file.close()
            self.csv.close()
            self.file.close()


class TiexuePipeline:
    def __init__(self):
        self.tag = 'tiexue'
        self.first = True
        self.data = []
        self.count = 0

    def process_item(self, item, spider):
        """
        保存
        """
        if self.first:
            self.count_file = create_count_file(SavePath, spider.crawl_id, self.tag, item["keyword"])
            self.first = False

        detail_ = {"keyword": item["keyword"], "source": item["source"], "title": item["title"], "url": item["url"],
                   "date": item["date"], "content": item["content"]}
        self.data.append(detail_)
        return item

    def close_spider(self, spider):
        if self.first is False and self.count > 0:
            self.file, self.file_csv, self.csv = CreatePath(SavePath, spider.crawl_id, self.tag, spider.keyword, spider.keyword_type)
            json.dump(self.data, self.file, indent=4, ensure_ascii=False)
            for item in self.data:
                if 'attributes' in item:
                    item['attributes'] = str(item['attributes'])
                item['all_content'] = '，'.join([x for x in item.values()])
                item['rowkey_id'] = md5(str(time.time()).encode()).hexdigest()
                self.file_csv.writerow(item)
            self.count_file.write(str(self.count))
            self.count_file.close()
            self.csv.close()
            self.file.close()


class ImagePipeline(ImagesPipeline):
    def get_media_requests(self, item, info):
        if 'attributes' in item and item['attributes'] is not None:
            if 'img_url' in item['attributes']:
                if type(item['attributes']['img_url']) == type({}):
                    for url in item['attributes']['img_url'].values():
                        yield scrapy.Request(url)
                elif type(item['attributes']['img_url']) == type([]):
                    for url in item['attributes']['img_url']:
                        yield scrapy.Request(url)

    # 保存图片时重命名
    def item_completed(self, results, item, info):
        if 'crawl_id' in item:
            index_path = SavePath + item['crawl_id'] + '/url'
            img_save_path = SavePath + item['crawl_id'] + '/images/'
            if os.path.exists(index_path) is False:
                os.makedirs((index_path))
            if os.path.exists(img_save_path) is False:
                os.makedirs((img_save_path))
            # 列表推导式，获取图片的保存路径
            image_path = [x["path"] for ok, x in results if ok]
            image_url = [x["url"] for ok, x in results if ok]
            count = 0
            # 重命名，由于都是jpg文件，所以直接拼上了
            url_file_name = index_path + '/' + item["keyword"]
            if os.path.isfile(url_file_name):
                with open(url_file_name, 'r') as f:
                    img_to_url = json.load(f)
            else:
                img_to_url = {}
            base_img_name = img_save_path + '{}_{}'.format(item['keyword'], item['label_type'])
            for img, url in zip(image_path, image_url):
                imgname = base_img_name + '_{}.jpg'.format(str(count))
                while os.path.isfile(imgname):
                    count += 1
                    imgname = base_img_name + '_{}.jpg'.format(str(count))
                os.rename(imgSavePath + '/' + img, imgname)
                img_to_url['{}_{}_{}'.format(item['keyword'], item['label_type'], str(count)) + ".jpg"] = url
            with open(url_file_name, 'w+') as url_file:
                json.dump(img_to_url, url_file, indent=4, ensure_ascii=False)
            return item