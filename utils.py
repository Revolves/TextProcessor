import ctypes
import datetime
import json
import logging
from multiprocessing.dummy import Process
import os
import shutil
import time

from scrapy.crawler import CrawlerRunner, Crawler
from scrapy.utils.log import configure_logging
from scrapy.utils.project import get_project_settings
from twisted.internet import reactor, error
from hashlib import md5
from crochet import setup
from Transwarp import Transwarp
from DataCleaning import DataCleaning
from UpdateShowTable import updateTextCrawlStatus, updateShowTextStatus

logging.basicConfig(level=logging.DEBUG,
                    format='[%(asctime)-15s] [%(levelname)8s] [%(name)10s ] - %(message)s (%(filename)s:%(lineno)s)',
                    datefmt='%Y-%m-%d %T'
                    )
logger = logging.getLogger(__name__)

WEB_MAP = {"baidu": 'baidu',
           "wiki": "wiki",
           "NASA": "nasa",
           "AIAA": "aiaa",
           "铁血论坛": "tiexue",
           'twitter': 'twitter',
           "facebook": "facebook",
           "jane's": "janes"
           }
NUM = 0


def rowkey_id_gen():
    return md5(str(time.time()).encode()).hexdigest()


def is_chinese(string):
    """
    检查整个字符串是否包含中文
    :param string: 需要检查的字符串
    :return: bool
    """
    for ch in string:
        if u'\u4e00' <= ch <= u'\u9fff':
            return True

    return False


def get_count(keyword):
    """
    获取本轮爬取数量信息

    :return: 爬取数量(str类型）
    """
    count = int(0)
    count_path = 'result/count/'
    for file in os.listdir(count_path):
        if file.endswith(keyword):
            with open(count_path + file, 'r') as f:
                r = f.readline()
                if r != '':
                    c = int(r)
                    count = count + c
                f.close()
            os.remove(count_path + file)
    return str(count)


def crawl_file_list(root):
    """
    获取本次爬取文本文件路径列表

    :param root: 爬取文件的保存主目录
    :return: 文件列表
    """
    # 进行数据清洗
    # DC = DataCleaning(root)
    # DC.data_clean()

    file_list = {}
    for root, dirs, files in os.walk(root):
        for file in files:
            if file.endswith(".json") or file.endswith(".pdf") or file.endswith(".csv"):  # 过滤得到json文件
                file_path = os.path.join(os.path.abspath("."), os.path.join(root, file))
                file_size = os.stat(file_path).st_size  # 文件大小
                if file_size <= 10:
                    continue
                # print("file_path:{}, file_size:{}".format(file_path, file_size))
                file_size = round(file_size / 1024, 2)
                file_list[file_path] = str(file_size) + 'KB'
    print(file_list)
    return file_list
    pass


def crawl_img_list(root):
    """
    获取本次爬取图片文件路径列表

    :param root: 爬取文件的保存主目录
    :return: 文件列表
    """
    file_list = {}
    for root, dirs, files in os.walk(root):
        for file in files:
            if file.endswith(".jpg") or file.endswith(".png"):  # 过滤得到json文件
                file_path = os.path.join(os.path.abspath("."), os.path.join(root, file))
                file_size = os.stat(file_path).st_size  # 文件大小
                if file_size <= 10:
                    continue
                # print("file_path:{}, file_size:{}".format(file_path, file_size))
                file_size = round(file_size / 1024, 2)
                file_list[file_path] = str(file_size) + 'KB'
    return file_list
    pass


def upload_crawl_img_new(file_index, image_index, data_id, connect):
    """
    上传本次爬取保存图片

    :param file_index: url文件根目录
    :param image_index: img文件根目录
    :param data_id: data_id
    :param connect: Transwarp连接（包括hdfs和Inceptor）
    :return: 上传状态
    """
    try:
        img_file_list = os.listdir(file_index)
        logger.info("文件列表：")
        logger.info(img_file_list)
        for img_file in img_file_list:
            with open(file_index + '/' + img_file) as f:
                data_list = json.load(f)
            os.remove(file_index + '/' + img_file)
            for file in data_list.keys():
                file_path = image_index + '/' + file
                file_size = os.stat(file_path).st_size  # 文件大小
                file_size = round(file_size / 1024, 2)
                size_ = str(file_size) + 'KB'
                logger.info("上传文件：{}".format(file_path))
                # print(file_path.replace("\\","\\\\"))
                connect.upload_file(file_path, "/imageSearch")
                os.remove(file_path)
                sql_ = "INSERT INTO hsold.metadata_imagesearch VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ? ,? ,?, ?, ?)"
                rowkey_id = rowkey_id_gen()
                date_ = str(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
                name_ = file.split("/")[-1]
                format_ = file.split('.')[-1]
                path_ = "/hs/imageSearch"
                url = data_list[file]
                label_ = name_.split('_')[0]
                label_type = name_.split('_')[1]
                neo4j_id = ''
                pram_ = [rowkey_id, data_id, name_, size_, format_, path_, url, date_, label_, label_type, '', '', '',
                         neo4j_id]
                connect.execute_sql(sql_, pram_)
        return "upload crawl file success"
    except Exception as e:
        print(e)
        logger.exception(e)
        return "upload crawl file failure"
    pass


def upload_crawl_file(path_list, data_id, connect):
    """
    上传本次爬取保存文件

    :param path_list: 爬取文件路径列表
    :param connect: Transwarp连接（包括hdfs和Inceptor）
    :return: 上传状态
    """

    try:
        for file in path_list:
            size_ = path_list[file]
            logger.info("上传文件：{}".format(file))
            format_ = file.split('.')[-1]
            path_ = ''
            if format_ == 'csv':
                path_ = "/hs/text_crawl_csv/"
                connect.upload_file(file, "/text_crawl_csv/")
            elif format_ == 'json':
                path_ = "/hs/text_crawl_file/"
                connect.upload_file(file, "/text_crawl_file/")
            elif format_ == 'pdf':
                path_ = "/hs/text_crawl_pdf/"
                connect.upload_file(file, "/text_crawl_pdf/")
            # os.remove(file)
            sql_ = "INSERT INTO hsold.metadata_text_crawl VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
            rowkey_id = rowkey_id_gen()
            date_ = str(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
            name_ = file.split("\\")[-1]
            # if len(name_.split('_')) < 2:
            label_ = ''
            label_type_ = ''
            # else:
            #     label_ = name_.split('_')[1]
            #     label_type_ = name_.split('_')[2]
            neo4j_id = ''
            pram_ = [rowkey_id, data_id, name_, size_, format_, path_, date_, label_, label_type_, neo4j_id]
            connect.execute_sql(sql_, pram_)
        return "upload crawl file success"
    except Exception as e:
        # print(e)
        logger.exception(e)
        return "upload crawl file failure"
    pass


def getCrawlStatus(crawl_id):
    """
    更新hsold.show_text_status表 和 hsold.text_crawl_status

    扫描文件夹是否有count变化,读取count目录下文件获取当前保存关键词爬取内容数据\\
    count目录命名规则 {时间戳}_{crawl_id}_{site_name}_{keyword} \\
    从关键词和网络两个角度保存爬取数量

    return:
        keywords_count_list: 按关键字统计爬取数量
        sites_count_list： 按网站统计爬取数量

    """
    count_path = (r'./result/{}/count/'.format(crawl_id))
    if os.path.isdir(count_path) is False:
        return {}, {}
    last_site_count_list = {'aiaa': [], 'baidu': [], 'wiki': [], 'tiexue': [], 'twitter': [], 'facebook': [],
                            'nasa': [], 'janes': []}
    site_file_list = {}  # 按网站分类文件
    crawl_id_list = []
    for file in os.listdir(count_path):
        if file.split('_')[3] in site_file_list:
            site_file_list[file.split('_')[3]].append(file)
        else:
            site_file_list[file.split('_')[3]] = [file]
    keywords_count_list = {}  # 按关键词分类获取数量
    sites_count_list = {}  # 按网站分获取数量
    for site in site_file_list:
        countAdd = 0
        # 遍历文件列表
        for file in site_file_list[site]:
            # 跳过空文件
            if crawl_id not in keywords_count_list:
                keywords_count_list[crawl_id] = {}
            if os.stat(count_path + file).st_size > 0:
                # 获取文件内容
                with open(count_path + file, 'r') as f:
                    count = int(f.read())
                    if site in sites_count_list:
                        sites_count_list[site] += count
                    else:
                        sites_count_list[site] = count
                    # 按关键词记录数量
                    if file.split('_')[-1] in keywords_count_list[crawl_id]:
                        keywords_count_list[crawl_id][file.split('_')[-1]] += count
                    else:
                        keywords_count_list[crawl_id][file.split('_')[-1]] = count
            os.remove(count_path + file)

    return keywords_count_list, sites_count_list


def stop(*args, **kwargs):
    if reactor.running:
        try:
            reactor.stop()
        except error.ReactorNotRunning:
            pass


def crawling(crawl_id, data_id, connect, status, keywords=None):
    """
    插入爬虫运行状态
    :param crawl_id: 爬虫id
    :param data_id: 数据id
    :param connect:数据库连接
    :param status:爬取状态
    :param 关键词:关键词
    
    :return :
    """
    try:
        result = connect.execute_sql(
            'SELECT * FROM hsold.text_crawl_finish WHERE crawl_id = \"{}\" and data_id = \"{}\"'.format(crawl_id,
                                                                                                        data_id), [])
        if result is not None and len(result) > 0:
            connect.execute_sql(
                'UPDATE hsold.text_crawl_finish SET status = \"{}\" WHERE crawl_id = \"{}\" and data_id = \"{}\"'.format(
                    status, crawl_id, data_id), [])
        else:
            pram_ = [rowkey_id_gen(), data_id, crawl_id, status, keywords]
            connect.execute_sql(
                'INSERT INTO hsold.text_crawl_finish (rowkey_id, data_id, crawl_id, status, keywords) VALUES (?, ?, ?, ?, ?)',
                pram_)
    except Exception as e:
        print(e)


def handle_info(info):
    """
    处理爬虫参数
    :param :
    
    :return :
    """
    if info is None:
        info = {"sites": ['baidu'],
                "keywords": ['earth fortification', '马公机场', '澎湖机场', '西屿雷达站', '花莲机场', '直升机', '装甲防护车', '装甲扫雷车', '装甲运输车',
                             '装甲救护车', '装甲侦察车'], "crawl_id": "test", "data_id": "test"}
    else:
        if info['status'] == '2':
            # info = control_info(info_url)  # 控制信息
            keywords = []
            f = open('file/keys.txt', 'r', encoding='gbk')
            _f = f.readline()
            cnt = 1
            while _f:
                if cnt == 250:
                    break
                keywords.append(_f.replace('\n', ''))
                _f = f.readline()
                cnt += 1
            # if info is None:
            #  info = {"sites": ['nasa', 'wiki', 'baidu', 'tiexue', 'aiaa', 'janes', 'twitter'],
            #          "keywords": ['earth fortification', '马公机场', '澎湖机场', '西屿雷达站', '花莲机场', '直升机', '装甲防护车', '装甲扫雷车', '装甲运输车', '装甲救护车', '装甲侦察车'], "crawl_id": "crawler1"}
            info["keywords"] = keywords
            info['sites'] = ['nasa', 'wiki', 'baidu', 'tiexue', 'aiaa', 'janes', 'twitter', 'facebook']
            info['data_id'] = 'test_http_interact'
        elif info['status'] == '1':
            if type(info['sites']) == type(''):
                info['sites'] = info['sites'].split(',')
            if type(info['keywords']) == type(''):
                info['keywords'] = info['keywords'].replace('\t', '').split(',')
    return info


def insert_http_interact(crawl_id, connect):
    """
    插入交互量
    :param :
    
    :return :
    """
    pass


def post_processing(connect, crawl_id, data_id, status):
    """
    爬取完成后的数据处理（删除本次爬取交互量）
    :param connect:数据库连接
    :param crawl_id:爬虫id
    :param status:爬取状态
    
    
    :return :
    """
    # 删除本次http交互量
    sql_ = "DELETE FROM hsold.text_crawl_http_interact WHERE crawl_id = ?"
    pram_ = [crawl_id]
    connect.execute_sql(sql_, pram_)

    # 上传本次爬取文件
    logger.info(upload_crawl_file(crawl_file_list("./result/{}".format(crawl_id)), data_id=data_id, connect=connect))

    # 上传爬取图片
    if os.path.isdir('./result/{}/url'.format(crawl_id)):
        logger.info(upload_crawl_img_new('./result/{}/url'.format(crawl_id), './result/{}/images'.format(crawl_id),
                                         data_id=data_id, connect=connect))

    # 上传爬取状态
    crawling(crawl_id, data_id, connect, status)
    keywords_count_list, sites_count_list = getCrawlStatus(crawl_id)
    if keywords_count_list != {}:
        print(keywords_count_list, sites_count_list)
        updateTextCrawlStatus(keywords_count_list, data_id, connect)
        updateShowTextStatus(sites_count_list, connect)

    # 删除文件夹
    if os.path.isdir('./result/{}'.format(crawl_id)):
        shutil.rmtree('./result/{}'.format(crawl_id))
