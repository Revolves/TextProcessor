import logging
import os
import random
import re
import time
from urllib.request import Request, urlopen

import pdfplumber
import pymssql
import pymysql
import redis
import pyodbc
from pdfminer.converter import PDFPageAggregator
from pdfminer.layout import LAParams
from pdfminer.pdfinterp import PDFResourceManager, PDFPageInterpreter
from pdfminer.pdfparser import PDFParser
from pdfminer.pdfdocument import PDFDocument

# 设置日志输出格式
logging.basicConfig(level=logging.DEBUG,
                    format='[%(asctime)-15s] [%(levelname)8s] [%(name)10s ] - %(message)s (%(filename)s:%(lineno)s)',
                    datefmt='%Y-%m-%d %T'
                    )
logger = logging.getLogger(__name__)


class DbConnect:
    def __init__(self):
        connect = pyodbc.connect('DSN=Inceptor Server')
        cursor = connect.cursor()
        sql = """
        #             ALTER TABLE hs.text_crawl ADD COLUMNS(
        #                 attributes STRING
        #             );
        #             """
        # cursor.execute(sql)
        print(cursor.execute("show tables in hs").fetchall())
        print(cursor.execute("SHOW COLUMNS in text_crawl in hs").fetchall())


def CreatePath(path):
    """
    判断保存路径是否存在，如果不存在则创建
    :param path:
    :return:
    """
    if os.path.exists(path) is False:
        os.makedirs(path)
        print("创建保存路径:", path)
    else:
        print("保存路径已存在： ", path)


# Truncate header and tailer blanks
def strip(data):
    if data is not None:
        return data.strip()
    return data


def content_pdf(pdf_url):
    """
    解析pdf
    :param pdf_url:
    :return:
    """
    try:
        request = Request(url=pdf_url)
        print(pdf_url)
        fp = urlopen(request)  # 打开在线PDF文档
        # 用文件对象创建一个PDF文档分析器
        parser = PDFParser(fp)
        # 创建一个PDF文档
        doc = PDFDocument()
        # 连接分析器，与文档对象
        parser.set_document(doc)
        doc.set_parser(parser)

        # 提供初始化密码，如果没有密码，就创建一个空的字符串
        doc.initialize()

        # # 检测文档是否提供txt转换，不提供就忽略
        # if not doc.is_extractable:
        #     raise PDFTextExtractionNotAllowed
        # else:

        # 创建PDF，资源管理器，来共享资源
        resources = PDFResourceManager()
        # 创建一个PDF设备对象
        laparams = LAParams()
        device = PDFPageAggregator(resources, laparams=laparams)
        # 创建一个PDF解释其对象
        interpreter = PDFPageInterpreter(resources, device)

        # 要返回的内容
        content_text = ""
        # 循环遍历列表，每次处理一个page内容
        # doc.get_pages() 获取page列表

        for page in doc.get_pages():
            interpreter.process_page(page)
            # 接受该页面的LTPage对象
            layout = device.get_result()
            # 这里layout是一个LTPage对象 里面存放着 这个page解析出的各种对象
            # 一般包括LTTextBox, LTFigure, LTImage, LTTextBoxHorizontal 等等
            # 想要获取文本就获得对象的text属性，
            for x in layout:
                if hasattr(x, 'get_text'):
                    x.get_text.replace('\n', ' ')
                    content_text += x.get_text()
        return content_text
    except:
        logger.info('parse pdf failed')
        return ""


def parse_pdf(path_or_url, mode=1, url_params=None, proxies=None, save_as=None):
    """
    <语法>
        参数path_or_url: PDF文档路径或者URL
        参数mode: 设置解析模式，
            [1, '1', 'text']返回文档内容 -> str
            [2, '2', 'table']返回表格信息 -> list
            [3, '3', 'text_and_table']返回文档内容及表格信息 -> tuple
        参数url_params: 读取在线PDF文档时，传入requests请求参数，类型 <- dict
        参数proxies: 读取在线PDF文档时，传入requests的代理
        参数save_as: 读取在线PDF文档时，若进行此项设置则另存为本地文档，方便后续使用
    </语法>
    """

    url_mode = False

    # 判断是本地文档还是在线文档
    if re.search(r'''(?x)\A([a-z][a-z0-9+\-.]*)://([a-z0-9\-._~%]+|\[[a-z0-9\-._~%!$&'()*+,;=:]+\])''', path_or_url):
        url_mode = True
    else:
        pdf_path = path_or_url

    if url_mode:
        import requests
        pdf_url = path_or_url
        headers_d = None
        headers_d = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; WOW64; Trident/7.0; SLCC2; .NET CLR 2.0.50727; .NET CLR 3.5.30729; .NET CLR 3.0.30729; Media Center PC 6.0; .NET4.0C; rv:11.0) like Gecko)'}
        if not proxies:
            proxy_host = {}
        if not url_params:
            url_params = {}
            url_params['headers'] = headers_d
            url_params['data'] = None
            url_params['params'] = None
            url_params['proxies'] = proxies
        if not url_params['headers']: url_params['headers'] = headers_d
        if url_params['data'] or url_params['params']:
            response = requests.post(pdf_url, **url_params)
        else:
            response = requests.get(pdf_url, **url_params)

        # 写入临时文件再进行解析
        pdf_path = save_as if save_as else f'~temp{time.time()}~.pdf'
        with open(pdf_path, 'wb') as f:
            for chunk in response.iter_content(chunk_size=1024):
                if chunk:
                    f.write(chunk)
                    f.flush()

    pdf_path = os.path.abspath(pdf_path)

    # 用pdfplumber对pdf文档进行解析
    pdf_text = ''
    pdf_tables = []
    with pdfplumber.open(pdf_path) as pdf:
        for page in pdf.pages:
            if str(mode).lower() in ['1', 'text', '0', '3']:
                pdf_text += page.extract_text()
            if str(mode).lower() in ['2', 'table', '0', '3']:
                pdf_tables += page.extract_tables()

    # 删除临时pdf文档
    if url_mode and not save_as:
        try:
            os.remove(pdf_path)
        except Exception as e:
            pass

    if str(mode).lower() in ['1', 'text']:
        return pdf_text.replace('\n', ' ')
    elif str(mode).lower() in ['2', 'table']:
        return pdf_tables
    elif str(mode).lower() in ['3', 'text_and_table']:
        return pdf_text, pdf_tables


def connect_db():
    connect = pymysql.connect(host='127.0.0.1', user="root", database="HSDB", password='root', autocommit=True,
                              port=3306)
    # connect = pymssql.connect()
    return connect


def delete_table(cursor, tag):
    try:
        cursor.execute("""
        DROP TABLE data_{}
        """.format(tag))
    except:
        logger.info("table not exist")


def create_table(cursor, tag):
    """
    create a table in db
    :param cursor:

    :return:
    """
    try:
        cursor.execute("""
            CREATE TABLE data_{} (
                keyword VARCHAR(100) DEFAULT NULL,
                source VARCHAR(100) DEFAULT NULL,
                title VARCHAR(100) DEFAULT NULL,
                url VARCHAR(100) DEFAULT NULL,
                date VARCHAR(100) DEFAULT NULL,
                content text
            )
            """.format(tag))
    except:
        logger.warning("Table data_{} existed!".format(tag))


def insert_to_db(cursor, tag, item):
    """

    :param cursor:
    :param item:
    :return:
    """
    try:
        key_str = ",".join(list(item.keys()))
        value_str = ",".join(map(lambda x: "{}".format(x) if str(x).isdigit() else "'{}'".format(x), item.values()))
        cursor.execute("""
        INSERT INTO data_{} ({}) 
        VALUES ({})
        """.format(tag, key_str, value_str))
    except Exception as e:
        logger.exception('insert error, detail:{}'.format(e))


def load_json_to_db():
    pass


def get_random_ip():
    connect = redis.Redis(host="127.0.0.1", port=6379, password="password")
    count = connect.hlen('use_proxy')
    rand = random.randint(1, count)
    ip = str(connect.hkeys('use_proxy')[rand])[2:][:-1]
    print("switch ip {}".format(ip))
    return "https://" + ip


def mkdirs(dirs):
    """ Create `dirs` if not exist. """
    if not os.path.exists(dirs):
        os.makedirs(dirs)


if __name__ == '__main__':
    pass
