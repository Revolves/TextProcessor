# 1. 导入Flask类
from ast import keyword
import os, re
from flask import Flask, request
import json
from Transwarp import Transwarp
from MultisiteSchedule import start_spiders, stop, rowkey_id_gen
from Precision import TextSimilarity, cos_sim
from scrapy import cmdline

stop_signal = 'stop_signal'
app = Flask(__name__)  # 创建一个服务，赋值给APP

sites = []

transwarp = Transwarp("Transwarp/JavaJar/InceptorUtil.jar", "Transwarp/libs")

WEB_MAP = {"baidu": 'baidu',
           "wiki": "wiki",
           "NASA": "nasa",
           "AIAA": "aiaa",
           "铁血论坛": "tiexue",
           'twitter': 'twitter',
           "facebook": "facebook",
           "jane's": "janes"
           }
@app.route('/')
def homepage():
    return "主页"


@app.route('/text_crawler_start', methods=['post', 'get'])
def start_text_crawler():
    status = request.values['status']
    crawl_id = request.values['crawl_id']
    keywords = request.values['keywords']
    data_type = request.values['data_type']
    sites = request.values['website']
    data_dict = {"crawl_id": crawl_id, "sites": sites, "keywords": keywords, "status": status, "data_type":data_type}
    # # status = request.args.get('status')
    # # j_data = json.loads(data)
    print(data_dict)
    transwarp.connect_inceptor()
    transwarp.connect_hdfs()
    # data_dict = None
    start_spiders(transwarp=transwarp, info=data_dict)

    return 'success'

@app.route('/text_crawler_stop', methods=['post', 'get'])
def stop_text_crawler():
    status = '0'
    crawl_id = 'test'
    status = request.values['status']
    if request.values['crawl_id'] != '':
        crawl_id = request.values['crawl_id']
    print(crawl_id)
    if os.path.isdir(stop_signal) is False:
        os.mkdir(stop_signal)
    if status == '0':
        f = open('{}/{}'.format(stop_signal,crawl_id),'w')
        f.close()
    return 'stop'


@app.route('/text_test_start', methods=['post', 'get'])
def test_start_text_crawler():
    status = request.values['status']
    crawl_id = request.values['crawl_id']
    keywords = request.values['keywords']
    data_type = request.values['data_type']
    sites = request.values['website']
    data_dict = {"crawl_id": crawl_id, "sites": sites, "keywords": keywords, "status": status, "data_type":data_type}
    # # status = request.args.get('status')
    # # j_data = json.loads(data)
    print(data_dict)
    transwarp.connect_inceptor()
    transwarp.connect_hdfs()
    # data_dict = None
    start_spiders(transwarp=transwarp, info=data_dict)

    return 'success'

@app.route('/text_test_stop', methods=['post', 'get'])
def test_stop_text_crawler():
    status = '0'
    crawl_id = 'test'
    status = request.values['status']
    if request.values['crawl_id'] != '':
        crawl_id = request.values['crawl_id']
    print(crawl_id)
    if os.path.isdir(stop_signal) is False:
        os.mkdir(stop_signal)
    if status == '0':
        f = open('{}/{}'.format(stop_signal,crawl_id),'w')
        f.close()
    return 'stop'

@app.route('/text_crawler_precision', methods=['post', 'get'])
def compute_precision():
    crawl_id = None
    status = request.values['status']
    if status != '3':
        return '0'
    crawl_id = request.values['crawl_id']
    files_data = request.values['files']
    print(str(type(files_data))+":"+files_data)
    file_name_list = files_data.split(',')
    print(crawl_id, file_name_list)
    crawl_file = r'./result' 
    ts = TextSimilarity(file_name_list, crawl_file)
    precision = (str(ts.splitWordSimlaryty(sim=cos_sim)))
    if crawl_id is not None:
        transwarp.connect_inceptor()
        sql_ = "INSERT INTO  hsold.text_precision  VALUES (?, ?, ?)"
        pram_ = [rowkey_id_gen(), crawl_id, precision]
        transwarp.execute_sql(sql_, pram_)
        transwarp.close_inceptor()
    return str(ts.splitWordSimlaryty(sim=cos_sim))

@app.route('/test', methods=['post', 'get'])
def get_status():
    # data = request.get_data()
    status = request.values['status']
    crawl_id = request.values['crawl_id']
    keywords = request.values['keywords']
    data_type = request.values['data_type']
    websites = request.values['website']
    # status = request.args.get('status')
    # j_data = json.loads(data)
    print(status, crawl_id, keywords, data_type, websites)
    # print(status)# 使用request.args.get方式获取拼接的入参数据
    return 'success'

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=6008, debug=True, use_reloader=False)
    # 运行Flask应用
    # 127.0.0.1----回环地址IP， 每台主机都有====localhost
    # 如何设置， 使得服务奇特主机的浏览器可以访问?  '0.0.0.0'开放所有的IP， 使得可以访问
    # 如何修改端口?  # 可能会报错:Address already in use
    # 启动服务
    # app.run(host='0.0.0.0'`, port=8080, debug=True)
