# 1. 导入Flask类
from flask import Flask, request
import json
from Transwarp import Transwarp
from MultisiteSchedule import start_spiders

# from ZGJSProcessor import ZGJSProcessor

app = Flask(__name__)  # 创建一个服务，赋值给APP


# 实例化server，把当前这个python文件当做一个服务，__name__代表当前这个python文件
# server = Flask(__name__)
#     实现主页
@app.route('/')
def homepage():
    return "主页"


@app.route('/text_crawler', methods=['post', 'get'])
def start_textCrawler():
    status = request.values['status']
    crawl_id = request.values['crawl_id']
    keywords = request.values['keywords']
    data_type = request.values['data_type']
    sites = request.values['website']
    data_dict = {"crawl_id": crawl_id, "sites": sites, "keywords": keywords}
    # status = request.args.get('status')
    # j_data = json.loads(data)
    print(data_dict)
    transwarp = Transwarp("Transwarp/JavaJar/Util.jar", "Transwarp/libs")
    transwarp.connect_inceptor()
    start_spiders(transwarp=transwarp, info=data_dict)

    return 'success'


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


# 基本路由   /video_crawler/video_speed/---访问路径
# 视图函数   告诉app当用户访问/login/这个路径时， 执行login函数的内容， 最终将return的内容返回给客户端;
@app.route('/video_crawler/video_speed/')
def return_speed():
    res = {'msg': '接口返回信息', '视频获取速度': 'speed'}
    return json.dumps(res, ensure_ascii=False)


# json.dumps序列化时对中文默认使用ascii编码，想输出真正的中文需要指定ensure_ascii=False


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=6008, debug=True)
    # 运行Flask应用
    # 127.0.0.1----回环地址IP， 每台主机都有====localhost
    # 如何设置， 使得服务奇特主机的浏览器可以访问?  '0.0.0.0'开放所有的IP， 使得可以访问
    # 如何修改端口?  # 可能会报错:Address already in use
    # 启动服务
    # app.run(host='0.0.0.0', port=8080, debug=True)
