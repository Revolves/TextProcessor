# 1. 导入Flask类
import json

from flask import Flask
from MultisiteSchedule import start_spiders
from Transwarp import Transwarp

app = Flask(__name__)


# 实例化server，把当前这个python文件当做一个服务，__name__代表当前这个python文件
# server = Flask(__name__)
#     实现主页
@app.route('/', methods=['get'])
def index():
    return "网站主页"


# 基本路由   /video_crawler/video_speed/---访问路径
# 视图函数   告诉app当用户访问/login/这个路径时， 执行login函数的内容， 最终将return的内容返回给客户端;
@app.route('/text_crawler/')
def return_speed():
    transwarp = Transwarp("Transwarp/JavaJar/Util.jar", "Transwarp/libs")
    transwarp.connect_inceptor()
    start_spiders(transwarp)
    return json.dumps('success'.encode(), ensure_ascii=False)


# json.dumps序列化时对中文默认使用ascii编码，想输出真正的中文需要指定ensure_ascii=False


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8080, debug=True)
    # 运行Flask应用
    # 127.0.0.1----回环地址IP， 每台主机都有====localhost
    # 如何设置， 使得服务奇特主机的浏览器可以访问?  '0.0.0.0'开放所有的IP， 使得可以访问
    # 如何修改端口?  # 可能会报错:Address already in use
    # 启动服务
    # app.run(host='0.0.0.0', port=8080, debug=True)
