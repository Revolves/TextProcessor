"""
user: adm
date: 2021/7/20
time: 10:16
IDE: PyCharm  
"""
from selenium import webdriver

options = webdriver.ChromeOptions()
prefs = {
    'profile.default_content_setting_values':
        {'notifications': 2,
         'images': 2}  # 禁止谷歌浏览器弹出通知消息
}
options.add_experimental_option('prefs', prefs)
# 设置chrome浏览器无界面模式
options.add_argument('--headless')
options.add_argument('--no-sandbox')
options.add_argument('--disable-dev-shm-usage')
options.add_argument('--ignore-certificate-errors')
options.add_argument('--disable-software-rasterizer')
options.add_argument('-ignore -ssl-errors')
options.add_argument('--log-level=3')
options.add_experimental_option('excludeSwitches', ['enable-logging'])