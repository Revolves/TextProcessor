# coding:utf-8
"""
user: adm
date: 2021/7/23
time: 15:21
IDE: PyCharm
"""

import time
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.wait import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC


def get_javascript0_links(url, class_name, sleep_second=0.01):
    """
    Selenium模拟用户点击爬取url
    :param url: 目标页面
    :param class_name: 模拟点击的类
    :param sleep_second: 留给页面后退的时间
    :return: list, 点击class为class_name进去的超链接
    """

    def wait(locator, timeout=10):
        """等到元素加载完成"""
        WebDriverWait(driver, timeout).until(EC.presence_of_element_located(locator))

    options = Options()
    # options.add_argument("--headless")  # 无界面
    driver = webdriver.Chrome(chrome_options=options)
    driver.get(url)

    locator = (By.CLASS_NAME, class_name)
    wait(locator)
    elements = driver.find_elements_by_class_name(class_name)
    link = []
    linkNum = len(elements)
    for i in range(linkNum):
        wait(locator)
        elements = driver.find_elements_by_class_name(class_name)
        driver.execute_script("arguments[0].click();", elements[i])
        time.sleep(sleep_second)
        link.append(driver.current_url)
        time.sleep(sleep_second)
        driver.back()
    driver.quit()
    return link


if __name__ == "__main__":
    url = "https://pubs.cstam.org.cn/search?q=%E7%88%86%E7%82%B8"
    class_name = 'download-pdf'
    link = get_javascript0_links(url, class_name)
    for i, _link in enumerate(link):
        print(i, _link)