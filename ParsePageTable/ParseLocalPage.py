"""
user: adm
date: 2021/8/30
time: 19:22
IDE: PyCharm  
"""
import json
import os
import re
import csv
from typing import TextIO

import pandas
import requests
from w3lib.html import remove_tags

from bs4 import BeautifulSoup as bs

PATH = "台印装备"
SAVE_PATH = "result/"
URL_HEADER = "https://zh.wikipedia.org"
HTTPS = "https:"
reg = re.compile("<[^>]*>")
country = ['俄罗斯', '']


def get_file(path):
    """
    获取文件
    :param path:
    :return:
    """
    file_list = []
    for root, dirs, files in os.walk(path):
        for file in files:
            # if file.endswith('.html'):
            file_list.append(os.path.join(root, file))
    return file_list


def parse_page(file_path, urls):
    web_page = open(file_path, 'r', encoding='utf-8')
    soup = bs(web_page, features='html.parser')
    title = soup.find("h1", {'class', 'firstHeading'}).text.split('[')[0]  # 获取主页标题
    print(title)
    wiki_table = soup.find_all("table", {'class', 'wikitable'})  # 选择页面表格
    for table in wiki_table:
        # 遍历一个表格属性
        table_title = table.previous_sibling.previous_sibling.text.split('[')[0].replace('/', '，')  # 获取表格标题
        if len(table_title) > 20:
            table_title = table.previous_sibling.previous_sibling.previous_sibling.previous_sibling.text.split('[')[
                0].replace('/', '，')
        csv_writer = csv.writer(
            open('{}{}_{}.csv'.format(SAVE_PATH, title, table_title), 'w+', newline='', encoding='utf-8-sig'))
        print("table_title:{}".format(table_title))
        header = True
        table_header = []  # 表格表头
        table_data = []  # 表格单行数据
        if table.find('thead'):
            header = False
            for th in table.find('thead').find('tr').children:
                if th.string.strip('\n') != '':
                    table_header.append(th.string.strip('\n'))
            csv_writer.writerow(table_header)
        table = table.find('tbody')
        for child in table.children:
            if header:
                for th in child.find_all('th'):
                    table_header.append(th.text.strip("\n"))
                csv_writer.writerow(table_header)
                header = False
                continue
            if child.string:
                if child.string.strip('\n') == '':
                    child = child.next_sibling
                if child:
                    for td in child.find_all('td'):
                        text = td.text.replace("/", ',').replace("\xa0", '').strip("\n").strip(" ").strip(u"\xa0")
                        try:
                            if td.a['href']:
                                url = td.a['href']
                                if '.' in url.split("/")[-1]:
                                    continue
                                if url not in urls:
                                    urls[url] = text
                        except Exception as e:
                            pass
                        table_data.append(text)
                    csv_writer.writerow(table_data)
                    table_data = []
        continue


def parse_url(url_list):
    """
    获取链接页面的信息表格
    :param url_list:
    :return:
    """
    save_file = open("{}info.json".format(SAVE_PATH), 'w', encoding='utf-8')
    all_info = []  # 页面所需信息保存
    for url in url_list:
        info = {}  # 当前页面信息
        soup = bs(requests.get(url).text, "html.parser")
        info_title = soup.find("h1", {'class', 'firstHeading'}).text.split('[')[0]  # 获取主页标题
        info_tables = soup.find_all("table", {'class', 'infobox'})  # 包含信息的表格

        image_urls = []  # 页面图像连接集合 （//div[@class='thumbinner']）图像标签
        flag_name = True  # 是否获取表名
        # flag_image = True  # 是否已保存图像
        weapon_name = ''  # 装备名

        # 搜索页面中的图像
        images = soup.find_all("div", {'class', 'thumbinner'})
        for image in images:
            try:
                image_urls.append(HTTPS + image.find('a', ('class', 'image')).find('img')['src'])
            except:
                continue

        # 获取页面表格信息
        for table in info_tables:
            table = table.find('tbody')
            for tr in table.children:
                # print("tr 1 :{}".format(tr))
                # 装备名称
                if flag_name:
                    flag_name = False
                    for th in tr.find_all('th'):
                        weapon_name += th.text.replace("/", ',').replace("\xa0", '').strip("\n")
                    info[weapon_name] = {}
                    continue
                try:
                    """
                    获取表格信息
                    """
                    # 表格内的图像
                    if tr.find('a', ('class', 'image')) is not None:
                        # flag_image = False
                        img = tr.find('a', ('class', 'image')).find('img')  # 图像标签
                        image_urls.append(HTTPS + img['src'])
                        info[weapon_name]['图片'] = image_urls
                        continue

                    # 表格内容
                    if tr.find('td') and tr.find('th'):
                        th = tr.find('th').text.replace("/", ',').replace("\xa0", '').strip("\n")  # 属性名
                        td = tr.find('td').text.replace("/", ',').replace("\xa0", '').strip("\n")  # 属性值
                        info[weapon_name][th] = td
                except:
                    """
                    出现异常，输出当前标签
                    """
                    continue

        print("{} finish！".format(url_list[url]))
        all_info.append(info)
    json.dump(all_info, save_file, indent=4, ensure_ascii=False)


if __name__ == '__main__':
    file_list = get_file(PATH)
    # save_file = open('result.json', 'a+', newline='', encoding='utf-8-sig')
    result = []
    info_urls = {
        # 'https://zh.wikipedia.org/wiki/%E7%96%BE%E9%A2%A8%E6%88%B0%E9%AC%A5%E6%A9%9F': '阵风战斗机',
        # 'https://zh.wikipedia.org/wiki/%E7%BE%8E%E6%B4%B2%E8%B1%B9%E6%94%BB%E6%93%8A%E6%A9%9F': '美洲豹攻击机'
    }
    for file in file_list:
        print("now file is {}".format(file))
        parse_page(file, info_urls)
        # break
    print(info_urls)
    # json.dump(result, save_file, indent=4, ensure_ascii=False)
    # save_file.close()
    parse_url(info_urls)
