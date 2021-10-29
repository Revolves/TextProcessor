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
URL_HEADER = "https://zh.wikipedia.org//zh-cn/"
HTTPS = "https:"
reg = re.compile("\\[.*]")
country = ['俄罗斯', '美国', '奥地利', '法国', '日本']
TITLE_TYPE = ["h2", "h3"]


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


def create_save_table_file(path, filename):
    """
    创建保存获取表格信息的文件

    :type filename: str
    :param path: 保存路径
    :param filename: 保存文件名
    :return: csv.writer
    """
    # 文件命名方式 当前页面标题_当前表标题.csv
    print("filename: {}".format(filename))
    file = open('{}{}.csv'.format(path, filename), 'w+', newline='', encoding='utf-8-sig')
    return csv.writer(file)


def parse_page(file_path, urls):
    web_page = open(file_path, 'r', encoding='utf-8')
    soup = bs(web_page, features='html.parser')
    title = soup.find("h1", {'class', 'firstHeading'}).text.split('[')[0]  # 获取主页标题
    print(title)
    wiki_table = soup.find_all("table", {'class', 'wikitable'})  # 选择页面表格
    for table in wiki_table:
        # 遍历一个表格属性
        title_attr = table.previous_sibling
        while title_attr.name not in TITLE_TYPE:
            title_attr = title_attr.previous_sibling
        table_title = title_attr.text.split('[')[0].replace('/', '，')  # 获取表格标题
        writer = csv_writer = csv.writer(
            open('{}{}_{}.csv'.format(SAVE_PATH, title, table_title), 'w+', newline='', encoding='utf-8-sig'))
        header = True
        savefile = True
        has_rowspan = False
        has_colspan = False
        # writer = None  # csv.writer
        table_header = []  # 表格表头
        table_header_len = 0  # 表格表头长度
        table_data = []  # 表格单行数据
        last_table_data = []  # 上一行表格数据
        rowspan = {}  # 标签的rowspan属性会使该值对应多行，该变量用于记录那个位置的rowspan及其对应值，{1:2} :指第一列的值对应2行。
        colspan = 0  # 标签的colspan属性使该值对应多列。
        # 部分表格以thead作为表头属性名
        if table.find('thead'):
            header = False
            for th in table.find('thead').find('tr').children:
                if th.string.strip('\n') != '':
                    table_header.append(th.string.strip('\n'))
                    # table_header_len += 1
            # 若获取属性为空，则该数据无效
            if str(table_header).strip("\n") == '':
                table_header = []
                # table_header_len = 0
            else:
                writer.writerow(table_header)
        table = table.find('tbody')
        # 遍历每个表格，获取信息
        for child in table.children:
            table_data_len = 0  # 表格数据的位置指示
            # print(child)
            # if child.find('th'):
            #     table_title = child.find('th').text
            #     print(table_title)
            #     writer = create_save_table_file(SAVE_PATH, (title + "_" + table_title).strip("\n"))
            #     if header is False:
            #         writer.writerow(table_header)
            #     continue
            if child.string:
                if child.string.strip('\n') == '':
                    continue
            if header:
                for th in child.find_all('th'):
                    if th.text.strip("\n") != '':
                        table_header.append(re.sub(reg, '', th.text.strip("\n")))
                        table_header_len += 1
                if str(table_header).strip("\n") == '[]' or len(table_header) <= 1:
                    table_header = []
                    table_header_len = 0
                    continue
                writer.writerow(table_header)
                header = False
                continue
            for td in child.find_all('td'):
                if has_colspan:
                    if colspan > 0:
                        table_data.append('')
                        colspan -= 1
                        continue
                    else:
                        has_colspan = False
                if td.has_attr("rowspan"):
                    rowspan[table_data_len] = int(td["rowspan"]) - 1
                    has_rowspan = True
                if td.has_attr("colspan"):
                    colspan = int(td["colspan"])
                    has_colspan = True
                if td.a is not None:
                    pass
                    # text = str(td.a.string).replace("/", ',').replace("\xa0", '').strip("\n").strip(" ").strip(u"\xa0")
                    # if text == 'None':
                    #     text = ''
                # elif td.find("small") is not None:
                #     text = td.find("small").text.replace("/", ',').replace("\xa0", '').strip("\n").strip(" ").strip(u"\xa0")
                #     print(text)
                else:
                    pass
                text = re.sub(reg, '',
                              td.text.replace("/", ',').replace("\xa0", '').strip("\n").strip(" ").strip(u"\xa0"))
                if text == '':
                    if td.find('a', ("class", "image")):
                        text = td.a['href']
                    elif td.find('a', ("class", "new")):
                        text = td.a["title"]
                        # print(text)

                if has_rowspan:
                    table_data.append(text.strip("\t"))
                    table_data_len += 1
                else:
                    if table_data_len in list(rowspan.keys()):
                        table_data.append(last_table_data[table_data_len])
                        rowspan[table_data_len] -= 1
                        if rowspan[table_data_len] <= 0:
                            del rowspan[table_data_len]
                        table_data_len += 1
                    table_data.append(text.strip("\t"))
                    table_data_len += 1
                    try:
                        if td.a:
                            for a in td.find_all('a'):
                                if td.a.parent.name == 'sup':
                                    continue
                                url = td.a['href']
                                if '.' in url.split("/")[-1]:
                                    continue
                                if url.replace("/wiki/", "/zh-cn/") not in urls:
                                    urls[url.replace("/wiki/", "/zh-cn/")] = text
                    except Exception as e:
                        # print(e)
                        pass
                has_rowspan = False
            if str(table_data).strip("\n") == '[]' and len(str(table_data)) <= table_data_len:
                continue
            while table_data_len < table_header_len:
                table_data.append('')
                table_data_len += 1
            writer.writerow(table_data)
            last_table_data = table_data
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
        soup = bs(requests.get(url, timeout = 5).text, "html.parser")
        info_title = soup.find("h1", {'class', 'firstHeading'}).text.split('[')[0]  # 获取主页标题
        info_tables = soup.find_all("table", {'class', 'infobox'})  # 包含信息的表格

        image_urls = []  # 页面图像连接集合 （//div[@class='thumbinner']）图像标签
        flag_name = True  # 是否获取表名
        # flag_image = True  # 是否已保存图像
        weapon_name = info_title  # 装备名
        info[weapon_name] = {}

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
                if flag_name is False:
                    flag_name = True
                    if tr.find('span'):
                        weapon_name = tr.find('span').text
                    else:
                        for th in tr.find_all('th'):
                            print(th)
                            weapon_name = th.contents[0].replace("/", ',').replace("\xa0", '').strip("\n")
                            continue
                    if weapon_name == '':
                        weapon_name = info_title
                    info[weapon_name] = {}
                    continue
                try:
                    """
                    获取表格信息
                    """
                    # 表格内的图像
                    if weapon_name == '':
                        weapon_name = info_title
                    if tr.find('a', ('class', 'image')) is not None:
                        # flag_image = False
                        img = tr.find('a', ('class', 'image')).find('img')  # 图像标签
                        image_urls.append(HTTPS + img['src'])
                        info[weapon_name]['图片'] = image_urls
                        continue

                    # 表格内容
                    if tr.find('td') and tr.find('th'):
                        th = tr.find('th').text.replace("/", ',').replace("\xa0", '').strip("\n").strip("•").strip(" ")  # 属性名
                        if th == "舰种": # 统一标签名
                            th = "类型"
                        td = tr.find('td').text.replace("\xa0", '').replace("\n", ";")  # 属性值
                        info[weapon_name][th] = re.sub(reg, '', td)
                except:
                    """
                    出现异常，输出当前标签
                    """
                    # print(tr)
                    continue
        # 除去国家或地名
        if "首都" in info[weapon_name] or "国家" in info[weapon_name] or "类型" not in info[weapon_name]:
            continue
        if str(info[weapon_name]) != "{}":
            print(info)
            print("{} finish！".format(url_list[url]))
            all_info.append(info)
    json.dump(all_info, save_file, indent=4, ensure_ascii=False)


if __name__ == '__main__':
    file_list = get_file(PATH)
    save_file = open('result.json', 'a+', newline='', encoding='utf-8-sig')
    result = []
    info_urls = {
        # 'https://zh.wikipedia.org/zh-cn/%E7%96%BE%E9%A2%A8%E6%88%B0%E9%AC%A5%E6%A9%9F': '阵风战斗机',
        # 'https://zh.wikipedia.org/zh-cn/%E7%BE%8E%E6%B4%B2%E8%B1%B9%E6%94%BB%E6%93%8A%E6%A9%9F': '美洲豹攻击机'
    }
    for file in file_list:
        print("now file is {}".format(file))
        parse_page(file, info_urls)
        # break
    print(info_urls)
    json.dump(result, save_file, indent=4, ensure_ascii=False)
    save_file.close()
    parse_url(info_urls)
