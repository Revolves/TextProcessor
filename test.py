"""
user: adm
date: 2021/7/23
time: 15:21
IDE: PyCharm
"""
import datetime
import imp
import os
import random
import time
from turtle import st
from Transwarp import Transwarp
import pandas as pd
transwarp = Transwarp("Transwarp/JavaJar/InceptorUtil.jar", "Transwarp/libs")
transwarp.connect_inceptor()
transwarp.connect_hdfs()

# transwarp1 = Transwarp("Transwarp/JavaJar/Util.jar", "Transwarp/libs")
# transwarp1.connect_inceptor()
transwarp.upload_file("Transwarp/JavaJar/Util.jar", "//text_crawl")
# cmdline.execute(['scrapy'])
# print(type(md5(str(time.time()).encode()).hexdigest()))
# transwarp.execute_sql('CREATE TABLE hs.show_text_interact(id STRING, http_interact STRING) CLUSTERED BY (id) into 2 buckets stored \
#                                 as HYPERDRIVE TBLProperties ("transactional"="true")',[])
# transwarp.execute_sql('CREATE TABLE hs.show_text_location(loc STRING, id STRING,  count STRING) CLUSTERED BY (loc) into 2 buckets stored \
#                                 as HYPERDRIVE TBLProperties ("transactional"="true")',[])
# transwarp.execute_sql('CREATE TABLE hs.show_text_keyword(keyword STRING, id STRING,  count STRING) CLUSTERED BY (keyword) into 2 buckets stored \
#                                  as HYPERDRIVE TBLProperties ("transactional"="true")',[])
# transwarp.execute_sql('CREATE TABLE hs.show_text_status(site STRING, id STRING,  count STRING) CLUSTERED BY (site) into 2 buckets stored \
#                                   as HYPERDRIVE TBLProperties ("transactional"="true")',[])
# table = ['show_text_interact', 'show_text_status', 'show_text_location', 'show_text_keyword']
# path = r'可视化设计\show_text_table.xlsx'
# data_dict = pd.read_excel(path, sheet_name=[0,1,2,3])
# map_d = [10, 8, 5, 5]
# for idx in data_dict:
#     if idx != 1:
#         continue
#     sheet = data_dict[idx]
#     print(table[idx])
#     if idx == 0:
#         sql = 'INSERT INTO  hs.{}  VALUES (?, ?)'.format(table[idx])
#     else:
#         sql = 'INSERT INTO  hs.{}  VALUES (?, ?, ?)'.format(table[idx])
#     columns = sheet.columns.values.tolist() ### 获取excel 表头 ，第一行
#     count = 0
#     first = 0
#     for _, row in sheet.iterrows(): ### 迭代数据 以键值对的形式 获取 每行的数据
#         data = []
#         for col in columns:
#             data.append(str(row[col]))
#         print(data)
#         start = datetime.datetime.now()
#         transwarp.execute_sql(sql, data)
#         print('插入用时：{}'.format(datetime.datetime.now() - start))
#         count += 1
#         if count == map_d[idx]:
#             break