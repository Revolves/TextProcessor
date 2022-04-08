# coding:utf-8
"""
user: adm
date: 2021/7/23
time: 15:21
IDE: PyCharm
"""
from ast import keyword
import csv
import datetime
from hashlib import md5
import os
import random
import time
from turtle import st
from Transwarp import Transwarp
import pandas as pd

import datetime
from threading import Timer

item = {'a': 'aaaaa', 'b': 'bbbbbbb'}

item['all_content'] = '，'.join([x for x in item.values()])
print(item.values())