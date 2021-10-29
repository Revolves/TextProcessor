"""
user: adm
date: 2021/7/23
time: 15:21
IDE: PyCharm
"""
import os
import threading
import time

i = 0


def loop_print():
    while True:
        time.sleep(10)
        global i
        print(i)


t = threading.Thread(target=loop_print)
t.start()
while True:
    i += 1
    time.sleep(1)
