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


class Foo:
    def __init__(self, name):
        self.name = name

    def __getitem__(self, item):
        print(self.__dict__[item])

    def __setitem__(self, key, value):
        print('obj[key]= vlaue时我执行')
        self.__dict__[key] = value

    def __delitem__(self, key):
        print('del obj[key]时,我执行')
        self.__dict__.pop(key)

    def __delattr__(self, item):
        print('del obj.key时,我执行')
        self.__dict__.pop(item)


f1 = Foo('sb')

f1['age'] = 18

f1['age1'] = 19

del f1.age1

del f1['age']

f1['name'] = 'alex'

print(f1.__dict__)
l = os.listdir('result')
print(l)