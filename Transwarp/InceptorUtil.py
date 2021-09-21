"""
user: Jhao
date: 2021/9/17
time: 16:18
IDE: PyCharm  
"""
import os

import jpype


class InceptorUtil:
    def __init__(self, jar_path, dependency_path):
        self.jarpath = os.path.join(os.path.abspath("."), jar_path)
        self.dependency_path = os.path.join(os.path.abspath('.'), dependency_path)
        # 获取jvm.dll 的文件路径
        jvmPath = jpype.getDefaultJVMPath()
        # 开启jvm
        jpype.startJVM(jvmPath, "-ea", "-Djava.class.path=%s" % self.jarpath,
                       "-Djava.ext.dirs=%s" % self.dependency_path)
        self.inceptor = jpype.JClass("DB.InceptorUtil")

    def connect(self):
        self.inceptor.Connect()


# ic = InceptorUtil('JavaJar/InceptorUtil.jar', 'libs')
# ic.connect()
