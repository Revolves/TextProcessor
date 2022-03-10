"""
user: adm
date: 2021/9/17
time: 11:21
IDE: PyCharm
"""
import os

import jpype


class Transwarp:
    def __init__(self, jar_path, dependency_path):
        self.jarpath = os.path.join(os.path.abspath("."), jar_path)
        self.dependency_path = os.path.join(os.path.abspath('.'), dependency_path)
        hdfs_file = os.path.join(os.path.abspath('.'), 'Transwarp/hdfs')
        print(hdfs_file)
        print("jar_path:{}".format(self.jarpath))
        print("dependency_path:{}".format(self.dependency_path))
        # 获取jvm.dll 的文件路径
        jvmPath = jpype.getDefaultJVMPath()
        # 开启jvm
        try:
            jpype.startJVM(jvmPath, "-ea", "-Djava.class.path=%s" % self.jarpath, "-Djava.ext.dirs=%s" % self.dependency_path)
        except:
            pass
            # jpype.shutdownJVM()
            # jpype.startJVM(jvmPath, "-ea", "-Djava.class.path=%s" % self.jarpath,
            #                "-Djava.ext.dirs=%s" % self.dependency_path)
        self.hdfs = jpype.JClass("DB.HdfsUtil")
        # self.hdfs = jpype.JClass("java.lang.Class").forName("DB.HdfsUtil")
        self.inceptor = jpype.JClass("DB.InceptorUtil")
        javaInstance = self.hdfs(hdfs_file)

    def connect_hdfs(self):
        """
        创建hdfs连接
        """
        try:
            self.hdfs.GetHdfsClient()
        except Exception as e:
            print("{} 连接失败".format(e))

    def connect_inceptor(self):
        """
        建立Inceptor连接

        :return:
        """
        self.inceptor.Connect()

    def execute_sql(self, sql, prams):
        """
        Inceptor执行sql语句

        :param sql: sql语句
        :param prams: sql语句中的参数（该属性为string数组）
        :return:
        """
        if isinstance(prams, list):
            self.inceptor.ExecuteSql(sql, prams)

    def upload_file(self, path_from, path_to):
        """
        上传文件到hdfs

        :param path_from: 本地路径
        :param path_to: 上传hdfs路径
        :return:成功返回保存路径，失败返回upload failure
        """
        return self.hdfs.uploadFile(path_from, path_to)

    def download_file(self, path_from, path_to):
        """
        hdfs下载文件

        :param path_from:源路径（文件/文件夹）
        :param path_to:目标文件夹
        :return:成功返回保存路径，download failure
        """
        return self.hdfs.downloadFile(path_from, path_to)

    def delete_file(self, path):
        """
        hdfs删除文件

        :param path: 目标路径
        :return:None
        """
        self.hdfs.deleteFile(path)

    def make_dir(self, path):
        """
        hdfs创建文件夹

        :param path:
        :return:None
        """
        self.hdfs.makeDir(path)

    def get_file(self, path):
        """
        获取服务器hdfs系统里指定文件  以字节流的形式返回

        :param path: 目标路径
        :return:
        """
        self.hdfs.getFile(path)

    def upload_files(self, path_from, path_to):
        """
        上传文件夹或者文件到hdfs

        :param path_from: 源路径
        :param path_to: 目的路径
        :return: 成功返回保存路径，失败upload failure
        """
        return self.hdfs.uploadFiles(path_from, path_to)

    def close_inceptor(self):
        """
        断开连接

        :return:
        """
        self.inceptor.CloseConnect()

    def close_hdfs(self):
        self.hdfs.CloseHdfs()

# hdfs_util = Transwarp('JavaJar/Util.jar', 'libs/')
# hdfs_util.connect_hdfs()
