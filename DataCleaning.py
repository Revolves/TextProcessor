"""
user: adm
date: 2021/8/15
time: 16:07
IDE: PyCharm  
"""
import csv
import datetime
import json
import logging
import os

import jpype

to_day = datetime.datetime.now()
logging.basicConfig(level=logging.DEBUG,
                    format='[%(asctime)-15s] [%(levelname)8s] [%(name)10s ] - %(message)s (%(filename)s:%(lineno)s)',
                    datefmt='%Y-%m-%d %T',
                    filename='log/data_cleaning_{}_{}_{}.log'.format(to_day.year, to_day.month, to_day.day),
                    )
logger = logging.getLogger(__name__)


class DataCleaning:
    def __init__(self, file_path):
        self.TotalData = 0
        self.EnableData = 0
        self.UselessData = 0
        self.FilePath = file_path
        self.CommitData = []
        self.connect_db()

    def get_dirs(self, main_dir):
        """
        获取目录结构
        :param main_dir:
        :return:
        """
        list_dirs = []
        for root, dirs, files in os.walk(main_dir):
            for _dir in dirs:
                list_dirs.append(os.path.join(root, _dir))
        return list_dirs

    def get_file(self, dir_path):
        """
        获取目录下文件
        :param dir_path:
        :return:
        """
        list_json = []
        for _dir in dir_path:
            for root, dirs, files in os.walk(_dir):
                for file in files:
                    if file.endswith(".json"):  # 过滤得到json文件
                        list_json.append(os.path.join(root, file))

        return list_json  # 得到所有的jpg文件和json文件的列表(包含路径)

    def data_clean(self):
        """
        数据处理并存入数据库
        :return:
        """
        DataFile = self.get_file(self.get_dirs(self.FilePath))
        Useful = True  # 数据可用性
        for File in DataFile:
            with open(File, 'r', encoding='utf-8-sig') as file_json:
                try:
                    Data = json.load(file_json)
                except:
                    logger.exception('NO DATA')
                    continue
                self.TotalData += len(Data)
                if len(Data) > 0:
                    for _data in Data:
                        if len(_data['content'].replace(' ', '').replace("\n", '')) <= 20 or _data['content'] == '':
                            self.UselessData += 1
                        else:
                            self.CommitData.append(_data)
                if len(self.CommitData) > 500:
                    self.commit_data(self.CommitData)
                    self.CommitData = []
        # self.save_csv(self.CommitData)
        self.commit_data(self.CommitData)
        Precision = (self.TotalData - self.UselessData) / self.TotalData
        logger.info('Precision：{}'.format(Precision))
        print('查准率：{}'.format(Precision))
        return (self.TotalData - self.UselessData) / self.TotalData

    def connect_db(self):
        """
        使用jar jdbc连接数据库
        :return:
        """
        jarpath = os.path.join(os.path.abspath("."), r"file\DB.jar")
        dependency_path = os.path.join(os.path.abspath('.'), r'file\libs')
        # 获取jvm.dll 的文件路径
        jvmPath = jpype.getDefaultJVMPath()

        # 开启jvm
        jpype.startJVM(jvmPath, "-ea", "-Djava.class.path=%s" % jarpath, "-Djava.ext.dirs=%s" % dependency_path)
        self.javaClass = jpype.JClass("DB.ConnectDB")
        javaInstance = self.javaClass()
        self.javaClass.LinkDB()

    def save_csv(self, data):
        """
        将数据保存为csv
        :return:
        """
        csv_file = open("data.csv", "w+", encoding='utf-8')
        csv_writer = csv.writer(csv_file)
        for _data in data:
            if 'attributes' in _data:
                _params = (str(_data['keyword']),
                           str(_data['source']),
                           str(_data['title']),
                           str(_data['url']),
                           str(_data['date']),
                           str(_data['content']),
                           str(_data['attributes']))
            else:
                _params = (str(_data['keyword']),
                           str(_data['source']),
                           str(_data['title']),
                           str(_data['url']),
                           str(_data['date']),
                           str(_data['content']),
                           '')
            csv_writer.writerow(_params)
        csv_file.close()

    def commit_data(self, data):
        """
        提交数据
        :param data:
        :param connect:
        :return:
        """
        for _data in data:
            if 'attributes' in _data:
                _params = [str(_data['keyword']),
                           str(_data['source']),
                           str(_data['title']),
                           str(_data['url']),
                           str(_data['date']),
                           str(_data['content']),
                           str(_data['attributes'])]
            else:
                _params = [str(_data['keyword']),
                           str(_data['source']),
                           str(_data['title']),
                           str(_data['url']),
                           str(_data['date']),
                           str(_data['content']),
                           '']
            self.javaClass.InsertData(_params)
        logger.info('this time commit {} data'.format(len(data)))
        print('this time commit {} data'.format(len(data)))


if __name__ == '__main__':
    DC = DataCleaning('result')
    DC.data_clean()

