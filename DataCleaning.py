"""
user: adm
date: 2021/8/15
time: 16:07
IDE: PyCharm  
"""
import datetime
import json
import logging
import os

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
                        list_json.append(os.path.join(root, file))  # json文件只有一个，就不用列表了

        return list_json  # 得到所有的jpg文件和json文件的列表(包含路径)

    def data_clean(self):
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
        useful_pre = (self.TotalData - self.UselessData) / self.TotalData
        logger.info('数据可用率为：{}'.format(useful_pre))
        return (self.TotalData - self.UselessData) / self.TotalData

    def connect_db(self):
        pass


if __name__ == '__main__':
    DC = DataCleaning('result')
    DC.data_clean()
