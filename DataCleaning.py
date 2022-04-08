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
        """

        :param file_path: 目录根目录
        """
        self.FilePath = file_path

    def data_clean(self):
        """
        数据清洗

        :return:
        """
        start = datetime.datetime.now()
        dir_list = self.get_dirs(self.FilePath)
        print(dir_list)
        # for d in dir_list:
        #     file_list = self.get_file(self.FilePath + '/' + d)
        self.file_cat(dir_list, self.FilePath)
        end = datetime.datetime.now()
        logger.info("本次清洗用时：{}".format(end - start))

    @staticmethod
    def get_dirs(main_dir):
        """
        获取目录结构

        :param main_dir: 根目录
        :return: 根目录文件夹列表
        """
        return os.listdir(main_dir)

    @staticmethod
    def get_file(dir_path):
        """
        获取目录下文件（清除不符合要求文件夹）

        :param dir_path: 目录
        :return: 目录下所有符合要求文件列表
        """
        list_json = []
        for root, dirs, files in os.walk(dir_path):
            for file in files:
                if file.endswith(".json"):  # 过滤得到json文件
                    file_path = os.path.join(root, file)
                    file_size = os.stat(file_path).st_size
                    if file_size > 10:
                        list_json.append(file_path)
                    else:
                        os.remove(file_path)

        return list_json  # 得到所有的jpg文件和json文件的列表(包含路径)

    @staticmethod
    def file_cat(file_list, path):
        """
        将一类爬虫数据归一到一个文件

        :param file_list: 文件列表
        :return:
        """
        if len(file_list) == 0:
            return
        for file in file_list:
            file_p = path + '/' + file
            data = []
            f = open(file_p, 'r', encoding='utf-8-sig')
            f_json = json.load(f)
            print(f_json)
            for _ in f_json:
                if len(str(_['content'])) > 20 and len(str(_['title']).replace(" ", "")) > 0:
                    if _ not in data:
                        data.append(_)
            f.close()
            # os.remove(file)
            with open(file_p, "w", newline="", encoding="utf-8-sig") as f:
                json.dump(data, f, indent=4, ensure_ascii=False)


if __name__ == '__main__':
    # DC = DataCleaning('baseline')
    # DC.data_clean()
    file_p = 'baseline/nasa_20210603212042888030.json'
    data = []
    f = open(file_p, 'r', encoding='utf-8-sig')
    f_json = json.load(f)
    print(f_json)
    for _ in f_json:
        if len(str(_['内容'])) > 20 and len(str(_['标题']).replace(" ", "")) > 0:
            if _ not in data:
                _['内容'].replace("\n"," ")
                data.append(_)
    f.close()
    # os.remove(file)
    with open(file_p, "w", newline="", encoding="utf-8-sig") as f:
        json.dump(data, f, indent=4, ensure_ascii=False)
