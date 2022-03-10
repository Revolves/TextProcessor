"""
user: Jhao
date: 2021/10/19
time: 16:26
IDE: PyCharm  
"""
# coding: utf-8


# 基于分词的文本相似度的计算,
# 利用jieba分词进行中文分析
import json
import os

import jieba
import jieba.posseg as pseg
import numpy as np
from jieba import analyse

from MultisiteSchedule import rowkey_id_gen
from Transwarp import Transwarp

'''
文本相似度的计算，基于几种常见的算法的实现
'''


def cos_sim(a, b):
    """
    余弦相似度

    :param a:
    :param b:
    :return:
    """
    a = np.array(a)
    b = np.array(b)

    return np.sum(a * b) / (np.sqrt(np.sum(a ** 2)) * np.sqrt(np.sum(b ** 2)))


def eucl_sim(a, b):
    """
    欧几里得相似度

    :param a:
    :param b:
    :return:
    """
    a = np.array(a)
    b = np.array(b)
    return 1 / (1 + np.sqrt((np.sum(a - b) ** 2)))


def pers_sim(a, b):
    """
    皮尔森相似度

    :param a:
    :param b:
    :return:
    """
    a = np.array(a)
    b = np.array(b)

    a = a - np.average(a)
    b = b - np.average(b)

    # print(a,b)
    # return {"文本的皮尔森相似度:":np.sum(a*b) / (np.sqrt(np.sum(a ** 2)) * np.sqrt(np.sum(b ** 2)))}
    return np.sum(a * b) / (np.sqrt(np.sum(a ** 2)) * np.sqrt(np.sum(b ** 2)))

class TextSimilarity:
    def __init__(self, file_a, file_b='baseline'):
        """
        加载目标文件为字符串

        :param file_a: 输入数据集
        :param file_b: 基准集
        """
        str_a = ''
        str_b = ''
        # if not os.path.isfile(file_a) and not os.path.isdir(file_a):
        #     print(file_a, "is not file")
        #     return
        # elif not os.path.isdir(file_b) and not os.path.isfile(file_b):
        #     print(file_b, "is not dir")
        #     return
        # else:
        try:
            if type([]) == type(file_a):
                for fname in file_a:
                    if os.stat('./baseline/' + fname).st_size < 10:
                        continue
                    with open('./baseline/' + fname, 'r', encoding='utf-8-sig') as f:
                        load_json = json.load(f)
                        for _ in load_json:
                            str_a += _['content']
            elif not os.path.isdir(file_a):
                with open(file_a, 'r',encoding='utf-8-sig') as f:
                    load_json = json.load(f)
                    for _ in load_json:
                        str_a += _['content']
            
            else:
                for dirfile in os.walk(file_a):
                    for fname in dirfile[2]:
                        if os.stat(dirfile[0] + '/' + fname).st_size < 10:
                            continue
                        with open(dirfile[0] + '/' + fname, 'r', encoding='utf-8-sig') as f:
                            load_json = json.load(f)
                            for _ in load_json:
                                str_a += _['content']
            if not os.path.isdir(file_b):
                with open(file_b, 'r', encoding='utf-8-sig') as f:
                    load_json = json.load(f)
                    for _ in load_json:
                        str_b += _['content']
            else:
                for dirfile in os.walk(file_b):
                    for fname in dirfile[2]:
                        if os.stat(dirfile[0] + '/' + fname).st_size < 10 or not (dirfile[0] + '/' + fname).endswith('json'):
                            continue
                        with open(dirfile[0] + '/' + fname, 'r', encoding='utf-8-sig') as f:
                            load_json = json.load(f)
                            for _ in load_json:
                                str_b += _['content']
        except Exception as e:
            print(e)
            pass
        self.str_a = str_a
        self.str_b = str_b

    def minimumEditDistance(self):
        """
        最小编辑距离，只有三种操作方式 替换、插入、删除
        """
        lensum = float(len(self.str_a) + len(self.str_b))
        if len(self.str_a) > len(self.str_b):  # 得到最短长度的字符串
            str_a, str_b = self.str_b, self.str_a
        distances = range(len(str_a) + 1)  # 设置默认值
        for index2, char2 in enumerate(str_b):  # self.str_b > self.str_a
            newDistances = [index2 + 1]  # 设置新的距离，用来标记
            for index1, char1 in enumerate(str_a):
                if char1 == char2:  # 如果相等，证明在下标index1出不用进行操作变换，最小距离跟前一个保持不变，
                    newDistances.append(distances[index1])
                else:  # 得到最小的变化数，
                    newDistances.append(1 + min((distances[index1],  # 删除
                                                 distances[index1 + 1],  # 插入
                                                 newDistances[-1])))  # 变换
            distances = newDistances  # 更新最小编辑距离

        mindist = distances[-1]
        ratio = (lensum - mindist) / lensum
        # return {'distance':mindist, 'ratio':ratio}
        return ratio

    def levenshteinDistance(self):
        '''
        编辑距离——莱文斯坦距离,计算文本的相似度
        '''
        m = len(self.str_a)
        n = len(self.str_b)
        lensum = float(m + n)
        d = []
        for i in range(m + 1):
            d.append([i])
        del d[0][0]
        for j in range(n + 1):
            d[0].append(j)
        for j in range(1, n + 1):
            for i in range(1, m + 1):
                if self.str_a[i - 1] == self.str_b[j - 1]:
                    d[i].insert(j, d[i - 1][j - 1])
                else:
                    minimum = min(d[i - 1][j] + 1, d[i][j - 1] + 1, d[i - 1][j - 1] + 2)
                    d[i].insert(j, minimum)
        ldist = d[-1][-1]
        ratio = (lensum - ldist) / lensum
        # return {'distance':ldist, 'ratio':ratio}
        return ratio

    @classmethod
    def splitWords(self, str_a):
        '''
        接受一个字符串作为参数，返回分词后的结果字符串(空格隔开)和集合类型
        '''
        wordsa = pseg.cut(str_a)
        cuta = ""
        seta = set()
        for key in wordsa:
            # print(key.word,key.flag)
            cuta += key.word + " "
            seta.add(key.word)

        return [cuta, seta]

    def JaccardSim(self):
        '''
        Jaccard相似性系数
        计算sa和sb的相似度 len（sa & sb）/ len（sa | sb）
        '''
        seta = self.splitWords(self.str_a)[1]
        setb = self.splitWords(self.str_b)[1]

        sa_sb = 1.0 * len(seta & setb) / len(seta | setb)

        return sa_sb

    def countIDF(self, text, topK):
        """
        text:字符串，topK根据TF-IDF得到前topk个关键词的词频，用于计算相似度
        return 词频vector
        """
        tfidf = analyse.extract_tags

        cipin = {}  # 统计分词后的词频

        fenci = jieba.cut(text)

        # 记录每个词频的频率
        for word in fenci:
            if word not in cipin.keys():
                cipin[word] = 0
            cipin[word] += 1

        # 基于tfidf算法抽取topK个关键词，包含每个词项的权重
        keywords = tfidf(text, topK, withWeight=True)

        ans = []
        # keywords.count(keyword)得到keyword的词频
        # help(tfidf)
        # 输出抽取出的关键词
        for keyword in keywords:
            ans.append(cipin[keyword[0]])  # 得到前topk频繁词项的词频
        return ans

    def splitWordSimlaryty(self, topK=40, sim = pers_sim):
        '''
        基于分词求相似度，默认使用cos_sim 余弦相似度,默认使用前40个最频繁词项进行计算
        '''
        # 得到前topK个最频繁词项的字频向量
        vec_a = self.countIDF(self.str_a, topK)
        vec_b = self.countIDF(self.str_b, topK)

        return sim(vec_a, vec_b)#self.cos_sim(vec_a, vec_b)

    @staticmethod
    def string_hash(self, source):  # 局部哈希算法的实现
        if source == "":
            return 0
        else:
            # ord()函数 return 字符的Unicode数值
            x = ord(source[0]) << 7
            m = 1000003  # 设置一个大的素数
            mask = 2 ** 128 - 1  # key值
            for c in source:  # 对每一个字符基于前面计算hash
                x = ((x * m) ^ ord(c)) & mask

            x ^= len(source)  #
            if x == -1:  # 证明超过精度
                x = -2
            x = bin(x).replace('0b', '').zfill(64)[-64:]
            # print(source,x)

        return str(x)

    def simhash(self, str_a, str_b):
        '''
        使用simhash计算相似度
        '''
        pass

if __name__ == '__main__':
    ts = TextSimilarity(r"./baseline/baseline_aiaa_1.json", r"result")
    print("ts.splitWordSimlaryty:{}".format(ts.splitWordSimlaryty(sim=cos_sim)))
    print("ts.JaccardSim: {}".format(ts.JaccardSim()))
    # print("ts.levenshteinDistance:{}".format(ts.levenshteinDistance()))
    # print("ts.minimumEditDistance:{}".format(ts.minimumEditDistance()))
    