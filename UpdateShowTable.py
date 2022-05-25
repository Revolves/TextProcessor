from hashlib import md5
import os
from threading import Timer
import time

from Transwarp import Transwarp


# 网站名字转换列表
siteTransfer = {
    'aiaa': 'AIAA',
    'baidu': '百度百科',
    'wiki': '维基百科',
    'tiexue': '铁血论坛',
    'twitter': 'Twitter',
    'facebook': 'Facebook',
    'nasa': 'NASA',
    'janes': 'Janes',
}

def rowkey_id_gen():
    return md5(str(time.time()).encode()).hexdigest()

def updateShowTextStatus(sites_count_list, connect):
    """
    更新hsold.show_text_status\\

    """
    connect.connect_inceptor()
    for site in sites_count_list:
        sql_ = 'UPDATE hsold.show_text_status SET count = count + \"{}\" WHERE site = \"{}\"'.format(
            sites_count_list[site], site)
        connect.execute_sql(sql_, [])
    connect.close_inceptor()
    pass


def updateShowTextKeyword(connect):
    """
    更新hsold.show_text_keyword\\
    获取hsold.text_crawl_stats表中关键词数量top-5，插入hsold.show_text_keyword

    :param connect : 数据库连接
    """
    connect.connect_inceptor()
    sql_ = 'DELETE FROM hsold.show_text_keyword;INSERT INTO hsold.show_text_keyword (keyword, count) SELECT keywords, count(count) AS sum FROM hsold.text_crawl_stats GROUP BY keywords ORDER BY sum DESC LIMIT 5'
    connect.execute_sql(sql_, [])
    connect.close_inceptor()
    pass


def updateShowHttpInteract(connect):
    """
    更新hsold.show_http_interact表

    从hsold.text_crawl_http_interact获取get_id最大10个值覆盖插入hsold.show_http_interact
    """
    connect.connect_inceptor()
    sql_ = 'SELECT ten_sceonds_interact FROM (SELECT ten_sceonds_interact, get_id FROM hsold.text_crawl_http_interact ORDER BY get_id DESC  LIMIT 10 ) ORDER BY get_id'
    result = connect.execute_sql(sql_, [])
    # print(result)
    for i in range(10):
        print(result[i])
        sql_ = 'UPDATE hsold.show_text_interact SET http_interact = {} WHERE id = \"{}\"'.format(result[i], i)
        connect.execute_sql(sql_, [])
    connect.connect_inceptor()
    pass


def updateTextCrawlStatus(keywords_count_list, connect):
    """
    更新hsold.text_crawl_status

    :param keywords_count_list: 爬取数量列表(按关键词)
    :param crawl_id :爬虫id
    :param connect : 数据库连接
    """
    connect.connect_inceptor()
    for crawl_id in keywords_count_list:
        for keyword in keywords_count_list[crawl_id]:
            count = str(keywords_count_list[crawl_id][keyword])
            sql_ = """
            BEGIN
            IF NOT EXISTS (SELECT * FROM hsold.text_crawl_stats WHERE keywords = "{}" AND crawlerid = "{}") THEN
                INSERT INTO hsold.text_crawl_stats (rowkey_id, crawlerid, keywords, count) VALUES (?, ?, ?, ?)
            ELSE 
                UPDATE hsold.text_crawl_stats SET count = count + "{}" WHERE keywords = "{}" AND crawlerid = "{}"
            END IF
            END
            """.format(keyword, crawl_id, count, keyword, crawl_id)
            param_ = [rowkey_id_gen(), crawl_id, keyword, count]
            # getCrawlStatus()
            connect.execute_sql(sql_, param_)
    connect.close_inceptor()


def getCrawlStatus():
    """
    更新hsold.show_text_status表 和 hsold.text_crawl_status

    扫描文件夹是否有count变化,读取count目录下文件获取当前保存关键词爬取内容数据\\
    count目录命名规则 {时间戳}_{crawl_id}_{site_name}_{keyword} \\
    从关键词和网络两个角度保存爬取数量

    return:
        keywords_count_list: 按关键字统计爬取数量
        sites_count_list： 按网站统计爬取数量

    """
    count_path = (r'./result/count/')
    if os.path.isdir(count_path) is False:
        return {}, {}
    last_site_count_list = {'aiaa': [], 'baidu': [], 'wiki': [], 'tiexue': [], 'twitter': [], 'facebook': [],
                            'nasa': [], 'janes': []}
    site_file_list = {}  # 按网站分类文件
    crawl_id_list = []
    for file in os.listdir(r'./result/count/'):
        if file.split('_')[2] in site_file_list:
            site_file_list[file.split('_')[2]].append(file)
        else:
            site_file_list[file.split('_')[2]] = [file]
    keywords_count_list = {}  # 按关键词分类获取数量
    sites_count_list = {}  # 按网站分获取数量
    for site in site_file_list:
        countAdd = 0
        # 遍历文件列表
        for file in site_file_list[site]:
            # 跳过空文件
            if file.split('_')[1] not in keywords_count_list:
                keywords_count_list[file.split('_')[1]] = {}
            if os.stat(count_path + file).st_size > 0:
                # 获取文件内容
                with open(count_path + file, 'r') as f:
                    count = int(f.read())
                    if site in sites_count_list:
                        sites_count_list[site] += count
                    else:
                        sites_count_list[site] = count
                    # 按关键词记录数量
                    if file.split('_')[-1] in keywords_count_list[file.split('_')[1]]:
                        keywords_count_list[file.split('_')[1]][file.split('_')[-1]] += count
                    else:
                        keywords_count_list[file.split('_')[1]][file.split('_')[-1]] = count
                os.remove(count_path + file)

    return keywords_count_list, sites_count_list


def run(connect):
    keywords_count_list, sites_count_list = getCrawlStatus()
    if keywords_count_list != {}:
        # print(keywords_count_list, sites_count_list)
        updateTextCrawlStatus(keywords_count_list, connect)
        updateShowHttpInteract(connect)
        updateShowTextKeyword(connect)
        updateShowTextStatus(sites_count_list, connect)
    loop_monitor(connect)


def loop_monitor(connect):
    t = Timer(30, run, (connect,))
    t.start()
    return t


if __name__ == "__main__":
    transwarp = Transwarp("Transwarp/JavaJar/InceptorUtil.jar", "Transwarp/libs")
    t = loop_monitor(transwarp)
    # keywords_count_list, sites_count_list = getCrawlStatus()
    # print(keywords_count_list, sites_count_list)
