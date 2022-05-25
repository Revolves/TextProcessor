from hashlib import md5
import time


def rowkey_id_gen():
    return md5(str(time.time()).encode()).hexdigest()

def crawling(crawl_id, connect, status, keywords = None):
    """
    插入爬虫运行状态
    :param :
    
    :return :
    """
    sql_ = """
            BEGIN
            IF NOT EXISTS (SELECT * FROM hsold.text_crawl_finish WHERE crawl_id = "{}") THEN
                INSERT INTO hsold.text_crawl_finish (rowkey_id, crawl_id, status, keywords) VALUES (?, ?, ?, ?)
            ELSE 
                UPDATE hsold.text_crawl_finish SET status = "{}" WHERE crawl_id = "{}"
            END IF
            END
            """.format(crawl_id, status, crawl_id)
    if keywords:
        pram_ = [rowkey_id_gen(), crawl_id, status, keywords]
    else:
        pram_ = []
    connect.execute_sql(sql_, pram_)