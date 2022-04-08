from hashlib import md5
import time


def rowkey_id_gen():
    return md5(str(time.time()).encode()).hexdigest()