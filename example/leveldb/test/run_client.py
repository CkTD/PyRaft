import threading

#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import sys
import plyvel
import time

sys.path.append("../../../")
from pyraft.client import RaftClient


localhost = "127.0.0.1"
node_ports = ["8000", "8001", "8002"]
node_addrs = [":".join((localhost, node_port)) for node_port in node_ports]


# for i in range(10):
#     message = {
#         'type': "get",
#         'args': {'key':bytes("key_%d"%i, 'utf-8')}
#     }
#     res = client.request(message, readonly = True)
#     if res is False:
#         print("can't get key key_%d" % i)
#     elif res is None:
#         print('key_%d not exist.' % i)
#     else:
#         print("get_key: key_%d=%s" % (i,str(res, 'utf-8')))

lock = threading.Lock()
raft_time = 0
def raft(begin_key, number, value_size):
    global raft_time
    client = RaftClient(node_addrs)
    i = 0
    for i in range(number):
        key = "%d" % (begin_key + i)
        value = b'0' * value_size 
        message = {
            'type': "put",
            'args': {'key':bytes(key, 'utf-8'),
                    'value': value}
        }
        st = time.time()
        res = client.request(message, readonly = False)
        et = time.time()
        lock.acquire()
        raft_time += et -st
        lock.release()
        if res is False:
            raise ValueError
            #print("can't put key [%s]" % key)
        else:
            pass
            #print("put key [%s] success." % value)
        i += 1


def fio(nums, value_size):
    
    _db = plyvel.DB("fio.db" , create_if_missing=True)
    st = time.time()
    for i in range(nums):
        _db.put(bytes(str(i), 'utf-8'),b'0'*value_size)
    et = time.time()
    print("fio:",st-et)




if __name__ == '__main__':
    total_size = 128 * 1024 * 1024
    value_size = 4 * 1024
    nums = int(total_size/value_size)

    #fio(nums, value_size)

    number_per_c = int(nums/100)
    begin = 0
    l = []
    for num in range(100):
        t = threading.Thread(target=raft, args=(begin,number_per_c,value_size))
        begin += number_per_c
        l.append(t)
    for t in l:
        t.start()
    for t in l:
        t.join()
    
    print("raft:", raft_time)

# 1k 7.01 45.09
# 2k 6.66 31.67
# 4k 5.71 12.26
# 8k 6.31 18.58