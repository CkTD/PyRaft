# -*- coding: utf-8 -*-

import sys

sys.path.append("../../")
from pyraft.client import RaftClient


localhost = "127.0.0.1"
node_ports = sys.argv[1:]
node_addrs = [":".join((localhost, node_port)) for node_port in node_ports]
client = RaftClient(node_addrs)


for i in range(10):
    message = {
        'type': "put",
        'args': {'key':bytes("key_%d"%i, 'utf-8'),
                'value': bytes("value_%d"%i, 'utf-8')}
    }
    res = client.request(message, readonly = False)
    if res is False:
        print("can't put key key_%d" % i)
    else:
        print("put key %d success." % i)


for i in range(10):
    message = {
        'type': "get",
        'args': {'key':bytes("key_%d"%i, 'utf-8')}
    }
    res = client.request(message, readonly = True)
    if res is False:
        print("can't get key key_%d" % i)
    elif res is None:
        print('key_%d not exist.' % i)
    else:
        print("get_key: key_%d=%s" % (i,str(res, 'utf-8')))