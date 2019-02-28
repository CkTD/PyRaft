import sys

sys.path.append("../")
from pyraft.client import RaftClient

localhost = "127.0.0.1"
node_ports = sys.argv[1:]
node_addrs = [":".join((localhost, node_port)) for node_port in node_ports]
client = RaftClient(node_addrs)



for _ in range(10):
    message = {
        'type': "add_counter",
        'args': [10]
    }
    res = client.request(message, readonly = False)
    if res != True:
        break
    print("add_counter success.")

    message = {
        'type': "get_counter",
        'args': None
    }
    res = client.request(message, readonly = True)
    print("get_counter: %d" % res)