import sys

sys.path.append("../")
from pyraft.client import RaftClient

localhost = "127.0.0.1"
node_ports = sys.argv[1:]
node_addrs = [":".join((localhost, node_port)) for node_port in node_ports]
client = RaftClient(node_addrs)

res = client.request({
    'type': "What To Do?",
    'args': "arguements"
})
print(res)