import sys

sys.path.append("../")
from pyraft.core import RaftStateMachine


localhost = "127.0.0.1"
self_port = sys.argv[1]
peer_ports = sys.argv[2:]
self_addr = ":".join((localhost, self_port))
peer_addrs = [":".join((localhost, peer_port)) for peer_port in peer_ports]



def on_apply_log(command):
    print("apply log: ", str(command))

rsm = RaftStateMachine(self_addr, peer_addrs)
rsm.set_on_apply_log(on_apply_log)
rsm.run()