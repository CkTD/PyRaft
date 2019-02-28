import sys

sys.path.append("../")
from pyraft.core import RaftStateMachine


localhost = "127.0.0.1"
self_port = sys.argv[1]
peer_ports = sys.argv[2:]
self_addr = ":".join((localhost, self_port))
peer_addrs = [":".join((localhost, peer_port)) for peer_port in peer_ports]



i = 0

def appyl_log_handler(message):
    global i
    m_type = message['type']
    m_args = message['args']
    if m_type == 'add_counter':
        i += m_args[0]
        print("add counter by %d." % m_args[0])
    else:
        print("not supported type %s" % m_type)

def readonly_handler(message):
    global i
    m_type = message['type']
    m_args = message['args']
    if m_type == 'get_counter':
        return i
    else:
        print("not supported type %s" % m_type)

rsm = RaftStateMachine(self_addr, peer_addrs)
rsm.set_on_apply_log(appyl_log_handler)
rsm.set_on_readonly(readonly_handler)

rsm.run()