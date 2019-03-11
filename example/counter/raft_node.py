# -*- coding: utf-8 -*-

import sys

sys.path.append("../../")
from pyraft.core import RaftStateMachine

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

rsm = RaftStateMachine(sys.argv[1:])
rsm.set_on_apply_log(appyl_log_handler)
rsm.set_on_readonly(readonly_handler)

rsm.run()