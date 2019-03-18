# -*- coding: utf-8 -*-

import sys

import plyvel

sys.path.append("../../")
from pyraft.core import RaftStateMachine

class LevelNode():

    def __init__(self, args):
        self._rsm = RaftStateMachine(args)
        self._rsm.set_on_apply_log(self._appyl_log_handler)
        self._rsm.set_on_readonly(self._readonly_handler)

        self._db = plyvel.DB("./%s" % self._rsm._self_node, create_if_missing=True)
        self._rsm.run()

    def _appyl_log_handler(self, message):
        m_type = message['type']
        m_args = message['args']
        if m_type == 'put':
            key = m_args['key']
            value = m_args['value']
            self._db.put(key, value)
        elif m_type == 'delete':
            key = m_args['key']
            self._db.delete(key)
        else:
            assert False

    def _readonly_handler(self, message):
        m_type = message['type']
        m_args = message['args']
        if m_type == 'get':
            key = m_args['key']
            return self._db.get(key)
        else:
            assert False




LevelNode(sys.argv[1:])