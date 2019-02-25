from .network import TcpTransport
from .network import EventLoop, EVENT_TYPE


class RaftStateMachine():
    def __init__(self, self_node, peer_nodes):
        self._self_node = self_node
        self._peer_nodes = peer_nodes

        self._eventloop = EventLoop()
        self._transport = TcpTransport(self_node,peer_nodes)