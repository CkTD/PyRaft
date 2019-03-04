import time
import socket
import struct
import pickle
import logging
import os
import errno

from .eventloop import EventLoop, EVENT_TYPE

logger = logging.getLogger('raft.network')


def geterror(err):
    if isinstance(err, int):
        no = err
    elif hasattr(err, "errno"):
        no = err.errno
    else:
        return ""
    return " ".join((errno.errorcode[no], os.strerror(no)))

####################################################
#########   TcpConnection   ########################
####################################################
class TCP_CONNECTION_STATE:
    DISCONNECTED = 0
    CONNECTING = 1
    CONNECTED = 2


class TcpConnection():
    """ non blocking Tcp connection """
    def __init__(self, eventloop, socket=None, 
                 send_buffer_size=2 ** 13, 
                 recv_buffer_size=2 ** 13):
        self._eventloop = eventloop
        self._socket = socket
        self._remote = None
        self._fileno = None
        self._state = TCP_CONNECTION_STATE.DISCONNECTED
        self._read_buffer = None
        self._write_buffer = None
        self._last_active = 0

        if self._socket is not None:
            self._socket = socket
            self._remote = socket.getpeername()
            self._state = TCP_CONNECTION_STATE.CONNECTED
            self._fileno = self._socket.fileno()
            self._read_buffer = bytes()
            self._write_buffer = bytes()
            self._last_active = time.time()

            self._eventloop.register_file_event(
                self._fileno, EVENT_TYPE.READ | EVENT_TYPE.ERROR, 
                self._process_event)

        self._send_buffer_size = send_buffer_size
        self._recv_buffer_size = recv_buffer_size
        self._on_connected_transport = None
        self._on_disconnected_transport = None
        self._on_message_received_transport = None

    @property
    def state(self):
        return self._state

    @property
    def remote_addr(self):
        return self._remote

    def set_on_connected(self, callback):
        self._on_connected_transport = callback

    def set_on_message_received(self, callback):
        self._on_message_received_transport = callback

    def set_on_disconnected(self, callback):
        self._on_disconnected_transport = callback

    def connect(self, host, port):
        assert self._state is TCP_CONNECTION_STATE.DISCONNECTED

        self._remote = (host, int(port))

        logger.debug("TcpConnection: try to connect nonblocking [%s]"
            % ":".join((host, str(port))))

        self._socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._socket.setsockopt(
            socket.SOL_SOCKET, socket.SO_SNDBUF, self._send_buffer_size
        )
        self._socket.setsockopt(
            socket.SOL_SOCKET, socket.SO_RCVBUF, self._recv_buffer_size
        )
        self._socket.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        self._socket.setblocking(0)
        self._read_buffer = bytes()
        self._write_buffer = bytes()
        self._last_active = time.time()
        try:
            self._socket.connect((host, port))
        except socket.error as e:
            if e.errno != socket.errno.EINPROGRESS:
                logger.error("  TcpConnection: [%s]" % geterror(e))
                return False
            #logger.debug("  TcpConnection: [%s]" % geterror(e))
        self._fileno = self._socket.fileno()
        self._state = TCP_CONNECTION_STATE.CONNECTING
        self._eventloop.register_file_event(
            self._fileno, EVENT_TYPE.WRITE, self._on_connect_done)
        return True

    def _on_connect_done(self, fd, event_mask):
        if self._state == TCP_CONNECTION_STATE.DISCONNECTED:  
            # while connecting to peer, new connection(by peer) established. self.destory already be called. 
            # just return and self will be recycled after eventloop unrefer to self.
            return

        assert event_mask & EVENT_TYPE.WRITE

        err = self._socket.getsockopt(socket.SOL_SOCKET, socket.SO_ERROR)
        if err != 0:
            logger.debug("TcpConnection: failed connect to [%s],  [%s]"
                % (self.remote_addr, geterror(err)))
            self.disconnect()
            return

        logger.debug("TcpConnection: successfully connect to [%s],  [%s]")

        self._state = TCP_CONNECTION_STATE.CONNECTED
        self._last_active = time.time()
        self._eventloop.unregister_file_event(self._fileno)
        event_mask = EVENT_TYPE.READ | EVENT_TYPE.ERROR
        if self._write_buffer:
            event_mask |= EVENT_TYPE.WRITE

        self._eventloop.register_file_event(self._fileno, event_mask, self._process_event)

        if self._on_connected_transport is not None:
            self._on_connected_transport(self)

    def disconnect(self):
        if self._state == TCP_CONNECTION_STATE.CONNECTED:
            logger.debug("TcpConnection: disconnected [%s]", str(self.remote_addr))
            if self._on_disconnected_transport is not None:
                self._on_disconnected_transport(self)

        if self._socket is not None:
            self._socket.close()
            self._socket = None
        if self._fileno is not None:
            self._eventloop.unregister_file_event(self._fileno)
            self._fileno = None

        self.set_on_connected(None)
        self.set_on_disconnected(None)
        self.set_on_message_received(None)

        self._read_buffer = None
        self._write_buffer = None
        self._state = TCP_CONNECTION_STATE.DISCONNECTED

    def _process_event(self, fd, event_mask):
        if event_mask & EVENT_TYPE.ERROR:
            err = self._socket.getsockopt(socket.SOL_SOCKET, socket.SO_ERROR)
            if err != 0:
                logger.debug("TcpConnection: socket error [%s],  [%s]"
                    % (self.remote_addr, geterror(err)))
            self.disconnect()
            return

        if event_mask & EVENT_TYPE.READ:
            self._do_read()
            if self._state == TCP_CONNECTION_STATE.DISCONNECTED:
                return

            messages = self._parse_messages()
            if messages is not None:
                if self._on_message_received_transport is not None:
                    #logger.debug("TcpConnection: Message recved: [%s] [%s]"
                    #    % (str(self.remote_addr), str(messages)))
                    for message in messages:
                        self._on_message_received_transport(self, message)

        if event_mask & EVENT_TYPE.WRITE:
            self._do_write()
            if self._state == TCP_CONNECTION_STATE.DISCONNECTED:
                return

            event_mask = EVENT_TYPE.READ | EVENT_TYPE.ERROR
            if self._write_buffer:
                event_mask |= EVENT_TYPE.WRITE
            self._eventloop.modify(self._fileno, event_mask)

    def _do_read(self):
        while True:
            try:
                data = self._socket.recv(self._recv_buffer_size)
            except socket.error as e:
                if e.errno not in (socket.errno.EAGAIN, socket.errno.EWOULDBLOCK):
                    self.disconnect()
                return
            if not data:
                self.disconnect()
                return
            self._read_buffer += data

    def _do_write(self):
        while True:
            if not self._write_buffer:
                break
            try:
                res = self._socket.send(self._write_buffer)
                if res < 0:
                    self.disconnect()
                    return
                self._write_buffer = self._write_buffer[res:]
            except socket.error as e:
                if e.errno not in (socket.errno.EAGAIN, socket.errno.EWOULDBLOCK):
                    print(e)
                    self.disconnect()
                return

    def send(self, message):
        data = pickle.dumps(message)
        length = struct.pack("i", len(data))
        data = length + data
        self._write_buffer += data
        self._do_write()

    def _parse_messages(self):
        messages = []
        while True:
            if len(self._read_buffer) < 4:
                break
            length = struct.unpack("i", self._read_buffer[:4])[0]
            if len(self._read_buffer) - 4 < length:
                break
            data = self._read_buffer[4: 4 + length]
            message = pickle.loads(data)
            messages.append(message)
            self._read_buffer = self._read_buffer[4 + length:]

        return messages

    def destory(self):
        self.disconnect()
        self.set_on_connected(None)
        self.set_on_disconnected(None)
        self.set_on_message_received(None)

    def __del__(self):
        logger.debug("TcpConnection: DESTORIED: [%s]" % str(self.remote_addr))


####################################################
#########   TcpServer    ###########################
####################################################
class TCP_SERVER_STATE:
    UNBINDED = 0
    BINDED = 1


class TcpServer():
    """ non blocking Tcp Server """
    def __init__(self, host, port,
        eventloop, on_connected_transport,
        send_buffer_size=2 ** 13,
        recv_buffer_size=2 ** 13):
        self._host = host
        self._port = port
        self._eventloop = eventloop
        self._send_buffer_size = send_buffer_size
        self._recv_buffer_size = recv_buffer_size
        self._socket = None
        self._fileno = None
        self._state = TCP_SERVER_STATE.UNBINDED
        self._on_connected_transport = on_connected_transport

        self._bind()

    def _bind(self):
        self._socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._socket.setsockopt(
            socket.SOL_SOCKET, socket.SO_SNDBUF, self._send_buffer_size
        )
        self._socket.setsockopt(
            socket.SOL_SOCKET, socket.SO_RCVBUF, self._recv_buffer_size
        )
        self._socket.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        self._socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._socket.setblocking(0)
        self._socket.bind((self._host, self._port))
        self._socket.listen()
        self._fileno = self._socket.fileno()

        self._eventloop.register_file_event(
            self._fileno, EVENT_TYPE.READ | EVENT_TYPE.ERROR, self._on_connected_server
        )
        logger.info("TcpServer: bind success. listen to [%s]" %
                     ":".join((self._host, str(self._port))))

        self._state = TCP_SERVER_STATE.BINDED

    def _unbind(self):
        self._state = TCP_SERVER_STATE.UNBINDED
        if self._fileno is not None:
            self._eventloop.unregister_file_event(self._fileno)
            self._fileno = None
        if not self._socket._closed:
            self._socket.close()

    def _on_connected_server(self, fd, event_mask):
        if event_mask & EVENT_TYPE.READ:
            sock, address = self._socket.accept()
            logger.debug("TcpServer: new connection: [%s]" % str(address))
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF,
                            self._send_buffer_size)
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF,
                            self._recv_buffer_size)
            sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
            sock.setblocking(0)
            conn = TcpConnection(self._eventloop, socket=sock)
            self._on_connected_transport(conn)

    def destory(self):
        self._eventloop = None
        self._unbind()

    def __del__(self):
        logger.debug("TcpServer: DESTORIED.")


####################################################
#########    TcpTransport  #########################
####################################################
class TcpTransport():
    def __init__(self, self_node, peer_nodes, eventloop, connect_retry_interval=5):
        self._self_node = self_node
        self._peer_nodes = set() # node is "host:port" for that node's tcp server's bind address(peer's id)

        self._node_to_conn = {}  # node -> TCPconnection

        self._unknown_conn = set()

        self._last_connect_attempt = {}
        self._connect_retry_interval = connect_retry_interval
        self._server = None
        self._eventloop = eventloop
        self._eventloop.register_time_event(connect_retry_interval,self._check_peers_connection, period=connect_retry_interval)

        self._on_message_received_raft = None
        self._on_node_connected_raft = None
        self._on_node_disconnected_raft = None

        for node in peer_nodes:
            self._add_peer_node(node)

        self._create_server()

    def _conn_to_node(self, conn):
        for node in self._node_to_conn:
            if self._node_to_conn[node] == conn:
                return node
        return None

    def set_on_message_received(self, callback):
        self._on_message_received_raft = callback

    def set_on_node_connected(self, callback):
        self._on_node_connected_raft = callback

    def set_on_node_disconnected(self, callback):
        self._on_node_disconnected_raft = callback

    def _add_peer_node(self, node):
        conn = TcpConnection(self._eventloop)

        self._peer_nodes.add(node)
        self._last_connect_attempt[node] = 0
        self._node_to_conn[node] = conn

    def _check_peers_connection(self):
        logger.debug("TcpTransport: now, check connections.")
        for node in self._peer_nodes:
            if self._node_to_conn[node].state != TCP_CONNECTION_STATE.DISCONNECTED:
                continue
            if time.time() - self._last_connect_attempt[node] < self._connect_retry_interval:
                continue
            host, port = node.split(":")
            self._node_to_conn[node].set_on_connected(self._on_outgoing_connected_transport)
            self._node_to_conn[node].connect(host, int(port))

    def _on_outgoing_connected_transport(self, conn):
        logger.info("TcpTransport: node connected(out) [%s]" % self._conn_to_node(conn))
        self._on_node_connected_raft(self._conn_to_node(conn))
        conn.set_on_disconnected(self._on_disconnected_transport)
        conn.set_on_message_received(self._on_message_received_transport)
        conn.send(self._self_node)

    def _create_server(self):
        host, port = self._self_node.split(":")
        self._server = TcpServer(
            host, int(port), self._eventloop, self._on_incoming_connected_transport
        )

    def _on_incoming_connected_transport(self, conn):
        self._unknown_conn.add(conn)
        logger.debug("TcpTransport: new incoming unknown peer connection [%s]"
            % str(conn.remote_addr))
        conn.set_on_disconnected(
            self._on_disconnected_before_initial_message_transport)
        conn.set_on_message_received(self._on_initial_message_received)
        conn.set_on_connected(None)

    def _on_initial_message_received(self, conn, message):
        assert conn in self._unknown_conn
        self._unknown_conn.remove(conn)
        node = message
        if node == 'client' or node in self._peer_nodes:
            if node == 'client':
                logger.info("TcpTransport: incoming client [%s]" % str(conn.remote_addr))
            else:
                logger.debug("TcpTransport: incoming peer [%s] is [%s]" % 
                            (str(conn.remote_addr), node))
                logger.info("TcpTransport: node connected (in) [%s]" % node)
    
            conn.set_on_disconnected(self._on_disconnected_transport)
            conn.set_on_message_received(self._on_message_received_transport)
            if node == 'client':
                host, port =  conn.remote_addr
                node = ":".join((host, str(port)))
            
            if node in self._node_to_conn:
                self._node_to_conn[node].destory()
                # del self._node_to_conn[node]
            self._node_to_conn[node] = conn

            self._on_node_connected_raft(node)            
        else:
            logger.warning("TcpTransport: failed to init connction for [%s] bad initial message received." % str(conn.remote_addr))
            conn.disconnect()

    def _on_disconnected_before_initial_message_transport(self, conn):
        logger.debug(
            "TcpTransport: incoming connect abort before get intial message")
        self._unknown_conn.remove(conn)

    def _on_disconnected_transport(self, conn):
        logger.info("TcpTransport: node disconnected [%s]"%self._conn_to_node(conn))
        node = self._conn_to_node(conn)
        if node not in self._peer_nodes:
            del self._node_to_conn[node]
        self._on_node_disconnected_raft(node)

    def _on_message_received_transport(self, conn, message):
        self._on_message_received_raft(self._conn_to_node(conn), message)

    def send(self, node, message):
        if node not in self._node_to_conn or \
           self._node_to_conn[node].state != TCP_CONNECTION_STATE.CONNECTED:
            return False
        self._node_to_conn[node].send(message)
        if self._node_to_conn[node].state != TCP_CONNECTION_STATE.CONNECTED:
            return False
        return True

    def destory(self):
        self._eventloop = None
        if self._server is not None:
            self._server.destory()
            del self._server
        for node in self._peer_nodes:
            conn = self._node_to_conn[node]
            conn.destory()
            del self._node_to_conn[node]

    def __del__(self):
        logger.debug("TcpTranspot: DESTORIED.")




# To run following example for TcpTransport
# python -m pyraft.network self_port [peer_port ,...]
if __name__ == "__main__":
    import sys
    localhost = "127.0.0.1"
    self_port = sys.argv[1]
    peer_ports = sys.argv[2:]
    self_addr = ":".join((localhost, self_port))
    peer_addrs = [":".join((localhost, peer_port)) for peer_port in peer_ports]

    def on_message_received(node, message):
        print("Recv message. [%s] [%s]" % (node, str(message)))

    def on_node_connected(node):
        print("Node Connectd. [%s]" % node)

    def on_node_disconnected(node):
        print("Node Disconnectd. [%s]" % node)

    eventloop = EventLoop()
    transport = TcpTransport(self_addr, peer_addrs, eventloop)
    transport.set_on_message_received(on_message_received)
    transport.set_on_node_connected(on_node_connected)
    transport.set_on_node_disconnected(on_node_disconnected)

    eventloop.run()

