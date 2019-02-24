import select
import time
import socket
import struct
import pickle
import logging
import os
import errno

logging.basicConfig(
    level=logging.DEBUG,
    # filename='network.log',
    format="%(asctime)s %(levelname)-7s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)


def geterror(err):
    if isinstance(err, int):
        no = err
    elif hasattr(err, "errno"):
        no = err.errno
    else:
        return ""
    return " ".join((errno.errorcode[no], os.strerror(no)))


####################################################
#########   EventLoop       ########################
####################################################
class EVENT_TYPE:
    READ = select.EPOLLIN
    WRITE = select.EPOLLOUT
    ERROR = select.EPOLLERR


class EventLoop:
    def __init__(self, interval=5):
        self._poller = select.epoll()
        self._fd_to_handler = {}
        self._periodic_func = []
        self._last_periodic_call = time.time()
        self._periodic_interval = interval
        self._running = False

    def register(self, fd, event_mask, handler):
        self._fd_to_handler[fd] = handler
        self._poller.register(fd, event_mask)

    def unregister(self, fd):
        del self._fd_to_handler[fd]
        self._poller.unregister(fd)

    def modity(self, fd, event_mask):
        self._poller.modify(fd, event_mask)

    def register_periodic_callback(self, callback):
        self._periodic_func.append(callback)

    def unregister_periodic_callback(self, callback):
        self._periodic_func.remove(callback)

    def poll(self):
        events = self._poller.poll(self._periodic_interval)

        return [(fd, event_mask, self._fd_to_handler[fd]) for fd, event_mask in events]

    def run(self):
        self._running = True
        while self._running:
            logging.debug("EventLoop: next loop")
            fired_events = self.poll()
            for fd, event_mask, handler in fired_events:
                handler(fd, event_mask)

            now = time.time()
            if now - self._last_periodic_call >= self._periodic_interval:
                for func in self._periodic_func:
                    func()
                self._last_periodic_call = now

    def stop(self):
        self._running = False

    def destory(self):
        self.stop()
        self._poller.close()

    def __del__(self):
        logging.debug("EventLoop: DESTORIED.")


####################################################
#########   TcpConnection   ########################
####################################################
class TCP_CONNECTION_STATE:
    DISCONNECTED = 0
    CONNECTING = 1
    CONNECTED = 2


class TcpConnection:
    def __init__(
        self, eventloop, socket=None, send_buffer_size=2 ** 13, recv_buffer_size=2 ** 13
    ):
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

            self._eventloop.register(
                self._fileno, EVENT_TYPE.READ | EVENT_TYPE.ERROR, self._process_event
            )

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

        logging.debug(
            "TcpConnection: try to connect nonblocking [%s]"
            % ":".join((host, str(port)))
        )

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
            if e.errno is not socket.errno.EINPROGRESS:
                logging.warning("  TcpConnection: [%s]" % geterror(e))
                return False
            logging.debug("  TcpConnection: [%s]" % geterror(e))
        self._fileno = self._socket.fileno()
        self._state = TCP_CONNECTION_STATE.CONNECTING
        self._eventloop.register(self._fileno, EVENT_TYPE.WRITE, self._on_connect_done)
        return True

    def _on_connect_done(self, fd, event_mask):
        if (
            self._state == TCP_CONNECTION_STATE.DISCONNECTED
        ):  # connected by peer while try to connect... ``
            # self will be recycled after eventloop unrefer to this method.
            return

        assert event_mask & EVENT_TYPE.WRITE

        err = self._socket.getsockopt(socket.SOL_SOCKET, socket.SO_ERROR)
        if err != 0:
            logging.debug(
                "TcpConnection: can't connect to [%s],  [%s]"
                % (self.remote_addr, geterror(err))
            )
            self.disconnect()
            return

        if self._on_connected_transport is not None:
            self._on_connected_transport(self)
        self._state = TCP_CONNECTION_STATE.CONNECTED
        self._last_active = time.time()
        self._eventloop.unregister(self._fileno)
        event_mask = EVENT_TYPE.READ | EVENT_TYPE.ERROR
        if len(self._write_buffer) > 0:
            event_mask |= EVENT_TYPE.WRITE

        self._eventloop.register(self._fileno, event_mask, self._process_event)

    def disconnect(self):
        if self._state == TCP_CONNECTION_STATE.CONNECTING:
            logging.debug("TcpConnection: abort connect [%s]", str(self.remote_addr))
        else:
            logging.debug("TcpConnection: disconnect to [%s]", str(self.remote_addr))

        if self._state == TCP_CONNECTION_STATE.CONNECTED:
            if self._on_disconnected_transport is not None:
                self._on_disconnected_transport(self)

        if self._socket is not None:
            self._socket.close()
            self._socket = None
        if self._fileno is not None:
            self._eventloop.unregister(self._fileno)
            self._fileno = None

        self.set_on_connected(None)
        self.set_on_disconnected(None)
        self.set_on_message_received(None)

        self._read_buffer = None
        self._write_buffer = None
        self._state = TCP_CONNECTION_STATE.DISCONNECTED

    def _process_event(self, fd, event_mask):
        if event_mask & EVENT_TYPE.ERROR:
            self.disconnect()
            return

        if event_mask & EVENT_TYPE.READ:
            self._do_read()
            if self._state == TCP_CONNECTION_STATE.DISCONNECTED:
                return

            messages = self._parse_messages()
            if messages is not None:
                if self._on_message_received_transport is not None:
                    logging.debug(
                        "TcpConnection: Message recved: [%s] [%s]"
                        % (str(self.remote_addr), str(messages))
                    )
                    self._on_message_received_transport(self, messages)

        if event_mask & EVENT_TYPE.WRITE:
            self._do_write()
            if self._state == TCP_CONNECTION_STATE.DISCONNECTED:
                return

            event_mask = EVENT_TYPE.READ | EVENT_TYPE.ERROR
            if len(self._write_buffer) > 0:
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
            if len(self._write_buffer) == 0:
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
            data = self._read_buffer[4 : 4 + length]
            message = pickle.loads(data)
            messages.append(message)
            self._read_buffer = self._read_buffer[4 + length :]

        return messages

    def destory(self):
        self.disconnect()
        self.set_on_connected(None)
        self.set_on_disconnected(None)
        self.set_on_message_received(None)

    def __del__(self):
        logging.debug("TcpConnection: DESTORIED: [%s]" % str(self.remote_addr))


####################################################
#########   TcpServer    ###########################
####################################################
class TCP_SERVER_STATE:
    UNBINDED = 0
    BINDED = 1


class TcpServer:
    def __init__(
        self,
        host,
        port,
        eventloop,
        on_connected_transport,
        send_buffer_size=2 ** 13,
        recv_buffer_size=2 ** 13,
    ):
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

        self._eventloop.register(
            self._fileno, EVENT_TYPE.READ | EVENT_TYPE.ERROR, self._on_connected_server
        )
        logging.info(
            "TcpServer: bind success. listen to "
            + ":".join((self._host, str(self._port)))
        )

        self._state = TCP_SERVER_STATE.BINDED

    def _unbind(self):
        self._state = TCP_SERVER_STATE.UNBINDED
        if self._fileno is not None:
            self._eventloop.unregister(self._fileno)
            self._fileno = None
        if not self._socket._closed:
            self._socket.close()

    def _on_connected_server(self, fd, event_mask):
        if event_mask & EVENT_TYPE.READ:
            sock, address = self._socket.accept()
            logging.debug("TcpServer: new connection: [%s]" % str(address))
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, self._send_buffer_size)
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, self._recv_buffer_size)
            sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
            sock.setblocking(0)
            conn = TcpConnection(self._eventloop, socket=sock)
            self._on_connected_transport(conn)

    def destory(self):
        self._eventloop = None
        self._unbind()

    def __del__(self):
        logging.debug("TcpServer: DESTORIED.")


####################################################
#########    TcpTransport  #########################
####################################################
class TcpTransport:
    def __init__(self, self_node, peer_nodes, eventloop, connect_retry_interval=5):
        self._self_node = self_node
        self._peer_nodes = (
            set()
        )  # node is "host:port" for that node's tcp server's bind address(peer's id)

        self._node_to_conn = {}  # node -> TCPconnection

        self._unknown_conn = set()

        self._last_connect_attempt = {}
        self._connect_retry_interval = connect_retry_interval
        self._server = None
        self._eventloop = eventloop
        self._eventloop.register_periodic_callback(self._on_tick)

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

    def _on_tick(self):
        logging.debug("TcpTransport: on tick, check connections.")
        self._check_peers_connection()

    def _check_peers_connection(self):
        for node in self._peer_nodes:
            if self._node_to_conn[node].state is not TCP_CONNECTION_STATE.DISCONNECTED:
                continue
            if (
                time.time() - self._last_connect_attempt[node]
                < self._connect_retry_interval
            ):
                continue
            host, port = node.split(":")
            self._node_to_conn[node].set_on_connected(
                self._on_outgoing_connected_transport
            )
            self._node_to_conn[node].connect(host, int(port))

    def _on_outgoing_connected_transport(self, conn):
        logging.info(
            "TcpTransport:  outgoing connection success to node [%s]"
            % self._conn_to_node(conn)
        )
        self._on_node_connected_raft(self._conn_to_node(conn))
        conn.set_on_disconnected(self._on_disconnected_transport)
        conn.set_on_message_received(self._on_message_received_transport)
        conn.send(self._self_node)

    def _create_server(self):
        host, port = self._self_node.split(":")
        self._server = TcpServer(
            host, int(port), eventloop, self._on_incoming_connected_transport
        )

    def _on_incoming_connected_transport(self, conn):
        self._unknown_conn.add(conn)
        logging.info(
            "TcpTransport: new incoming unknown peer connection [%s]"
            % str(conn.remote_addr)
        )
        conn.set_on_disconnected(self._on_disconnected_before_initial_message_transport)
        conn.set_on_message_received(self._on_initial_message_received)
        conn.set_on_connected(None)

    def _on_initial_message_received(self, conn, message):
        assert conn in self._unknown_conn
        node = message[0]
        if node not in self._peer_nodes:
            logging.info(
                "TcpTransport: incoming peer connection [%s] is not in the configured peer nodes"
                % node
            )
            self._unknown_conn.remove(conn)
            conn.destory()
        else:
            logging.info(
                "TcpTransport: incoming peer [%s] is [%s]" % (conn.remote_addr, node)
            )
            conn.set_on_disconnected(self._on_disconnected_transport)
            conn.set_on_message_received(self._on_message_received_transport)

            self._node_to_conn[node].destory()
            # del self._node_to_conn[node]

            self._node_to_conn[node] = conn
            self._unknown_conn.remove(conn)
            self._on_node_connected_raft(node)
            if len(message) > 1:
                self._on_message_received_transport(conn, message[1:])

    def _on_disconnected_before_initial_message_transport(self, conn):
        logging.debug("TcpTransport: incoming connect abort before get intial message")
        self._unknown_conn.remove(conn)

    def _on_disconnected_transport(self, conn):
        self._on_node_disconnected_raft(self._conn_to_node(conn))

    def _on_message_received_transport(self, conn, message):
        self._on_message_received_raft(self._conn_to_node(conn), message)

    def send(self, node, message):
        if (
            node not in self._node_to_conn
            or self._node_to_conn[node].state is not TCP_CONNECTION_STATE.CONNECTED
        ):
            return False
        self._node_to_conn[node].send(message)
        if self._node_to_conn[node].state is not TCP_CONNECTION_STATE.CONNECTED:
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
        logging.debug("TcpTranspot: DESTORIED.")


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

