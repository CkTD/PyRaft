import socket
import random
import pickle
import struct
import errno
import os
import logging

logging.basicConfig(
        level=logging.DEBUG,
        # filename='client.log',
        format="%(asctime)s %(levelname)-7s %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S")

def geterror(err):
    if isinstance(err, int):
        no = err
    elif hasattr(err, "errno"):
        no = err.errno
    if not no:
        return str(err)
    else:
        return " ".join((errno.errorcode[no], "["+str(no)+"]",os.strerror(no)))

class RaftClient():
    def __init__(self, nodes, max_retry = 3, time_out = 5):
        self._nodes = list(nodes)
        self._leader = None
        self._max_retry = max_retry
        self._time_out = time_out

    def _get_addr(self):
        if self._leader is not None:
            leader =  self._leader
        else:
            leader =  self._nodes[random.randrange(0, len(self._nodes))]

        host, port = leader.split(":")
        return (host,int(port))

    def _send(self, sock ,message):
        data = pickle.dumps(message)
        length = struct.pack("i", len(data))
        data = length + data

        sock.send(data)

    def _recv(self, sock):
        buf = bytes()
        while True:
            buf += sock.recv(4096)
            if len(buf) > 4:
                length = struct.unpack("i", buf[:4])[0]
                if len(buf) - 4 < length:
                    continue
                data = buf[4: 4 + length]
                message = pickle.loads(data)
                return message
        
    def _request(self, command, randid, node_addr):
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(self._time_out)

        sock.connect(node_addr)
        
        message = "client"
        self._send(sock, message)

        message = {
            'type': 'client_request',
            'command': (randid, command)
        }
        self._send(sock, message)

        response =  self._recv(sock)
        return response

    def request(self, command):
        randid = random.randint(1,10000000000)

        for _ in range(self._max_retry):
            try:
                node_addr = self._get_addr()
                response = self._request(command, randid, node_addr)
            except socket.timeout as e:
                logging.debug('id: [%s], [%d] times try, [%s], failed, [%s]' % (randid, _ + 1 , str(node_addr), str(e)))
            except OSError as e:
                if e.errno not in (errno.ECONNREFUSED, errno.ECONNRESET, errno.EPIPE, errno.ENOTCONN):
                    raise e
                logging.debug('id: [%s], [%d] times try, [%s], failed, [%s]' % (randid, _ + 1 , str(node_addr), geterror(e)))
            else:
                if response['command_id'] == randid:
                    if response['success'] == True:
                        logging.debug('id: [%s], [%d] times try, [%s], success' % (randid, _ + 1, str(node_addr)))
                        return True
                    else:
                        logging.debug('id: [%s], [%d] times try, [%s], redirect to [%s]' % (randid, _ + 1 , str(node_addr),response['redirect']))
                        self._leader = response['redirect']
        return False
