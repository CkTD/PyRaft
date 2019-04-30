# -*- coding: utf-8 -*-

import logging
import sys
import getopt

def help_exit():
    help_str = """ARGS
    [OPTION...] SELF PEERS...
    
    SELF and PEERS can be HOST:PORT or only PORT (use default host 127.0.0.1)

OPTION
  raft core
    election_timeout_min      5000    in milliseconds
    election_timeout_max      10000   in milliseconds
    heartbeat_timeout_min     2000    in milliseconds
    heartbeat_timeout_max     4000    in milliseconds
    max_log_entries_per_call  10
    raft_log_file             NONE
  network
    connect_retry_interval    5       in seconds
    send_buffer_size          2**13   in bytes
    recv_buffer_size          2**13   in bytes
  log
    log_file                  
    log_level_core            INFO
    log_level_network         INFO"""
    print(help_str)
    sys.exit(1)


def get_config(args):
    """
    args:  [OPTION...] SELF PEERS...
    SELF and PEERS can be HOST:PORT or PORT
    """
    shortopts = 'h'
    longopts = ['election_timeout_min=','election_timeout_max=','heartbeat_timeout_min=','heartbeat_timeout_max=',
                'max_log_entries_per_call=','raft_log_file=','connect_retry_interval=','send_buffer_size=','recv_buffer_size=',
                'log_file=', 'log_level_core=','log_level_network=', 'show_statistic', 'help']
    try:
        optlist, args = getopt.getopt(args, shortopts, longopts)
    except getopt.GetoptError as e:
        print(e)
        help_exit()

    config = {
        # raft core
        'election_timeout_min': 500,    # in milliseconds
        'election_timeout_max': 1000,   # in milliseconds
        'heartbeat_timeout_min': 10,   # in milliseconds
        'heartbeat_timeout_max': 10,   # in milliseconds
        'max_log_entries_per_call': 50,
        'raft_log_file': None,           # use MemLog if None else PersistLog
        # network
        'connect_retry_interval': 5,     # in seconds
        'send_buffer_size': 2**25,# in bytes
        'recv_buffer_size': 2**25,# in bytes
        # log
        'log_file': None,                # if None, log to stdout and stderr
        'log_level_core':  logging.WARNING,
        'log_level_network': logging.WARNING,
        'show_statistic': True
    }

    for key, value in optlist:
        if key == '--help' or key == '-h':
            help_exit()
        elif key == '--election_timeout_min':
            config['election_timeout_min'] = int(value)
        elif key == '--election_timeout_max':
            config['election_timeout_max'] = int(value)
        elif key == '--heartbeat_timeout_min':
            config['heartbeat_timeout_min'] = int(value)
        elif key == '--heartbeat_timeout_max':
            config['heartbeat_timeout_min'] = int(value)
        elif key == '--max_log_entries_per_call':
            config['max_log_entries_per_call'] = int(value)
        elif key == '--raft_log_file':
            config['raft_log_file'] = value
        elif key == '--connect_retry_interval':
            config['connect_retry_interval'] =int(value)
        elif key == '--send_buffer_size':
            config['send_buffer_size'] = int(value)
        elif key == '--recv_buffer_size':
            config['recv_buffer_size'] = int(value)
        elif key == '--log_file':
            config['log_file'] = value
        elif key == '--log_level_core':
            config['log_level_core'] = getattr(logging, value)
        elif key == '--log_level_network':
            config['log_level_network'] = getattr(logging, value)
        elif key == '--show_statistic':
            config['show_statistic'] = True

    try:
        self = args[0]
        if self.isnumeric():
            self_node = ":".join(("127.0.0.1", self))
        else:
            self_node = self
    
        peers = args[1:]
        peer_nodes =set()
        for peer in peers:
            if peer.isnumeric():
                peer_nodes.add(":".join(("127.0.0.1", peer)))
            else:
                peer_nodes.add(peer)
    except IndexError:
        help_exit()


    config['self_node'] = self_node
    config['peer_nodes'] = peer_nodes
    return config

def init_log(config):
    logger = logging.getLogger('raft')
    logger.setLevel(logging.DEBUG)

    formatter = logging.Formatter('%(asctime)s %(name)-14s %(levelname)-7s %(message)s')

    if config['log_file']:
        fh = logging.FileHandler(config['log_file'])
        fh.setLevel(logging.DEBUG)
        fh.setFormatter(formatter)
        logger.addHandler(fh)
    else:
        sh = logging.StreamHandler()
        sh.setLevel(logging.DEBUG)
        sh.setFormatter(formatter)
        logger.addHandler(sh)



    logging.getLogger('raft.core').setLevel(config['log_level_core'])
    logging.getLogger('raft.network').setLevel(config['log_level_network'])
    logging.getLogger('raft.eventloop').setLevel(logging.INFO)
