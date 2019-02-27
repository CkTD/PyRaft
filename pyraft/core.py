import random
import logging

from .network import TcpTransport
from .network import EventLoop, EVENT_TYPE


logger = logging.getLogger('raft.core')

class MemoryLog():
    """ 
    Log are tuple (term, command)
    first log has index 1.
    """
    def __init__(self):
        self._l = []

    def __len__(self):
        return len(self._l)

    def __getitem__(self, index):
        if index <= 0:
            raise KeyError("first log index is 1")
        return self._l[index - 1]

    def add(self, term, command):
        """
        add a new log, return the index of that log.
        """
        self._l.append((term, command))
        return len(self._l)

    def contain(self, index, term):
        if index == 0:
            raise KeyError("first log index is 1")
        if len(self._l) < index:
            return False
        if self._l[index - 1][0] == term:
            return True
        return False

    def get_term(self, index):
        if index == 0:
            return 0
        else:
            return self._l[index - 1][0]

    def delete_after(self, index):
        self._l = self._l[:index]

    def __setitem__(self, key, value):
        if key == 0:
            raise KeyError("first index is 1")
        return super().__setitem__(key - 1, value)

class PersistLog():
    def __init__(self):
        raise NotImplementedError

class NODE_STATE:
    FOLLOWER = 0
    CANDIDATE = 1
    LEADER = 2

class RaftStateMachine():
    def __init__(self, self_node, peer_nodes):
        # "host:port" for this node, it's id
        self._self_node = self_node
        # list of "host:port" for raft peer nodes, peer's ids
        self._peer_nodes = set(peer_nodes)
        
        if (len(peer_nodes) + 1) % 2 == 0:
            raise ValueError("number of node in cluster should be odd")
        self._majority = (len(peer_nodes) + 1 + 1) // 2 

        self._eventloop = EventLoop()
        self._transport = TcpTransport(self_node, peer_nodes, 
                                       self._eventloop)
        self._transport.set_on_message_received(self._on_message_received)
        self._transport.set_on_node_connected(self._on_node_connected)
        self._transport.set_on_node_disconnected(self._on_node_disconnected)

        ### Persistent state on all servers ###
        ### (updated on stable storage before responding to RPCs) ###
        # latest term server has seen
        # initialized to 0 on firest boot, increases monotonically
        self._current_term = 0
        # candidate_id that received vote in current term or None if none
        self._voted_for = None
        # log entries, each entry contains command for state machine and 
        # term when entry was received by leader
        # first index is 1
        self._log = MemoryLog()

        ### volatile state on all servers ###
        # index of highest log entry known to be committed
        # initialized to 0, increases monotonically
        self._commit_index = 0  
        # index of higheset log entry applied to state machine
        # initialized to 0, increases monotonically
        self._last_applied = 0  
        
        ### volatile state on leaders ###
        # for each server, index of the next log entry to send to that server
        # initialized to leader last log index + 1
        self._next_index = {}   
        # for each server, index of highest log entry known to be relicated on server
        # initialized to 0, increases monotonically
        self._match_index = {}

        self._state = None
        self._leader = None
        self._last_heartbeat = 0

        self._voted_by = set()
        self._get_random_election_timeout = lambda: random.randint(5, 10)
        self._election_timeout_id = None

        self._get_random_heartbeat_timeout = lambda: random.randint(3, 5)
        self._heartbeat_timeout_id = None

        self._to_follower()

        self._on_apply_log = None

        self._alive_peer_nodes = set()
        self._client_nodes = set()
        self._log_to_client = []

    @property
    def _last_log_index(self):
        return len(self._log)

    @property
    def _last_log_term(self):
        if len(self._log) == 0:
            return 0
        else:
            return self._log[len(self._log)][0]
        
    def set_on_apply_log(self, callback):
        self._on_apply_log = callback

    def _to_follower(self):
        self._state = NODE_STATE.FOLLOWER
        self._leader = None
        
        if self._election_timeout_id is not None:
            self._eventloop.unregister_time_event(self._election_timeout_id)
        self._election_timeout_id = self._eventloop.register_time_event(
                                        self._get_random_election_timeout(),
                                        self._election_timeout_handler,
                                        self._get_random_election_timeout())        

        if self._heartbeat_timeout_id is not None:
            self._eventloop.unregister_time_event(self._heartbeat_timeout_id)
        self._heartbeat_timeout_id = None
    
    def _to_leader(self):
        assert self._state == NODE_STATE.CANDIDATE  # only candidate can be leader

        self._state = NODE_STATE.LEADER
        self._leader = self._self_node

        for node in self._peer_nodes:
            self._next_index[node] = self._last_log_index + 1
            self._match_index[node] = 0
        
        if self._election_timeout_id is not None:
            self._eventloop.unregister_time_event(self._election_timeout_id)
        self._election_timeout_id = None
        
        self._heartbeat_timeout_id = self._eventloop.register_time_event(
                                        0,
                                        self._heartbeat_timeout_handler,
                                        self._get_random_heartbeat_timeout())

        self._client_nodes = set()
        self._log_to_client = []

    def _to_candidate(self):
        """
        After election timeout, begin a new election. 
        To begin an election, a follower increments its current
        term and transitions to candidate state. It then votes for
        itself and issues RequestVote RPCs in parallel to each of
        the other servers in the cluster. A candidate continues 
        inthis state until one of three things happens: 
        (a) it wins the election, 
        (b) another server establishes itself as leader, or
        (c) a period of time goes by with no winner. 
        """
        self._state = NODE_STATE.CANDIDATE
        self._leader = None
        self._current_term += 1
        self._voted_for = self._self_node
        self._voted_by = set()
        self._voted_by.add(self._self_node)

        if self._heartbeat_timeout_id is not None:
            self._eventloop.unregister_time_event(self._heartbeat_timeout_id)

        self._request_vote()

    def _election_timeout_handler(self):
        self._to_candidate()

    def _heartbeat_timeout_handler(self):
        self._append_entries()

    def _on_message_received(self, node, message):
        """ process incoming message from node """
        
        m_type = message['type']

        # message from node in raft cluster.
        if node in self._peer_nodes:
            m_term = message['term']
            if m_term > self._current_term:
                self._current_term = m_term
                self._voted_for = None
                self._to_follower()

            if m_type == 'request_vote':
                m_last_log_index = message['last_log_index']
                m_last_log_term = message['last_log_term']
                if self._state == NODE_STATE.FOLLOWER:
                    if m_term < self._current_term:
                        self._request_vode_response(node, False)
                        return
                    if self._voted_for is None and not (
                        self._last_log_term > m_last_log_term or
                            (self._last_log_term == m_last_log_term and
                            self._last_log_index > m_last_log_index) ):
                        self._request_vode_response(node, True)
                        self._voted_for = node

            elif m_type == 'request_vote_response':
                vote_granted = message['vote_granted']
                if self._state == NODE_STATE.CANDIDATE:
                    if m_term == self._current_term:
                        if vote_granted is True:
                            self._voted_by.add(node)
                            if len(self._voted_by) >= self._majority:
                                self._to_leader()

            elif m_type == 'append_entries':
                m_leader_id = message['leader_id']
                m_prev_log_index = message['prev_log_index']
                m_prev_log_term = message['prev_log_term']
                m_entry = message['entry']
                m_leader_commit = message['leader_commit']
                if self._state in (NODE_STATE.CANDIDATE, NODE_STATE.FOLLOWER):
                    if m_term != self._current_term:
                        self._append_entry_response(node, False)
                        return
                    # candidate may find there is already a leader for this term.
                    if self._state == NODE_STATE.CANDIDATE:
                        self._to_follower()
                    # we know who is leader.
                    self._leader = node
                    if self._log.contain(m_prev_log_index, m_prev_log_term) is False:
                        self._append_entry_response(node, False, m_prev_log_index + 1)
                        return
                    self._log.delete_after(m_prev_log_index) 
                    if m_entry is not None:
                        self._log.add(self._current_term ,m_entry)
                        if m_leader_commit > self._commit_index:
                            self._commit_index = min(m_leader_commit, self._last_log_index)
                        self._append_entry_response(node, True, m_prev_log_index + 1)
                        self._apply_log()

            elif m_type == 'append_entry_response':
                m_success = message['success']
                m_for_index = message['for_index']
                if self._state == NODE_STATE.LEADER:
                    if m_term != self._current_term:
                        return
                    if m_success is True and m_for_index == self._next_index[node]:
                        self._match_index[node] = self._next_index[node]
                        self._leader_check_commit()
                        self._next_index[node] += 1
                    elif m_success is False and m_for_index == self._next_index[node]:
                        self._next_index[node] -= 1
        
        # message from client
        elif node in self._client_nodes:
            if m_type == 'client_request':
                if self._state == NODE_STATE.LEADER:
                    m_command = message['command']
                    index = self._log.add(self._current_term, m_command)
                    self._log_to_client[index] = node
                else:
                    self._server_response(node, False, m_command[0])
        
        # bugs in TcpTransport or RaftStateMachine
        else:
            assert False

    def _on_node_connected(self, node):
        if node in self._peer_nodes:
            self._alive_peer_nodes.add(node)
        else:
            self._client_nodes.add(node)

    def _on_node_disconnected(self, node):
        if node in self._peer_nodes:
            self._alive_peer_nodes.remove(node)
        else:
            self._client_nodes.remove(node)

    def _request_vote(self):
        """ invoked by candidates to gather votes """
        """
        Raft uses the voting process to prevent a candidate from
        winning an election unless its log contains all committed
        entries. A candidate must contact a majority of the cluster
        in order to be elected, which means that every committed
        entry must be present in at least one of those servers. If the
        candidate’s log is at least as up-to-date as any other log
        in that majority (where “up-to-date” is defined precisely
        below), then it will hold all the committed entries. The
        RequestVote RPC implements this restriction: the RPC
        includes information about the candidate’s log, and the
        voter denies its vote if its own log is more up-to-date than
        that of the candidate.
        """
        message = {
            'type':'request_vote',
            'term': self._current_term,
            'candidate_id': self._self_node,
            'last_log_index': self._last_log_index,
            'last_log_term': self._last_log_term
        }
        for node in self._peer_nodes:
            self._transport.send(node, message)

    def _request_vode_response(self, node, vote_granted):
        """ invoked by follower to response request_vote """
        self._transport.send(node, {
            'type': 'request_vote_response',
            'term': self._current_term,
            'vote_granted' : vote_granted
        })

    def _append_entries(self):
        """ invoked by leader to replicated log entries, 
        also used as heartbeat """
        for node in self._peer_nodes:
            next_to_send = self._next_index[node]
            prev_log_index = next_to_send - 1
            prev_log_term = self._log.get_term(prev_log_index)
            if self._last_log_index >= next_to_send:
                entries = self._log[next_to_send]
            else:
                entries = []

            message = {
                'type': 'append_entries',
                'term': self._current_term,
                'leader_id': self._self_node,
                'prev_log_index': prev_log_index,
                'prev_log_term': prev_log_term,
                'entries': entries,
                'leader_commit': self._commit_index
            }
            self._transport.send(node, message)

    def _append_entry_response(self, node, success, for_index):
        """ invoked by follower of candidate response append_entry request """
        self._transport.send(node, {
            'type': 'append_entry_response',
            'term': self._current_term,
            'success': success,
            'for_index': for_index
        })

    def _server_response(self, node, success, id):
        if success:
            message = {
                'type': 'server_response',
                'success' : True,
                'command_id': id
            }
        else:
            message = {
                'type': 'server_response',
                'success': False,
                'command_id': id,
                'redirect': self._leader
            }
        self._transport.send(node, message)

    def _leader_check_commit(self):
        index = self._last_log_index
        while index > 0:
            if self._log[index][0] != self._current_term:
                break
            count = 0
            for node in self._peer_nodes:
                if self._match_index[node] >= index:
                    count += 1
            if count >= self._majority:
                self._commit_index = index
                self._apply_log()
                break

    def _apply_log(self):
        while self._commit_index > self._last_applied:
            self._last_applied += 1
            self._on_apply_log(self._log[self._last_applied][1])

    def run(self):
        self._eventloop.run()



if __name__ == '__main__':
    import sys


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
