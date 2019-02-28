import random
import logging

from .network import TcpTransport
from .network import EventLoop, EVENT_TYPE


logger = logging.getLogger('raft.core')

class MemoryLog():
    """ 
    log is a tuple (term, (serial,command))
    log is a tuple (term, serial, command))
    first log has index 1.
    no-op entry is (term, (None,None))
    """
    def __init__(self):
        self._l = []

    def __len__(self):
        return len(self._l)

    def __getitem__(self, index):
        if index <= 0:
            raise KeyError("first log index is 1")
        return self._l[index - 1]

    def add(self, term, serial_number, command):
        """
        add a new log, return the index of that log.
        """
        self._l.append((term, serial_number, command))
        return len(self._l)

    def contain(self, index, term):
        """
        return True if there exist a log with index=index, term=term
        if index == 0, always return True
        """
        if index == 0:
            return True
        if len(self._l) < index:
            return False
        if self._l[index - 1][0] == term:
            return True
        return False

    def get_term(self, index):
        """
        return the term of log with index = index
        if index == 0, return 0
        """
        if index == 0:
            return 0
        else:
            return self._l[index - 1][0]
    
    def get_serial_number(self, index):
        if index == 0:
            raise IndexError("first log has index 1")
        return self._l[index - 1][1]

    def get_command(self, index):
        if index == 0:
            raise IndexError("first log has index 1")
        return self._l[index - 1][2]


    def delete_after(self, index):
        """
        delete all logs having index > index
        """
        self._l = self._l[:index]


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
        # log entry, each entry contains command for state machine and 
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

        # for all servers
        self._state = None
        self._leader = None
        self._on_apply_log = None
        self._alive_peer_nodes = set()
        self._client_nodes = set()
        self._commit_serial = set()     # all serial numbers of committed log

        # for candidate
        self._voted_by = set()

        # for candidate and follower
        self._get_random_election_timeout = lambda: random.randint(5, 10)
        self._election_timeout_id = None

        # for leader 
        # log index -> client
        # which client request the log? response that client after the log is committed 
        self._log_to_client = {}   
        self._get_random_heartbeat_timeout = lambda: random.randint(3, 5)
        self._heartbeat_timeout_id = None
        self._no_op_index = None
        self._no_op_committed = False
        self._devoted_followers = 0
        self._readonly_request = []        # [(node, serial_number, command),(...), ...]
        self._on_readonly = None            # handler for readonly request

        self._to_follower()  # when servers start up, they begin as followers. 

    @property
    def _last_log_index(self):
        return len(self._log)

    @property
    def _last_log_term(self):
        if len(self._log) == 0:
            return 0
        else:
            return self._log.get_term(self._last_log_index)
        
    def set_on_apply_log(self, callback):
        self._on_apply_log = callback

    def set_on_readonly(self, callback):
        self._on_readonly = callback

    def _to_follower(self):
        """
        A server remains in follower state as long as it receives valid
        5RPCs from a leader or candidate. Leaders send periodic
        heartbeats (AppendEntries RPCs that carry no log entries)
        to all followers in order to maintain their authority. If a
        follower receives no communication over a period of time
        called the election timeout, then it assumes there is no vi-
        able leader and begins an election to choose a new leader.
        """
        logger.info("convert to follower. current term: %d" % self._current_term)

        self._state = NODE_STATE.FOLLOWER
        self._leader = None
        
        self._reset_election_timeout()

        if self._heartbeat_timeout_id != None:
            self._eventloop.unregister_time_event(self._heartbeat_timeout_id)
        self._heartbeat_timeout_id = None
    
    def _to_leader(self):
        logger.info("convert to leader. current term: %d" % self._current_term)

        assert self._state == NODE_STATE.CANDIDATE  # only candidate can be leader

        self._state = NODE_STATE.LEADER
        self._leader = self._self_node

        self._client_nodes = set()
        self._log_to_client = {}
        self._devoted_followers = 1 # 1 for it self
        self._no_op_committed = False
        self._no_op_index = self._log.add(self._current_term, None, None)
        self._readonly_request = []
        for node in self._peer_nodes:
            self._next_index[node] = self._last_log_index + 1
            self._match_index[node] = 0
        
        if self._election_timeout_id != None:
            self._eventloop.unregister_time_event(self._election_timeout_id)
        self._election_timeout_id = None
        
        self._heartbeat_timeout_id = self._eventloop.register_time_event(
                                        0,
                                        self._heartbeat_timeout_handler,
                                        self._get_random_heartbeat_timeout())

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

        if self._heartbeat_timeout_id != None:
            self._eventloop.unregister_time_event(self._heartbeat_timeout_id)

        self._request_vote()

    def _election_timeout_handler(self):
        logger.info("election timeout, convert to candidate and to new term: [%d]" % (self._current_term + 1))
        self._to_candidate()

    def _heartbeat_timeout_handler(self):
        logger.info("heartbeat timeout, send append entry to all follower.")
        self._devoted_followers = 1 # reset it
        self._append_entry()

    def _on_message_received(self, node, message):
        """ process incoming request
        All Servers:
            • If commitIndex > lastApplied: increment lastApplied, apply
              log[lastApplied] to state machine (§5.3)
            • If RPC request or response contains term T > currentTerm:
              set currentTerm = T, convert to follower (§5.1)
        Followers (§5.2):
            • Respond to RPCs from candidates and leaders
            • If command received from client: return False and tell the 
              client who is leader if we know.
        Candidates (§5.2):
            • If AppendEntries RPC received from new leader: convert to
              follower
        Leaders:
            • If command received from client: append entry to local log,
              respond after entry applied to state machine (§5.3)
            • If last log index ≥ nextIndex for a follower: send
              AppendEntries RPC with log entries starting at nextIndex
            • If successfully replicated log to follower: update nextIndex 
              and matchIndex for follower (§5.3)
            • If AppendEntries fails because of log inconsistency:
              decrement nextIndex and retry (§5.3)
            • If there exists an N such that N > commitIndex, a majority
              of matchIndex[i] ≥ N, and log[N].term == currentTerm:
              set commitIndex = N (§5.3, §5.4).
        """
        logger.debug("receive message from [%s]: \n\t%s" %(node, str(message)))

        m_type = message['type']

        # message from node in raft cluster.
        if node in self._peer_nodes:
            m_term = message['term']
            if m_term > self._current_term:
                logger.info("new term detected: [%d], switch form current: [%d]" % (m_term, self._current_term))
                self._current_term = m_term
                self._voted_for = None
                self._to_follower()

            if m_type == 'request_vote':
                m_last_log_index = message['last_log_index']
                m_last_log_term = message['last_log_term']
                if self._state == NODE_STATE.FOLLOWER:
                    if m_term < self._current_term:
                        logger.info("reject request_vote by [%s]. from old term [%d]." %(node, m_term))
                        self._request_vote_response(node, False)
                        return
                    if self._voted_for == None and not (
                        self._last_log_term > m_last_log_term or
                            (self._last_log_term == m_last_log_term and
                            self._last_log_index > m_last_log_index) ):
                        logger.info("vote for %s" % node)
                        self._request_vote_response(node, True)
                        self._voted_for = node
                    else:
                        if self._voted_for != None:
                            logger.info("reject request_vote by [%s]. already voted for" %(node, self._voted_for))
                        else:
                            logger.info("reject request_vote by [%s]. that candidate's log is not up-to-date" % node)

            elif m_type == 'request_vote_response':
                vote_granted = message['vote_granted']
                if self._state == NODE_STATE.CANDIDATE:
                    if m_term == self._current_term:
                        if vote_granted == True:
                            # a node can vote for only one time in a given term.
                            # now, the response is must for corresponding request.
                            logger.info('voted by [%s]' % node)
                            self._voted_by.add(node)
                            if len(self._voted_by) >= self._majority:
                                self._to_leader()

            elif m_type == 'append_entry':
                m_leader_id = message['leader_id']
                m_prev_log_index = message['prev_log_index']
                m_prev_log_term = message['prev_log_term']
                m_entry = message['entry']
                m_leader_commit = message['leader_commit']
                if self._state in (NODE_STATE.CANDIDATE, NODE_STATE.FOLLOWER):
                    if m_term != self._current_term:
                        logger.info('reject append_entry from old term[%d].'% m_term)
                        self._append_entry_response(node, False)
                        return
                    # candidate find there is already a leader for this term.
                    if self._state == NODE_STATE.CANDIDATE:
                        logger.info('new leader [%s] detected in this term' % node)
                        self._to_follower()
                    
                    # now, we are a follower and know who is leader
                    if self._leader == None:
                        logger.info('new leader [%s] detected' % node)
                        self._leader = node
                    else:
                        assert self._leader == node # it is impossible to have different leaders in same term.
                    
                    # reset the election timeout.
                    self._reset_election_timeout()
                    
                    # perform the consistency check.
                    if self._log.contain(m_prev_log_index, m_prev_log_term) == False:
                        logger.info("append_entry: consistency check failed: local log has no entry with index=[%d], term=[%d]" % (m_prev_log_index, m_prev_log_term))
                        self._append_entry_response(node, False, m_prev_log_index + 1)
                        return

                    # delete the entry conflicting to leader 
                    self._log.delete_after(m_prev_log_index)

                    # if heartbeat contain a new entry, add to self log and response to the RPC. 
                    if m_entry != None:
                        logger.info("append_entry: received new entry from leader, index:[%d], term:[%d]" %(m_prev_log_index + 1 ,m_term))
                        self._log.add(*m_entry)
                        ##### !!!!!
                        ##### update on stable storage here before response.
                    else:
                        logger.info("append_entry: heartbeat without entry")
                    self._append_entry_response(node, True, m_prev_log_index + 1)

                    # check if leader have committed some log.
                    if m_leader_commit > self._commit_index:
                        logger.info("append_entry: commit new entry, leader commit: [%d], local commit: [%d], local_last: [%d]" %(m_leader_commit, self._commit_index, self._last_log_index))
                        index = min(m_leader_commit, self._last_log_index)
                        while self._commit_index < index:
                            self._commit_index += 1
                            self._commit_serial.add(self._log.get_serial_number(self._commit_index))
                        self._apply_log()

            elif m_type == 'append_entry_response':
                m_success = message['success']
                m_for_index = message['for_index']
                if self._state == NODE_STATE.LEADER:
                    if m_term != self._current_term:
                        logger.info("append_entry_response for old term. ignore")
                        return
                    if m_for_index != self._next_index[node]:
                        logger.info("append_entry_response for previous index. ignore")
                        return
                    # now, the node must be a valid follower.
                    self._devoted_followers +=1
                    self._leader_check_readonly_request()

                    if m_success == True:
                        # if respond to a request without entry, do nothing
                        if m_for_index > self._last_log_index:
                            return
                        logger.info("append_entry_response log [%d] replicated to node [%s] sucess" %(m_for_index, node))
                        self._match_index[node] = m_for_index
                        self._next_index[node] += 1
                        self._leader_check_commit()
                    else:
                        self._next_index[node] -= 1
                        logger.info("append_entry_response failed for node [%s], decrease next_index to [%d]" %(node, self._next_index[node]))
        
            else:
                assert False # it's impossible...

        # message from client
        elif node in self._client_nodes:
            if m_type == 'client_request':
                m_serial_number = message ['serial_number']
                m_readonly =  message['readonly']
                m_command = message['command']
                if self._state == NODE_STATE.LEADER:
                    # if the leader crashes after committing the log entry but before respond-
                    # ing to the client, the client will retry the command with a
                    # new leader, causing it to be executed a second time. The
                    # solution is for clients to assign unique serial numbers to
                    # every command. Then, the state machine tracks the latest
                    # serial number processed for each client, along with the as-
                    # sociated response. If it receives a command whose serial
                    # number has already been executed, it responds immedi-
                    # ately without re-executing the request.
                    if m_readonly == True:
                        self._readonly_request.append((node, m_serial_number, m_command))
                        self._leader_check_readonly_request()
                    else:
                        if m_serial_number in self._commit_serial:
                            logger.info("client request: command with serial id [%d] .already success in previous" % m_serial_number)
                            self._server_response(node, m_serial_number, True, None)
                        else:
                            logger.info("client request: add the command to log.")
                            index = self._log.add(self._current_term, m_serial_number,m_command)
                            self._log_to_client[index] = node
                else:
                    logger.info("client request: redirect client to leader...")
                    self._server_response(node, m_serial_number, False, self._leader)
        
        # bugs in TcpTransport
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
        """ 
        invoked by candidates to gather votes.
        Raft uses the voting process to prevent a candidate from
        winning an election unless its log contains all committed
        entry. A candidate must contact a majority of the cluster
        in order to be elected, which means that every committed
        entry must be present in at least one of those servers. If the
        candidate’s log is at least as up-to-date as any other log
        in that majority (where “up-to-date” is defined precisely
        below), then it will hold all the committed entry. The
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

    def _request_vote_response(self, node, vote_granted):
        """ invoked by follower to response request_vote """
        self._transport.send(node, {
            'type': 'request_vote_response',
            'term': self._current_term,
            'vote_granted' : vote_granted
        })

    def _append_entry(self):
        """ invoked by leader to replicated log entry, 
        also used as heartbeat """
        for node in self._peer_nodes:
            next_to_send = self._next_index[node]
            prev_log_index = next_to_send - 1
            prev_log_term = self._log.get_term(prev_log_index)
            if self._last_log_index >= next_to_send:
                entry = self._log[next_to_send]
            else:
                entry = None

            message = {
                'type': 'append_entry',
                'term': self._current_term,
                'leader_id': self._self_node,
                'prev_log_index': prev_log_index,
                'prev_log_term': prev_log_term,
                'entry': entry,
                'leader_commit': self._commit_index
            }
            self._transport.send(node, message)

    def _append_entry_response(self, node, success, for_index):
        """ 
        invoked by follower of candidate response append_entry request.
        without RPC, several response to one log cause error on leader
        so, for an append_entry requese, alway add for_index to the response to let 
        the leader check if the response is for the corresponding request.

        for_index is (pre_log_index + 1), which is also leader's next_index for this node  
        the leader just check for_index == next_index to ignore duplicated response for same log 
        """
        self._transport.send(node, {
            'type': 'append_entry_response',
            'term': self._current_term,
            'success': success,
            'for_index': for_index
        })

    def _server_response(self, node, serial_number, success, data):
        """ invoked by leader to respond client request """
        message = {
                'type': 'server_response',
                'success' : success,
                'serial_number': serial_number
            }
        if success:
            message['response'] = data
        else:
            message['redirect'] = data
        self._transport.send(node, message)

    def _leader_check_commit(self):
        """
        Raft never commits log entry from previous terms by count-
        ing replicas. Only log entry from the leader’s current
        term are committed by counting replicas; once an entry
        from the current term has been committed in this way,
        then all prior entry are committed indirectly because
        of the Log Matching Property.
        """
        logger.debug("leader check commit...")
        logger.debug("%s" % str(self))
        index = self._last_log_index
        while index > self._commit_index:
            if  self._log.get_term(index) != self._current_term:
                break
            count = 1 # it is always leader's logs
            for node in self._peer_nodes:
                if self._match_index[node] >= index:
                    count += 1
            if count >= self._majority:
                # commit entrys
                while self._commit_index != index:
                    self._commit_index +=1
                    self._commit_serial.add(self._log.get_serial_number(self._commit_index))
                    logger.info("commit new log with index [%d]" % self._commit_index)
                    if self._commit_index == self._no_op_index:
                        logger.info("no-op log entry committed.")
                        self._no_op_committed = True
                        self._leader_check_readonly_request()
                    # response to client the success.
                    if self._commit_index in self._log_to_client:
                        logger.info("respons to client [%s] success" % self._log_to_client[self._commit_index])
                        self._server_response(self._log_to_client[self._commit_index], 
                                              self._log.get_serial_number(self._commit_index),
                                              True,
                                              None)
                self._apply_log()
                break
            index -= 1
    
    def _leader_check_readonly_request(self):
        """
        Read-only operations can be handled without writing
        anything into the log. However, with no additional mea-
        sures, this would run the risk of returning stale data, since
        the leader responding to the request might have been su-
        perseded by a newer leader of which it is unaware. Lin-
        earizable reads must not return stale data, and Raft needs
        two extra precautions to guarantee this without using the
        log. First, a leader must have the latest information on
        which entries are committed. The Leader Completeness
        Property guarantees that a leader has all committed en-
        tries, but at the start of its term, it may not know which
        those are. To find out, it needs to commit an entry from
        its term. Raft handles this by having each leader com-
        mit a blank no-op entry into the log at the start of its
        term. Second, a leader must check whether it has been de-
        posed before processing a read-only request (its informa-
        tion may be stale if a more recent leader has been elected).
        Raft handles this by having the leader exchange heart-
        beat messages with a majority of the cluster before re-
        sponding to read-only requests.
        """
        if self._no_op_committed and self._devoted_followers >= self._majority:
            for node ,serial_number, command in self._readonly_request:
                logger.info("handle readonly request [%d]" % serial_number)
                response = self._on_readonly(command)
                self._server_response(node, serial_number, True, response)
            self._readonly_request.clear()

    def __str__(self):
        state_d = {
            NODE_STATE.LEADER: "leader",
            NODE_STATE.CANDIDATE: "candidate",
            NODE_STATE.FOLLOWER: "follower"
        }
        return str({
            "state:": state_d[self._state],
            "term": self._current_term,
            "leader": self._leader,
            "vote for": self._voted_for,
            "commit index": self._commit_index,
            "last applied": self._last_applied,
            "last log index": self._last_log_index,
            "match index": self._match_index,
            "next index": self._next_index
        })

    def _apply_log(self):
        while self._commit_index > self._last_applied:
            self._last_applied += 1
            # do nothing for the no-op log entry
            if self._log.get_serial_number(self._last_applied) == None:
                continue
            self._on_apply_log(self._log.get_command(self._last_applied))

    def _reset_election_timeout(self):
        if self._election_timeout_id != None:
            self._eventloop.unregister_time_event(self._election_timeout_id)
        self._election_timeout_id = self._eventloop.register_time_event(
                                        self._get_random_election_timeout(),
                                        self._election_timeout_handler,
                                        self._get_random_election_timeout())

    def run(self):
        self._eventloop.run()

