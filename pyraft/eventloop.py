# -*- coding: utf-8 -*-

import select
import time
import bisect
import logging

logger = logging.getLogger('raft.eventloop')

class KeyGetter():
    """ helper class for insort """
    def __init__(self, l, key):
        self.l = l
        self.key = key
    def __len__(self):
        return len(self.l)
    def __getitem__(self, index):
        return self.key(self.l[index])

def insort(l, key, item):
    """ insert item to sorted list l. key is the custom key funcion """
    index = bisect.bisect(KeyGetter(l, key=key), key(item))
    l.insert(index, item)


class EVENT_TYPE:
    READ = select.EPOLLIN
    WRITE = select.EPOLLOUT
    ERROR = select.EPOLLERR


class EventLoop():
    def __init__(self):
        self._poller = select.epoll()
        self._running = False

        # file event
        # for file event, eventloop only keep fd and it's handler, event_mask is kept by poller.
        self._fd_to_file_handler = {}

        # time event
        # (id  ,fire_time       , handler, period)
        # (int ,float(in second), func   , None if once else timeinterval)
        self._time_events = []
        self._time_event_next_id = 0
        self._id_to_time_events = {}

        # statistic info
        self._statistic_interval = 1

        self._skew = 0
        self._idle = 0
        self._start_time = 0
        self._fired_file_events = 0
        self._fired_time_events = 0

        self._last_time = 0
        self._last_idle = 0

        self._load_current = 0
        self._load_since_up = 0

    def statistic(self):
        return (self._start_time, 
                self._fired_file_events,
                self._fired_time_events,
                self._skew,
                self._load_since_up,
                self._load_current)

    def register_file_event(self, fd, event_mask, handler):
        self._fd_to_file_handler[fd] = handler
        self._poller.register(fd, event_mask)

    def unregister_file_event(self, fd):
        del self._fd_to_file_handler[fd]
        try:
            self._poller.unregister(fd)
        except PermissionError as e:
            logger.warning("unregister file event for [%d] error %s"%(fd, str(e)))
    def register_time_event(self, second, handler, period = None):
        assert period is None or isinstance(period, (int,float))

        now = time.time()
        fire_time = now + second

        self._time_event_next_id += 1
        event_id = self._time_event_next_id
        event = (event_id, fire_time, handler, period)
        
        self._id_to_time_events[event_id] = event
        insort(self._time_events, lambda x: x[1], event)
        
        return event_id

    def unregister_time_event(self, event_id):
        event = self._id_to_time_events[event_id]
        
        self._time_events.remove(event)
        del self._id_to_time_events[event_id]

    def poll(self, time_out):
        events = self._poller.poll(time_out)

        return [(fd, event_mask, self._fd_to_file_handler[fd]) for fd, event_mask in events]

    def _statistic_handler(self):
        now = time.time()
        self._load_current = (1 - self._last_idle / (now - self._last_time)) * 100
        self._load_since_up = (1 - self._idle / (now - self._start_time)) * 100

        self._last_time = now
        self._last_idle = 0

    def _on_begin(self):
        logger.info("eventloop started")
        now = time.time()
        self._skew = 0
        self._idle = 0
        self._start_time = now
        self._last_time = now
        self._last_idle = 0
        self._load_current = 0
        self._load_since_up = 0
        self._fired_file_events = 0
        self._fired_time_events = 0
        self._statistic_handler_id = self.register_time_event(self._statistic_interval, 
                                                              self._statistic_handler, 
                                                              self._statistic_interval)
    
    def _on_end(self):
        logger.info("eventloop stopped")
        self.unregister_time_event(self._statistic_handler_id)

    def run(self):
        self._running = True
        self._on_begin()

        while self._running:
            logger.debug("EventLoop: next loop")
            self._process_event()

        self._on_end()
    
    def _get_time_to_nearest_time_event(self):
        now = time.time()

        if self._time_events:
            fire_time =  self._time_events[0][1]
            if fire_time < now:
                return 0
            else:
                return fire_time - now
        else:
            return -1

    def _process_event(self):
        shortest = self._get_time_to_nearest_time_event()

        #Call the multiplexing API, will return only on timeout or when some file event fires.
        st = time.time()
        fired_events = self.poll(shortest)
        et = time.time()
        idle = et - st
        self._idle += idle
        self._last_idle += idle

        # process fired file event if has any
        for fd, event_mask, handler in fired_events:
            self._fired_file_events += 1
            handler(fd, event_mask)

        # process fired time event if has any
        now = time.time()
        while True:
            if not self._time_events: # have no time event? done
                break
            if self._time_events[0][1] > now: # neareast time event not fired? done
                break
            self._skew += now - self._time_events[0][1] 
            self._fired_time_events += 1
            event_id, fire_time, handler, period = self._time_events.pop(0)
            handler()

            del self._id_to_time_events[event_id]
            if period is not None:
                event = (event_id, fire_time + period, handler, period)
                insort(self._time_events, lambda x: x[1], event)
                self._id_to_time_events[event_id] = event

    def stop(self):
        self._running = False


    def destory(self):
        self.stop()
        self._poller.close()

    def __del__(self):
        logger.debug("EventLoop: DESTORIED.")