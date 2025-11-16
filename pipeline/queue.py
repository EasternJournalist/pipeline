"""
Extensions of queue.Queue.
"""

import sys
from time import time, perf_counter
import threading
from typing import Callable, Iterable, List, Dict, Optional

from queue import Full, Empty
from queue import Queue as _Queue

__all__ = [
    'ShutDown',
    'Queue',
    'SharedQueue',
    'EndOfInput'
]

# Define a shutdownable queue for Python versions < 3.13
# Copied from Python 3.13's queue module 
class ShutDown(Exception):
    """Raised when put/get with shut-down queue."""
    pass


class EndOfInput:
    pass

class Queue(_Queue):
    '''An extension of queue.Queue, with
    - backported shutdown() method from Python 3.13
    - waiters and putters count
    '''
    get_waiters: int
    put_waiters: int
    is_shutdown: bool

    def __init__(self, maxsize: int = 0):
        super().__init__(maxsize)
        self.is_shutdown = False
        self.get_waiters = 0
        self.put_waiters = 0  
        self.on_put = None
        self.on_get = None
    
    def shutdown(self, immediate=False):
        '''Shut-down the queue, making queue gets and puts raise ShutDown.

        By default, gets will only raise once the queue is empty. Set
        'immediate' to True to make gets raise immediately instead.

        All blocked callers of put() and get() will be unblocked. If
        'immediate', a task is marked as done for each item remaining in
        the queue, which may unblock callers of join().
        '''
        with self.mutex:
            self.is_shutdown = True
            if immediate:
                while self._qsize():
                    self._get()
                    if self.unfinished_tasks > 0:
                        self.unfinished_tasks -= 1
                # release all blocked threads in `join()`
                self.all_tasks_done.notify_all()
            # All getters need to re-check queue-empty to raise ShutDown
            self.not_empty.notify_all()
            self.not_full.notify_all()

    def put(self, item, block=True, timeout=None):
        '''Put an item into the queue.

        If optional args 'block' is true and 'timeout' is None (the default),
        block if necessary until a free slot is available. If 'timeout' is
        a non-negative number, it blocks at most 'timeout' seconds and raises
        the Full exception if no free slot was available within that time.
        Otherwise ('block' is false), put an item on the queue if a free slot
        is immediately available, else raise the Full exception ('timeout'
        is ignored in that case).

        Raises ShutDown if the queue has been shut down.
        '''
        with self.not_full:
            if self.is_shutdown:
                raise ShutDown
            self.put_waiters += 1
            try:
                if self.maxsize > 0:
                    if not block:
                        # no wait
                        if self._qsize() >= self.maxsize:
                            raise Full
                    elif timeout is None:
                        while self._qsize() >= self.maxsize:
                            self.not_full.wait()
                            if self.is_shutdown:
                                raise ShutDown
                    elif timeout < 0:
                        raise ValueError("'timeout' must be a non-negative number")
                    else:
                        endtime = time() + timeout
                        while self._qsize() >= self.maxsize:
                            remaining = endtime - time()
                            if remaining <= 0.0:
                                raise Full
                            self.not_full.wait(remaining)
                            if self.is_shutdown:
                                raise ShutDown
            finally:
                self.put_waiters -= 1
            self._put(item)
            self.unfinished_tasks += 1
            self.not_empty.notify()
            if self.on_put is not None:
                self.on_put()

    def get(self, block=True, timeout=None):
        '''Remove and return an item from the queue.

        If optional args 'block' is true and 'timeout' is None (the default),
        block if necessary until an item is available. If 'timeout' is
        a non-negative number, it blocks at most 'timeout' seconds and raises
        the Empty exception if no item was available within that time.
        Otherwise ('block' is false), return an item if one is immediately
        available, else raise the Empty exception ('timeout' is ignored
        in that case).

        Raises ShutDown if the queue has been shut down and is empty,
        or if the queue has been shut down immediately.
        '''
        with self.not_empty:
            if self.is_shutdown and not self._qsize():
                raise ShutDown
            self.get_waiters += 1
            if self.on_get is not None:
                self.on_get()
            try:
                if not block:
                    # no wait
                    if not self._qsize():
                        raise Empty
                elif timeout is None:
                    while not self._qsize():
                        self.not_empty.wait()
                        if self.is_shutdown and not self._qsize():
                            raise ShutDown
                elif timeout < 0:
                    raise ValueError("'timeout' must be a non-negative number")
                else:
                    endtime = time() + timeout
                    while not self._qsize():
                        remaining = endtime - time()
                        if remaining <= 0.0:
                            raise Empty
                        self.not_empty.wait(remaining)
                        if self.is_shutdown and not self._qsize():
                            raise ShutDown
            finally:
                self.get_waiters -= 1
            item = self._get()
            self.not_full.notify()
            return item

    if sys.version_info >= (3, 9):
        from types import GenericAlias
        __class_getitem__ = classmethod(GenericAlias)


class SharedQueue:
    "A queue that can share with upstream/downstream SharedQueues."
    queue: Optional[Queue]
    "The actual queue instance."
    on_get: Optional[Callable]
    "Callback before each get() operation."
    on_put: Optional[Callable]
    "Callback after each put() operation."
    shared: set['SharedQueue']
    "The set of SharedQueue instances sharing the same underlying Queue."

    _upstream: Optional['SharedQueue']
    _downstream: Optional['SharedQueue']

    def __init__(self, maxsize: int = 0):
        self.maxsize = maxsize
        self._upstream = None
        self._downstream = None
        self.shared = set((self,))
        self.queue = None
        
        # Callbacks
        self.on_get = None
        self.on_put = None
        
        # Originally, mutex is released during get/put blocking.
        # Now we use additionally locks for get/put respectively.
        # This avoids overlapping get/put time measurements.
        self.get_lock = threading.Lock()
        self.put_lock = threading.Lock() 
        
        # Profiling info
        self.total_get_time = 0.0
        self.total_put_time = 0.0
        self.count_get = 0
        self.count_put = 0

    @property
    def is_shutdown(self):
        if self.queue is None:
            raise RuntimeError("SharedQueue must be initialized before using.")
        return self.queue.is_shutdown
    
    @property
    def mutex(self):
        if self.queue is None:
            raise RuntimeError("SharedQueue must be initialized before using.")
        return self.queue.mutex

    def init(self):
        if self.queue is None:
            if any(q.maxsize == 0 for q in self.shared):
                maxsize = 0
            else:
                maxsize = sum(q.maxsize for q in self.shared)
            queue = Queue(maxsize)
            for q in self.shared:
                assert q.queue is None, "SharedQueue has been unexpectedly initialized."
                q.queue = queue

    @property
    def upstream(self):
        return self._upstream

    @upstream.setter
    def upstream(self, value: 'SharedQueue'):
        if getattr(value, 'downstream', None) is not None and value.downstream is not self:
            raise RuntimeError("Cannot set upstream whose downstream is set and is not this queue.")
        shared = set((*self.shared, *value.shared))
        for q in shared:
            q.shared = shared
        self._upstream = value

    @property
    def downstream(self):
        return self._downstream

    @downstream.setter
    def downstream(self, value: 'SharedQueue'):
        if getattr(value, 'upstream', None) is not None and value.upstream is not self:
            raise RuntimeError("Cannot set downstream whose upstream is set and is not this queue.")
        shared = set((*self.shared, *value.shared))
        for q in shared:
            q.shared = shared
        self._downstream = value

    @property
    def getable(self):
        return self.downstream is None or self.downstream.upstream is self

    @property
    def putable(self):
        return self.upstream is None or self.upstream.downstream is self

    def get(self, block=True, timeout=None):
        if self.downstream is not None and self.downstream.upstream is not self:
            raise RuntimeError("Cannot get from a queue with downstream. Items have gone to downstream queue.")
        with self.get_lock:
            if self.on_get is not None:
                self.on_get()
            start_time = perf_counter()
            item = (self.upstream or self.queue).get(block=block, timeout=timeout)
            end_time = perf_counter()
            if not isinstance(item, EndOfInput):
                self.total_get_time += end_time - start_time
                self.count_get += 1
            return item
        
    def put(self, item, block=True, timeout=None):
        if self.upstream is not None and self.upstream.downstream is not self:
            raise RuntimeError("Cannot put to a queue with upstream. Items come from upstream queue.")
        with self.put_lock:
            start_time = perf_counter()
            (self.downstream or self.queue).put(item, block=block, timeout=timeout)
            end_time = perf_counter()
            if not isinstance(item, EndOfInput):
                self.total_put_time += end_time - start_time
                self.count_put += 1
            if self.on_put is not None:
                self.on_put()
            

    def qsize(self):
        if self.queue is None:
            raise RuntimeError("SharedQueue must be initialized before using.")
        return self.queue.qsize()

    def empty(self):
        if self.queue is None:
            raise RuntimeError("SharedQueue must be initialized before using.")
        return self.queue.empty()

    def full(self):
        if self.queue is None:
            raise RuntimeError("SharedQueue must be initialized before using.")
        return self.queue.full()
    
    def join(self):
        if self.queue is None:
            raise RuntimeError("SharedQueue must be initialized before using.")
        return self.queue.join()

    def put_nowait(self, item):
        return self.put(item, block=False)
    
    def get_nowait(self):
        return self.get(block=False)

    def shutdown(self, immediate=False):
        if self.queue is None:
            raise RuntimeError("SharedQueue must be initialized before using.")
        self.queue.shutdown(immediate=immediate)

    if sys.version_info >= (3, 9):
        from types import GenericAlias
        __class_getitem__ = classmethod(GenericAlias)




