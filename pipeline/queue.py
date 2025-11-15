import sys
from time import time
import threading
from typing import Callable

from queue import Full, Empty
from queue import Queue as _Queue

# Define a shutdownable queue for Python versions < 3.13
# Copied from Python 3.13's queue module 
class ShutDown(Exception):
    """Raised when put/get with shut-down queue."""
    pass


class Queue(_Queue):
    '''An extension of queue.Queue, with
    - backported shutdown() method from Python 3.13
    - waiters and putters count
    - callbacks on put() and get()
    '''
    on_get: Callable[['Queue'], None] | None
    "Callback before each get() operation."
    on_put: Callable[['Queue'], None] | None
    "Callback after each put() operation."
    get_waiters: int
    put_waiters: int
    is_shutdown: bool

    def __init__(self, maxsize: int = 0):
        self.maxsize = maxsize
        self._init(maxsize)

        # mutex must be held whenever the queue is mutating.  All methods
        # that acquire mutex must release it before returning.  mutex
        # is shared between the three conditions, so acquiring and
        # releasing the conditions also acquires and releases mutex.
        self.mutex = threading.RLock()  # NOTE: different from queue.Queue, using RLock here

        # Notify not_empty whenever an item is added to the queue; a
        # thread waiting to get is notified then.
        self.not_empty = threading.Condition(self.mutex)

        # Notify not_full whenever an item is removed from the queue;
        # a thread waiting to put is notified then.
        self.not_full = threading.Condition(self.mutex)

        # Notify all_tasks_done whenever the number of unfinished tasks
        # drops to zero; thread waiting to join() is notified to resume
        self.all_tasks_done = threading.Condition(self.mutex)
        self.unfinished_tasks = 0

        # Backport shutdown feature from Python 3.13
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
            if self.on_get is not None:
                self.on_get()
            self.get_waiters += 1
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
        

class SharedQueue:
    queue: Queue
    "The actual queue instance."
    on_get: Callable[['Queue'], None] | None
    "Callback before each get() operation."
    on_put: Callable[['Queue'], None] | None
    "Callback after each put() operation."

    def __init__(self, maxsize: int = 0):
        self.maxsize = maxsize
        self.shared_queues = set()
        self.queue = None
        self.mutex = threading.Lock()
    
    def get(self, block=True, timeout=None):
        return self.queue.get(block=block, timeout=timeout)

    def put (self, block=True, timeout=None):
        return self.queue.get(block=block, timeout=timeout)
