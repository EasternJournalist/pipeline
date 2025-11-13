import sys
from time import time, perf_counter
import threading

if sys.version_info >= (3, 13):
    # Use the built-in shutdownable queue for Python 3.13 and above
    from queue import Queue, ShutDown, Full, Empty
else:
    # Define a shutdownable queue for Python versions < 3.13
    # Copied from Python 3.13's queue module 
    class ShutDown(Exception):
        """Raised when put/get with shut-down queue."""
        pass
    
    from queue import Full, Empty
    from queue import Queue as _Queue
    
    class Queue(_Queue):
        def __init__(self, maxsize=0):
            super().__init__(maxsize)
            # Queue shutdown state
            self.is_shutdown = False
        
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
                if self.maxsize > 0:
                    if not block:
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
                self._put(item)
                self.unfinished_tasks += 1
                self.not_empty.notify()

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
                if not block:
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
                item = self._get()
                self.not_full.notify()
                return item
