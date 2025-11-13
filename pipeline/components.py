from typing import *
from .queue import Empty, Full, ShutDown
from .queue import Queue
from threading import Thread, Event
import threading
import inspect
import time
import random
import itertools
import functools

__all__ = [
    'Worker', 
    'Source',
    'Batch',
    'Unbatch',
    'Buffer',
    'Sequential',
    'Parallel',
    'Distribute',
    'Broadcast',
    'Switch',
    'Router',
    'Filter'
]


DEFAULT_QUEUE_SIZE = 1


def _add_indent(s: str, num_spaces: int = 4) -> str:
    indent = ' ' * num_spaces
    return indent + s.replace('\n', '\n' + indent)

def _format_time(seconds: float) -> str:
    if seconds < 1e-3:
        return f"{seconds * 1e6:.2f}µs"
    elif seconds < 1:
        return f"{seconds * 1e3:.2f}ms"
    elif seconds < 60:
        return f"{seconds:.2f}s"
    elif seconds < 3600:
        return f"{seconds / 60:.2f}min"
    else:
        return f"{seconds / 3600:.2f}h"
    
class EndOfInput:
    pass


class ProfiledQueue(Queue):
    """A Queue that records the get/put block time for profiling purposes."""

    def __init__(self, maxsize: int = 0):
        super().__init__(maxsize)
        self.profiling_mutex = threading.Lock()
        self.total_get_block_time = 0.0
        self.total_put_block_time = 0.0
        self.count_get = 0
        self.count_put = 0

    def put(self, item, block=True, timeout=None):
        start_time = time.perf_counter()
        super().put(item, block, timeout)
        end_time = time.perf_counter()
        if not isinstance(item, EndOfInput):
            with self.profiling_mutex:
                self.total_put_block_time += end_time - start_time
                self.count_put += 1

    def get(self, block=True, timeout=None):
        start_time = time.perf_counter()
        item = super().get(block, timeout)
        end_time = time.perf_counter()
        if not isinstance(item, EndOfInput):
            with self.profiling_mutex:
                self.total_get_block_time += end_time - start_time
                self.count_get += 1
        return item


class Node:
    def __init__(self):
        self._in_queue_lock = threading.Lock()
        self._out_queue_lock = threading.Lock()
        self._input = None
        self._output = None
        self._is_started = False
        self._is_shutdown = False

    @property
    def input(self) -> ProfiledQueue:
        with self._in_queue_lock:
            if self._input is None:
                self._input = ProfiledQueue(maxsize=DEFAULT_QUEUE_SIZE)
        return self._input

    @input.setter
    def input(self, value: ProfiledQueue):
        with self._in_queue_lock:
            if self._input is not None:
                raise AttributeError("Node input is already set.")
            self._input = value

    @property
    def output(self) -> ProfiledQueue:
        with self._out_queue_lock:
            if self._output is None:
                self._output = ProfiledQueue(maxsize=DEFAULT_QUEUE_SIZE)
        return self._output
    
    @output.setter
    def output(self, value: ProfiledQueue):
        with self._out_queue_lock:
            if self._output is not None:
                raise AttributeError("Node output is already set.")
            self._output = value

    def start(self):
        "Start the node."
        self._is_started = True

    def shutdown(self):
        "Send shutdown signal without waiting for threads/processes to finish. NOTE: Once shutdown is called, the node cannot be restarted."
        self._is_shutdown = True
        self.input.shutdown()
        self.output.shutdown()

    def stop(self):
        "Stop the node and wait for all threads/processes to finish. NOTE: Once stop is called, the node cannot be restarted."
        self.shutdown()

    def put(self, data: Any, timeout: float = None) -> None:
        "Put data into the input queue. Blocks if the input queue is full."
        assert self._is_started, "Node is not started."
        self.input.put(data, timeout=timeout)
    
    def get(self, timeout: float = None) -> Any:
        "Get data from the output queue. Blocks if the output queue is empty."
        assert self._is_started, "Node is not started."
        return self.output.get(timeout=timeout)

    def put_nowait(self, data: Any) -> None:
        assert self._is_started, "Node is not started."
        self.input.put_nowait(data)
    
    def get_nowait(self) -> Any:
        assert self._is_started, "Node is not started."
        return self.output.get_nowait()

    def __enter__(self):
        self.start()
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.stop()

    def __call__(self, iterator: Iterable):
        assert self._is_started, "Node is not started."
        return NodeIterator(self, iterator)

    def get_profile_str(self) -> str:
        if not self._is_started:
            return "Not started"
        avg_put_block_time = self.input.total_put_block_time / max(1, self.input.count_put)
        avg_get_block_time = self.output.total_get_block_time / max(1, self.output.count_get)
        profile_str = f"Blocking Time Get/Put = {_format_time(avg_get_block_time)}/{_format_time(avg_put_block_time)}"\
                        f" | Count Get/Put = {self.output.count_get}/{self.input.count_put}"
        return profile_str

    def __repr__(self):
        return f"Node(class={self.__class__.__name__})   {self.get_profile_str()}"


class NodeIterator:
    def __init__(self, node: Node, iterator: Iterable):
        self.node = node
        self.iterator = iterator
        self.source_thread = Thread(target=self._source_thread_fn)
        self.source_thread.start()
    
    def __iter__(self):
        return self
    
    def _source_thread_fn(self):
        try:
            for item in self.iterator:
                self.node.put(item)
            self.node.put(EndOfInput())
        except ShutDown:
            pass
    
    def __next__(self):
        item = self.node.get()
        if isinstance(item, EndOfInput):
            raise StopIteration()
        return item


class ThreadingNode(Node):
    thread_functions: List[Callable]
    threads: List[Thread]

    def start(self):
        super().start()
        self.threads = [Thread(target=fn) for fn in self.thread_functions]
        for thread in self.threads:
            thread.start()

    def stop(self):
        super().stop()
        for thread in self.threads:
            thread.join()


class Worker(ThreadingNode):
    def __init__(self, work: Callable = None):
        super().__init__()
        self.work_fn = work
        self.thread_functions = [self.loop]
        self.working_time = 0.0
        self.count_work = 0

    def init(self) -> None:
        """
        This method is called the the thread is started, to initialize any resources that is only held in the thread.
        """
        pass

    def work(self, *args, **kwargs) -> Union[Any, Dict[str, Any]]:
        """
        This method defines the job that the node should do for each input item. 
        A item obtained from the input queue is passed as arguments to this method, and the result is placed in the output queue.
        The method is executed concurrently with other nodes.
        """
        return self.work_fn(*args, **kwargs)

    def loop(self):
        self.init()
        try:
            while True:
                item = self.input.get()
                if isinstance(item, EndOfInput):
                    self.output.put(EndOfInput())
                    continue
                start_time = time.perf_counter()
                result = self.work(item)
                end_time = time.perf_counter()
                self.working_time += end_time - start_time
                self.count_work += 1
                self.output.put(result)
        except ShutDown:
            return

    def get_profile_str(self) -> str:
        if not self._is_started:
            return "Not started."
        avg_put_block_time = self.input.total_put_block_time / max(1, self.input.count_put)
        avg_get_block_time = self.output.total_get_block_time / max(1, self.output.count_get)
        avg_work_time = self.working_time / max(1, self.count_work)
        efficiency = (avg_work_time) / (avg_work_time + avg_get_block_time + avg_put_block_time)
        profile_str = f"Blocking Time Get/Work/Put = {_format_time(avg_get_block_time)}/{_format_time(avg_work_time)}/{_format_time(avg_put_block_time)} | "\
                    f"Efficiency = {efficiency * 100:.1f}% | "\
                    f"Counts = {self.output.count_put}"
        return profile_str

    def __repr__(self):
        if self.work_fn is None:
            return f"Worker(class={self.__class__.__name__})   {self.get_profile_str()}"
        else:
            return f"Worker(fn={self.work_fn.__name__})   {self.get_profile_str()}"


class Source(ThreadingNode):
    """
    A node that provides data to successive nodes. It takes no input and provides data to the output queue.
    """
    def __init__(self, provide: Callable = None):
        super().__init__()
        self.provide_fn = provide
        self.thread_functions = [self.loop]

    def init(self) -> None:
        """
        This method is called the the thread or process is started, to initialize any resources that is only held in the thread or process.
        """
        pass

    def provide(self) -> Generator[Any, None, None]:
        for item in self.provide_fn():
            yield item

    def loop(self):
        self.init()
        try:
            for data in self.provide():
                self.output.put(data)
        except ShutDown:
            return

    def __repr__(self):
        if self.provide_fn is None:
            return f"Source(class={self.__class__.__name__})   {self.get_profile_str()}"
        else:
            return f"Source(fn={self.provide_fn.__name__})   {self.get_profile_str()}"


class Batch(ThreadingNode):
    """
    Groups every `batch_size` items into a batch (a list of items) and passes the batch to successive nodes.
    The `patience` parameter specifies the maximum time to wait for a batch to be filled before sending it to the next node,
    i.e., when the earliest item in the batch is out of `patience` seconds, the batch is sent regardless of its size.
    """
    def __init__(self, batch_size: int, patience: float = None):
        assert batch_size > 0, "Batch size must be greater than 0."
        super().__init__()
        self.batch_size = batch_size
        self.patience = patience
        self.thread_functions = [self.loop]

    def loop(self):
        try:
            while True:
                batch = []
                # Try to fill the batch
                for i in range(self.batch_size):
                    if i == 0 or self.patience is None:
                        # Wait forever for the first item or if patience is not set
                        timeout = None
                    else:
                        # Calculate the remaining time for the batch
                        timeout = self.patience - (time.time() - earliest_time)
                        if timeout < 0:
                            break
                    # Try to get an item within the remaining time
                    try:
                        item = self.input.get(timeout=timeout)
                    except Empty:
                        break
                    # If the item is EndOfInput, break the loop
                    if isinstance(item, EndOfInput):
                        break
                    # If the first item, start timing
                    if i == 0:
                        earliest_time = time.time()
                    batch.append(item)

                if len(batch) > 0:
                    self.output.put(batch)

                if isinstance(item, EndOfInput):
                    self.output.put(EndOfInput())
                    continue
        except ShutDown:
            return

    def __repr__(self):
        if self.patience is None:
            return f"Batch(size={self.batch_size})   {self.get_profile_str()}"
        else:
            return f"Batch(size={self.batch_size}, patience={self.patience})   {self.get_profile_str()}"


class Unbatch(ThreadingNode):
    """
    Ungroups every batch (a list of items) into individual items and passes them to successive nodes.
    """
    def __init__(self):
        super().__init__()
        self.thread_functions = [self.loop]

    def loop(self):
        try:
            while True:
                batch = self.input.get()
                if isinstance(batch, EndOfInput):
                    self.output.put(EndOfInput())
                    continue
                for item in batch:
                    self.output.put(item)
        except ShutDown:
            return

    def __repr__(self):
        return "Unbatch()"


class Buffer(ThreadingNode):
    def __init__(self, size: int):
        super().__init__()
        self.size = size
        self.thread_functions = [self.loop]
    
    @property
    def output(self):
        if self._output is None:
            self._output = Queue(maxsize=self.size)
        return self._output

    def loop(self):
        try:
            while True:
                item = self.input.get()
                self.output.put(item)
        except ShutDown:
            return

    def __repr__(self):
        return f"Buffer(size={self.size})   {self.get_profile_str()}"


class Sequential(Node):
    """
    Pipeline of nodes in sequential order, where each node takes the output of the previous node as input.
    The order of input and output items is preserved (FIFO)
    """
    nodes: List[Node]
    def __init__(self, nodes: List[Union[Node, Callable]]):
        super().__init__()
        self.nodes = []
        for node in nodes:
            if isinstance(node, Node):
                pass
            elif isinstance(node, Callable):
                if inspect.isgeneratorfunction(node):
                    node = Source(node)
                else:
                    node = Worker(node)
            else:
                raise ValueError(f"Invalid node type: {type(node)}")
            self.nodes.append(node)
        
        for node_pre, node_suc in zip(self.nodes[:-1], self.nodes[1:]):
            node_suc.input = node_pre.output
    
    @property
    def input(self) -> Queue:
        return self.nodes[0].input    

    @input.setter
    def input(self, value: Queue):
        self.nodes[0].input = value

    @property
    def output(self) -> Queue:
        return self.nodes[-1].output

    def start(self):
        super().start()
        for node in self.nodes:
            node.start()

    def stop(self):
        super().stop()
        for node in self.nodes:
            node.stop()

    def __repr__(self):
        return f"Sequential( {self.get_profile_str()}\n" + "\n".join([_add_indent(repr(node)) for node in self.nodes]) + "\n)"


class Parallel(ThreadingNode):
    """
    A FIFO node that runs multiple nodes in parallel to process the input items. Each input item is handed to one of the nodes whoever is available.
    NOTE: It is FIFO if and only if all the nested nodes are FIFO.
    """
    nodes: List[Node]
    working_nodes: Queue[Node]
    idle_nodes: Queue[Node]

    def __init__(self, nodes_or_callable: Union[Callable, Sequence[Node]], num_duplicates: int = None):
        super().__init__()
        self.num_duplicates = num_duplicates
        if isinstance(nodes_or_callable, Callable):
            assert num_duplicates is not None, "Duplicates count must be specified for callable"
            self.nodes = [Worker(nodes_or_callable) for _ in range(num_duplicates)]
        else:
            self.nodes = []
            for node in nodes_or_callable:
                if isinstance(node, Node):
                    pass
                elif isinstance(node, Callable):
                    if inspect.isgeneratorfunction(node):
                        node = Source(node)
                    else:
                        node = Worker(node)
                else:
                    raise ValueError(f"Invalid node type: {type(node)}")
                self.nodes.append(node)
        self.working_nodes = Queue()
        self.idle_nodes = Queue()
        for node in self.nodes:
            self.idle_nodes.put(node)
        self.thread_functions = [self._in_thread_fn, self._out_thread_fn]

    def _in_thread_fn(self):
        try:
            while True:
                item = self.input.get()
                node = self.idle_nodes.get()
                self.working_nodes.put(node)
                node.input.put(item)
        except ShutDown:
            return
    
    def _out_thread_fn(self):
        try:
            while True:
                node = self.working_nodes.get()
                item = node.output.get()
                self.output.put(item)
                self.idle_nodes.put(node)
        except ShutDown:
            return

    def start(self):
        super().start()
        for node in self.nodes:
            node.start()
    
    def shutdown(self):
        super().shutdown()
        self.working_nodes.shutdown()
        self.idle_nodes.shutdown()
        for node in self.nodes:
            node.shutdown()

    def stop(self):
        super().stop()
        for node in self.nodes:
            node.stop()

    def __repr__(self):
        return f"Parallel(   {self.get_profile_str()}\n" + "\n".join([_add_indent(repr(node)) for node in self.nodes]) + "\n)"


class Distribute(ThreadingNode):
    branches: Dict[str, Node]

    def __init__(self, branches: Dict[str, Node]):
        super().__init__()
        self.branches = {}
        for key, node in branches.items():
            if isinstance(node, Node):
                pass
            elif isinstance(node, Callable):
                if inspect.isgeneratorfunction(node):
                    raise ValueError("Source node is not allowed in Distribute block")
                else:
                    node = Worker(node)
            else:
                raise ValueError(f"Invalid node type: {type(node)}")
            self.branches[key] = node
        self.thread_functions = [self._in_thread_fn] + [self._out_thread_fn]
    
    def _in_thread_fn(self):
        try:
            while True:
                item = self.input.get()

                if isinstance(item, EndOfInput):
                    for node in self.branches.values():
                        node.input.put(EndOfInput())
                    continue

                if any(k not in self.branches for k in item) or any(k not in item for k in self.branches):
                    raise ValueError(f"Distribute keys mismatch. Input keys: {list(item.keys())}. Required keys: {list(self.branches.keys())}.")
                for k, v in item.items():
                    self.branches[k].input.put(v)
        except ShutDown:
            return

    def _out_thread_fn(self):
        try:
            while True:
                item = {k: node.output.get() for k, node in self.branches.items()}
                if all(isinstance(v, EndOfInput) for v in item.values()):
                    self.output.put(EndOfInput())
                    continue
                self.output.put(item)
        except ShutDown:
            return

    def start(self):
        for node in self.branches.values():
            node.start()
        super().start()

    def shutdown(self):
        for node in self.branches.values():
            node.shutdown()
        super().shutdown()  

    def stop(self):
        for node in self.branches.values():
            node.stop()
        super().stop()

    def __repr__(self):
        return f"Distribute(  {self.get_profile_str()}\n" + "\n".join([_add_indent(f"\"{k}\": {repr(v)}") for k, v in self.branches.items()]) + "\n)"


class Switch(ThreadingNode):
    branches: Dict[str, Node]
    fifo_order: Queue[str]

    def __init__(self, predicate: Callable[[Any], str], branches: Dict[str, Node]):
        self.predicate = predicate
        self.branches = {}
        for key, node in branches.items():
            if isinstance(node, Node):
                pass
            elif isinstance(node, Callable):
                if inspect.isgeneratorfunction(node):
                    raise ValueError("Source node is not allowed in Dispatch block")
                else:
                    node = Worker(node)
            else:
                raise ValueError(f"Invalid node type: {type(node)}")
            self.branches[key] = node
        self.fifo_order = Queue()

    def _in_thread_fn(self):
        try:
            while True:
                item = self.input.get()
                if isinstance(item, EndOfInput):
                    self.fifo_order.put(EndOfInput())
                    continue
                key = self.predicate(item)
                if key not in self.branches:
                    raise ValueError(f"Switch block key mismatches. \"{key}\" not in found in {list(self.branches.keys())}.")
                self.branches[key].input.put(item)
                self.fifo_order.put(key)
        except ShutDown:
            return

    def _out_thread_fn(self):
        try:
            while True:
                key = self.fifo_order.get()
                if isinstance(key, EndOfInput):
                    self.output.put(EndOfInput())
                    continue
                item = self.branches[key].output.get()
                self.output.put(item)
        except ShutDown:
            return
        
    def start(self):
        super().start()
        for node in self.branches.values():
            node.start()

    def shutdown(self):
        super().shutdown()
        self.fifo_order.shutdown()
        for node in self.branches.values():
            node.shutdown()

    def stop(self):
        super().stop()
        for node in self.branches.values():
            node.stop()

    def __repr__(self):
        return f"Switch(    {self.get_profile_str()}\n" + "\n".join([_add_indent(f"\"{k}\": {repr(v)}") for k, v in self.branches.items()]) + "\n)"
        

class Router(ThreadingNode):
    branches: Dict[str, Node]
    fifo_order: Queue[str]

    def __init__(self, predicate: Callable[[Any], List[str]], branches: Dict[str, Node]):
        self.predicate = predicate
        self.branches = {}
        for key, node in branches.items():
            if isinstance(node, Node):
                pass
            elif isinstance(node, Callable):
                if inspect.isgeneratorfunction(node):
                    raise ValueError("Source node is not allowed in Dispatch block")
                else:
                    node = Worker(node)
            else:
                raise ValueError(f"Invalid node type: {type(node)}")
            self.branches[key] = node
        self.fifo_order = Queue(self)

    def _in_thread_fn(self):
        try:
            while True:
                item = self.input.get()
                
                if isinstance(item, EndOfInput):
                    self.fifo_order.put(EndOfInput())
                    continue
                
                keys = self.predicate(item)
                if any(k not in self.branches for k in keys) or any(k not in keys for k in self.branches):
                    raise ValueError(f"Switch block key mismatches. Input keys: {list(keys)}. Expected keys: {list(self.branches.keys())}.")
                for key in keys:
                    self.branches[key].input.put(item)
                self.fifo_order.put(keys)
        except ShutDown:
            return

    def _out_thread_fn(self):
        try:
            while True:
                keys = self.fifo_order.get()
                if isinstance(keys, EndOfInput):
                    self.output.put(EndOfInput())
                    continue
                item = {k: self.branches[k].output.get() for k in keys}
                self.output.put(item)
        except ShutDown:
            return
    
    def shutdown(self):
        super().shutdown()
        self.fifo_order.shutdown()
        for node in self.branches.values():
            node.shutdown()

    def start(self):
        super().start()
        for node in self.branches.values():
            node.start()

    def stop(self):
        super().stop()
        for node in self.branches.values():
            node.stop()
    
    def __repr__(self):
        return f"Router(    {self.get_profile_str()}\n" + "\n".join([_add_indent(f"\"{k}\": {repr(v)}") for k, v in self.branches.items()]) + "\n)"
        

class Broadcast(ThreadingNode):
    branches: Union[List[Node], Dict[str, Node]]

    def __init__(self, branches: Union[List[Node], Dict[str, Node]]):
        
        if isinstance(branches, list):
            self.branches = []
            for node in branches:
                if isinstance(node, Node):
                    pass
                elif isinstance(node, Callable):
                    if inspect.isgeneratorfunction(node):
                        raise ValueError("Source node is not allowed in Broadcast block")
                    else:
                        node = Worker(node)
                else:
                    raise ValueError(f"Invalid node type: {type(node)}")
                self.branches.append(node)
        elif isinstance(branches, dict):
            self.branches = {}
            for key, node in branches.items():
                if isinstance(node, Node):
                    pass
                elif isinstance(node, Callable):
                    if inspect.isgeneratorfunction(node):
                        raise ValueError("Source node is not allowed in Broadcast block")
                    else:
                        node = Worker(node)
                else:
                    raise ValueError(f"Invalid node type: {type(node)}")
                self.branches[key] = node

    def _in_thread_fn(self):
        try:
            while True:
                item = self.input.get()
                if isinstance(self.branches, list):
                    for node in self.branches:
                        node.input.put(item)
                else:
                    for key, node in self.branches.items():
                        node.input.put(item)
        except ShutDown:
            return

    def _out_thread_fn(self):
        try:
            while True:
                if isinstance(self.branches, list):
                    item = [node.output.get() for node in self.branches]
                else:
                    item = {k: node.output.get() for k, node in self.branches.items()}

                if (isinstance(self.branches, list) and all(isinstance(v, EndOfInput) for v in item)) \
                    or (isinstance(self.branches, dict) and all(isinstance(v, EndOfInput) for v in item.values())):
                    self.output.put(EndOfInput())
                    continue

                self.output.put(item)
        except ShutDown:
            return
        
    def start(self):
        super().start()
        for node in self.branches.values():
            node.start()

    def stop(self):
        super().stop()
        for node in self.branches.values():
            node.stop()
    
    def __repr__(self):
        if isinstance(self.branches, list):
            return f"Broadcast(    {self.get_profile_str()}\n" + "\n".join(_add_indent(repr(v)) for v in self.branches) + "\n)"
        else:
            return f"Broadcast(    {self.get_profile_str()}\n" + "\n".join(_add_indent(f"\"{k}\": {repr(v)}") for k, v in self.branches.items()) + "\n)"


class Filter(ThreadingNode):
    """
    A node that filters items based on a predicate function. 
    If the predicate returns True, the item is passed to the output queue, otherwise it is discarded.
    """
    def __init__(self, predicate: Optional[Callable[[Any], bool]] = None):
        """
        Parameters
        ---
        - `predicate`: A function that takes an item and returns True if the item should be passed to the output queue. Default to pass items that are not None.
        """
        super().__init__()
        self.predicate = predicate
        self.thread_functions = [self.loop]

    def loop(self):
        try:
            while True:
                item = self.input.get()
                if isinstance(item, EndOfInput):
                    self.output.put(EndOfInput())
                    continue
                if (self.predicate is None and item is not None) or (self.predicate is not None and self.predicate(item)):
                    self.output.put(item)
        except ShutDown:
            return

    def __repr__(self):
        if self.predicate is None:
            return "Filter()    {self.get_profile_str()}"
        else:
            return f"Filter   {self.get_profile_str()}(fn={self.predicate.__name__})"