from typing import *
from threading import Thread, Event
import threading
import inspect
import time
import random
import itertools
import functools
from .queue import Empty, Full, ShutDown
from .queue import Queue, SharedQueue, EndOfInput
from queue import PriorityQueue
from .utils import format_time, format_throughput, format_table, format_percent


__all__ = [
    'Node',
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



_PROFILE_FORMATTER = {
    'Waiting for / Waited by Upstream': lambda x: format_percent(x[0]) + ' / ' + format_percent(x[1]),
    'Working': format_percent,
    'Waiting for / Waited by Downstream': lambda x: format_percent(x[0]) + ' / ' + format_percent(x[1]),
    'Throughput In / Out': lambda x: format_throughput(x[0]) + ' / ' + format_throughput(x[1]),
    'Count In / Out': lambda x: str(x[0]) + ' / ' + str(x[1]),
}

_PROFILE_COLUMNS = [
    'Node',
    "Problem",
    'Waiting for / Waited by Upstream',
    "Working",
    'Waiting for / Waited by Downstream',
    'Throughput In / Out',
    'Count In / Out',
]

_PROFILE_ALIGN = {
    'Node': 'left',
    'Waiting for / Waited by Upstream': 'center',
    "Working": 'right',
    'Waiting for / Waited by Downstream': 'center',
    "Problem": 'left',
    'Throughput In / Out': 'center',
    'Count In / Out': 'center',
}


class Node:
    nodes: Union[List['Node'], Dict[str, 'Node']]
    """Child nodes"""
    input: SharedQueue
    output: SharedQueue

    def __init__(self, name: str = None, maxsize: int = 1):
        self._name = name
        self.input = SharedQueue(maxsize=maxsize)
        self.output = SharedQueue(maxsize=maxsize)
        self.is_started = False
        self.is_shutdown = False

    @property
    def name(self) -> str:
        if self._name is not None:
            return self._name
        else:
            return self._default_name()
    
    @name.setter
    def name(self, value: str):
        self._name = value

    def _default_name(self):
        return f"{self.__class__.__name__}"

    def start(self):
        "Start the node."
        self.is_started = True
        self.start_time = time.perf_counter()
        self.input.init()
        self.output.init()

    def shutdown(self):
        "Send shutdown signal without waiting for threads/processes to finish. NOTE: Once shutdown is called, the node cannot be restarted."
        self.is_shutdown = True
        self.input.shutdown()
        self.output.shutdown()

    def stop(self):
        "Stop the node and wait for all threads/processes to finish. NOTE: Once stop is called, the node cannot be restarted."
        self.shutdown()

    def put(self, data: Any, timeout: float = None) -> None:
        "Put data into the input queue. Blocks if the input queue is full."
        assert self.is_started, "Node is not started."
        self.input.put(data, timeout=timeout)
    
    def get(self, timeout: float = None) -> Any:
        "Get data from the output queue. Blocks if the output queue is empty."
        assert self.is_started, "Node is not started."
        return self.output.get(timeout=timeout)

    def put_nowait(self, data: Any) -> None:
        assert self.is_started, "Node is not started."
        self.input.put_nowait(data)
    
    def get_nowait(self) -> Any:
        assert self.is_started, "Node is not started."
        return self.output.get_nowait()

    def __enter__(self):
        self.start()
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.stop()

    def __call__(self, iterator: Iterable):
        assert self.is_started, "Node is not started."
        return NodeIterator(self, iterator)

    def _profile_single(self) -> Dict[str, str]:
        if not self.is_started:
            return {}
        running_time = time.perf_counter() - self.start_time
        waiting_for_upstream = self.input.total_get_time / running_time if self.input.getable else None
        waited_by_upstream = self.input.total_put_time / running_time if self.input.putable else None
        waiting_for_downstream = self.output.total_put_time / running_time if self.output.putable else None
        waited_by_downstream = self.output.total_get_time / running_time if self.output.getable else None
        upstream_pressure = (waited_by_upstream or 0) - (waiting_for_upstream or 0)
        downstream_pressure = (waited_by_downstream or 0) - (waiting_for_downstream or 0)
        if upstream_pressure > 0 and downstream_pressure > 0:
            problem_type = 'Bottleneck'
        elif upstream_pressure > 0 and downstream_pressure <= 0:
            problem_type = 'Downstream-Bounded'
        elif upstream_pressure <= 0 and downstream_pressure > 0:
            problem_type = 'Upstream-Bounded'
        else:
            problem_type = 'Idle'
        abs_pressure = max(abs(upstream_pressure), abs(downstream_pressure))
        if abs_pressure >= 0.5: 
            severity = 'Severe'
        elif abs_pressure >= 0.25:
            severity = 'Moderate'
        elif abs_pressure >= 0.05:
            severity = 'Minor'
        else:
            severity = 'None'
        if severity != 'None':
            problem = severity + ' ' + problem_type
        else:
            problem = 'None'

        profile = {
            "Waiting for / Waited by Upstream": (waiting_for_upstream, waited_by_upstream),
            "Waiting for / Waited by Downstream": (waiting_for_downstream, waited_by_downstream),
            "Problem": problem,
            "Throughput In / Out": (self.input.count_get / running_time, self.output.count_put / running_time),
            "Count In / Out": (self.input.count_get, self.output.count_put),
        }
        return profile

    def iterate_tree(self, parents: Tuple['Node', ...] = None) -> Iterable[Tuple[Tuple[Tuple['Node', str], ...], 'Node']]:
        if parents is None:
            parents = tuple()
        yield parents, self
        if hasattr(self, 'nodes'):
            for key, node in self.nodes.items() if isinstance(self.nodes, dict) else enumerate(self.nodes):
                yield from node.iterate_tree(parents + ((self, key),))

    def profile(self) -> str:
        profiles = []
        for node_tree_name, (_, node) in zip(self.format_tree(), self.iterate_tree()):
            profiles.append({
                "Node": node_tree_name,
                **node._profile_single()
            })
        return format_table(profiles, sep=" | ", fill='-', formatter=_PROFILE_FORMATTER, columns=_PROFILE_COLUMNS, align=_PROFILE_ALIGN)

    def format_tree(self) -> List[str]:
        if not hasattr(self, 'nodes'):
            return [self.name]
        elif isinstance(self.nodes, (dict, list)):
            lines = [self.name]
            for i, (key, node) in enumerate(self.nodes.items() if isinstance(self.nodes, dict) else enumerate(self.nodes)):
                key = str(key)
                child_tree = node.format_tree()
                indent = len(key) + 2
                for j, line in enumerate(child_tree):
                    right = "└" if i == len(self.nodes) - 1 else "├"
                    down = "│"
                    if j == 0:
                        lines.append(f"{right}─{key} {line}")
                    else:
                        lines.append(down + ' ' * indent + line)
            return lines

    def __repr__(self):
        tree_lines = self.format_tree()
        return '\n'.join(tree_lines)


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
    def __init__(self, work: Callable = None, name: str = None):
        super().__init__(name=name)
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

    def _profile_single(self) -> Dict[str, str]:
        if not self.is_started:
            return {}
        profile = super()._profile_single()
        running_time = time.perf_counter() - self.start_time
        profile.update({
            "Working": self.working_time / running_time
        })
        return profile
    
    def _default_name(self):
        if self.work_fn is None:
            return f"Worker(class={self.__class__.__name__})"
        else:
            return f"Worker(fn={self.work_fn.__name__})"


class Source(ThreadingNode):
    """
    A node that provides data to successive nodes. It takes no input and provides data to the output queue.
    """
    def __init__(self, provide: Callable = None, name: str = None):
        super().__init__(name=name)
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

    def _default_name(self):
        if self.provide_fn is None:
            return f"Source(class={self.__class__.__name__})"
        else:
            return f"Source(fn={self.provide_fn.__name__})"


class Batch(ThreadingNode):
    """
    Groups every `batch_size` items into a batch (a list of items) and passes the batch to successive nodes.
    The `patience` parameter specifies the maximum time to wait for a batch to be filled before sending it to the next node,
    i.e., when the earliest item in the batch is out of `patience` seconds, the batch is sent regardless of its size.
    """
    def __init__(self, batch_size: int, patience: float = None, name: Optional[str] = None):
        """
        Parameters
        ---
        - `batch_size`: The size of each batch.
        - `patience`: Maximum time to wait for a batch to be filled before sending it to the next node. Default to None (wait indefinitely).
        """
        assert batch_size > 0, "Batch size must be greater than 0."
        super().__init__(name=name)
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

    def _default_name(self):
        if self.patience is None:
            return f"Batch(size={self.batch_size})"
        else:
            return f"Batch(size={self.batch_size}, patience={self.patience})"


class Unbatch(ThreadingNode):
    """
    Ungroups every batch (a list of items) into individual items and passes them to successive nodes.
    """
    def __init__(self, name: Optional[str] = None):
        super().__init__(name=name)
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


class Buffer(Node):
    def __init__(self, size: int, name: Optional[str] = None):
        super().__init__(name=name, maxsize=size)
        self.size = size
        self.input.downstream = self.output
        self.output.upstream = self.input

    def _default_name(self):
        return f"Buffer(size={self.size})"


class Filter(ThreadingNode):
    """
    A node that filters items based on a predicate function. 
    If the predicate returns True, the item is passed to the output queue, otherwise it is discarded.
    """
    def __init__(self, predicate: Optional[Callable[[Any], bool]] = None, name: Optional[str] = None):
        """
        Parameters
        ---
        - `predicate`: A function that takes an item and returns True if the item should be passed to the output queue. Default to pass items that are not None.
        """
        super().__init__(name=name)
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

    def _default_name(self):
        if self.predicate is None:
            return f"Filter()"
        else:
            return f"Filter(fn={self.predicate.__name__})"
        

class Sequential(Node):
    """
    Pipeline of nodes in sequential order, where each node takes the output of the previous node as input.
    The order of input and output items is preserved (FIFO)
    """
    nodes: List[Node]
    def __init__(self, nodes: List[Union[Node, Callable]], name: Optional[str] = None):
        super().__init__(name=name)
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
        
        self.input.downstream = self.nodes[0].input
        self.nodes[0].input.upstream = self.input
        for node_prev, node_next in zip(self.nodes[:-1], self.nodes[1:]):
            node_prev.output.downstream = node_next.input
            node_next.input.upstream = node_prev.output
        self.nodes[-1].output.downstream = self.output
        self.output.upstream = self.nodes[-1].output

    def start(self):
        super().start()
        for node in self.nodes:
            node.start()

    def stop(self):
        super().stop()
        for node in self.nodes:
            node.stop()


class Parallel(ThreadingNode):
    """
    A FIFO node that runs multiple nodes in parallel to process the input items. Each input item is handed to one of the nodes whoever is available.
    NOTE: It is FIFO if and only if all the nested nodes are FIFO.
    """
    nodes: List[Node]
    fifo_order: Queue[int]

    def __init__(self, nodes_or_callable: Union[Callable, Sequence[Union[Node, Callable]]], num_duplicates: int = None, name: Optional[str] = None):
        super().__init__(name=name)
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
        self.fifo_order = Queue()
        for i, node in enumerate(self.nodes):
            node.input.upstream = self.input
            node.input.on_get = functools.partial(self._on_get_callback, i)
        self.thread_functions = [self._out_thread_fn]

    def _on_get_callback(self, i: int):
        self.fifo_order.put(i)
    
    def _out_thread_fn(self):
        try:
            while True:
                idx = self.fifo_order.get()
                item = self.nodes[idx].output.get()
                self.output.put(item)
        except ShutDown:
            return

    def start(self):
        super().start()
        for node in self.nodes:
            node.start()
    
    def shutdown(self):
        super().shutdown()
        self.fifo_order.shutdown()
        for node in self.nodes:
            node.shutdown()

    def stop(self):
        super().stop()
        for node in self.nodes:
            node.stop()


class Distribute(ThreadingNode):
    nodes: Dict[str, Node]

    def __init__(self, nodes: Dict[str, Node], name: Optional[str] = None):
        super().__init__(name=name)
        self.nodes = {}
        for key, node in nodes.items():
            if isinstance(node, Node):
                pass
            elif isinstance(node, Callable):
                if inspect.isgeneratorfunction(node):
                    raise ValueError("Source node is not allowed in Distribute block")
                else:
                    node = Worker(node)
            else:
                raise ValueError(f"Invalid node type: {type(node)}")
            self.nodes[key] = node
        self.thread_functions = [self._in_thread_fn] + [self._out_thread_fn]
    
    def _in_thread_fn(self):
        try:
            while True:
                item = self.input.get()

                if isinstance(item, EndOfInput):
                    for node in self.nodes.values():
                        node.input.put(EndOfInput())
                    continue

                if any(k not in self.nodes for k in item) or any(k not in item for k in self.nodes):
                    raise ValueError(f"Distribute keys mismatch. Input keys: {list(item.keys())}. Required keys: {list(self.nodes.keys())}.")
                for k, v in item.items():
                    self.nodes[k].input.put(v)
        except ShutDown:
            return

    def _out_thread_fn(self):
        try:
            while True:
                item = {k: node.output.get() for k, node in self.nodes.items()}
                if all(isinstance(v, EndOfInput) for v in item.values()):
                    self.output.put(EndOfInput())
                    continue
                self.output.put(item)
        except ShutDown:
            return

    def start(self):
        for node in self.nodes.values():
            node.start()
        super().start()

    def shutdown(self):
        for node in self.nodes.values():
            node.shutdown()
        super().shutdown()  

    def stop(self):
        for node in self.nodes.values():
            node.stop()
        super().stop()


class Switch(ThreadingNode):
    nodes: Dict[str, Node]
    fifo_order: Queue[str]

    def __init__(self, predicate: Callable[[Any], str], nodes: Dict[str, Node], name: Optional[str] = None):
        super().__init__(name=name)
        self.predicate = predicate
        self.nodes = {}
        for key, node in nodes.items():
            if isinstance(node, Node):
                pass
            elif isinstance(node, Callable):
                if inspect.isgeneratorfunction(node):
                    raise ValueError("Source node is not allowed in Dispatch block")
                else:
                    node = Worker(node)
            else:
                raise ValueError(f"Invalid node type: {type(node)}")
            self.nodes[key] = node
        self.fifo_order = Queue()

    def _in_thread_fn(self):
        try:
            while True:
                item = self.input.get()
                if isinstance(item, EndOfInput):
                    self.fifo_order.put(EndOfInput())
                    continue
                key = self.predicate(item)
                if key not in self.nodes:
                    raise ValueError(f"Switch block key mismatches. \"{key}\" not in found in {list(self.nodes.keys())}.")
                self.nodes[key].input.put(item)
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
                item = self.nodes[key].output.get()
                self.output.put(item)
        except ShutDown:
            return
        
    def start(self):
        super().start()
        for node in self.nodes.values():
            node.start()

    def shutdown(self):
        super().shutdown()
        self.fifo_order.shutdown()
        for node in self.nodes.values():
            node.shutdown()

    def stop(self):
        super().stop()
        for node in self.nodes.values():
            node.stop()


class Router(ThreadingNode):
    nodes: Dict[str, Node]
    fifo_order: Queue[str]

    def __init__(self, predicate: Callable[[Any], List[str]], nodes: Dict[str, Union[Node, Callable]], name: Optional[str] = None):
        super().__init__(name=name)
        self.predicate = predicate
        self.nodes = {}
        for key, node in nodes.items():
            if isinstance(node, Node):
                pass
            elif isinstance(node, Callable):
                if inspect.isgeneratorfunction(node):
                    raise ValueError("Source node is not allowed in Dispatch block")
                else:
                    node = Worker(node)
            else:
                raise ValueError(f"Invalid node type: {type(node)}")
            self.nodes[key] = node
        self.fifo_order = Queue(self)

    def _in_thread_fn(self):
        try:
            while True:
                item = self.input.get()
                
                if isinstance(item, EndOfInput):
                    self.fifo_order.put(EndOfInput())
                    continue
                
                keys = self.predicate(item)
                if any(k not in self.nodes for k in keys) or any(k not in keys for k in self.nodes):
                    raise ValueError(f"Switch block key mismatches. Input keys: {list(keys)}. Expected keys: {list(self.nodes.keys())}.")
                for key in keys:
                    self.nodes[key].input.put(item)
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
                item = {k: self.nodes[k].output.get() for k in keys}
                self.output.put(item)
        except ShutDown:
            return
    
    def shutdown(self):
        super().shutdown()
        self.fifo_order.shutdown()
        for node in self.nodes.values():
            node.shutdown()

    def start(self):
        super().start()
        for node in self.nodes.values():
            node.start()

    def stop(self):
        super().stop()
        for node in self.nodes.values():
            node.stop()


class Broadcast(ThreadingNode):

    def __init__(self, nodes: Union[List[Union[Node, Callable]], Dict[str, Union[Node, Callable]]], name: Optional[str] = None):
        super().__init__(name=name)
        if isinstance(nodes, list):
            self.nodes = []
            for node in nodes:
                if isinstance(node, Node):
                    pass
                elif isinstance(node, Callable):
                    if inspect.isgeneratorfunction(node):
                        raise ValueError("Source node is not allowed in Broadcast block")
                    else:
                        node = Worker(node)
                else:
                    raise ValueError(f"Invalid node type: {type(node)}")
                self.nodes.append(node)
        elif isinstance(nodes, dict):
            self.nodes = {}
            for key, node in nodes.items():
                if isinstance(node, Node):
                    pass
                elif isinstance(node, Callable):
                    if inspect.isgeneratorfunction(node):
                        raise ValueError("Source node is not allowed in Broadcast block")
                    else:
                        node = Worker(node)
                else:
                    raise ValueError(f"Invalid node type: {type(node)}")
                self.nodes[key] = node

    def _in_thread_fn(self):
        try:
            while True:
                item = self.input.get()
                if isinstance(self.nodes, list):
                    for node in self.nodes:
                        node.input.put(item)
                else:
                    for key, node in self.nodes.items():
                        node.input.put(item)
        except ShutDown:
            return

    def _out_thread_fn(self):
        try:
            while True:
                if isinstance(self.nodes, list):
                    item = [node.output.get() for node in self.nodes]
                else:
                    item = {k: node.output.get() for k, node in self.nodes.items()}

                if (isinstance(self.nodes, list) and all(isinstance(v, EndOfInput) for v in item)) \
                    or (isinstance(self.nodes, dict) and all(isinstance(v, EndOfInput) for v in item.values())):
                    self.output.put(EndOfInput())
                    continue

                self.output.put(item)
        except ShutDown:
            return
        
    def start(self):
        super().start()
        for node in self.nodes.values():
            node.start()

    def stop(self):
        super().stop()
        for node in self.nodes.values():
            node.stop()

