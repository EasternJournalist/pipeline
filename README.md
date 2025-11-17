# Pipeline

A multithreaded Python framework for building concurrent data pipelines.

* **Minimal code and intuitive usage**.
* **Targets to I/O and NumPy/PyTorch data workloads**.
* **Handles complex, branched pipelines**.
* **Guarantees FIFO data order**.
* **Built-in profiling of performance**.

## Installation

```
pip install git+https://github.com/EasternJournalist/pipeline.git
```

## Quick Start

Let's build a simple pipeline that performs the following operations:
- Adds 1 to each input number.
- Multiplies the result by 3 (slowly). To make it *faster*, use 3 parallel workers for this step.
- Filters out odd results, keeping only even numbers.


```python
import pipeline
import time
import random

# Example functions
def slow_add(x):
    time.sleep(0.2) # <= some slow computation
    return x + 1

def slow_mul(x):
    time.sleep(random.random()) # <= some slow computation
    return x * 3

# Building the pipeline
pipe = pipeline.Sequential([
    slow_add,
    pipeline.Parallel(slow_mul, num_duplicates=3), 
    pipeline.Filter(lambda x: x % 2 == 0)
])
print(pipe)
# Sequential
# ├─0 Worker(fn=slow_add)
# ├─1 Parallel
# │   ├─0 Worker(fn=slow_mul)
# │   ├─1 Worker(fn=slow_mul)
# │   └─2 Worker(fn=slow_mul)
# └─2 Filter(fn=<lambda>)

# Start the pipeline
with pipe:  
    data = range(20)              # Pass an iterable of data
    for result in pipe(data):     # Iterate over processed results
        print(f"Result: {result}")

```

## More Usage

### Available Components

Beyond `Parallel` and `Sequential`, the following components are available for flow control, multi-branch routing, buffering, and batching:

<table>
  <thead>
    <tr>
      <th>Category</th>
      <th>Component</th>
      <th>Description</th>
    </tr>
  </thead>
  <tbody>
    <!-- Basic Units -->
    <tr>
      <td rowspan="2">Basic</td>
      <td><code>Worker</code></td>
      <td>Applies a user-defined function to each input item.</td>
    </tr>
    <tr>
      <td><code>Source</code></td>
      <td>Generates data into the pipeline; usually the starting point.</td>
    </tr>
    <!-- Execution -->
    <tr>
      <td rowspan="2">Structural</td>
      <td><code>Sequential</code></td>
      <td>Pipeline of nodes in a sequential order.</td>
    </tr>
    <tr>
      <td><code>Parallel</code></td>
      <td>Runs a pool of parallel nodes.</td>
    </tr>
    <!-- Batching -->
    <tr>
      <td rowspan="4">Batching & Flow Control</td>
      <td><code>Batch</code></td>
      <td>Groups incoming items into batches of a given size, or within time of patience.</td>
    </tr>
    <tr>
      <td><code>Unbatch</code></td>
      <td>Splits batched input into individual items.</td>
    </tr>
    <tr>
      <td><code>Buffer</code></td>
      <td>Buffers items in a queue between upstream and downstream stages.</td>
    </tr>
    <tr>
      <td><code>Filter</code></td>
      <td>Filter items.</td>
    </tr>
    <!-- Routing -->
    <tr>
      <td rowspan="4">Multi-Branch Routing</td>
      <td><code>Distribute</code></td>
      <td>Takes a dictionary input and sends each value to corresponding named branch.
    </tr>
    <tr>
      <td><code>Broadcast</code></td>
      <td>Sends a copy of input to all branches.</td>
    </tr>
    <tr>
      <td><code>Switch</code></td>
      <td>Uses a key function to send data to a single selected branch.</td>
    </tr>
    <tr>
      <td><code>Router</code></td>
      <td>Uses a key function to send data to multiple selected branches.</td>
    </tr>
  </tbody>
</table>


### Profiling Performance

The pipeline provides built-in profiling of blocking time and throughput. 

```python
... # After running the pipeline, or at any time
print(pipe.profile())
```

```text
Node                        |      Indicator      | Waiting In / Out  |  Waited In / Out  | Working |  Throughput In / Out  | Count In / Out
--------------------------- | ------------------- | ----------------- | ----------------- | ------- | --------------------- | --------------
Sequential                  |      >>> / <<<<     |   0.0 % /   0.0 % |  69.3 % / 100.0 % |       - | 4.07 it/s / 2.04 it/s |    20 / 10    
├─0 Worker(fn=slow_add)     |      >>> / <<       |   0.0 % /   0.0 % |  69.3 % /  41.1 % |  81.5 % | 4.07 it/s / 4.07 it/s |    20 / 20    
├─1 Parallel                |       << / <<<<     |  41.1 % /   0.0 % |   0.0 % / 100.0 % |       - | 4.07 it/s / 4.07 it/s |    20 / 20    
│   ├─0 Worker(fn=slow_mul) |        < / <<<      |   8.4 % /   0.0 % |   0.0 % /  54.2 % |  80.1 % | 1.22 it/s / 1.22 it/s |     6 / 6     
│   ├─1 Worker(fn=slow_mul) |        < / <        |  23.3 % /   1.1 % |   0.0 % /  21.2 % |  75.5 % | 1.63 it/s / 1.63 it/s |     8 / 8     
│   └─2 Worker(fn=slow_mul) |        < / <        |  21.7 % /   0.0 % |   0.0 % /  24.6 % |  66.1 % | 1.22 it/s / 1.22 it/s |     6 / 6     
└─2 Filter(fn=<lambda>)     |     <<<< / <<<<     | 100.0 % /   0.0 % |   0.0 % /  99.9 % |       - | 4.07 it/s / 2.04 it/s |    20 / 10      
```

The indicators summarize each node’s blocking status and can be read as **`Upstream vs Me / Me vs Downstream`**.  
Symbols show who is faster: `>` means “left is faster and waiting for right,” `<` means the opposite.  
The number of symbols reflects blocking severity:  
`=`: 0–5%, `>`: 5–25%, `>>`: 25–50%, `>>>`: 50–75%, `>>>>`: 75–100%.

A practical rule is to **follow the arrows**—they point toward the bottleneck.  
For example:  
- `>>> / <<<` means *this node* is the bottleneck.  
- `<<< / <<<` means the *upstream node* is too slow.

If both `>` and `<` appear on the same side, it usually indicates an unstable or fluctuating stream where the node alternates between waiting and being waited on. Adding buffering components can help smooth out these fluctuations.

Here is a quick reference for the indicators:

Indicator | Problem Type | Suggested Actions
-------------|-------------| -----------------
`>>> / <<<` | Bottleneck | Consider optimizing this node / adding more parallelism.
`<<< / <<<` | Upstream slow | Consider improving upstream performance.
`>>> / >>>` | Downstream slow | Consider improving downstream performance.
`<<< / >>>` | Idle | This node may be over-provisioned.
`>><< / >><<` | Fluctuating | Consider adding buffering in between stages to smooth out fluctuations.


### Exception Handling

When a worker node raises an exception during processing, the exception is propagated to the pipeline output and will be raised as an `ExceptionInNode` when calling `get()` or iterating over results. This ensures errors are not silently ignored and allows the main thread to handle them.

```python
with pipe:  
    data = range(20)              # Pass an iterable of data
    iterator = pipe(data)         # Get an iterator over processed results
    while True:
        try:
            result = next(iterator)  # Get next processed result
            print(f"Result: {result}")
        except pipeline.ExceptionInNode as e:
            print(f"Caught an exception from node '{e.node.name}': {e.original_exception}")
        except StopIteration:
            break
```

<details>
<summary>
Exception behaviors (Click to expand)
</summary>

- Behavior of the node that raises an exception
  - *Stops processing*: The node will no longer handle any further inputs.
  - *Cannot automatically recover*: The node is considered "dead" for the remainder of the pipeline’s lifetime.
- Propagation of exceptions in the pipeline
  - *Downstream nodes continue working*: Exceptions are passed along as results without stopping downstream nodes.
  - *Result order is preserved*: Exceptions occupy the position corresponding to their input, maintaining the overall output order.
- Handling in the main thread and default behavior
  - *Default behavior if uncaught*: An uncaught ExceptionInNode will typically exit the pipeline context, shutting down all nodes and terminating the program. This prevents partially failed nodes from silently continuing.
  - Continuing after an exception: You can catch ExceptionInNode in the main thread to allow the pipeline to keep running.
- Effect of exceptions on different component types
  - *Redundant components* (e.g., Parallel):
    - Only the failing worker stops.
    - Other parallel workers continue processing their tasks.
    - The pipeline continues, but with reduced parallelism.
  - *Non-redundant components* (e.g., Sequential):
    - The failing node can no longer produce outputs.
    - Downstream nodes are blocked waiting for input.
    - The component (and possibly the pipeline) cannot progress further.
- Output order after exceptions
  - *Order is always preserved*: Exceptions do not disrupt the correspondence between input and output.
  - *Catching exceptions does not break the sequence*: If you catch the exception and continue iterating, subsequent valid results will still be yielded in order.

</details>


