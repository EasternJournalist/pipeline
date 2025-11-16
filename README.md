# Pipeline

A multithreaded Python framework for building concurrent data pipelines.

* **Minimal code and intuitive usage**.
* **Targets to I/O and NumPy/PyTorch data workloads**.
* **Handles complex, branched pipelines**.
* **Guarantees FIFO data order**.

## Installation

```
pip install git+https://github.com/EasternJournalist/pipeline.git
```


## Usage

### Quick Start

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

### Profiling Performance

The pipeline provides built-in profiling of blocking time and throughput. 

```python
... # After running the pipeline, or at any time
print(pipe.profile())
```

```text
Node                        | Problem                   | Waiting for / Waited by Upstream | Working | Waiting for / Waited by Downstream |  Throughput In / Out  | Count In / Out
--------------------------- | ------------------------- | -------------------------------- | ------- | ---------------------------------- | --------------------- | --------------
Sequential                  | Severe Bottleneck         |          0.0 % /  77.6 %         |       - |           0.0 % /  99.9 %          | 4.56 it/s / 2.28 it/s |    20 / 10    
├─0 Worker(fn=slow_add)     | Severe Bottleneck         |          0.0 % /  77.6 %         |  91.3 % |           0.0 % /  85.1 %          | 4.56 it/s / 4.56 it/s |    20 / 20    
├─1 Parallel                | Severe Upstream-Bounded   |         85.1 % /   0.0 %         |       - |           0.0 % /  99.9 %          | 4.56 it/s / 4.56 it/s |    20 / 20    
│   ├─0 Worker(fn=slow_mul) | Severe Upstream-Bounded   |         50.2 % /      -          |  47.7 % |           0.0 % /  31.3 %          | 1.60 it/s / 1.60 it/s |     7 / 7     
│   ├─1 Worker(fn=slow_mul) | Moderate Upstream-Bounded |         37.0 % /      -          |  63.0 % |           0.0 % /  45.7 %          | 1.37 it/s / 1.37 it/s |     6 / 6     
│   └─2 Worker(fn=slow_mul) | Moderate Upstream-Bounded |         48.9 % /      -          |  45.0 % |           4.0 % /  22.9 %          | 1.60 it/s / 1.60 it/s |     7 / 7     
└─2 Filter(fn=<lambda>)     | Severe Upstream-Bounded   |        100.0 % /   0.0 %         |       - |           0.0 % / 100.0 %          | 4.56 it/s / 2.28 it/s |    20 / 10 
```

> Tips for interpreting the profile:
>
> Problem type | Description | Suggested Actions
> -------------|-------------| -----------------
> Bottleneck | Waited by both upstream and downstream. | Consider optimizing this node / adding more parallelism.
> Upstream-Bounded | Waiting for upstream & Waited by downstream. | Consider improving upstream performance.
> Downstream-Bounded | Waited by upstream & Waiting for downstream. | Consider improving downstream performance.
> Idle | Waiting for both upstream and downstream. | This node may be over-provisioned.

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