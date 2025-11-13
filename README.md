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
    time.sleep(random.random()) # <= some slow computation
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
    data = range(10)              # Pass an iterable of data
    for result in pipe(data):     # Iterate over processed results
        print(f"Result: {result}")
```

### Profiling Performance

The pipeline provides built-in profiling of blocking time and throughput. 

```python
... # After running the pipeline, or at any time
print(pipe.profile_str())
```

```text
Node                        | Starvation | Overload | Backpressure | Efficiency | In-Throughput | Out-Throughput | In-Count | Out-Count
---------------------------------------------------------------------------------------------------------------------------------------
Sequential                  | 0.0 %      | 66.5 %   | 0.0 %        | -          | 2.47 it/s     | 1.23 it/s      | 10       | 5        
├─0 Worker(fn=slow_add)     | 0.0 %      | 66.5 %   | 0.0 %        | 87.4 %     | 2.47 it/s     | 2.47 it/s      | 10       | 10       
├─1 Parallel                | 85.0 %     | 0.0 %    | 0.0 %        | -          | 2.47 it/s     | 2.47 it/s      | 10       | 10       
│   ├─0 Worker(fn=slow_mul) | 54.8 %     | 0.0 %    | 0.0 %        | 45.1 %     | 1.01 s/it     | 1.01 s/it      | 4        | 4        
│   ├─1 Worker(fn=slow_mul) | 35.0 %     | 0.0 %    | 0.0 %        | 38.0 %     | 1.35 s/it     | 1.35 s/it      | 3        | 3        
│   └─2 Worker(fn=slow_mul) | 71.4 %     | 0.0 %    | 0.0 %        | 22.6 %     | 1.35 s/it     | 1.35 s/it      | 3        | 3        
└─2 Filter                  | 100.0 %    | 0.0 %    | 0.0 %        | -          | 2.47 it/s     | 1.23 it/s      | 10       | 5          
```


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