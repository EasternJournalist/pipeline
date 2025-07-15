# Pipeline

Multithread python pipeline framework.

* Preserving first-in-first-out order of data.
* Supporting complex and non-linear data flow.
* Maximizing thouroughput. Each stage runs in parallel. 
* Easy and intuitive. Minimal code to build a pipeline. 

# Componets

<table>
  <thead>
    <tr>
      <th>Category</th>
      <th>Component</th>
      <th>Description</th>
      <th>Typical Behavior</th>
    </tr>
  </thead>
  <tbody>
    <!-- Basic Units -->
    <tr>
      <td rowspan="2">Basic</td>
      <td><code>Worker</code></td>
      <td>Applies a user-defined function to each input item.</td>
      <td>Stateless map operation</td>
    </tr>
    <tr>
      <td><code>Source</code></td>
      <td>Generates data into the pipeline; usually the starting point.</td>
      <td>Data emitter</td>
    </tr>
    <!-- Batching -->
    <tr>
      <td rowspan="3">Batching & Flow Control</td>
      <td><code>Batch</code></td>
      <td>Groups incoming items into batches of a given size.</td>
      <td>Creates mini-batches like <code>[x1, x2, x3]</code></td>
    </tr>
    <tr>
      <td><code>Unbatch</code></td>
      <td>Splits batched input back into individual items.</td>
      <td>Yields items one by one from a list</td>
    </tr>
    <tr>
      <td><code>Buffer</code></td>
      <td>Buffers items in a queue between upstream and downstream stages.</td>
      <td>Decouples speed mismatch</td>
    </tr>
    <!-- Execution -->
    <tr>
      <td rowspan="2">Execution</td>
      <td><code>Sequential</code></td>
      <td>Processes items one at a time in arrival order.</td>
      <td>Single-threaded, ordered</td>
    </tr>
    <tr>
      <td><code>Parallel</code></td>
      <td>Runs multiple workers concurrently with the same logic.</td>
      <td>Thread/worker pool</td>
    </tr>
    <!-- Routing -->
    <tr>
      <td rowspan="4">Multi-Branch Routing</td>
      <td><code>Distribute</code></td>
      <td>Takes a dictionary input and sends each value to a named downstream branch.</td>
      <td><code>{"a": x, "b": y}</code> → branches "a", "b"</td>
    </tr>
    <tr>
      <td><code>Broadcast</code></td>
      <td>Sends a copy of input to all downstream branches.</td>
      <td>Broadcast to all, collect results as list</td>
    </tr>
    <tr>
      <td><code>Switch</code></td>
      <td>Uses a key function to send data to a single selected branch.</td>
      <td>Single-target routing</td>
    </tr>
    <tr>
      <td><code>Router</code></td>
      <td>Uses a key function to send data to multiple selected branches.</td>
      <td>Multi-target routing, collect results as dict</td>
    </tr>
  </tbody>
</table>


# Usage

```python
import time
import pipeline


def a(data):
    time.sleep(0.02)
    return data * 4 - 3

def b(data):
    time.sleep(0.1)
    return {'x': data * 2, 'y': 1}

def c_x(data):
    time.sleep(0.05)
    return data ** 2

def c_y(data):
    time.sleep(0.05)
    return data - 1

def d(data):
    time.sleep(0.05)
    return data['x'] + data['y']


# Build the pipeline
pipe = pipeline.Sequential([                    # A sequential pipeline
    a,                                          # Function wrapped as a node automatically
    pipeline.Parallel([b, b, b]),               # Three parallel branches
    pipeline.Distribute({'x': c_x, 'y': c_y}),  # Split the dict and distribute to two branches
    pipeline.Buffer(3),                         # The buffer can smooth out variations in processing time within the pipeline.
    d,
])

# Start the pipeline and run it
with pipe:  
    
    # Usage 1 - For regular iterable data source
    for x in pipe(range(100)):
        print(x)

    # Usage 2 - For irregularlly sourced data
    pipe.put(10)
    print(pipe.get())

    # NOTE for usage 2: 
    # If you put too many data and do not get them, 
    # the pipeline will be full, and the put operation will block until the pipeline has space to accept the data.
    # Consider putting data in a separate thread.
    # Or use a Buffer(0) node at the beginning of the sequential to hold infinite number of inputs.
```