# Pipeline

Multithread python pipeline framework.

* Preserving first-in-first-out order of data.
* Maximize thouroughput. Each stage runs in parallel. 
* Easy and intuitive. Minimal code to build a pipeline. 

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