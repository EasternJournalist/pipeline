import time
import pipeline

# Define the functions of each node
def A(data):
    time.sleep(0.03)
    return data * 4 - 3

def B(data):
    time.sleep(0.15)
    return {'x': data * 2, 'y': 1}

def CX(data):
    time.sleep(0.04)
    return data ** 2

def CY(data):
    time.sleep(0.12)
    return data - 1

def D(data):
    time.sleep(0.05)
    return data['x'] + data['y']


# Build the pipeline
pipe = pipeline.Sequential([ 
    A, 
    pipeline.Parallel([B, B, B]),
    pipeline.Distribute({
      'x': CX, 
      'y': pipeline.Parallel([CY, CY, CY]), 
    }),
    D,
])

# Start the pipeline and run it
with pipe:  
    
    last_time = time.time()

    for i, result in zip(range(100), pipe(range(100))):
        now = time.time()
        print(f"No. {i}, result: {result}, throughput: {now - last_time:.4f}s/it.")
        last_time = now