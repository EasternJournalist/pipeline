import pipeline
import time
import random

# Example functions
def slow_add(x):
    time.sleep(0.2) # <= some slow computation
    return x + 1

def slow_mul(x):
    print(x)
    time.sleep(random.random()) # <= some slow computation
    if x == 13:
        raise ValueError("Number 13!")  # <= simulate an error
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
    iterator = pipe(data)        # Get an iterator over processed results
    while True:
        try:
            result = next(iterator)  # Get next processed result
            print(f"Result: {result}")
        except pipeline.ExceptionInNode as e:
            print(f"Caught an exception from node")
        except StopIteration:
            break

print(pipe.profile())  # Print profiling information