import random
import time
from pyspark import StorageLevel

NUM_SAMPLES=100_000_000

def inside(p):
    x, y = random.random(), random.random()
    return x*x + y*y < 1

start_time = time.time()
res = sc.parallelize(range(0,NUM_SAMPLES),128).persist(StorageLevel.MEMORY_ONLY).repartition(128)

count = res.filter(inside).count()
print(f"Pi is roughly {(4.0 * count / NUM_SAMPLES)} Execution time: {time.time() - start_time}")