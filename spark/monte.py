import random
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Monte").getOrCreate()
sc = spark.sparkContext

NUM_SAMPLES=10000

def inside(p):
    x, y = random.random(), random.random()
    return x*x + y*y < 1

count = sc.parallelize(range(0,NUM_SAMPLES)).filter(inside).count()
print("Pi is roughly %f" % (4.0 * count / NUM_SAMPLES))