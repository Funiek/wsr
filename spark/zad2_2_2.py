from pyspark.sql import SparkSession
import time

spark = SparkSession.builder.master("spark://grid-1:7077").appName("zad_2_2_2").getOrCreate()
sc = spark.sparkContext

# podpunkt 1 i 2
log_path = "hdfs:///data/sparklogs/new/logs100M.txt"

rdd = sc.textFile(log_path).cache()

start_time = time.time()
res = rdd.map(lambda line: (line.split("\t")[1], int(line.split("\t")[5]))).reduceByKey(lambda a, b: a + b)
end_time = time.time()

for key, sum_value in res.collect():
    print(f"{key}: {sum_value}")

start_time_cached = time.time()
rdd.map(lambda line: (line.split("\t")[1], int(line.split("\t")[5]))).reduceByKey(lambda a, b: a + b)
end_time_cached = time.time()

print(f"Execution time before cached: {end_time - start_time} Execution time after cached: {end_time_cached - start_time_cached}")

# podpunkt 3 filtrowanie boba
bob_rdd = rdd.filter(lambda line: "bob" == line.split("\t")[1]).cache()
bob_rdd.map(lambda line: int(line.split("\t")[5])) \
                      .reduce(lambda a, b: a + b)

start_time_bob_time_sum = time.time()
bob_time_sum = bob_rdd.map(lambda line: int(line.split("\t")[5])) \
                      .reduce(lambda a, b: a + b)
print(f"Total time for bob: {bob_time_sum} Execution time: {time.time() - start_time_bob_time_sum}")

for i in (4, 8, 16, 32, 64, 128):
    rdd_partitioned = rdd.repartition(i)
    start_time_rdd_partitioned = time.time()
    res_partitioned = rdd_partitioned.map(lambda line: (line.split("\t")[1], int(line.split("\t")[5]))).reduceByKey(lambda a, b: a + b)
    end_time_rdd_partitioned = time.time()
    print(f"Total execution time for {i} partitions: {end_time_rdd_partitioned - start_time_rdd_partitioned}")