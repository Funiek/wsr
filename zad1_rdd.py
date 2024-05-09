from pyspark.sql import SparkSession
import time

spark = SparkSession.builder.appName("Log Processing RDD").getOrCreate()
sc = spark.sparkContext

log_path = "hdfs:///data/sparklogs/huge.log"

rdd = sc.textFile(log_path).cache()
rdd.count()
rdd.filter(lambda line: "bob" in line.split("\t")[1] or "alice" in line.split("\t")[1]).count()
rdd.filter(lambda line: "bob" == line.split("\t")[1]) \
                  .map(lambda line: int(line.split("\t")[5])) \
                  .reduce(lambda a, b: a + b)

start_time_record_count = time.time()
record_count = rdd.count()
print(f"Record count: {record_count} Execution time: {time.time() - start_time_record_count}")


start_time_filtered_users_count = time.time()
filtered_users_count = rdd.filter(lambda line: "bob" in line.split("\t")[1] or "alice" in line.split("\t")[1]).count()
print(f"Filtered users count: {filtered_users_count} Execution time: {time.time() - start_time_filtered_users_count}")

start_time_bob_time_sum = time.time()
bob_time_sum = rdd.filter(lambda line: "bob" == line.split("\t")[1]) \
                  .map(lambda line: int(line.split("\t")[5])) \
                  .reduce(lambda a, b: a + b)
print(f"Total time for bob: {bob_time_sum} Execution time: {time.time() - start_time_bob_time_sum}")