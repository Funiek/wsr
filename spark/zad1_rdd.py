from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Log Processing RDD").getOrCreate()
sc = spark.sparkContext

log_path = "hdfs:///data/sparklogs/small.log"

rdd = sc.textFile(log_path)

record_count = rdd.take(2)
print(f"Record count: {record_count}")

# filtered_users_count = logs_rdd.filter(lambda line: "bob" in line.split("\t")[1] or "alice" in line.split("\t")[1]).count()
# print(f"Filtered users count: {filtered_users_count}")

# bob_time_sum = logs_rdd.filter(lambda line: "bob" == line.split("\t")[1]).map(lambda line: int(line.split("\t")[4])).reduce(lambda a, b: a + b)

# print(f"Total time for bob: {bob_time_sum}")
