from pyspark.sql import SparkSession
from pyspark.sql.functions import col


spark = SparkSession.builder.appName("Log Processing DF").getOrCreate()

log_path = "hdfs:///data/sparklogs/huge.log"
df = spark.read.option("delimiter", "\t").csv(log_path)

# df.show()

record_count = df.count()
print(f"Record count: {record_count}")

filtered_users_count = df.select("_c1").filter((col("_c1") == "bob") | (col("_c1") == "alice")).count()
print(f"Filtered users count: {filtered_users_count}")

time_sum = df.select(col("_c1").alias("user"), \
                     col("_c5").cast("int").alias("time")) \
                        .filter(col("user") == "bob").groupBy("user").sum("time")
# print(f"Total time for bob: {time_sum}")
time_sum.show()