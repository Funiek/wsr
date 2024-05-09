from pyspark.sql import SparkSession
from pyspark.sql.functions import col


spark = SparkSession.builder.appName("Log Processing DF").getOrCreate()

log_path = "hdfs:///data/sparklogs/huge.log"
df = spark.read.option("delimiter", "\t").csv(log_path)
df.createOrReplaceTempView("logs")

record_count = spark.sql("SELECT COUNT(*) FROM logs")
print(f"Record count:")
record_count.show()

filtered_users_count = spark.sql("SELECT COUNT(*) FROM logs WHERE _c1 = 'bob' OR _c1 = 'alice'")
print(f"Filtered users count:")
filtered_users_count.show()