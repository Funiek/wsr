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


# Podpunkt 1.1
start_time_record_count = time.time()
record_count = rdd.count()
print(f"Record count: {record_count} Execution time: {time.time() - start_time_record_count}")

# Podpunkt 1.2
start_time_filtered_users_count = time.time()
filtered_users_count = rdd.filter(lambda line: "bob" in line.split("\t")[1] or "alice" in line.split("\t")[1]).count()
print(f"Filtered users count: {filtered_users_count} Execution time: {time.time() - start_time_filtered_users_count}")

# Podpunkt 1.3
start_time_bob_time_sum = time.time()
bob_time_sum = rdd.filter(lambda line: "bob" == line.split("\t")[1]) \
                  .map(lambda line: int(line.split("\t")[5])) \
                  .reduce(lambda a, b: a + b)
print(f"Total time for bob: {bob_time_sum} Execution time: {time.time() - start_time_bob_time_sum}")

# Podpunkt 2
res = rdd.map(lambda line: ((line.split("\t")[0], line.split("\t")[1]), int(line.split("\t")[5]))).reduceByKey(lambda a, b: a + b)
print("WARTOSCI DLA KAZDEGO UZYTKOWNIKA CALOSCI")
res = rdd.map(lambda line: (line.split("\t")[1], int(line.split("\t")[5]))).reduceByKey(lambda a, b: a + b)
for key, sum_value in res.collect():
    print(f"{key}: {sum_value}")

# Podpunkt 3
res_2_total_times = rdd.map(lambda line: (line.split("\t")[0], int(line.split("\t")[5]))).reduceByKey(lambda a, b: a + b)
res_2 = rdd.map(lambda line: ((line.split("\t")[0], line.split("\t")[1]), int(line.split("\t")[5]))).reduceByKey(lambda a, b: a + b)
total_time = res.map(lambda x: x[1]).sum()

print("WARTOSCI PROCENTOWE CALOSCI")
# Obliczanie procentu czasu dla każdego użytkownika
res_percentage = res.map(lambda x: (x[0], (x[1] / total_time) * 100))
for key, sum_value in res_percentage.collect():
    print(f"{key}: {sum_value}")


print("WARTOSCI PROCENTOWE CZASY BOBA NA ROZNYCH HOSTACH")
res_3_total_times = rdd.map(lambda line: (line.split("\t")[0], int(line.split("\t")[5]))).reduceByKey(lambda a, b: a + b)
res_3_bob = rdd.filter(lambda line: "bob" == line.split("\t")[1]) \
                .map(lambda line: (line.split("\t")[0], int(line.split("\t")[5]))) \
                .reduceByKey(lambda a, b: a + b)

res_3_result = res_3_total_times.join(res_3_bob).map(lambda x: (x[0], (x[1][1] / x[1][0]) * 100))
for key, value in res_3_result.collect():
    print(f"{key}: {value}")

# print(res_3_user_time_per_host.take(10))