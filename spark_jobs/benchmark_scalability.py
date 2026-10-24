import time
from pyspark.sql import SparkSession, functions as F

start = time.time()
spark = SparkSession.builder.appName("benchmark-scalability").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

df = spark.read.parquet("hdfs://namenode:8020/data/cicids/labelled_parquet")
n = df.count()

# Same kind of rule-style groupby/aggregate workload as your real detection jobs
result = (
    df.filter(F.col("label") != "BENIGN")
    .groupBy("src_ip")
    .agg(F.count("*").alias("flows"), F.countDistinct("dst_port").alias("ports"))
    .filter(F.col("flows") >= 10)
    .count()
)

elapsed = time.time() - start
executors = spark.sparkContext.getConf().get("spark.executor.instances", "dynamic")
total_cores = spark.sparkContext.defaultParallelism

print(f"rows processed: {n}")
print(f"flagged src_ips: {result}")
print(f"elapsed seconds: {elapsed:.2f}")
print(f"default parallelism (cores available): {total_cores}")
spark.stop()