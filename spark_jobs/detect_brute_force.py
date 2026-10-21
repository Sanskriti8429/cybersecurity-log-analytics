from pyspark.sql import SparkSession, functions as F

from parse_logs import parse_auth

spark = (
    SparkSession.builder.appName("detect-brute-force")
    .config("spark.hadoop.dfs.replication", "2")
    .getOrCreate()
)

raw = spark.read.parquet("hdfs://namenode:8020/logs/raw/auth")
logs = parse_auth(raw)

FAIL_THRESHOLD = 10
alerts = (
    logs.filter("failed = 1")
    .withColumn("ts", F.to_timestamp("time_str", "MMM d HH:mm:ss"))
    .groupBy("src_ip", F.window("ts", "60 seconds"))
    .agg(F.count("*").alias("failures"), F.collect_set("event_id").alias("event_ids"))
    .filter(F.col("failures") >= FAIL_THRESHOLD)
)

alerts.select("src_ip", "window.start", "failures").show(truncate=False)
print(f"Alerts: {alerts.count()}")
spark.stop()