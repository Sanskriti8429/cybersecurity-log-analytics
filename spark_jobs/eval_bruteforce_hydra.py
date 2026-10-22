from pyspark.sql import SparkSession, functions as F

from parse_logs import parse_auth

spark = SparkSession.builder.appName("eval-bruteforce-hydra").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

raw = spark.read.text("hdfs://namenode:8020/data/hydra/raw/hydra_auth.log")
raw = (
    raw.withColumn("event_id", F.monotonically_increasing_id().cast("string"))
    .withColumn("kafka_ts", F.current_timestamp())
)
parsed = parse_auth(raw).filter(F.col("outcome") != "")
failed = parsed.filter("failed = 1")


def window_alerts(window, min_failures):
    return (
        failed.groupBy(F.window("ts", window), "src_ip")
        .agg(F.count("*").alias("n"))
        .filter(F.col("n") >= min_failures)
    )


print("Rule A: 10+ failures from one IP / 60s")
window_alerts("60 seconds", 10).show(truncate=False)

print("Rule B: 20+ failures from one IP / 10 min")
window_alerts("10 minutes", 20).show(truncate=False)

print(f"total failed attempts: {failed.count()}")
print(f"first failed ts: {failed.agg(F.min('ts')).collect()[0][0]}")
print(f"last failed ts:  {failed.agg(F.max('ts')).collect()[0][0]}")

spark.stop()