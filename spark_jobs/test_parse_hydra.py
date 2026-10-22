from pyspark.sql import SparkSession, functions as F

from parse_logs import parse_auth

spark = SparkSession.builder.appName("test-parse-hydra").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

raw = spark.read.text("hdfs://namenode:8020/data/hydra/raw/hydra_auth.log")
raw = (
    raw.withColumn("event_id", F.monotonically_increasing_id().cast("string"))
    .withColumn("kafka_ts", F.current_timestamp())  # syslog lines have no year; parse_auth needs this
)

print(f"total lines: {raw.count()}")

parsed = parse_auth(raw).filter(F.col("outcome") != "")
print(f"parsed rows: {parsed.count()}")
parsed.select("ts", "outcome", "user", "src_ip", "port").show(10, truncate=False)

print("distinct src_ip in parsed rows:")
parsed.groupBy("src_ip").count().show()

unparsed = raw.join(parsed.select("event_id"), "event_id", "left_anti")
print(f"unparsed lines: {unparsed.count()}")
unparsed.select("value").show(10, truncate=False)

spark.stop()