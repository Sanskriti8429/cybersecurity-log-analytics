from pyspark.sql import SparkSession, functions as F

from parse_logs import detect_port_scans

spark = SparkSession.builder.appName("eval-portscan-nmap").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

PATTERN = r"^(\S+ \S+) IP (\S+)\.(\d+) > (\S+)\.(\d+): Flags \[S\]"

raw = spark.read.text("hdfs://namenode:8020/data/nmap/raw/nmap_scan.txt")
raw = raw.withColumn("event_id", F.monotonically_increasing_id().cast("string"))

parsed = raw.select(
    "event_id",
    F.to_timestamp(F.regexp_extract("value", PATTERN, 1), "yyyy-MM-dd HH:mm:ss.SSSSSS").alias("ts"),
    F.regexp_extract("value", PATTERN, 2).alias("src_ip"),
    F.regexp_extract("value", PATTERN, 5).cast("int").alias("dst_port"),
).filter(F.col("src_ip") != "")

print(f"total lines: {raw.count()}")
print(f"parsed rows: {parsed.count()}")
parsed.show(10, truncate=False)

print("Rule: 15+ distinct ports from one src_ip in 60s")
detect_port_scans(parsed, threshold=15, window="1 minute", slide="10 seconds").show(truncate=False)

spark.stop()