from pyspark.sql import SparkSession, functions as F

from parse_logs import parse_firewall, detect_port_scans  # replace 'parsers'

spark = SparkSession.builder.appName("portscan-eval").getOrCreate()

raw = spark.read.parquet("hdfs://namenode:8020/logs/raw/firewall")
truth = (
    spark.read.parquet("hdfs://namenode:8020/logs/firewall_truth")
    .select("event_id", F.col("value").alias("label"))
)

events = parse_firewall(raw)
print("unparsed lines:", events.filter(F.col("src_ip") == "").count())  # should be 0

flagged = detect_port_scans(events).withColumn("flagged", F.lit(1))
scored = (
    events.join(truth, "event_id")
    .join(flagged, "src_ip", "left")
    .withColumn("predicted", F.coalesce("flagged", F.lit(0)))
    .withColumn("actual", (F.col("label") != "normal").cast("int"))
)

tp = scored.filter("predicted = 1 AND actual = 1").count()
fp = scored.filter("predicted = 1 AND actual = 0").count()
fn = scored.filter("predicted = 0 AND actual = 1").count()
precision = tp / (tp + fp) if tp + fp else 0.0
recall = tp / (tp + fn) if tp + fn else 0.0
print(f"tp={tp} fp={fp} fn={fn} precision={precision:.3f} recall={recall:.3f}")

spark.stop()