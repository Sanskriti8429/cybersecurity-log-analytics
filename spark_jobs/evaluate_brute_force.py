from pyspark.sql import SparkSession, functions as F

from parse_logs import parse_auth

spark = (
    SparkSession.builder.appName("evaluate-brute-force")
    .config("spark.hadoop.dfs.replication", "2")
    .getOrCreate()
)

logs = parse_auth(spark.read.parquet("hdfs://namenode:8020/logs/raw/auth"))
truth = spark.read.parquet("hdfs://namenode:8020/logs/truth").select(
    "event_id", F.col("value").alias("label")
)

FAIL_THRESHOLD = 10
alerts = (
    logs.filter("failed = 1")
    .groupBy("src_ip", F.window("ts", "60 seconds"))
    .agg(F.count("*").alias("failures"), F.collect_list("event_id").alias("event_ids"))
    .filter(F.col("failures") >= FAIL_THRESHOLD)
)
flagged = (
    alerts.select(F.explode("event_ids").alias("event_id"))
    .distinct()
    .withColumn("predicted", F.lit(1))
)

scored = (
    truth.join(flagged, "event_id", "left")
    .fillna({"predicted": 0})
    .withColumn("actual", (F.col("label") != "normal").cast("int"))
)

counts = {(r["predicted"], r["actual"]): r["count"]
          for r in scored.groupBy("predicted", "actual").count().collect()}
tp = counts.get((1, 1), 0)
fp = counts.get((1, 0), 0)
fn = counts.get((0, 1), 0)
tn = counts.get((0, 0), 0)

precision = tp / (tp + fp) if tp + fp else 0.0
recall = tp / (tp + fn) if tp + fn else 0.0
f1 = 2 * precision * recall / (precision + recall) if precision + recall else 0.0

print(f"TP={tp} FP={fp} FN={fn} TN={tn}")
print(f"Precision={precision:.4f} Recall={recall:.4f} F1={f1:.4f}")
print("Per-label results (predicted=1 means the rule flagged it):")
scored.groupBy("label", "predicted").count().orderBy("label", "predicted").show()
spark.stop()