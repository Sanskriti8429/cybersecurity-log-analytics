from pyspark.sql import SparkSession, functions as F

from parse_logs import parse_auth

spark = (
    SparkSession.builder.appName("evaluate-rules-v2")
    .config("spark.hadoop.dfs.replication", "2")
    .getOrCreate()
)

logs = parse_auth(spark.read.parquet("hdfs://namenode:8020/logs/raw/auth"))
truth = spark.read.parquet("hdfs://namenode:8020/logs/truth").select(
    "event_id", F.col("value").alias("label")
)
failed = logs.filter("failed = 1")


def window_alerts(keys, window, min_failures, min_ips=1):
    """Event ids flagged when a group has enough failures (from enough distinct IPs) in a window."""
    return (
        failed.groupBy(*keys, F.window("ts", window))
        .agg(
            F.count("*").alias("n"),
            F.countDistinct("src_ip").alias("ips"),
            F.collect_list("event_id").alias("event_ids"),
        )
        .filter((F.col("n") >= min_failures) & (F.col("ips") >= min_ips))
        .select(F.explode("event_ids").alias("event_id"))
        .distinct()
    )


def score(name, flagged):
    scored = (
        truth.join(flagged.withColumn("predicted", F.lit(1)), "event_id", "left")
        .fillna({"predicted": 0})
        .withColumn("actual", (F.col("label") != "normal").cast("int"))
    )
    c = {(r["predicted"], r["actual"]): r["count"]
         for r in scored.groupBy("predicted", "actual").count().collect()}
    tp, fp, fn = c.get((1, 1), 0), c.get((1, 0), 0), c.get((0, 1), 0)
    p = tp / (tp + fp) if tp + fp else 0.0
    r = tp / (tp + fn) if tp + fn else 0.0
    f1 = 2 * p * r / (p + r) if p + r else 0.0
    print(f"{name:42} TP={tp:4} FP={fp:3} FN={fn:4}  P={p:.3f} R={r:.3f} F1={f1:.3f}")
    return scored


rule_a = window_alerts(["src_ip"], "60 seconds", 10)
rule_b = window_alerts(["src_ip"], "10 minutes", 20)
rule_c = window_alerts(["user"], "60 seconds", 20, min_ips=5)
combined = rule_a.union(rule_b).union(rule_c).distinct()

score("A: per-IP, 10 failures / 60 s", rule_a)
score("B: per-IP, 20 failures / 10 min", rule_b)
score("C: per-user, 20 failures from 5+ IPs / 60 s", rule_c)
final = score("A + B + C combined", combined)

print("Per-label results for the combined rules:")
final.groupBy("label", "predicted").count().orderBy("label", "predicted").show()
spark.stop()