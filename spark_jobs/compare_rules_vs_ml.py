from pyspark.ml import PipelineModel
from pyspark.sql import SparkSession, functions as F

from parse_logs import detect_port_scans

spark = SparkSession.builder.appName("compare-rules-vs-ml").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

test = spark.read.parquet("hdfs://namenode:8020/data/cicids/ml/test")
model = PipelineModel.load("hdfs://namenode:8020/data/cicids/ml/rf_model_weighted")
labels = model.stages[0].labels

preds = model.transform(test)
pred_label_col = F.udf(lambda idx: labels[int(idx)])(F.col("prediction"))
preds = preds.withColumn("pred_label", pred_label_col)


def score(name, flagged_ips, true_label):
    flagged_set = {r["src_ip"] for r in flagged_ips.select("src_ip").distinct().collect()}
    actual = test.filter(F.col("label") == true_label)
    actual_ips = {r["src_ip"] for r in actual.select("src_ip").distinct().collect()}
    tp = len(flagged_set & actual_ips)
    fp = len(flagged_set - actual_ips)
    fn = len(actual_ips - flagged_set)
    p = tp / (tp + fp) if tp + fp else 0.0
    r = tp / (tp + fn) if tp + fn else 0.0
    print(f"  {name:30} IPs: tp={tp} fp={fp} fn={fn}  precision={p:.3f} recall={r:.3f}")


print("=== PortScan: rule-based vs ML ===")
scan_rule = detect_port_scans(
    test.select("ts", "src_ip", "dst_port"), threshold=15, window="1 minute", slide="10 seconds"
)
score("Rule (15 ports/min)", scan_rule, "PortScan")
score("ML (flagged, any class)", preds.filter(F.col("pred_label") == "PortScan"), "PortScan")

print("\n=== SSH Patator (port 22): rule-based proxy vs ML ===")
# Proxy rule: each CICIDS flow ~ one connection attempt, so flag src_ip with
# >= 10 flows to port 22 within 60s (mirrors the Rule A brute-force threshold)
ssh_flows = test.filter(F.col("dst_port") == 22).select("ts", "src_ip")
ssh_rule = (
    ssh_flows.groupBy(F.window("ts", "60 seconds", "10 seconds"), "src_ip")
    .agg(F.count("*").alias("n"))
    .filter(F.col("n") >= 10)
    .select("src_ip")
    .distinct()
)
score("Rule (10 conns/60s to :22)", ssh_rule, "SSH Patator")
score("ML (flagged, any class)", preds.filter(F.col("pred_label") == "SSH Patator"), "SSH Patator")

print("\n=== FTP Patator (port 21): rule-based proxy vs ML ===")
ftp_flows = test.filter(F.col("dst_port") == 21).select("ts", "src_ip")
ftp_rule = (
    ftp_flows.groupBy(F.window("ts", "60 seconds", "10 seconds"), "src_ip")
    .agg(F.count("*").alias("n"))
    .filter(F.col("n") >= 10)
    .select("src_ip")
    .distinct()
)
score("Rule (10 conns/60s to :21)", ftp_rule, "FTP Patator")
score("ML (flagged, any class)", preds.filter(F.col("pred_label") == "FTP Patator"), "FTP Patator")

print("\n=== sanity check ===")
preds.groupBy("pred_label").count().orderBy(F.desc("count")).show(20, truncate=False)
ssh_pred_ips = preds.filter(F.col("pred_label") == "SSH Patator").select("src_ip").distinct()
print(f"distinct src_ip predicted SSH Patator: {ssh_pred_ips.count()}")
ssh_pred_ips.show(20, truncate=False)

spark.stop()