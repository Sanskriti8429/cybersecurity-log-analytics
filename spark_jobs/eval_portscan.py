from pyspark.sql import SparkSession, functions as F

from parse_logs import parse_firewall, detect_port_scans  # replace 'parsers' with your module name

spark = SparkSession.builder.appName("portscan-eval").getOrCreate()

raw = spark.read.parquet("hdfs://namenode:8020/logs/raw/firewall")
truth = (
    spark.read.parquet("hdfs://namenode:8020/logs/firewall_truth")
    .select("event_id", F.col("value").alias("label"))
)

events = parse_firewall(raw)
print("unparsed lines:", events.filter(F.col("src_ip") == "").count())  # should be 0

labelled = events.join(truth, "event_id").withColumn(
    "actual", (F.col("label") != "normal").cast("int")
)


def prf(tp, fp, fn):
    precision = tp / (tp + fp) if tp + fp else 0.0
    recall = tp / (tp + fn) if tp + fn else 0.0
    return precision, recall


CONFIGS = [
    ("1 minute", "10 seconds"),
    ("5 minutes", "30 seconds"),
]

for window, slide in CONFIGS:
    flagged = (
        detect_port_scans(labelled, threshold=15, window=window, slide=slide)
        .withColumn("flagged", F.lit(1))
    )
    scored = labelled.join(flagged, "src_ip", "left").withColumn(
        "predicted", F.coalesce("flagged", F.lit(0))
    )

    # Event level: every log line counts once.
    tp = scored.filter("predicted = 1 AND actual = 1").count()
    fp = scored.filter("predicted = 1 AND actual = 0").count()
    fn = scored.filter("predicted = 0 AND actual = 1").count()
    ep, er = prf(tp, fp, fn)

    # Attacker level: every source IP counts once, so small attacks matter as much as big ones.
    per_ip = scored.groupBy("src_ip").agg(
        F.max("actual").alias("actual"), F.max("predicted").alias("predicted")
    )
    itp = per_ip.filter("predicted = 1 AND actual = 1").count()
    ifp = per_ip.filter("predicted = 1 AND actual = 0").count()
    ifn = per_ip.filter("predicted = 0 AND actual = 1").count()
    ip_p, ip_r = prf(itp, ifp, ifn)

    print(f"[window={window}, slide={slide}]")
    print(f"  events:    tp={tp} fp={fp} fn={fn} precision={ep:.3f} recall={er:.3f}")
    print(f"  attackers: tp={itp} fp={ifp} fn={ifn} precision={ip_p:.3f} recall={ip_r:.3f}")

spark.stop()