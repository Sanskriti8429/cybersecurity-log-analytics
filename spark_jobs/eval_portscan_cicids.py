from pyspark.sql import SparkSession, functions as F

from parse_logs import detect_port_scans

spark = (
    SparkSession.builder.appName("eval-portscan-cicids")
    .config("spark.sql.shuffle.partitions", "8")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

flows = spark.read.parquet("hdfs://namenode:8020/data/cicids/labelled_parquet").select(
    "ts", "src_ip", "dst_ip", "dst_port", "label"
)

# Slim, deduplicated input for the detector (it only needs ts, src_ip, dst_port)
det_input = flows.select("ts", "src_ip", "dst_port").dropDuplicates()

actual = F.col("label") == "PortScan"
true_attackers = flows.filter(actual).select("src_ip").distinct()


def evaluate(window, slide):
    flagged = detect_port_scans(det_input, threshold=15, window=window, slide=slide)
    scored = (
        flows.join(F.broadcast(flagged.withColumn("flagged", F.lit(1))), "src_ip", "left")
        .fillna({"flagged": 0})
        .withColumn("actual", actual.cast("int"))
    )
    c = {
        (r["flagged"], r["actual"]): r["count"]
        for r in scored.groupBy("flagged", "actual").count().collect()
    }
    tp, fp, fn = c.get((1, 1), 0), c.get((1, 0), 0), c.get((0, 1), 0)
    p = tp / (tp + fp) if tp + fp else 0.0
    r = tp / (tp + fn) if tp + fn else 0.0

    flagged_ips = {row["src_ip"] for row in flagged.collect()}
    attackers = {row["src_ip"] for row in true_attackers.collect()}
    a_tp = len(flagged_ips & attackers)
    a_fp = len(flagged_ips - attackers)
    a_fn = len(attackers - flagged_ips)

    print(f"[window={window}, slide={slide}]")
    print(f"  flows:     tp={tp} fp={fp} fn={fn} precision={p:.3f} recall={r:.3f}")
    print(f"  attackers: tp={a_tp} fp={a_fp} fn={a_fn}")
    print(f"  flagged src_ips: {len(flagged_ips)}")
    print("  flagged flows by IP and label (top 15):")
    (
        scored.filter("flagged = 1")
        .groupBy("src_ip", "label")
        .count()
        .orderBy(F.desc("count"))
        .show(15, truncate=False)
    )


evaluate("1 minute", "10 seconds")
evaluate("5 minutes", "30 seconds")
spark.stop()