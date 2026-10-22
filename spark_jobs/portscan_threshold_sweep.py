from pyspark.sql import SparkSession, functions as F

spark = (
    SparkSession.builder.appName("portscan-threshold-sweep")
    .config("spark.sql.shuffle.partitions", "8")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

flows = spark.read.parquet("hdfs://namenode:8020/data/cicids/labelled_parquet").select(
    "ts", "src_ip", "dst_port", "total_backward_packets", "label"
)
scanners = {
    r["src_ip"]
    for r in flows.filter(F.col("label") == "PortScan").select("src_ip").distinct().collect()
}


def per_ip_max_ports(df):
    """Highest distinct-dst-port count any 1-minute window reached, per src_ip."""
    return (
        df.groupBy(F.window("ts", "1 minute"), "src_ip")
        .agg(F.countDistinct("dst_port").alias("p"))
        .groupBy("src_ip")
        .agg(F.max("p").alias("max_ports"))
        .collect()
    )


variants = {
    "all flows": flows,
    "no-reply flows only (backward packets = 0)": flows.filter(F.col("total_backward_packets") == 0),
}

for name, df in variants.items():
    rows = per_ip_max_ports(df)
    att = [r["max_ports"] for r in rows if r["src_ip"] in scanners]
    ben = sorted(r["max_ports"] for r in rows if r["src_ip"] not in scanners)
    pct = lambda q: ben[int(q * (len(ben) - 1))] if ben else 0
    print(f"== {name}")
    print(f"  scanner IPs: {sorted(scanners)}  max distinct ports/min: {att}")
    print(f"  benign IPs seen: {len(ben)}  p50={pct(.5)} p90={pct(.9)} p99={pct(.99)} max={ben[-1] if ben else 0}")
    for t in (15, 50, 100, 200, 500, 1000, 2000):
        caught = sum(1 for a in att if a >= t)
        fp = sum(1 for b in ben if b >= t)
        print(f"  threshold={t:5}: scanners caught {caught}/{len(scanners)}, benign IPs flagged {fp}")

spark.stop()