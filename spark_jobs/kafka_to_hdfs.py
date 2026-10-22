from pyspark.sql import SparkSession

spark = (
    SparkSession.builder.appName("kafka-to-hdfs")
    .config("spark.hadoop.dfs.replication", "2")
    .getOrCreate()
)

TOPICS = {
    "auth-logs": "hdfs://namenode:8020/logs/raw/auth",
    "ground-truth": "hdfs://namenode:8020/logs/truth",
    "firewall-logs": "hdfs://namenode:8020/logs/raw/firewall",
    "firewall-truth": "hdfs://namenode:8020/logs/firewall_truth",
}

for topic, path in TOPICS.items():
    df = (
        spark.read.format("kafka")
        .option("kafka.bootstrap.servers", "kafka:29092")
        .option("subscribe", topic)
        .option("startingOffsets", "earliest")
        .option("endingOffsets", "latest")
        .load()
        .selectExpr(
            "CAST(key AS STRING) AS event_id",
            "CAST(value AS STRING) AS value",
            "timestamp AS kafka_ts",
        )
    )
    df.write.mode("overwrite").parquet(path)
    print(f"{topic}: wrote {spark.read.parquet(path).count()} rows to {path}")

spark.stop()