from pyspark.sql import SparkSession, functions as F

from parse_logs import parse_auth

spark = (
    SparkSession.builder.appName("stream-brute-force")
    .config("spark.sql.shuffle.partitions", "4")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

raw = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", "kafka:29092")
    .option("subscribe", "auth-logs")
    .option("startingOffsets", "latest")
    .load()
    .selectExpr(
        "CAST(key AS STRING) AS event_id",
        "CAST(value AS STRING) AS value",
        "timestamp AS kafka_ts",
    )
)

logs = parse_auth(raw)

# Same rule as the batch job: 10+ failures from one IP within 60 seconds
alerts = (
    logs.filter("failed = 1")
    .withWatermark("ts", "2 minutes")
    .groupBy("src_ip", F.window("ts", "60 seconds"))
    .agg(F.count("*").alias("failures"))
    .filter(F.col("failures") >= 10)
)

query = (
    alerts.select("src_ip", F.col("window.start").alias("window_start"), "failures")
    .writeStream.outputMode("update")
    .format("console")
    .option("truncate", "false")
    .start()
)
query.awaitTermination()