import datetime

from pymongo import MongoClient
from pyspark.sql import SparkSession, functions as F

from parse_logs import parse_auth

spark = (
    SparkSession.builder.appName("stream-brute-force-mongo")
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

alerts = (
    logs.filter("failed = 1")
    .withWatermark("ts", "2 minutes")
    .groupBy("src_ip", F.window("ts", "60 seconds"))
    .agg(
        F.count("*").alias("failures"),
        F.max("kafka_ts").alias("last_kafka_ts"),  # real arrival time of the triggering event
    )
    .filter(F.col("failures") >= 10)
)


def write_to_mongo(batch_df, batch_id):
    rows = batch_df.select(
        "src_ip",
        F.col("window.start").alias("window_start"),
        F.col("window.end").alias("window_end"),
        "failures",
        "last_kafka_ts",
    ).collect()
    if not rows:
        return
    client = MongoClient("mongodb://mongodb:27017")
    coll = client["security_logs"]["alerts"]
    docs = [
        {
            "rule": "brute_force_ssh",
            "src_ip": r["src_ip"],
            "window_start": r["window_start"].isoformat(),
            "window_end": r["window_end"].isoformat(),
            "failures": r["failures"],
            "last_kafka_ts": r["last_kafka_ts"].isoformat(),
            "detected_at": datetime.datetime.utcnow().isoformat(),
        }
        for r in rows
    ]
    coll.insert_many(docs)
    client.close()
    print(f"batch {batch_id}: wrote {len(docs)} alerts to MongoDB")


query = (
    alerts.writeStream.outputMode("update")
    .foreachBatch(write_to_mongo)
    .start()
)
query.awaitTermination()