import re

from pyspark.sql import SparkSession, functions as F

spark = (
    SparkSession.builder.appName("load-cicids-labelled")
    .config("spark.hadoop.dfs.replication", "2")
    .config("spark.sql.legacy.timeParserPolicy", "CORRECTED")
    .getOrCreate()
)

OUT = "hdfs://namenode:8020/data/cicids/labelled_parquet"

raw = (
    spark.read.option("header", True)
    .option("encoding", "ISO-8859-1")  # some labels contain a cp1252 dash
    .csv("hdfs://namenode:8020/data/cicids/labelled/*.csv")
)


def clean_names(columns):
    """snake_case names, fix header typos, suffix duplicates with _dup."""
    out, seen = [], set()
    for c in columns:
        n = re.sub(r"[^0-9a-zA-Z]+", "_", c.strip()).strip("_").lower()
        n = re.sub(r"^(fwd|bwd)(packet|header)", r"\1_\2", n)
        while n in seen:
            n += "_dup"
        seen.add(n)
        out.append(n)
    return out


df = raw.toDF(*clean_names(raw.columns))
df = (
    df.withColumnRenamed("source_ip", "src_ip")
    .withColumnRenamed("destination_ip", "dst_ip")
    .withColumnRenamed("source_port", "src_port")
    .withColumnRenamed("destination_port", "dst_port")
    .withColumn("source_file", F.element_at(F.split(F.input_file_name(), "/"), -1))
)

# Label cleanup: strip odd characters, tidy spaces
df = df.withColumn(
    "label",
    F.trim(F.regexp_replace(F.regexp_replace("label", r"[^A-Za-z0-9 ]+", " "), r"\s+", " ")),
)

# Thursday-Morning-WebAttacks contains 288,602 fully blank rows; drop them
df = df.filter(F.col("flow_id").isNotNull() & F.col("label").isNotNull())

# Timestamp: day/month/year, 12-hour clock without AM/PM, some rows with seconds.
# Capture ran about 08:59-17:05, so hours 1-7 are PM.
ts_sec = F.to_timestamp("timestamp", "d/M/yyyy H:mm:ss")
ts_min = F.to_timestamp("timestamp", "d/M/yyyy H:mm")
ts_parsed = F.coalesce(ts_sec, ts_min)
df = (
    df.withColumn(
        "ts",
        F.when(F.hour(ts_parsed) < 8, ts_parsed + F.expr("INTERVAL 12 HOURS")).otherwise(ts_parsed),
    )
    .withColumn("ts_has_seconds", ts_sec.isNotNull())
    .drop("timestamp")
)

# Numeric columns: cast to double, Infinity / NaN -> null
text_cols = {"flow_id", "src_ip", "dst_ip", "label", "source_file", "ts", "ts_has_seconds"}
int_cols = {"src_port", "dst_port", "protocol"}


def numeric(c):
    x = F.col(c).cast("double")
    bad = x.isNull() | F.isnan(x) | (x == float("inf")) | (x == float("-inf"))
    clean = F.when(bad, None).otherwise(x)
    return (clean.cast("int") if c in int_cols else clean).alias(c)


df = df.select([F.col(c) if c in text_cols else numeric(c) for c in df.columns])

# Write first, then read the parquet back for checks (no cache needed)
df.write.mode("overwrite").parquet(OUT)
print("wrote parquet to /data/cicids/labelled_parquet")

out = spark.read.parquet(OUT)
print(f"rows: {out.count()}, columns: {len(out.columns)}")
print("null timestamps:", out.filter(F.col("ts").isNull()).count())

print("time range per file:")
out.groupBy("source_file", "ts_has_seconds").agg(
    F.count("*").alias("n"), F.min("ts").alias("first"), F.max("ts").alias("last")
).orderBy("source_file", "ts_has_seconds").show(40, truncate=False)

print("label counts:")
out.groupBy("label").count().orderBy(F.desc("count")).show(truncate=False)

spark.stop()