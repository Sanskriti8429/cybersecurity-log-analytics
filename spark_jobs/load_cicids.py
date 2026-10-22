import re

from pyspark.sql import SparkSession, functions as F

spark = (
    SparkSession.builder.appName("load-cicids")
    .config("spark.hadoop.dfs.replication", "2")
    .getOrCreate()
)

raw = (
    spark.read.option("header", True)
    .option("encoding", "ISO-8859-1")  # Thursday labels contain a cp1252 dash
    .csv("hdfs://namenode:8020/data/cicids/raw/*.csv")
)


def clean_names(columns):
    """snake_case names; second copy of a duplicate gets a _dup suffix."""
    out, seen = [], set()
    for c in columns:
        n = re.sub(r"[^0-9a-zA-Z]+", "_", c.strip()).strip("_").lower()
        n = n.replace("bwdpacket", "bwd_packet")  # header typo in the source
        while n in seen:
            n += "_dup"
        seen.add(n)
        out.append(n)
    return out


df = raw.toDF(*clean_names(raw.columns))
df = df.withColumn("source_file", F.element_at(F.split(F.input_file_name(), "/"), -1))

# Label: strip non-alphanumeric characters (fixes the Thursday dash), tidy spaces
df = df.withColumn(
    "label",
    F.trim(F.regexp_replace(F.regexp_replace("label", r"[^A-Za-z0-9 ]+", " "), r"\s+", " ")),
)

# Features: cast to double, turn Infinity / NaN into null
features = [c for c in df.columns if c not in ("label", "source_file")]
bad_counts = {}
for c in features:
    x = F.col(c).cast("double")
    is_bad = x.isNull() | F.isnan(x) | (x == float("inf")) | (x == float("-inf"))
    df = df.withColumn(c, F.when(is_bad, None).otherwise(x))

df = df.cache()
total = df.count()
print(f"rows: {total}, columns: {len(df.columns)}")

nulls = df.select(
    [F.sum(F.col(c).isNull().cast("int")).alias(c) for c in features]
).first().asDict()
print("columns with nulls:", {k: v for k, v in nulls.items() if v})

print("label counts:")
df.groupBy("label").count().orderBy(F.desc("count")).show(truncate=False)

df.write.mode("overwrite").parquet("hdfs://namenode:8020/data/cicids/parquet")
print("wrote parquet to /data/cicids/parquet")
spark.stop()