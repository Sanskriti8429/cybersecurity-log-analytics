from pyspark.sql import SparkSession, functions as F
from pyspark.sql.window import Window

spark = (
    SparkSession.builder.appName("prepare-ml-data")
    .config("spark.hadoop.dfs.replication", "2")
    .getOrCreate()
)

df = spark.read.parquet("hdfs://namenode:8020/data/cicids/labelled_parquet")

# Drop identifiers that would leak or aren't real features
df = df.drop("flow_id", "dst_ip", "ts_has_seconds")

# Group the three near-empty classes into "Other"
rare = {"Heartbleed", "Infiltration", "Web Attack Sql Injection"}
df = df.withColumn(
    "label", F.when(F.col("label").isin(rare), "Other").otherwise(F.col("label"))
)

# Fill nulls in numeric feature columns with 0
non_feature = {"label", "source_file", "ts"}
feature_cols = [c for c in df.columns if c not in non_feature]
df = df.fillna(0, subset=feature_cols)

# Stratified temporal split: within each (label, source_file) group, first 70% by time -> train
w = Window.partitionBy("label", "source_file").orderBy("ts")
df = df.withColumn("pct_rank", F.percent_rank().over(w))

train = df.filter(F.col("pct_rank") < 0.7).drop("pct_rank")
test = df.filter(F.col("pct_rank") >= 0.7).drop("pct_rank")

print(f"train rows: {train.count()}, test rows: {test.count()}")
print("train label counts:")
train.groupBy("label").count().orderBy(F.desc("count")).show(20, truncate=False)
print("test label counts:")
test.groupBy("label").count().orderBy(F.desc("count")).show(20, truncate=False)

train.write.mode("overwrite").parquet("hdfs://namenode:8020/data/cicids/ml/train")
test.write.mode("overwrite").parquet("hdfs://namenode:8020/data/cicids/ml/test")
print("wrote train/test parquet")
spark.stop()