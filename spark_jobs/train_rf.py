from pyspark.ml import Pipeline
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.ml.feature import StringIndexer, VectorAssembler
from pyspark.sql import SparkSession, functions as F

spark = SparkSession.builder.appName("train-rf").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

train = spark.read.parquet("hdfs://namenode:8020/data/cicids/ml/train")
test = spark.read.parquet("hdfs://namenode:8020/data/cicids/ml/test")

non_feature = {"label", "source_file", "ts"}
feature_cols = [c for c in train.columns if c not in non_feature]

label_idx = StringIndexer(inputCol="label", outputCol="label_idx", handleInvalid="skip")
assembler = VectorAssembler(inputCols=feature_cols, outputCol="features", handleInvalid="skip")


def per_class_metrics(preds, labels):
    """Return {label: (precision, recall, f1, support)} from a confusion-matrix count."""
    counts = {
        (r["label_idx"], r["prediction"]): r["n"]
        for r in preds.groupBy("label_idx", "prediction").agg(F.count("*").alias("n")).collect()
    }
    out = {}
    for i, name in enumerate(labels):
        tp = counts.get((i, i), 0)
        fn = sum(v for (a, p), v in counts.items() if a == i and p != i)
        fp = sum(v for (a, p), v in counts.items() if p == i and a != i)
        support = tp + fn
        prec = tp / (tp + fp) if tp + fp else 0.0
        rec = tp / (tp + fn) if tp + fn else 0.0
        f1 = 2 * prec * rec / (prec + rec) if prec + rec else 0.0
        out[name] = (prec, rec, f1, support)
    return out


def report(name, preds, labels):
    acc = MulticlassClassificationEvaluator(
        labelCol="label_idx", predictionCol="prediction", metricName="accuracy"
    ).evaluate(preds)
    weighted_f1 = MulticlassClassificationEvaluator(
        labelCol="label_idx", predictionCol="prediction", metricName="f1"
    ).evaluate(preds)
    metrics = per_class_metrics(preds, labels)
    macro_f1 = sum(v[2] for v in metrics.values()) / len(metrics)

    print(f"\n=== {name} ===")
    print(f"accuracy: {acc:.4f}  weighted F1: {weighted_f1:.4f}  macro F1: {macro_f1:.4f}")
    print(f"{'label':28}{'precision':>10}{'recall':>10}{'f1':>10}{'support':>10}")
    for label, (p, r, f1, support) in metrics.items():
        print(f"{label:28}{p:10.3f}{r:10.3f}{f1:10.3f}{support:10}")
    return metrics


# --- Baseline: no class weighting ---
rf_base = RandomForestClassifier(
    labelCol="label_idx", featuresCol="features", numTrees=100, maxDepth=10, seed=42
)
pipeline_base = Pipeline(stages=[label_idx, assembler, rf_base])
model_base = pipeline_base.fit(train)
preds_base = model_base.transform(test)
labels = model_base.stages[0].labels
report("Baseline (unweighted)", preds_base, labels)

# --- Weighted: inverse class frequency ---
counts = train.groupBy("label").count().collect()
total = train.count()
n_classes = len(counts)
weight_map = {r["label"]: total / (n_classes * r["count"]) for r in counts}
weight_expr = F.create_map([F.lit(x) for pair in weight_map.items() for x in pair])
train_w = train.withColumn("weight", weight_expr[F.col("label")])

rf_weighted = RandomForestClassifier(
    labelCol="label_idx", featuresCol="features", weightCol="weight",
    numTrees=100, maxDepth=10, seed=42
)
pipeline_weighted = Pipeline(stages=[label_idx, assembler, rf_weighted])
model_weighted = pipeline_weighted.fit(train_w)
preds_weighted = model_weighted.transform(test)
report("Class-weighted", preds_weighted, labels)

model_weighted.write().overwrite().save("hdfs://namenode:8020/data/cicids/ml/rf_model_weighted")
print("\nsaved weighted model")
spark.stop()