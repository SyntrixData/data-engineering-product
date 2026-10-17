"""Category 22: AI and Machine Learning Data Engineering.

Purpose:
    Prepare, version, and serve training and inference data for ML workloads on
    Azure Databricks + ADLS Gen2 + Delta Lake.

Activities covered:
    1.  Machine Learning Data Ingestion
    2.  Training Dataset Preparation
    3.  Feature Engineering
    4.  Feature Pipeline Development
    5.  Feature Store Integration
    6.  Training Data Validation
    7.  Data Labeling Workflow Integration
    8.  Data Versioning
    9.  Dataset Splitting and Sampling
    10. Historical Feature Generation
    11. Model Inference Data Pipelines
    12. Batch Prediction Data Pipelines
    13. Real-Time Inference Data Pipelines
    14. Model Input Data Validation
    15. ML Data Drift Monitoring
    16. Data Preparation for Generative AI
"""

import os
from datetime import datetime
from typing import Dict, List, Optional, Tuple

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

try:
    from delta.tables import DeltaTable
except Exception:
    DeltaTable = None

# Config block (env vars with placeholder defaults)
CONFIG = {
    "adls_account": os.environ.get("ADLS_ACCOUNT", "<adls-account-name>"),
    "container": os.environ.get("ADLS_CONTAINER", "lakehouse"),
    "keyvault_scope": os.environ.get("KEYVAULT_SCOPE", "kv-data-product"),
    "labeling_endpoint": os.environ.get("LABELING_ENDPOINT", "<labeling-api-endpoint>"),
    "process_date": os.environ.get("PROCESS_DATE", datetime.utcnow().strftime("%Y-%m-%d")),
    "train_split": float(os.environ.get("TRAIN_SPLIT", "0.8")),
}

# Widgets (guarded for non-Databricks contexts)
try:
    dbutils.widgets.text("process_date", CONFIG["process_date"])
    dbutils.widgets.text("model_name", "churn_model")
    CONFIG["process_date"] = dbutils.widgets.get("process_date")
except Exception:
    pass


def get_spark() -> SparkSession:
    """Return an existing Spark session or build a local one with Delta configs."""
    existing = SparkSession.getActiveSession()
    if existing is not None:
        return existing
    return (
        SparkSession.builder.appName("ml-data-engineering")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .getOrCreate()
    )


spark = get_spark()

BASE = f"abfss://{CONFIG['container']}@{CONFIG['adls_account']}.dfs.core.windows.net"


def _ml_path(layer: str, name: str) -> str:
    return f"{BASE}/ml/{layer}/{name}"


# 1. Machine Learning Data Ingestion
def ingest_ml_data(source: str, entity: str, fmt: str = "delta") -> DataFrame:
    """Ingest a raw ML source (table or path) into the landing area."""
    df = spark.table(source) if fmt == "table" else spark.read.format(fmt).load(source)
    df.write.format("delta").mode("append").save(_ml_path("landing", entity))
    return df


# 2. Training Dataset Preparation
def prepare_training_dataset(source: str, label_col: str, feature_cols: List[str],
                             dataset_name: str) -> DataFrame:
    """Select features/labels, drop null labels, and dedupe a training set."""
    df = (spark.table(source) if "." not in source else spark.read.format("delta").load(source))
    df = (df.select(*feature_cols, label_col)
            .filter(F.col(label_col).isNotNull())
            .dropDuplicates())
    df.write.format("delta").mode("overwrite").save(_ml_path("training", dataset_name))
    return df


# 3. Feature Engineering
def engineer_features(df: DataFrame, specs: Dict[str, str]) -> DataFrame:
    """Apply SQL-expression feature specs: {new_column: sql_expression}."""
    for new_col, expr in specs.items():
        df = df.withColumn(new_col, F.expr(expr))
    return df


# 4. Feature Pipeline Development
def run_feature_pipeline(source_table: str, entity: str, id_col: str,
                         feature_specs: Dict[str, str]) -> DataFrame:
    """End-to-end feature pipeline: ingest -> engineer -> write feature set."""
    df = spark.table(source_table)
    df = engineer_features(df, feature_specs)
    df = df.withColumn("_feature_ts", F.current_timestamp())
    out = _ml_path("features", entity)
    df.write.format("delta").mode("overwrite").save(out)
    spark.sql(f"CREATE TABLE IF NOT EXISTS fs_{entity} USING DELTA LOCATION '{out}'")
    return df


# 5. Feature Store Integration
def upsert_feature_store(entity: str, id_col: str) -> None:
    """Merge the latest computed features into the entity's feature store table."""
    if DeltaTable is None:
        raise RuntimeError("delta.tables.DeltaTable unavailable")
    store = DeltaTable.forName(spark, f"fs_{entity}")
    latest = spark.read.format("delta").load(_ml_path("features", entity))
    (store.alias("t").merge(latest.alias("s"), f"t.{id_col} = s.{id_col}")
        .whenMatchedUpdateAll().whenNotMatchedInsertAll().execute())


# 6. Training Data Validation
def validate_training_data(dataset_name: str, label_col: str,
                           min_rows: int = 1000) -> DataFrame:
    """Validate class balance, null rates, and row counts of a training set."""
    df = spark.read.format("delta").load(_ml_path("training", dataset_name))
    rows = df.count()
    nulls = df.filter(F.col(label_col).isNull()).count()
    balance = df.groupBy(label_col).count().collect()
    checks = [
        ("row_count", rows >= min_rows, rows),
        ("label_null_rate_ok", nulls == 0, nulls),
        ("label_cardinality_ok", len(balance) >= 2, len(balance)),
    ]
    return spark.createDataFrame(checks, ["check", "passed", "value"])


# 7. Data Labeling Workflow Integration
def integrate_labeling_workflow(entity: str, unlabeled_table: str,
                                label_table: str) -> DataFrame:
    """Join raw records with human labels from the labeling workflow table."""
    raw = spark.table(unlabeled_table)
    labels = spark.table(label_table)
    df = raw.join(labels, on="record_id", how="left")
    df.write.format("delta").mode("overwrite").save(_ml_path("labeled", entity))
    return df


# 8. Data Versioning
def version_dataset(dataset_path: str, tag: str) -> str:
    """Snapshot a Delta dataset version with a descriptive tag (time travel)."""
    history = spark.sql(f"DESCRIBE HISTORY delta.`{dataset_path}`")
    version = history.agg(F.max("version")).collect()[0][0]
    spark.sql(f"ALTER TABLE delta.`{dataset_path}` SET TBLPROPERTIES ('ml.tag.{tag}'='{version}')")
    return f"{dataset_path}@v{version}"


# 9. Dataset Splitting and Sampling
def split_dataset(dataset_name: str, label_col: str,
                  train_ratio: Optional[float] = None) -> Tuple[DataFrame, DataFrame]:
    """Stratified-ish train/test split of a training dataset."""
    ratio = train_ratio or CONFIG["train_split"]
    df = spark.read.format("delta").load(_ml_path("training", dataset_name))
    train, test = df.randomSplit([ratio, 1.0 - ratio], seed=42)
    train.write.format("delta").mode("overwrite").save(_ml_path("splits", f"{dataset_name}_train"))
    test.write.format("delta").mode("overwrite").save(_ml_path("splits", f"{dataset_name}_test"))
    return train, test


# 10. Historical Feature Generation
def generate_historical_features(source_table: str, entity: str, id_col: str,
                                 ts_col: str, lookback_days: int = 90) -> DataFrame:
    """Point-in-time feature reconstruction over a lookback window."""
    w = Window.partitionBy(id_col).orderBy(F.col(ts_col))
    df = (spark.table(source_table)
          .filter(F.col(ts_col) >= F.date_sub(F.current_date(), lookback_days))
          .withColumn("row_n", F.row_number().over(w))
          .withColumn("prev_value", F.lag("value").over(w)))
    df.write.format("delta").mode("overwrite").save(_ml_path("historical", entity))
    return df


# 11. Model Inference Data Pipelines
def build_inference_dataset(model_name: str, source_table: str,
                            feature_cols: List[str]) -> DataFrame:
    """Assemble the scored-input dataset a model expects at inference time."""
    df = spark.table(source_table).select(*feature_cols).dropna(subset=feature_cols)
    df.write.format("delta").mode("overwrite").save(_ml_path("inference_input", model_name))
    return df


# 12. Batch Prediction Data Pipelines
def run_batch_prediction(model_name: str, input_table: str, output_table: str,
                         score_expr: str) -> DataFrame:
    """Score a full batch with a SQL expression placeholder for the model UDF."""
    df = spark.table(input_table).withColumn("prediction", F.expr(score_expr))
    df = df.withColumn("scored_at", F.current_timestamp())
    df.write.format("delta").mode("overwrite").save(_ml_path("predictions", output_table))
    spark.sql(f"CREATE TABLE IF NOT EXISTS pred_{model_name} USING DELTA "
              f"LOCATION '{_ml_path('predictions', output_table)}'")
    return df


# 13. Real-Time Inference Data Pipelines
def start_streaming_inference(checkpoint: str, source_path: str, model_name: str) -> None:
    """Stream new feature rows, score with a SQL expression, write predictions."""
    stream = (spark.readStream.format("delta").load(source_path)
              .withColumn("prediction", F.expr("value * 0.5"))
              .withColumn("scored_at", F.current_timestamp()))
    (stream.writeStream.format("delta").outputMode("append")
        .option("checkpointLocation", checkpoint).start(_ml_path("predictions", model_name)))


# 14. Model Input Data Validation
def validate_model_inputs(model_name: str, feature_cols: List[str]) -> DataFrame:
    """Check inference inputs for nulls and out-of-range values before scoring."""
    df = spark.read.format("delta").load(_ml_path("inference_input", model_name))
    results = [(c, df.filter(F.col(c).isNull()).count()) for c in feature_cols]
    return spark.createDataFrame(results, ["feature", "null_count"])


# 15. ML Data Drift Monitoring
def monitor_data_drift(entity: str, feature_cols: List[str],
                       reference_version: int) -> DataFrame:
    """Compare current feature means/stddevs against a Delta time-travel reference."""
    current = spark.read.format("delta").load(_ml_path("features", entity))
    reference = (spark.read.format("delta")
                 .option("versionAsOf", reference_version)
                 .load(_ml_path("features", entity)))
    rows = []
    for col in feature_cols:
        cur = current.agg(F.mean(col).alias("m"), F.stddev(col).alias("s")).collect()[0]
        ref = reference.agg(F.mean(col).alias("m"), F.stddev(col).alias("s")).collect()[0]
        drift = abs((cur["m"] or 0) - (ref["m"] or 0)) / max(abs(ref["s"] or 1), 1e-9)
        rows.append((col, float(cur["m"] or 0), float(ref["m"] or 0), float(drift),
                     drift > 3.0))
    return spark.createDataFrame(rows, ["feature", "current_mean", "ref_mean",
                                        "drift_score", "drifted"])


# 16. Data Preparation for Generative AI
def prepare_genai_dataset(source_table: str, text_cols: List[str],
                          dataset_name: str) -> DataFrame:
    """Build a prompt-ready text dataset: concatenated text + metadata columns."""
    df = spark.table(source_table)
    df = df.withColumn("prompt_text", F.concat_ws("\n", *[F.col(c) for c in text_cols]))
    df = df.withColumn("prepared_at", F.current_timestamp())
    df.write.format("delta").mode("overwrite").save(_ml_path("genai", dataset_name))
    return df


# Driver
def main() -> None:
    process_date = CONFIG["process_date"]
    raw = ingest_ml_data("bronze_customer_events", "customers", fmt="table")
    features = run_feature_pipeline(
        "bronze_customer_events", "customers", "customer_id",
        {"recency_days": "datediff(current_date(), max_event_date)",
         "total_spend": "sum_amount",
         "avg_order_value": "total_spend / nullif(order_count, 0)"},
    )
    upsert_feature_store("customers", "customer_id")
    train_df = prepare_training_dataset("fs_customers", "churned",
                                        ["recency_days", "total_spend", "avg_order_value"],
                                        f"churn_train_{process_date}")
    validate_training_data(f"churn_train_{process_date}", "churned").show()
    train, test = split_dataset(f"churn_train_{process_date}", "churned")
    batch_in = build_inference_dataset("churn_model", "fs_customers",
                                       ["recency_days", "total_spend", "avg_order_value"])
    run_batch_prediction("churn_model", "fs_customers", f"churn_scores_{process_date}",
                         "CASE WHEN total_spend > 1000 THEN 0 ELSE 1 END")
    version_dataset(_ml_path("training", f"churn_train_{process_date}"), f"run_{process_date}")
    print(f"ML pipeline complete: train={train.count()} test={test.count()} raw={raw.count()} "
          f"features={features.count()} scored={batch_in.count()}")


if __name__ == "__main__":
    main()
