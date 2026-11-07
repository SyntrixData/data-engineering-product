"""Category 6 - Big Data Engineering.

PySpark scripts for large-scale distributed processing on Azure Databricks
with ADLS Gen2 and Delta Lake. Activities covered: (1) Apache Spark
Development, (2) PySpark Development, (3) Distributed Data Processing,
(4) Hadoop Ecosystem Integration, (5) Large-Scale Batch Processing,
(6) Stream Processing, (7) High-Volume Data Ingestion, (8) Complex Joins
and Aggregations, (9) Memory and Compute Optimization, (10) Distributed
Job Troubleshooting, (11) Spark Cluster Configuration, (12) Big Data
Workload Migration, (13) Large-Scale File Processing, (14) Processing
Architecture Design.
"""

import logging
import os
from datetime import datetime

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("big_data_engineering")

# Config block: env vars with placeholder defaults.
STORAGE_ACCOUNT = os.environ.get("ADLS_STORAGE_ACCOUNT", "<storage-account>")
CONTAINER = os.environ.get("ADLS_CONTAINER", "raw")
BRONZE = os.environ.get("BRONZE_CONTAINER", "bronze")
SILVER = os.environ.get("SILVER_CONTAINER", "silver")
GOLD = os.environ.get("GOLD_CONTAINER", "gold")
CATALOG = os.environ.get("DELTA_CATALOG", "main")
APP_NAME = os.environ.get("SPARK_APP_NAME", "big-data-engineering")
LEGACY_PATH = os.environ.get("LEGACY_HDFS_PATH", "wasbs://legacy@<account>.blob.core.windows.net/migrate")
SHUFFLE_PARTS = int(os.environ.get("SHUFFLE_PARTITIONS", "512"))


def adls(path: str, container: str = CONTAINER) -> str:
    return f"abfss://{container}@{STORAGE_ACCOUNT}.dfs.core.windows.net/{path}"


def get_spark(app_name: str = APP_NAME) -> SparkSession:
    """Active session on Databricks; local Delta-enabled fallback otherwise."""
    existing = SparkSession.getActiveSession()
    if existing is not None:
        return existing
    return (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.shuffle.partitions", str(SHUFFLE_PARTS))
        .master("local[*]").getOrCreate()
    )


def read_job_config() -> dict:
    """dbutils.widgets params; env/defaults when run outside Databricks."""
    try:
        from pyspark.dbutils import DBUtils
        dbu = DBUtils(SparkSession.getActiveSession())
        return {"source": dbu.widgets.get("source_path"),
                "target": dbu.widgets.get("target_path"),
                "run_date": dbu.widgets.get("run_date")}
    except Exception:
        return {"source": adls("ingest/events"),
                "target": adls("curated/events", SILVER),
                "run_date": datetime.utcnow().strftime("%Y-%m-%d")}


def build_batch_job(spark: SparkSession, source_path: str) -> DataFrame:
    """Activities 1-2: Apache Spark Development / PySpark Development."""
    df = spark.read.format("delta").load(source_path)
    return (df.withColumn("event_ts", F.to_timestamp("event_ts"))
              .withColumn("amount", F.col("amount").cast("decimal(18,2)"))
              .withColumn("country", F.upper(F.trim("country")))
              .filter(F.col("event_ts").isNotNull()))


def distribute_work(df: DataFrame, partition_cols: list) -> DataFrame:
    """Activity 3: Distributed Data Processing - repartition by natural keys."""
    return df.repartition(SHUFFLE_PARTS, *[F.col(c) for c in partition_cols])


def migrate_hdfs_path(spark: SparkSession, relative_path: str) -> DataFrame:
    """Activity 4: Hadoop Ecosystem Integration - read legacy HDFS layout."""
    return (spark.read.format("parquet").option("mergeSchema", "true")
            .load(f"{LEGACY_PATH}/{relative_path}"))


def run_large_batch(spark: SparkSession, cfg: dict) -> None:
    """Activity 5: Large-Scale Batch Processing - bronze to silver batch."""
    spark.conf.set("spark.sql.adaptive.enabled", "true")
    spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
    df = distribute_work(build_batch_job(spark, cfg["source"]), ["country", "event_date"])
    (df.write.format("delta").mode("overwrite").option("overwriteSchema", "true")
       .partitionBy("event_date", "country").save(cfg["target"]))
    logger.info("Large batch written to %s", cfg["target"])


def build_stream_pipeline(spark: SparkSession, source: str, sink: str):
    """Activity 6: Stream Processing - watermarked streaming aggregation."""
    stream = (spark.readStream.format("delta").option("maxFilesPerTrigger", 10)
              .load(source).withWatermark("event_ts", "15 minutes")
              .groupBy(F.window("event_ts", "5 minutes"), F.col("country"))
              .agg(F.sum("amount").alias("total_amount"), F.count("*").alias("events")))
    return (stream.writeStream.format("delta").outputMode("append")
            .option("checkpointLocation", adls("checkpoints/stream_agg", SILVER))
            .trigger(processingTime="5 minutes").start(sink))


def ingest_high_volume(spark: SparkSession, pattern: str, target: str) -> None:
    """Activity 7: High-Volume Data Ingestion - glob many files, compact output."""
    df = spark.read.option("mergeSchema", "true").parquet(pattern)
    df = df.coalesce(max(1, df.rdd.getNumPartitions() // 4))
    (df.write.format("delta").mode("append").option("dataChange", "false")
       .save(adls(target, BRONZE)))


def complex_join_agg(events: DataFrame, customers: DataFrame, products: DataFrame) -> DataFrame:
    """Activity 8: Complex Joins and Aggregations - salted skew join + rollup."""
    salted = (events.withColumn("salt", (F.rand() * 16).cast("int"))
              .withColumn("salted_key", F.concat_ws("#", F.col("customer_id"), F.col("salt"))))
    cust = (F.broadcast(customers)
            .withColumn("salt", F.explode(F.array(*[F.lit(i) for i in range(16)])))
            .withColumn("salted_key", F.concat_ws("#", F.col("customer_id"), F.col("salt"))))
    joined = salted.join(cust, "salted_key", "inner").join(F.broadcast(products), "product_id", "left")
    window = Window.partitionBy("country", "category").orderBy(F.desc("total_amount"))
    return (joined.groupBy("country", "category", "customer_id")
            .agg(F.sum("amount").alias("total_amount"))
            .withColumn("rank_in_category", F.row_number().over(window))
            .filter(F.col("rank_in_category") <= 100))


def optimize_spark(spark: SparkSession, table_path: str, zorder_cols: list) -> None:
    """Activity 9: Memory and Compute Optimization - AQE/Kryo tuning + ZORDER."""
    for key, value in {
        "spark.sql.adaptive.enabled": "true",
        "spark.sql.adaptive.skewJoin.enabled": "true",
        "spark.sql.adaptive.coalescePartitions.enabled": "true",
        "spark.serializer": "org.apache.spark.serializer.KryoSerializer",
        "spark.sql.shuffle.partitions": str(SHUFFLE_PARTS),
    }.items():
        spark.conf.set(key, value)
    spark.sql(f"OPTIMIZE delta.`{table_path}` ZORDER BY ({', '.join(zorder_cols)})")


def diagnose_job(spark: SparkSession, df: DataFrame) -> dict:
    """Activity 10: Distributed Job Troubleshooting - partitions, skew, plan."""
    top_skew = [(r["country"], r["count"]) for r in
                df.groupBy("country").count().orderBy(F.desc("count")).limit(5).collect()]
    report = {"partitions": df.rdd.getNumPartitions(), "top_skew_keys": top_skew}
    logger.warning("Job diagnostics: %s", report)
    return report


def recommended_cluster_config(workload: str = "batch") -> dict:
    """Activity 11: Spark Cluster Configuration - Databricks profile per workload."""
    profiles = {
        "batch": {"node_type": "Standard_D8ds_v5", "num_workers": 8,
                  "autoscale": {"min_workers": 4, "max_workers": 16},
                  "spark_version": "15.4.x-scala2.12", "photon": True},
        "streaming": {"node_type": "Standard_D4ds_v5", "num_workers": 4,
                      "autoscale": {"min_workers": 2, "max_workers": 8},
                      "spark_version": "15.4.x-scala2.12", "photon": True},
    }
    return profiles.get(workload, profiles["batch"])


def migrate_workload(spark: SparkSession, legacy_relative: str, target_table: str) -> None:
    """Activity 12: Big Data Workload Migration - HDFS to Delta with row audit."""
    legacy_df = migrate_hdfs_path(spark, legacy_relative)
    before = legacy_df.count()
    (legacy_df.write.format("delta").mode("overwrite").option("mergeSchema", "true")
       .saveAsTable(f"{CATALOG}.migrated.{target_table}"))
    after = spark.table(f"{CATALOG}.migrated.{target_table}").count()
    if before != after:
        raise ValueError(f"Row count mismatch migrating {target_table}: {before} vs {after}")
    logger.info("Migrated %s: %d rows", target_table, after)


def process_large_files(spark: SparkSession, pattern: str, target: str) -> None:
    """Activity 13: Large-Scale File Processing - explicit schema, corrupt handling."""
    schema = ("event_id STRING, event_ts STRING, customer_id STRING, product_id STRING, "
              "country STRING, amount STRING, event_date STRING")
    df = (spark.read.schema(schema).option("mode", "PERMISSIVE")
          .option("columnNameOfCorruptRecord", "_corrupt").csv(pattern))
    clean = (df.filter(F.col("_corrupt").isNull()).drop("_corrupt")
              .withColumn("event_ts", F.to_timestamp("event_ts"))
              .withColumn("amount", F.col("amount").cast("decimal(18,2)")))
    clean.repartition(256).write.format("delta").mode("append").save(target)


def describe_architecture() -> dict:
    """Activity 14: Processing Architecture Design - medallion reference."""
    return {"landing": adls("landing"), "bronze": adls("bronze", BRONZE),
            "silver": adls("silver", SILVER), "gold": adls("gold", GOLD),
            "compute": "Azure Databricks (autoscaling job clusters, Photon)",
            "format": "Delta Lake (ACID, time travel, CDF, Z-ORDER)",
            "streaming": "Structured Streaming + Event Hubs",
            "serving": "Delta Sharing / Synapse serverless / Power BI"}


def main() -> None:
    """End-to-end: tune -> batch -> ingest -> complex join -> optimize."""
    spark = get_spark()
    cfg = read_job_config()
    run_large_batch(spark, cfg)
    ingest_high_volume(spark, adls("landing/events/dt=2026-10-16/*.parquet"), "events_late")
    events = spark.read.format("delta").load(cfg["target"])
    customers = spark.read.format("delta").load(adls("dims/customers", SILVER))
    products = spark.read.format("delta").load(adls("dims/products", SILVER))
    top = complex_join_agg(events, customers, products)
    top.write.format("delta").mode("overwrite").save(adls("gold/top_customers", GOLD))
    optimize_spark(spark, cfg["target"], ["event_date", "country"])
    logger.info("Cluster profile: %s", recommended_cluster_config("batch"))
    logger.info("Big data engineering main() completed")


if __name__ == "__main__":
    main()
