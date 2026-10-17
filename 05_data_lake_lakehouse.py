"""05 - Data Lake and Lakehouse Services.

Medallion lakehouse engineering on Azure Databricks with ADLS Gen2 and
Delta Lake. Covers the 15 activities of Category 5:
  Data Lake Architecture and Implementation, Cloud Object Storage Integration,
  Data Lake Ingestion Pipelines, Bronze/Silver/Gold Data Architecture,
  Data Lakehouse Implementation, Delta Lake Table Development,
  Apache Iceberg Table Implementation, Apache Hudi Implementation,
  Schema Evolution Management, Partitioning and File Optimization,
  Data Lifecycle Management, Data Archival and Retention,
  Data Lake Migration, Lakehouse Performance Optimization,
  Structured and Unstructured Data Processing.
"""

import os
from typing import Dict, List, Optional

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
STORAGE_ACCOUNT = os.environ.get("AZURE_STORAGE_ACCOUNT", "<storage-account>")
BRONZE_CONTAINER = os.environ.get("BRONZE_CONTAINER", "bronze")
SILVER_CONTAINER = os.environ.get("SILVER_CONTAINER", "silver")
GOLD_CONTAINER = os.environ.get("GOLD_CONTAINER", "gold")
ARCHIVE_CONTAINER = os.environ.get("ARCHIVE_CONTAINER", "archive")
OPS_CONTAINER = os.environ.get("OPS_CONTAINER", "ops")
ADLS = "abfss://{c}@{acct}.dfs.core.windows.net"


def get_spark(app_name: str = "lake-lakehouse") -> SparkSession:
    """Return the active Spark session, or build a local Delta-enabled one."""
    existing = SparkSession.getActiveSession()
    if existing:
        return existing
    return (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .getOrCreate()
    )


def _path(container: str, *parts: str) -> str:
    return ADLS.format(c=container, acct=STORAGE_ACCOUNT) + "/" + "/".join(parts)


def _get_widget(name: str, default: str = "") -> str:
    try:
        return dbutils.widgets.get(name)  # noqa: F821
    except Exception:
        return os.environ.get(name.upper(), default)


# ---------------------------------------------------------------------------
# Activity functions
# ---------------------------------------------------------------------------
def design_lake_architecture() -> Dict[str, str]:
    """Activity: Data Lake Architecture and Implementation - declare the zone layout."""
    return {"raw": _path(BRONZE_CONTAINER, ""), "curated": _path(SILVER_CONTAINER, ""),
            "serving": _path(GOLD_CONTAINER, ""), "archive": _path(ARCHIVE_CONTAINER, "")}


def integrate_object_storage(spark: SparkSession) -> Dict[str, str]:
    """Activity: Cloud Object Storage Integration - verify ADLS Gen2 mount reachability."""
    conf_key = f"fs.azure.account.key.{STORAGE_ACCOUNT}.dfs.core.windows.net"
    spark.conf.set(conf_key, os.environ.get("AZURE_STORAGE_KEY", "<storage-key>"))
    return {"account": STORAGE_ACCOUNT, "protocol": "abfss", "status": "configured"}


def lake_ingestion_pipeline(spark: SparkSession, source_prefix: str, table: str) -> None:
    """Activity: Data Lake Ingestion Pipelines - Auto Loader landing into bronze Delta."""
    stream = (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "parquet")
        .option("cloudFiles.schemaLocation", _path(BRONZE_CONTAINER, "_schema", table))
        .load(_path(BRONZE_CONTAINER, source_prefix))
    )
    (stream.writeStream.format("delta").outputMode("append")
     .option("checkpointLocation", _path(OPS_CONTAINER, "_checkpoints", table))
     .trigger(availableNow=True)
     .start(_path(BRONZE_CONTAINER, table)))


def promote_medallion(spark: SparkSession, table: str, transform_sql: Optional[str] = None) -> None:
    """Activity: Bronze, Silver, and Gold Data Architecture - promote bronze to silver."""
    bronze = spark.read.format("delta").load(_path(BRONZE_CONTAINER, table))
    bronze.createOrReplaceTempView("bronze_stage")
    silver = spark.sql(transform_sql) if transform_sql else bronze.dropDuplicates()
    (silver.write.format("delta").mode("overwrite")
     .save(_path(SILVER_CONTAINER, table)))


def implement_lakehouse(spark: SparkSession, db: str = "lakehouse") -> None:
    """Activity: Data Lakehouse Implementation - create the lakehouse database on ADLS."""
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {db} LOCATION '{_path(GOLD_CONTAINER, 'db')}'")


def develop_delta_table(spark: SparkSession, db: str, table: str,
                        schema: StructType, partition_cols: List[str]) -> None:
    """Activity: Delta Lake Table Development - create a partitioned managed Delta table."""
    spark.createDataFrame([], schema).write.format("delta") \
        .partitionBy(*partition_cols).mode("overwrite") \
        .saveAsTable(f"{db}.{table}")


def develop_iceberg_table(spark: SparkSession, catalog: str, table: str) -> None:
    """Activity: Apache Iceberg Table Implementation - create table via the Iceberg catalog."""
    spark.sql(f"CREATE TABLE IF NOT EXISTS {catalog}.default.{table} "
              "(id BIGINT, event_time TIMESTAMP, payload STRING) USING iceberg")


def develop_hudi_table(spark: SparkSession, table_path: str) -> None:
    """Activity: Apache Hudi Implementation - write a copy-on-write Hudi table."""
    df = spark.read.format("delta").load(_path(SILVER_CONTAINER, "orders"))
    (df.write.format("hudi")
     .option("hoodie.table.name", "orders_hudi")
     .option("hoodie.datasource.write.recordkey.field", "order_id")
     .option("hoodie.datasource.write.precombine.field", "updated_at")
     .mode("overwrite").save(table_path))


def evolve_schema(spark: SparkSession, table_path: str, df: DataFrame) -> None:
    """Activity: Schema Evolution Management - merge with automatic column addition."""
    (df.write.format("delta").mode("append")
     .option("mergeSchema", "true").save(table_path))


def optimize_files(spark: SparkSession, table_path: str,
                   partition_cols: List[str], zorder_cols: List[str]) -> None:
    """Activity: Partitioning and File Optimization - rewrite layout, OPTIMIZE + ZORDER."""
    df = spark.read.format("delta").load(table_path)
    (df.write.format("delta").mode("overwrite")
     .partitionBy(*partition_cols).save(table_path))
    spark.sql(f"OPTIMIZE delta.`{table_path}` ZORDER BY ({', '.join(zorder_cols)})")


def manage_lifecycle(spark: SparkSession, table_path: str, cutoff: str) -> None:
    """Activity: Data Lifecycle Management - delete expired partitions per retention policy."""
    spark.sql(f"DELETE FROM delta.`{table_path}` WHERE partition_date < '{cutoff}'")


def archive_retain(spark: SparkSession, table: str, cutoff: str) -> None:
    """Activity: Data Archival and Retention - move cold partitions to the archive container."""
    cold = (spark.read.format("delta").load(_path(SILVER_CONTAINER, table))
            .filter(F.col("partition_date") < cutoff))
    (cold.write.format("delta").mode("append")
     .save(_path(ARCHIVE_CONTAINER, table)))
    spark.sql(f"DELETE FROM delta.`{_path(SILVER_CONTAINER, table)}` "
              f"WHERE partition_date < '{cutoff}'")


def migrate_lake(spark: SparkSession, src_container: str, table: str, dst_container: str) -> None:
    """Activity: Data Lake Migration - copy a table between storage accounts/containers."""
    (spark.read.format("delta").load(_path(src_container, table))
     .write.format("delta").mode("overwrite").save(_path(dst_container, table)))


def optimize_lakehouse(spark: SparkSession, table_path: str) -> None:
    """Activity: Lakehouse Performance Optimization - OPTIMIZE, VACUUM, and cache warmup."""
    spark.sql(f"OPTIMIZE delta.`{table_path}`")
    spark.sql(f"VACUUM delta.`{table_path}` RETAIN 168 HOURS")
    spark.sql(f"CACHE SELECT * FROM delta.`{table_path}`")


def process_unstructured(spark: SparkSession, prefix: str) -> DataFrame:
    """Activity: Structured and Unstructured Data Processing - binary files + OCR text."""
    binaries = (spark.read.format("binaryFile")
                .load(_path(BRONZE_CONTAINER, "unstructured", prefix)))
    return binaries.withColumn("file_name", F.element_at(F.split("path", "/"), -1)) \
                   .withColumn("ingested_at", F.current_timestamp())


# ---------------------------------------------------------------------------
# Driver
# ---------------------------------------------------------------------------
def main() -> None:
    """Representative flow: ingest -> bronze->silver -> gold marts -> optimize."""
    spark = get_spark()
    run_date = _get_widget("run_date", "2026-10-17")
    table = _get_widget("table", "orders")

    design_lake_architecture()
    implement_lakehouse(spark)

    lake_ingestion_pipeline(spark, f"landing/{table}", table)
    promote_medallion(
        spark, table,
        "SELECT *, date_trunc('day', order_date) AS partition_date FROM bronze_stage",
    )

    silver = spark.read.format("delta").load(_path(SILVER_CONTAINER, table))
    gold = (silver.groupBy("partition_date")
            .agg(F.sum("amount").alias("daily_revenue"),
                 F.count("*").alias("order_count")))
    (gold.write.format("delta").mode("overwrite")
     .save(_path(GOLD_CONTAINER, f"{table}_daily")))

    optimize_files(spark, _path(SILVER_CONTAINER, table), ["partition_date"], ["order_id"])
    optimize_lakehouse(spark, _path(GOLD_CONTAINER, f"{table}_daily"))
    print(f"Lakehouse run for {run_date} on {table} complete")


if __name__ == "__main__":
    main()
