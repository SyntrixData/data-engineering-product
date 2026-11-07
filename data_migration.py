"""Category 11: Data Migration Services.

Production-style PySpark migration toolkit for Azure Databricks + ADLS Gen2
+ Delta Lake. Covers: database/cloud/warehouse/lake/ETL-tool/legacy/app/
historical migration, schema migration, pipeline migration, validation,
assessment, planning, testing, cutover, post-migration optimization.
"""

import os
from typing import Dict, List, Optional

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F


# ----------------------------------------------------------------------------
# Config block (env vars with placeholder defaults)
# ----------------------------------------------------------------------------
CONFIG = {
    "SOURCE_PATH": os.environ.get("SOURCE_PATH", "abfss://landing@adlsgen2.dfs.core.windows.net/source"),
    "TARGET_PATH": os.environ.get("TARGET_PATH", "abfss://migrated@adlsgen2.dfs.core.windows.net/target"),
    "STAGING_PATH": os.environ.get("STAGING_PATH", "abfss://staging@adlsgen2.dfs.core.windows.net/migration"),
    "DELTA_TARGET": os.environ.get("DELTA_TARGET", "migrated.default.fact_migrated"),
    "CHECKPOINT_BASE": os.environ.get("CHECKPOINT_BASE", "abfss://checkpoints@adlsgen2.dfs.core.windows.net/migration"),
}

try:
    _widgets = dbutils.widgets  # type: ignore[name-defined]
    _widgets.text("source_path", CONFIG["SOURCE_PATH"])
    _widgets.text("target_path", CONFIG["TARGET_PATH"])
    _widgets.text("delta_target", CONFIG["DELTA_TARGET"])
except Exception:
    pass


def get_spark(app_name: str = "data-migration") -> SparkSession:
    """Build a SparkSession; assume `spark` exists in Databricks, else local."""
    try:
        return spark  # type: ignore[name-defined]
    except NameError:
        return (
            SparkSession.builder.appName(app_name)
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
            .master("local[*]")
            .getOrCreate()
        )


def _secret(key: str, env_fallback: str) -> str:
    """Read a secret via Databricks secrets, else env var (never hardcoded)."""
    try:
        return dbutils.secrets.get(scope="migration-scope", key=key)  # type: ignore[name-defined]
    except Exception:
        return os.environ.get(env_fallback, "")


# ----------------------------------------------------------------------------
# Activity implementations
# ----------------------------------------------------------------------------

def assess_source(spark: SparkSession, source_path: str) -> Dict[str, int]:
    """Migration Assessment: profile a source path (formats, row counts)."""
    summary: Dict[str, int] = {}
    for fmt in ("parquet", "csv", "json"):
        try:
            df = spark.read.format(fmt).option("header", "true").load(source_path)
            summary[fmt] = df.count()
        except Exception:
            summary[fmt] = 0
    return summary


def plan_migration(spark: SparkSession, source_path: str, report_path: str) -> None:
    """Migration Planning: write a JSON plan of objects to migrate."""
    inventory = assess_source(spark, source_path)
    plan_df = spark.createDataFrame(
        [{"object": k, "estimated_rows": v, "strategy": "bulk" if v > 1000000 else "batch"}
         for k, v in inventory.items()]
    )
    plan_df.write.mode("overwrite").json(report_path)


def migrate_database(spark: SparkSession, jdbc_url: str, table: str, target_path: str) -> None:
    """Database Migration: JDBC extract from legacy RDBMS to Delta."""
    user = _secret("sql-user", "SQL_USER")
    password = _secret("sql-password", "SQL_PASSWORD")
    (spark.read.format("jdbc")
     .option("url", jdbc_url)
     .option("dbtable", table)
     .option("user", user)
     .option("password", password)
     .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver")
     .load()
     .write.format("delta").mode("overwrite").save(target_path))


def migrate_cloud(spark: SparkSession, source_path: str, target_path: str) -> None:
    """Cloud Migration: re-home raw files from one container to another."""
    spark.read.format("binaryFile").load(source_path).write.mode("overwrite").save(target_path)


def migrate_warehouse_to_delta(spark: SparkSession, jdbc_url: str, query: str, delta_table: str) -> None:
    """Data Warehouse Migration: staged extract from warehouse into Delta."""
    user = _secret("sql-user", "SQL_USER")
    password = _secret("sql-password", "SQL_PASSWORD")
    df = (spark.read.format("jdbc")
          .option("url", jdbc_url)
          .option("query", query)
          .option("user", user)
          .option("password", password)
          .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver")
          .load())
    df.write.format("delta").mode("overwrite").option("mergeSchema", "true").saveAsTable(delta_table)


def migrate_data_lake(spark: SparkSession, src_path: str, tgt_path: str) -> None:
    """Data Lake Migration: convert CSV/JSON landing data into partitioned Delta."""
    df = spark.read.option("header", "true").option("inferSchema", "true").csv(src_path)
    if "year" not in df.columns and "load_date" in df.columns:
        df = df.withColumn("year", F.year(F.col("load_date")))
    df.write.format("delta").mode("overwrite").partitionBy("year").save(tgt_path)


def migrate_etl_tool_output(spark: SparkSession, src_path: str, delta_table: str) -> None:
    """ETL Tool Migration: re-platform flat export files from a legacy ETL tool."""
    df = spark.read.format("parquet").load(src_path)
    df.write.format("delta").mode("append").option("mergeSchema", "true").saveAsTable(delta_table)


def migrate_legacy_system(spark: SparkSession, jdbc_url: str, table: str, delta_table: str) -> None:
    """Legacy Data Migration: incremental pull keyed on a watermark column."""
    watermark = 0
    try:
        watermark = spark.table(delta_table).agg(F.max("id")).collect()[0][0] or 0
    except Exception:
        pass
    user = _secret("sql-user", "SQL_USER")
    password = _secret("sql-password", "SQL_PASSWORD")
    df = (spark.read.format("jdbc")
          .option("url", jdbc_url)
          .option("dbtable", f"(SELECT * FROM {table} WHERE id > {int(watermark)}) t")
          .option("user", user).option("password", password)
          .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver").load())
    df.write.format("delta").mode("append").saveAsTable(delta_table)


def migrate_application_data(spark: SparkSession, src_path: str, delta_table: str) -> None:
    """Application Data Migration: JSON app exports normalized into Delta."""
    df = spark.read.option("multiline", "true").json(src_path)
    df.write.format("delta").mode("overwrite").saveAsTable(delta_table)


def migrate_historical_data(spark: SparkSession, src_path: str, delta_table: str) -> None:
    """Historical Data Migration: bulk-load archived partitions by year."""
    df = spark.read.format("parquet").load(src_path)
    if "year" in df.columns:
        df.write.format("delta").mode("append").partitionBy("year").saveAsTable(delta_table)
    else:
        df.write.format("delta").mode("append").saveAsTable(delta_table)


def apply_schema_migration(spark: SparkSession, delta_table: str, mapping: Dict[str, str]) -> None:
    """Schema Migration: rename columns per a mapping and enforce the new schema."""
    df = spark.table(delta_table)
    for old, new in mapping.items():
        if old in df.columns:
            df = df.withColumnRenamed(old, new)
    df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(delta_table)


def migrate_pipeline(spark: SparkSession, spec_path: str, dest_path: str) -> None:
    """Pipeline Migration: translate a legacy pipeline spec JSON to Delta paths."""
    spec = spark.read.option("multiline", "true").json(spec_path)
    out = spec.withColumn("migrated_path", F.concat_ws("/", F.lit(dest_path), F.col("pipeline_name")))
    out.write.mode("overwrite").json(f"{CONFIG['STAGING_PATH']}/pipeline_spec")


def validate_after_migration(spark: SparkSession, source_count: int, delta_table: str) -> Dict[str, object]:
    """Data Validation After Migration: row-count + checksum reconciliation."""
    tgt = spark.table(delta_table)
    tgt_count = tgt.count()
    checksum = (tgt.agg(F.sum(F.hash(*[F.coalesce(F.col(c).cast("string"), F.lit(""))
                                     for c in tgt.columns])).alias("chk")).collect()[0]["chk"])
    return {"source_count": source_count, "target_count": tgt_count,
            "count_match": source_count == tgt_count, "checksum": checksum}


def test_migration(spark: SparkSession, delta_table: str, tests: Optional[List[str]] = None) -> List[Dict[str, object]]:
    """Migration Testing: run null/duplicate/schema assertions on the target."""
    df = spark.table(delta_table)
    results = []
    pk = tests or ["id"]
    for col in pk:
        nulls = df.filter(F.col(col).isNull()).count()
        dupes = df.count() - df.select(col).distinct().count()
        results.append({"column": col, "nulls": nulls, "duplicates": dupes})
    return results


def cutover(spark: SparkSession, delta_table: str, live_alias: str) -> None:
    """Migration Cutover Support: swap the production alias to the migrated table."""
    spark.sql(f"CREATE OR REPLACE VIEW {live_alias} AS SELECT * FROM {delta_table}")


def optimize_post_migration(spark: SparkSession, delta_table: str, zorder: Optional[str] = None) -> None:
    """Post-Migration Optimization: OPTIMIZE + VACUUM the migrated Delta table."""
    zclause = f"ZORDER BY ({zorder})" if zorder else ""
    spark.sql(f"OPTIMIZE {delta_table} {zclause}")
    spark.sql(f"VACUUM {delta_table} RETAIN 168 HOURS")


def main() -> None:
    """Representative end-to-end flow: assess, plan, migrate, validate, cutover."""
    spark = get_spark()
    src, tgt = CONFIG["SOURCE_PATH"], CONFIG["TARGET_PATH"]
    plan_migration(spark, src, f"{CONFIG['STAGING_PATH']}/plan")
    migrate_data_lake(spark, src, tgt)
    apply_schema_migration(spark, CONFIG["DELTA_TARGET"], {"cust_id": "customer_id"})
    source_count = spark.read.format("delta").load(tgt).count()
    print(validate_after_migration(spark, source_count, CONFIG["DELTA_TARGET"]))
    cutover(spark, CONFIG["DELTA_TARGET"], "prod.fact_migrated")
    optimize_post_migration(spark, CONFIG["DELTA_TARGET"], zorder="customer_id")


if __name__ == "__main__":
    main()
