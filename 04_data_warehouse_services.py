"""04 - Data Warehouse Services.

Dimensional warehouse engineering on Azure Databricks with ADLS Gen2 and
Delta Lake. Covers the 12 activities of Category 4:
  Data Warehouse Design, Data Warehouse Implementation,
  Cloud Data Warehouse Setup, Data Warehouse Migration,
  Data Mart Development, Fact and Dimension Modeling,
  Star Schema Development, Snowflake Schema Development,
  Historical Data Management, Warehouse Optimization,
  Warehouse Cost Optimization, Data Warehouse Maintenance.
"""

import os
from typing import Dict, List, Optional

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
STORAGE_ACCOUNT = os.environ.get("AZURE_STORAGE_ACCOUNT", "<storage-account>")
WAREHOUSE_CONTAINER = os.environ.get("WAREHOUSE_CONTAINER", "warehouse")
SILVER_CONTAINER = os.environ.get("SILVER_CONTAINER", "silver")
OPS_CONTAINER = os.environ.get("OPS_CONTAINER", "ops")
WAREHOUSE_DB = os.environ.get("WAREHOUSE_DB", "dw")
ADLS = "abfss://{c}@{acct}.dfs.core.windows.net"


def get_spark(app_name: str = "data-warehouse") -> SparkSession:
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
def design_warehouse(spark: SparkSession) -> None:
    """Activity: Data Warehouse Design - create the warehouse database and layer folders."""
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {WAREHOUSE_DB} "
              f"LOCATION '{_path(WAREHOUSE_CONTAINER, 'db')}'")


def implement_warehouse(spark: SparkSession, source: str, staging_table: str) -> None:
    """Activity: Data Warehouse Implementation - stage source data as a managed Delta table."""
    (spark.read.format("delta").load(source)
     .write.format("delta").mode("overwrite")
     .saveAsTable(f"{WAREHOUSE_DB}.{staging_table}"))


def setup_cloud_warehouse(spark: SparkSession) -> Dict[str, str]:
    """Activity: Cloud Data Warehouse Setup - register warehouse location and defaults."""
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {WAREHOUSE_DB} "
              f"LOCATION '{_path(WAREHOUSE_CONTAINER, 'db')}'")
    spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", "false")
    return {"database": WAREHOUSE_DB,
            "location": _path(WAREHOUSE_CONTAINER, "db")}


def migrate_warehouse(spark: SparkSession, legacy_table: str, new_table: str) -> None:
    """Activity: Data Warehouse Migration - deep-clone a legacy table into the warehouse."""
    spark.sql(f"CREATE OR REPLACE TABLE {WAREHOUSE_DB}.{new_table} "
              f"DEEP CLONE {legacy_table}")


def develop_data_mart(spark: SparkSession, mart: str, source_tables: List[str]) -> None:
    """Activity: Data Mart Development - build a subject-area mart from warehouse tables."""
    for table in source_tables:
        (spark.table(f"{WAREHOUSE_DB}.{table}")
         .write.format("delta").mode("overwrite")
         .saveAsTable(f"{WAREHOUSE_DB}.{mart}_{table}"))


def model_fact_dimension(spark: SparkSession, fact: str, dims: Dict[str, DataFrame]) -> DataFrame:
    """Activity: Fact and Dimension Modeling - assemble a fact joined to its dimensions."""
    df = spark.table(f"{WAREHOUSE_DB}.{fact}")
    for alias, dim in dims.items():
        df = df.join(dim, on=f"{alias}_key", how="left")
    return df


def build_star_schema(spark: SparkSession, fact_name: str, dim_specs: Dict[str, DataFrame]) -> None:
    """Activity: Star Schema Development - write denormalized dims and the fact table."""
    for dim_name, dim_df in dim_specs.items():
        (dim_df.write.format("delta").mode("overwrite")
         .saveAsTable(f"{WAREHOUSE_DB}.dim_{dim_name}"))
    (spark.table(f"{WAREHOUSE_DB}.stg_{fact_name}")
     .write.format("delta").mode("overwrite")
     .saveAsTable(f"{WAREHOUSE_DB}.fact_{fact_name}"))


def build_snowflake_schema(spark: SparkSession, dim_name: str,
                           normalized: Dict[str, DataFrame]) -> None:
    """Activity: Snowflake Schema Development - write normalized dimension levels."""
    for level, df in normalized.items():
        (df.write.format("delta").mode("overwrite")
         .saveAsTable(f"{WAREHOUSE_DB}.dim_{dim_name}_{level}"))


def scd_type2(spark: SparkSession, dim_table: str, source: DataFrame,
              key: str, attrs: List[str]) -> None:
    """Activity: Historical Data Management - SCD Type 2 MERGE with effective dates."""
    target = f"{WAREHOUSE_DB}.{dim_table}"
    source.createOrReplaceTempView("scd_src")
    changed = " OR ".join(f"t.{a} <> s.{a}" for a in attrs)
    spark.sql(
        f"""MERGE INTO {target} t USING scd_src s ON t.{key} = s.{key} AND t.is_current
            WHEN MATCHED AND ({changed}) THEN
              UPDATE SET t.is_current = false, t.end_date = current_date()
            WHEN NOT MATCHED THEN INSERT *"""
    )
    new_rows = source.withColumn("is_current", F.lit(True))
    new_rows.createOrReplaceTempView("scd_new")
    spark.sql(f"""INSERT INTO {target}
                  SELECT * FROM scd_new s
                  WHERE NOT EXISTS (SELECT 1 FROM {target} t
                                    WHERE t.{key} = s.{key} AND t.is_current)""")


def optimize_warehouse(spark: SparkSession, table: str, zorder_cols: List[str]) -> None:
    """Activity: Warehouse Optimization - OPTIMIZE with ZORDER on a warehouse table."""
    spark.sql(f"OPTIMIZE {WAREHOUSE_DB}.{table} ZORDER BY ({', '.join(zorder_cols)})")


def optimize_cost(spark: SparkSession, table: str, retain_hours: int = 168) -> None:
    """Activity: Warehouse Cost Optimization - VACUUM old files to cut storage cost."""
    spark.sql(f"VACUUM {WAREHOUSE_DB}.{table} RETAIN {retain_hours} HOURS")


def maintain_warehouse(spark: SparkSession, tables: List[str]) -> DataFrame:
    """Activity: Data Warehouse Maintenance - DESCRIBE HISTORY audit across tables."""
    rows = []
    for table in tables:
        hist = spark.sql(f"DESCRIBE HISTORY {WAREHOUSE_DB}.{table}").limit(5)
        rows.append(hist.withColumn("table_name", F.lit(table)))
    audit = rows[0]
    for extra in rows[1:]:
        audit = audit.unionByName(extra, allowMissingColumns=True)
    (audit.write.format("delta").mode("append")
     .save(_path(OPS_CONTAINER, "maintenance_audit")))
    return audit


# ---------------------------------------------------------------------------
# Driver
# ---------------------------------------------------------------------------
def main() -> None:
    """Representative flow: setup -> stage -> star schema -> SCD2 -> optimize."""
    spark = get_spark()
    run_date = _get_widget("run_date", "2026-10-17")

    setup_cloud_warehouse(spark)
    implement_warehouse(spark, _path(SILVER_CONTAINER, "orders"), "stg_orders")

    dim_customer = (spark.read.format("delta")
                    .load(_path(SILVER_CONTAINER, "customers"))
                    .select("customer_key", "customer_name", "country", "segment"))
    dim_date = spark.sql(
        "SELECT explode(sequence(to_date('2020-01-01'), to_date('2030-12-31'))) AS date_key"
    ).withColumn("year", F.year("date_key")).withColumn("month", F.month("date_key"))

    build_star_schema(spark, "orders", {"customer": dim_customer, "date": dim_date})
    optimize_warehouse(spark, "fact_orders", ["customer_key", "date_key"])
    optimize_cost(spark, "fact_orders")
    audit = maintain_warehouse(spark, ["fact_orders", "dim_customer", "dim_date"])
    print(f"Warehouse load for {run_date} complete; audit rows: {audit.count()}")


if __name__ == "__main__":
    main()
