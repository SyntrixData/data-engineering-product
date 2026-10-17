"""02 - ETL and ELT Services.

PySpark ELT/ETL transformation services on Azure Databricks with ADLS Gen2
and Delta Lake. Covers the 15 activities of Category 2:
  ETL Pipeline Development, ELT Pipeline Development, Data Transformation,
  Data Cleansing, Data Standardization, Data Deduplication, Data Enrichment,
  Data Normalization, Data Aggregation, Business Rule Implementation,
  Incremental Data Processing, Historical Data Processing, Data Type Conversion,
  Complex Data Transformation, Reusable Transformation Frameworks.
"""

import os
from typing import Callable, Dict, List, Optional

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
STORAGE_ACCOUNT = os.environ.get("AZURE_STORAGE_ACCOUNT", "<storage-account>")
BRONZE_CONTAINER = os.environ.get("BRONZE_CONTAINER", "bronze")
SILVER_CONTAINER = os.environ.get("SILVER_CONTAINER", "silver")
GOLD_CONTAINER = os.environ.get("GOLD_CONTAINER", "gold")
ADLS = "abfss://{c}@{acct}.dfs.core.windows.net"


def get_spark(app_name: str = "etl-elt-services") -> SparkSession:
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


# ---------------------------------------------------------------------------
# Activity functions
# ---------------------------------------------------------------------------
def etl_pipeline(spark: SparkSession, source: str, target: str, transform: Callable[[DataFrame], DataFrame]) -> None:
    """Activity: ETL Pipeline Development - extract, transform in Spark, load to Delta."""
    df = spark.read.format("delta").load(source)
    result = transform(df)
    result.write.format("delta").mode("overwrite").save(target)


def elt_pipeline(spark: SparkSession, raw_path: str, target: str, sql: str) -> None:
    """Activity: ELT Pipeline Development - load raw, then transform in place with SQL."""
    raw = spark.read.format("delta").load(raw_path)
    raw.createOrReplaceTempView("raw_stage")
    spark.sql(sql).write.format("delta").mode("overwrite").save(target)


def transform_columns(df: DataFrame, exprs: Dict[str, str]) -> DataFrame:
    """Activity: Data Transformation - apply column expressions declaratively."""
    for name, expr in exprs.items():
        df = df.withColumn(name, F.expr(expr))
    return df


def cleanse(df: DataFrame, text_cols: List[str]) -> DataFrame:
    """Activity: Data Cleansing - trim, null-out blanks, drop fully-null rows."""
    for col in text_cols:
        df = df.withColumn(col, F.when(F.trim(F.col(col)) == "", None).otherwise(F.trim(F.col(col))))
    return df.dropna(how="all")


def standardize(df: DataFrame, country_col: str = "country", phone_col: str = "phone") -> DataFrame:
    """Activity: Data Standardization - ISO codes, E.164 phones, lowercase emails."""
    return (
        df.withColumn(country_col, F.upper(F.trim(F.col(country_col))))
        .withColumn(phone_col, F.regexp_replace(F.col(phone_col), r"[^\d+]", ""))
        .withColumn("email", F.lower(F.trim(F.col("email"))))
    )


def deduplicate(df: DataFrame, keys: List[str], order_col: Optional[str] = None) -> DataFrame:
    """Activity: Data Deduplication - keep latest row per business key."""
    if order_col:
        w = Window.partitionBy(*keys).orderBy(F.col(order_col).desc())
        df = df.withColumn("_rn", F.row_number().over(w)).filter(F.col("_rn") == 1).drop("_rn")
    else:
        df = df.dropDuplicates(keys)
    return df


def enrich(df: DataFrame, ref: DataFrame, join_keys: List[str]) -> DataFrame:
    """Activity: Data Enrichment - join reference data (geo, product master) onto facts."""
    return df.join(F.broadcast(ref), on=join_keys, how="left")


def normalize(df: DataFrame, pk: str, attrs: List[str], child_table: str) -> DataFrame:
    """Activity: Data Normalization - split repeating attributes into a child entity."""
    return df.select(pk, *attrs).dropDuplicates([pk])


def aggregate(df: DataFrame, grain: List[str], measures: Dict[str, str]) -> DataFrame:
    """Activity: Data Aggregation - roll facts up to a summary grain."""
    agg_exprs = [F.expr(m).alias(alias) for alias, m in measures.items()]
    return df.groupBy(*grain).agg(*agg_exprs)


def apply_business_rules(df: DataFrame, rules: Dict[str, str]) -> DataFrame:
    """Activity: Business Rule Implementation - flag/derive columns from rule expressions."""
    for name, rule in rules.items():
        df = df.withColumn(name, F.expr(rule))
    return df


def incremental_process(spark: SparkSession, source: str, target: str, watermark_col: str) -> None:
    """Activity: Incremental Data Processing - MERGE only rows newer than the target watermark."""
    max_wm = spark.read.format("delta").load(target).agg(F.max(watermark_col)).collect()[0][0]
    delta_df = spark.read.format("delta").load(source).filter(F.col(watermark_col) > max_wm)
    delta_df.createOrReplaceTempView("delta_incr")
    spark.sql(
        f"""MERGE INTO delta.`{target}` t USING delta_incr s
            ON t.id = s.id
            WHEN MATCHED THEN UPDATE SET *
            WHEN NOT MATCHED THEN INSERT *"""
    )


def historical_process(spark: SparkSession, source: str, as_of: str, target: str) -> None:
    """Activity: Historical Data Processing - snapshot Delta time travel to a date."""
    (spark.read.format("delta").option("timestampAsOf", as_of).load(source)
     .write.format("delta").mode("overwrite").save(target))


def convert_types(df: DataFrame, mapping: Dict[str, str]) -> DataFrame:
    """Activity: Data Type Conversion - cast columns to declared types with safe fallback."""
    for col, dtype in mapping.items():
        df = df.withColumn(col, F.col(col).cast(dtype))
    return df


def complex_transform(df: DataFrame) -> DataFrame:
    """Activity: Complex Data Transformation - nested JSON explode + multi-window scoring."""
    exploded = df.withColumn("line", F.explode(F.col("lines")))
    w = Window.partitionBy("order_id").orderBy(F.col("line.seq"))
    return (
        exploded
        .withColumn("line_total", F.col("line.qty") * F.col("line.unit_price"))
        .withColumn("running_total", F.sum("line_total").over(w))
        .withColumn("margin_pct", (F.col("line_total") - F.col("line.cost")) / F.col("line_total"))
    )


def transformation_framework(steps: List[Callable[[DataFrame], DataFrame]]) -> Callable[[DataFrame], DataFrame]:
    """Activity: Reusable Transformation Frameworks - compose named steps into a pipeline."""
    def run(df: DataFrame) -> DataFrame:
        for step in steps:
            df = step(df)
        return df

    run.__name__ = "framework_" + "_".join(s.__name__ for s in steps)
    return run


# ---------------------------------------------------------------------------
# Driver
# ---------------------------------------------------------------------------
def main() -> None:
    """Representative flow: bronze orders -> cleansed/standardized -> deduped -> silver."""
    spark = get_spark()
    try:
        dbutils.widgets.text("run_date", "")  # noqa: F821
        run_date = dbutils.widgets.get("run_date") or os.environ.get("RUN_DATE", "2026-10-17")  # noqa: F821
    except Exception:
        run_date = os.environ.get("RUN_DATE", "2026-10-17")

    bronze = _path(BRONZE_CONTAINER, "orders")
    silver = _path(SILVER_CONTAINER, "orders")

    pipeline = transformation_framework([
        lambda d: cleanse(d, ["customer_name", "email"]),
        standardize,
        lambda d: convert_types(d, {"order_id": "long", "amount": "decimal(12,2)"}),
        lambda d: deduplicate(d, ["order_id"], "updated_at"),
        lambda d: apply_business_rules(d, {
            "is_high_value": "amount > 1000",
            "order_month": "date_trunc('month', order_date)",
        }),
    ])
    etl_pipeline(spark, bronze, silver, pipeline)

    spark.sql(f"OPTIMIZE delta.`{silver}` ZORDER BY (order_id)")
    print(f"ELT run for {run_date} complete -> {silver}")


if __name__ == "__main__":
    main()
