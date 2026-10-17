"""Category 14: Master Data Management (MDM).

MDM hub on Azure Databricks + ADLS Gen2 + Delta Lake: customer/product/
supplier/employee master domains, golden-record development, duplicate
entity identification, record matching and merging, master synchronization,
master data quality monitoring, and governance workflows.
"""

import os
from typing import Dict, List, Optional

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F


# ----------------------------------------------------------------------------
# Config block (env vars with placeholder defaults)
# ----------------------------------------------------------------------------
CONFIG = {
    "MDM_DB": os.environ.get("MDM_DB", "mdm"),
    "QUARANTINE_DB": os.environ.get("QUARANTINE_DB", "mdm_quarantine"),
}

try:
    _widgets = dbutils.widgets  # type: ignore[name-defined]
    _widgets.text("domain", "customer")
    _widgets.text("match_threshold", "0.85")
except Exception:
    pass


def get_spark(app_name: str = "master-data-management") -> SparkSession:
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


# ----------------------------------------------------------------------------
# Activity implementations
# ----------------------------------------------------------------------------

def build_customer_master(spark: SparkSession, sources: List[str]) -> DataFrame:
    """Customer Master Data Management: union customer feeds into one staging frame."""
    frames = [spark.table(t).withColumn("_source", F.lit(t)) for t in sources]
    df = frames[0]
    for other in frames[1:]:
        df = df.unionByName(other, allowMissingColumns=True)
    return df


def build_product_master(spark: SparkSession, sources: List[str]) -> DataFrame:
    """Product Master Data Management: union product feeds into one staging frame."""
    frames = [spark.table(t).withColumn("_source", F.lit(t)) for t in sources]
    df = frames[0]
    for other in frames[1:]:
        df = df.unionByName(other, allowMissingColumns=True)
    return df


def build_supplier_master(spark: SparkSession, sources: List[str]) -> DataFrame:
    """Supplier Master Data Management: union supplier feeds into one staging frame."""
    frames = [spark.table(t).withColumn("_source", F.lit(t)) for t in sources]
    df = frames[0]
    for other in frames[1:]:
        df = df.unionByName(other, allowMissingColumns=True)
    return df


def build_employee_master(spark: SparkSession, sources: List[str]) -> DataFrame:
    """Employee Master Data Management: union employee feeds into one staging frame."""
    frames = [spark.table(t).withColumn("_source", F.lit(t)) for t in sources]
    df = frames[0]
    for other in frames[1:]:
        df = df.unionByName(other, allowMissingColumns=True)
    return df


def identify_duplicates(df: DataFrame, keys: List[str]) -> DataFrame:
    """Duplicate Entity Identification: flag records sharing business keys."""
    w = __import__("pyspark.sql.window", fromlist=["Window"]).Window.partitionBy(*keys)
    return df.withColumn("_dup_count", F.count("*").over(w)).filter(F.col("_dup_count") > 1)


def match_records(df: DataFrame, name_col: str, threshold: float = 0.85) -> DataFrame:
    """Record Matching: fuzzy-match candidate pairs on a normalized name column."""
    norm = df.withColumn("_norm", F.lower(F.regexp_replace(F.col(name_col), r"[^a-z0-9 ]", "")))
    a = norm.alias("a").join(norm.alias("b"), F.col("a.id") < F.col("b.id"))
    return a.withColumn("_similarity",
                        F.levenshtein(F.col("a._norm"), F.col("b._norm")) /
                        F.greatest(F.length(F.col("a._norm")), F.length(F.col("b._norm")))) \
        .withColumn("_score", 1 - F.col("_similarity")).filter(F.col("_score") >= threshold)


def merge_records(df: DataFrame, pairs: DataFrame, id_col: str = "id") -> DataFrame:
    """Record Merging: collapse matched pairs, keeping the lowest id as survivor."""
    survivors = pairs.select(F.least(F.col("a.id"), F.col("b.id")).alias("survivor"),
                             F.greatest(F.col("a.id"), F.col("b.id")).alias("victim")).distinct()
    merged = df.join(survivors, df[id_col] == survivors["victim"], "left_anti")
    return merged


def create_master_record(spark: SparkSession, golden_df: DataFrame, master_table: str) -> None:
    """Master Record Creation: persist golden records to the MDM hub table."""
    golden_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(master_table)


def develop_golden_records(df: DataFrame, id_col: str, priority_source: Optional[str] = None) -> DataFrame:
    """Golden Record Development: survivorship rules across duplicate clusters."""
    w = __import__("pyspark.sql.window", fromlist=["Window"]).Window.partitionBy(id_col) \
        .orderBy(F.col("_source") == F.lit(priority_source or ""), F.desc("_ingested_at"))
    deduped = df.withColumn("_rn", F.row_number().over(w)).filter(F.col("_rn") == 1).drop("_rn")
    return deduped


def synchronize_master_data(spark: SparkSession, master_table: str, downstream: List[str]) -> None:
    """Master Data Synchronization: MERGE hub records into downstream consumers."""
    spark.table(master_table).createOrReplaceTempView("hub")
    for table in downstream:
        spark.sql(f"""
            MERGE INTO {table} t USING hub s ON t.id = s.id
            WHEN MATCHED THEN UPDATE SET * WHEN NOT MATCHED THEN INSERT *
        """)


def monitor_master_quality(spark: SparkSession, master_table: str) -> Dict[str, object]:
    """Master Data Quality Monitoring: completeness, uniqueness, and freshness."""
    df = spark.table(master_table)
    total = df.count()
    null_pk = df.filter(F.col("id").isNull()).count()
    distinct_pk = df.select("id").distinct().count()
    return {"total": total, "null_keys": null_pk,
            "uniqueness_ratio": (distinct_pk / total) if total else 0.0}


def run_governance_workflow(spark: SparkSession, master_table: str, steward: str) -> None:
    """Master Data Governance Workflows: steward approval log entry for a release."""
    log = spark.createDataFrame([{"table": master_table, "steward": steward,
                                  "approved_at": None, "status": "pending_review"}])
    log = log.withColumn("approved_at", F.current_timestamp())
    log.write.format("delta").mode("append").saveAsTable(f"{CONFIG['QUARANTINE_DB']}.governance_log")


def main() -> None:
    """Representative flow: build customer master, dedupe, golden records, sync."""
    spark = get_spark()
    staged = build_customer_master(spark, ["mdm.stg_crm_customers", "mdm.stg_erp_customers"])
    dupes = identify_duplicates(staged, ["email"])
    print(f"duplicate clusters found: {dupes.select('email').distinct().count()}")
    golden = develop_golden_records(staged, "customer_id", priority_source="mdm.stg_crm_customers")
    create_master_record(spark, golden, "mdm.customer_master")
    synchronize_master_data(spark, "mdm.customer_master", ["mdm.customer_mart"])
    print(monitor_master_quality(spark, "mdm.customer_master"))
    run_governance_workflow(spark, "mdm.customer_master", "data-steward@contoso.com")


if __name__ == "__main__":
    main()
