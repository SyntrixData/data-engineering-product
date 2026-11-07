"""Category 15: Metadata Management and Data Cataloging.

Catalog and metadata services for Azure Databricks + ADLS Gen2 + Delta Lake,
aligned to Unity Catalog / Microsoft Purview concepts: automated metadata
extraction, catalog implementation, documentation, discovery, asset
inventory, business glossary, ownership, lineage docs, classification,
searchable catalogs, and usage tracking.
"""

import os
from typing import Dict, List, Optional

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F


# ----------------------------------------------------------------------------
# Config block (env vars with placeholder defaults)
# ----------------------------------------------------------------------------
CONFIG = {
    "CATALOG_DB": os.environ.get("CATALOG_DB", "data_catalog"),
    "PURVIEW_ACCOUNT": os.environ.get("PURVIEW_ACCOUNT", "purview-catalog"),
    "UNITY_CATALOG": os.environ.get("UNITY_CATALOG", "main"),
}

try:
    _widgets = dbutils.widgets  # type: ignore[name-defined]
    _widgets.text("schema_pattern", "*")
    _widgets.text("search_term", "")
except Exception:
    pass


def get_spark(app_name: str = "metadata-cataloging") -> SparkSession:
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


def _ensure_catalog_db(spark: SparkSession) -> None:
    """Create the backing catalog database if it does not exist."""
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CONFIG['CATALOG_DB']}")


# ----------------------------------------------------------------------------
# Activity implementations
# ----------------------------------------------------------------------------

def extract_metadata(spark: SparkSession, schema_pattern: str = "*") -> DataFrame:
    """Automated Metadata Extraction: table/column metadata via information schema."""
    return spark.sql(f"""
        SELECT table_catalog, table_schema, table_name, column_name, data_type
        FROM {CONFIG['UNITY_CATALOG']}.information_schema.columns
        WHERE table_schema LIKE '{schema_pattern}'
    """)


def implement_catalog(spark: SparkSession) -> None:
    """Data Catalog Implementation: create the backing catalog tables."""
    _ensure_catalog_db(spark)
    db = CONFIG["CATALOG_DB"]
    spark.sql(f"""CREATE TABLE IF NOT EXISTS {db}.datasets
        (dataset_id STRING, catalog STRING, schema_name STRING, table_name STRING,
         description STRING, owner STRING, classification STRING,
         created_at TIMESTAMP) USING DELTA""")
    spark.sql(f"""CREATE TABLE IF NOT EXISTS {db}.glossary
        (term STRING, definition STRING, domain STRING, steward STRING) USING DELTA""")
    spark.sql(f"""CREATE TABLE IF NOT EXISTS {db}.lineage
        (source STRING, target STRING, transform STRING, captured_at TIMESTAMP) USING DELTA""")
    spark.sql(f"""CREATE TABLE IF NOT EXISTS {db}.usage
        (table_name STRING, principal STRING, query_count BIGINT, last_accessed TIMESTAMP) USING DELTA""")


def document_tables_columns(spark: SparkSession, table: str, descriptions: Dict[str, str]) -> None:
    """Table and Column Documentation: apply COMMENT metadata to columns."""
    for col, desc in descriptions.items():
        spark.sql(f"ALTER TABLE {table} ALTER COLUMN {col} COMMENT '{desc}'")


def discover_datasets(spark: SparkSession, pattern: str) -> DataFrame:
    """Dataset Discovery: list tables matching a naming pattern."""
    return spark.sql(f"SHOW TABLES LIKE '{pattern}'")


def build_asset_inventory(spark: SparkSession, schemas: List[str]) -> DataFrame:
    """Data Asset Inventory: union of tables across schemas into an inventory frame."""
    frames = [spark.sql(f"SHOW TABLES IN {s}").withColumn("schema_name", F.lit(s)) for s in schemas]
    inv = frames[0]
    for other in frames[1:]:
        inv = inv.unionByName(other)
    return inv


def manage_business_glossary(spark: SparkSession, terms: List[Dict[str, str]]) -> None:
    """Business Glossary Management: upsert approved business terms."""
    df = spark.createDataFrame(terms)
    df.createOrReplaceTempView("glossary_src")
    db = CONFIG["CATALOG_DB"]
    spark.sql(f"""
        MERGE INTO {db}.glossary t USING glossary_src s ON t.term = s.term
        WHEN MATCHED THEN UPDATE SET * WHEN NOT MATCHED THEN INSERT *
    """)


def manage_technical_metadata(spark: SparkSession, table: str) -> DataFrame:
    """Technical Metadata Management: DESCRIBE DETAIL plus partition info."""
    return spark.sql(f"DESCRIBE DETAIL {table}")


def assign_data_ownership(spark: SparkSession, table: str, owner: str) -> None:
    """Data Ownership Assignment: grant ownership and record the owner in the catalog."""
    spark.sql(f"ALTER TABLE {table} OWNER TO `{owner}`")
    db = CONFIG["CATALOG_DB"]
    spark.sql(f"UPDATE {db}.datasets SET owner = '{owner}' WHERE table_name = '{table.split('.')[-1]}'")


def document_lineage(spark: SparkSession, source: str, target: str, transform: str = "") -> None:
    """Data Lineage Documentation: record a source-to-target lineage edge."""
    db = CONFIG["CATALOG_DB"]
    edge = spark.createDataFrame([{"source": source, "target": target,
                                   "transform": transform, "captured_at": None}]) \
        .withColumn("captured_at", F.current_timestamp())
    edge.write.format("delta").mode("append").saveAsTable(f"{db}.lineage")


def classify_data(spark: SparkSession, table: str, column: str, classification: str) -> None:
    """Data Classification: tag a column (e.g. PII, Confidential) in the catalog."""
    db = CONFIG["CATALOG_DB"]
    spark.sql(f"""UPDATE {db}.datasets SET classification = '{classification}'
                  WHERE table_name = '{table.split('.')[-1]}'""")


def sync_metadata(spark: SparkSession, schema_pattern: str = "*") -> None:
    """Metadata Synchronization: refresh the catalog datasets table from live metadata."""
    meta = extract_metadata(spark, schema_pattern)
    db = CONFIG["CATALOG_DB"]
    (meta.groupBy("table_catalog", "table_schema", "table_name")
     .agg(F.count("*").alias("column_count"))
     .withColumnRenamed("table_catalog", "catalog")
     .withColumnRenamed("table_schema", "schema_name")
     .withColumn("description", F.lit(""))
     .withColumn("owner", F.lit("unassigned"))
     .withColumn("classification", F.lit("unclassified"))
     .withColumn("created_at", F.current_timestamp())
     .withColumn("dataset_id", F.concat_ws(".", F.col("catalog"), F.col("schema_name"), F.col("table_name")))
     .write.format("delta").mode("overwrite").saveAsTable(f"{db}.datasets"))


def search_catalog(spark: SparkSession, term: str) -> DataFrame:
    """Searchable Data Catalogs: full-text style search across catalog tables."""
    db = CONFIG["CATALOG_DB"]
    return spark.sql(f"""
        SELECT dataset_id, table_name, description, owner, classification
        FROM {db}.datasets
        WHERE lower(table_name) LIKE '%{term.lower()}%'
           OR lower(description) LIKE '%{term.lower()}%'
    """)


def track_dataset_usage(spark: SparkSession, table: str, principal: str) -> None:
    """Dataset Usage Tracking: log an access event for a dataset."""
    db = CONFIG["CATALOG_DB"]
    event = (spark.createDataFrame([{"table_name": table, "principal": principal,
                                     "query_count": 1, "last_accessed": None}])
             .withColumn("last_accessed", F.current_timestamp()))
    event.write.format("delta").mode("append").saveAsTable(f"{db}.usage")


def generate_documentation(spark: SparkSession, table: str, output_path: str) -> None:
    """Data Documentation Generation: export table docs (columns + comments) as JSON."""
    docs = spark.sql(f"DESCRIBE TABLE {table}")
    docs.write.mode("overwrite").json(f"{output_path}/{table.replace('.', '_')}")


def main() -> None:
    """Representative flow: implement catalog, extract, document, classify, search."""
    spark = get_spark()
    implement_catalog(spark)
    sync_metadata(spark, schema_pattern="mdm")
    document_tables_columns(spark, "mdm.customer_master",
                            {"customer_id": "Surrogate key for the golden customer record",
                             "email": "Primary contact email (PII)"})
    assign_data_ownership(spark, "mdm.customer_master", "data-stewards@contoso.com")
    classify_data(spark, "mdm.customer_master", "email", "PII")
    document_lineage(spark, "mdm.stg_crm_customers", "mdm.customer_master", "golden-record survivorship")
    manage_business_glossary(spark, [{"term": "Golden Record", "definition": "Single trusted master record",
                                      "domain": "MDM", "steward": "data-stewards@contoso.com"}])
    track_dataset_usage(spark, "mdm.customer_master", "analysts@contoso.com")
    search_catalog(spark, "customer").show()


if __name__ == "__main__":
    main()
