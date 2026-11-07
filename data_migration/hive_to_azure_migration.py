"""
29. Data Migration: Source Hive -> Azure Blob -> Azure Data Lake
================================================================
Azure Data Engineering Product | PySpark

PURPOSE
-------
Migrate data from a source Hive system into Azure - in three explicit steps:

    STEP 1  Read the table from the source Hive metastore and capture its
            metadata (columns, types, partitions, location, row count).
    STEP 2  Land BOTH the data (Parquet) and the metadata (JSON) into
            Azure Blob Storage - a durable, replayable staging area.
    STEP 3  Read the metadata back from Blob, create the schema (database)
            and table in Azure Data Lake Storage (Delta Lake), and load
            the data from Blob into the lake table.

The entry point is ONE function:

    migrate_hive_table_to_azure(spark, schema_name, table_name)

taking exactly the two inputs the migration needs: the source Hive schema
(database) name and the table name. Everything else (accounts, containers,
keys) comes from configuration below - environment variables / Key Vault,
never hardcoded credentials.

Run: spark-submit --packages io.delta:delta-spark_2.12:3.x 29_hive_to_azure_migration.py
"""

import json
import os
from typing import Any, Dict, List, Optional, Tuple

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F

# ---------------------------------------------------------------------------
# CONFIGURATION
# ---------------------------------------------------------------------------
# Staging lives in classic Azure Blob Storage (wasbs:// scheme); the final
# target is Azure Data Lake Storage Gen2 (abfss:// scheme). Both are the same
# underlying Azure storage - Blob is used here as a cheap landing zone and
# ADLS as the governed lake with a hierarchical namespace.
CONFIG = {
    # --- Step 2 staging: Azure Blob Storage ---
    "blob_account": os.environ.get("AZURE_BLOB_ACCOUNT", "<blob-account>"),
    "blob_container": os.environ.get("AZURE_BLOB_CONTAINER", "migration-staging"),
    # --- Step 3 target: Azure Data Lake Storage Gen2 ---
    "lake_account": os.environ.get("AZURE_LAKE_ACCOUNT", "<lake-account>"),
    "lake_container": os.environ.get("AZURE_LAKE_CONTAINER", "lake"),
    # Target database in the lake; source schema is kept as a sub-database
    # suffix unless TARGET_DB_MODE=flat. "lake_customers" style by default.
    "target_db_prefix": os.environ.get("TARGET_DB_PREFIX", "lake"),
    # How much data history to keep: overwrite | append
    "write_mode": os.environ.get("MIGRATION_WRITE_MODE", "overwrite"),
}


# ---------------------------------------------------------------------------
# SPARK SESSION
# ---------------------------------------------------------------------------
def get_spark(app_name: str = "hive-to-azure-migration") -> SparkSession:
    """Create (or reuse) a Hive-enabled Spark session wired for Azure.

    - enableHiveSupport() is required to read the SOURCE Hive metastore.
    - Blob auth uses the account key (from env / Key Vault).
    - ADLS Gen2 auth uses OAuth (service principal) - the recommended mode.
    """
    try:
        active = SparkSession.getActiveSession()
        if active is not None:
            return active
    except Exception:
        pass

    spark = (
        SparkSession.builder.appName(app_name)
        .enableHiveSupport()  # source tables come from the Hive metastore
        .getOrCreate()
    )

    # --- Azure Blob Storage (wasbs) authentication: account key ---
    blob_key = os.environ.get("AZURE_BLOB_KEY", "")
    if blob_key:
        spark.conf.set(
            f"fs.azure.account.key.{CONFIG['blob_account']}.blob.core.windows.net",
            blob_key,
        )

    # --- Azure Data Lake Gen2 (abfss) authentication: OAuth service principal ---
    client_id = os.environ.get("AZURE_CLIENT_ID", "")
    client_secret = os.environ.get("AZURE_CLIENT_SECRET", "")
    tenant_id = os.environ.get("AZURE_TENANT_ID", "")
    acct = CONFIG["lake_account"]
    if client_id and client_secret and tenant_id:
        spark.conf.set(f"fs.azure.account.auth.type.{acct}.dfs.core.windows.net",
                       "OAuth")
        spark.conf.set(f"fs.azure.account.oauth.provider.type.{acct}.dfs.core.windows.net",
                       "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
        spark.conf.set(f"fs.azure.account.oauth2.client.id.{acct}.dfs.core.windows.net",
                       client_id)
        spark.conf.set(f"fs.azure.account.oauth2.client.secret.{acct}.dfs.core.windows.net",
                       client_secret)
        spark.conf.set(
            f"fs.azure.account.oauth2.client.endpoint.{acct}.dfs.core.windows.net",
            f"https://login.microsoftonline.com/{tenant_id}/oauth2/token")
    return spark


# ---------------------------------------------------------------------------
# PATH HELPERS
# ---------------------------------------------------------------------------
def blob_base() -> str:
    """Root of the migration staging area in Azure Blob Storage."""
    return (f"wasbs://{CONFIG['blob_container']}"
            f"@{CONFIG['blob_account']}.blob.core.windows.net/migration")


def lake_base() -> str:
    """Root of the target data lake in ADLS Gen2."""
    return (f"abfss://{CONFIG['lake_container']}"
            f"@{CONFIG['lake_account']}.dfs.core.windows.net")


def staging_paths(schema_name: str, table_name: str) -> Tuple[str, str]:
    """(data_path, metadata_path) for one table inside Blob staging."""
    root = f"{blob_base()}/{schema_name}/{table_name}"
    return f"{root}/data", f"{root}/metadata.json"


def target_db_name(schema_name: str) -> str:
    """Lake database for a source schema, e.g. sales -> lake_sales."""
    return f"{CONFIG['target_db_prefix']}_{schema_name}".lower()


# ===========================================================================
# STEP 1 - READ FROM SOURCE HIVE + CAPTURE METADATA
# ===========================================================================
def read_hive_table_with_metadata(
    spark: SparkSession, schema_name: str, table_name: str
) -> Tuple[DataFrame, Dict[str, Any]]:
    """STEP 1: read a source Hive table and collect its metadata.

    Returns (dataframe, metadata) where metadata is a plain dict capturing
    everything Step 3 needs to recreate the table in the lake:
    columns (name/type/nullable), partition columns, source location,
    table type, and the exact Spark schema as JSON.
    """
    fq_name = f"{schema_name}.{table_name}"
    print(f"[STEP 1] Reading source Hive table {fq_name} ...")

    # -- the data itself --
    df = spark.table(fq_name)

    # -- the metadata: Spark's Catalog API is the reliable source --
    catalog_table = spark.catalog.getTable(fq_name)
    storage = catalog_table.storage

    metadata: Dict[str, Any] = {
        "source_schema": schema_name,
        "source_table": table_name,
        "fully_qualified_name": fq_name,
        # Column definitions straight from the table schema
        "columns": [
            {
                "name": f.name,
                "type": f.dataType.simpleString(),   # e.g. "bigint", "decimal(10,2)"
                "nullable": f.nullable,
            }
            for f in df.schema.fields
        ],
        # The full schema serialized - Step 3 replays this verbatim
        "schema_json": df.schema.json(),
        "partition_columns": list(catalog_table.partitionColumnNames),
        "table_type": catalog_table.tableType,        # MANAGED / EXTERNAL / VIEW
        "source_location": storage.locationUri if storage else None,
        "source_format": (storage.inputFormat or "") if storage else "",
        "description": catalog_table.description,
        # Row count helps Step 3 validation prove nothing was lost
        "row_count": df.count(),
    }
    print(f"[STEP 1] {fq_name}: {metadata['row_count']} rows, "
          f"{len(metadata['columns'])} columns, "
          f"partitions={metadata['partition_columns']}")
    return df, metadata


# ===========================================================================
# STEP 2 - LAND DATA + METADATA IN AZURE BLOB STORAGE
# ===========================================================================
def write_to_azure_blob(
    spark: SparkSession,
    df: DataFrame,
    metadata: Dict[str, Any],
    schema_name: str,
    table_name: str,
) -> Tuple[str, str]:
    """STEP 2: write the table data (Parquet) and metadata (JSON) to Blob.

    Layout in the staging container:
        migration/<schema>/<table>/data/           <- Parquet files
        migration/<schema>/<table>/metadata.json   <- one JSON document

    Parquet is used for staging because it is self-describing (schema is
    embedded) and splittable; the JSON sidecar carries the Hive-level
    metadata Parquet cannot express (partitions, table type, provenance).
    Returns (data_path, metadata_path).
    """
    data_path, metadata_path = staging_paths(schema_name, table_name)
    print(f"[STEP 2] Landing data -> {data_path}")

    # -- data: preserve the source partitioning when the table has any --
    writer = df.write.mode("overwrite").format("parquet")
    if metadata["partition_columns"]:
        writer = writer.partitionBy(*metadata["partition_columns"])
    writer.save(data_path)

    # -- metadata: a single-row DataFrame is the simplest way to write one
    #    JSON object to object storage with Spark alone --
    print(f"[STEP 2] Landing metadata -> {metadata_path}")
    meta_df = spark.createDataFrame(
        [(json.dumps(metadata),)], ["metadata_json"]
    )
    # coalesce(1) -> exactly one file; rename it to metadata.json below via
    # the single-part output directory convention used by Step 3's reader.
    (meta_df.coalesce(1).write.mode("overwrite")
     .option("header", "false").csv(f"{metadata_path}.tmp"))
    _rename_single_part_file(spark, f"{metadata_path}.tmp", metadata_path)

    return data_path, metadata_path


def _rename_single_part_file(spark: SparkSession, tmp_dir: str,
                             dest_file: str) -> None:
    """Rename Spark's part-00000-* output to a clean single file name.

    Uses the JVM Hadoop FileSystem API through py4j - available in every
    Spark runtime, no extra tooling needed.
    """
    jvm = spark.sparkContext._jvm
    hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
    Path = jvm.org.apache.hadoop.fs.Path
    fs = Path(tmp_dir).getFileSystem(hadoop_conf)

    # find the single part file Spark wrote
    part = None
    for status in fs.listStatus(Path(tmp_dir)):
        name = status.getPath().getName()
        if name.startswith("part-"):
            part = status.getPath()
            break
    if part is None:
        raise FileNotFoundError(f"no part file found under {tmp_dir}")
    fs.rename(part, Path(dest_file))
    fs.delete(Path(tmp_dir), True)  # clean up _SUCCESS etc.


# ===========================================================================
# STEP 3 - CREATE SCHEMA + TABLE IN AZURE DATA LAKE AND LOAD
# ===========================================================================
def build_create_table_ddl(
    db: str, table: str, metadata: Dict[str, Any], location: str
) -> str:
    """Build the Delta CREATE TABLE DDL from the staged metadata.

    Partition columns are declared via PARTITIONED BY (Delta requires them
    to also appear in the column list, which they do - they were real
    columns in the Hive table).
    """
    col_defs = ",\n  ".join(
        f"`{c['name']}` {c['type']}" for c in metadata["columns"]
    )
    partition_clause = ""
    if metadata["partition_columns"]:
        parts = ", ".join(f"`{p}`" for p in metadata["partition_columns"])
        partition_clause = f"\nPARTITIONED BY ({parts})"
    return (
        f"CREATE TABLE IF NOT EXISTS {db}.{table} (\n  {col_defs}\n) "
        f"USING DELTA{partition_clause}\nLOCATION '{location}'"
    )


def load_from_blob_to_adls(
    spark: SparkSession, schema_name: str, table_name: str
) -> Dict[str, Any]:
    """STEP 3: recreate the table in Azure Data Lake and load its data.

    Reads the metadata JSON written in Step 2 (single source of truth),
    creates the lake database + Delta table, then streams the staged
    Parquet data into it. Returns a load summary dict.
    """
    data_path, metadata_path = staging_paths(schema_name, table_name)
    print(f"[STEP 3] Reading staged metadata <- {metadata_path}")

    # -- metadata back from Blob (it is a one-line JSON file) --
    raw = spark.read.text(metadata_path).collect()
    metadata: Dict[str, Any] = json.loads("".join(r["value"] for r in raw))

    db = target_db_name(schema_name)
    table_location = f"{lake_base()}/{db}.db/{table_name}"

    # -- create the schema (database) in the lake, rooted on ADLS --
    print(f"[STEP 3] Creating lake database {db} ...")
    spark.sql(
        f"CREATE DATABASE IF NOT EXISTS {db} "
        f"LOCATION '{lake_base()}/{db}.db'"
    )

    # -- create the table from the Hive metadata captured in Step 1 --
    ddl = build_create_table_ddl(db, table_name, metadata, table_location)
    print(f"[STEP 3] Creating table {db}.{table_name} (Delta) ...")
    spark.sql(ddl)

    # -- load: read the staged Parquet, align to the metadata schema --
    print(f"[STEP 3] Loading data <- {data_path}")
    from pyspark.sql.types import StructType

    target_schema = StructType.fromJson(json.loads(metadata["schema_json"]))
    staged = spark.read.schema(target_schema).parquet(data_path)

    # INSERT-style load keeps the DDL-created table definition authoritative
    (staged.write.format("delta")
     .mode(CONFIG["write_mode"])
     .option("mergeSchema", "true")
     .save(table_location))

    loaded = spark.table(f"{db}.{table_name}").count()
    summary = {
        "source": metadata["fully_qualified_name"],
        "target": f"{db}.{table_name}",
        "target_location": table_location,
        "expected_rows": metadata["row_count"],
        "loaded_rows": loaded,
        "row_count_match": loaded == metadata["row_count"],
    }
    print(f"[STEP 3] Done: {summary}")
    return summary


# ===========================================================================
# ORCHESTRATOR - THE FUNCTION THE CALLER USES
# ===========================================================================
def migrate_hive_table_to_azure(
    spark: SparkSession, schema_name: str, table_name: str
) -> Dict[str, Any]:
    """Migrate ONE Hive table to Azure Data Lake, end to end.

    INPUTS (the only two the caller must supply):
        schema_name - source Hive database/schema, e.g. "sales"
        table_name  - source Hive table,          e.g. "orders"

    Flow: Step 1 read Hive + metadata -> Step 2 land both in Azure Blob
    -> Step 3 create schema/table in ADLS and load. Step 2 is deliberately
    a separate landing: if Step 3 ever fails, the migration can be replayed
    from Blob without touching the source Hive system again.

    Returns the Step 3 load summary (incl. row-count validation).
    """
    print(f"=== Migrating {schema_name}.{table_name} "
          f"Hive -> Blob -> Azure Data Lake ===")

    # STEP 1 - source Hive read + metadata capture
    df, metadata = read_hive_table_with_metadata(spark, schema_name, table_name)

    # STEP 2 - land data + metadata in Azure Blob Storage
    write_to_azure_blob(spark, df, metadata, schema_name, table_name)

    # STEP 3 - create schema/table in Azure Data Lake + load from Blob
    summary = load_from_blob_to_adls(spark, schema_name, table_name)

    if not summary["row_count_match"]:
        # Do not fail silently: surface the mismatch, keep the staged copy.
        print(f"WARNING: row count mismatch for {schema_name}.{table_name} "
              f"(expected {summary['expected_rows']}, "
              f"loaded {summary['loaded_rows']}). Staged data retained at "
              f"{staging_paths(schema_name, table_name)[0]} for investigation.")
    return summary


def migrate_schema_to_azure(
    spark: SparkSession, schema_name: str,
    tables: Optional[List[str]] = None,
) -> List[Dict[str, Any]]:
    """Migrate a whole Hive schema (or a chosen list of its tables).

    When `tables` is None, every table in the schema is discovered from
    the Hive metastore. One table's failure does not stop the rest - it is
    recorded in the results with its error.
    """
    if tables is None:
        tables = [t.name for t in spark.catalog.listTables(schema_name)
                  if not t.isTemporary]
    results: List[Dict[str, Any]] = []
    for tbl in tables:
        try:
            results.append(
                migrate_hive_table_to_azure(spark, schema_name, tbl))
        except Exception as e:  # keep migrating the remaining tables
            results.append({"source": f"{schema_name}.{tbl}",
                            "error": str(e)[:500]})
            print(f"ERROR migrating {schema_name}.{tbl}: {e}")
    return results


# ===========================================================================
# MAIN - example usage
# ===========================================================================
def main() -> None:
    """Example: migrate sales.orders, then the whole hr schema."""
    spark = get_spark()

    # -- single table: the two-argument call the task specifies --
    summary = migrate_hive_table_to_azure(spark, "sales", "orders")
    print(json.dumps(summary, indent=2, default=str))

    # -- whole schema in one call --
    # results = migrate_schema_to_azure(spark, "hr")
    # print(json.dumps(results, indent=2, default=str))

    spark.stop()


if __name__ == "__main__":
    main()
