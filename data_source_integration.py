"""01 - Data Source Integration and Ingestion.

PySpark integration and ingestion patterns for Azure Databricks backed by
ADLS Gen2 and Delta Lake. Covers the 16 activities of Category 1:
  Database Integration, API Integration, SaaS Application Integration,
  File-Based Data Ingestion, Cloud Storage Integration, ERP Data Integration,
  CRM Data Integration, E-commerce Data Integration, Real-Time Data Ingestion,
  Batch Data Ingestion, Change Data Capture (CDC), Database Replication,
  Webhook Integration, Streaming Data Ingestion, Third-Party Data Integration,
  Legacy System Integration.
"""

import json
import os
from typing import Dict, Optional

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
STORAGE_ACCOUNT = os.environ.get("AZURE_STORAGE_ACCOUNT", "<storage-account>")
BRONZE_CONTAINER = os.environ.get("BRONZE_CONTAINER", "bronze")
SILVER_CONTAINER = os.environ.get("SILVER_CONTAINER", "silver")
RAW_CONTAINER = os.environ.get("RAW_CONTAINER", "raw")
DB_SECRET_SCOPE = os.environ.get("DB_SECRET_SCOPE", "data-eng")
SQL_SERVER_HOST = os.environ.get("SQL_SERVER_HOST", "<server>.database.windows.net")
SQL_DATABASE = os.environ.get("SQL_DATABASE", "sqldb")
EVENTHUBS_CONN_STR_SECRET = os.environ.get("EH_CONN_STR_SECRET", "eventhubs-conn-string")

ADLS = "abfss://{c}@{acct}.dfs.core.windows.net"


def get_spark(app_name: str = "data-source-ingestion") -> SparkSession:
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


def _secret(key: str, default: str = "") -> str:
    """Read a secret from Databricks secrets, falling back to env vars."""
    try:
        return dbutils.secrets.get(scope=DB_SECRET_SCOPE, key=key)  # noqa: F821
    except Exception:
        return os.environ.get(key.upper(), default)


def _adls_path(container: str, *parts: str) -> str:
    return ADLS.format(c=container, acct=STORAGE_ACCOUNT) + "/" + "/".join(parts)


# ---------------------------------------------------------------------------
# Activity functions
# ---------------------------------------------------------------------------
def database_integration(spark: SparkSession, table: str, predicate: Optional[str] = None) -> DataFrame:
    """Activity: Database Integration - bulk-read a table from Azure SQL via JDBC."""
    url = f"jdbc:sqlserver://{SQL_SERVER_HOST};database={SQL_DATABASE}"
    user = _secret("sql-user")
    password = _secret("sql-password")
    reader = (
        spark.read.format("com.microsoft.sqlserver.jdbc.spark")
        .option("url", url)
        .option("dbtable", table)
        .option("user", user)
        .option("password", password)
    )
    if predicate:
        reader = reader.option("queryCustom", f"SELECT * FROM {table} WHERE {predicate}")
    return reader.load()


def api_integration(spark: SparkSession, endpoint: str, target_path: str) -> DataFrame:
    """Activity: API Integration - land paginated REST API JSON into bronze Delta."""
    import urllib.request

    records = []
    url = endpoint
    while url:
        with urllib.request.urlopen(url) as resp:  # noqa: S310
            payload = json.loads(resp.read().decode())
        records.extend(payload.get("data", []))
        url = payload.get("next")
    df = spark.createDataFrame(records)
    (df.write.format("delta").mode("overwrite").save(_adls_path(BRONZE_CONTAINER, target_path)))
    return df


def saas_integration(spark: SparkSession, object_name: str, incremental_col: str, watermark: str) -> DataFrame:
    """Activity: SaaS Application Integration - incremental SaaS object extract (e.g. Salesforce)."""
    endpoint = f"{os.environ.get('SAAS_API_URL', 'https://api.saas.example.com')}/{object_name}"
    df = api_integration(spark, endpoint, f"saas/{object_name}/landing")
    return df.filter(F.col(incremental_col) > watermark)


def file_based_ingestion(spark: SparkSession, pattern: str, schema: str) -> DataFrame:
    """Activity: File-Based Data Ingestion - CSV batch read with explicit schema."""
    return (
        spark.read.format("csv")
        .option("header", "true")
        .schema(schema)
        .load(_adls_path(RAW_CONTAINER, pattern))
    )


def cloud_storage_ingestion(spark: SparkSession, prefix: str) -> DataFrame:
    """Activity: Cloud Storage Integration - Auto Loader stream from ADLS Gen2."""
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "parquet")
        .option("cloudFiles.schemaLocation", _adls_path(BRONZE_CONTAINER, "_schema", prefix))
        .load(_adls_path(RAW_CONTAINER, prefix))
    )


def erp_integration(spark: SparkSession, module: str) -> DataFrame:
    """Activity: ERP Data Integration - extract an ERP module (finance, procurement)."""
    return database_integration(spark, f"erp.{module}")


def crm_integration(spark: SparkSession) -> DataFrame:
    """Activity: CRM Data Integration - extract accounts and opportunities from CRM."""
    accounts = database_integration(spark, "crm.accounts")
    opportunities = database_integration(spark, "crm.opportunities")
    return accounts.join(opportunities, on="account_id", how="left")


def ecommerce_integration(spark: SparkSession, date_str: str) -> DataFrame:
    """Activity: E-commerce Data Integration - daily orders extract from the storefront DB."""
    return database_integration(spark, "ecom.orders", predicate=f"order_date = '{date_str}'")


def realtime_ingestion(spark: SparkSession, eventhub: str) -> DataFrame:
    """Activity: Real-Time Data Ingestion - low-latency stream from Azure Event Hubs."""
    eh_conf = {
        "eventhubs.connectionString": _secret(EVENTHUBS_CONN_STR_SECRET),
        "eventhubs.consumerGroup": os.environ.get("EH_CONSUMER_GROUP", "$Default"),
    }
    raw = spark.readStream.format("eventhubs").options(**eh_conf).option("eventHubName", eventhub).load()
    return raw.withColumn("payload", F.col("body").cast("string"))


def batch_ingestion(spark: SparkSession, table: str, watermark_col: str, last_value: str) -> DataFrame:
    """Activity: Batch Data Ingestion - watermark-based incremental batch read."""
    return database_integration(spark, table, predicate=f"{watermark_col} > '{last_value}'")


def cdc_ingestion(spark: SparkSession, source_table: str, cdc_table: str, last_lsn: str) -> DataFrame:
    """Activity: Change Data Capture (CDC) - read net changes from a CDC feed."""
    changes = database_integration(spark, cdc_table, predicate=f"__$start_lsn > {last_lsn}")
    current = database_integration(spark, source_table)
    return (
        changes.join(current, on="pk_id", how="left")
        .withColumn("cdc_op", F.col("__$operation"))
    )


def database_replication(spark: SparkSession, source_table: str, target_delta: str) -> None:
    """Activity: Database Replication - mirror a source table to a Delta target with MERGE."""
    df = database_integration(spark, source_table)
    df.createOrReplaceTempView("src_repl")
    spark.sql(
        f"""MERGE INTO delta.`{_adls_path(SILVER_CONTAINER, target_delta)}` t
            USING src_repl s ON t.pk_id = s.pk_id
            WHEN MATCHED THEN UPDATE SET *
            WHEN NOT MATCHED THEN INSERT *"""
    )


def webhook_ingestion(spark: SparkSession, topic: str) -> DataFrame:
    """Activity: Webhook Integration - consume webhook events via an Event Hubs bridge."""
    raw = realtime_ingestion(spark, topic)
    return raw.select(
        F.col("payload").cast("string").alias("raw_json"),
        F.col("enqueuedTime").alias("webhook_received_at"),
    ).withColumn("event", F.from_json(F.col("raw_json"), "event_type STRING, id STRING, data STRING"))


def streaming_ingestion(spark: SparkSession, kafka_topic: str) -> DataFrame:
    """Activity: Streaming Data Ingestion - Kafka structured streaming with watermarking."""
    return (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", os.environ.get("KAFKA_BOOTSTRAP", "<broker>:9092"))
        .option("subscribe", kafka_topic)
        .load()
        .withColumn("value_str", F.col("value").cast("string"))
        .withColumn("event_time", F.col("timestamp"))
        .withWatermark("event_time", "10 minutes")
    )


def third_party_integration(spark: SparkSession, vendor: str, endpoint: str, creds: Dict[str, str]) -> DataFrame:
    """Activity: Third-Party Data Integration - vendor feed with API-key auth and landing."""
    auth_url = f"{endpoint}?api_key={creds.get('api_key', _secret(f'{vendor}-api-key'))}"
    df = api_integration(spark, auth_url, f"thirdparty/{vendor}/landing")
    return df.withColumn("vendor", F.lit(vendor)).withColumn("ingested_at", F.current_timestamp())


def legacy_system_integration(spark: SparkSession, file_pattern: str) -> DataFrame:
    """Activity: Legacy System Integration - fixed-width mainframe extracts via file landing."""
    raw = (
        spark.read.format("text")
        .load(_adls_path(RAW_CONTAINER, "legacy", file_pattern))
    )
    return raw.withColumn(
        "record",
        F.struct(
            F.substring("value", 1, 10).alias("acct_no"),
            F.substring("value", 11, 8).alias("tran_date"),
            F.substring("value", 19, 12).cast("decimal(12,2)").alias("amount"),
        ),
    )


# ---------------------------------------------------------------------------
# Driver
# ---------------------------------------------------------------------------
def main() -> None:
    """Representative flow: batch orders + CDC changes + streaming events to bronze Delta."""
    spark = get_spark()
    try:
        batch_date = dbutils.widgets.get("batch_date")  # noqa: F821
    except Exception:
        batch_date = os.environ.get("BATCH_DATE", "2026-10-17")

    orders = batch_ingestion(spark, "ecom.orders", "updated_at", "2026-10-16")
    (orders.write.format("delta").mode("append")
     .save(_adls_path(BRONZE_CONTAINER, "orders")))

    cdc_changes = cdc_ingestion(spark, "crm.accounts", "cdc.crm_accounts_CT", "0x00000000")
    (cdc_changes.write.format("delta").mode("append")
     .save(_adls_path(BRONZE_CONTAINER, "accounts_cdc")))

    events = streaming_ingestion(spark, "clickstream")
    query = (
        events.writeStream.format("delta")
        .outputMode("append")
        .option("checkpointLocation", _adls_path(BRONZE_CONTAINER, "_checkpoints", "clickstream"))
        .start(_adls_path(BRONZE_CONTAINER, "clickstream"))
    )
    print(f"Batch date: {batch_date}; streaming query: {query.id}")
    spark.sql(f"OPTIMIZE delta.`{_adls_path(BRONZE_CONTAINER, 'orders')}`")


if __name__ == "__main__":
    main()
