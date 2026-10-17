"""Category 24: Data API Development and Data Delivery.

Purpose:
    Publish curated Delta datasets as versioned data products and deliver them
    to consumers via managed exports, scheduled jobs, webhooks, and monitored
    subscriptions on Azure.

Activities covered:
    1.  REST API Development        9.  Managed File Delivery
    2.  GraphQL Data Access         10. Scheduled Dataset Delivery
    3.  Data Extraction APIs        11. Webhook-Based Data Delivery
    4.  Data Export Services        12. Data Subscription Management
    5.  Secure Dataset Delivery     13. API Usage Monitoring
    6.  Database-to-API Integration 14. Data Access Auditing
    7.  API-Based Data Sync         15. Data Product Publishing
    8.  Data Sharing Between Systems
"""

import hashlib
import json
import os
import uuid
from datetime import datetime
from typing import Dict, List, Optional

import requests
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType, TimestampType

try:
    from delta.tables import DeltaTable
except Exception:
    DeltaTable = None

CONFIG = {
    "adls_account": os.environ.get("ADLS_ACCOUNT", "<adls-account-name>"),
    "container": os.environ.get("ADLS_CONTAINER", "lakehouse"),
    "keyvault_scope": os.environ.get("KEYVAULT_SCOPE", "kv-data-product"),
    "api_gateway_url": os.environ.get("API_GATEWAY_URL", "https://<apim>.azure-api.net"),
    "process_date": os.environ.get("PROCESS_DATE", datetime.utcnow().strftime("%Y-%m-%d")),
}

try:
    dbutils.widgets.text("product_id", "sales_product_v1")
    dbutils.widgets.text("delivery_mode", "scheduled")
except Exception:
    pass

def get_spark() -> SparkSession:
    """Return an existing Spark session or build a local one with Delta configs."""
    existing = SparkSession.getActiveSession()
    if existing is not None:
        return existing
    return (SparkSession.builder.appName("data-api-delivery")
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config("spark.sql.catalog.spark_catalog",
                    "org.apache.spark.sql.delta.catalog.DeltaCatalog").getOrCreate())

spark = get_spark()
BASE = f"abfss://{CONFIG['container']}@{CONFIG['adls_account']}.dfs.core.windows.net"
PRODUCTS = f"{BASE}/products"

USAGE_SCHEMA = StructType([
    StructField("event_id", StringType(), False),
    StructField("event_ts", TimestampType(), False),
    StructField("consumer", StringType(), False),
    StructField("product_id", StringType(), False),
    StructField("channel", StringType(), False),
    StructField("rows_delivered", StringType(), True),
    StructField("status", StringType(), False),
])

def _get_secret(key: str, env_fallback: str) -> str:
    """Fetch a secret from Databricks secret scope, falling back to env vars."""
    try:
        return dbutils.secrets.get(scope=CONFIG["keyvault_scope"], key=key)
    except Exception:
        return os.environ.get(env_fallback, "")

def _product_path(product_id: str, layer: str = "curated") -> str:
    return f"{PRODUCTS}/{product_id}/{layer}"

def _system_path(name: str) -> str:
    return f"{PRODUCTS}/_system/{name}"

def log_delivery_event(consumer: str, product_id: str, channel: str,
                       rows: int, status: str = "success") -> None:
    """Append a delivery/usage event to the lightweight usage tracker table."""
    row = spark.createDataFrame(
        [(str(uuid.uuid4()), datetime.utcnow(), consumer, product_id, channel,
          str(rows), status)], USAGE_SCHEMA)
    row.write.format("delta").mode("append").save(_system_path("usage_log"))

def log_audit_event(actor: str, action: str, product_id: str, detail: str) -> None:
    """Append an access-audit record (who did what, when)."""
    row = spark.createDataFrame(
        [(str(uuid.uuid4()), datetime.utcnow(), actor, action, product_id, detail)],
        ["event_id", "event_ts", "actor", "action", "product_id", "detail"])
    row.write.format("delta").mode("append").save(_system_path("audit_log"))

def build_rest_access_dataset(product_id: str, source_table: str,
                              filters: Optional[Dict[str, str]] = None) -> DataFrame:
    """Curate a REST-servable dataset: filtered, denormalized, cursor-keyed."""
    df = spark.table(source_table)
    for col, val in (filters or {}).items():
        df = df.filter(F.col(col) == val)
    df = df.withColumn("_api_cursor", F.monotonically_increasing_id())
    df.write.format("delta").mode("overwrite").save(_product_path(product_id, "rest"))
    return df

def build_graphql_schema_dataset(product_id: str, entities: List[str]) -> DataFrame:
    """Union entity tables with a __typename column for GraphQL resolvers."""
    frames = [spark.table(e).withColumn("__typename", F.lit(e)) for e in entities]
    df = frames[0]
    for f in frames[1:]:
        df = df.unionByName(f, allowMissingColumns=True)
    df.write.format("delta").mode("overwrite").save(_product_path(product_id, "graphql"))
    return df

def extract_api_snapshot(product_id: str, source_table: str, watermark_col: str,
                         watermark: str) -> DataFrame:
    """Incremental extraction: rows newer than the consumer's watermark."""
    df = spark.table(source_table).filter(F.col(watermark_col) > watermark)
    df.write.format("delta").mode("overwrite").save(_product_path(product_id, "extract"))
    return df

def export_dataset(product_id: str, fmt: str = "parquet") -> str:
    """Export the curated product dataset to a dated consumer drop zone."""
    df = spark.read.format("delta").load(_product_path(product_id))
    out = f"{_product_path(product_id, 'exports')}/{CONFIG['process_date']}"
    df.write.format(fmt).mode("overwrite").save(out)
    log_delivery_event("export_service", product_id, f"file_{fmt}", df.count())
    return out

def deliver_secure_dataset(product_id: str, consumer: str,
                           allowed_columns: List[str]) -> DataFrame:
    """Deliver a column-masked, hashed-PII dataset to one consumer."""
    df = spark.read.format("delta").load(_product_path(product_id))
    for c in df.columns:
        if c not in allowed_columns:
            df = df.drop(c)
    if "email" in df.columns:
        df = df.withColumn("email", F.sha2(F.col("email"), 256))
    out = f"{_product_path(product_id, 'secure')}/{consumer}"
    df.write.format("delta").mode("overwrite").save(out)
    log_audit_event("delivery_engine", "secure_deliver", product_id, f"consumer={consumer}")
    log_delivery_event(consumer, product_id, "secure_dataset", df.count())
    return df

def sync_database_to_api_layer(product_id: str, source_label: str) -> DataFrame:
    """Mirror the curated product into the API serving layer with a sync stamp."""
    df = (spark.read.format("delta").load(_product_path(product_id))
          .withColumn("_db_sync_ts", F.current_timestamp()))
    df.write.format("delta").mode("overwrite").save(_product_path(product_id, "api_mirror"))
    log_audit_event("delivery_engine", "db_to_api_sync", product_id, f"source={source_label}")
    return df

def sync_from_upstream_api(product_id: str, api_path: str,
                           params: Optional[Dict] = None) -> DataFrame:
    """Pull upstream API pages with a Key Vault-backed token and land as Delta."""
    token = _get_secret("apim-subscription-key", "APIM_SUBSCRIPTION_KEY")
    resp = requests.get(f"{CONFIG['api_gateway_url']}{api_path}",
                        headers={"Ocp-Apim-Subscription-Key": token},
                        params=params or {}, timeout=60)
    resp.raise_for_status()
    rows = resp.json().get("value", [])
    df = spark.createDataFrame(rows) if rows else spark.createDataFrame([], "a STRING")
    df.write.format("delta").mode("append").save(_product_path(product_id, "upstream"))
    return df

def share_dataset(product_id: str, target_system: str, share_mode: str = "delta_sharing") -> str:
    """Register a Delta Sharing recipient link (metadata) for cross-system sharing."""
    share_id = str(uuid.uuid4())
    spark.createDataFrame(
        [(share_id, product_id, target_system, share_mode, datetime.utcnow())],
        ["share_id", "product_id", "target_system", "share_mode", "created_at"]) \
        .write.format("delta").mode("append").save(_system_path("shares"))
    log_audit_event("delivery_engine", "share_created", product_id, f"target={target_system}")
    return share_id

def deliver_managed_files(product_id: str, consumer: str, fmt: str = "csv") -> str:
    """Write consumer-scoped files with a checksum sidecar for managed delivery."""
    df = spark.read.format("delta").load(_product_path(product_id))
    out = f"{_product_path(product_id, 'managed')}/{consumer}/{CONFIG['process_date']}"
    df.write.format(fmt).mode("overwrite").option("header", "true").save(out)
    checksum = hashlib.sha256(f"{product_id}{consumer}{df.count()}".encode()).hexdigest()
    spark.createDataFrame([(checksum, datetime.utcnow())], ["checksum", "created_at"]) \
        .write.format("delta").mode("overwrite").save(f"{out}_checksum")
    log_delivery_event(consumer, product_id, "managed_file", df.count())
    return out

def run_scheduled_delivery(product_id: str, consumers: List[str]) -> List[str]:
    """Fan out the daily product snapshot to every subscribed consumer."""
    return [deliver_managed_files(product_id, c) for c in consumers]

def notify_webhook(product_id: str, webhook_url: str, payload: Dict) -> dict:
    """POST a signed webhook notification that a new product version is ready."""
    secret = _get_secret("webhook-signing-secret", "WEBHOOK_SIGNING_SECRET")
    body = json.dumps({"product_id": product_id, **payload})
    signature = hashlib.sha256((secret + body).encode()).hexdigest()
    resp = requests.post(webhook_url, data=body,
                         headers={"Content-Type": "application/json",
                                  "X-Signature": signature}, timeout=30)
    resp.raise_for_status()
    log_delivery_event("webhook", product_id, "webhook", 0)
    return {"status": resp.status_code}

def manage_subscription(consumer: str, product_id: str, action: str = "subscribe") -> None:
    """Add or cancel a consumer subscription in the subscription registry."""
    reg = _system_path("subscriptions")
    if action == "subscribe":
        spark.createDataFrame([(consumer, product_id, datetime.utcnow(), "active")],
                              ["consumer", "product_id", "subscribed_at", "status"]) \
            .write.format("delta").mode("append").save(reg)
    elif DeltaTable is not None:
        (DeltaTable.forPath(spark, reg).alias("t")
         .merge(spark.createDataFrame([(consumer, product_id)],
                                      ["consumer", "product_id"]).alias("s"),
                "t.consumer = s.consumer AND t.product_id = s.product_id")
         .whenMatchedUpdate(set={"status": F.lit("cancelled")}).execute())
    log_audit_event(consumer, f"subscription_{action}", product_id, "")

def list_subscribers(product_id: str) -> DataFrame:
    """Return active subscribers for a product."""
    return (spark.read.format("delta").load(_system_path("subscriptions"))
            .filter((F.col("product_id") == product_id) & (F.col("status") == "active")))

def monitor_api_usage(product_id: Optional[str] = None) -> DataFrame:
    """Aggregate delivery events: volume and errors per consumer/product/channel."""
    df = spark.read.format("delta").load(_system_path("usage_log"))
    if product_id:
        df = df.filter(F.col("product_id") == product_id)
    return df.groupBy("consumer", "product_id", "channel", "status").agg(
        F.count("*").alias("events"), F.sum("rows_delivered").alias("total_rows"))

def audit_data_access(product_id: Optional[str] = None,
                      since: Optional[str] = None) -> DataFrame:
    """Query the audit log for access events, optionally filtered."""
    df = spark.read.format("delta").load(_system_path("audit_log"))
    if product_id:
        df = df.filter(F.col("product_id") == product_id)
    if since:
        df = df.filter(F.col("event_ts") >= since)
    return df.orderBy(F.col("event_ts").desc())

def publish_data_product(product_id: str, source_table: str, version: str,
                         description: str, owner: str) -> str:
    """Publish a versioned data product: curated Delta + manifest + contract."""
    df = spark.table(source_table).withColumn("_product_version", F.lit(version))
    curated = _product_path(product_id)
    df.write.format("delta").mode("overwrite").save(curated)
    manifest = {
        "product_id": product_id, "version": version, "description": description,
        "owner": owner, "published_at": datetime.utcnow().isoformat(),
        "schema": [{"name": f.name, "type": f.dataType.simpleString()} for f in df.schema.fields],
        "row_count": df.count(), "location": curated,
    }
    spark.createDataFrame([(product_id, version, json.dumps(manifest))],
                          ["product_id", "version", "manifest"]) \
        .write.format("delta").mode("append").save(_system_path("manifests"))
    log_audit_event(owner, "product_published", product_id, f"version={version}")
    return curated

def main() -> None:
    product_id = "sales_product_v1"
    publish_data_product(product_id, "gold_fact_sales", "1.0.0",
                         "Curated sales dataset for external consumers", "data-engineering")
    build_rest_access_dataset(product_id, "gold_fact_sales", {"region": "South"})
    export_dataset(product_id, "parquet")
    manage_subscription("acme_corp", product_id, "subscribe")
    manage_subscription("globex", product_id, "subscribe")
    subscribers = [r["consumer"] for r in list_subscribers(product_id).collect()]
    run_scheduled_delivery(product_id, subscribers)
    deliver_secure_dataset(product_id, "acme_corp", ["order_id", "amount", "event_date"])
    monitor_api_usage(product_id).show()
    print(f"Data product {product_id} published and delivered to {subscribers}")

if __name__ == "__main__":
    main()
