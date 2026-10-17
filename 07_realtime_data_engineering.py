"""Category 7 - Real-Time Data Engineering.

Structured Streaming on Azure Databricks with Azure Event Hubs ingestion,
event-driven pipelines, schema management, monitoring, late/duplicate
handling, and replay/recovery. Activities covered: (1) Real-Time Data
Ingestion, (2) Event Streaming Architecture, (3) Kafka Integration and
Development, (4) Stream Processing Pipeline Development, (5) Event-Driven
Data Integration, (6) Real-Time Data Transformation, (7) Real-Time Database
Synchronization, (8) Streaming Analytics Data Preparation, (9) Event Schema
Management, (10) Stream Monitoring and Alerting, (11) Late-Arriving Event
Handling, (12) Duplicate Event Handling, (13) Replay and Recovery of
Event Streams, (14) Real-Time Data Delivery to Downstream Systems.
"""

import logging
import os
from datetime import datetime

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, StringType, StructField, StructType, TimestampType

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("realtime_data_engineering")

# Config block: env vars with placeholder defaults.
STORAGE_ACCOUNT = os.environ.get("ADLS_STORAGE_ACCOUNT", "<storage-account>")
SILVER = os.environ.get("SILVER_CONTAINER", "silver")
EVENTHUB_NAME = os.environ.get("EVENTHUB_NAME", "events")
CONSUMER_GROUP = os.environ.get("EVENTHUB_CONSUMER_GROUP", "$Default")
CHECKPOINTS = os.environ.get("CHECKPOINT_BASE", "checkpoints")
SECRETS_SCOPE = os.environ.get("SECRETS_SCOPE", "data-eng")
SECRETS_KEY = os.environ.get("SECRETS_KEY", "eventhub-connection-string")

EVENT_SCHEMA = StructType([
    StructField("event_id", StringType(), True),
    StructField("event_ts", TimestampType(), True),
    StructField("customer_id", StringType(), True),
    StructField("event_type", StringType(), True),
    StructField("amount", DoubleType(), True),
    StructField("country", StringType(), True),
])


def adls(path: str, container: str = SILVER) -> str:
    return f"abfss://{container}@{STORAGE_ACCOUNT}.dfs.core.windows.net/{path}"


def get_spark(app_name: str = "realtime-data-engineering") -> SparkSession:
    """Active session on Databricks; local Delta-enabled fallback otherwise."""
    existing = SparkSession.getActiveSession()
    if existing is not None:
        return existing
    return (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .master("local[*]").getOrCreate()
    )


def get_eventhub_connection_string() -> str:
    """Secrets via dbutils.secrets with env fallback; never hardcoded."""
    try:
        from pyspark.dbutils import DBUtils
        return DBUtils(SparkSession.getActiveSession()).secrets.get(
            scope=SECRETS_SCOPE, key=SECRETS_KEY)
    except Exception:
        value = os.environ.get("EVENTHUB_CONNECTION_STRING")
        if not value:
            raise RuntimeError("Set EVENTHUB_CONNECTION_STRING or the Databricks secret")
        return value


def read_job_config() -> dict:
    """dbutils.widgets params; env/defaults when run outside Databricks."""
    try:
        from pyspark.dbutils import DBUtils
        dbu = DBUtils(SparkSession.getActiveSession())
        return {"eventhub": dbu.widgets.get("eventhub_name"),
                "sink": dbu.widgets.get("sink_path"),
                "trigger": dbu.widgets.get("trigger_interval")}
    except Exception:
        return {"eventhub": EVENTHUB_NAME, "sink": adls("streaming/events_silver"),
                "trigger": "1 minute"}


def read_eventhub_stream(spark: SparkSession, eventhub_name: str) -> DataFrame:
    """Activities 1-3: Real-Time Ingestion / Streaming Architecture / Kafka Integration."""
    raw = (spark.readStream.format("eventhubs")
           .option("eventhubs.connectionString", get_eventhub_connection_string())
           .option("eventhubs.consumerGroup", CONSUMER_GROUP)
           .option("eventhubs.name", eventhub_name)
           .option("maxEventsPerTrigger", "50000").load())
    return (raw.withColumn("payload", F.from_json(F.col("body").cast("string"), EVENT_SCHEMA))
            .select("payload.*", "enqueuedTime", "partition", "offset"))


def transform_streaming_events(events: DataFrame) -> DataFrame:
    """Activity 6: Real-Time Data Transformation - normalize + derive in-stream."""
    return (events.withColumn("event_date", F.to_date("event_ts"))
            .withColumn("country", F.upper(F.trim("country")))
            .withColumn("value_band",
                        F.when(F.col("amount") >= 1000, "high")
                         .when(F.col("amount") >= 100, "medium").otherwise("low"))
            .filter(F.col("event_id").isNotNull() & F.col("event_ts").isNotNull()))


def handle_duplicate_events(events: DataFrame) -> DataFrame:
    """Activity 12: Duplicate Event Handling - stateful dedup on natural key."""
    return events.withWatermark("event_ts", "1 hour").dropDuplicates(["event_id", "event_type"])


def validate_event_schema(events: DataFrame) -> DataFrame:
    """Activity 9: Event Schema Management - quarantine rows violating the contract."""
    valid = events.filter(F.col("event_id").isNotNull() & F.col("event_ts").isNotNull()
                          & F.col("event_type").isNotNull())
    (events.subtract(valid).writeStream.format("delta").outputMode("append")
     .option("checkpointLocation", adls(f"{CHECKPOINTS}/quarantine"))
     .trigger(processingTime="5 minutes").start(adls("streaming/quarantine")))
    return valid


def build_streaming_pipeline(spark: SparkSession, cfg: dict):
    """Activity 4: Stream Processing Pipeline Development - Event Hubs to Delta."""
    events = handle_duplicate_events(transform_streaming_events(
        validate_event_schema(read_eventhub_stream(spark, cfg["eventhub"]))))
    query = (events.writeStream.format("delta").outputMode("append")
             .option("checkpointLocation", adls(f"{CHECKPOINTS}/events_silver"))
             .option("mergeSchema", "true")
             .trigger(processingTime=cfg["trigger"]).start(cfg["sink"]))
    logger.info("Streaming query started: %s", query.id)
    return query


def route_events_by_type(events: DataFrame, sinks: dict) -> list:
    """Activity 5: Event-Driven Data Integration - fan-out per event type."""
    queries = []
    for event_type, sink_path in sinks.items():
        q = (events.filter(F.col("event_type") == event_type)
             .writeStream.format("delta").outputMode("append")
             .option("checkpointLocation", adls(f"{CHECKPOINTS}/route_{event_type}"))
             .trigger(processingTime="2 minutes").start(sink_path))
        queries.append(q)
    return queries


def sync_to_serving_table(events: DataFrame, target_table: str):
    """Activity 7: Real-Time Database Synchronization - foreachBatch MERGE upsert."""
    def upsert(micro_batch: DataFrame, batch_id: int) -> None:
        micro_batch.createOrReplaceTempView("updates")
        micro_batch.sparkSession.sql(
            f"MERGE INTO {target_table} t USING updates s ON t.event_id = s.event_id "
            "WHEN MATCHED THEN UPDATE SET * WHEN NOT MATCHED THEN INSERT *")
        logger.info("Synced micro-batch %d into %s", batch_id, target_table)

    return (events.writeStream.foreachBatch(upsert).outputMode("update")
            .option("checkpointLocation", adls(f"{CHECKPOINTS}/sync_serving"))
            .trigger(processingTime="1 minute").start())


def prepare_streaming_aggregates(events: DataFrame, sink_path: str):
    """Activity 8: Streaming Analytics Data Preparation - windowed aggregates."""
    agg = (events.withWatermark("event_ts", "10 minutes")
           .groupBy(F.window("event_ts", "5 minutes"), F.col("country"), F.col("event_type"))
           .agg(F.count("*").alias("event_count"), F.sum("amount").alias("total_amount"),
                F.approx_count_distinct("customer_id").alias("unique_customers"))
           .select(F.col("window.start").alias("window_start"), F.col("window.end").alias("window_end"),
                   "country", "event_type", "event_count", "total_amount", "unique_customers"))
    return (agg.writeStream.format("delta").outputMode("append")
            .option("checkpointLocation", adls(f"{CHECKPOINTS}/streaming_agg"))
            .trigger(processingTime="5 minutes").start(sink_path))


def monitor_stream(query, sla_lag_seconds: int = 300) -> dict:
    """Activity 10: Stream Monitoring and Alerting - SLA check on batch duration."""
    progress = query.lastProgress or {}
    batch_s = (progress.get("durationMs") or {}).get("triggerExecution", 0) / 1000
    status = {"query_id": str(query.id), "is_active": query.isActive,
              "batch_seconds": round(batch_s, 2), "sla_breach": batch_s > sla_lag_seconds,
              "checked_at": datetime.utcnow().isoformat()}
    (logger.error if status["sla_breach"] else logger.info)("Stream status: %s", status)
    return status


def handle_late_events(events: DataFrame, sink_path: str):
    """Activity 11: Late-Arriving Event Handling - route late rows to recovery."""
    late = (events.withWatermark("event_ts", "30 minutes")
            .filter(F.col("event_ts") < F.current_timestamp() - F.expr("INTERVAL 30 MINUTES")))
    return (late.writeStream.format("delta").outputMode("append")
            .option("checkpointLocation", adls(f"{CHECKPOINTS}/late_events"))
            .trigger(processingTime="10 minutes").start(sink_path))


def replay_stream_range(spark: SparkSession, source_table: str, sink_path: str,
                        start_version: int, end_version: int) -> None:
    """Activity 13: Replay and Recovery - Delta time-travel range via change feed."""
    replay = (spark.read.format("delta").option("readChangeFeed", "true")
              .option("startingVersion", start_version).option("endingVersion", end_version)
              .load(source_table))
    replay.write.format("delta").mode("append").option("mergeSchema", "true").save(sink_path)
    logger.info("Replayed versions %d-%d of %s", start_version, end_version, source_table)


def deliver_to_downstream(events: DataFrame, webhook_env: str = "DOWNSTREAM_WEBHOOK_URL"):
    """Activity 14: Real-Time Data Delivery - foreachBatch webhook push."""
    import json
    import urllib.request
    webhook_url = os.environ.get(webhook_env, "")

    def deliver(micro_batch: DataFrame, batch_id: int) -> None:
        rows = micro_batch.limit(1000).toJSON().collect()
        if not rows or not webhook_url:
            logger.warning("Skipping delivery for batch %d", batch_id)
            return
        payload = json.dumps({"batch_id": batch_id,
                              "events": [json.loads(r) for r in rows]})
        req = urllib.request.Request(webhook_url, data=payload.encode("utf-8"),
                                     headers={"Content-Type": "application/json"}, method="POST")
        with urllib.request.urlopen(req, timeout=30) as resp:
            logger.info("Delivered batch %d -> HTTP %d", batch_id, resp.status)

    return (events.writeStream.foreachBatch(deliver).outputMode("append")
            .option("checkpointLocation", adls(f"{CHECKPOINTS}/downstream_delivery"))
            .trigger(processingTime="2 minutes").start())


def main() -> None:
    """End-to-end: ingest -> validate -> transform -> serve + monitor."""
    spark = get_spark()
    cfg = read_job_config()
    query = build_streaming_pipeline(spark, cfg)
    events = validate_event_schema(read_eventhub_stream(spark, cfg["eventhub"]))
    transformed = transform_streaming_events(events)
    route_events_by_type(transformed, {"purchase": adls("streaming/by_type/purchase"),
                                       "refund": adls("streaming/by_type/refund")})
    prepare_streaming_aggregates(events, adls("streaming/agg_5min"))
    sync_to_serving_table(transformed, "main.silver.events_serving")
    monitor_stream(query)
    query.awaitTermination(timeout=600)
    logger.info("Real-time main() completed")


if __name__ == "__main__":
    main()
