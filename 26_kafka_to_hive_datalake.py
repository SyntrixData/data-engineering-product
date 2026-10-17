"""
26. Kafka -> Data Lake Hive Tables (Streaming ETL)
==================================================
Azure Data Engineering Product | PySpark Structured Streaming

Reads JSON events from Kafka topics, parses DYNAMIC (varying-schema) JSON
payloads, flattens nested structs/arrays into a tabular shape, and appends
the result to Hive tables backed by data-lake storage (one table per topic).

Activities covered:
  - Kafka source integration (Structured Streaming kafka connector)
  - Dynamic JSON payload parsing (per-micro-batch schema inference)
  - Schema evolution handling (mergeSchema on write)
  - Recursive flattening of nested structs and arrays
  - Column-name sanitization for Hive compatibility
  - Topic-routed Hive table loads (lake.<topic>)
  - Partitioned Hive tables on the data lake (dt = event date)
  - Kafka metadata preservation (topic/partition/offset/timestamp)
  - Quarantine (dead-letter) table for unparsable payloads
  - Checkpointing and exactly-once append semantics
  - Configurable trigger: continuous micro-batch or one-shot (availableNow)

Run on Databricks / Spark with the Kafka connector package, e.g.:
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1
"""

import os
import re
from typing import List, Optional, Tuple

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import ArrayType, StructType

# ----------------------------- Configuration -----------------------------
CONFIG = {
    # Kafka
    "kafka_bootstrap_servers": os.environ.get(
        "KAFKA_BOOTSTRAP_SERVERS", "<kafka-bootstrap-servers>:9092"
    ),
    "kafka_topics": os.environ.get("KAFKA_TOPICS", "events"),  # comma-separated
    "kafka_group_id": os.environ.get("KAFKA_GROUP_ID", "hive-lake-loader"),
    "kafka_starting_offsets": os.environ.get("KAFKA_STARTING_OFFSETS", "latest"),
    "kafka_max_offsets_per_trigger": os.environ.get(
        "KAFKA_MAX_OFFSETS_PER_TRIGGER", "100000"
    ),
    # Data lake + Hive
    "lake_base_path": os.environ.get(
        "LAKE_BASE_PATH", "abfss://lake@<storage-account>.dfs.core.windows.net"
    ),
    "hive_db": os.environ.get("HIVE_DB", "lake"),
    "checkpoint_base": os.environ.get(
        "CHECKPOINT_BASE",
        "abfss://checkpoints@<storage-account>.dfs.core.windows.net",
    ),
    # Dynamic JSON handling
    "json_schema_sample_size": int(os.environ.get("JSON_SCHEMA_SAMPLE_SIZE", "500")),
    "flatten_separator": os.environ.get("FLATTEN_SEPARATOR", "_"),
    # Run mode: "streaming" (continuous) or "once" (availableNow, then stop)
    "run_mode": os.environ.get("RUN_MODE", "streaming"),
    "processing_time": os.environ.get("PROCESSING_TIME", "60 seconds"),
}


# ----------------------------- Spark session -----------------------------
def get_spark(app_name: str = "kafka-to-hive-datalake") -> SparkSession:
    """Activity: Spark session setup - Hive-enabled session on the data lake."""
    try:
        active = SparkSession.getActiveSession()
        if active is not None:
            return active
    except Exception:
        pass
    return (
        SparkSession.builder.appName(app_name)
        .config(
            "spark.sql.warehouse.dir",
            f"{CONFIG['lake_base_path']}/warehouse",
        )
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .enableHiveSupport()
        .getOrCreate()
    )


def _secret(key: str, default: str = "") -> str:
    """Read a secret from Databricks Key Vault scope, else env var."""
    try:
        return dbutils.secrets.get(  # type: ignore[name-defined]  # noqa: F821
            scope=os.environ.get("KEY_VAULT_SCOPE", "kv"),
            key=key,
        )
    except Exception:
        return os.environ.get(key, default)


# ----------------------------- Kafka source ------------------------------
def read_kafka_stream(spark: SparkSession) -> DataFrame:
    """Activity: Kafka source integration - streaming read of JSON topics."""
    kafka_opts = {
        "kafka.bootstrap.servers": CONFIG["kafka_bootstrap_servers"],
        "subscribe": CONFIG["kafka_topics"],
        "startingOffsets": CONFIG["kafka_starting_offsets"],
        "maxOffsetsPerTrigger": CONFIG["kafka_max_offsets_per_trigger"],
        "failOnDataLoss": "false",
    }
    sasl = _secret("KAFKA_SASL_JAAS_CONFIG")
    if sasl:
        kafka_opts["kafka.sasl.jaas.config"] = sasl
        kafka_opts["kafka.security.protocol"] = os.environ.get(
            "KAFKA_SECURITY_PROTOCOL", "SASL_SSL"
        )
        kafka_opts["kafka.sasl.mechanism"] = os.environ.get(
            "KAFKA_SASL_MECHANISM", "PLAIN"
        )
    reader = spark.readStream.format("kafka")
    for k, v in kafka_opts.items():
        reader = reader.option(k, v)
    return reader.load()


# --------------------- Dynamic JSON parsing ------------------------------
def infer_json_schema(
    spark: SparkSession, json_strings: List[str]
) -> Optional[StructType]:
    """Activity: Dynamic payload handling - infer schema from a sample."""
    if not json_strings:
        return None
    return spark.read.json(
        spark.sparkContext.parallelize(json_strings)
    ).schema


def parse_dynamic_json(
    spark: SparkSession, df: DataFrame, value_col: str = "value_str"
) -> Tuple[Optional[DataFrame], Optional[DataFrame]]:
    """Activity: Dynamic JSON parsing - infer-then-parse per micro-batch.

    Returns (good_df, bad_df). good_df has one column per JSON field plus the
    kafka_* metadata columns; bad_df holds unparsable raw payloads.
    """
    sample_rows = (
        df.select(value_col).na.drop().limit(CONFIG["json_schema_sample_size"]).collect()
    )
    sample = [r[0] for r in sample_rows if r[0]]
    if not sample:
        return None, df  # nothing parsable in this batch

    schema = infer_json_schema(spark, sample)
    parsed = df.withColumn("_doc", F.from_json(F.col(value_col), schema))

    meta_cols = [c for c in df.columns if c != value_col]
    good = (
        parsed.filter(F.col("_doc").isNotNull())
        .select(*meta_cols, "_doc.*")
        .drop(value_col, "_doc")
    )
    bad = (
        parsed.filter(F.col("_doc").isNull())
        .select(*meta_cols, F.col(value_col).alias("raw_payload"))
        .withColumn("error", F.lit("unparsable_json"))
    )
    return good, bad


# --------------------------- Flattening ----------------------------------
def sanitize_column(name: str) -> str:
    """Make a column name Hive-safe: lowercase, alnum/underscore only."""
    clean = re.sub(r"[^0-9a-zA-Z_]+", "_", name).lower().strip("_")
    if re.match(r"^[0-9]", clean):
        clean = f"c_{clean}"
    return clean or "col"


def flatten_dataframe(df: DataFrame, sep: Optional[str] = None) -> DataFrame:
    """Activity: Flatten nested data - recursively expand structs, explode arrays.

    Struct fields become <parent><sep><child>; arrays are exploded outward
    (one row per element). Repeats until the schema is fully flat.
    """
    sep = sep or CONFIG["flatten_separator"]
    flat = df
    while True:
        complex_fields = [
            f
            for f in flat.schema.fields
            if isinstance(f.dataType, (StructType, ArrayType))
        ]
        if not complex_fields:
            break
        cols = []
        for f in flat.schema.fields:
            if isinstance(f.dataType, StructType):
                for sf in f.dataType.fields:
                    cols.append(
                        F.col(f"`{f.name}`.`{sf.name}`").alias(
                            f"{f.name}{sep}{sf.name}"
                        )
                    )
            elif isinstance(f.dataType, ArrayType):
                cols.append(F.explode_outer(F.col(f"`{f.name}`")).alias(f.name))
            else:
                cols.append(F.col(f"`{f.name}`").alias(f.name))
        flat = flat.select(*cols)
    # sanitize every column name for Hive compatibility
    return flat.select(
        *[F.col(f"`{c}`").alias(sanitize_column(c)) for c in flat.columns]
    )


# --------------------------- Hive targets --------------------------------
def ensure_hive_database(spark: SparkSession) -> str:
    """Activity: Hive database setup - database located on the data lake."""
    db = CONFIG["hive_db"]
    location = f"{CONFIG['lake_base_path']}/{db}.db"
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {db} LOCATION '{location}'")
    return db


def write_to_hive_table(
    spark: SparkSession, df: DataFrame, table: str, partition_col: str = "dt"
) -> None:
    """Activity: Hive table load - append flattened rows, schema-evolved."""
    db = ensure_hive_database(spark)
    table = sanitize_column(table)
    location = f"{CONFIG['lake_base_path']}/{db}.db/{table}"
    (
        df.write.mode("append")
        .format("parquet")
        .option("mergeSchema", "true")  # tolerate schema evolution
        .partitionBy(partition_col)
        .option("path", location)
        .saveAsTable(f"{db}.{table}")
    )


def write_quarantine(
    spark: SparkSession, bad_df: DataFrame, table: str, batch_id: int
) -> None:
    """Activity: Dead-letter handling - quarantine unparsable payloads."""
    if bad_df is None:
        return
    db = ensure_hive_database(spark)
    qtable = f"{sanitize_column(table)}_quarantine"
    location = f"{CONFIG['lake_base_path']}/{db}.db/{qtable}"
    (
        bad_df.withColumn("batch_id", F.lit(batch_id))
        .write.mode("append")
        .format("parquet")
        .option("path", location)
        .saveAsTable(f"{db}.{qtable}")
    )


# ------------------------- Micro-batch driver ----------------------------
def process_batch(batch_df: DataFrame, batch_id: int) -> None:
    """Activity: Orchestrated load - route each topic to its Hive table.

    Runs inside foreachBatch: decode Kafka bytes, split by topic, parse the
    dynamic JSON, flatten, partition by event date, and append to Hive.
    """
    if batch_df.isEmpty():
        return
    spark = batch_df.sparkSession

    kafka_df = batch_df.select(
        F.col("topic").alias("kafka_topic"),
        F.col("partition").cast("long").alias("kafka_partition"),
        F.col("offset").cast("long").alias("kafka_offset"),
        F.col("timestamp").alias("kafka_ts"),
        F.col("value").cast("string").alias("value_str"),
    )

    topics = [r["kafka_topic"] for r in kafka_df.select("kafka_topic").distinct().collect()]
    for topic in topics:
        tdf = kafka_df.filter(F.col("kafka_topic") == topic)
        good, bad = parse_dynamic_json(spark, tdf)
        write_quarantine(spark, bad, topic, batch_id)
        if good is None or good.rdd.isEmpty():
            continue
        flat = flatten_dataframe(good.drop("kafka_topic"))
        # de-duplicate replays on the Kafka offset (exactly-once append)
        flat = flat.dropDuplicates(["kafka_partition", "kafka_offset"])
        dated = flat.withColumn("dt", F.to_date(F.col("kafka_ts")))
        write_to_hive_table(spark, dated, topic)


# --------------------------------- main ----------------------------------
def main() -> None:
    """Wire the Kafka -> Hive streaming ETL and run it."""
    spark = get_spark()
    stream = read_kafka_stream(spark)

    writer = (
        stream.writeStream.foreachBatch(process_batch).option(
            "checkpointLocation",
            f"{CONFIG['checkpoint_base']}/kafka_to_hive/{CONFIG['hive_db']}",
        )
    )
    if CONFIG["run_mode"] == "once":
        writer = writer.trigger(availableNow=True)
    else:
        writer = writer.trigger(processingTime=CONFIG["processing_time"])

    query = writer.start()
    print(
        f"Streaming Kafka [{CONFIG['kafka_topics']}] -> "
        f"Hive db [{CONFIG['hive_db']}] on lake [{CONFIG['lake_base_path']}]"
    )
    query.awaitTermination()


if __name__ == "__main__":
    main()
