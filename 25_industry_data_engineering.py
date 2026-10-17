"""Category 25: Industry-Specific Data Engineering.

Purpose:
    One parameterized module that materializes bronze/silver/gold pipelines
    for 12 industry verticals, driven by a registry mapping each vertical to
    its source entities, keys, measures, and quality rules.

Verticals covered:
    retail, banking, healthcare, manufacturing, logistics, telecom,
    insurance, real_estate, marketing, hr, energy, saas
"""

import os
from datetime import datetime
from typing import Dict, List

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

try:
    from delta.tables import DeltaTable
except Exception:
    DeltaTable = None

# ----------------------------------------------------------------------------
# Config block (env vars with placeholder defaults)
# ----------------------------------------------------------------------------
CONFIG = {
    "adls_account": os.environ.get("ADLS_ACCOUNT", "<adls-account-name>"),
    "container": os.environ.get("ADLS_CONTAINER", "lakehouse"),
    "keyvault_scope": os.environ.get("KEYVAULT_SCOPE", "kv-data-product"),
    "process_date": os.environ.get("PROCESS_DATE", datetime.utcnow().strftime("%Y-%m-%d")),
}

# ----------------------------------------------------------------------------
# Widgets (guarded for non-Databricks contexts)
# ----------------------------------------------------------------------------
try:
    dbutils.widgets.dropdown("vertical", "retail",
                             ["retail", "banking", "healthcare", "manufacturing",
                              "logistics", "telecom", "insurance", "real_estate",
                              "marketing", "hr", "energy", "saas"])
    dbutils.widgets.text("process_date", CONFIG["process_date"])
    CONFIG["process_date"] = dbutils.widgets.get("process_date")
    CONFIG["vertical"] = dbutils.widgets.get("vertical")
except Exception:
    CONFIG["vertical"] = os.environ.get("VERTICAL", "retail")


def get_spark() -> SparkSession:
    """Return an existing Spark session or build a local one with Delta configs."""
    existing = SparkSession.getActiveSession()
    if existing is not None:
        return existing
    return (
        SparkSession.builder.appName("industry-data-engineering")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .getOrCreate()
    )


spark = get_spark()

BASE = f"abfss://{CONFIG['container']}@{CONFIG['adls_account']}.dfs.core.windows.net"


# ----------------------------------------------------------------------------
# Vertical registry: entities, business keys, measures, quality rules
# ----------------------------------------------------------------------------
VERTICAL_REGISTRY: Dict[str, Dict] = {
    "retail": {
        "entities": ["sales", "inventory", "customers", "orders"],
        "key": "order_id", "measures": ["amount", "quantity"],
        "kpi": "total_revenue",
    },
    "banking": {
        "entities": ["transactions", "reconciliation", "risk_positions"],
        "key": "transaction_id", "measures": ["amount", "risk_score"],
        "kpi": "total_volume",
    },
    "healthcare": {
        "entities": ["encounters", "claims", "patients"],
        "key": "encounter_id", "measures": ["claim_amount"],
        "kpi": "governed_row_count",
    },
    "manufacturing": {
        "entities": ["iot_telemetry", "production_runs", "equipment_status"],
        "key": "telemetry_id", "measures": ["temperature", "vibration", "output_units"],
        "kpi": "oee_proxy",
    },
    "logistics": {
        "entities": ["shipments", "fleet_pings", "deliveries"],
        "key": "shipment_id", "measures": ["distance_km", "weight_kg"],
        "kpi": "on_time_rate",
    },
    "telecom": {
        "entities": ["network_events", "usage_records", "cdr_stream"],
        "key": "event_id", "measures": ["bytes", "duration_sec"],
        "kpi": "total_gb",
    },
    "insurance": {
        "entities": ["claims", "policies", "reconciliation"],
        "key": "claim_id", "measures": ["claim_amount", "reserve"],
        "kpi": "loss_ratio",
    },
    "real_estate": {
        "entities": ["listings", "price_history", "market_stats"],
        "key": "listing_id", "measures": ["list_price", "sqft"],
        "kpi": "median_price",
    },
    "marketing": {
        "entities": ["campaigns", "customer_profiles", "attribution"],
        "key": "campaign_id", "measures": ["spend", "conversions", "revenue"],
        "kpi": "roas",
    },
    "hr": {
        "entities": ["employees", "headcount_snapshots", "attrition_events"],
        "key": "employee_id", "measures": ["salary"],
        "kpi": "attrition_rate",
    },
    "energy": {
        "entities": ["sensor_readings", "consumption", "grid_events"],
        "key": "reading_id", "measures": ["kwh", "voltage"],
        "kpi": "total_kwh",
    },
    "saas": {
        "entities": ["product_events", "usage_daily", "tenants"],
        "key": "event_id", "measures": ["events_count"],
        "kpi": "dau",
    },
}


def _layer_path(vertical: str, layer: str, entity: str) -> str:
    return f"{BASE}/industry/{vertical}/{layer}/{entity}"


def get_vertical_config(vertical: str) -> Dict:
    """Return the registry entry for a vertical (raises on unknown vertical)."""
    if vertical not in VERTICAL_REGISTRY:
        raise ValueError(f"Unknown vertical '{vertical}'. Known: {sorted(VERTICAL_REGISTRY)}")
    return VERTICAL_REGISTRY[vertical]


# ----------------------------------------------------------------------------
# Generic pipeline stages
# ----------------------------------------------------------------------------
def ingest_entity_bronze(vertical: str, entity: str, source_table: str) -> DataFrame:
    """Bronze: land the raw entity with ingestion metadata."""
    df = (spark.table(source_table)
          .withColumn("_ingested_at", F.current_timestamp())
          .withColumn("_source", F.lit(source_table)))
    df.write.format("delta").mode("append").save(_layer_path(vertical, "bronze", entity))
    return df


def build_entity_silver(vertical: str, entity: str, cfg: Dict) -> DataFrame:
    """Silver: dedupe on business key, drop null keys, standardize types."""
    df = spark.read.format("delta").load(_layer_path(vertical, "bronze", entity))
    key = cfg["key"]
    df = df.filter(F.col(key).isNotNull()).dropDuplicates([key])
    for m in cfg["measures"]:
        if m in df.columns:
            df = df.withColumn(m, F.col(m).cast("double"))
    df = df.withColumn("_silver_at", F.current_timestamp())
    df.write.format("delta").mode("overwrite").save(_layer_path(vertical, "silver", entity))
    return df


def build_entity_gold(vertical: str, entity: str, cfg: Dict) -> DataFrame:
    """Gold: aggregate measures by day into an analytics-ready summary."""
    df = spark.read.format("delta").load(_layer_path(vertical, "silver", entity))
    ts_col = next((c for c in ("event_date", "event_ts", "_ingested_at") if c in df.columns),
                  "_ingested_at")
    agg_exprs = [F.sum(m).alias(f"sum_{m}") for m in cfg["measures"] if m in df.columns]
    agg_exprs.append(F.count("*").alias("row_count"))
    gold = df.groupBy(F.to_date(F.col(ts_col)).alias("biz_date")).agg(*agg_exprs)
    gold = gold.withColumn("kpi_name", F.lit(cfg["kpi"]))
    gold.write.format("delta").mode("overwrite").save(_layer_path(vertical, "gold", entity))
    return gold


def check_entity_quality(vertical: str, entity: str, cfg: Dict) -> DataFrame:
    """Run completeness + duplicate checks on the silver entity."""
    df = spark.read.format("delta").load(_layer_path(vertical, "silver", entity))
    rows = df.count()
    dupes = rows - df.select(cfg["key"]).distinct().count()
    results = [
        ("completeness", cfg["key"], rows > 0, rows),
        ("duplicate_keys", cfg["key"], dupes == 0, dupes),
    ]
    return spark.createDataFrame(results, ["check", "column", "passed", "value"])


def build_vertical_pipeline(vertical: str, source_tables: Dict[str, str]) -> Dict[str, DataFrame]:
    """Generic pipeline builder: bronze -> silver -> gold + quality per entity."""
    cfg = get_vertical_config(vertical)
    outputs: Dict[str, DataFrame] = {}
    for entity in cfg["entities"]:
        if entity not in source_tables:
            continue
        ingest_entity_bronze(vertical, entity, source_tables[entity])
        silver = build_entity_silver(vertical, entity, cfg)
        gold = build_entity_gold(vertical, entity, cfg)
        qa = check_entity_quality(vertical, entity, cfg)
        qa.write.format("delta").mode("overwrite").save(
            _layer_path(vertical, "qa", f"{entity}_{CONFIG['process_date']}"))
        outputs[entity] = gold
        failed = qa.filter(~F.col("passed")).count()
        if failed:
            raise RuntimeError(f"Quality checks failed for {vertical}.{entity}")
    return outputs


def build_vertical_kpi_mart(vertical: str) -> DataFrame:
    """Union all entity gold tables of a vertical into one KPI mart."""
    cfg = get_vertical_config(vertical)
    frames = [spark.read.format("delta").load(_layer_path(vertical, "gold", e))
              .withColumn("entity", F.lit(e)) for e in cfg["entities"]]
    mart = frames[0]
    for f in frames[1:]:
        mart = mart.unionByName(f, allowMissingColumns=True)
    out = f"{BASE}/industry/{vertical}/gold/_kpi_mart"
    mart.write.format("delta").mode("overwrite").save(out)
    return mart


def list_verticals() -> List[str]:
    """Return all registered vertical names."""
    return sorted(VERTICAL_REGISTRY.keys())


# ----------------------------------------------------------------------------
# Driver
# ----------------------------------------------------------------------------
def main() -> None:
    vertical = CONFIG.get("vertical", "retail")
    cfg = get_vertical_config(vertical)
    # Convention: source tables named bronze_<entity> (override via mapping as needed)
    source_tables = {entity: f"bronze_{entity}" for entity in cfg["entities"]}
    outputs = build_vertical_pipeline(vertical, source_tables)
    mart = build_vertical_kpi_mart(vertical)
    print(f"Vertical '{vertical}' complete: entities={sorted(outputs)} "
          f"kpi_mart_rows={mart.count()}")


if __name__ == "__main__":
    main()
