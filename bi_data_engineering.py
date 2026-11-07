"""Category 21: Business Intelligence Data Engineering.

Purpose:
    Build and maintain the data assets that power BI tools (Power BI, Tableau,
    Looker) from raw Delta tables on Azure Data Lake Storage Gen2.

Activities covered:
    1.  Power BI Data Integration
    2.  Tableau Data Integration
    3.  Looker Data Integration
    4.  Reporting Data Mart Development
    5.  BI Data Model Development
    6.  Dashboard Data Pipeline Development
    7.  KPI Calculation Pipelines
    8.  Reporting Dataset Optimization
    9.  Automated Report Data Refresh (incl. Power BI dataset refresh via REST)
    10. BI Data Quality Testing
    11. Semantic Model Preparation
    12. Self-Service Analytics Data Preparation
"""

import os
import time
from datetime import datetime
from typing import List, Optional

import requests
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, LongType

try:
    from delta.tables import DeltaTable
except Exception:  # Delta optional in plain CI runs
    DeltaTable = None

CONFIG = {
    "adls_account": os.environ.get("ADLS_ACCOUNT", "<adls-account-name>"),
    "container": os.environ.get("ADLS_CONTAINER", "lakehouse"),
    "keyvault_scope": os.environ.get("KEYVAULT_SCOPE", "kv-data-product"),
    "pbi_workspace_id": os.environ.get("PBI_WORKSPACE_ID", "<pbi-workspace-id>"),
    "tableau_site": os.environ.get("TABLEAU_SITE", "<tableau-site>"),
    "looker_base_url": os.environ.get("LOOKER_BASE_URL", "https://<looker>.cloud.looker.com"),
    "process_date": os.environ.get("PROCESS_DATE", datetime.utcnow().strftime("%Y-%m-%d")),
}

try:
    dbutils.widgets.text("process_date", CONFIG["process_date"])
    dbutils.widgets.text("bi_tool", "powerbi")
    CONFIG["process_date"] = dbutils.widgets.get("process_date")
except Exception:
    pass

def get_spark() -> SparkSession:
    """Return an existing Spark session or build a local one with Delta configs."""
    existing = SparkSession.getActiveSession()
    if existing is not None:
        return existing
    return (
        SparkSession.builder.appName("bi-data-engineering")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .getOrCreate()
    )

spark = get_spark()

BASE = f"abfss://{CONFIG['container']}@{CONFIG['adls_account']}.dfs.core.windows.net"

def _get_secret(key: str, env_fallback: str) -> str:
    """Fetch a secret from Databricks secret scope, falling back to env vars."""
    try:
        return dbutils.secrets.get(scope=CONFIG["keyvault_scope"], key=key)
    except Exception:
        return os.environ.get(env_fallback, "")

def _bronze_path(entity: str) -> str:
    return f"{BASE}/bronze/{entity}"

def _gold_path(entity: str) -> str:
    return f"{BASE}/gold/{entity}"

# 1. Power BI Data Integration
def integrate_power_bi(dataset_name: str, source_table: str) -> DataFrame:
    """Prepare and register a curated DataFrame for Power BI consumption. Writes the source gold table to an optimized, Power BI-friendly Delta layout (partitioned by year/month) and returns the DataFrame."""
    df = spark.table(source_table).withColumn(
        "year_month", F.date_format(F.col("event_date"), "yyyy-MM")
    )
    out = f"{_gold_path('powerbi')}/{dataset_name}"
    (df.write.format("delta").mode("overwrite").partitionBy("year_month").save(out))
    return df

# 2. Tableau Data Integration
def integrate_tableau(source_table: str, extract_name: str) -> DataFrame:
    """Materialize a flat, denormalized extract for Tableau (one wide table)."""
    df = spark.table(source_table)
    out = f"{_gold_path('tableau')}/{extract_name}"
    df.write.format("delta").mode("overwrite").save(out)
    return df

# 3. Looker Data Integration
def integrate_looker(source_table: str, view_name: str) -> DataFrame:
    """Persist a derived table plus a LookML-style field-mapping sidecar."""
    df = spark.table(source_table)
    out = f"{_gold_path('looker')}/{view_name}"
    df.write.format("delta").mode("overwrite").save(out)
    mapping = {f.name: str(f.dataType.simpleString()) for f in df.schema.fields}
    spark.createDataFrame(
        [(k, v) for k, v in mapping.items()], ["field", "type"]
    ).write.format("delta").mode("overwrite").save(f"{out}_fields")
    return df

# 4. Reporting Data Mart Development
def build_reporting_data_mart(fact_table: str, dim_tables: List[str], mart_name: str) -> DataFrame:
    """Join a fact table to its dimensions into a star-shaped reporting mart."""
    df = spark.table(fact_table)
    for dim in dim_tables:
        key = f"{dim}_key"
        df = df.join(spark.table(dim), on=key, how="left")
    out = f"{_gold_path('marts')}/{mart_name}"
    df.write.format("delta").mode("overwrite").save(out)
    spark.sql(f"CREATE TABLE IF NOT EXISTS gold_{mart_name} USING DELTA LOCATION '{out}'")
    return df

# 5. BI Data Model Development
def build_bi_data_model(mart_table: str, model_name: str, measures: List[str]) -> DataFrame:
    """Create a BI model table with explicit measure columns for dashboards."""
    df = spark.table(mart_table)
    for measure in measures:
        df = df.withColumn(f"measure_{measure}", F.col(measure))
    out = f"{_gold_path('models')}/{model_name}"
    df.write.format("delta").mode("overwrite").save(out)
    return df

# 6. Dashboard Data Pipeline Development
def build_dashboard_pipeline(source_tables: List[str], dashboard_table: str) -> DataFrame:
    """Union multiple source extracts into one dashboard-ready dataset."""
    frames = [spark.table(t) for t in source_tables]
    df = frames[0]
    for other in frames[1:]:
        df = df.unionByName(other, allowMissingColumns=True)
    df = df.withColumn("loaded_at", F.current_timestamp())
    df.write.format("delta").mode("overwrite").save(f"{_gold_path('dashboards')}/{dashboard_table}")
    return df

# 7. KPI Calculation Pipelines
KPI_SCHEMA = StructType(
    [StructField("kpi_name", StringType(), False), StructField("kpi_value", LongType(), True),
     StructField("period", StringType(), False)]
)

def run_kpi_pipeline(source_table: str, kpi_table: str) -> DataFrame:
    """Compute core KPIs (orders, revenue, active customers) per period."""
    df = spark.table(source_table)
    kpis = df.groupBy(F.date_format(F.col("event_date"), "yyyy-MM").alias("period")).agg(
        F.count("*").alias("orders"),
        F.sum("amount").alias("revenue"),
        F.countDistinct("customer_id").alias("active_customers"),
    )
    melted = (
        kpis.select("period", F.explode(F.array(
            F.struct(F.lit("total_orders").alias("k"), F.col("orders").cast(LongType()).alias("v")),
            F.struct(F.lit("total_revenue").alias("k"), F.col("revenue").cast(LongType()).alias("v")),
            F.struct(F.lit("active_customers").alias("k"), F.col("active_customers").alias("v")),
        )).alias("kv"))
        .select(F.col("kv.k").alias("kpi_name"), F.col("kv.v").alias("kpi_value"), "period")
    )
    melted.write.format("delta").mode("overwrite").save(f"{_gold_path('kpis')}/{kpi_table}")
    return melted

# 8. Reporting Dataset Optimization
def optimize_reporting_dataset(table_or_path: str, zorder_cols: Optional[List[str]] = None) -> None:
    """OPTIMIZE + (optional) ZORDER a reporting Delta table for scan speed."""
    if zorder_cols:
        spark.sql(f"OPTIMIZE delta.`{table_or_path}` ZORDER BY ({', '.join(zorder_cols)})")
    else:
        spark.sql(f"OPTIMIZE delta.`{table_or_path}`")
    spark.sql(f"VACUUM delta.`{table_or_path}` RETAIN 168 HOURS")

# 9. Automated Report Data Refresh (Power BI REST refresh)
def trigger_power_bi_refresh(dataset_id: str, workspace_id: Optional[str] = None) -> dict:
    """Trigger a Power BI dataset refresh via the Power BI REST API. Uses a service-principal bearer token stored in the Key Vault scope."""
    ws = workspace_id or CONFIG["pbi_workspace_id"]
    token = _get_secret("pbi-access-token", "PBI_ACCESS_TOKEN")
    url = f"https://api.powerbi.com/v1.0/myorg/groups/{ws}/datasets/{dataset_id}/refreshes"
    resp = requests.post(
        url,
        headers={"Authorization": f"Bearer {token}"},
        json={"notifyOption": "MailOnFailure"},
        timeout=60,
    )
    resp.raise_for_status()
    return {"status": "refresh_triggered", "dataset_id": dataset_id}

def get_power_bi_refresh_status(dataset_id: str, workspace_id: Optional[str] = None) -> dict:
    """Poll the latest refresh execution state for a Power BI dataset."""
    ws = workspace_id or CONFIG["pbi_workspace_id"]
    token = _get_secret("pbi-access-token", "PBI_ACCESS_TOKEN")
    url = f"https://api.powerbi.com/v1.0/myorg/groups/{ws}/datasets/{dataset_id}/refreshes?$top=1"
    resp = requests.get(url, headers={"Authorization": f"Bearer {token}"}, timeout=60)
    resp.raise_for_status()
    values = resp.json().get("value", [{}])
    return values[0] if values else {}

def run_automated_refresh_pipeline(
    source_tables: List[str], dataset_id: str, max_wait_min: int = 30
) -> str:
    """Refresh pipeline: rebuild extracts then trigger Power BI refresh."""
    for table in source_tables:
        df = spark.table(table)
        df.write.format("delta").mode("overwrite").save(f"{_gold_path('refresh')}/{table}")
    trigger_power_bi_refresh(dataset_id)
    deadline = time.time() + max_wait_min * 60
    while time.time() < deadline:
        status = get_power_bi_refresh_status(dataset_id).get("status")
        if status in ("Completed", "Failed", "Disabled"):
            return status or "Unknown"
        time.sleep(30)
    return "Timeout"

# 10. BI Data Quality Testing
def test_bi_data_quality(table: str, rules: List[dict]) -> DataFrame:
    """Run row-count, null-check and duplicate rules against a BI table."""
    df = spark.table(table)
    results = []
    for rule in rules:
        name, kind = rule["name"], rule["kind"]
        if kind == "row_count":
            results.append((name, kind, df.count() >= rule.get("min_rows", 1)))
        elif kind == "not_null":
            results.append((name, kind, df.filter(F.col(rule["column"]).isNull()).count() == 0))
        elif kind == "unique":
            results.append((name, kind,
                            df.count() == df.select(rule["column"]).distinct().count()))
    return spark.createDataFrame(results, ["rule_name", "rule_kind", "passed"])

# 11. Semantic Model Preparation
def prepare_semantic_model(source_table: str, model_name: str,
                           relationships: List[dict]) -> DataFrame:
    """Publish a semantic layer: curated table + relationship metadata table."""
    df = spark.table(source_table)
    out = f"{_gold_path('semantic')}/{model_name}"
    df.write.format("delta").mode("overwrite").save(out)
    rel_df = spark.createDataFrame(relationships)
    rel_df.write.format("delta").mode("overwrite").save(f"{out}_relationships")
    return df

# 12. Self-Service Analytics Data Preparation
def prepare_self_service_dataset(source_table: str, dataset_name: str,
                                 allowed_columns: List[str]) -> DataFrame:
    """Publish a governed, column-restricted dataset for self-service users."""
    df = spark.table(source_table).select(*allowed_columns)
    df = df.withColumn("_row_level_owner", F.lit(None).cast(StringType()))
    out = f"{_gold_path('self_service')}/{dataset_name}"
    df.write.format("delta").mode("overwrite").save(out)
    return df

def main() -> None:
    process_date = CONFIG["process_date"]
    df = integrate_power_bi("sales_dashboard", "gold_fact_sales")
    build_reporting_data_mart("gold_fact_sales", ["dim_customer", "dim_date"],
                              f"mart_sales_{process_date}")
    run_kpi_pipeline("gold_fact_sales", f"kpis_{process_date}")
    quality = test_bi_data_quality("gold_fact_sales", [
        {"name": "min_rows", "kind": "row_count", "min_rows": 1},
        {"name": "amount_not_null", "kind": "not_null", "column": "amount"},
        {"name": "order_unique", "kind": "unique", "column": "order_id"},
    ])
    quality.write.format("delta").mode("overwrite").save(f"{_gold_path('qa')}/bi_quality")
    prepare_self_service_dataset("gold_fact_sales", f"selfserve_{process_date}",
                                 ["order_id", "customer_id", "amount", "event_date"])
    optimize_reporting_dataset(f"{_gold_path('marts')}/mart_sales_{process_date}",
                               ["customer_id"])
    print(f"BI pipeline complete for {process_date}, rows={df.count()}")

if __name__ == "__main__":
    main()
