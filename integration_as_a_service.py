"""Category 13: Data Integration as a Service.

Connector-style integration layer for Azure Databricks + ADLS Gen2 + Delta:
prebuilt DB/SaaS connectors, custom API connectors, ERP-to-CRM,
CRM-to-warehouse, e-commerce-to-warehouse, payment/marketing/HR/finance
integrations, cross-platform sync, master sync, mapping, monitoring,
maintenance, and error management.
"""

import json
import os
from typing import Callable, Dict, List, Optional

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F


# ----------------------------------------------------------------------------
# Config block (env vars with placeholder defaults)
# ----------------------------------------------------------------------------
CONFIG = {
    "ADLS_BASE": os.environ.get("ADLS_BASE", "abfss://integrate@adlsgen2.dfs.core.windows.net/raw"),
    "DELTA_DB": os.environ.get("DELTA_DB", "integrations"),
    "CHECKPOINT_BASE": os.environ.get("CHECKPOINT_BASE", "abfss://checkpoints@adlsgen2.dfs.core.windows.net/integrations"),
    "API_BASE_URL": os.environ.get("API_BASE_URL", "https://api.example.com/v1"),
}

try:
    _widgets = dbutils.widgets  # type: ignore[name-defined]
    _widgets.text("connector", "crm_to_warehouse")
    _widgets.text("run_date", "")
except Exception:
    pass


def get_spark(app_name: str = "integration-as-a-service") -> SparkSession:
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


def _api_token(service: str) -> str:
    """API token via Databricks secrets, else env var (never hardcoded)."""
    try:
        return dbutils.secrets.get(scope="integrations-scope", key=f"{service}-token")  # type: ignore[name-defined]
    except Exception:
        return os.environ.get(f"{service.upper()}_TOKEN", "")


def _jdbc_opts() -> Dict[str, str]:
    try:
        user = dbutils.secrets.get(scope="integrations-scope", key="sql-user")  # type: ignore[name-defined]
        pwd = dbutils.secrets.get(scope="integrations-scope", key="sql-password")
    except Exception:
        user = os.environ.get("SQL_USER", "")
        pwd = os.environ.get("SQL_PASSWORD", "")
    return {"user": user, "password": pwd, "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver"}


# Connector registry: name -> builder function.
CONNECTOR_REGISTRY: Dict[str, Callable] = {}


def connector(name: str) -> Callable:
    """Decorator registering a named connector in the registry."""
    def wrap(fn: Callable) -> Callable:
        CONNECTOR_REGISTRY[name] = fn
        return fn
    return wrap


@connector("database")
def prebuilt_database_connector(spark: SparkSession, jdbc_url: str, table: str) -> DataFrame:
    """Prebuilt Database Connectors: JDBC extract from Azure SQL/Synapse."""
    reader = spark.read.format("jdbc").option("url", jdbc_url).option("dbtable", table)
    for k, v in _jdbc_opts().items():
        reader = reader.option(k, v)
    return reader.load()


@connector("saas")
def prebuilt_saas_connector(spark: SparkSession, service: str, entity: str) -> DataFrame:
    """Prebuilt SaaS Connectors: load a SaaS entity staged as JSON in ADLS."""
    token = _api_token(service)  # resolved for downstream calls; staged extract path below
    path = f"{CONFIG['ADLS_BASE']}/{service}/{entity}"
    return spark.read.option("multiline", "true").json(path)


@connector("custom_api")
def custom_api_connector(spark: SparkSession, service: str, endpoint: str) -> DataFrame:
    """Custom API Connectors: request spec materialized as a staged JSON payload."""
    payload_path = f"{CONFIG['ADLS_BASE']}/api/{service}/{endpoint.replace('/', '_')}.json"
    df = spark.read.option("multiline", "true").json(payload_path)
    return df.withColumn("_service", F.lit(service)).withColumn("_endpoint", F.lit(endpoint))


def integrate_erp_to_crm(spark: SparkSession, erp_table: str, crm_table: str) -> None:
    """ERP-to-CRM Integration: upsert ERP accounts into the CRM Delta table."""
    erp = spark.table(erp_table)
    erp.createOrReplaceTempView("src")
    spark.sql(f"""
        MERGE INTO {crm_table} t
        USING src s ON t.account_id = s.account_id
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """)


def integrate_crm_to_warehouse(spark: SparkSession, crm_path: str, warehouse_table: str) -> None:
    """CRM-to-Data-Warehouse Integration: land CRM exports into Delta."""
    df = spark.read.option("header", "true").option("inferSchema", "true").csv(crm_path)
    df.write.format("delta").mode("append").option("mergeSchema", "true").saveAsTable(warehouse_table)


def integrate_ecommerce_to_warehouse(spark: SparkSession, orders_path: str, warehouse_table: str) -> None:
    """E-commerce-to-Warehouse Integration: order events into Delta with dedupe."""
    df = (spark.read.option("multiline", "true").json(orders_path)
          .dropDuplicates(["order_id"]))
    df.write.format("delta").mode("append").saveAsTable(warehouse_table)


def integrate_payment_system(spark: SparkSession, payments_path: str, ledger_table: str) -> None:
    """Payment System Integration: append payment records to the ledger table."""
    spark.read.format("parquet").load(payments_path).write.format("delta").mode("append").saveAsTable(ledger_table)


def integrate_marketing_platform(spark: SparkSession, campaigns_path: str, marts_table: str) -> None:
    """Marketing Platform Integration: campaign metrics into the marketing mart."""
    spark.read.format("parquet").load(campaigns_path).write.format("delta").mode("append").saveAsTable(marts_table)


def integrate_hr_system(spark: SparkSession, employees_path: str, hr_table: str) -> None:
    """HR System Integration: merge employee snapshots by employee_id."""
    src = spark.read.format("parquet").load(employees_path)
    src.createOrReplaceTempView("hr_src")
    spark.sql(f"""
        MERGE INTO {hr_table} t USING hr_src s ON t.employee_id = s.employee_id
        WHEN MATCHED THEN UPDATE SET * WHEN NOT MATCHED THEN INSERT *
    """)


def integrate_financial_system(spark: SparkSession, gl_path: str, finance_table: str) -> None:
    """Financial System Integration: general-ledger rows into the finance Delta table."""
    df = spark.read.format("parquet").load(gl_path).withColumn("_ingested_at", F.current_timestamp())
    df.write.format("delta").mode("append").saveAsTable(finance_table)


def sync_cross_platform(spark: SparkSession, source_table: str, target_table: str, key: str) -> None:
    """Cross-Platform Synchronization: Delta MERGE keyed sync between two tables."""
    spark.table(source_table).createOrReplaceTempView("xsrc")
    spark.sql(f"""
        MERGE INTO {target_table} t USING xsrc s ON t.{key} = s.{key}
        WHEN MATCHED THEN UPDATE SET * WHEN NOT MATCHED THEN INSERT *
    """)


def sync_master_data(spark: SparkSession, master_table: str, consumer_tables: List[str]) -> None:
    """Master Data Synchronization: broadcast master records to consumer tables."""
    master = spark.table(master_table)
    for consumer in consumer_tables:
        master.createOrReplaceTempView("msrc")
        spark.sql(f"""
            MERGE INTO {consumer} t USING msrc s ON t.id = s.id
            WHEN MATCHED THEN UPDATE SET * WHEN NOT MATCHED THEN INSERT *
        """)


def map_and_transform(df: DataFrame, mapping: Dict[str, str]) -> DataFrame:
    """Data Mapping and Transformation: rename + cast per a mapping spec."""
    for src_col, spec in mapping.items():
        new_name, _, dtype = spec.partition(":")
        df = df.withColumnRenamed(src_col, new_name or src_col)
        if dtype:
            df = df.withColumn(new_name or src_col, F.col(new_name or src_col).cast(dtype))
    return df


def monitor_api_integration(spark: SparkSession, log_table: str) -> DataFrame:
    """API Integration Monitoring: error-rate summary over the integration log."""
    return spark.sql(f"""
        SELECT connector, COUNT(*) calls, SUM(CASE WHEN status >= 400 THEN 1 ELSE 0 END) errors
        FROM {log_table} GROUP BY connector
    """)


def maintain_connectors(spark: SparkSession) -> DataFrame:
    """Connector Maintenance and Upgrades: inventory of registered connectors."""
    return spark.createDataFrame(
        [{"connector": name, "status": "active"} for name in sorted(CONNECTOR_REGISTRY)]
    )


def manage_integration_errors(spark: SparkSession, log_table: str, quarantine_table: str) -> None:
    """Integration Error Management: quarantine failed rows from the error log."""
    failed = spark.sql(f"SELECT * FROM {log_table} WHERE status >= 400")
    failed.write.format("delta").mode("append").saveAsTable(quarantine_table)


def main() -> None:
    """Representative flow: CRM to warehouse, e-commerce to warehouse, sync, monitor."""
    spark = get_spark()
    integrate_crm_to_warehouse(spark, f"{CONFIG['ADLS_BASE']}/crm/accounts", "integrations.crm_accounts")
    integrate_ecommerce_to_warehouse(spark, f"{CONFIG['ADLS_BASE']}/shopify/orders", "integrations.orders")
    sync_cross_platform(spark, "integrations.orders", "integrations.orders_replica", "order_id")
    print(monitor_api_integration(spark, "integrations.api_log").collect())
    maintain_connectors(spark).show()


if __name__ == "__main__":
    main()
