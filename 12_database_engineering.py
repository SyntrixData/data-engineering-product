"""Category 12: Database Engineering and Management.

PySpark + Spark SQL toolkit targeting Azure SQL / Synapse and Delta Lake:
schema development, SQL/stored-procedure-style logic, views, query and index
optimization, partitioning, replication, backup/recovery, upgrades, connection
management, capacity planning, access control, and maintenance automation.
"""

import os
from typing import Dict, List, Optional

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F


# ----------------------------------------------------------------------------
# Config block (env vars with placeholder defaults)
# ----------------------------------------------------------------------------
CONFIG = {
    "JDBC_URL": os.environ.get("JDBC_URL", "jdbc:sqlserver://sqlserver.database.windows.net:1433;database=appdb"),
    "DELTA_DB": os.environ.get("DELTA_DB", "app"),
    "ADLS_BASE": os.environ.get("ADLS_BASE", "abfss://data@adlsgen2.dfs.core.windows.net/db"),
}

try:
    _widgets = dbutils.widgets  # type: ignore[name-defined]
    _widgets.text("jdbc_url", CONFIG["JDBC_URL"])
    _widgets.text("delta_db", CONFIG["DELTA_DB"])
except Exception:
    pass


def get_spark(app_name: str = "database-engineering") -> SparkSession:
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


def _jdbc_opts() -> Dict[str, str]:
    """JDBC auth options via Databricks secrets, else env vars (never hardcoded)."""
    try:
        user = dbutils.secrets.get(scope="db-scope", key="sql-user")  # type: ignore[name-defined]
        pwd = dbutils.secrets.get(scope="db-scope", key="sql-password")
    except Exception:
        user = os.environ.get("SQL_USER", "")
        pwd = os.environ.get("SQL_PASSWORD", "")
    return {"user": user, "password": pwd,
            "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver"}


def _jdbc_read(spark: SparkSession, query: str) -> DataFrame:
    """Read from Azure SQL / Synapse via JDBC."""
    reader = spark.read.format("jdbc").option("url", CONFIG["JDBC_URL"]).option("query", query)
    for k, v in _jdbc_opts().items():
        reader = reader.option(k, v)
    return reader.load()


def _jdbc_write(df: DataFrame, table: str, mode: str = "append") -> None:
    """Write to Azure SQL / Synapse via JDBC."""
    writer = df.write.format("jdbc").option("url", CONFIG["JDBC_URL"]).option("dbtable", table).mode(mode)
    for k, v in _jdbc_opts().items():
        writer = writer.option(k, v)
    writer.save()


# ----------------------------------------------------------------------------
# Activity implementations
# ----------------------------------------------------------------------------

def design_database_schema(spark: SparkSession, ddl: str) -> None:
    """Database Design and Implementation: apply a DDL script to create schemas."""
    for stmt in [s.strip() for s in ddl.split(";") if s.strip()]:
        spark.sql(stmt)


def develop_database_schema(spark: SparkSession, table: str, columns: Dict[str, str]) -> None:
    """Database Schema Development: create a Delta table from a column/type map."""
    cols = ", ".join(f"{c} {t}" for c, t in columns.items())
    spark.sql(f"CREATE TABLE IF NOT EXISTS {table} ({cols}) USING DELTA")


def develop_sql(spark: SparkSession, sql: str) -> DataFrame:
    """SQL Development: execute an ad-hoc Spark SQL statement and return results."""
    return spark.sql(sql)


def develop_stored_procedure(spark: SparkSession, name: str, body_sql: str) -> None:
    """Stored Procedure Development: register a SQL routine (Spark SQL function/view)."""
    spark.sql(f"CREATE OR REPLACE TEMP VIEW proc_{name} AS {body_sql}")


def develop_view(spark: SparkSession, name: str, query: str, materialized: bool = False) -> None:
    """View and Materialized View Development: standard or Delta materialized view."""
    if materialized:
        spark.sql(f"CREATE OR REPLACE MATERIALIZED VIEW {name} AS {query}")
    else:
        spark.sql(f"CREATE OR REPLACE VIEW {name} AS {query}")


def optimize_query(spark: SparkSession, sql: str) -> DataFrame:
    """Query Optimization: force broadcast for small dims and cache the plan."""
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "10485760")
    return spark.sql(sql).cache()


def optimize_index(spark: SparkSession, delta_table: str, zorder_cols: List[str]) -> None:
    """Index Optimization: OPTIMIZE with ZORDER as the Delta equivalent of indexes."""
    cols = ", ".join(zorder_cols)
    spark.sql(f"OPTIMIZE {delta_table} ZORDER BY ({cols})")


def partition_database_table(spark: SparkSession, delta_table: str, part_col: str) -> None:
    """Database Partitioning: rewrite the table partitioned by the given column."""
    df = spark.table(delta_table)
    (df.write.format("delta").mode("overwrite")
     .option("overwriteSchema", "true").partitionBy(part_col).saveAsTable(delta_table))


def monitor_database_performance(spark: SparkSession) -> DataFrame:
    """Database Performance Monitoring: recent query/operation history from Delta log."""
    return spark.sql("SELECT operation, operationParameters, timestamp FROM (DESCRIBE HISTORY app.audit_log) LIMIT 50")


def configure_replication(spark: SparkSession, source: str, replica_path: str) -> None:
    """Database Replication Configuration: continuous Delta-to-Delta replica copy."""
    spark.readStream.format("delta").table(source).writeStream.format("delta") \
        .option("checkpointLocation", f"{CONFIG['ADLS_BASE']}/_chk/replica").start(replica_path)


def plan_backup_and_recovery(spark: SparkSession, delta_table: str, backup_path: str) -> None:
    """Database Backup and Recovery Planning: deep-clone the Delta table to ADLS."""
    spark.sql(f"CREATE OR REPLACE TABLE delta.`{backup_path}` DEEP CLONE {delta_table}")


def upgrade_database_version(spark: SparkSession, delta_table: str) -> None:
    """Database Version Upgrades: set reader/writer protocol versions for new features."""
    spark.sql(f"ALTER TABLE {delta_table} SET TBLPROPERTIES ('delta.minReaderVersion'='2','delta.minWriterVersion'='5')")


def manage_connections(spark: SparkSession, max_conns: int = 50) -> None:
    """Database Connection Management: cap concurrent JDBC reads."""
    spark.conf.set("spark.sql.shuffle.partitions", "200")
    spark.conf.set("spark.jdbc.maxConnections", str(max_conns))


def plan_capacity(spark: SparkSession, delta_table: str) -> Dict[str, int]:
    """Database Capacity Planning: table size and file counts from DESCRIBE DETAIL."""
    detail = spark.sql(f"DESCRIBE DETAIL {delta_table}").collect()[0]
    return {"size_in_bytes": detail["sizeInBytes"], "num_files": detail["numFiles"]}


def troubleshoot_database(spark: SparkSession, delta_table: str) -> DataFrame:
    """Database Troubleshooting: surface recent failed/skipped operations."""
    return spark.sql(f"SELECT * FROM (DESCRIBE HISTORY {delta_table}) WHERE operation LIKE '%FAILED%' LIMIT 20")


def control_database_access(spark: SparkSession, principal: str, table: str, privilege: str = "SELECT") -> None:
    """Database Access Control: grant privileges to a principal (Unity Catalog)."""
    spark.sql(f"GRANT {privilege} ON TABLE {table} TO `{principal}`")


def automate_maintenance(spark: SparkSession, delta_table: str) -> None:
    """Database Maintenance Automation: OPTIMIZE + VACUUM in one scheduled call."""
    spark.sql(f"OPTIMIZE {delta_table}")
    spark.sql(f"VACUUM {delta_table} RETAIN 168 HOURS")


def migrate_database_table(spark: SparkSession, source_table: str, target_table: str) -> None:
    """Database Migration: copy a JDBC table into a Delta target."""
    _jdbc_read(spark, f"(SELECT * FROM {source_table}) t").write.format("delta").mode("overwrite").saveAsTable(target_table)


def main() -> None:
    """Representative flow: design schema, load from SQL, index, secure, maintain."""
    spark = get_spark()
    design_database_schema(spark, "CREATE SCHEMA IF NOT EXISTS app")
    develop_database_schema(spark, "app.orders",
                            {"order_id": "BIGINT", "customer_id": "BIGINT", "amount": "DECIMAL(12,2)", "order_date": "DATE"})
    migrate_database_table(spark, "dbo.Orders", "app.orders")
    optimize_index(spark, "app.orders", ["customer_id"])
    develop_view(spark, "app.v_order_summary",
                 "SELECT customer_id, SUM(amount) total FROM app.orders GROUP BY customer_id")
    control_database_access(spark, "analysts@contoso.com", "app.orders")
    automate_maintenance(spark, "app.orders")
    print(plan_capacity(spark, "app.orders"))


if __name__ == "__main__":
    main()
