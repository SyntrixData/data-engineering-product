"""Category 10 - Data Modeling and Transformation Management.

PySpark/SQL scripts for data modeling on Azure Databricks. Activities:
(1) Conceptual Modeling, (2) Logical Modeling, (3) Physical Modeling,
(4) Dimensional Modeling, (5) Fact/Dimension Development, (6) Star and
Snowflake Schemas, (7) Slowly Changing Dimensions, (8) Data Vault,
(9) Semantic Models, (10) Reusable SQL Transforms, (11) Dependency
Management, (12) Model Testing, (13) Documentation, (14) Version
Control, (15) Model Optimization.
"""

import hashlib
import json
import logging
import os
from datetime import datetime

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("data_modeling")

# Config block: env vars with placeholder defaults.
STORAGE_ACCOUNT = os.environ.get("ADLS_STORAGE_ACCOUNT", "<storage-account>")
SILVER = os.environ.get("SILVER_CONTAINER", "silver")
GOLD = os.environ.get("GOLD_CONTAINER", "gold")
CATALOG = os.environ.get("DELTA_CATALOG", "main")
MODEL_SCHEMA = os.environ.get("MODEL_SCHEMA", "gold")
MODEL_VERSION = os.environ.get("MODEL_VERSION", "v1")
REGISTRY_PATH = os.environ.get("MODEL_REGISTRY_PATH", "model-registry/data-model")


def adls(path: str, container: str = SILVER) -> str:
    return f"abfss://{container}@{STORAGE_ACCOUNT}.dfs.core.windows.net/{path}"


def get_spark(app_name: str = "data-modeling") -> SparkSession:
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


def read_job_config() -> dict:
    """dbutils.widgets params; env/defaults when run outside Databricks."""
    try:
        from pyspark.dbutils import DBUtils
        dbu = DBUtils(SparkSession.getActiveSession())
        return {"model_version": dbu.widgets.get("model_version"),
                "full_refresh": dbu.widgets.get("full_refresh") == "true"}
    except Exception:
        return {"model_version": MODEL_VERSION, "full_refresh": False}  # local fallback


def conceptual_model() -> dict:
    """Activity 1: Conceptual Data Modeling - business entities/relationships."""
    return {"entities": {"Customer": "A person or account placing orders",
                          "Order": "A purchase event with line items",
                          "Product": "A sellable item in the catalog",
                          "Payment": "Settlement for an order"},
            "relationships": [("Customer", "places", "Order", "1:N"),
                              ("Order", "contains", "Product", "M:N"),
                              ("Order", "settled_by", "Payment", "1:N")]}


def logical_model() -> dict:
    """Activity 2: Logical Data Modeling - attributes/keys, platform-neutral."""
    return {"Customer": {"customer_id": "string (PK)", "full_name": "string",
                         "email": "string", "country": "string(2)", "segment": "string"},
            "Order": {"order_id": "string (PK)", "customer_id": "string (FK)",
                      "order_ts": "timestamp", "status": "string", "total_amount": "decimal(18,2)"},
            "Product": {"product_id": "string (PK)", "sku": "string",
                        "category": "string", "list_price": "decimal(18,2)"}}


def apply_physical_model(spark: SparkSession) -> None:
    """Activity 3: Physical Data Modeling - Delta DDL on ADLS Gen2."""
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{MODEL_SCHEMA}")
    spark.sql(f"CREATE TABLE IF NOT EXISTS {CATALOG}.{MODEL_SCHEMA}.dim_customer (customer_sk BIGINT, "
              "customer_id STRING, full_name STRING, email STRING, country STRING, segment STRING, row_hash STRING, "
              "effective_from TIMESTAMP, effective_to TIMESTAMP, is_current BOOLEAN) USING DELTA PARTITIONED BY (country) "
              "TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')")
    spark.sql(f"CREATE TABLE IF NOT EXISTS {CATALOG}.{MODEL_SCHEMA}.fact_orders (order_id STRING, customer_sk BIGINT, "
              "product_sk BIGINT, order_ts TIMESTAMP, order_date DATE, quantity INT, total_amount DECIMAL(18,2)) "
              "USING DELTA PARTITIONED BY (order_date) TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')")


def dimensional_bus_matrix() -> dict:
    """Activity 4: Dimensional Modeling - bus matrix of processes to dims."""
    return {"processes": {"orders": ["dim_customer", "dim_product", "dim_date"],
                          "payments": ["dim_customer", "dim_date"]},
            "conformed_dimensions": ["dim_customer", "dim_product", "dim_date"]}


def build_dimensions(spark: SparkSession, silver_customers: str, silver_products: str) -> None:
    """Activity 5a: Fact and Dimension Table Development - dimensions."""
    customers = spark.read.format("delta").load(silver_customers)
    (customers.withColumn("customer_sk", F.monotonically_increasing_id())
     .withColumn("row_hash", F.sha2(F.concat_ws("|", "full_name", "email",
                                                "country", "segment"), 256))
     .withColumn("effective_from", F.current_timestamp())
     .withColumn("effective_to", F.lit(None).cast("timestamp"))
     .withColumn("is_current", F.lit(True))
     .write.format("delta").mode("overwrite")
     .saveAsTable(f"{CATALOG}.{MODEL_SCHEMA}.dim_customer"))
    (spark.read.format("delta").load(silver_products)
     .withColumn("product_sk", F.monotonically_increasing_id())
     .write.format("delta").mode("overwrite")
     .saveAsTable(f"{CATALOG}.{MODEL_SCHEMA}.dim_product"))


def build_fact_orders(spark: SparkSession, silver_orders: str) -> None:
    """Activity 5b: Fact and Dimension Table Development - fact with surrogate keys."""
    orders = spark.read.format("delta").load(silver_orders)
    dim_c = spark.table(f"{CATALOG}.{MODEL_SCHEMA}.dim_customer").filter("is_current")
    dim_p = spark.table(f"{CATALOG}.{MODEL_SCHEMA}.dim_product")
    fact = (orders.join(dim_c.select("customer_id", "customer_sk"), "customer_id", "left")
            .join(dim_p.select("product_id", "product_sk"), "product_id", "left")
            .withColumn("order_date", F.to_date("order_ts"))
            .select("order_id", "customer_sk", "product_sk", "order_ts", "order_date",
                    "quantity", "total_amount"))
    (fact.write.format("delta").mode("append").option("mergeSchema", "true")
     .saveAsTable(f"{CATALOG}.{MODEL_SCHEMA}.fact_orders"))


def build_star_schema_view(spark: SparkSession) -> None:
    """Activity 6: Star and Snowflake Schema Implementation."""
    spark.sql(f"CREATE OR REPLACE VIEW {CATALOG}.{MODEL_SCHEMA}.v_orders_star AS "
              f"SELECT f.order_id, f.order_ts, f.quantity, f.total_amount, c.full_name, c.country, c.segment, "
              f"p.sku, p.category, p.list_price FROM {CATALOG}.{MODEL_SCHEMA}.fact_orders f "
              f"LEFT JOIN {CATALOG}.{MODEL_SCHEMA}.dim_customer c ON f.customer_sk = c.customer_sk "
              f"LEFT JOIN {CATALOG}.{MODEL_SCHEMA}.dim_product p ON f.product_sk = p.product_sk")
    spark.sql(f"CREATE OR REPLACE VIEW {CATALOG}.{MODEL_SCHEMA}.v_product_snowflake AS "
              f"SELECT p.product_sk, p.sku, p.category, cat.category_name, cat.department "
              f"FROM {CATALOG}.{MODEL_SCHEMA}.dim_product p "
              f"LEFT JOIN {CATALOG}.{MODEL_SCHEMA}.dim_category cat ON p.category = cat.category_code")


def apply_scd_type2(spark: SparkSession, dim_table: str, staging_df: DataFrame,
                    business_key: str, tracked_cols: list) -> None:
    """Activity 7: Slowly Changing Dimension Implementation (Type 2)."""
    hash_expr = "sha2(concat_ws('|', " + ", ".join(tracked_cols) + "), 256)"
    staging_df.withColumn("row_hash", F.expr(hash_expr)).createOrReplaceTempView("scd_stage")
    spark.sql(
        f"MERGE INTO {dim_table} t USING scd_stage s "
        f"ON t.{business_key} = s.{business_key} AND t.is_current = true "
        f"WHEN MATCHED AND t.row_hash != s.row_hash THEN "
        f"UPDATE SET t.effective_to = current_timestamp(), t.is_current = false")
    new_versions = spark.sql(
        f"SELECT s.* FROM scd_stage s LEFT JOIN {dim_table} t "
        f"ON t.{business_key} = s.{business_key} AND t.is_current = true "
        f"WHERE t.{business_key} IS NULL OR t.row_hash != s.row_hash")
    (new_versions.withColumn("effective_from", F.current_timestamp())
     .withColumn("effective_to", F.lit(None).cast("timestamp"))
     .withColumn("is_current", F.lit(True))
     .write.format("delta").mode("append").saveAsTable(dim_table))


def build_data_vault(spark: SparkSession, source_df: DataFrame, hub_table: str) -> None:
    """Activity 8: Data Vault Modeling - hub with SHA-256 hash keys."""
    hub = (source_df
           .withColumn("customer_hk", F.sha2(F.concat_ws("|", F.col("customer_id"),
                                                        F.lit("CUSTOMER")), 256))
           .withColumn("load_ts", F.current_timestamp())
           .withColumn("record_source", F.lit("silver_customers"))
           .select("customer_hk", "customer_id", "load_ts", "record_source")
           .dropDuplicates(["customer_hk"]))
    hub.write.format("delta").mode("append").saveAsTable(hub_table)


def build_semantic_view(spark: SparkSession) -> None:
    """Activity 9: Semantic Data Model Development - BI-friendly view."""
    spark.sql(f"CREATE OR REPLACE VIEW {CATALOG}.{MODEL_SCHEMA}.v_orders_semantic AS "
              f"SELECT order_id AS `Order ID`, order_ts AS `Order Date`, full_name AS `Customer`, "
              f"country AS `Country`, segment AS `Segment`, category AS `Category`, quantity AS `Units`, "
              f"total_amount AS `Revenue` FROM {CATALOG}.{MODEL_SCHEMA}.v_orders_star")


SQL_TRANSFORMS = {
    "clean_customer": "SELECT customer_id, TRIM(full_name) AS full_name, LOWER(TRIM(email)) AS email, "
                      "UPPER(TRIM(country)) AS country, COALESCE(segment,'UNKNOWN') AS segment FROM {source}",
    "daily_revenue": "SELECT order_date, country, SUM(total_amount) AS revenue, COUNT(*) AS orders, "
                     "COUNT(DISTINCT customer_sk) AS customers FROM {source} GROUP BY order_date, country",
}


def run_sql_transform(spark: SparkSession, name: str, source: str,
                      target_table: str, mode: str = "overwrite") -> None:
    """Activity 10: Reusable SQL Transformation Development."""
    df = spark.sql(SQL_TRANSFORMS[name].format(source=source))
    df.write.format("delta").mode(mode).saveAsTable(target_table)
    logger.info("SQL transform '%s' -> %s", name, target_table)


TRANSFORM_DAG = {"dim_customer": [], "dim_product": [],
                 "fact_orders": ["dim_customer", "dim_product"],
                 "v_orders_star": ["fact_orders"], "daily_revenue": ["v_orders_star"]}


def topological_order(dag: dict) -> list:
    """Activity 11: Transformation Dependency Management (Kahn's algorithm)."""
    indegree = {n: 0 for n in dag}
    dependents = {n: [] for n in dag}
    for node, deps in dag.items():
        for dep in deps:
            indegree[node] += 1
            dependents.setdefault(dep, []).append(node)
            indegree.setdefault(dep, 0)
    queue = [n for n, d in indegree.items() if d == 0]
    order = []
    while queue:
        node = queue.pop(0)
        order.append(node)
        for dependent in dependents.get(node, []):
            indegree[dependent] -= 1
            if indegree[dependent] == 0:
                queue.append(dependent)
    if len(order) != len(indegree):
        raise ValueError("Circular dependency detected in transform DAG")
    return order


def test_data_model(spark: SparkSession) -> dict:
    """Activity 12: Data Model Testing - PK/FK/measure assertions."""
    fact = spark.table(f"{CATALOG}.{MODEL_SCHEMA}.fact_orders")
    dim = spark.table(f"{CATALOG}.{MODEL_SCHEMA}.dim_customer")
    tests = {
        "fact_pk_unique": fact.groupBy("order_id").count().filter("count > 1").count() == 0,
        "fact_customer_fk_resolves": fact.filter(F.col("customer_sk").isNull()).count() == 0,
        "fact_amounts_non_negative": fact.filter(F.col("total_amount") < 0).count() == 0,
        "dim_scd2_single_current": (dim.filter("is_current").groupBy("customer_id").count()
                                    .filter("count > 1").count() == 0),
    }
    for name, passed in tests.items():
        logger.info("Model test %s: %s", name, "PASS" if passed else "FAIL")
    return tests


def document_data_model(spark: SparkSession, output_path: str) -> None:
    """Activity 13: Data Model Documentation - JSON data dictionary to ADLS."""
    dictionary = {"generated_at": datetime.utcnow().isoformat(), "tables": {}}
    for table in ["dim_customer", "dim_product", "fact_orders"]:
        df = spark.table(f"{CATALOG}.{MODEL_SCHEMA}.{table}")
        dictionary["tables"][table] = [{"name": f.name, "type": f.dataType.simpleString()}
                                       for f in df.schema.fields]
    spark.sparkContext.parallelize([json.dumps(dictionary, indent=2)]).saveAsTextFile(adls(output_path, GOLD))


def version_data_model(spark: SparkSession, version: str = MODEL_VERSION) -> str:
    """Activity 14: Data Model Version Control - hashed snapshot to registry."""
    payload = json.dumps({"conceptual": conceptual_model(), "logical": logical_model(),
                          "dag": TRANSFORM_DAG, "version": version}, sort_keys=True)
    digest = hashlib.sha256(payload.encode("utf-8")).hexdigest()[:12]
    path = adls(f"{REGISTRY_PATH}/{version}-{digest}.json", GOLD)
    spark.sparkContext.parallelize([payload]).saveAsTextFile(path)
    logger.info("Model version %s registered at %s", version, path)
    return path


def optimize_data_model(spark: SparkSession) -> None:
    """Activity 15: Data Model Optimization - OPTIMIZE + ZORDER on join keys."""
    spark.sql(f"OPTIMIZE {CATALOG}.{MODEL_SCHEMA}.fact_orders "
              f"ZORDER BY (order_date, customer_sk)")
    spark.sql(f"OPTIMIZE {CATALOG}.{MODEL_SCHEMA}.dim_customer ZORDER BY (customer_id)")


def main() -> None:
    """End-to-end: model -> build -> test -> document -> version -> optimize."""
    spark = get_spark()
    cfg = read_job_config()
    apply_physical_model(spark)
    build_dimensions(spark, adls("curated/customers"), adls("curated/products"))
    build_fact_orders(spark, adls("curated/orders"))
    build_star_schema_view(spark)
    build_semantic_view(spark)
    for step in topological_order(TRANSFORM_DAG):
        logger.info("Transform step: %s", step)
    run_sql_transform(spark, "daily_revenue", f"{CATALOG}.{MODEL_SCHEMA}.v_orders_star",
                      f"{CATALOG}.{MODEL_SCHEMA}.agg_daily_revenue")
    results = test_data_model(spark)
    if not all(results.values()):
        raise RuntimeError(f"Data model tests failed: {results}")
    document_data_model(spark, "docs/data-dictionary")
    version_data_model(spark, cfg["model_version"])
    optimize_data_model(spark)
    logger.info("Data modeling main() completed")


if __name__ == "__main__":
    main()
