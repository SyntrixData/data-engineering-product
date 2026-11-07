"""Category 8 - Data Quality Engineering.

PySpark data quality framework for Azure Databricks: profiling, validation,
reconciliation, scoring, remediation and alerting, persisted to a Delta DQ
mart. Activities covered: (1) Data Quality Assessment, (2) Data Profiling,
(3) Data Validation, (4) Data Completeness Checks, (5) Duplicate Detection,
(6) Schema Validation, (7) Referential Integrity Checks, (8) Source-to-Target
Reconciliation, (9) Data Freshness Monitoring, (10) Data Anomaly Detection,
(11) Automated Data Quality Testing, (12) Data Quality Scoring,
(13) Data Quality Dashboards, (14) Data Quality Remediation,
(15) Data Quality Alerts.
"""

import logging
import os
from datetime import datetime

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("data_quality_engineering")

# Config block: env vars with placeholder defaults.
STORAGE_ACCOUNT = os.environ.get("ADLS_STORAGE_ACCOUNT", "<storage-account>")
SILVER = os.environ.get("SILVER_CONTAINER", "silver")
GOLD = os.environ.get("GOLD_CONTAINER", "gold")
CATALOG = os.environ.get("DELTA_CATALOG", "main")
DQ_SCHEMA = os.environ.get("DQ_SCHEMA", "dq_mart")
COMPLETENESS_THRESHOLD = float(os.environ.get("COMPLETENESS_THRESHOLD", "0.98"))
FRESHNESS_SLA_MINUTES = int(os.environ.get("FRESHNESS_SLA_MINUTES", "60"))


def adls(path: str, container: str = SILVER) -> str:
    return f"abfss://{container}@{STORAGE_ACCOUNT}.dfs.core.windows.net/{path}"


def get_spark(app_name: str = "data-quality-engineering") -> SparkSession:
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
        return {"table": dbu.widgets.get("table_path"),
                "dataset": dbu.widgets.get("dataset_name"),
                "run_id": dbu.widgets.get("run_id")}
    except Exception:
        return {"table": adls("curated/events"), "dataset": "events_silver",
                "run_id": datetime.utcnow().strftime("%Y%m%d%H%M%S")}


def profile_dataset(df: DataFrame, dataset: str) -> DataFrame:
    """Activities 1-2: Data Quality Assessment / Data Profiling per column."""
    total = df.count()
    rows = []
    for field in df.schema.fields:
        col = field.name
        stats = df.agg(F.sum(F.col(col).isNull().cast("int")).alias("nulls"),
                       F.approx_count_distinct(col).alias("distincts")).first()
        num = df.select(F.col(col).cast("double")).agg(
            F.min(col).alias("mn"), F.max(col).alias("mx"), F.mean(col).alias("avg")).first()
        rows.append((dataset, col, str(field.dataType), total, int(stats["nulls"]),
                     round(int(stats["nulls"]) / total, 4) if total else 0.0,
                     int(stats["distincts"]), str(num["mn"]), str(num["mx"]), str(num["avg"])))
    return df.sparkSession.createDataFrame(
        rows, "dataset STRING, column_name STRING, data_type STRING, row_count LONG, "
              "null_count LONG, null_rate DOUBLE, distinct_count LONG, "
              "min_val STRING, max_val STRING, mean_val STRING")


RULES = {
    "amount_non_negative": "amount >= 0",
    "event_id_present": "event_id IS NOT NULL",
    "event_ts_present": "event_ts IS NOT NULL",
    "country_iso2": "country RLIKE '^[A-Z]{2}$'",
    "event_type_allowed": "event_type IN ('purchase','refund','view')",
}


def validate_rules(df: DataFrame, dataset: str, run_id: str) -> DataFrame:
    """Activity 3: Data Validation - declarative rules with violation counts."""
    total = df.count()
    now = datetime.utcnow().isoformat()
    rows = [(dataset, run_id, name, total, df.filter(f"NOT ({expr})").count(),
             now) for name, expr in RULES.items()]
    return df.sparkSession.createDataFrame(
        rows, "dataset STRING, run_id STRING, rule_name STRING, total_rows LONG, "
              "violation_rows LONG, checked_at STRING")


def check_completeness(df: DataFrame, required_cols: list,
                       threshold: float = COMPLETENESS_THRESHOLD) -> dict:
    """Activity 4: Data Completeness Checks vs threshold."""
    total = df.count()
    report = {}
    for col in required_cols:
        ratio = df.filter(F.col(col).isNotNull()).count() / total if total else 0.0
        report[col] = {"completeness": round(ratio, 4), "passed": ratio >= threshold}
        if ratio < threshold:
            logger.warning("Completeness breach on %s: %.4f", col, ratio)
    return report


def detect_duplicates(df: DataFrame, key_cols: list) -> DataFrame:
    """Activity 5: Duplicate Detection - duplicated key groups with counts."""
    return (df.groupBy(*key_cols).agg(F.count("*").alias("occurrences"))
            .filter(F.col("occurrences") > 1).orderBy(F.desc("occurrences")))


def validate_schema(df: DataFrame, expected: dict) -> list:
    """Activity 6: Schema Validation - missing/extra/type-mismatch columns."""
    actual = {f.name: str(f.dataType) for f in df.schema.fields}
    issues = ([f"missing column: {c}" for c in expected if c not in actual]
              + [f"unexpected column: {c}" for c in actual if c not in expected]
              + [f"type mismatch: {c} (expected {t}, got {actual[c]})"
                 for c, t in expected.items()
                 if c in actual and t.lower() not in actual[c].lower()])
    if issues:
        logger.warning("Schema issues: %s", issues)
    return issues


def check_referential_integrity(fact: DataFrame, dim: DataFrame,
                                fact_key: str, dim_key: str) -> int:
    """Activity 7: Referential Integrity Checks - orphan fact row count."""
    orphans = fact.join(dim, fact[fact_key] == dim[dim_key], "left_anti").count()
    logger.info("Orphan rows on %s: %d", fact_key, orphans)
    return orphans


def reconcile_source_target(source: DataFrame, target: DataFrame,
                            key: str, measure_cols: list) -> DataFrame:
    """Activity 8: Source-to-Target Reconciliation - mismatched keys."""
    s = source.groupBy(key).agg(F.count("*").alias("n"),
                                *[F.sum(c).alias(f"s_{c}") for c in measure_cols])
    t = target.groupBy(key).agg(F.count("*").alias("n"),
                                *[F.sum(c).alias(f"t_{c}") for c in measure_cols])
    joined = s.join(t, key, "full_outer")
    mismatch = F.col("s.n") != F.col("t.n")
    for c in measure_cols:
        mismatch = mismatch | (F.col(f"s_{c}") != F.col(f"t_{c}"))
    return joined.filter(mismatch)


def check_freshness(df: DataFrame, ts_col: str,
                    sla_minutes: int = FRESHNESS_SLA_MINUTES) -> dict:
    """Activity 9: Data Freshness Monitoring - lag vs SLA."""
    max_ts = df.agg(F.max(ts_col).alias("m")).first()["m"]
    lag = (datetime.utcnow() - max_ts).total_seconds() / 60 if max_ts else float("inf")
    status = {"max_ts": str(max_ts), "lag_minutes": round(lag, 2),
              "breached": lag > sla_minutes}
    if status["breached"]:
        logger.error("Freshness SLA breached: %s", status)
    return status


def detect_anomalies(df: DataFrame, metric_col: str, group_col: str,
                     sigma: float = 3.0) -> DataFrame:
    """Activity 10: Data Anomaly Detection - z-score outliers per group."""
    stats = df.groupBy(group_col).agg(F.mean(metric_col).alias("avg"),
                                      F.stddev(metric_col).alias("sd"))
    return (df.join(stats, group_col)
            .withColumn("zscore", (F.col(metric_col) - F.col("avg")) / F.nullif(F.col("sd"), 0))
            .filter(F.abs(F.col("zscore")) > sigma))


def run_dq_test_suite(df: DataFrame, dataset: str, run_id: str) -> DataFrame:
    """Activity 11: Automated Data Quality Testing - rules + duplicates union."""
    results = validate_rules(df, dataset, run_id)
    dupe_rows = [(dataset, run_id, f"duplicate_key:{r['event_id']}", 0,
                  int(r["occurrences"]), datetime.utcnow().isoformat())
                 for r in detect_duplicates(df, ["event_id"]).limit(1000).collect()]
    if dupe_rows:
        results = results.unionByName(df.sparkSession.createDataFrame(dupe_rows, results.schema))
    return results


def score_data_quality(test_results: DataFrame) -> float:
    """Activity 12: Data Quality Scoring - 0-100 from violation share."""
    agg = test_results.agg(F.sum("total_rows").alias("t"),
                           F.sum("violation_rows").alias("v")).first()
    score = 100.0 * (1 - (agg["v"] or 0) / (agg["t"] or 1)) if agg["t"] else 0.0
    logger.info("DQ score: %.2f", score)
    return round(score, 2)


def publish_dq_mart(spark: SparkSession, results: DataFrame, table: str) -> None:
    """Activity 13: Data Quality Dashboards - persist results to the DQ mart."""
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{DQ_SCHEMA}")
    (results.write.format("delta").mode("append").option("mergeSchema", "true")
     .saveAsTable(f"{CATALOG}.{DQ_SCHEMA}.{table}"))


def remediate_dataset(df: DataFrame) -> DataFrame:
    """Activity 14: Data Quality Remediation - deterministic fixes."""
    fixed = df
    for field in df.schema.fields:
        if isinstance(field.dataType, StringType):
            fixed = fixed.withColumn(field.name, F.trim(F.col(field.name)))
    return (fixed
            .withColumn("amount", F.when(F.col("amount") < 0, F.lit(None).cast("double"))
                        .otherwise(F.col("amount")))
            .withColumn("country", F.coalesce(F.col("country"), F.lit("UNKNOWN")))
            .dropDuplicates(["event_id"]))


def send_dq_alert(score: float, dataset: str,
                  webhook_env: str = "DQ_ALERT_WEBHOOK_URL") -> None:
    """Activity 15: Data Quality Alerts - webhook when score drops below threshold."""
    import json
    import urllib.request
    threshold = float(os.environ.get("DQ_ALERT_THRESHOLD", "95.0"))
    url = os.environ.get(webhook_env, "")
    if score >= threshold or not url:
        logger.info("DQ score %.2f OK for %s", score, dataset)
        return
    payload = json.dumps({"dataset": dataset, "dq_score": score,
                          "threshold": threshold, "severity": "high"})
    req = urllib.request.Request(url, data=payload.encode("utf-8"),
                                 headers={"Content-Type": "application/json"}, method="POST")
    with urllib.request.urlopen(req, timeout=30) as resp:
        logger.warning("DQ alert sent for %s -> HTTP %d", dataset, resp.status)


def main() -> None:
    """End-to-end: profile -> test -> score -> publish -> remediate -> alert."""
    spark = get_spark()
    cfg = read_job_config()
    df = spark.read.format("delta").load(cfg["table"])
    dataset, run_id = cfg["dataset"], cfg["run_id"]

    publish_dq_mart(spark, profile_dataset(df, dataset).withColumn("run_id", F.lit(run_id)),
                    "dq_profile_history")
    results = run_dq_test_suite(df, dataset, run_id)
    check_completeness(df, ["event_id", "event_ts", "customer_id"])
    schema_issues = validate_schema(df, {"event_id": "StringType",
                                         "event_ts": "TimestampType", "amount": "DoubleType"})
    dims = spark.read.format("delta").load(adls("dims/customers"))
    check_referential_integrity(df, dims, "customer_id", "customer_id")
    check_freshness(df, "event_ts")

    gold = spark.read.format("delta").load(adls("gold/daily_country", GOLD))
    silver_agg = df.groupBy("country").agg(F.sum("amount").alias("amount"))
    logger.info("Recon mismatches: %d",
                reconcile_source_target(silver_agg, gold, "country", ["amount"]).count())
    logger.info("Anomalies: %d", detect_anomalies(df, "amount", "country").count())

    score = score_data_quality(results)
    publish_dq_mart(spark, results, "dq_test_results")
    remediate_dataset(df).write.format("delta").mode("overwrite").save(
        adls("curated/events_remediated"))
    send_dq_alert(score, dataset)
    logger.info("DQ main() done: score=%.2f schema_issues=%d", score, len(schema_issues))


if __name__ == "__main__":
    main()
