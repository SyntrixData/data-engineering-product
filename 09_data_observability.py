"""Category 9 - Data Observability and Pipeline Monitoring.

PySpark observability scripts for Azure Databricks backed by a Delta
observability mart. Activities: (1) Pipeline Monitoring, (2) Failure
Detection, (3) Freshness Monitoring, (4) Volume Monitoring, (5) Schema
Change Detection, (6) Distribution Monitoring, (7) Lineage Monitoring,
(8) Anomaly Detection, (9) SLA Monitoring, (10) Downtime Detection,
(11) Root Cause Analysis, (12) Incident Alerts, (13) Execution
Dashboards, (14) Historical Incident Analysis, (15) Dependency Monitoring.
"""

import json
import logging
import os
import urllib.request
from datetime import datetime, timedelta

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("data_observability")

# Config block: env vars with placeholder defaults.
STORAGE_ACCOUNT = os.environ.get("ADLS_STORAGE_ACCOUNT", "<storage-account>")
SILVER = os.environ.get("SILVER_CONTAINER", "silver")
CATALOG = os.environ.get("DELTA_CATALOG", "main")
OBS_SCHEMA = os.environ.get("OBS_SCHEMA", "observability")
FRESHNESS_SLA = int(os.environ.get("FRESHNESS_SLA_MINUTES", "60"))
PIPELINE_SLA = int(os.environ.get("PIPELINE_SLA_MINUTES", "120"))
VOLUME_DEVIATION = float(os.environ.get("VOLUME_DEVIATION_PCT", "0.30"))
ALERT_WEBHOOK_ENV = "OBS_ALERT_WEBHOOK_URL"


def adls(path: str, container: str = SILVER) -> str:
    return f"abfss://{container}@{STORAGE_ACCOUNT}.dfs.core.windows.net/{path}"


def get_spark(app_name: str = "data-observability") -> SparkSession:
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
        return {"pipeline": dbu.widgets.get("pipeline_name"),
                "table": dbu.widgets.get("table_path"),
                "run_id": dbu.widgets.get("run_id")}
    except Exception:
        return {"pipeline": "events_silver_pipeline", "table": adls("curated/events"), "run_id": datetime.utcnow().strftime("%Y%m%d%H%M%S")}


def write_obs_event(spark: SparkSession, event: dict) -> None:
    """Append one health event row to the Delta observability mart."""
    df = spark.createDataFrame(
        [(event.get("pipeline_name"), event.get("run_id"), event.get("check_type"),
          event.get("status"), event.get("metric_name"), float(event.get("metric_value", 0.0)),
          event.get("details"), datetime.utcnow().isoformat())],
        "pipeline_name STRING, run_id STRING, check_type STRING, status STRING, "
        "metric_name STRING, metric_value DOUBLE, details STRING, observed_at STRING")
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{OBS_SCHEMA}")
    (df.write.format("delta").mode("append")
     .saveAsTable(f"{CATALOG}.{OBS_SCHEMA}.pipeline_health"))


def _event(pipeline, run_id, check, status, metric, value, details) -> dict:
    return {"pipeline_name": pipeline, "run_id": run_id, "check_type": check,
            "status": status, "metric_name": metric, "metric_value": value,
            "details": details}


def monitor_pipeline_run(spark: SparkSession, pipeline: str, run_id: str,
                         started_at: datetime) -> dict:
    """Activity 1: Data Pipeline Monitoring - heartbeat with elapsed time."""
    elapsed = (datetime.utcnow() - started_at).total_seconds() / 60
    event = _event(pipeline, run_id, "pipeline_heartbeat", "RUNNING", "elapsed_minutes", elapsed, "Heartbeat")
    write_obs_event(spark, event)
    return event


def detect_pipeline_failures(spark: SparkSession, pipeline: str,
                             lookback_hours: int = 24) -> DataFrame:
    """Activity 2: Pipeline Failure Detection - FAILED runs in lookback window."""
    cutoff = (datetime.utcnow() - timedelta(hours=lookback_hours)).isoformat()
    return spark.sql(
        f"SELECT pipeline_name, run_id, check_type, details, observed_at FROM {CATALOG}.{OBS_SCHEMA}.pipeline_health "
        f"WHERE pipeline_name = '{pipeline}' AND status = 'FAILED' AND observed_at >= '{cutoff}' ORDER BY observed_at DESC")


def monitor_freshness(spark: SparkSession, df: DataFrame, ts_col: str,
                      pipeline: str, run_id: str) -> dict:
    """Activity 3: Data Freshness Monitoring - lag vs SLA."""
    max_ts = df.agg(F.max(ts_col).alias("m")).first()["m"]
    lag = (datetime.utcnow() - max_ts).total_seconds() / 60 if max_ts else float("inf")
    event = _event(pipeline, run_id, "freshness", "OK" if lag <= FRESHNESS_SLA else "BREACH",
                   "lag_minutes", lag, f"max({ts_col})={max_ts}")
    write_obs_event(spark, event)
    return event


def monitor_volume(spark: SparkSession, df: DataFrame, pipeline: str, run_id: str,
                   baseline_rows: float) -> dict:
    """Activity 4: Data Volume Monitoring - row count vs baseline deviation."""
    current = df.count()
    deviation = abs(current - baseline_rows) / baseline_rows if baseline_rows else 1.0
    event = _event(pipeline, run_id, "volume",
                   "OK" if deviation <= VOLUME_DEVIATION else "ANOMALY",
                   "row_count", float(current), f"baseline={baseline_rows}")
    write_obs_event(spark, event)
    return event


def detect_schema_change(spark: SparkSession, df: DataFrame, pipeline: str,
                         run_id: str, expected: dict) -> dict:
    """Activity 5: Schema Change Detection - diff live vs registered schema."""
    actual = {f.name: f.dataType.simpleString() for f in df.schema.fields}
    added = sorted(set(actual) - set(expected))
    removed = sorted(set(expected) - set(actual))
    changed = sorted(c for c in set(actual) & set(expected) if actual[c] != expected[c])
    drift = len(added) + len(removed) + len(changed)
    event = _event(pipeline, run_id, "schema_change",
                   "OK" if drift == 0 else "SCHEMA_DRIFT", "drifted_columns",
                   float(drift), json.dumps({"added": added, "removed": removed,
                                             "changed": changed}))
    write_obs_event(spark, event)
    return event


def monitor_distribution(df: DataFrame, group_col: str, top_n: int = 10) -> DataFrame:
    """Activity 6: Data Distribution Monitoring - top-N group shares."""
    total = df.count()
    return (df.groupBy(group_col).agg(F.count("*").alias("rows"))
            .withColumn("share", F.round(F.col("rows") / total, 4))
            .orderBy(F.desc("rows")).limit(top_n))


def record_lineage(spark: SparkSession, source: str, target: str,
                   transform: str, run_id: str) -> None:
    """Activity 7: Data Lineage Monitoring - append source->target edge."""
    df = spark.createDataFrame(
        [(source, target, transform, run_id, datetime.utcnow().isoformat())],
        "source STRING, target STRING, transform STRING, run_id STRING, recorded_at STRING")
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{OBS_SCHEMA}")
    (df.write.format("delta").mode("append")
     .saveAsTable(f"{CATALOG}.{OBS_SCHEMA}.lineage_edges"))


def detect_metric_anomaly(history: DataFrame, value_col: str,
                          sigma: float = 3.0) -> DataFrame:
    """Activity 8: Data Anomaly Detection - z-score over a metric series."""
    stats = history.agg(F.mean(value_col).alias("avg"), F.stddev(value_col).alias("sd")).first()
    if not stats["sd"]:
        return history.limit(0)
    return (history.withColumn("zscore", (F.col(value_col) - F.lit(stats["avg"]))
                               / F.lit(stats["sd"]))
            .filter(F.abs(F.col("zscore")) > sigma))


def check_pipeline_sla(spark: SparkSession, pipeline: str, run_id: str,
                       started_at: datetime) -> dict:
    """Activity 9: Pipeline SLA Monitoring - elapsed vs duration SLA."""
    elapsed = (datetime.utcnow() - started_at).total_seconds() / 60
    event = _event(pipeline, run_id, "pipeline_sla",
                   "OK" if elapsed <= PIPELINE_SLA else "SLA_BREACH",
                   "elapsed_minutes", elapsed, f"sla={PIPELINE_SLA}min")
    write_obs_event(spark, event)
    return event


def detect_downtime(spark: SparkSession, df: DataFrame, ts_col: str, pipeline: str,
                    run_id: str, gap_minutes: int = 180) -> dict:
    """Activity 10: Data Downtime Detection - ingestion gap beyond threshold."""
    max_ts = df.agg(F.max(ts_col).alias("m")).first()["m"]
    gap = (datetime.utcnow() - max_ts).total_seconds() / 60 if max_ts else float("inf")
    event = _event(pipeline, run_id, "downtime",
                   "DOWNTIME" if gap >= gap_minutes else "OK",
                   "gap_minutes", gap, f"max_ts={max_ts}")
    write_obs_event(spark, event)
    if event["status"] == "DOWNTIME":
        logger.error("Downtime detected for %s: gap %.1f min", pipeline, gap)
    return event


def root_cause_analysis(spark: SparkSession, pipeline: str, run_id: str) -> dict:
    """Activity 11: Root Cause Analysis - earliest failing check is the cause."""
    rows = spark.sql(
        f"SELECT check_type, status, metric_name, metric_value, details, observed_at "
        f"FROM {CATALOG}.{OBS_SCHEMA}.pipeline_health "
        f"WHERE pipeline_name = '{pipeline}' AND run_id = '{run_id}' "
        f"AND status NOT IN ('OK','RUNNING') ORDER BY observed_at ASC").collect()
    summary = {"pipeline": pipeline, "run_id": run_id, "incident_count": len(rows),
               "likely_root_cause": rows[0].asDict() if rows else {}}
    logger.info("RCA: %s", json.dumps(summary, default=str))
    return summary


def send_incident_alert(incident: dict) -> None:
    """Activity 12: Automated Incident Alerts - webhook post (no-op if unset)."""
    url = os.environ.get(ALERT_WEBHOOK_ENV, "")
    if not url:
        logger.warning("Incident (no webhook): %s", json.dumps(incident, default=str))
        return
    payload = json.dumps({"text": "Data pipeline incident", "incident": incident}, default=str)
    req = urllib.request.Request(url, data=payload.encode("utf-8"),
                                 headers={"Content-Type": "application/json"}, method="POST")
    with urllib.request.urlopen(req, timeout=30) as resp:
        logger.info("Incident alert sent -> HTTP %d", resp.status)


def build_execution_dashboard_view(spark: SparkSession) -> None:
    """Activity 13: Pipeline Execution Dashboards - BI-facing view."""
    spark.sql(
        f"CREATE OR REPLACE VIEW {CATALOG}.{OBS_SCHEMA}.v_pipeline_runs AS "
        f"SELECT pipeline_name, run_id, check_type, status, metric_name, "
        f"metric_value, observed_at FROM {CATALOG}.{OBS_SCHEMA}.pipeline_health")


def analyze_incident_history(spark: SparkSession, pipeline: str, days: int = 30) -> DataFrame:
    """Activity 14: Historical Incident Analysis - incidents by check type."""
    cutoff = (datetime.utcnow() - timedelta(days=days)).isoformat()
    return spark.sql(
        f"SELECT check_type, status, COUNT(*) AS incidents, "
        f"MIN(observed_at) AS first_seen, MAX(observed_at) AS last_seen "
        f"FROM {CATALOG}.{OBS_SCHEMA}.pipeline_health "
        f"WHERE pipeline_name = '{pipeline}' AND observed_at >= '{cutoff}' "
        f"AND status NOT IN ('OK','RUNNING') "
        f"GROUP BY check_type, status ORDER BY incidents DESC")


def check_dependencies(spark: SparkSession, pipeline: str, run_id: str,
                       dependencies: list) -> dict:
    """Activity 15: Data Dependency Monitoring - upstream freshness per table."""
    results = {}
    for dep in dependencies:
        try:
            df = spark.table(dep["table"])
            max_ts = df.agg(F.max(dep["ts_col"]).alias("m")).first()["m"]
            lag = (datetime.utcnow() - max_ts).total_seconds() / 60 if max_ts else float("inf")
            results[dep["table"]] = {"ok": lag <= dep.get("sla_minutes", FRESHNESS_SLA),
                                     "lag_minutes": round(lag, 2)}
        except Exception as exc:  # dependency check must not crash the run
            results[dep["table"]] = {"ok": False, "error": str(exc)}
    event = _event(pipeline, run_id, "dependencies",
                   "OK" if all(r["ok"] for r in results.values()) else "BLOCKED",
                   "failed_dependencies",
                   float(sum(1 for r in results.values() if not r["ok"])),
                   json.dumps(results, default=str))
    write_obs_event(spark, event)
    return results


def main() -> None:
    """End-to-end: heartbeat -> dependencies -> checks -> RCA -> alert."""
    spark = get_spark()
    cfg = read_job_config()
    started_at = datetime.utcnow()
    pipeline, run_id = cfg["pipeline"], cfg["run_id"]
    df = spark.read.format("delta").load(cfg["table"])

    monitor_pipeline_run(spark, pipeline, run_id, started_at)
    deps = check_dependencies(
        spark, pipeline, run_id,
        [{"table": f"{CATALOG}.bronze.events_landing", "ts_col": "event_ts",
          "sla_minutes": 120}])
    if not all(r["ok"] for r in deps.values()):
        send_incident_alert({"pipeline": pipeline, "run_id": run_id,
                             "issue": "dependency_blocked", "deps": deps})
        return

    monitor_freshness(spark, df, "event_ts", pipeline, run_id)
    monitor_volume(spark, df, pipeline, run_id, baseline_rows=1_000_000)
    detect_schema_change(spark, df, pipeline, run_id,
                         {f.name: f.dataType.simpleString() for f in df.schema.fields})
    detect_downtime(spark, df, "event_ts", pipeline, run_id)
    check_pipeline_sla(spark, pipeline, run_id, started_at)
    record_lineage(spark, cfg["table"], f"{CATALOG}.gold.daily_country",
                   "daily aggregation", run_id)
    build_execution_dashboard_view(spark)
    logger.info("Historical incident rows: %d",
                analyze_incident_history(spark, pipeline).count())
    if detect_pipeline_failures(spark, pipeline).count():
        send_incident_alert(root_cause_analysis(spark, pipeline, run_id))
    logger.info("Observability main() completed")


if __name__ == "__main__":
    main()
