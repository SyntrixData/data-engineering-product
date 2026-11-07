"""Category 20 - Data Engineering Production Support.

Purpose: Production support toolkit for Azure Databricks pipelines - job
monitoring, incident management, failed-job investigation, root-cause
analysis, reruns and recovery, reconciliation, performance troubleshooting,
SLA tracking, maintenance, incident reporting, on-call, capacity, DR, and
runbooks.

Activities covered:
  1. Pipeline Monitoring and Support
  2. Production Incident Management
  3. Failed Job Investigation
  4. Root Cause Analysis
  5. Data Issue Resolution
  6. Pipeline Reruns and Recovery
  7. Data Reconciliation Support
  8. Performance Troubleshooting
  9. SLA Monitoring
  10. Scheduled Maintenance
  11. Incident Reporting
  12. On-Call Engineering Support
  13. Capacity Management
  14. Disaster Recovery Planning
  15. Operational Documentation
"""

import json
import os
import uuid
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

# ---------------------------------------------------------------------------
# Config block - env vars with placeholder defaults (never hardcode secrets)
# ---------------------------------------------------------------------------
CONFIG = {
    "storage_account": os.environ.get("STORAGE_ACCOUNT", "PLACEHOLDER_STORAGE_ACCOUNT"),
    "container": os.environ.get("ADLS_CONTAINER", "datalake"),
    "support_db": os.environ.get("SUPPORT_DB", "prod_support"),
    "incident_table": os.environ.get("INCIDENT_TABLE", "incidents"),
    "run_table": os.environ.get("RUN_TABLE", "pipeline_runs"),
    "sla_table": os.environ.get("SLA_TABLE", "sla_definitions"),
    "oncall_table": os.environ.get("ONCALL_TABLE", "oncall_rota"),
    "secret_scope": os.environ.get("SECRET_SCOPE", "kv-scope"),
    "alert_webhook_key": os.environ.get("ALERT_WEBHOOK_KEY", "alert-webhook-url"),
}


def get_secret(name: str) -> str:
    """Fetch a secret from Databricks secrets with an env fallback."""
    try:
        return dbutils.secrets.get(scope=CONFIG["secret_scope"], key=name)  # type: ignore[name-defined]
    except Exception:
        return os.environ.get(name, "")


def get_spark() -> SparkSession:
    """Build a local SparkSession with Delta configs as a fallback."""
    try:
        from delta import configure_spark_with_delta_pip  # type: ignore

        builder = SparkSession.builder
        return configure_spark_with_delta_pip(builder).getOrCreate()
    except Exception:
        return (
            SparkSession.builder.appName("production-support")
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config(
                "spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog",
            )
            .getOrCreate()
        )


try:
    spark = SparkSession.builder.getOrCreate()  # type: ignore
except Exception:
    spark = None  # type: ignore


def get_param(name: str, default: str) -> str:
    try:
        return dbutils.widgets.get(name)  # type: ignore[name-defined]
    except Exception:
        return default


def ensure_support_tables(s: SparkSession) -> None:
    """Create the production-support Delta registers."""
    db = CONFIG["support_db"]
    s.sql(f"CREATE DATABASE IF NOT EXISTS {db}")
    s.sql(
        f"""CREATE TABLE IF NOT EXISTS {db}.{CONFIG['run_table']} (
        run_id STRING, pipeline STRING, status STRING, started_at STRING,
        ended_at STRING, duration_s DOUBLE, error STRING) USING DELTA"""
    )
    s.sql(
        f"""CREATE TABLE IF NOT EXISTS {db}.{CONFIG['incident_table']} (
        incident_id STRING, pipeline STRING, severity STRING, title STRING,
        status STRING, opened_at STRING, closed_at STRING,
        root_cause STRING, resolution STRING) USING DELTA"""
    )
    s.sql(
        f"""CREATE TABLE IF NOT EXISTS {db}.{CONFIG['sla_table']} (
        pipeline STRING, sla_minutes INT, owner STRING, enabled BOOLEAN) USING DELTA"""
    )
    s.sql(
        f"""CREATE TABLE IF NOT EXISTS {db}.{CONFIG['oncall_table']} (
        engineer STRING, week_start STRING, primary_contact STRING) USING DELTA"""
    )


def monitor_pipelines(s: SparkSession, hours: int = 24) -> DataFrame:
    """Activity: Pipeline Monitoring and Support - recent run health."""
    cutoff = (datetime.now(timezone.utc) - timedelta(hours=hours)).isoformat()
    return s.sql(
        f"""SELECT pipeline, status, COUNT(*) AS runs,
                   AVG(duration_s) AS avg_duration_s,
                   MAX(CASE WHEN status = 'FAILED' THEN error END) AS last_error
        FROM {CONFIG['support_db']}.{CONFIG['run_table']}
        WHERE started_at >= '{cutoff}'
        GROUP BY pipeline, status
        ORDER BY pipeline, status"""
    )


def send_alert(title: str, body: str) -> bool:
    """Post an alert to the configured webhook (returns False if unconfigured)."""
    import urllib.request

    url = get_secret(CONFIG["alert_webhook_key"])
    if not url:
        return False
    req = urllib.request.Request(
        url,
        data=json.dumps({"title": title, "text": body}).encode(),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        urllib.request.urlopen(req, timeout=10)
        return True
    except Exception:
        return False


def open_incident(
    s: SparkSession, pipeline: str, severity: str, title: str
) -> str:
    """Activity: Production Incident Management - open and page on-call."""
    incident_id = f"INC-{uuid.uuid4().hex[:8].upper()}"
    s.createDataFrame(
        [(incident_id, pipeline, severity, title, "OPEN",
          datetime.now(timezone.utc).isoformat(), "", "", "")],
        "incident_id STRING, pipeline STRING, severity STRING, title STRING,"
        " status STRING, opened_at STRING, closed_at STRING,"
        " root_cause STRING, resolution STRING",
    ).write.format("delta").mode("append").saveAsTable(
        f"{CONFIG['support_db']}.{CONFIG['incident_table']}"
    )
    oncall = get_oncall_engineer(s)
    send_alert(f"[{severity}] {incident_id}: {title}",
               f"Pipeline {pipeline}. On-call: {oncall}")
    return incident_id


def investigate_failed_job(s: SparkSession, pipeline: str) -> DataFrame:
    """Activity: Failed Job Investigation - latest failures with error text."""
    return s.sql(
        f"""SELECT run_id, started_at, duration_s, error
        FROM {CONFIG['support_db']}.{CONFIG['run_table']}
        WHERE pipeline = '{pipeline}' AND status = 'FAILED'
        ORDER BY started_at DESC LIMIT 10"""
    )


def analyze_root_cause(s: SparkSession, incident_id: str) -> Dict[str, str]:
    """Activity: Root Cause Analysis - classify the failure pattern."""
    row = s.sql(
        f"""SELECT pipeline FROM {CONFIG['support_db']}.{CONFIG['incident_table']}
        WHERE incident_id = '{incident_id}'"""
    ).collect()
    if not row:
        return {"incident_id": incident_id, "root_cause": "UNKNOWN_INCIDENT"}
    failures = investigate_failed_job(s, row[0]["pipeline"]).collect()
    cause = "UNKNOWN"
    if failures:
        err = (failures[0]["error"] or "").lower()
        if "outofmemory" in err or "java heap" in err:
            cause = "OOM - increase executor memory / repartition"
        elif "timeout" in err or "timed out" in err:
            cause = "TIMEOUT - check source latency / cluster autoscaling"
        elif "analysisexception" in err or "cannot resolve" in err:
            cause = "SCHEMA_DRIFT - column renamed or dropped upstream"
        elif "filenotfound" in err:
            cause = "MISSING_INPUT - upstream file or partition absent"
    s.sql(
        f"""UPDATE {CONFIG['support_db']}.{CONFIG['incident_table']}
        SET root_cause = '{cause}' WHERE incident_id = '{incident_id}'"""
    )
    return {"incident_id": incident_id, "root_cause": cause}


def resolve_data_issue(
    s: SparkSession, incident_id: str, resolution: str
) -> None:
    """Activity: Data Issue Resolution - record the fix and close the incident."""
    s.sql(
        f"""UPDATE {CONFIG['support_db']}.{CONFIG['incident_table']}
        SET status = 'RESOLVED', resolution = '{resolution.replace("'", "''")}',
            closed_at = current_timestamp()
        WHERE incident_id = '{incident_id}'"""
    )


def rerun_pipeline(
    s: SparkSession, pipeline: str, run_id: str, repair: bool = False
) -> str:
    """Activity: Pipeline Reruns and Recovery - log a rerun/repair request."""
    new_run_id = str(uuid.uuid4())
    mode = "REPAIR" if repair else "RERUN"
    s.createDataFrame(
        [(new_run_id, pipeline, f"{mode}_QUEUED",
          datetime.now(timezone.utc).isoformat(), "", 0.0,
          f"retry of {run_id}")],
        "run_id STRING, pipeline STRING, status STRING, started_at STRING,"
        " ended_at STRING, duration_s DOUBLE, error STRING",
    ).write.format("delta").mode("append").saveAsTable(
        f"{CONFIG['support_db']}.{CONFIG['run_table']}"
    )
    return new_run_id


def reconcile_counts(
    s: SparkSession, source: str, target: str, key: str
) -> DataFrame:
    """Activity: Data Reconciliation Support - source vs target key counts."""
    src = s.table(source).select(key).distinct().withColumn("side", F.lit("source"))
    tgt = s.table(target).select(key).distinct().withColumn("side", F.lit("target"))
    return (
        src.unionByName(tgt)
        .groupBy(key)
        .agg(F.collect_set("side").alias("sides"))
        .filter(F.size("sides") < 2)
    )


def troubleshoot_performance(s: SparkSession, pipeline: str) -> Dict[str, object]:
    """Activity: Performance Troubleshooting - flag slow runs and skew hints."""
    rows = s.sql(
        f"""SELECT AVG(duration_s) AS avg_s, MAX(duration_s) AS max_s,
                   STDDEV(duration_s) AS std_s
        FROM {CONFIG['support_db']}.{CONFIG['run_table']}
        WHERE pipeline = '{pipeline}' AND status = 'SUCCEEDED'"""
    ).collect()[0]
    recs = []
    if rows["max_s"] and rows["avg_s"] and rows["max_s"] > 3 * rows["avg_s"]:
        recs.append("max run 3x average: check data skew / partition counts")
    if rows["avg_s"] and rows["avg_s"] > 3600:
        recs.append("average run > 1h: enable AQE, auto-compaction, ZORDER")
    return {"pipeline": pipeline, "avg_s": rows["avg_s"],
            "max_s": rows["max_s"], "recommendations": recs}


def check_sla_compliance(s: SparkSession) -> DataFrame:
    """Activity: SLA Monitoring - flag pipelines breaching their SLA."""
    return s.sql(
        f"""WITH latest AS (
          SELECT pipeline, MAX(duration_s) AS last_duration_s,
                 MAX(started_at) AS last_run
          FROM {CONFIG['support_db']}.{CONFIG['run_table']}
          GROUP BY pipeline)
        SELECT l.pipeline, s.sla_minutes, l.last_duration_s,
               CASE WHEN l.last_duration_s > s.sla_minutes * 60
                    THEN 'BREACH' ELSE 'OK' END AS sla_status
        FROM latest l JOIN {CONFIG['support_db']}.{CONFIG['sla_table']} s
          ON l.pipeline = s.pipeline
        WHERE s.enabled = true"""
    )


def run_scheduled_maintenance(s: SparkSession, table: str) -> Dict[str, str]:
    """Activity: Scheduled Maintenance - OPTIMIZE + VACUUM a Delta table."""
    s.sql(f"OPTIMIZE {table} ZORDER BY (ingest_date)")
    s.sql("VACUUM {t} RETAIN 168 HOURS".format(t=table))
    return {"table": table, "optimize": "done", "vacuum": "done",
            "at": datetime.now(timezone.utc).isoformat()}


def generate_incident_report(s: SparkSession, incident_id: str) -> str:
    """Activity: Incident Reporting - render a markdown postmortem."""
    row = s.sql(
        f"""SELECT * FROM {CONFIG['support_db']}.{CONFIG['incident_table']}
        WHERE incident_id = '{incident_id}'"""
    ).collect()[0]
    report = (
        f"# Incident {incident_id}\n\n"
        f"- Pipeline: {row['pipeline']}\n- Severity: {row['severity']}\n"
        f"- Title: {row['title']}\n- Status: {row['status']}\n"
        f"- Opened: {row['opened_at']}\n- Closed: {row['closed_at']}\n"
        f"- Root cause: {row['root_cause']}\n- Resolution: {row['resolution']}\n"
    )
    path = (f"abfss://{CONFIG['container']}@{CONFIG['storage_account']}"
            f".dfs.core.windows.net/support/incidents/{incident_id}.md")
    s.createDataFrame([(report,)], ["content"]).write.mode("overwrite").text(path)
    return path


def get_oncall_engineer(s: SparkSession) -> str:
    """Activity: On-Call Engineering Support - current primary on-call."""
    rows = s.sql(
        f"""SELECT engineer, primary_contact
        FROM {CONFIG['support_db']}.{CONFIG['oncall_table']}
        ORDER BY week_start DESC LIMIT 1"""
    ).collect()
    return rows[0]["engineer"] if rows else "unassigned"


def manage_capacity(s: SparkSession) -> Dict[str, object]:
    """Activity: Capacity Management - run volume vs cluster guidance."""
    counts = s.sql(
        f"""SELECT COUNT(*) AS runs_24h,
                   SUM(CASE WHEN status='FAILED' THEN 1 ELSE 0 END) AS failed_24h
        FROM {CONFIG['support_db']}.{CONFIG['run_table']}
        WHERE started_at >= current_timestamp() - INTERVAL 24 HOURS"""
    ).collect()[0]
    guidance = "steady"
    if counts["failed_24h"] and counts["failed_24h"] > 5:
        guidance = "spike in failures - add on-call capacity, freeze deploys"
    return {"runs_24h": counts["runs_24h"], "failed_24h": counts["failed_24h"],
            "guidance": guidance}


def plan_disaster_recovery() -> Dict[str, object]:
    """Activity: Disaster Recovery Planning - RPO/RTO targets and runbook refs."""
    return {
        "rpo_minutes": 15,
        "rto_minutes": 60,
        "strategy": "ADLS geo-redundant storage + Delta time travel",
        "drills": "quarterly failover drill; runbook: support/runbooks/dr.md",
        "backups": "Delta clones to secondary region weekly",
    }


def write_runbook(s: SparkSession, pipeline: str, steps: List[str]) -> str:
    """Activity: Operational Documentation - publish a pipeline runbook."""
    doc = f"# Runbook: {pipeline}\n\n" + "\n".join(
        f"{i+1}. {step}" for i, step in enumerate(steps)
    )
    path = (f"abfss://{CONFIG['container']}@{CONFIG['storage_account']}"
            f".dfs.core.windows.net/support/runbooks/{pipeline}.md")
    s.createDataFrame([(doc,)], ["content"]).write.mode("overwrite").text(path)
    return path


def main() -> None:
    s = spark or get_spark()
    pipeline = get_param("pipeline", "sales_silver_pipeline")

    ensure_support_tables(s)
    health = monitor_pipelines(s)
    print(f"Pipeline health (24h):\n{health.toPandas().to_string()}")
    sla = check_sla_compliance(s)
    breaches = sla.filter(F.col("sla_status") == "BREACH").collect()
    for b in breaches:
        inc = open_incident(s, b["pipeline"], "P1",
                            f"SLA breach: {b['last_duration_s']}s > SLA")
        rca = analyze_root_cause(s, inc)
        print(f"Opened {inc}: {rca['root_cause']}")
    perf = troubleshoot_performance(s, pipeline)
    print(f"Performance: {json.dumps(perf, indent=2, default=str)}")
    print(f"Capacity: {json.dumps(manage_capacity(s), indent=2)}")
    write_runbook(s, pipeline, [
        "Check monitor_pipelines() for latest failure",
        "Run investigate_failed_job() and analyze_root_cause()",
        "Rerun via rerun_pipeline(); escalate to on-call if P1",
    ])


if __name__ == "__main__":
    main()
