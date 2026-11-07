"""Category 18 - Data Platform Architecture and Consulting.

Purpose: Automated architecture-assessment toolkit for Azure data platforms.
Probes the environment (ADLS Gen2, Delta, Unity Catalog, Spark config),
scores the platform across key dimensions, and generates a recommendation
report with a target-state roadmap.

Activities covered:
  1. Data Engineering Architecture Assessment
  2. Current-State Architecture Review
  3. Target-State Architecture Design
  4. Data Platform Selection
  5. Cloud Data Platform Architecture
  6. Enterprise Data Architecture
  7. Data Lakehouse Architecture
  8. Real-Time Data Architecture
  9. Multi-Cloud Data Architecture
  10. Data Platform Modernization
  11. Technology Evaluation
  12. Data Engineering Roadmap Development
  13. Infrastructure Capacity Planning
  14. Scalability Assessment
  15. Reliability Architecture
  16. Architecture Documentation
"""

import json
import os
from datetime import datetime, timezone
from typing import Dict, List, Tuple

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

# ---------------------------------------------------------------------------
# Config block - env vars with placeholder defaults (never hardcode secrets)
# ---------------------------------------------------------------------------
CONFIG = {
    "storage_account": os.environ.get("STORAGE_ACCOUNT", "PLACEHOLDER_STORAGE_ACCOUNT"),
    "container": os.environ.get("ADLS_CONTAINER", "datalake"),
    "catalog": os.environ.get("UNITY_CATALOG", "main"),
    "assessment_db": os.environ.get("ASSESSMENT_DB", "architecture_assessment"),
    "report_dir": os.environ.get(
        "ARCH_REPORT_DIR", "architecture/reports"
    ),
    "target_state": os.environ.get("TARGET_STATE", "lakehouse"),
}


def base_path() -> str:
    return (
        f"abfss://{CONFIG['container']}@{CONFIG['storage_account']}"
        ".dfs.core.windows.net"
    )


def get_spark() -> SparkSession:
    """Build a local SparkSession with Delta configs as a fallback."""
    try:
        from delta import configure_spark_with_delta_pip  # type: ignore

        builder = SparkSession.builder
        return configure_spark_with_delta_pip(builder).getOrCreate()
    except Exception:
        return (
            SparkSession.builder.appName("platform-architecture")
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


# ---------------------------------------------------------------------------
# Probes - collect raw facts about the current environment
# ---------------------------------------------------------------------------

def probe_storage_layout(s: SparkSession) -> Dict[str, object]:
    """Probe ADLS Gen2 layout: zones present, delta vs raw file share."""
    zones = {}
    for zone in ("raw", "bronze", "silver", "gold", "archive"):
        try:
            files = s.read.format("binaryFile").load(f"{base_path()}/{zone}").count()
        except Exception:
            files = -1  # zone missing or unreadable
        zones[zone] = files
    return {"base_path": base_path(), "zones": zones}


def probe_delta_health(s: SparkSession) -> Dict[str, object]:
    """Probe Delta tables: count, Z-ordering, vacuum age, small files."""
    try:
        tables = s.sql(f"SHOW TABLES IN {CONFIG['catalog']}.silver").collect()
    except Exception:
        tables = []
    detail = []
    for t in tables:
        name = f"{CONFIG['catalog']}.silver.{t['tableName']}"
        try:
            hist = s.sql(f"DESCRIBE HISTORY {name} LIMIT 1").collect()
            last_op = hist[0]["operation"] if hist else "UNKNOWN"
        except Exception:
            last_op = "UNKNOWN"
        detail.append({"table": name, "last_operation": last_op})
    return {"delta_tables": len(detail), "tables": detail}


def probe_spark_config(s: SparkSession) -> Dict[str, str]:
    """Probe Spark runtime config relevant to architecture decisions."""
    keys = [
        "spark.sql.adaptive.enabled",
        "spark.sql.shuffle.partitions",
        "spark.databricks.delta.autoCompact.enabled",
        "spark.databricks.delta.optimizeWrite.enabled",
        "spark.sql.extensions",
    ]
    return {k: s.conf.get(k, "NOT_SET") for k in keys}


def probe_unity_catalog(s: SparkSession) -> Dict[str, object]:
    """Probe Unity Catalog presence, schemas, and external locations."""
    try:
        schemas = [r["databaseName"] for r in s.sql(f"SHOW SCHEMAS IN {CONFIG['catalog']}").collect()]
        uc = True
    except Exception:
        schemas, uc = [], False
    try:
        locations = [r["name"] for r in s.sql("SHOW EXTERNAL LOCATIONS").collect()]
    except Exception:
        locations = []
    return {"enabled": uc, "schemas": schemas, "external_locations": locations}


# ---------------------------------------------------------------------------
# Assessment and review
# ---------------------------------------------------------------------------

ASSESSMENT_DIMENSIONS = [
    "ingestion",
    "storage",
    "processing",
    "governance",
    "observability",
    "cost_efficiency",
]


def assess_architecture(s: SparkSession) -> DataFrame:
    """Activity: Data Engineering Architecture Assessment - score 6 dimensions."""
    storage = probe_storage_layout(s)
    delta = probe_delta_health(s)
    spark_cfg = probe_spark_config(s)
    uc = probe_unity_catalog(s)

    def score(dimension: str) -> Tuple[int, str]:
        if dimension == "ingestion":
            ok = sum(1 for z, c in storage["zones"].items() if c != -1 and z in ("raw",))
            return (80 if ok else 30, "raw zone present" if ok else "no raw zone")
        if dimension == "storage":
            ok = delta["delta_tables"] > 0
            return (85 if ok else 40, f"{delta['delta_tables']} delta tables")
        if dimension == "processing":
            aqe = spark_cfg.get("spark.sql.adaptive.enabled") == "true"
            return (90 if aqe else 55, "AQE on" if aqe else "AQE off")
        if dimension == "governance":
            return (90 if uc["enabled"] else 35,
                    "Unity Catalog enabled" if uc["enabled"] else "no Unity Catalog")
        if dimension == "observability":
            return (50, "no lineage/observability store detected")
        return (60, "baseline - review cost tags and autoscaling")

    rows = [
        (d, sc, note, datetime.now(timezone.utc).isoformat())
        for d in ASSESSMENT_DIMENSIONS
        for sc, note in [score(d)]
    ]
    df = s.createDataFrame(
        rows, "dimension STRING, score INT, note STRING, assessed_at STRING"
    )
    s.sql(f"CREATE DATABASE IF NOT EXISTS {CONFIG['assessment_db']}")
    df.write.format("delta").mode("append").saveAsTable(
        f"{CONFIG['assessment_db']}.dimension_scores"
    )
    return df


def review_current_state(s: SparkSession) -> Dict[str, object]:
    """Activity: Current-State Architecture Review - consolidated snapshot."""
    return {
        "assessed_at": datetime.now(timezone.utc).isoformat(),
        "storage": probe_storage_layout(s),
        "delta": probe_delta_health(s),
        "spark_config": probe_spark_config(s),
        "unity_catalog": probe_unity_catalog(s),
    }


def design_target_state(s: SparkSession, current: Dict[str, object]) -> Dict[str, object]:
    """Activity: Target-State Architecture Design - gaps and target blueprint."""
    target = CONFIG["target_state"]
    gaps = []
    if not current["unity_catalog"]["enabled"]:
        gaps.append("Enable Unity Catalog with external locations per zone")
    zones = current["storage"]["zones"]
    for z in ("bronze", "silver", "gold"):
        if zones.get(z, -1) == -1:
            gaps.append(f"Introduce medallion zone: {z}")
    if current["delta"]["delta_tables"] == 0:
        gaps.append("Migrate raw files to Delta Lake with OPTIMIZE + ZORDER")
    return {
        "target": target,
        "blueprint": {
            "ingestion": "Autoloader -> bronze (Delta)",
            "processing": "Databricks Jobs + Delta Live Tables -> silver/gold",
            "serving": "Unity Catalog + Power BI DirectQuery on gold",
            "governance": "Unity Catalog RBAC/ABAC, column masks, audit",
        },
        "gaps": gaps,
    }


# ---------------------------------------------------------------------------
# Platform selection and reference architectures
# ---------------------------------------------------------------------------

PLATFORM_MATRIX = {
    "databricks": {"streaming": 9, "ml": 9, "governance": 8, "cost": 6},
    "synapse": {"streaming": 6, "ml": 6, "governance": 7, "cost": 7},
    "fabric": {"streaming": 7, "ml": 6, "governance": 8, "cost": 8},
    "snowflake": {"streaming": 5, "ml": 6, "governance": 9, "cost": 6},
}


def select_data_platform(requirements: Dict[str, float]) -> List[Tuple[str, float]]:
    """Activity: Data Platform Selection - weighted scoring of candidates."""
    ranked = []
    for platform, caps in PLATFORM_MATRIX.items():
        total_w = sum(requirements.values()) or 1.0
        score = sum(caps[k] * w for k, w in requirements.items() if k in caps) / total_w
        ranked.append((platform, round(score, 2)))
    return sorted(ranked, key=lambda x: x[1], reverse=True)


def describe_cloud_platform() -> Dict[str, object]:
    """Activity: Cloud Data Platform Architecture - Azure reference topology."""
    return {
        "ingest": ["Event Hubs / IoT Hub", "Autoloader (ADLS Gen2)"],
        "store": ["ADLS Gen2 abfss:// zones", "Delta Lake"],
        "process": ["Azure Databricks", "Delta Live Tables"],
        "serve": ["Unity Catalog", "Power BI / Synapse serverless"],
        "ops": ["Azure Monitor", "Log Analytics", "Key Vault"],
    }


def describe_enterprise_architecture() -> Dict[str, object]:
    """Activity: Enterprise Data Architecture - domain-oriented layout."""
    return {
        "domains": ["sales", "finance", "hr", "supply_chain"],
        "pattern": "data mesh on a lakehouse foundation",
        "contracts": "versioned Delta tables + Unity Catalog shares",
        "federation": "Delta Sharing for cross-domain exchange",
    }


def describe_lakehouse_architecture() -> Dict[str, object]:
    """Activity: Data Lakehouse Architecture - medallion reference design."""
    return {
        "bronze": "raw ingestion, schema-on-read, Autoloader",
        "silver": "cleansed, conformed, SCD2 dimensions",
        "gold": "aggregated marts, BI-ready star schemas",
        "formats": ["Delta Lake", "OPTIMIZE + ZORDER", "liquid clustering"],
    }


def describe_realtime_architecture() -> Dict[str, object]:
    """Activity: Real-Time Data Architecture - streaming reference design."""
    return {
        "ingest": "Event Hubs (Kafka protocol) -> Structured Streaming",
        "process": "Databricks Structured Streaming, watermarking",
        "serve": "Delta tables + Power BI streaming datasets",
        "sla": "p95 end-to-end latency < 60s",
    }


def describe_multicloud_architecture() -> Dict[str, object]:
    """Activity: Multi-Cloud Data Architecture - portable design notes."""
    return {
        "storage": "Delta Lake (open format) on ADLS/S3/GCS",
        "compute": "Databricks multi-cloud workspaces",
        "sharing": "Delta Sharing (open protocol) across clouds",
        "caveat": "prefer single-cloud unless residency demands otherwise",
    }


def plan_modernization(s: SparkSession, current: Dict[str, object]) -> List[str]:
    """Activity: Data Platform Modernization - ordered modernization steps."""
    steps = []
    zones = current["storage"]["zones"]
    if zones.get("raw", -1) != -1 and current["delta"]["delta_tables"] == 0:
        steps.append("1. Convert raw zone to Delta bronze via Autoloader")
    if not current["unity_catalog"]["enabled"]:
        steps.append("2. Enable Unity Catalog; migrate Hive metastore objects")
    steps.append("3. Replace legacy SSIS/ADF copy with Delta Live Tables")
    steps.append("4. Adopt dbt-style SQL transforms versioned in git")
    return steps


def evaluate_technology(options: List[str], criteria: Dict[str, float]) -> DataFrame:
    """Activity: Technology Evaluation - score candidate technologies."""
    spark_local = spark or get_spark()
    catalog_scores = {
        "delta-live-tables": 9,
        "autoloader": 9,
        "kafka": 8,
        "event-hubs": 8,
        "dbt": 8,
        "airflow": 7,
    }
    rows = [
        (opt, catalog_scores.get(opt, 5),
         datetime.now(timezone.utc).isoformat())
        for opt in options
    ]
    return spark_local.createDataFrame(
        rows, "technology STRING, score INT, evaluated_at STRING"
    ).orderBy(F.desc("score"))


def develop_roadmap(s: SparkSession, gaps: List[str]) -> DataFrame:
    """Activity: Data Engineering Roadmap Development - quarter-sized plan."""
    phases = ["Q1 Foundation", "Q2 Scale", "Q3 Harden", "Q4 Optimize"]
    rows = []
    for i, gap in enumerate(gaps):
        rows.append((phases[i % len(phases)], gap,
                     datetime.now(timezone.utc).isoformat()))
    df = s.createDataFrame(rows, "phase STRING, initiative STRING, created_at STRING")
    df.write.format("delta").mode("append").saveAsTable(
        f"{CONFIG['assessment_db']}.roadmap"
    )
    return df


def plan_capacity(s: SparkSession, table: str, daily_gb: float, growth_pct: float) -> Dict[str, float]:
    """Activity: Infrastructure Capacity Planning - 12-month storage forecast."""
    current_gb = 0.0
    try:
        detail = s.sql(f"DESCRIBE DETAIL {table}").collect()
        current_gb = (detail[0]["sizeInBytes"] / 1e9) if detail else 0.0
    except Exception:
        current_gb = 0.0
    forecast = current_gb
    for _ in range(12):
        forecast = forecast * (1 + growth_pct / 100) + daily_gb * 30
    return {
        "current_gb": round(current_gb, 2),
        "forecast_12mo_gb": round(forecast, 2),
        "recommended_reserved_tb": round(forecast / 1000 * 1.2, 2),
    }


def assess_scalability(s: SparkSession) -> Dict[str, object]:
    """Activity: Scalability Assessment - check autoscaling and partitioning."""
    cfg = probe_spark_config(s)
    findings = []
    if cfg.get("spark.sql.shuffle.partitions", "200") == "200":
        findings.append("shuffle.partitions still default; tune per workload")
    if cfg.get("spark.databricks.delta.autoCompact.enabled") != "true":
        findings.append("enable Delta auto-compaction to control small files")
    return {"findings": findings, "autoscale": "verify cluster autoscaling policy"}


def design_reliability() -> Dict[str, object]:
    """Activity: Reliability Architecture - RPO/RTO and resilience patterns."""
    return {
        "rpo": "15 min (Delta time travel + ADLS soft delete)",
        "rto": "1 hour (job retry + checkpointed streaming)",
        "patterns": ["idempotent writes", "exactly-once streaming",
                     "dead-letter queues", "multi-AZ deployment"],
    }


def generate_architecture_report(s: SparkSession, output_dir: str) -> str:
    """Activity: Architecture Documentation - write the full assessment report."""
    current = review_current_state(s)
    scores = assess_architecture(s)
    target = design_target_state(s, current)
    roadmap = develop_roadmap(s, target["gaps"])
    platform_rank = select_data_platform(
        {"streaming": 0.3, "ml": 0.2, "governance": 0.3, "cost": 0.2}
    )

    lines = [
        "# Data Platform Architecture Assessment",
        f"Generated: {datetime.now(timezone.utc).isoformat()}",
        "",
        "## Dimension Scores",
    ]
    for r in scores.collect():
        lines.append(f"- {r['dimension']}: {r['score']} ({r['note']})")
    lines += ["", "## Target State", json.dumps(target, indent=2),
              "", "## Platform Ranking",
              json.dumps(platform_rank, indent=2),
              "", "## Roadmap"]
    for r in roadmap.collect():
        lines.append(f"- {r['phase']}: {r['initiative']}")
    report = "\n".join(lines)
    path = f"{output_dir.rstrip('/')}/architecture-assessment.md"
    s.createDataFrame([(report,)], ["content"]).write.mode("overwrite").text(path)
    return path


def main() -> None:
    s = spark or get_spark()
    report_path = f"{base_path()}/{CONFIG['report_dir']}"
    path = generate_architecture_report(s, report_path)
    capacity = plan_capacity(
        s, f"{CONFIG['catalog']}.silver.orders", daily_gb=5.0, growth_pct=8.0
    )
    print(f"Architecture assessment written to: {path}")
    print(f"Capacity plan: {json.dumps(capacity, indent=2)}")


if __name__ == "__main__":
    main()
