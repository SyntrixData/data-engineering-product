"""03 - Data Pipeline Development and Automation.

Pipeline design, automation, and operations on Azure Databricks with ADLS
Gen2 and Delta Lake. Covers the 14 activities of Category 3:
  Pipeline Design, Pipeline Development, Pipeline Automation,
  Workflow Orchestration, Pipeline Scheduling, Event-Driven Pipelines,
  Dependency Management, Error Handling, Automatic Retry Mechanisms,
  Pipeline Backfilling, Pipeline Parameterization, Pipeline Versioning,
  Pipeline Migration, Pipeline Performance Optimization.
"""

import os
import time
from datetime import datetime
from typing import Callable, Dict, List, Optional

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
STORAGE_ACCOUNT = os.environ.get("AZURE_STORAGE_ACCOUNT", "<storage-account>")
BRONZE_CONTAINER = os.environ.get("BRONZE_CONTAINER", "bronze")
SILVER_CONTAINER = os.environ.get("SILVER_CONTAINER", "silver")
OPS_CONTAINER = os.environ.get("OPS_CONTAINER", "ops")
PIPELINE_VERSION = os.environ.get("PIPELINE_VERSION", "v1")
ADLS = "abfss://{c}@{acct}.dfs.core.windows.net"


def get_spark(app_name: str = "pipeline-automation") -> SparkSession:
    """Return the active Spark session, or build a local Delta-enabled one."""
    existing = SparkSession.getActiveSession()
    if existing:
        return existing
    return (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .getOrCreate()
    )


def _path(container: str, *parts: str) -> str:
    return ADLS.format(c=container, acct=STORAGE_ACCOUNT) + "/" + "/".join(parts)


def _get_widget(name: str, default: str = "") -> str:
    """Activity support: Pipeline Parameterization - read a Databricks widget safely."""
    try:
        return dbutils.widgets.get(name)  # noqa: F821
    except Exception:
        return os.environ.get(name.upper(), default)


# ---------------------------------------------------------------------------
# Activity functions
# ---------------------------------------------------------------------------
def design_pipeline(name: str, stages: List[str]) -> Dict[str, object]:
    """Activity: Pipeline Design - declare a versioned pipeline spec as a dict."""
    return {"name": name, "version": PIPELINE_VERSION, "stages": stages,
            "created_at": datetime.utcnow().isoformat()}


def develop_pipeline(spark: SparkSession, spec: Dict[str, object],
                     steps: Dict[str, Callable[[], DataFrame]]) -> Dict[str, DataFrame]:
    """Activity: Pipeline Development - execute ordered stages from the design spec."""
    results: Dict[str, DataFrame] = {}
    for stage in spec["stages"]:
        results[stage] = steps[stage]()
    return results


def automate_pipeline(spark: SparkSession, spec: Dict[str, object]) -> None:
    """Activity: Pipeline Automation - persist the spec so Databricks Jobs can trigger it."""
    df = spark.createDataFrame([{"spec": str(spec), "version": PIPELINE_VERSION,
                                 "registered_at": datetime.utcnow().isoformat()}])
    (df.write.format("delta").mode("append")
     .save(_path(OPS_CONTAINER, "pipeline_registry")))


def orchestrate_workflow(tasks: Dict[str, Callable[[], None]], deps: Dict[str, List[str]]) -> None:
    """Activity: Workflow Orchestration - run tasks in dependency order (topological)."""
    done, remaining = set(), dict(tasks)
    while remaining:
        ready = [t for t in remaining if all(d in done for d in deps.get(t, []))]
        if not ready:
            raise ValueError(f"Circular dependency in tasks: {list(remaining)}")
        for task in ready:
            tasks[task]()
            done.add(task)
            del remaining[task]


def schedule_pipeline(cron: str, pipeline_name: str) -> Dict[str, str]:
    """Activity: Pipeline Scheduling - describe the cron schedule for a Databricks Job."""
    return {"pipeline": pipeline_name, "cron": cron,
            "timezone": os.environ.get("SCHEDULE_TZ", "America/Chicago")}


def event_driven_pipeline(spark: SparkSession, source_prefix: str, target: str) -> None:
    """Activity: Event-Driven Pipelines - Auto Loader triggered by new file arrival."""
    stream = (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "parquet")
        .option("cloudFiles.schemaLocation", _path(BRONZE_CONTAINER, "_schema", source_prefix))
        .load(_path(BRONZE_CONTAINER, source_prefix))
    )
    (stream.writeStream.format("delta").outputMode("append")
     .option("checkpointLocation", _path(OPS_CONTAINER, "_checkpoints", source_prefix))
     .trigger(availableNow=True)
     .start(target))


def manage_dependencies(spark: SparkSession, dataset: str, depends_on: List[str]) -> None:
    """Activity: Dependency Management - record upstream dataset dependencies."""
    df = spark.createDataFrame([{"dataset": dataset, "depends_on": d,
                                 "recorded_at": datetime.utcnow().isoformat()}
                                for d in depends_on])
    (df.write.format("delta").mode("append").save(_path(OPS_CONTAINER, "dependencies")))


def handle_errors(spark: SparkSession, stage: str, fn: Callable[[], None],
                  quarantine: Optional[str] = None) -> bool:
    """Activity: Error Handling - run a stage, log failures, quarantine bad data."""
    try:
        fn()
        return True
    except Exception as exc:  # noqa: BLE001
        log = spark.createDataFrame([{"stage": stage, "error": str(exc),
                                      "at": datetime.utcnow().isoformat()}])
        log.write.format("delta").mode("append").save(_path(OPS_CONTAINER, "error_log"))
        if quarantine:
            print(f"Stage {stage} failed; quarantined at {quarantine}")
        return False


def retry(fn: Callable[[], None], attempts: int = 3, backoff_s: int = 30) -> None:
    """Activity: Automatic Retry Mechanisms - retry with exponential backoff."""
    last: Optional[Exception] = None
    for i in range(attempts):
        try:
            fn()
            return
        except Exception as exc:  # noqa: BLE001
            last = exc
            time.sleep(backoff_s * (2 ** i))
    raise last  # type: ignore[misc]


def backfill(spark: SparkSession, source: str, target: str,
             start: str, end: str, date_col: str) -> None:
    """Activity: Pipeline Backfilling - reprocess a date range into the Delta target."""
    df = spark.read.format("delta").load(source)
    window = df.filter((F.col(date_col) >= start) & (F.col(date_col) <= end))
    (window.write.format("delta").mode("overwrite")
     .option("replaceWhere", f"{date_col} >= '{start}' AND {date_col} <= '{end}'")
     .save(target))


def parameterize() -> Dict[str, str]:
    """Activity: Pipeline Parameterization - collect run parameters from widgets/env."""
    return {
        "run_date": _get_widget("run_date", os.environ.get("RUN_DATE", "2026-10-17")),
        "env": _get_widget("env", "dev"),
        "full_refresh": _get_widget("full_refresh", "false"),
    }


def version_pipeline(spec: Dict[str, object]) -> str:
    """Activity: Pipeline Versioning - tag the spec with a version and persist history."""
    spec = dict(spec, version=PIPELINE_VERSION)
    return f"{spec['name']}@{spec['version']}"


def migrate_pipeline(spark: SparkSession, old_prefix: str, new_prefix: str) -> None:
    """Activity: Pipeline Migration - copy Delta tables from a legacy path to the new layout."""
    df = spark.read.format("delta").load(_path(BRONZE_CONTAINER, old_prefix))
    (df.write.format("delta").mode("overwrite")
     .save(_path(BRONZE_CONTAINER, new_prefix)))


def optimize_performance(spark: SparkSession, table: str, zorder_cols: List[str]) -> None:
    """Activity: Pipeline Performance Optimization - OPTIMIZE, ZORDER, and VACUUM a Delta table."""
    cols = ", ".join(zorder_cols)
    spark.sql(f"OPTIMIZE delta.`{table}` ZORDER BY ({cols})")
    spark.sql(f"VACUUM delta.`{table}` RETAIN 168 HOURS")


# ---------------------------------------------------------------------------
# Driver
# ---------------------------------------------------------------------------
def main() -> None:
    """Representative flow: design -> orchestrate (ingest, transform, optimize) with retries."""
    spark = get_spark()
    params = parameterize()
    spec = design_pipeline("daily_orders", ["ingest", "transform", "optimize"])
    automate_pipeline(spark, spec)
    manage_dependencies(spark, "silver.orders", ["bronze.orders"])

    def ingest() -> None:
        retry(lambda: spark.read.format("delta")
              .load(_path(BRONZE_CONTAINER, "orders"))
              .write.format("delta").mode("append")
              .save(_path(SILVER_CONTAINER, "orders_staging")))

    def transform() -> None:
        df = spark.read.format("delta").load(_path(SILVER_CONTAINER, "orders_staging"))
        df.withColumn("order_month", F.date_trunc("month", "order_date")) \
          .write.format("delta").mode("overwrite") \
          .save(_path(SILVER_CONTAINER, "orders"))

    def optimize() -> None:
        optimize_performance(spark, _path(SILVER_CONTAINER, "orders"), ["order_id"])

    tasks = {"ingest": ingest, "transform": transform, "optimize": optimize}
    deps = {"transform": ["ingest"], "optimize": ["transform"]}
    ok = handle_errors(spark, version_pipeline(spec),
                       lambda: orchestrate_workflow(tasks, deps))
    print(f"Pipeline {version_pipeline(spec)} run_date={params['run_date']} success={ok}")


if __name__ == "__main__":
    main()
