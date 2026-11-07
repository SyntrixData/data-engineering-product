"""Category 16 - Data Lineage Services.

Purpose: Capture, store, and analyze lineage across PySpark pipelines on
Azure Databricks using OpenLineage-style event emission and a Delta-backed
lineage store on ADLS Gen2.

Activities covered:
  1. Source-to-Target Lineage
  2. Table-Level Lineage
  3. Column-Level Lineage
  4. Pipeline Dependency Mapping
  5. Transformation Lineage
  6. Cross-Platform Lineage
  7. Impact Analysis
  8. Upstream Dependency Analysis
  9. Downstream Dependency Analysis
  10. Lineage Visualization
  11. Automated Lineage Documentation
  12. Lineage Change Detection
"""

import json
import os
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    ArrayType,
    StringType,
    StructField,
    StructType,
)

# ---------------------------------------------------------------------------
# Config block - env vars with placeholder defaults (never hardcode secrets)
# ---------------------------------------------------------------------------
CONFIG = {
    "storage_account": os.environ.get("STORAGE_ACCOUNT", "PLACEHOLDER_STORAGE_ACCOUNT"),
    "container": os.environ.get("ADLS_CONTAINER", "datalake"),
    "lineage_db": os.environ.get("LINEAGE_DB", "lineage_catalog"),
    "lineage_events_table": os.environ.get(
        "LINEAGE_EVENTS_TABLE", "lineage_events"
    ),
    "lineage_nodes_table": os.environ.get(
        "LINEAGE_NODES_TABLE", "lineage_nodes"
    ),
    "lineage_edges_table": os.environ.get(
        "LINEAGE_EDGES_TABLE", "lineage_edges"
    ),
    "openlineage_namespace": os.environ.get(
        "OPENLINEAGE_NAMESPACE", "azure-databricks-prod"
    ),
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
            SparkSession.builder.appName("data-lineage")
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
    """Read a Databricks widget parameter with a safe fallback."""
    try:
        return dbutils.widgets.get(name)  # type: ignore[name-defined]
    except Exception:
        return default


LINEAGE_EVENT_SCHEMA = StructType(
    [
        StructField("event_id", StringType(), False),
        StructField("event_time", StringType(), False),
        StructField("namespace", StringType(), False),
        StructField("job_name", StringType(), False),
        StructField("run_id", StringType(), False),
        StructField("event_type", StringType(), False),
        StructField("inputs", ArrayType(StringType()), True),
        StructField("outputs", ArrayType(StringType()), True),
        StructField("columns", ArrayType(StringType()), True),
        StructField("payload", StringType(), True),
    ]
)


def ensure_lineage_tables(s: SparkSession) -> None:
    """Create the Delta lineage store (events, nodes, edges) if missing."""
    s.sql(f"CREATE DATABASE IF NOT EXISTS {CONFIG['lineage_db']}")
    s.sql(
        f"""
        CREATE TABLE IF NOT EXISTS
            {CONFIG['lineage_db']}.{CONFIG['lineage_events_table']} (
            event_id STRING, event_time STRING, namespace STRING,
            job_name STRING, run_id STRING, event_type STRING,
            inputs ARRAY<STRING>, outputs ARRAY<STRING>,
            columns ARRAY<STRING>, payload STRING
        ) USING DELTA
        """
    )
    s.sql(
        f"""
        CREATE TABLE IF NOT EXISTS
            {CONFIG['lineage_db']}.{CONFIG['lineage_nodes_table']} (
            node_id STRING, node_type STRING, name STRING,
            platform STRING, first_seen STRING, last_seen STRING
        ) USING DELTA
        """
    )
    s.sql(
        f"""
        CREATE TABLE IF NOT EXISTS
            {CONFIG['lineage_db']}.{CONFIG['lineage_edges_table']} (
            edge_id STRING, src_node STRING, dst_node STRING,
            edge_kind STRING, job_name STRING, run_id STRING,
            transform STRING, first_seen STRING, last_seen STRING
        ) USING DELTA
        """
    )


def emit_openlineage_event(
    s: SparkSession,
    job_name: str,
    run_id: str,
    event_type: str,
    inputs: List[str],
    outputs: List[str],
    columns: Optional[List[str]] = None,
    extra_facets: Optional[Dict[str, Any]] = None,
) -> str:
    """Emit an OpenLineage-style event and persist it to the Delta store."""
    event_id = str(uuid.uuid4())
    now = datetime.now(timezone.utc).isoformat()
    payload = json.dumps(
        {
            "producer": "data-engineering-product/16_data_lineage",
            "eventType": event_type,
            "namespace": CONFIG["openlineage_namespace"],
            "facets": extra_facets or {},
        }
    )
    row = [
        (
            event_id,
            now,
            CONFIG["openlineage_namespace"],
            job_name,
            run_id,
            event_type,
            inputs,
            outputs,
            columns or [],
            payload,
        )
    ]
    df = s.createDataFrame(row, schema=LINEAGE_EVENT_SCHEMA)
    df.write.format("delta").mode("append").saveAsTable(
        f"{CONFIG['lineage_db']}.{CONFIG['lineage_events_table']}"
    )
    return event_id


def record_source_to_target_lineage(
    s: SparkSession,
    job_name: str,
    run_id: str,
    source_paths: List[str],
    target_paths: List[str],
    platform: str = "adls-gen2",
) -> str:
    """Activity: Source-to-Target Lineage - register end-to-end lineage."""
    for path in source_paths + target_paths:
        s.sql(
            f"""
            MERGE INTO {CONFIG['lineage_db']}.{CONFIG['lineage_nodes_table']} t
            USING (SELECT '{path}' AS name) src
            ON t.name = src.name
            WHEN MATCHED THEN UPDATE SET last_seen = current_timestamp()
            WHEN NOT MATCHED THEN INSERT
                (node_id, node_type, name, platform, first_seen, last_seen)
            VALUES
                (uuid(), 'dataset', '{path}', '{platform}',
                 current_timestamp(), current_timestamp())
            """
        )
    event_id = emit_openlineage_event(
        s, job_name, run_id, "COMPLETE", source_paths, target_paths
    )
    for src_path in source_paths:
        for dst_path in target_paths:
            s.sql(
                f"""
                INSERT INTO {CONFIG['lineage_db']}.{CONFIG['lineage_edges_table']}
                VALUES (uuid(), '{src_path}', '{dst_path}', 'dataset',
                        '{job_name}', '{run_id}', 'etl_copy',
                        current_timestamp(), current_timestamp())
                """
            )
    return event_id


def record_table_level_lineage(
    s: SparkSession, job_name: str, run_id: str, df: DataFrame, source_table: str, target_table: str
) -> str:
    """Activity: Table-Level Lineage - infer lineage from a DataFrame read."""
    return emit_openlineage_event(
        s,
        job_name,
        run_id,
        "COMPLETE",
        inputs=[source_table],
        outputs=[target_table],
        columns=df.columns,
        extra_facets={"schema": {"fields": df.columns}},
    )


def record_column_level_lineage(
    s: SparkSession,
    job_name: str,
    run_id: str,
    column_map: Dict[str, str],
    source_table: str,
    target_table: str,
) -> str:
    """Activity: Column-Level Lineage - map target columns to source columns."""
    columns = [f"{tgt} <- {src}" for tgt, src in column_map.items()]
    edges = s.createDataFrame(
        [
            (
                str(uuid.uuid4()),
                f"{source_table}.{src}",
                f"{target_table}.{tgt}",
                "column",
                job_name,
                run_id,
                "expression",
                datetime.now(timezone.utc).isoformat(),
                datetime.now(timezone.utc).isoformat(),
            )
            for tgt, src in column_map.items()
        ],
        "edge_id STRING, src_node STRING, dst_node STRING, edge_kind STRING,"
        " job_name STRING, run_id STRING, transform STRING,"
        " first_seen STRING, last_seen STRING",
    )
    edges.write.format("delta").mode("append").saveAsTable(
        f"{CONFIG['lineage_db']}.{CONFIG['lineage_edges_table']}"
    )
    return emit_openlineage_event(
        s,
        job_name,
        run_id,
        "COMPLETE",
        inputs=[source_table],
        outputs=[target_table],
        columns=columns,
    )


def build_pipeline_dependency_map(s: SparkSession, job_name: str) -> DataFrame:
    """Activity: Pipeline Dependency Mapping - return the DAG edges for a job."""
    return s.sql(
        f"""
        SELECT src_node, dst_node, edge_kind, transform, run_id
        FROM {CONFIG['lineage_db']}.{CONFIG['lineage_edges_table']}
        WHERE job_name = '{job_name}'
        ORDER BY first_seen
        """
    )


def record_transformation_lineage(
    s: SparkSession,
    job_name: str,
    run_id: str,
    transform_sql: str,
    inputs: List[str],
    outputs: List[str],
) -> str:
    """Activity: Transformation Lineage - attach the transform logic to lineage."""
    return emit_openlineage_event(
        s,
        job_name,
        run_id,
        "COMPLETE",
        inputs,
        outputs,
        extra_facets={"transformation": {"sql": transform_sql}},
    )


def record_cross_platform_lineage(
    s: SparkSession,
    job_name: str,
    run_id: str,
    platform_links: List[Dict[str, str]],
) -> List[str]:
    """Activity: Cross-Platform Lineage - link assets across platforms."""
    event_ids = []
    for link in platform_links:
        event_ids.append(
            emit_openlineage_event(
                s,
                job_name,
                run_id,
                "COMPLETE",
                inputs=[link["source"]],
                outputs=[link["target"]],
                extra_facets={
                    "crossPlatform": {
                        "sourcePlatform": link.get("source_platform", "unknown"),
                        "targetPlatform": link.get("target_platform", "unknown"),
                    }
                },
            )
        )
    return event_ids


def _traverse(s: SparkSession, start: str, direction: str, max_depth: int) -> DataFrame:
    """Generic BFS over the edges table (upstream or downstream)."""
    col_src, col_dst = ("dst_node", "src_node") if direction == "up" else (
        "src_node",
        "dst_node",
    )
    visited = [start]
    depth = 0
    frames = []
    frontier = [start]
    while frontier and depth < max_depth:
        rows = (
            s.table(f"{CONFIG['lineage_db']}.{CONFIG['lineage_edges_table']}")
            .filter(F.col(col_src).isin(frontier))
            .select(F.col(col_dst).alias("node"), "edge_kind", "job_name")
            .distinct()
            .collect()
        )
        frontier = [r["node"] for r in rows if r["node"] not in visited]
        visited.extend(frontier)
        if rows:
            frames.append(s.createDataFrame(rows))
        depth += 1
    if not frames:
        return s.createDataFrame([], "node STRING, edge_kind STRING, job_name STRING")
    out = frames[0]
    for f_ in frames[1:]:
        out = out.unionByName(f_)
    return out.distinct()


def impact_analysis(s: SparkSession, node: str, max_depth: int = 5) -> DataFrame:
    """Activity: Impact Analysis - find everything downstream of a node."""
    return _traverse(s, node, "down", max_depth)


def upstream_dependency_analysis(s: SparkSession, node: str, max_depth: int = 5) -> DataFrame:
    """Activity: Upstream Dependency Analysis - find everything upstream of a node."""
    return _traverse(s, node, "up", max_depth)


def downstream_dependency_analysis(s: SparkSession, node: str, max_depth: int = 5) -> DataFrame:
    """Activity: Downstream Dependency Analysis - alias of impact analysis."""
    return impact_analysis(s, node, max_depth)


def render_lineage_visualization(s: SparkSession, job_name: str) -> str:
    """Activity: Lineage Visualization - render the DAG as Mermaid text."""
    edges = build_pipeline_dependency_map(s, job_name).collect()
    lines = ["graph TD"]
    seen = set()
    for e in edges:
        src = e["src_node"].split("/")[-1].replace("-", "_")
        dst = e["dst_node"].split("/")[-1].replace("-", "_")
        label = f"{src}[{e['src_node']}] -->|{e['edge_kind']}| {dst}[{e['dst_node']}]"
        if (src, dst) not in seen:
            lines.append("    " + label)
            seen.add((src, dst))
    mermaid = "\n".join(lines)
    path = f"{base_path()}/lineage/visualizations/{job_name}.mmd"
    s.createDataFrame([(mermaid,)], ["content"]).write.mode("overwrite").text(path)
    return path


def generate_lineage_documentation(s: SparkSession, output_dir: str) -> str:
    """Activity: Automated Lineage Documentation - write a markdown catalog."""
    tables = [
        CONFIG["lineage_events_table"],
        CONFIG["lineage_nodes_table"],
        CONFIG["lineage_edges_table"],
    ]
    lines = ["# Data Lineage Catalog", ""]
    for t in tables:
        count = s.table(f"{CONFIG['lineage_db']}.{t}").count()
        lines.append(f"## {t} ({count} records)")
        for col in s.table(f"{CONFIG['lineage_db']}.{t}").columns:
            lines.append(f"- {col}")
        lines.append("")
    doc = "\n".join(lines)
    path = f"{output_dir.rstrip('/')}/lineage-catalog.md"
    s.createDataFrame([(doc,)], ["content"]).write.mode("overwrite").text(path)
    return path


def detect_lineage_changes(s: SparkSession, job_name: str, run_id: str) -> DataFrame:
    """Activity: Lineage Change Detection - diff edges vs the previous run."""
    edges = s.table(f"{CONFIG['lineage_db']}.{CONFIG['lineage_edges_table']}")
    current = edges.filter(
        (F.col("job_name") == job_name) & (F.col("run_id") == run_id)
    ).select("src_node", "dst_node", "edge_kind")
    previous = edges.filter(
        (F.col("job_name") == job_name) & (F.col("run_id") != run_id)
    ).select("src_node", "dst_node", "edge_kind")
    added = current.exceptAll(previous).withColumn("change", F.lit("ADDED"))
    removed = previous.exceptAll(current).withColumn("change", F.lit("REMOVED"))
    return added.unionByName(removed)


def main() -> None:
    s = spark or get_spark()
    job_name = get_param("job_name", "demo_etl_job")
    run_id = get_param("run_id", str(uuid.uuid4()))
    src = f"{base_path()}/raw/sales"
    tgt = f"{base_path()}/delta/sales_clean"

    ensure_lineage_tables(s)
    record_source_to_target_lineage(s, job_name, run_id, [src], [tgt])
    record_column_level_lineage(
        s,
        job_name,
        run_id,
        {"order_total": "amount", "order_date": "txn_dt"},
        src,
        tgt,
    )
    record_transformation_lineage(
        s,
        job_name,
        run_id,
        "SELECT CAST(amount AS DECIMAL(12,2)) AS order_total FROM raw",
        [src],
        [tgt],
    )
    downstream = impact_analysis(s, src)
    render_lineage_visualization(s, job_name)
    generate_lineage_documentation(s, f"{base_path()}/lineage/docs")
    changes = detect_lineage_changes(s, job_name, run_id)
    print(f"Lineage flow complete: {downstream.count()} downstream nodes, "
          f"{changes.count()} edge changes detected.")


if __name__ == "__main__":
    main()
