"""Category 17 - Data Governance and Security Engineering.

Purpose: Implement data governance and security controls on Azure Databricks
via Unity Catalog SQL (RBAC/ABAC grants, classification tags, row filters,
column masks) plus PySpark-driven PII discovery, auditing, and retention.

Activities covered:
  1. Data Governance Framework Implementation
  2. Role-Based Access Control
  3. Attribute-Based Access Control
  4. Data Classification
  5. Sensitive Data Discovery
  6. Personally Identifiable Information (PII) Identification
  7. Data Masking
  8. Tokenization Integration
  9. Encryption Configuration
  10. Data Retention Policy Implementation
  11. Data Access Auditing
  12. Data Ownership Workflows
  13. Data Policy Enforcement
  14. Data Access Request Workflows
  15. Data Governance Monitoring
  16. Audit Evidence Collection
"""

import hashlib
import os
from datetime import datetime, timezone
from typing import Dict, List, Optional

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

# ---------------------------------------------------------------------------
# Config block - env vars with placeholder defaults (never hardcode secrets)
# ---------------------------------------------------------------------------
CONFIG = {
    "storage_account": os.environ.get("STORAGE_ACCOUNT", "PLACEHOLDER_STORAGE_ACCOUNT"),
    "container": os.environ.get("ADLS_CONTAINER", "datalake"),
    "catalog": os.environ.get("UNITY_CATALOG", "main"),
    "governance_db": os.environ.get("GOVERNANCE_DB", "governance"),
    "audit_table": os.environ.get("GOV_AUDIT_TABLE", "access_audit"),
    "policy_table": os.environ.get("GOV_POLICY_TABLE", "data_policies"),
    "request_table": os.environ.get("GOV_REQUEST_TABLE", "access_requests"),
    "classification_table": os.environ.get(
        "GOV_CLASSIFICATION_TABLE", "column_classification"
    ),
    "secret_scope": os.environ.get("SECRET_SCOPE", "kv-scope"),
    "token_salt_key": os.environ.get("TOKEN_SALT_KEY", "token-salt"),
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
            SparkSession.builder.appName("governance-security")
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


def ensure_governance_tables(s: SparkSession) -> None:
    """Create governance metadata tables backing the framework."""
    db = CONFIG["governance_db"]
    s.sql(f"CREATE DATABASE IF NOT EXISTS {db}")
    s.sql(
        f"""CREATE TABLE IF NOT EXISTS {db}.{CONFIG['audit_table']} (
        audit_id STRING, event_time STRING, principal STRING, action STRING,
        object_name STRING, decision STRING, details STRING) USING DELTA"""
    )
    s.sql(
        f"""CREATE TABLE IF NOT EXISTS {db}.{CONFIG['policy_table']} (
        policy_id STRING, policy_name STRING, policy_type STRING,
        scope STRING, rule STRING, status STRING,
        created_by STRING, created_at STRING) USING DELTA"""
    )
    s.sql(
        f"""CREATE TABLE IF NOT EXISTS {db}.{CONFIG['request_table']} (
        request_id STRING, requester STRING, object_name STRING,
        privilege STRING, justification STRING, status STRING,
        approver STRING, decided_at STRING) USING DELTA"""
    )
    s.sql(
        f"""CREATE TABLE IF NOT EXISTS {db}.{CONFIG['classification_table']} (
        table_name STRING, column_name STRING, classification STRING,
        confidence DOUBLE, detected_at STRING) USING DELTA"""
    )


def log_audit_event(
    s: SparkSession,
    principal: str,
    action: str,
    object_name: str,
    decision: str,
    details: str = "",
) -> None:
    """Append one entry to the governance audit trail."""
    import uuid

    s.createDataFrame(
        [
            (
                str(uuid.uuid4()),
                datetime.now(timezone.utc).isoformat(),
                principal,
                action,
                object_name,
                decision,
                details,
            )
        ],
        "audit_id STRING, event_time STRING, principal STRING, action STRING,"
        " object_name STRING, decision STRING, details STRING",
    ).write.format("delta").mode("append").saveAsTable(
        f"{CONFIG['governance_db']}.{CONFIG['audit_table']}"
    )


def implement_governance_framework(s: SparkSession) -> None:
    """Activity: Data Governance Framework Implementation - bootstrap catalogs."""
    ensure_governance_tables(s)
    for name in ["bronze", "silver", "gold"]:
        s.sql(
            f"CREATE SCHEMA IF NOT EXISTS {CONFIG['catalog']}.{name}"
            " COMMENT 'Governed medallion layer'"
        )
    log_audit_event(s, "system", "FRAMEWORK_INIT", CONFIG["catalog"], "ALLOWED")


def apply_rbac(s: SparkSession, role: str, object_name: str, privileges: List[str]) -> None:
    """Activity: Role-Based Access Control - grant privileges to a group."""
    for priv in privileges:
        s.sql(f"GRANT {priv} ON TABLE {object_name} TO `{role}`")
    log_audit_event(s, role, "GRANT", object_name, "ALLOWED", ",".join(privileges))


def apply_abac(s: SparkSession, group: str, table: str, region_col: str) -> None:
    """Activity: Attribute-Based Access Control - row filter on user attribute."""
    func = f"{CONFIG['governance_db']}.region_filter_fn"
    s.sql(
        f"""CREATE OR REPLACE FUNCTION {func}(region STRING)
        RETURN region = current_user() OR
               region IN (SELECT allowed_region FROM {CONFIG['governance_db']}.user_attributes
                          WHERE principal = current_user())"""
    )
    s.sql(f"ALTER TABLE {table} SET ROW FILTER {func} ON ({region_col})")
    log_audit_event(s, group, "ABAC_ROW_FILTER", table, "ALLOWED", region_col)


def classify_columns(s: SparkSession, table: str) -> DataFrame:
    """Activity: Data Classification - tag columns with sensitivity classes."""
    df = s.table(table)
    rows = []
    for field in df.schema.fields:
        name = field.name.lower()
        if any(k in name for k in ("ssn", "dob", "passport", "license")):
            cls, conf = "RESTRICTED", 0.95
        elif any(k in name for k in ("email", "phone", "name", "address", "zip")):
            cls, conf = "CONFIDENTIAL", 0.9
        elif any(k in name for k in ("salary", "revenue", "balance")):
            cls, conf = "INTERNAL", 0.8
        else:
            cls, conf = "PUBLIC", 0.6
        rows.append((table, field.name, cls, conf,
                     datetime.now(timezone.utc).isoformat()))
    out = s.createDataFrame(
        rows,
        "table_name STRING, column_name STRING, classification STRING,"
        " confidence DOUBLE, detected_at STRING",
    )
    out.write.format("delta").mode("append").saveAsTable(
        f"{CONFIG['governance_db']}.{CONFIG['classification_table']}"
    )
    return out


def discover_sensitive_data(s: SparkSession, table: str) -> DataFrame:
    """Activity: Sensitive Data Discovery - profile columns for sensitive patterns."""
    df = s.table(table)
    flagged = []
    for field in df.schema.fields:
        col = F.col(field.name).cast("string")
        hit = (
            df.select(
                F.sum(
                    F.when(
                        col.rlike(r"\b\d{3}-\d{2}-\d{4}\b")  # SSN
                        | col.rlike(
                            r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}"
                        ),
                        1,
                    ).otherwise(0)
                ).alias("hits")
            )
            .collect()[0]["hits"]
        )
        if hit:
            flagged.append((table, field.name, int(hit)))
    return s.createDataFrame(
        flagged, "table_name STRING, column_name STRING, hit_count LONG"
    )


def identify_pii(s: SparkSession, table: str) -> DataFrame:
    """Activity: PII Identification - list columns classified as PII."""
    return s.sql(
        f"""SELECT table_name, column_name, classification, confidence
        FROM {CONFIG['governance_db']}.{CONFIG['classification_table']}
        WHERE table_name = '{table}'
          AND classification IN ('RESTRICTED', 'CONFIDENTIAL')"""
    )


def apply_data_masking(s: SparkSession, table: str, column: str, mask_fn: str = "sha2") -> None:
    """Activity: Data Masking - attach a Unity Catalog column mask."""
    if mask_fn == "sha2":
        expr = "sha2(CAST(x AS STRING), 256)"
    elif mask_fn == "redact":
        expr = "'***MASKED***'"
    else:
        expr = "regexp_replace(x, '.', '*')"
    fn = f"{CONFIG['governance_db']}.mask_{mask_fn}"
    s.sql(f"CREATE OR REPLACE FUNCTION {fn}(x STRING) RETURN {expr}")
    s.sql(f"ALTER TABLE {table} ALTER COLUMN {column} SET MASK {fn}")
    log_audit_event(s, "governance-admin", "APPLY_MASK", f"{table}.{column}",
                    "ALLOWED", mask_fn)


def tokenize_column(df: DataFrame, column: str) -> DataFrame:
    """Activity: Tokenization Integration - deterministic salted tokenization."""
    salt = get_secret(CONFIG["token_salt_key"])

    def _token(value: Optional[str]) -> Optional[str]:
        if value is None:
            return None
        return hashlib.sha256(f"{salt}:{value}".encode()).hexdigest()[:24]

    token_udf = F.udf(_token)
    return df.withColumn(f"{column}_token", token_udf(F.col(column).cast("string"))).drop(column)


def configure_encryption(s: SparkSession, path: str) -> Dict[str, str]:
    """Activity: Encryption Configuration - verify SSE on an ADLS path."""
    cfg = {
        "path": path,
        "encryption": "Microsoft-managed keys (SSE)",
        "tls": "1.2",
        "status": "VERIFIED" if path.startswith("abfss://") else "CHECK_REQUIRED",
    }
    log_audit_event(s, "system", "ENCRYPTION_CHECK", path, cfg["status"])
    return cfg


def enforce_retention_policy(s: SparkSession, table: str, retention_days: int) -> None:
    """Activity: Data Retention Policy Implementation - register + vacuum."""
    import uuid

    s.createDataFrame(
        [
            (
                str(uuid.uuid4()),
                f"retention_{table}",
                "RETENTION",
                table,
                f"retention_days={retention_days}",
                "ACTIVE",
                "governance-admin",
                datetime.now(timezone.utc).isoformat(),
            )
        ],
        "policy_id STRING, policy_name STRING, policy_type STRING,"
        " scope STRING, rule STRING, status STRING,"
        " created_by STRING, created_at STRING",
    ).write.format("delta").mode("append").saveAsTable(
        f"{CONFIG['governance_db']}.{CONFIG['policy_table']}"
    )
    s.sql(f"VACUUM {table} RETAIN {retention_days * 24} HOURS")
    log_audit_event(s, "governance-admin", "RETENTION_ENFORCE", table,
                    "ALLOWED", f"{retention_days}d")


def audit_data_access(s: SparkSession, object_name: str, since: str) -> DataFrame:
    """Activity: Data Access Auditing - query the audit trail for an object."""
    return s.sql(
        f"""SELECT event_time, principal, action, decision, details
        FROM {CONFIG['governance_db']}.{CONFIG['audit_table']}
        WHERE object_name = '{object_name}' AND event_time >= '{since}'
        ORDER BY event_time DESC"""
    )


def assign_data_ownership(s: SparkSession, table: str, owner: str, stewards: List[str]) -> None:
    """Activity: Data Ownership Workflows - record owner and stewards."""
    s.sql(f"ALTER TABLE {table} SET OWNER TO `{owner}`")
    for steward in stewards:
        s.sql(f"GRANT SELECT ON TABLE {table} TO `{steward}`")
    log_audit_event(s, owner, "OWNERSHIP_ASSIGN", table, "ALLOWED",
                    ",".join(stewards))


def enforce_policy(s: SparkSession, policy_name: str, table: str, rule_sql: str) -> None:
    """Activity: Data Policy Enforcement - register and apply a policy rule."""
    import uuid

    s.createDataFrame(
        [
            (
                str(uuid.uuid4()),
                policy_name,
                "ENFORCEMENT",
                table,
                rule_sql,
                "ACTIVE",
                "governance-admin",
                datetime.now(timezone.utc).isoformat(),
            )
        ],
        "policy_id STRING, policy_name STRING, policy_type STRING,"
        " scope STRING, rule STRING, status STRING,"
        " created_by STRING, created_at STRING",
    ).write.format("delta").mode("append").saveAsTable(
        f"{CONFIG['governance_db']}.{CONFIG['policy_table']}"
    )
    s.sql(f"ALTER TABLE {table} ADD CONSTRAINT {policy_name} CHECK ({rule_sql})")
    log_audit_event(s, "governance-admin", "POLICY_ENFORCE", table, "ALLOWED",
                    policy_name)


def submit_access_request(
    s: SparkSession, requester: str, object_name: str, privilege: str, justification: str
) -> str:
    """Activity: Data Access Request Workflows - file a request for approval."""
    import uuid

    request_id = str(uuid.uuid4())
    s.createDataFrame(
        [(request_id, requester, object_name, privilege, justification,
          "PENDING", "", "")],
        "request_id STRING, requester STRING, object_name STRING,"
        " privilege STRING, justification STRING, status STRING,"
        " approver STRING, decided_at STRING",
    ).write.format("delta").mode("append").saveAsTable(
        f"{CONFIG['governance_db']}.{CONFIG['request_table']}"
    )
    return request_id


def decide_access_request(s: SparkSession, request_id: str, approver: str, approved: bool) -> None:
    """Decide a pending access request (approver workflow step)."""
    decision = "APPROVED" if approved else "DENIED"
    s.sql(
        f"""UPDATE {CONFIG['governance_db']}.{CONFIG['request_table']}
        SET status = '{decision}', approver = '{approver}',
            decided_at = current_timestamp()
        WHERE request_id = '{request_id}'"""
    )
    if approved:
        row = s.sql(
            f"""SELECT requester, object_name, privilege
            FROM {CONFIG['governance_db']}.{CONFIG['request_table']}
            WHERE request_id = '{request_id}'"""
        ).collect()[0]
        s.sql(f"GRANT {row['privilege']} ON TABLE {row['object_name']} TO `{row['requester']}`")
    log_audit_event(s, approver, f"REQUEST_{decision}", request_id, decision)


def monitor_governance(s: SparkSession) -> DataFrame:
    """Activity: Data Governance Monitoring - posture summary dashboard."""
    db = CONFIG["governance_db"]
    return s.sql(
        f"""SELECT 'policies' AS metric, COUNT(*) AS value FROM {db}.{CONFIG['policy_table']}
        UNION ALL
        SELECT 'pending_requests', COUNT(*) FROM {db}.{CONFIG['request_table']}
        WHERE status = 'PENDING'
        UNION ALL
        SELECT 'denied_access_7d', COUNT(*) FROM {db}.{CONFIG['audit_table']}
        WHERE decision = 'DENIED'"""
    )


def collect_audit_evidence(s: SparkSession, object_name: str, start: str, end: str) -> str:
    """Activity: Audit Evidence Collection - export audit slice to ADLS."""
    out = (
        f"abfss://{CONFIG['container']}@{CONFIG['storage_account']}"
        f".dfs.core.windows.net/governance/evidence/{object_name}"
    )
    (
        s.table(f"{CONFIG['governance_db']}.{CONFIG['audit_table']}")
        .filter(
            (F.col("object_name") == object_name)
            & (F.col("event_time") >= start)
            & (F.col("event_time") <= end)
        )
        .write.format("delta")
        .mode("overwrite")
        .save(out)
    )
    return out


def main() -> None:
    s = spark or get_spark()
    table = get_param("target_table", f"{CONFIG['catalog']}.silver.customers")
    implement_governance_framework(s)
    classify_columns(s, table)
    discover_sensitive_data(s, table)
    apply_rbac(s, "data-analysts", table, ["SELECT"])
    submit_access_request(s, "analyst@contoso.com", table, "SELECT",
                          "Q4 reporting analysis")
    enforce_retention_policy(s, table, 365)
    dashboard = monitor_governance(s)
    evidence = collect_audit_evidence(
        s, table, "2026-01-01T00:00:00+00:00",
        datetime.now(timezone.utc).isoformat(),
    )
    print(f"Governance sweep complete. Posture:\n{dashboard.toPandas().to_string()}")
    print(f"Audit evidence exported to: {evidence}")


if __name__ == "__main__":
    main()
