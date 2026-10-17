"""Category 19 - DataOps and CI/CD.

Purpose: DataOps toolkit for Azure Databricks pipelines - git version-control
checks, automated PySpark/SQL test harness, environment management, IaC
validation, deployment automation with approvals and rollback, plus config
and secret management.

Activities covered:
  1. DataOps Framework Implementation
  2. Git-Based Version Control
  3. Pipeline CI/CD Implementation
  4. Automated Pipeline Testing
  5. SQL Transformation Testing
  6. Development and Production Environment Management
  7. Infrastructure as Code Implementation
  8. Automated Deployment
  9. Deployment Approval Workflows
  10. Pipeline Version Management
  11. Rollback Mechanisms
  12. Configuration Management
  13. Secret Management Integration
  14. Release Monitoring
  15. Data Pipeline Development Standards
"""

import json
import os
import subprocess
from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

# ---------------------------------------------------------------------------
# Config block - env vars with placeholder defaults (never hardcode secrets)
# ---------------------------------------------------------------------------
CONFIG = {
    "storage_account": os.environ.get("STORAGE_ACCOUNT", "PLACEHOLDER_STORAGE_ACCOUNT"),
    "container": os.environ.get("ADLS_CONTAINER", "datalake"),
    "catalog": os.environ.get("UNITY_CATALOG", "main"),
    "cicd_db": os.environ.get("CICD_DB", "dataops"),
    "git_branch": os.environ.get("GIT_BRANCH", "main"),
    "environment": os.environ.get("DEPLOY_ENV", "dev"),
    "secret_scope": os.environ.get("SECRET_SCOPE", "kv-scope"),
    "artifact_root": os.environ.get("ARTIFACT_ROOT", "artifacts"),
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
            SparkSession.builder.appName("dataops-cicd")
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


def get_secret(name: str) -> str:
    """Fetch a secret from Databricks secrets with an env fallback."""
    try:
        return dbutils.secrets.get(scope=CONFIG["secret_scope"], key=name)  # type: ignore[name-defined]
    except Exception:
        return os.environ.get(name, "")


# ---------------------------------------------------------------------------
# Version control and standards
# ---------------------------------------------------------------------------

def check_git_status(repo_dir: str = ".") -> Dict[str, object]:
    """Activity: Git-Based Version Control - verify a clean, tagged tree."""
    def run(args: List[str]) -> str:
        return subprocess.run(
            ["git"] + args, cwd=repo_dir, capture_output=True, text=True
        ).stdout.strip()

    try:
        branch = run(["rev-parse", "--abbrev-ref", "HEAD"])
        dirty = bool(run(["status", "--porcelain"]))
        commit = run(["rev-parse", "--short", "HEAD"])
        tag = run(["describe", "--tags", "--abbrev=0"])
        return {"branch": branch, "clean": not dirty, "commit": commit,
                "tag": tag or "untagged"}
    except Exception as exc:
        return {"error": str(exc)}


def lint_pipeline_standards(paths: List[str]) -> List[str]:
    """Activity: Data Pipeline Development Standards - static checks."""
    violations = []
    for path in paths:
        try:
            with open(path) as fh:
                src = fh.read()
        except OSError:
            violations.append(f"{path}: unreadable")
            continue
        if "abfss://" in src and "PLACEHOLDER" not in src and "CONFIG" not in src:
            violations.append(f"{path}: possible hardcoded abfss path")
        for secret_word in ("password", "secret", "api_key"):
            if f'"{secret_word}"' in src.lower() and "getenv" not in src.lower():
                violations.append(f"{path}: possible hardcoded {secret_word}")
        if "def main(" not in src:
            violations.append(f"{path}: missing main() driver")
    return violations


# ---------------------------------------------------------------------------
# Test harness
# ---------------------------------------------------------------------------

TestResult = Tuple[str, bool, str]


def run_pipeline_tests(s: SparkSession) -> List[TestResult]:
    """Activity: Automated Pipeline Testing - smoke tests on test fixtures."""
    results: List[TestResult] = []
    try:
        df = s.createDataFrame([(1, "a"), (2, "b"), (None, "c")], ["id", "val"])
        cleaned = df.filter(F.col("id").isNotNull())
        results.append(("null_filter", cleaned.count() == 2, "drop null ids"))
    except Exception as exc:
        results.append(("null_filter", False, str(exc)))
    try:
        df = s.createDataFrame([(1,), (1,), (2,)], ["id"])
        deduped = df.dropDuplicates(["id"])
        results.append(("dedupe", deduped.count() == 2, "dropDuplicates"))
    except Exception as exc:
        results.append(("dedupe", False, str(exc)))
    try:
        df = s.createDataFrame([("2026-01-01",)], ["d"])
        casted = df.withColumn("d", F.to_date("d"))
        results.append(("type_cast", casted.schema["d"].dataType.simpleString() == "date",
                        "to_date cast"))
    except Exception as exc:
        results.append(("type_cast", False, str(exc)))
    return results


def run_sql_transform_tests(s: SparkSession, tests: List[Dict[str, str]]) -> List[TestResult]:
    """Activity: SQL Transformation Testing - assert expected row counts."""
    results = []
    for t in tests:
        try:
            count = s.sql(t["sql"]).count()
            passed = count == int(t["expected_count"])
            results.append((t["name"], passed,
                            f"expected {t['expected_count']}, got {count}"))
        except Exception as exc:
            results.append((t["name"], False, str(exc)))
    return results


# ---------------------------------------------------------------------------
# Environments, IaC, config, secrets
# ---------------------------------------------------------------------------

ENV_CONFIGS = {
    "dev": {"catalog": "dev_catalog", "retention_days": 7, "cluster": "dev-small"},
    "test": {"catalog": "test_catalog", "retention_days": 30, "cluster": "test-med"},
    "prod": {"catalog": "main", "retention_days": 365, "cluster": "prod-xl"},
}


def manage_environments(s: SparkSession, env: str) -> Dict[str, str]:
    """Activity: Dev/Prod Environment Management - materialize env config."""
    cfg = ENV_CONFIGS.get(env, ENV_CONFIGS["dev"])
    s.sql(f"CREATE DATABASE IF NOT EXISTS {CONFIG['cicd_db']}")
    s.createDataFrame(
        [(env, json.dumps(cfg), datetime.now(timezone.utc).isoformat())],
        "env STRING, config_json STRING, applied_at STRING",
    ).write.format("delta").mode("append").saveAsTable(
        f"{CONFIG['cicd_db']}.environment_configs"
    )
    return cfg


def validate_iac(template_path: str) -> List[str]:
    """Activity: Infrastructure as Code Implementation - validate ARM/Bicep."""
    issues = []
    try:
        with open(template_path) as fh:
            content = fh.read()
    except OSError:
        return [f"{template_path}: not found"]
    for required in ("Microsoft.Storage/storageAccounts",
                     "Microsoft.Databricks/workspaces"):
        if required not in content:
            issues.append(f"missing resource type: {required}")
    if "securestring" not in content.lower() and "secret" in content.lower():
        issues.append("secrets should use secureString parameters")
    return issues


def load_configuration(env: str) -> Dict[str, str]:
    """Activity: Configuration Management - layered config (env > defaults)."""
    cfg = dict(ENV_CONFIGS.get(env, ENV_CONFIGS["dev"]))
    for key in ("storage_account", "container", "catalog"):
        override = os.environ.get(key.upper())
        if override:
            cfg[key] = override
    return cfg


def resolve_secrets(names: List[str]) -> Dict[str, bool]:
    """Activity: Secret Management Integration - verify secrets resolve."""
    return {name: bool(get_secret(name)) for name in names}


# ---------------------------------------------------------------------------
# Deployment: approvals, versions, rollback, monitoring
# ---------------------------------------------------------------------------

def record_pipeline_version(
    s: SparkSession, pipeline: str, version: str, artifact: str
) -> None:
    """Activity: Pipeline Version Management - register a release version."""
    s.sql(f"CREATE DATABASE IF NOT EXISTS {CONFIG['cicd_db']}")
    s.createDataFrame(
        [(pipeline, version, artifact, CONFIG["environment"],
          datetime.now(timezone.utc).isoformat())],
        "pipeline STRING, version STRING, artifact STRING, env STRING,"
        " released_at STRING",
    ).write.format("delta").mode("append").saveAsTable(
        f"{CONFIG['cicd_db']}.pipeline_versions"
    )


def request_deployment_approval(
    s: SparkSession, pipeline: str, version: str, requester: str
) -> str:
    """Activity: Deployment Approval Workflows - open an approval gate."""
    import uuid

    approval_id = str(uuid.uuid4())
    s.sql(f"CREATE DATABASE IF NOT EXISTS {CONFIG['cicd_db']}")
    s.createDataFrame(
        [(approval_id, pipeline, version, requester, "PENDING", "",
          datetime.now(timezone.utc).isoformat())],
        "approval_id STRING, pipeline STRING, version STRING,"
        " requester STRING, status STRING, approver STRING, requested_at STRING",
    ).write.format("delta").mode("append").saveAsTable(
        f"{CONFIG['cicd_db']}.deployment_approvals"
    )
    return approval_id


def approve_deployment(s: SparkSession, approval_id: str, approver: str) -> bool:
    """Approve a pending deployment (approver step of the workflow)."""
    pending = s.sql(
        f"""SELECT COUNT(*) AS c FROM {CONFIG['cicd_db']}.deployment_approvals
        WHERE approval_id = '{approval_id}' AND status = 'PENDING'"""
    ).collect()[0]["c"]
    if not pending:
        return False
    s.sql(
        f"""UPDATE {CONFIG['cicd_db']}.deployment_approvals
        SET status = 'APPROVED', approver = '{approver}'
        WHERE approval_id = '{approval_id}'"""
    )
    return True


def deploy_pipeline(
    s: SparkSession, pipeline: str, version: str, env: str
) -> Dict[str, str]:
    """Activity: Automated Deployment - gate on approval, then release."""
    approvals = s.sql(
        f"""SELECT approval_id FROM {CONFIG['cicd_db']}.deployment_approvals
        WHERE pipeline = '{pipeline}' AND version = '{version}'
          AND status = 'APPROVED'"""
    ).collect()
    if not approvals:
        return {"status": "BLOCKED", "reason": "no approved deployment gate"}
    artifact = f"{base_path()}/{CONFIG['artifact_root']}/{pipeline}/{version}"
    record_pipeline_version(s, pipeline, version, artifact)
    return {"status": "DEPLOYED", "pipeline": pipeline, "version": version,
            "env": env, "artifact": artifact}


def rollback_pipeline(s: SparkSession, pipeline: str, env: str) -> Dict[str, str]:
    """Activity: Rollback Mechanisms - revert to the previous good version."""
    rows = s.sql(
        f"""SELECT version, artifact FROM {CONFIG['cicd_db']}.pipeline_versions
        WHERE pipeline = '{pipeline}' AND env = '{env}'
        ORDER BY released_at DESC LIMIT 2"""
    ).collect()
    if len(rows) < 2:
        return {"status": "FAILED", "reason": "no previous version to roll back to"}
    prev = rows[1]
    record_pipeline_version(s, pipeline, prev["version"] + "-rollback", prev["artifact"])
    return {"status": "ROLLED_BACK", "pipeline": pipeline,
            "version": prev["version"]}


def monitor_release(s: SparkSession, pipeline: str, version: str) -> DataFrame:
    """Activity: Release Monitoring - post-deploy health signals."""
    return s.sql(
        f"""SELECT version, env, released_at
        FROM {CONFIG['cicd_db']}.pipeline_versions
        WHERE pipeline = '{pipeline}' AND version LIKE '{version}%'
        ORDER BY released_at DESC"""
    )


def implement_dataops_framework(s: SparkSession) -> Dict[str, object]:
    """Activity: DataOps Framework Implementation - bootstrap all registers."""
    s.sql(f"CREATE DATABASE IF NOT EXISTS {CONFIG['cicd_db']}")
    git = check_git_status()
    cfg = manage_environments(s, CONFIG["environment"])
    secrets_ok = resolve_secrets(["storage-key", "sql-password"])
    return {"git": git, "environment": CONFIG["environment"],
            "env_config": cfg, "secrets_resolved": secrets_ok}


def run_cicd_pipeline(s: SparkSession, pipeline: str, version: str) -> Dict[str, object]:
    """Activity: Pipeline CI/CD Implementation - full gate sequence."""
    results: Dict[str, object] = {}
    results["git"] = check_git_status()
    results["standards"] = lint_pipeline_standards([__file__])
    results["unit_tests"] = run_pipeline_tests(s)
    results["sql_tests"] = run_sql_transform_tests(
        s, [{"name": "smoke", "sql": "SELECT 1 AS x", "expected_count": "1"}]
    )
    failed = [t for t in results["unit_tests"] + results["sql_tests"] if not t[1]]  # type: ignore[operator]
    if failed or results["standards"]:
        results["verdict"] = "FAILED"
        return results
    approval_id = request_deployment_approval(s, pipeline, version, "cicd-bot")
    results["approval_id"] = approval_id
    results["verdict"] = "AWAITING_APPROVAL"
    return results


def main() -> None:
    s = spark or get_spark()
    pipeline = get_param("pipeline", "sales_silver_pipeline")
    version = get_param("version", "1.4.0")

    bootstrap = implement_dataops_framework(s)
    print(f"DataOps bootstrap: {json.dumps(bootstrap, indent=2, default=str)}")
    outcome = run_cicd_pipeline(s, pipeline, version)
    print(f"CI verdict: {outcome['verdict']}")
    if outcome["verdict"] == "AWAITING_APPROVAL":
        if approve_deployment(s, outcome["approval_id"], "release-manager"):  # type: ignore[arg-type]
            deploy = deploy_pipeline(s, pipeline, version, CONFIG["environment"])
            print(f"Deployment: {json.dumps(deploy, indent=2)}")
            print(monitor_release(s, pipeline, version).toPandas().to_string())


if __name__ == "__main__":
    main()
