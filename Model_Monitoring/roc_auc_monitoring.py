"""
32. ROC AUC Monitoring: Score Performance by Month and Months-on-Book
======================================================================
Azure Data Engineering Product | PySpark + scikit-learn

PURPOSE
-------
Measure how well each score in PRODUCTION still separates bads from goods,
the way model-monitoring teams track it: for every score field, compute
ROC AUC and PR AUC per application MONTH, against cumulative
months-on-book (MOB) bad definitions - mob_0, mob_1, ..., mob_N, where
mob_k = 1 if the account went bad within its first k months on book.

Pattern (mirrors the reference monitoring implementation):
  1. Pull production data in Spark; derive the MOB targets in Spark SQL.
  2. Group by application Month_Year; for each month cohort, each score
     and each MOB target, compute the curves with scikit-learn:
         fpr, tpr, _        = roc_curve(target, score)
         roc_auc            = auc(fpr, tpr)
         precision, recall  = precision_recall_curve(target, score)
         pr_auc             = auc(recall, precision)
     plus the bad rate (target mean) for the primary score.
  3. Assemble per-score monthly result tables, convert back to Spark and
     append to the combined ROC AUC metrics table with a business date.

Spark does the heavy lifting (read, join-ready data, MOB derivation, one
month at a time); only one month cohort at a time is collected to pandas
for sklearn, so memory stays bounded by cohort size, not by table size.

INPUTS REQUIRED
---------------
  1. Production data - scored applications/accounts with:
       - one or more score columns,
       - an application date column,
       - a first-bad date column (first fraud / default date; null if
         the account never went bad).
  2. List of input fields - the score columns to evaluate.

OUTPUT: one row per (month, score, MOB window): roc_auc, pr_auc, gini,
ks, bad_rate, population, bads - printed as per-score monthly tables and
optionally appended to a Delta metrics table.

Run: spark-submit 32_roc_auc_monitoring.py
Demo: DEMO_MODE=1 spark-submit 32_roc_auc_monitoring.py
"""

import json
import os
from typing import Any, Dict, List, Tuple

import pandas as pd
# --- ROC AUC curve imports (scikit-learn), as in the reference script ---
from sklearn.metrics import auc, precision_recall_curve, roc_curve

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.functions import col, when

# ---------------------------------------------------------------------------
# CONFIGURATION (defaults only - the real inputs are function arguments)
# ---------------------------------------------------------------------------
CONFIG = {
    "production_source": json.loads(os.environ.get(
        "ROC_PROD_SOURCE_JSON",
        '{"type": "hive", "table": "risk.model_monitoring"}')),
    # The required input: the list of score fields to evaluate
    "score_fields": json.loads(os.environ.get(
        "ROC_SCORE_FIELDS_JSON",
        '["glbscore", "fcrascore", "ea_score"]')),
    # The score whose result rows also carry the bad rate (reference
    # pattern records bad rate alongside the primary model)
    "primary_score": os.environ.get("ROC_PRIMARY_SCORE", "glbscore"),
    "application_date_column": os.environ.get(
        "ROC_APPLICATION_DATE", "applicationdate"),
    "first_bad_date_column": os.environ.get(
        "ROC_FIRST_BAD_DATE", "first_frauddate"),
    "max_mob": int(os.environ.get("ROC_MAX_MOB", "19")),
    # Valid score range (the reference pattern filters scores to 0..1000)
    "score_min": 0.0,
    "score_max": 1000.0,
    "suite": os.environ.get("ROC_SUITE", "production"),  # result label
    # Optional Delta metrics table for the combined results; "" = print only
    "output_path": os.environ.get("ROC_OUTPUT_PATH", ""),
}


# ---------------------------------------------------------------------------
# SPARK SESSION + READING (same conventions as modules 30/31)
# ---------------------------------------------------------------------------
def get_spark(app_name: str = "roc-auc-monitoring") -> SparkSession:
    """Create (or reuse) a Hive-enabled Spark session."""
    try:
        active = SparkSession.getActiveSession()
        if active is not None:
            return active
    except Exception:
        pass
    return (SparkSession.builder.appName(app_name)
            .enableHiveSupport()
            .getOrCreate())


def read_dataset(spark: SparkSession, source: Dict[str, Any]) -> DataFrame:
    """Read the production dataset from its source dict."""
    stype = source["type"].lower()
    if stype == "hive":
        return spark.table(source["table"])
    if stype == "jdbc":
        return (spark.read.format("jdbc")
                .option("url", os.environ["JDBC_URL"])
                .option("dbtable", source["table"])
                .option("user", os.environ.get("JDBC_USER", ""))
                .option("password", os.environ.get("JDBC_PASSWORD", ""))
                .load())
    if stype in ("delta", "parquet", "csv", "json"):
        return (spark.read.format(stype)
                .options(**source.get("options", {})).load(source["path"]))
    raise ValueError(f"unsupported source type: {stype}")


# ===========================================================================
# MOB TARGET DERIVATION (the reference pattern, generalized)
# ===========================================================================
def derive_mob_targets(
    df: DataFrame, application_date_column: str,
    first_bad_date_column: str, max_mob: int = 19,
) -> Tuple[DataFrame, List[str]]:
    """Build the cumulative months-on-book bad flags mob_0 .. mob_N.

    mob (months on book at first bad) =
        ceil(datediff(first_bad_date, application_date) / (365/12))
    for accounts whose first bad date is on/after application; null when
    the account never went bad.

    mob_k = 1 when 0 <= mob <= k AND the bad month is fully observed
    (trunc(first_bad_date, 'MM') <= trunc(current_date, 'MM')) - an
    account cannot be counted bad in a month that has not closed yet.
    Also adds Month_Year ('yyyy-MM' of application) for cohort grouping.
    Returns (df_with_flags, [mob_0 ... mob_N]).
    """
    app, bad = col(application_date_column), col(first_bad_date_column)
    df = df.withColumn(
        "mob",
        when(bad.isNotNull() & (bad >= app),
             F.ceil(F.datediff(bad, app) / (365.0 / 12.0)).cast("int")))
    current_month = F.trunc(F.current_date(), "MM")
    bad_month = F.trunc(bad, "MM")
    targets = []
    for k in range(max_mob + 1):
        name = f"mob_{k}"
        df = df.withColumn(
            name,
            when(col("mob").between(0, k) & (bad_month <= current_month), 1)
            .otherwise(0))
        targets.append(name)
    df = df.withColumn("Month_Year",
                       F.date_format(app, "yyyy-MM"))
    return df, targets


# ===========================================================================
# SKLEARN CURVE COMPUTATION (per month cohort, on pandas)
# ===========================================================================
def compute_score_aucs(
    pdf: pd.DataFrame, score_fields: List[str], target_columns: List[str],
    primary_score: str, score_min: float = 0.0, score_max: float = 1000.0,
) -> List[Dict[str, Any]]:
    """ROC AUC + PR AUC for every score x MOB target in ONE month cohort.

    Directly follows the reference computation:
        fpr, tpr, _       = roc_curve(target, score)
        roc_auc           = auc(fpr, tpr)
        precision, recall = precision_recall_curve(target, score)
        pr_auc            = auc(recall, precision)
    Gini (= 2*AUC - 1) and KS (= max TPR - FPR) come from the same curve.
    Rows are dropped per score when the score is null or outside the
    valid range; a target with no bads (or no goods) in the cohort is
    skipped - AUC is undefined there. The bad rate (target mean) is
    recorded on the primary score's rows, as in the reference.
    """
    rows: List[Dict[str, Any]] = []
    for score in score_fields:
        if score not in pdf.columns:
            continue
        group = pdf.dropna(subset=[score])
        group = group[(group[score] >= score_min)
                      & (group[score] <= score_max)]
        if group.empty:
            continue
        features = group[score].astype(float).values
        for target_col in target_columns:
            target = group[target_col].astype(int).values
            bads = int(target.sum())
            if bads == 0 or bads == len(target):
                continue  # AUC undefined with a single class present
            fpr, tpr, _ = roc_curve(target, features)
            roc_auc = auc(fpr, tpr)
            precision, recall, _ = precision_recall_curve(target, features)
            pr_auc = auc(recall, precision)
            rows.append({
                "Model": score,
                "target": target_col,
                "roc_auc": round(float(roc_auc), 4),
                "pr_auc": round(float(pr_auc), 4),
                "gini": round(2 * float(roc_auc) - 1, 4),
                "ks": round(float((tpr - fpr).max()), 4),
                "bad_rate": (round(float(target.mean()), 4)
                             if score == primary_score else None),
                "population": int(len(target)),
                "bads": bads,
            })
    return rows


def calculate_roc_auc(
    df: DataFrame, score_fields: List[str], target_columns: List[str],
    primary_score: str, config: Dict[str, Any] = CONFIG,
) -> pd.DataFrame:
    """Loop month cohorts (Spark) -> sklearn AUCs (pandas) -> results.

    All inputs are explicit arguments: the production DataFrame (already
    carrying the MOB targets and Month_Year), the score field list, the
    MOB target columns, and the primary score for bad-rate reporting.
    Returns a pandas DataFrame in long format with a Month_Year column.
    """
    months = sorted(r["Month_Year"] for r in
                    df.select("Month_Year").distinct().collect())
    print(f"[ROC] cohorts: {months}; scores: {score_fields}")
    all_rows: List[Dict[str, Any]] = []
    for month in months:
        pdf = (df.filter(col("Month_Year") == month)
               .select(*score_fields, *target_columns).toPandas())
        rows = compute_score_aucs(pdf, score_fields, target_columns,
                                  primary_score, config["score_min"],
                                  config["score_max"])
        for r in rows:
            r["Month_Year"] = month
        all_rows.extend(rows)
        print(f"[ROC] {month}: {len(rows)} score/target AUCs computed")
    return pd.DataFrame(all_rows)


def print_auc_tables(results: pd.DataFrame) -> None:
    """Print per-score monthly tables, wide like the reference output:
    months as rows, MOB windows as columns, one table per score."""
    if results.empty:
        print("[ROC] no results - no cohort had both classes present")
        return
    for metric in ("roc_auc", "pr_auc"):
        print(f"\n================ {metric.upper()} BY SCORE ================")
        for score, sub in results.groupby("Model"):
            wide = sub.pivot_table(index="Month_Year", columns="target",
                                   values=metric)
            print(f"\nModel: {score}")
            print(wide.to_string(float_format=lambda v: f"{v:.4f}"))


def run_roc_auc(
    spark: SparkSession, production_source: Dict[str, Any],
    score_fields: List[str], application_date_column: str,
    first_bad_date_column: str, config: Dict[str, Any] = CONFIG,
) -> pd.DataFrame:
    """End-to-end ROC AUC monitoring run.

    The required inputs are explicit arguments: the production source,
    the score field list, and the two date columns the MOB derivation
    needs. Optionally appends the combined results to the Delta metrics
    table (with the run's business date), mirroring the reference's
    insert into its combined_roc_auc table.
    """
    df = read_dataset(spark, production_source)
    for s in score_fields:  # scores arrive as strings from some sources
        df = df.withColumn(s, col(s).cast("double"))
    df, targets = derive_mob_targets(df, application_date_column,
                                     first_bad_date_column,
                                     config["max_mob"])
    df = df.cache()
    results = calculate_roc_auc(df, score_fields, targets,
                                config["primary_score"], config)
    print_auc_tables(results)

    if config.get("output_path") and not results.empty:
        out = results.copy()
        out["Origination_Suite"] = config["suite"]
        sdf = spark.createDataFrame(out)
        sdf = sdf.withColumn("business_dt", F.current_date())
        (sdf.write.format("delta").mode("append")
         .save(config["output_path"]))
        print(f"[ROC] combined results appended to "
              f"{config['output_path']}")
    return results


# ===========================================================================
# DEMO DATA (DEMO_MODE=1)
# ===========================================================================
def make_demo_dataset(spark: SparkSession) -> DataFrame:
    """Synthetic scored applications across 6 application months.

    Three scores with different strengths (glbscore strong, fcrascore
    medium, ea_score weak) and first_frauddate set for bad accounts a
    random 0-14 months after application - so the MOB AUC tables show
    realistic separation and decay patterns.
    """
    base = spark.range(48_000).withColumn(
        "applicationdate",
        F.to_date(F.concat(F.lit("2025-0"),
                           (col("id") % 6 + 1).cast("string"),
                           F.lit("-15"))))
    latent = F.randn(7)  # unobserved risk driver the scores partly see
    df = (base
          .withColumn("glbscore", F.round(
              620 + latent * 60 + F.randn(8) * 45).cast("int"))
          .withColumn("fcrascore", F.round(
              640 + latent * 40 + F.randn(9) * 70).cast("int"))
          .withColumn("ea_score", F.round(
              600 + latent * 20 + F.randn(10) * 95).cast("int"))
          .withColumn("_is_bad",
                      when(F.rand(11) < F.lit(0.02) + latent * 0.012, 1)
                      .otherwise(0))
          .withColumn("first_frauddate",
                      when(col("_is_bad") == 1,
                           F.date_add(col("applicationdate"),
                                      (F.rand(12) * 420).cast("int"))))
          .drop("id", "_is_bad"))
    return df


# ===========================================================================
# MAIN
# ===========================================================================
def main() -> None:
    """ROC AUC run: the required inputs stated explicitly."""
    spark = get_spark()

    # --- the required inputs ---
    score_fields = CONFIG["score_fields"]          # list of input fields
    application_date_column = CONFIG["application_date_column"]
    first_bad_date_column = CONFIG["first_bad_date_column"]

    if os.environ.get("DEMO_MODE") == "1":
        print("[main] DEMO_MODE - synthetic production data")
        df = make_demo_dataset(spark)
        for s in score_fields:
            df = df.withColumn(s, col(s).cast("double"))
        df, targets = derive_mob_targets(df, application_date_column,
                                         first_bad_date_column,
                                         CONFIG["max_mob"])
        results = calculate_roc_auc(df, score_fields, targets,
                                    CONFIG["primary_score"])
        print_auc_tables(results)
    else:
        run_roc_auc(
            spark,
            production_source=CONFIG["production_source"],
            score_fields=score_fields,
            application_date_column=application_date_column,
            first_bad_date_column=first_bad_date_column)
    spark.stop()


if __name__ == "__main__":
    main()
