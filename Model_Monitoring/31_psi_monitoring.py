"""
31. PSI Monitoring: Population Stability Index by Quarter
==========================================================
Azure Data Engineering Product | PySpark

PURPOSE
-------
Monitor whether the data a model / rule set sees in PRODUCTION still looks
like the data it was built on. For every input field, this module computes
the Population Stability Index (PSI) of each production QUARTER against
the OOT (out-of-time) baseline dataset:

    PSI = SUM over bins of  (Actual% - Expected%) * ln(Actual% / Expected%)

    Expected% = the field's distribution in the OOT baseline
    Actual%   = the field's distribution in one production quarter

Standard interpretation (credit-risk / model-risk convention):
    PSI < 0.10          STABLE      - no significant population change
    0.10 <= PSI < 0.25  MODERATE    - shift detected, investigate
    PSI >= 0.25         SIGNIFICANT - major shift, action required

INPUTS REQUIRED
---------------
  1. OOT dataset        - the baseline (model-development / out-of-time)
                          data whose distribution is "expected".
  2. Production dataset - the live data to check, with a date column so
                          it can be split into quarters.
  3. Input fields       - the list of fields to monitor. Numeric fields
                          are binned on OOT quantiles; categorical fields
                          use their OOT categories (new production values
                          fall into an __OTHER__ bucket).

All three inputs are EXPLICIT function arguments (see run_psi_monitoring);
CONFIG below only supplies thresholds, binning and source defaults.

Run: spark-submit 31_psi_monitoring.py
Demo (synthetic drift): DEMO_MODE=1 spark-submit 31_psi_monitoring.py
"""

import json
import math
import os
from typing import Any, Dict, List, Tuple

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.functions import col, when

# ---------------------------------------------------------------------------
# CONFIGURATION (defaults only - the real inputs are function arguments)
# ---------------------------------------------------------------------------
CONFIG = {
    # Sources use the same dict format as module 30:
    # {"type": "hive"|"jdbc"|"delta"|"parquet"|"csv"|"json", "table"/"path": ...}
    "oot_source": json.loads(os.environ.get(
        "PSI_OOT_SOURCE_JSON",
        '{"type": "hive", "table": "risk.model_oot"}')),
    "production_source": json.loads(os.environ.get(
        "PSI_PROD_SOURCE_JSON",
        '{"type": "hive", "table": "risk.production"}')),
    # Defaults for the required inputs (callers may pass their own):
    "input_fields": json.loads(os.environ.get(
        "PSI_FIELDS_JSON",
        '["amount", "hour_of_day", "txn_count_1h", "device_age_days",'
        ' "country", "merchant_category"]')),
    "categorical_fields": ["country", "merchant_category"],
    "date_column": os.environ.get("PSI_DATE_COLUMN", "event_date"),
    # Binning + scoring controls
    "num_bins": 10,               # quantile bins per numeric field (deciles)
    "smoothing": 0.5,             # count smoothing so empty bins never log(0)
    "moderate_threshold": 0.10,   # PSI >= this  -> MODERATE
    "significant_threshold": 0.25,  # PSI >= this -> SIGNIFICANT
    # Optional: persist the report (Delta path); empty = print only
    "output_path": os.environ.get("PSI_OUTPUT_PATH", ""),
}


# ---------------------------------------------------------------------------
# SPARK SESSION
# ---------------------------------------------------------------------------
def get_spark(app_name: str = "psi-monitoring") -> SparkSession:
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


# ---------------------------------------------------------------------------
# DATA READING (same source-dict convention as module 30)
# ---------------------------------------------------------------------------
def read_dataset(spark: SparkSession, source: Dict[str, Any]) -> DataFrame:
    """Read one dataset (OOT or production) from its source dict."""
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


def add_quarter_column(df: DataFrame, date_column: str,
                       quarter_col: str = "quarter_label") -> DataFrame:
    """Derive a sortable quarter label like '2025Q3' from a date column."""
    return df.withColumn(
        quarter_col,
        F.concat(F.year(col(date_column)).cast("string"), F.lit("Q"),
                 F.quarter(col(date_column)).cast("string")))


# ===========================================================================
# PSI CORE (pure Python - the math, independently unit-testable)
# ===========================================================================
def calculate_psi(expected_counts: List[float], actual_counts: List[float],
                  smoothing: float = 0.5) -> Tuple[float, List[float]]:
    """PSI from aligned per-bin counts of baseline vs actual.

        PSI = SUM (a% - e%) * ln(a% / e%)

    `smoothing` is added to every bin count before converting to
    proportions, so a bin that is empty on one side yields a large but
    finite contribution instead of log(0). Returns (psi, per-bin
    contributions) - the contributions identify WHICH bins drifted.
    """
    if len(expected_counts) != len(actual_counts):
        raise ValueError("expected/actual bin counts must align")
    n = len(expected_counts)
    e_total = sum(expected_counts) + smoothing * n
    a_total = sum(actual_counts) + smoothing * n
    psi, contributions = 0.0, []
    for e, a in zip(expected_counts, actual_counts):
        e_pct = (e + smoothing) / e_total
        a_pct = (a + smoothing) / a_total
        contrib = (a_pct - e_pct) * math.log(a_pct / e_pct)
        contributions.append(contrib)
        psi += contrib
    return psi, contributions


def interpret_psi(psi: float, moderate: float = 0.10,
                  significant: float = 0.25) -> str:
    """Map a PSI value to its standard monitoring status."""
    if psi >= significant:
        return "SIGNIFICANT"
    if psi >= moderate:
        return "MODERATE"
    return "STABLE"


def bucket_index_expression(field: str, edges: List[float]):
    """Spark expression: which bin a numeric value falls into.

    Bin i = number of edges the value reaches, so bins run 0..len(edges)
    with the outer bins open-ended (-inf..first edge, last edge..+inf) -
    production values outside the OOT range still land in a bin.
    """
    bucket = F.lit(0)
    for e in edges:
        bucket = bucket + when(col(field) >= e, 1).otherwise(0)
    return bucket


# ===========================================================================
# PER-FIELD PSI
# ===========================================================================
def _counts_by_key(df: DataFrame, key_col: str) -> Dict[Any, int]:
    """{key: row_count} for one grouping column, collected to the driver."""
    return {r["k"]: int(r["n"]) for r in
            df.groupBy(col(key_col).alias("k"))
              .agg(F.count("*").alias("n")).collect()}


def psi_for_numeric_field(
    oot_df: DataFrame, prod_quarter_df: DataFrame, field: str,
    num_bins: int = 10, smoothing: float = 0.5,
) -> Tuple[float, List[Dict[str, Any]]]:
    """PSI for one numeric field in one production quarter.

    Bins are the OOT quantile edges (deciles by default) - the baseline
    defines the bins, production is measured against them. Bins with no
    production rows still count via smoothing.
    """
    edges = oot_df.approxQuantile(
        field, [i / num_bins for i in range(1, num_bins)], 0.01)
    edges = sorted(set(float(e) for e in edges))
    if not edges:  # constant field in OOT - nothing to monitor
        return 0.0, []
    oot_counts = _counts_by_key(
        oot_df.withColumn("_bin", bucket_index_expression(field, edges)),
        "_bin")
    prod_counts = _counts_by_key(
        prod_quarter_df.withColumn(
            "_bin", bucket_index_expression(field, edges)), "_bin")
    keys = list(range(len(edges) + 1))
    psi, contribs = calculate_psi(
        [oot_counts.get(k, 0) for k in keys],
        [prod_counts.get(k, 0) for k in keys], smoothing)
    bounds = [float("-inf")] + edges + [float("inf")]
    detail = [{"bin": f"[{bounds[i]}, {bounds[i + 1]})",
               "expected_count": oot_counts.get(k, 0),
               "actual_count": prod_counts.get(k, 0),
               "psi_contribution": contribs[i]}
              for i, k in enumerate(keys)]
    return psi, detail


def psi_for_categorical_field(
    oot_df: DataFrame, prod_quarter_df: DataFrame, field: str,
    smoothing: float = 0.5,
) -> Tuple[float, List[Dict[str, Any]]]:
    """PSI for one categorical field in one production quarter.

    Bins are the categories seen in OOT; any production value NOT in OOT
    is grouped into '__OTHER__' - a growing OTHER share is itself a drift
    signal (new countries, new merchants, upstream data changes).
    """
    oot_counts = _counts_by_key(oot_df.withColumn(
        "_cat", col(field).cast("string")), "_cat")
    known = list(oot_counts.keys())
    prod_grouped = prod_quarter_df.withColumn(
        "_cat", when(col(field).cast("string").isin(known),
                     col(field).cast("string")).otherwise("__OTHER__"))
    prod_counts = _counts_by_key(prod_grouped, "_cat")
    keys = known + ["__OTHER__"]
    psi, contribs = calculate_psi(
        [oot_counts.get(k, 0) for k in keys],
        [prod_counts.get(k, 0) for k in keys], smoothing)
    detail = [{"bin": str(k),
               "expected_count": oot_counts.get(k, 0),
               "actual_count": prod_counts.get(k, 0),
               "psi_contribution": contribs[i]}
              for i, k in enumerate(keys)]
    return psi, detail


# ===========================================================================
# QUARTERLY MONITORING - THE MAIN COMPUTATION
# ===========================================================================
def calculate_quarterly_psi(
    oot_df: DataFrame, production_df: DataFrame,
    input_fields: List[str], categorical_fields: List[str],
    date_column: str, config: Dict[str, Any] = CONFIG,
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    """PSI for every input field in every production quarter vs OOT.

    All inputs are explicit arguments: the two datasets, the field list,
    which fields are categorical, and the production date column used to
    form quarters. Returns (summary_rows, bin_detail_rows):
      summary: one row per field x quarter {field, field_type, quarter,
               psi, status}
      detail : one row per bin {field, quarter, bin, expected_count,
               actual_count, psi_contribution} for drill-down.
    """
    prod = add_quarter_column(production_df, date_column)
    quarters = sorted(r["quarter_label"] for r in
                      prod.select("quarter_label").distinct().collect())
    print(f"[PSI] baseline OOT rows: {oot_df.count()}; "
          f"production quarters: {quarters}")

    summary: List[Dict[str, Any]] = []
    detail_all: List[Dict[str, Any]] = []
    for quarter in quarters:
        q_df = prod.filter(col("quarter_label") == quarter).cache()
        for field in input_fields:
            if field in categorical_fields:
                psi, detail = psi_for_categorical_field(
                    oot_df, q_df, field, config["smoothing"])
                ftype = "categorical"
            else:
                psi, detail = psi_for_numeric_field(
                    oot_df, q_df, field, config["num_bins"],
                    config["smoothing"])
                ftype = "numeric"
            status = interpret_psi(psi, config["moderate_threshold"],
                                   config["significant_threshold"])
            summary.append({"field": field, "field_type": ftype,
                            "quarter": quarter, "psi": round(psi, 4),
                            "status": status})
            for d in detail:
                detail_all.append({"field": field, "quarter": quarter,
                                   **d})
        q_df.unpersist()
    return summary, detail_all


def print_psi_report(summary: List[Dict[str, Any]]) -> None:
    """Human-readable report: worst drift first, flags on bad quarters."""
    print("\n================ PSI REPORT (vs OOT baseline) ================")
    print(f"{'field':<20}{'quarter':<10}{'PSI':>8}  status")
    for r in sorted(summary, key=lambda x: -x["psi"]):
        flag = "  <-- ACTION" if r["status"] == "SIGNIFICANT" else (
            "  <-- investigate" if r["status"] == "MODERATE" else "")
        print(f"{r['field']:<20}{r['quarter']:<10}{r['psi']:>8.4f}  "
              f"{r['status']}{flag}")
    bad = [r for r in summary if r["status"] != "STABLE"]
    print(f"---- {len(summary)} field-quarters checked, "
          f"{len(bad)} not stable ----")


def run_psi_monitoring(
    spark: SparkSession, oot_source: Dict[str, Any],
    production_source: Dict[str, Any], input_fields: List[str],
    categorical_fields: List[str], date_column: str,
    config: Dict[str, Any] = CONFIG,
) -> List[Dict[str, Any]]:
    """End-to-end: read both datasets -> quarterly PSI -> report (+save).

    The three required inputs are explicit arguments: the OOT source, the
    production source, and the input field list (with its categorical
    subset and the production date column).
    """
    oot_df = read_dataset(spark, oot_source).cache()
    prod_df = read_dataset(spark, production_source).cache()
    summary, detail = calculate_quarterly_psi(
        oot_df, prod_df, input_fields, categorical_fields, date_column,
        config)
    print_psi_report(summary)

    if config.get("output_path"):
        (spark.createDataFrame(summary)
         .write.format("delta").mode("append")
         .save(config["output_path"]))
        (spark.createDataFrame(detail)
         .write.format("delta").mode("append")
         .save(config["output_path"] + "_detail"))
        print(f"[PSI] report saved to {config['output_path']} "
              f"(+ _detail)")
    return summary


# ===========================================================================
# DEMO DATA (DEMO_MODE=1) - stable fields + drift baked into later quarters
# ===========================================================================
def make_demo_datasets(spark: SparkSession) -> Tuple[DataFrame, DataFrame]:
    """OOT baseline (2024) + production (2025 Q1-Q4) with known drift.

    Drift design: `amount` shifts up from Q3, `country` mix changes in Q4,
    `merchant_category` stays stable all year - the report should show
    exactly that pattern.
    """
    def frame(n: int, seed: int, amount_shift: float, risky_share: float,
              year: int, month: int) -> DataFrame:
        rnd = F.rand(seed)
        return (spark.range(n)
                .withColumn("event_date",
                            F.to_date(F.lit(f"{year}-{month:02d}-15")))
                .withColumn("amount", F.round(
                    F.exp(F.randn(seed + 1) * 0.9 + 3.5 + amount_shift), 2))
                .withColumn("hour_of_day", (rnd * 24).cast("int"))
                .withColumn("txn_count_1h", (F.pow(rnd, 3) * 12).cast("int"))
                .withColumn("device_age_days",
                            (F.pow(F.rand(seed + 2), 2) * 900).cast("int"))
                .withColumn("country",
                            when(rnd < 1 - risky_share, "US")
                            .when(rnd < 1 - risky_share / 2, "GB")
                            .otherwise("NG"))
                .withColumn("merchant_category",
                            when(rnd < 0.5, "grocery")
                            .when(rnd < 0.8, "retail").otherwise("travel"))
                .drop("id"))

    oot = frame(40_000, 11, 0.0, 0.10, 2024, 6)
    prod = None
    #            quarter month, amount drift, risky-country share
    quarters = [(1, 0.00, 0.10), (4, 0.05, 0.11),
                (7, 0.35, 0.13), (10, 0.45, 0.30)]
    for i, (month, shift, risky) in enumerate(quarters):
        q = frame(15_000, 21 + i, shift, risky, 2025, month)
        prod = q if prod is None else prod.unionByName(q)
    return oot, prod


# ===========================================================================
# MAIN
# ===========================================================================
def main() -> None:
    """PSI monitoring run: the three required inputs stated explicitly."""
    spark = get_spark()

    # --- the required inputs ---
    input_fields = CONFIG["input_fields"]        # fields to monitor
    categorical_fields = CONFIG["categorical_fields"]
    date_column = CONFIG["date_column"]          # production quartering

    if os.environ.get("DEMO_MODE") == "1":
        print("[main] DEMO_MODE - synthetic OOT + production data")
        oot_df, prod_df = make_demo_datasets(spark)
        summary, detail = calculate_quarterly_psi(
            oot_df, prod_df, input_fields, categorical_fields, date_column)
        print_psi_report(summary)
        # drill-down example: which bins drive the worst field-quarter
        worst = max(summary, key=lambda r: r["psi"])
        print(f"\nBin detail for worst case "
              f"({worst['field']} {worst['quarter']}, PSI {worst['psi']}):")
        for d in detail:
            if (d["field"] == worst["field"]
                    and d["quarter"] == worst["quarter"]):
                print(f"  {d['bin']:<24} expected {d['expected_count']:>6} "
                      f"actual {d['actual_count']:>6} "
                      f"contribution {d['psi_contribution']:.4f}")
    else:
        run_psi_monitoring(
            spark,
            oot_source=CONFIG["oot_source"],
            production_source=CONFIG["production_source"],
            input_fields=input_fields,
            categorical_fields=categorical_fields,
            date_column=date_column)
    spark.stop()


if __name__ == "__main__":
    main()
