"""
33. Distribution Charts: Score Distribution by Percentile Bin
==============================================================
Azure Data Engineering Product | PySpark + pandas

PURPOSE
-------
Build the classic model-monitoring DISTRIBUTION CHART table for every
score: split the population into percentile bins (100 by default) ordered
by risk, and for each bin report the score range, population, bads,
goods, bad rate, share of all fraud, cumulative fraud captured, and the
good-to-bad odds - overall and per month. This is the table behind the
distribution chart: a healthy score shows bad rate falling smoothly
from bin 1 (riskiest) to bin N, and cumulative fraud capture rising
steeply - when that shape flattens or inverts, the score is decaying.

Mirrors the reference generate_distribution_chart() pattern:
  - scores are RANKED (method='first') to break ties, direction set by
    higher_is_riskier, then cut into percentile bins 1..bins with qcut;
  - bin 1 is always the RISKIEST end of the score;
  - all ratios use safe (zero-guarded) divisions;
  - an optional scope column carries the month value, or 'ALL'.

Spark reads and slices the production data (per month, per score);
the binning runs on one collected cohort at a time in pandas, exactly
like the reference - memory stays bounded by cohort size.

INPUTS REQUIRED
---------------
  1. Production data - scored records with a binary label column.
  2. List of input fields - the score columns to chart.
  3. Label column - the bad flag (e.g. fraudflag, or a mob_k target
     produced by module 32).
  Optional: a month column - charts are then produced per month too.

OUTPUT: one distribution table per (scope, score) with columns
  [scope] Percentile, Min, Max, total_cnt, bads, goods,
  bad_rate_per_bin, overall_fraud_pct_per_bin, cum_fraud_pct_per_bin,
  good_to_bad_ratio_per_bin, cum_good_to_bad_ratio
plus optional PNG charts (bad rate by bin + cumulative capture curve)
and an optional Delta table append.

Run: spark-submit 33_distribution_charts.py
Demo: DEMO_MODE=1 spark-submit 33_distribution_charts.py
"""

import json
import os
from typing import Any, Dict, List, Optional

import numpy as np
import pandas as pd

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.functions import col, when

# ---------------------------------------------------------------------------
# CONFIGURATION (defaults only - the real inputs are function arguments)
# ---------------------------------------------------------------------------
CONFIG = {
    "production_source": json.loads(os.environ.get(
        "DIST_PROD_SOURCE_JSON",
        '{"type": "hive", "table": "risk.model_monitoring"}')),
    # The required inputs: score fields + label column
    "score_fields": json.loads(os.environ.get(
        "DIST_SCORE_FIELDS_JSON", '["glbscore", "fcrascore", "ea_score"]')),
    "label_column": os.environ.get("DIST_LABEL_COLUMN", "fraudflag"),
    "month_column": os.environ.get("DIST_MONTH_COLUMN", "Month_Year"),
    "bins": int(os.environ.get("DIST_BINS", "100")),
    "higher_is_riskier": True,   # bin 1 = highest scores when True
    # Optional outputs; empty = print only
    "output_path": os.environ.get("DIST_OUTPUT_PATH", ""),
    "chart_dir": os.environ.get("DIST_CHART_DIR", ""),
}


# ---------------------------------------------------------------------------
# SPARK SESSION + READING (same conventions as modules 30-32)
# ---------------------------------------------------------------------------
def get_spark(app_name: str = "distribution-charts") -> SparkSession:
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
# CORE: ONE DISTRIBUTION CHART TABLE (pandas - mirrors the reference)
# ===========================================================================
def generate_distribution_chart(
    df: pd.DataFrame,
    score_col: str,
    label_col: str,
    month_year_value: Optional[str] = None,
    bins: int = 100,
    month_col: Optional[str] = None,
    higher_is_riskier: bool = True,
    include_scope_col: bool = True,
    scope_col_name: str = "Scope",
    scope_all_value: str = "ALL",
) -> pd.DataFrame:
    """Distribution chart table for ONE score in ONE pandas frame.

    Same computation as the reference implementation:
      1. optionally filter to one month value;
      2. drop rows missing score or label;
      3. rank scores (ties broken by order) so binning is stable and
         direction is controlled by higher_is_riskier;
      4. qcut the ranks into percentile bins 1..bins - bin 1 riskiest;
      5. aggregate per bin and derive the monitoring ratios with safe
         divisions (0 denominators -> NaN / 0.0, never an exception).
    """
    # -- decide whether we can / should filter by a month column --
    month_col_exists = month_col is not None and month_col in df.columns
    filter_by_month = month_col_exists and month_year_value is not None

    select_cols = [score_col, label_col] + ([month_col] if month_col_exists
                                            else [])
    df_work = df.loc[:, select_cols].copy()
    if filter_by_month:
        df_work = df_work.loc[df_work[month_col] == month_year_value]

    # -- drop rows with missing essentials --
    df_work = df_work.dropna(subset=[score_col, label_col])

    final_metric_cols = [
        "Percentile", "Min", "Max", "total_cnt", "bads", "goods",
        "bad_rate_per_bin", "overall_fraud_pct_per_bin",
        "cum_fraud_pct_per_bin", "good_to_bad_ratio_per_bin",
        "cum_good_to_bad_ratio",
    ]
    n = len(df_work)
    if n == 0:  # empty cohort -> empty table with the right shape
        prefix = ([month_col] if (filter_by_month and include_scope_col)
                  else ([scope_col_name] if include_scope_col else []))
        return pd.DataFrame(columns=prefix + final_metric_cols)

    # -- number of bins cannot exceed row count --
    n_bins = int(min(bins, n))

    # -- rank scores to break ties and control direction --
    ascending = not higher_is_riskier
    ranks = df_work[score_col].rank(method="first", ascending=ascending)

    # -- percentiles from ranks: labels 0..n_bins-1 -> shift to 1..n_bins --
    df_work["Percentile"] = pd.qcut(ranks, q=n_bins, labels=False) + 1

    # -- aggregate per bin (dict-style agg, portable across pandas) --
    grouped = (
        df_work.groupby("Percentile")
        .agg({score_col: ["min", "max"], label_col: ["size", "sum"]})
        .reset_index()
    )
    grouped.columns = ["Percentile", "Min", "Max", "total_cnt", "bads"]
    grouped = grouped.sort_values("Percentile")

    # -- goods and cumulative metrics --
    grouped["goods"] = grouped["total_cnt"] - grouped["bads"]
    grouped["cum_bad"] = grouped["bads"].cumsum()
    grouped["cum_good"] = grouped["goods"].cumsum()
    total_bads = grouped["bads"].sum()

    # -- safe divisions --
    grouped["bad_rate_per_bin"] = np.where(
        grouped["total_cnt"] > 0, grouped["bads"] / grouped["total_cnt"],
        np.nan)
    grouped["overall_fraud_pct_per_bin"] = np.where(
        total_bads > 0, grouped["bads"] / total_bads, 0.0)
    grouped["cum_fraud_pct_per_bin"] = np.where(
        total_bads > 0, grouped["cum_bad"] / total_bads, 0.0)
    grouped["good_to_bad_ratio_per_bin"] = np.where(
        grouped["bads"] > 0, grouped["goods"] / grouped["bads"], np.nan)
    grouped["cum_good_to_bad_ratio"] = np.where(
        grouped["cum_bad"] > 0, grouped["cum_good"] / grouped["cum_bad"],
        np.nan)

    # -- scope column: the month value when filtered, else 'ALL' --
    if include_scope_col:
        if filter_by_month:
            grouped.insert(0, month_col, month_year_value)
        else:
            grouped.insert(0, scope_col_name, scope_all_value)

    prefix_cols = ([month_col] if (filter_by_month and include_scope_col)
                   else ([scope_col_name] if include_scope_col else []))
    return grouped[prefix_cols + final_metric_cols]


# ===========================================================================
# SPARK WRAPPER: CHARTS FOR EVERY SCORE, OVERALL AND PER MONTH
# ===========================================================================
def build_distribution_charts(
    df: DataFrame, score_fields: List[str], label_column: str,
    month_column: Optional[str] = None, bins: int = 100,
    higher_is_riskier: bool = True,
) -> pd.DataFrame:
    """Distribution tables for all scores x all scopes, from Spark data.

    Scopes: 'ALL' (whole population) plus one per distinct month value
    when month_column is given. All inputs are explicit arguments.
    Spark filters and collects one (scope, score) cohort at a time; the
    binning itself is generate_distribution_chart(). Each output row is
    tagged with its Model (score field) so the combined table stays
    long-format and queryable.
    """
    frames: List[pd.DataFrame] = []
    scopes: List[Optional[str]] = [None]  # None = ALL
    if month_column and month_column in df.columns:
        scopes += sorted(r[month_column] for r in
                         df.select(month_column).distinct().collect()
                         if r[month_column] is not None)
    print(f"[DIST] scopes: "
          f"{['ALL' if s is None else s for s in scopes]}; "
          f"scores: {score_fields}; bins: {bins}")

    for scope in scopes:
        cohort = df if scope is None else df.filter(
            col(month_column) == scope)
        for score in score_fields:
            pdf = (cohort.select(score, label_column,
                                 *([month_column] if month_column else []))
                   .toPandas())
            table = generate_distribution_chart(
                pdf, score_col=score, label_col=label_column,
                month_year_value=scope, bins=bins, month_col=month_column,
                higher_is_riskier=higher_is_riskier,
                include_scope_col=False)  # wrapper owns scope tagging
            if table.empty:
                continue
            table.insert(0, month_column or "Scope",
                         scope if scope is not None else "ALL")
            table.insert(1, "Model", score)
            frames.append(table)
    if not frames:
        return pd.DataFrame()
    return pd.concat(frames, ignore_index=True)


def plot_distribution_chart(table: pd.DataFrame, score: str, scope: str,
                            out_dir: str) -> Optional[str]:
    """Render one distribution chart PNG: bad rate by percentile bin
    (bars, left axis) + cumulative fraud capture (line, right axis) -
    the visual this table exists to feed. Returns the file path."""
    try:
        import matplotlib
        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
    except ImportError:
        print("[DIST] matplotlib not available - skipping plots")
        return None
    sub = table[table["Model"] == score]
    scope_col = [c for c in table.columns
                 if c not in ("Model",) and sub[c].eq(scope).all()]
    if scope_col:
        sub = sub[sub[scope_col[0]] == scope]
    if sub.empty:
        return None
    os.makedirs(out_dir, exist_ok=True)
    fig, ax1 = plt.subplots(figsize=(11, 4.5))
    ax1.bar(sub["Percentile"], sub["bad_rate_per_bin"] * 100,
            color="#c0392b", alpha=0.75, label="Bad rate %")
    ax1.set_xlabel("Percentile bin (1 = riskiest)")
    ax1.set_ylabel("Bad rate %")
    ax2 = ax1.twinx()
    ax2.plot(sub["Percentile"], sub["cum_fraud_pct_per_bin"] * 100,
             color="#2471a3", linewidth=2, label="Cum. fraud captured %")
    ax2.set_ylabel("Cumulative fraud %")
    ax1.set_title(f"Distribution chart - {score} ({scope})")
    fig.tight_layout()
    path = os.path.join(
        out_dir, f"distribution_{score}_{scope}.png".replace("/", "-"))
    fig.savefig(path, dpi=120)
    plt.close(fig)
    return path


def run_distribution_charts(
    spark: SparkSession, production_source: Dict[str, Any],
    score_fields: List[str], label_column: str,
    month_column: Optional[str] = None,
    config: Dict[str, Any] = CONFIG,
) -> pd.DataFrame:
    """End-to-end: read production data -> distribution tables -> print,
    optionally save the combined table to Delta and render PNG charts.

    The required inputs are explicit arguments: the production source,
    the score field list, and the label column.
    """
    df = read_dataset(spark, production_source)
    for s in score_fields:
        df = df.withColumn(s, col(s).cast("double"))
    df = df.withColumn(label_column, col(label_column).cast("int")).cache()

    charts = build_distribution_charts(
        df, score_fields, label_column, month_column, config["bins"],
        config["higher_is_riskier"])
    if charts.empty:
        print("[DIST] no charts produced - check inputs")
        return charts

    pd.set_option("display.width", 200)
    for score in score_fields:
        sub = charts[charts["Model"] == score]
        scope_col = month_column or "Scope"
        for scope, part in sub.groupby(scope_col):
            print(f"\n===== Distribution chart: {score} | {scope} =====")
            print(part.to_string(index=False))

    if config.get("output_path"):
        sdf = spark.createDataFrame(charts.astype(object).where(
            pd.notnull(charts), None))
        (sdf.withColumn("business_dt", F.current_date())
         .write.format("delta").mode("append")
         .save(config["output_path"]))
        print(f"[DIST] combined table appended to {config['output_path']}")

    if config.get("chart_dir"):
        scope_col = month_column or "Scope"
        for (scope, score) in (charts[[scope_col, "Model"]]
                               .drop_duplicates().itertuples(index=False)):
            path = plot_distribution_chart(charts, score, scope,
                                           config["chart_dir"])
            if path:
                print(f"[DIST] chart saved: {path}")
    return charts


# ===========================================================================
# DEMO DATA (DEMO_MODE=1)
# ===========================================================================
def make_demo_dataset(spark: SparkSession) -> DataFrame:
    """Synthetic scored population over 3 months: a strong score whose
    bad rate should fall smoothly across bins, plus month labels."""
    latent = F.randn(7)
    return (spark.range(30_000)
            .withColumn("Month_Year",
                        F.concat(F.lit("2025-0"),
                                 (col("id") % 3 + 1).cast("string")))
            .withColumn("glbscore", F.round(
                620 + latent * 60 + F.randn(8) * 45).cast("int"))
            .withColumn("fcrascore", F.round(
                640 + latent * 40 + F.randn(9) * 70).cast("int"))
            .withColumn("ea_score", F.round(
                600 + latent * 20 + F.randn(10) * 95).cast("int"))
            .withColumn("fraudflag",
                        when(F.rand(11) < F.lit(0.02) + latent * 0.012, 1)
                        .otherwise(0))
            .drop("id"))


# ===========================================================================
# MAIN
# ===========================================================================
def main() -> None:
    """Distribution chart run: the required inputs stated explicitly."""
    spark = get_spark()

    # --- the required inputs ---
    score_fields = CONFIG["score_fields"]      # list of input fields
    label_column = CONFIG["label_column"]      # the bad flag
    month_column = CONFIG["month_column"] or None

    if os.environ.get("DEMO_MODE") == "1":
        print("[main] DEMO_MODE - synthetic production data")
        df = make_demo_dataset(spark)
        charts = build_distribution_charts(
            df, score_fields, label_column, month_column,
            bins=10, higher_is_riskier=CONFIG["higher_is_riskier"])
        scope_col = month_column or "Scope"
        for (scope, score), part in charts.groupby([scope_col, "Model"]):
            print(f"\n===== Distribution chart: {score} | {scope} =====")
            print(part.to_string(index=False))
    else:
        run_distribution_charts(
            spark,
            production_source=CONFIG["production_source"],
            score_fields=score_fields,
            label_column=label_column,
            month_column=month_column)
    spark.stop()


if __name__ == "__main__":
    main()
