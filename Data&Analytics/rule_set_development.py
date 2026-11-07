"""
30. Rule Set Development: Statistics + ML Assisted Rule Induction
==================================================================
Azure Data Engineering Product | PySpark

PURPOSE
-------
Analyze input data with statistical methods (profiling, binning, lift,
Information Value) assisted by a machine-learning benchmark, and develop
an interpretable RULE SET that identifies the target class (fraud, churn,
default, failure...) accurately with a MINIMAL FALSE POSITIVE RATE.

A rule set is a list of human-readable conditions combined with OR:
    predict target = 1 if ANY rule fires.
Rules stay explainable to business users and auditors - unlike a black-box
model - while the ML model is used as a performance benchmark ("ceiling")
to show how much accuracy the transparent rules give up.

INPUTS REQUIRED
---------------
  1. DATA SOURCES - where to pull data from and the logic to pull it:
     a list of source dicts (Hive table / JDBC / Delta / Parquet / CSV),
     see CONFIG["data_sources"]. The first source is the base table;
     further sources are LEFT-joined onto it on CONFIG["join_keys"].
  2. TARGET VARIABLE - the binary 0/1 column to identify
     (CONFIG["target_column"]) plus the feature columns to mine.

STEP 1  Read the source tables into one Spark DataFrame.
STEP 2  Statistically profile every feature against the target, generate
        candidate rules, score them, and greedily select a rule set that
        maximizes target capture under a hard FALSE-POSITIVE-RATE BUDGET.
STEP 3  Measure the finished rule set: confusion matrix, precision,
        recall, F1, FPR, specificity, MCC, lift - and the same measures
        for the ML benchmark, so the trade-off is quantified.

Run: spark-submit 30_rule_set_development.py
Demo (synthetic fraud data, no sources needed): DEMO_MODE=1 spark-submit ...
"""

import json
import math
import os
from typing import Any, Dict, List, Optional

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.functions import col, expr, when

# ---------------------------------------------------------------------------
# CONFIGURATION
# ---------------------------------------------------------------------------
CONFIG = {
    # --- Inputs: data sources + the logic to pull them ---
    # Each source: {"name": ..., "type": "hive"|"jdbc"|"delta"|"parquet"|"csv"|"json",
    #               "table": ... (hive/jdbc) or "path": ... (files),
    #               "options": {...} reader options, "filter": optional SQL filter}
    "data_sources": json.loads(os.environ.get("RULESET_SOURCES_JSON", """[
        {"name": "transactions", "type": "hive", "table": "risk.transactions"},
        {"name": "customers",    "type": "hive", "table": "risk.customers"}
    ]""")),
    "join_keys": ["customer_id"],          # keys to join enrichment sources on
    # --- Inputs: target + features ---
    "target_column": os.environ.get("RULESET_TARGET", "is_fraud"),
    "feature_columns": json.loads(os.environ.get("RULESET_FEATURES_JSON", """[
        "amount", "hour_of_day", "txn_count_1h", "device_age_days",
        "country", "merchant_category"
    ]""")),
    "categorical_columns": ["country", "merchant_category"],
    # --- Rule-mining controls (Step 2) ---
    "num_bins": 10,               # quantile bins per numeric feature
    "min_support": 200,           # a rule must fire on at least this many rows
    "min_lift": 3.0,              # a rule's target rate must beat base rate x3
    "max_fpr": 0.02,              # HARD budget: final rule set FPR <= 2%
    "max_rules": 15,              # stop adding rules after this many
    "target_capture_goal": 0.80,  # stop once 80% of targets are captured
    "train_fraction": 0.7,        # mine rules on train, report honestly on test
    "seed": 42,
}


# ---------------------------------------------------------------------------
# SPARK SESSION
# ---------------------------------------------------------------------------
def get_spark(app_name: str = "rule-set-development") -> SparkSession:
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


# ===========================================================================
# STEP 1 - READ SOURCE SYSTEMS INTO ONE PYSPARK DATAFRAME
# ===========================================================================
def read_source(spark: SparkSession, source: Dict[str, Any]) -> DataFrame:
    """Read ONE data source according to its type - the 'pull logic'.

    Supported types: hive (metastore table), jdbc (any RDBMS), and file
    formats on the lake (delta / parquet / csv / json). A per-source
    "filter" applies a SQL predicate at read time (predicate pushdown
    where the connector supports it) so only required rows are pulled.
    """
    stype = source["type"].lower()
    if stype == "hive":
        df = spark.table(source["table"])
    elif stype == "jdbc":
        # Credentials come from the environment - never hardcoded.
        df = (spark.read.format("jdbc")
              .option("url", os.environ["JDBC_URL"])
              .option("dbtable", source["table"])
              .option("user", os.environ.get("JDBC_USER", ""))
              .option("password", os.environ.get("JDBC_PASSWORD", ""))
              .options(**source.get("options", {}))
              .load())
    elif stype in ("delta", "parquet", "csv", "json"):
        reader = spark.read.format(stype).options(**source.get("options", {}))
        df = reader.load(source["path"])
    else:
        raise ValueError(f"unsupported source type: {stype}")

    if source.get("filter"):  # pull only the rows the analysis needs
        df = df.filter(source["filter"])
    print(f"[STEP 1] {source['name']}: {df.count()} rows pulled ({stype})")
    return df


def load_dataset(spark: SparkSession,
                 config: Dict[str, Any] = CONFIG) -> DataFrame:
    """STEP 1: pull every configured source and assemble the analysis set.

    The first source is the base (grain) table - typically transactions /
    events carrying the target. Each additional source is LEFT-joined on
    the configured join keys so features enrich the base without dropping
    any base rows (dropping rows would bias the target rate).
    """
    sources = config["data_sources"]
    base = read_source(spark, sources[0])
    for source in sources[1:]:
        enrich = read_source(spark, source)
        base = base.join(enrich, on=config["join_keys"], how="left")
    keep = list(dict.fromkeys(
        config["join_keys"] + config["feature_columns"]
        + [config["target_column"]]))
    df = base.select(*[c for c in keep if c in base.columns])
    print(f"[STEP 1] analysis set ready: {df.count()} rows, "
          f"target = {config['target_column']}")
    return df


# ===========================================================================
# DEMO DATA (used only when DEMO_MODE=1 - makes the module self-running)
# ===========================================================================
def make_demo_dataset(spark: SparkSession, n: int = 60_000) -> DataFrame:
    """Synthetic fraud-like data with real, learnable signal + noise.

    Target probability rises with amount, night hours, burst velocity,
    new devices and high-risk country/merchant combos - the kind of
    patterns a rule set should rediscover in Step 2.
    """
    rnd = F.rand(CONFIG["seed"])
    df = (spark.range(n)
          .withColumn("amount", F.round(F.exp(F.randn(CONFIG["seed"] + 1) * 0.9 + 3.5), 2))
          .withColumn("hour_of_day", (rnd * 24).cast("int"))
          .withColumn("txn_count_1h", (F.pow(rnd, 3) * 12).cast("int"))
          .withColumn("device_age_days", (F.pow(F.rand(CONFIG["seed"] + 2), 2) * 900).cast("int"))
          .withColumn("country", when(rnd < 0.82, "US")
                      .when(rnd < 0.90, "GB").when(rnd < 0.95, "IN")
                      .when(rnd < 0.98, "NG").otherwise("RU"))
          .withColumn("merchant_category",
                      when(rnd < 0.4, "grocery").when(rnd < 0.65, "retail")
                      .when(rnd < 0.8, "travel").when(rnd < 0.92, "electronics")
                      .otherwise("crypto"))
          .drop("id"))
    risk = (
        when(col("amount") > 900, 0.22).otherwise(0.0)
        + when(col("hour_of_day").between(1, 4), 0.13).otherwise(0.0)
        + when(col("txn_count_1h") >= 6, 0.20).otherwise(0.0)
        + when(col("device_age_days") < 7, 0.12).otherwise(0.0)
        + when(col("country").isin("NG", "RU"), 0.15).otherwise(0.0)
        + when(col("merchant_category") == "crypto", 0.15).otherwise(0.0)
        + F.lit(0.004))  # background fraud rate
    return df.withColumn(
        "is_fraud", when(F.rand(CONFIG["seed"] + 3) < risk, 1).otherwise(0))


# ===========================================================================
# STEP 2 - STATISTICAL PROFILING + RULE INDUCTION
# ===========================================================================
def metrics_from_counts(tp: int, fp: int, tn: int, fn: int) -> Dict[str, float]:
    """All headline measures from a confusion matrix (pure Python).

    Definitions that matter for rule work:
      precision  = TP/(TP+FP)  - of everything the rules flag, how much
                                 is truly target. Drives false positives.
      recall     = TP/(TP+FN)  - target capture: share of ALL targets found.
      FPR        = FP/(FP+TN)  - share of good population wrongly flagged;
                                 THE budget metric for this module.
      F1         = harmonic mean of precision and recall.
      MCC        = Matthews correlation - honest single score on imbalanced
                   data where accuracy lies (predicting all-0 looks great).
      lift       = precision / prevalence - how much better than random.
    """
    total = tp + fp + tn + fn
    precision = tp / (tp + fp) if (tp + fp) else 0.0
    recall = tp / (tp + fn) if (tp + fn) else 0.0
    specificity = tn / (tn + fp) if (tn + fp) else 0.0
    prevalence = (tp + fn) / total if total else 0.0
    mcc_den = math.sqrt((tp + fp) * (tp + fn) * (tn + fp) * (tn + fn))
    return {
        "total": total, "prevalence": prevalence,
        "accuracy": (tp + tn) / total if total else 0.0,
        "precision": precision, "recall": recall,
        "specificity": specificity, "fpr": 1.0 - specificity,
        "fnr": 1.0 - recall,
        "f1": (2 * precision * recall / (precision + recall)
               if (precision + recall) else 0.0),
        "balanced_accuracy": (recall + specificity) / 2,
        "mcc": ((tp * tn - fp * fn) / mcc_den if mcc_den else 0.0),
        "lift": (precision / prevalence if prevalence else 0.0),
    }


def _bin_stats(df: DataFrame, group_expr, target: str) -> List[Dict[str, Any]]:
    """Per-group target statistics + Information Value contribution.

    IV (Information Value) measures a bin's predictive strength:
        IV_i = (%targets_i - %non-targets_i) * ln(%targets_i / %non-targets_i)
    Total IV per feature: <0.02 useless, 0.1-0.3 medium, >0.3 strong.
    Smoothing (+0.5) avoids log(0) on empty bins.
    """
    rows = (df.groupBy(group_expr.alias("grp"))
            .agg(F.count("*").alias("n"),
                 F.sum(target).alias("pos"))
            .collect())
    total_pos = sum(r["pos"] for r in rows) or 1
    total_neg = sum(r["n"] - r["pos"] for r in rows) or 1
    total = total_pos + total_neg
    base_rate = total_pos / total
    stats = []
    for r in rows:
        n, pos = r["n"], r["pos"]
        neg = n - pos
        rate = pos / n if n else 0.0
        p_pos = (pos + 0.5) / total_pos
        p_neg = (neg + 0.5) / total_neg
        stats.append({
            "group": r["grp"], "n": n, "pos": pos, "neg": neg,
            "target_rate": rate,
            "lift": rate / base_rate if base_rate else 0.0,
            "iv": (p_pos - p_neg) * math.log(p_pos / p_neg),
        })
    return stats


def generate_candidate_rules(
    df: DataFrame, target_column: str, config: Dict[str, Any] = CONFIG
) -> List[Dict[str, Any]]:
    """STEP 2a: profile every feature and emit candidate rules.

    Numeric features: quantile-bin the values, score each bin, and emit
    threshold rules (>= top edges, < bottom edges) plus high-risk band
    rules. Categorical features: score each value, emit one rule per
    high-lift value and one combined IN (...) rule for all high-lift
    values together. Every candidate carries its profiling stats.
    """
    target = target_column
    candidates: List[Dict[str, Any]] = []

    for feature in config["feature_columns"]:
        if feature in config["categorical_columns"]:
            # ---- categorical: one stats row per distinct value ----
            stats = _bin_stats(df, col(feature).cast("string"), target)
            high = [s for s in stats
                    if s["lift"] >= config["min_lift"]
                    and s["n"] >= config["min_support"]]
            for s in high:
                val = str(s["group"]).replace("'", "\\'")
                candidates.append({
                    "feature": feature,
                    "condition": f"`{feature}` = '{val}'",
                    "profile_lift": s["lift"], "profile_iv": s["iv"],
                })
            if len(high) > 1:  # combined rule often beats singles on FPR
                vals = ", ".join(
                    f"'{str(s['group']).replace(chr(39), chr(92) + chr(39))}'"
                    for s in high)
                candidates.append({
                    "feature": feature,
                    "condition": f"`{feature}` IN ({vals})",
                    "profile_lift": max(s["lift"] for s in high),
                    "profile_iv": sum(s["iv"] for s in high),
                })
            iv_total = sum(s["iv"] for s in stats)
            print(f"  [profile] {feature} (categorical): IV={iv_total:.3f}, "
                  f"{len(high)} high-lift values")
        else:
            # ---- numeric: quantile binning via approxQuantile ----
            edges = df.approxQuantile(feature, 
                                      [i / config["num_bins"]
                                       for i in range(1, config["num_bins"])],
                                      0.01)
            edges = sorted(set(float(e) for e in edges))
            if not edges:
                continue
            # bucket id = number of edges the value reaches
            bucket = F.lit(0)
            for e in edges:
                bucket = bucket + when(col(feature) >= e, 1).otherwise(0)
            stats = _bin_stats(df, bucket, target)
            iv_total = sum(s["iv"] for s in stats)
            print(f"  [profile] {feature} (numeric): IV={iv_total:.3f}, "
                  f"edges={[round(e, 1) for e in edges]}")
            # threshold rules from the extreme edges
            for e in edges:
                candidates.append({
                    "feature": feature,
                    "condition": f"`{feature}` >= {e}",
                    "profile_lift": 0.0, "profile_iv": iv_total})
                candidates.append({
                    "feature": feature,
                    "condition": f"`{feature}` < {e}",
                    "profile_lift": 0.0, "profile_iv": iv_total})
            # band rules for individual high-lift bins
            bounds = [float("-inf")] + edges + [float("inf")]
            for s in stats:
                if (s["lift"] >= config["min_lift"]
                        and s["n"] >= config["min_support"]):
                    lo, hi = bounds[s["group"]], bounds[s["group"] + 1]
                    parts = []
                    if lo != float("-inf"):
                        parts.append(f"`{feature}` >= {lo}")
                    if hi != float("inf"):
                        parts.append(f"`{feature}` < {hi}")
                    if parts:
                        candidates.append({
                            "feature": feature,
                            "condition": " AND ".join(parts),
                            "profile_lift": s["lift"],
                            "profile_iv": s["iv"]})
    print(f"[STEP 2] {len(candidates)} candidate rules generated")
    return candidates


def evaluate_candidates(
    df: DataFrame, candidates: List[Dict[str, Any]], target: str,
    total_pos: int, total_neg: int,
) -> List[Dict[str, Any]]:
    """Score every candidate on a population in ONE aggregation pass.

    For each candidate we count its true positives and false positives;
    precision / recall / FPR / F1 follow from those two numbers. One pass
    matters: the greedy loop re-scores candidates many times.
    """
    if not candidates:
        return []
    aggs = []
    for i, c in enumerate(candidates):
        cond = expr(c["condition"])
        aggs.append(F.sum(when(cond & (col(target) == 1), 1)
                          .otherwise(0)).alias(f"tp_{i}"))
        aggs.append(F.sum(when(cond & (col(target) == 0), 1)
                          .otherwise(0)).alias(f"fp_{i}"))
    row = df.agg(*aggs).collect()[0]
    scored = []
    for i, c in enumerate(candidates):
        tp, fp = int(row[f"tp_{i}"] or 0), int(row[f"fp_{i}"] or 0)
        m = metrics_from_counts(tp, fp, total_neg - fp, total_pos - tp)
        scored.append({**c, "tp": tp, "fp": fp,
                       "support": tp + fp, **{k: m[k] for k in
                       ("precision", "recall", "fpr", "f1", "lift")}})
    return scored


def greedy_rule_selection(
    train: DataFrame, candidates: List[Dict[str, Any]], target_column: str,
    config: Dict[str, Any] = CONFIG,
) -> List[Dict[str, Any]]:
    """STEP 2b: greedily build the rule set under the FPR budget.

    Loop: score all remaining candidates on the rows NOT yet covered by
    the selected rules (their incremental value), take the best-F1 rule
    whose addition keeps the cumulative false positive rate within
    CONFIG['max_fpr'], and repeat. This set-cover style search captures
    the majority of targets while each added rule must 'pay for' its
    false positives - the direct implementation of 'accurate with
    minimal false positives'.
    """
    target = target_column
    totals = train.agg(
        F.sum(when(col(target) == 1, 1).otherwise(0)).alias("pos"),
        F.sum(when(col(target) == 0, 1).otherwise(0)).alias("neg"),
    ).collect()[0]
    total_pos, total_neg = int(totals["pos"]), int(totals["neg"])
    print(f"[STEP 2] train: {total_pos} targets / {total_neg} non-targets "
          f"(base rate {total_pos / (total_pos + total_neg):.4f})")

    selected: List[Dict[str, Any]] = []
    remaining = list(candidates)
    cum_tp = cum_fp = 0
    uncovered = train

    while remaining and len(selected) < config["max_rules"]:
        scored = evaluate_candidates(uncovered, remaining, target,
                                     total_pos, total_neg)
        # eligible: enough fresh evidence, strong precision signal
        eligible = [s for s in scored
                    if s["support"] >= config["min_support"]
                    and s["tp"] > 0 and s["lift"] >= config["min_lift"]]
        if not eligible:
            break
        eligible.sort(key=lambda s: (s["f1"], s["precision"]), reverse=True)
        picked = None
        for cand in eligible:
            # budget check: would this rule breach the cumulative FPR?
            if (cum_fp + cand["fp"]) / total_neg <= config["max_fpr"]:
                picked = cand
                break
        if picked is None:
            print("  [greedy] FPR budget reached - no rule fits, stopping")
            break
        selected.append(picked)
        cum_tp += picked["tp"]
        cum_fp += picked["fp"]
        capture = cum_tp / total_pos
        print(f"  [greedy] + rule {len(selected)}: {picked['condition']} "
              f"(+{picked['tp']} targets, +{picked['fp']} FP; cumulative "
              f"capture {capture:.3f}, FPR {cum_fp / total_neg:.4f})")
        remaining = [c for c in remaining
                     if c["condition"] != picked["condition"]]
        # next round scores only what the rule set still misses
        covered = " OR ".join(f"({r['condition']})" for r in selected)
        uncovered = train.filter(f"NOT ({covered})")
        if capture >= config["target_capture_goal"]:
            print(f"  [greedy] target capture goal "
                  f"{config['target_capture_goal']} reached")
            break
    return selected


def train_ml_benchmark(
    train: DataFrame, test: DataFrame, target_column: str,
    config: Dict[str, Any] = CONFIG
) -> Dict[str, float]:
    """ML benchmark: gradient-boosted trees as the accuracy 'ceiling'.

    The rule set is the deliverable; this model exists so Step 3 can state
    honestly how much performance the transparent rules trade away.
    Returns AUC-ROC and AUC-PR on the test split.
    """
    from pyspark.ml import Pipeline
    from pyspark.ml.classification import GBTClassifier
    from pyspark.ml.evaluation import BinaryClassificationEvaluator
    from pyspark.ml.feature import (StringIndexer, OneHotEncoder,
                                    VectorAssembler)

    target = target_column
    stages, feature_cols = [], []
    for f in config["feature_columns"]:
        if f in config["categorical_columns"]:
            idx, oh = f"{f}_idx", f"{f}_oh"
            stages += [StringIndexer(inputCol=f, outputCol=idx,
                                     handleInvalid="keep"),
                       OneHotEncoder(inputCol=idx, outputCol=oh)]
            feature_cols.append(oh)
        else:
            feature_cols.append(f)
    stages.append(VectorAssembler(inputCols=feature_cols,
                                  outputCol="features", handleInvalid="keep"))
    stages.append(GBTClassifier(labelCol=target, featuresCol="features",
                                maxIter=50, maxDepth=4, seed=config["seed"]))
    model = Pipeline(stages=stages).fit(train)
    preds = model.transform(test)
    ev = BinaryClassificationEvaluator(labelCol=target,
                                       rawPredictionCol="rawPrediction")
    return {
        "ml_auc_roc": ev.evaluate(preds, {ev.metricName: "areaUnderROC"}),
        "ml_auc_pr": ev.evaluate(preds, {ev.metricName: "areaUnderPR"}),
    }


def develop_rule_set(
    df: DataFrame, target_column: str, config: Dict[str, Any] = CONFIG
) -> Dict[str, Any]:
    """STEP 2 orchestrator: split, profile, mine, select, benchmark.

    target_column is an explicit REQUIRED input - the binary 0/1 column
    the rule set must learn to identify - and is threaded through every
    stage below rather than read from global config.
    """
    train, test = df.randomSplit(
        [config["train_fraction"], 1 - config["train_fraction"]],
        seed=config["seed"])
    train = train.cache()
    candidates = generate_candidate_rules(train, target_column, config)
    rules = greedy_rule_selection(train, candidates, target_column, config)
    benchmark = train_ml_benchmark(train, test, target_column, config)
    print(f"[STEP 2] rule set developed: {len(rules)} rules; "
          f"ML benchmark AUC-ROC {benchmark['ml_auc_roc']:.3f}")
    return {"rules": rules, "benchmark": benchmark, "train": train,
            "test": test}


# ===========================================================================
# STEP 3 - PERFORMANCE MEASUREMENT
# ===========================================================================
def apply_rule_set(df: DataFrame, rules: List[Dict[str, Any]],
                   pred_col: str = "rule_prediction") -> DataFrame:
    """Apply the rule set: prediction = 1 if ANY rule fires (OR logic)."""
    if not rules:
        return df.withColumn(pred_col, F.lit(0))
    combined = " OR ".join(f"({r['condition']})" for r in rules)
    return df.withColumn(pred_col, when(expr(combined), 1).otherwise(0))


def evaluate_rule_set(
    df: DataFrame, rules: List[Dict[str, Any]], target_column: str,
    config: Dict[str, Any] = CONFIG,
    benchmark: Optional[Dict[str, float]] = None,
    label: str = "test",
) -> Dict[str, Any]:
    """STEP 3: confusion matrix + full statistical report for the rule set.

    Evaluated on the held-out TEST split by default - measuring on the
    data the rules were mined from would flatter every number below.
    """
    target = target_column
    scored = apply_rule_set(df, rules)
    row = scored.agg(
        F.sum(when((col("rule_prediction") == 1) & (col(target) == 1), 1)
              .otherwise(0)).alias("tp"),
        F.sum(when((col("rule_prediction") == 1) & (col(target) == 0), 1)
              .otherwise(0)).alias("fp"),
        F.sum(when((col("rule_prediction") == 0) & (col(target) == 0), 1)
              .otherwise(0)).alias("tn"),
        F.sum(when((col("rule_prediction") == 0) & (col(target) == 1), 1)
              .otherwise(0)).alias("fn"),
    ).collect()[0]
    tp, fp, tn, fn = (int(row[k] or 0) for k in ("tp", "fp", "tn", "fn"))
    m = metrics_from_counts(tp, fp, tn, fn)

    print(f"\n================ RULE SET PERFORMANCE ({label}) ================")
    print("Confusion matrix:")
    print(f"                    actual target=1   actual target=0")
    print(f"  predicted = 1        TP = {tp:<8}     FP = {fp}")
    print(f"  predicted = 0        FN = {fn:<8}     TN = {tn}")
    print(f"  precision (of flagged, truly target) : {m['precision']:.4f}")
    print(f"  recall / target capture              : {m['recall']:.4f}")
    print(f"  FALSE POSITIVE RATE                  : {m['fpr']:.4f} "
          f"(budget {config['max_fpr']})")
    print(f"  specificity                          : {m['specificity']:.4f}")
    print(f"  F1 score                             : {m['f1']:.4f}")
    print(f"  accuracy                             : {m['accuracy']:.4f}")
    print(f"  balanced accuracy                    : {m['balanced_accuracy']:.4f}")
    print(f"  MCC                                  : {m['mcc']:.4f}")
    print(f"  lift vs random                       : {m['lift']:.2f}x")
    print(f"  prevalence (base rate)               : {m['prevalence']:.4f}")
    if benchmark:
        print(f"  ML benchmark AUC-ROC / AUC-PR        : "
              f"{benchmark['ml_auc_roc']:.4f} / {benchmark['ml_auc_pr']:.4f}")
    print("===============================================================")
    return {"tp": tp, "fp": fp, "tn": tn, "fn": fn, **m,
            "rules": [r["condition"] for r in rules]}


# ===========================================================================
# MAIN
# ===========================================================================
def main() -> None:
    """Full flow: Step 1 pull -> Step 2 develop -> Step 3 measure."""
    spark = get_spark()

    if os.environ.get("DEMO_MODE") == "1":
        print("[main] DEMO_MODE - using synthetic data")
        df = make_demo_dataset(spark)
    else:
        df = load_dataset(spark)                      # STEP 1
    df = df.cache()

    # The target column is stated explicitly here and passed down the
    # whole pipeline - it is an input to the analysis, not a global.
    target_column = CONFIG["target_column"]
    developed = develop_rule_set(df, target_column)   # STEP 2
    report = evaluate_rule_set(developed["test"], developed["rules"],
                               target_column,
                               benchmark=developed["benchmark"],
                               label="test (held-out)")   # STEP 3
    # Also show the train numbers so overfitting is visible, not hidden.
    evaluate_rule_set(developed["train"], developed["rules"],
                      target_column, label="train")

    print("\nFINAL RULE SET (predict target=1 if ANY rule fires):")
    for i, cond in enumerate(report["rules"], 1):
        print(f"  R{i}: {cond}")
    spark.stop()


if __name__ == "__main__":
    main()
