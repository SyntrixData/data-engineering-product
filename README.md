# Azure Data Engineering Product — PySpark Script Library

33 production-style PySpark modules — one per service category, plus a
Kafka-to-Hive streaming ETL module, a universal file-ingestion
framework, a legacy-migration/decrypt module, a data-migration
series (Script 1: Hive → Azure, in `data_migration/`), a Data & Analytics
series (rule set development, in `Data&Analytics/`), and a Model Monitoring
series (quarterly PSI, ROC AUC by month × months-on-book, and percentile
distribution charts, in `Model_Monitoring/`) — targeting
**Azure Databricks + ADLS Gen2 + Delta Lake**. Each module implements one
function per listed activity (with a docstring naming the activity) plus a
`main()` driver wiring a representative end-to-end flow.

## Platform assumptions

- Compute: Azure Databricks (a `spark` session is assumed; each module also
  ships a `get_spark()` fallback that builds a local Delta-configured session).
- Storage: ADLS Gen2 via `abfss://<container>@<account>.dfs.core.windows.net/...`
- Table format: Delta Lake (MERGE, OPTIMIZE/ZORDER, VACUUM, time travel,
  Change Data Feed, DEEP CLONE).
- Secrets: `dbutils.secrets.get()` backed by Azure Key Vault, with
  `os.environ` fallback. **No credentials are hardcoded anywhere.**
- Parameters: `dbutils.widgets` where useful, guarded so modules also import
  outside Databricks.

## Configuration

Every module reads its settings from environment variables at the top of the
file, e.g.:

```bash
export AZURE_STORAGE_ACCOUNT="<your-storage-account>"
export BRONZE_CONTAINER="bronze"
export SILVER_CONTAINER="silver"
export GOLD_CONTAINER="gold"
export KEY_VAULT_SCOPE="<databricks-secret-scope>"
```

Replace the `<...>` placeholders with your values before running.

## Module index

| # | File | Category |
|---|------|----------|
| 1 | `data_source_integration.py` | Data Source Integration and Ingestion — DB, API, SaaS, file, cloud storage, ERP, CRM, e-commerce, real-time, batch, CDC, replication, webhooks, streaming, third-party, legacy |
| 2 | `etl_elt_services.py` | ETL and ELT Services — pipeline dev, cleansing, standardization, dedup, enrichment, normalization, aggregation, business rules, incremental/historical processing, reusable frameworks |
| 3 | `pipeline_development_automation.py` | Data Pipeline Development and Automation — design, orchestration, scheduling, event-driven, dependencies, error handling, retries, backfill, parameterization, versioning, migration, tuning |
| 4 | `data_warehouse_services.py` | Data Warehouse Services — design, implementation, cloud setup, migration, data marts, fact/dimension modeling, star/snowflake schemas, SCDs, optimization, cost control, maintenance |
| 5 | `data_lake_lakehouse.py` | Data Lake and Lakehouse Services — architecture, object storage, medallion (bronze/silver/gold), Delta/Iceberg/Hudi tables, schema evolution, partitioning, lifecycle, archival, migration |
| 6 | `big_data_engineering.py` | Big Data Engineering — Spark/PySpark dev, distributed processing, Hadoop interop, large-scale batch, stream processing, skew joins, memory/compute tuning, cluster config, migration |
| 7 | `realtime_data_engineering.py` | Real-Time Data Engineering — Event Hubs/Kafka ingestion, structured streaming, event-driven integration, real-time transforms, schema mgmt, monitoring, late/duplicate handling, replay/recovery |
| 8 | `data_quality_engineering.py` | Data Quality Engineering — assessment, profiling, validation, completeness, dedup, schema checks, referential integrity, reconciliation, freshness, anomaly detection, scoring, dashboards, remediation, alerts |
| 9 | `data_observability.py` | Data Observability and Pipeline Monitoring — failure/freshness/volume/schema/anomaly detection, lineage monitoring, SLA tracking, downtime detection, RCA, incident alerts, dashboards |
| 10 | `data_modeling.py` | Data Modeling and Transformation Management — conceptual/logical/physical/dimensional modeling, star/snowflake, SCDs, Data Vault, semantic models, reusable SQL transforms, testing, docs, versioning |
| 11 | `data_migration.py` | Data Migration Services — DB/cloud/warehouse/lake/ETL-tool/legacy/app/historical migration, schema migration, validation, assessment, planning, testing, cutover, post-migration tuning |
| 12 | `database_engineering.py` | Database Engineering and Management — schema dev, SQL, stored procedures, views, query/index optimization, partitioning, replication, backup/recovery, upgrades, access control |
| 13 | `integration_as_a_service.py` | Data Integration as a Service — prebuilt DB/SaaS connectors, custom API connectors, ERP/CRM/e-commerce/payment/marketing/HR/finance integrations, sync, mapping, monitoring |
| 14 | `master_data_management.py` | Master Data Management — customer/product/supplier/employee MDM, golden records, duplicate identification, matching/merging, synchronization, quality monitoring, governance workflows |
| 15 | `metadata_cataloging.py` | Metadata Management and Data Cataloging — metadata extraction, catalog implementation, documentation, discovery, asset inventory, business glossary, ownership, classification, search |
| 16 | `data_lineage.py` | Data Lineage Services — OpenLineage-style events on a Delta lineage store; source-to-target/table/column lineage, dependency mapping, impact/upstream/downstream analysis, visualization, change detection |
| 17 | `governance_security.py` | Data Governance and Security Engineering — RBAC/ABAC, classification, PII discovery, masking, tokenization, encryption, retention, auditing, ownership workflows, policy enforcement |
| 18 | `platform_architecture.py` | Data Platform Architecture and Consulting — automated assessment toolkit: environment probes, architecture scoring, current/target-state design, platform selection, roadmaps, capacity planning |
| 19 | `dataops_cicd.py` | DataOps and CI/CD — pipeline testing harness, SQL transform tests, environment management, deployment approvals, versioning, rollback, config/secret management, release monitoring |
| 20 | `production_support.py` | Data Engineering Production Support — health monitoring, incident management, failed-job investigation, RCA, reruns/recovery, reconciliation, SLA monitoring, maintenance, runbooks, DR planning |
| 21 | `bi_data_engineering.py` | BI Data Engineering — Power BI/Tableau/Looker integration, reporting marts, KPI pipelines, dataset optimization, automated refresh (incl. Power BI REST refresh), semantic models, self-service prep |
| 22 | `ml_data_engineering.py` | AI and Machine Learning Data Engineering — training dataset prep, feature engineering, feature pipelines, feature store integration, validation, versioning, batch + real-time inference pipelines, drift monitoring |
| 23 | `genai_data_engineering.py` | Generative AI Data Engineering — document ingestion (PDF/Word/text), chunking, Azure OpenAI embeddings, vector DB ingestion, incremental sync, RAG freshness monitoring, access controls, eval, agent support |
| 24 | `data_api_delivery.py` | Data API Development and Data Delivery — data-product publisher: curated Delta datasets + manifests, secure masked delivery, managed file exports, subscriptions, webhooks, usage monitoring, auditing |
| 25 | `industry_data_engineering.py` | Industry-Specific Data Engineering — parameterized vertical registry (retail, banking, healthcare, manufacturing, logistics, telecom, insurance, real estate, marketing, HR, energy, SaaS) driving bronze→silver→gold + KPI marts |
| 26 | `kafka_to_hive_datalake.py` | Kafka → Hive Streaming ETL — Kafka Structured Streaming source, dynamic JSON parsing, recursive flatten, topic-routed Hive tables on the lake, quarantine table for bad payloads, exactly-once appends |
| 27 | `file_ingestion_framework.py` | Universal File Ingestion Framework — one function per source-file category (structured, semi-structured, big-data, DB exports, logs, archives, documents, media, geospatial, scientific, cloud objects, config, API, messaging, ML/AI, enterprise exports), each taking a `file_type` arg and writing to Hive tables on the lake, plus a 21-operation dispatcher (validation, DQ, lineage, audit) |
| 28 | `legacy_migration_decrypt.py` | Legacy Migration + Decrypt — metadata-driven legacy → cloud migration; decrypts AES-256-GCM/CBC, Fernet, RSA-OAEP, handles one-way obfuscation (hash/mask/tokenize), loads to Azure (ADLS+Delta) or AWS (S3+Parquet). Decrypt paths are code-correct but were never tested live — install `cryptography` on the cluster before running for real. |
| 29 | `data_migration/hive_to_azure_migration.py` | Data Migration Series, Script 1 — Hive → Azure Blob (Parquet + metadata JSON) → Azure Data Lake (Delta). Entry point `migrate_hive_table_to_azure(spark, schema_name, table_name)`, with whole-schema helper `migrate_schema_to_azure()`. See “Data migration series” below. |
| 30 | `Data&Analytics/rule_set_development.py` | Data & Analytics Series — Rule Set Development: statistical profiling (quantile binning, lift, Information Value) + greedy rule selection under a hard 2% false-positive-rate budget, with a GBT ML benchmark; Step 3 reports confusion matrix, precision, recall, F1, FPR, specificity, MCC and lift on a held-out split. Entry point `develop_rule_set(df, target_column)`. See “Data & Analytics series” below. |
| 31 | `Model_Monitoring/psi_monitoring.py` | Model Monitoring Series — Quarterly Population Stability Index: compares production data per quarter against an out-of-time (OOT) baseline for a list of input fields; numeric fields binned on OOT deciles, categorical fields on OOT categories with unseen values in an `__OTHER__` bucket; PSI < 0.10 STABLE, 0.10–0.25 MODERATE, ≥ 0.25 SIGNIFICANT. Entry point `run_psi_monitoring(spark, oot_source, production_source, input_fields, categorical_fields, date_column)`. See “Model Monitoring series” below. |
| 32 | `Model_Monitoring/roc_auc_monitoring.py` | Model Monitoring Series — ROC AUC Monitoring: per-score ROC AUC + PR AUC by application month against cumulative months-on-book bad windows (`mob_0`…`mob_19`), computed with scikit-learn (`roc_curve` / `precision_recall_curve` / `auc`) on month cohorts collected from Spark; also reports Gini, KS and bad rate. Entry point `run_roc_auc(spark, production_source, score_fields, application_date_column, first_bad_date_column)`. See “Model Monitoring series” below. |
| 33 | `Model_Monitoring/distribution_charts.py` | Model Monitoring Series — Distribution Charts: percentile-bin distribution tables per score (overall and per month) — score range, population, bads/goods, bad rate per bin, fraud share, cumulative fraud capture and good-to-bad ratios, bin 1 = riskiest; optional PNG charts and Delta save. Entry points `run_distribution_charts(...)` and `generate_distribution_chart(df, score_col, label_col, ...)`. See “Model Monitoring series” below. |

## Data & Analytics series

Analytics modules live in `Data&Analytics/`, separate from the category
modules:

- **Rule Set Development** (`Data&Analytics/rule_set_development.py`):
  Step 1 pulls the configured sources (Hive/JDBC/Delta/Parquet/CSV, first
  source as base, others left-joined) into one DataFrame; Step 2 profiles
  every feature against the target, generates candidate rules and greedily
  selects a set that maximizes target capture while keeping the cumulative
  false positive rate inside a hard 2% budget (stops at 80% capture or
  15 rules), with a GBT model trained alongside as the ML benchmark
  ceiling; Step 3 measures the finished rule set on a held-out split.
  Run with `DEMO_MODE=1` for a self-generated synthetic fraud demo.

## Model Monitoring series

Model monitoring modules live in `Model_Monitoring/`:

- **PSI Monitoring** (`Model_Monitoring/psi_monitoring.py`):
  Takes an OOT dataset, production data, and a list of input fields, and
  calculates the Population Stability Index per field per quarter
  (production split into quarters from the date column, e.g. `2025Q3`).
  Numeric fields are binned on the OOT deciles (outer bins open-ended so
  out-of-range production values still land somewhere); categorical fields
  use their OOT categories, with any production value never seen in OOT
  falling into an `__OTHER__` bucket so new values show up as drift
  instead of being silently dropped. Count smoothing (0.5) keeps empty
  bins from blowing up the logarithm. The report is sorted worst-first
  with the standard thresholds — PSI < 0.10 STABLE, 0.10–0.25 MODERATE
  (investigate), ≥ 0.25 SIGNIFICANT (action) — and the summary + bin
  detail can optionally be written to Delta. Run with `DEMO_MODE=1`
  for a self-generated demo with drift baked into Q3/Q4.
- **ROC AUC Monitoring** (`Model_Monitoring/roc_auc_monitoring.py`):
  Derives cumulative months-on-book bad flags (`mob_0`…`mob_19`) from the
  application and first-bad dates in Spark, then for every score field,
  every application month and every MOB window computes ROC AUC and
  PR AUC with scikit-learn, collecting one month cohort at a time from
  Spark so memory stays bounded. Results are printed as per-score monthly
  tables (months × MOB windows) and can be appended to a Delta metrics
  table with the run's business date. Run with `DEMO_MODE=1` for
  synthetic scored applications.
- **Distribution Charts** (`Model_Monitoring/distribution_charts.py`):
  For every score (overall and per month), ranks the population and cuts
  it into percentile bins (100 by default, bin 1 = riskiest end) and
  reports per bin: score range, population, bads, goods, bad rate, share
  of all fraud, cumulative fraud captured, and good-to-bad ratios — the
  table behind the classic distribution chart. Optional PNG charts
  (bad-rate bars + cumulative-capture line) and Delta save. Run with
  `DEMO_MODE=1` for a synthetic demo.

## Data migration series

Migration scripts live in `data_migration/`, separate from the category
modules:

- **Script 1 — Hive → Azure** (`data_migration/hive_to_azure_migration.py`):
  Step 1 reads a Hive table and captures its metadata; Step 2 lands the
  data (Parquet) and metadata (JSON) in Azure Blob Storage as a replayable
  staging area; Step 3 reads the metadata back from Blob, creates the
  schema/table in Azure Data Lake (Delta), loads the data, and validates
  the row count. One function, two arguments:
  `migrate_hive_table_to_azure(spark, schema_name, table_name)`.

## Running a module

On a Databricks cluster, upload a module (or the whole folder as a repo) and
run it as a notebook/job, or invoke `main()` after setting the env vars above.
For local syntax validation only:

```bash
python3 -m py_compile <module>.py
```

## Notes

- Modules are syntax-validated (`py_compile`); they have not been executed
  against a live Spark cluster, so runtime behavior (e.g., connector package
  availability on your Databricks runtime) should be smoke-tested in dev first.
- REST integrations (Power BI refresh, Azure OpenAI, vector DB, webhooks) call
  out to Azure services — keys come from Key Vault / env vars.
- Licensed under the MIT License — see `LICENSE`.
