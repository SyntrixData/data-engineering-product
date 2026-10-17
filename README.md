# Azure Data Engineering Product — PySpark Script Library

25 production-style PySpark modules, one per service category, targeting
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
| 1 | `01_data_source_integration.py` | Data Source Integration and Ingestion — DB, API, SaaS, file, cloud storage, ERP, CRM, e-commerce, real-time, batch, CDC, replication, webhooks, streaming, third-party, legacy |
| 2 | `02_etl_elt_services.py` | ETL and ELT Services — pipeline dev, cleansing, standardization, dedup, enrichment, normalization, aggregation, business rules, incremental/historical processing, reusable frameworks |
| 3 | `03_pipeline_development_automation.py` | Data Pipeline Development and Automation — design, orchestration, scheduling, event-driven, dependencies, error handling, retries, backfill, parameterization, versioning, migration, tuning |
| 4 | `04_data_warehouse_services.py` | Data Warehouse Services — design, implementation, cloud setup, migration, data marts, fact/dimension modeling, star/snowflake schemas, SCDs, optimization, cost control, maintenance |
| 5 | `05_data_lake_lakehouse.py` | Data Lake and Lakehouse Services — architecture, object storage, medallion (bronze/silver/gold), Delta/Iceberg/Hudi tables, schema evolution, partitioning, lifecycle, archival, migration |
| 6 | `06_big_data_engineering.py` | Big Data Engineering — Spark/PySpark dev, distributed processing, Hadoop interop, large-scale batch, stream processing, skew joins, memory/compute tuning, cluster config, migration |
| 7 | `07_realtime_data_engineering.py` | Real-Time Data Engineering — Event Hubs/Kafka ingestion, structured streaming, event-driven integration, real-time transforms, schema mgmt, monitoring, late/duplicate handling, replay/recovery |
| 8 | `08_data_quality_engineering.py` | Data Quality Engineering — assessment, profiling, validation, completeness, dedup, schema checks, referential integrity, reconciliation, freshness, anomaly detection, scoring, dashboards, remediation, alerts |
| 9 | `09_data_observability.py` | Data Observability and Pipeline Monitoring — failure/freshness/volume/schema/anomaly detection, lineage monitoring, SLA tracking, downtime detection, RCA, incident alerts, dashboards |
| 10 | `10_data_modeling.py` | Data Modeling and Transformation Management — conceptual/logical/physical/dimensional modeling, star/snowflake, SCDs, Data Vault, semantic models, reusable SQL transforms, testing, docs, versioning |
| 11 | `11_data_migration.py` | Data Migration Services — DB/cloud/warehouse/lake/ETL-tool/legacy/app/historical migration, schema migration, validation, assessment, planning, testing, cutover, post-migration tuning |
| 12 | `12_database_engineering.py` | Database Engineering and Management — schema dev, SQL, stored procedures, views, query/index optimization, partitioning, replication, backup/recovery, upgrades, access control |
| 13 | `13_integration_as_a_service.py` | Data Integration as a Service — prebuilt DB/SaaS connectors, custom API connectors, ERP/CRM/e-commerce/payment/marketing/HR/finance integrations, sync, mapping, monitoring |
| 14 | `14_master_data_management.py` | Master Data Management — customer/product/supplier/employee MDM, golden records, duplicate identification, matching/merging, synchronization, quality monitoring, governance workflows |
| 15 | `15_metadata_cataloging.py` | Metadata Management and Data Cataloging — metadata extraction, catalog implementation, documentation, discovery, asset inventory, business glossary, ownership, classification, search |
| 16 | `16_data_lineage.py` | Data Lineage Services — OpenLineage-style events on a Delta lineage store; source-to-target/table/column lineage, dependency mapping, impact/upstream/downstream analysis, visualization, change detection |
| 17 | `17_governance_security.py` | Data Governance and Security Engineering — RBAC/ABAC, classification, PII discovery, masking, tokenization, encryption, retention, auditing, ownership workflows, policy enforcement |
| 18 | `18_platform_architecture.py` | Data Platform Architecture and Consulting — automated assessment toolkit: environment probes, architecture scoring, current/target-state design, platform selection, roadmaps, capacity planning |
| 19 | `19_dataops_cicd.py` | DataOps and CI/CD — pipeline testing harness, SQL transform tests, environment management, deployment approvals, versioning, rollback, config/secret management, release monitoring |
| 20 | `20_production_support.py` | Data Engineering Production Support — health monitoring, incident management, failed-job investigation, RCA, reruns/recovery, reconciliation, SLA monitoring, maintenance, runbooks, DR planning |
| 21 | `21_bi_data_engineering.py` | BI Data Engineering — Power BI/Tableau/Looker integration, reporting marts, KPI pipelines, dataset optimization, automated refresh (incl. Power BI REST refresh), semantic models, self-service prep |
| 22 | `22_ml_data_engineering.py` | AI and Machine Learning Data Engineering — training dataset prep, feature engineering, feature pipelines, feature store integration, validation, versioning, batch + real-time inference pipelines, drift monitoring |
| 23 | `23_genai_data_engineering.py` | Generative AI Data Engineering — document ingestion (PDF/Word/text), chunking, Azure OpenAI embeddings, vector DB ingestion, incremental sync, RAG freshness monitoring, access controls, eval, agent support |
| 24 | `24_data_api_delivery.py` | Data API Development and Data Delivery — data-product publisher: curated Delta datasets + manifests, secure masked delivery, managed file exports, subscriptions, webhooks, usage monitoring, auditing |
| 25 | `25_industry_data_engineering.py` | Industry-Specific Data Engineering — parameterized vertical registry (retail, banking, healthcare, manufacturing, logistics, telecom, insurance, real estate, marketing, HR, energy, SaaS) driving bronze→silver→gold + KPI marts |

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
