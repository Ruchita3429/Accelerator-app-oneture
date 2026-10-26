# 🚀 Cloud Data Warehouse Migration Accelerator
### *Snowflake & Databricks → Amazon S3 → Amazon Redshift*

[![Python 3.10+](https://img.shields.io/badge/python-3.10+-blue.svg)](https://www.python.org/downloads/)
[![Streamlit](https://img.shields.io/badge/Streamlit-1.63.0-FF4B4B.svg)](https://streamlit.io/)
[![AWS Redshift](https://img.shields.io/badge/AWS-Redshift%20%26%20S3-232F3E.svg)](https://aws.amazon.com/redshift/)
[![Snowflake](https://img.shields.io/badge/Snowflake-Connector-29B5E8.svg)](https://www.snowflake.com/)
[![Databricks](https://img.shields.io/badge/Databricks-SQL-FF3621.svg)](https://www.databricks.com/)
[![Apache Iceberg](https://img.shields.io/badge/Apache-Iceberg-008080.svg)](https://iceberg.apache.org/)

An enterprise-grade, CXO-ready Streamlit application for end-to-end data pipeline orchestration, automated schema conversion, dialect-aware query migration, cross-cloud data validation, and executive source health auditing.

---

## 📑 Table of Contents

- [Overview](#-overview)
- [Architecture & Data Flow](#-architecture--data-flow)
- [Key Features & Modules](#-key-features--modules)
  - [1. Table Discovery & Queue Engine](#1-table-discovery--queue-engine)
  - [2. Multi-Format S3 Staging (CSV, Parquet, Iceberg)](#2-multi-format-s3-staging-csv-parquet-iceberg)
  - [3. High-Performance Execution & Monitoring](#3-high-performance-execution--monitoring)
  - [4. Cross-Cloud Data Validation](#4-cross-cloud-data-validation)
  - [5. DDL & Query / Stored Procedure Migrator](#5-ddl--query--stored-procedure-migrator)
  - [6. Source Health Check & Executive Insights](#6-source-health-check--executive-insights)
- [Repository Structure](#-repository-structure)
- [Installation & Setup](#-installation--setup)
- [Configuration Guide](#-configuration-guide)
- [Usage Workflow](#-usage-workflow)
- [Troubleshooting & FAQs](#-troubleshooting--faqs)

---

## 🌟 Overview

Migrating mission-critical enterprise workloads from **Snowflake** or **Databricks** to **Amazon Redshift** is complex: schema differences, data type incompatibilities, massive table sizes, network constraints, and governance verification create significant friction.

This **Migration Accelerator** solves these challenges by providing a single, unified control plane that:
- **Discovers** source schemas and sizes dynamically.
- **Transfers** data at multi-gigabyte scale to Amazon S3 using **CSV**, **Parquet**, or **Apache Iceberg** (with AWS Glue registration).
- **Loads** target tables into Amazon Redshift (Serverless or Provisioned) or exposes them via Redshift Spectrum external schemas.
- **Validates** data parity with row-level metrics and bitwise MD5 checksum parity across clouds.
- **Translates** DDL, SQL queries, and Stored Procedures with automatic DISTKEY/SORTKEY optimization.
- **Audits** source account health with executive-ready dashboards and prioritized recommendations.

---

## 🏗 Architecture & Data Flow

```
┌─────────────────────────────────┐
│     Source Data Platforms       │
│  ❄️ Snowflake   🟠 Databricks   │
└──────────────┬──────────────────┘
               │
               ▼  (UNLOAD / PyArrow / PyIceberg)
┌─────────────────────────────────────────────────────────────┐
│                    Intermediate Staging                     │
│                       Amazon S3                             │
│   ├─ 📄 CSV (Gzip, Multi-part)                              │
│   ├─ 📦 Parquet (Snappy, Columnar)                          │
│   └─ 🧊 Apache Iceberg (v2 Metadata + AWS Glue Catalog)     │
└──────────────┬──────────────────────────────┬───────────────┘
               │                              │
     (COPY into Redshift)          (External Spectrum Catalog)
               │                              │
               ▼                              ▼
┌─────────────────────────────────────────────────────────────┐
│                       Amazon Redshift                       │
│      🔴 Local Schemas (public)  /  🌐 External Schemas      │
└─────────────────────────────────────────────────────────────┘
```

---

## 🚀 Key Features & Modules

### 1. Table Discovery & Queue Engine
- **Multi-Schema Browser**: Introspects Snowflake `ACCOUNT_USAGE` or Databricks Unity Catalog / Hive Metastore (`information_schema`).
- **Interactive Filtering**: Filter by table name, row count, or data size with one-click multi-selection.
- **Dynamic Target Mapping**: Maps source schemas to Redshift schemas, guaranteeing that local schemas like `public` are prioritized.
- **Pipeline Destination Modes**:
  - **Full Pipeline**: Exports to S3 and automatically runs Redshift `COPY`.
  - **S3-Only Mode**: Exports to S3 (and registers Glue catalog tables for Iceberg), skipping local Redshift ingestion.

### 2. Multi-Format S3 Staging (CSV, Parquet, Iceberg)
- **CSV & Parquet**: Automated compression (Gzip/Snappy) and partitioned file generation.
- **Apache Iceberg (`iceberg_s3_writer.py`)**:
  - Native Apache Iceberg metadata creation via `pyiceberg`.
  - Automatic table and partition registration in **AWS Glue Data Catalog**.
  - Direct queryability via **Amazon Redshift Spectrum** (`SELECT * FROM "awsdatacatalog"."table"`).
- **High-Capacity Unload**:
  - Configured with `MAX_FILE_SIZE=5368709120` (5 GB, Snowflake single-file maximum).
  - Built-in automatic fallback from `SINGLE=TRUE` to multi-part chunks (`SINGLE=FALSE`) when tables exceed 5 GB, avoiding Snowflake error `100083`.

### 3. High-Performance Execution & Monitoring
- **Threaded Job Queue (`chunk_02_job_queue_engine.py`)**:
  - Sequential or concurrent execution with state persistence.
  - Interactive **Resume / Checkpoint** capability: retry only failed or pending tables without re-running completed ones.
- **Real-Time Monitoring Grid (`chunk_03_monitoring_ui.py`)**:
  - Live progress bars, duration tracking, rows transferred, and byte counts.
  - Per-table and schema-level rollups.
- **Metrics & Generated SQL (`chunk_04_metrics_report.py`)**:
  - Migration throughput analytics (MB/s, rows/sec).
  - One-click generated Redshift `COPY` statements downloadable as a `.sql` script.
  - Exportable run metrics (`.csv`).

### 4. Cross-Cloud Data Validation
Located in **Tab 6 (`tab_data_validation.py`)**:
- **Multi-Source Support**: Validate **Snowflake ↔ Redshift** or **Databricks ↔ Redshift**.
- **Row Count Parity**: Instant comparative row count delta with match/mismatch status badges.
- **Schema & Type Mapping**: Normalizes Databricks/Snowflake types (`string`, `double`, `bigint`, `timestamp`) against Redshift types (`varchar`, `float`, `bigint`, `timestamp`).
- **Statistical Profiling**: Compares `MIN`, `MAX`, `AVG`, and `NULL_COUNT` for all numerical and temporal columns.
- **MD5 Bitwise Checksum Parity**: Computes an aggregate hash across all rows and columns using matching STRTOL hex-sum algorithms across both source and target engines.
- **Side-by-Side Data Sample**: Side-by-side preview of source and destination rows.

### 5. DDL & Query / Stored Procedure Migrator
- **DDL Migrator (Tab 7)**:
  - **Paste & Convert**: Transforms Snowflake/Databricks `CREATE TABLE` to Redshift syntax.
  - **Bulk Schema Converter**: Converts an entire source database or schema into an executable `.sql` script.
  - **Key Optimization**: Automatically analyzes and recommends `DISTKEY` and `SORTKEY` configurations.
- **Query & Procedure Migrator (Tab 8 & `query_migrator_app_v2.py`)**:
  - Powered by `sqlglot` for Snowflake $\rightarrow$ Redshift translation.
  - Automatically translates functions: `STRING_AGG` $\rightarrow$ `LISTAGG`, `NVL` $\rightarrow$ `COALESCE`, `DATEADD`, `DATEDIFF`, and window functions.
  - Converts Snowflake JavaScript and SQL Scripting Stored Procedures into Redshift `PL/pgSQL`.

### 6. Source Health Check & Executive Insights
Located in **Tab 9 (`app_final.py`)**:
- **6 Diagnostic Sub-Tabs**: Account/Workspace Overview, Warehouse/Cluster Utilization, Query Performance, Table & Schema, Pipelines & Ingestion, Security & RBAC.
- **🎯 Executive Insights Sub-Tab**:
  - **Composite Health Score ($0–100$)**: Evaluates Cost Efficiency, Warehouse Efficiency, Query Performance, Security, Pipeline Health, and Freshness.
  - **Cost & Object Hero Banner**: 7-day and 30-day estimated dollar spend and table counts.
  - **Plotly Visualizations**: 7-Day Spend Trend, Top Warehouse Spenders, and Top Users by Execution Time.
  - **🚨 Risk Flags**: Color-coded warnings (under/over-provisioned compute, queue build-up, remote storage spills, idle pipes, micro-file ingestion, unmanaged schemas).
  - **💡 Prioritized Recommendations**: Board-ready action plan ($P1, P2, P3\dots$) with findings, impacts, and remediation steps.
  - **⬇️ CSV Export**: Download complete executive health reports with a single click.

---

## 📂 Repository Structure

```
sf_rs_accelerator/
├── app_final.py                        # Main Streamlit orchestration application
├── chunk_01_table_discovery.py         # Schema browsing, filtering, & queue builder
├── chunk_02_job_queue_engine.py        # Migration execution engine, S3 unload, Redshift COPY
├── chunk_03_monitoring_ui.py           # Real-time monitoring status grid
├── chunk_04_metrics_report.py          # Migration throughput metrics & COPY generator
├── tab_data_validation.py              # Cross-cloud data validation suite
├── iceberg_s3_writer.py                # Apache Iceberg v2 writer & AWS Glue catalog sync
├── st_to_rs_bi_query_migration.py      # Core SQLGlot query & stored procedure translator
├── query_migrator_app_v2.py            # Standalone Streamlit query & SP migration app
├── mmh3_compat.py                      # Pure-Python MurmurHash3 fallback (WDAC / AppLocker safe)
├── requirements.txt                    # Project Python dependencies
└── walkthrough.md                      # Detailed technical walkthrough and test reports
```

---

## 💻 Installation & Setup

### Prerequisites
- **Python 3.10 to 3.14**
- **AWS Account** with access to Amazon S3 and Amazon Redshift (Serverless or Provisioned)
- **Snowflake Account** (AccountAdmin, SysAdmin, or custom migration role) **OR** **Databricks Workspace** (with Personal Access Token and SQL Warehouse / Cluster)

### 1. Clone or Extract the Project
```bash
cd sf_rs_accelerator
```

### 2. Create and Activate a Virtual Environment
```bash
# Windows (PowerShell)
py -m venv .venv
.\.venv\Scripts\Activate.ps1

# Linux / macOS
python3 -m venv .venv
source .venv/bin/activate
```

### 3. Install Dependencies
```bash
python -m pip install -r requirements.txt
```

### 4. Launch the Accelerator
```bash
python -m streamlit run app_final.py
```
*The app will automatically open in your browser at `http://localhost:8501`.*

---

## ⚙️ Configuration Guide

Use the **Streamlit Sidebar** to configure your source and target connections:

### 1. Source Platform Selection
Choose **❄️ Snowflake** or **🟠 Databricks** at the top of the sidebar.

#### For Snowflake:
- **Account Identifier**: `orgname-accountname` (or locator `xy12345.us-east-1`)
- **User & Password / Key**: Authentication credentials
- **Role**: `ACCOUNTADMIN`, `SYSADMIN`, or custom ETL role
- **Warehouse**: e.g., `COMPUTE_WH`
- **Database & Schema**: Source database and schema

#### For Databricks:
- **Server Hostname**: `dbc-xxxx.cloud.databricks.com`
- **HTTP Path**: `/sql/1.0/warehouses/xxxx`
- **Personal Access Token (PAT)**: `dapi...`
- **Catalog & Schema**: Target Unity Catalog or Hive metastore

### 2. Amazon S3 (Staging)
- **S3 Bucket**: Bucket name (e.g., `my-migration-bucket`)
- **S3 Prefix**: Folder prefix (e.g., `staging/migration_run_01`)
- **AWS Region**: e.g., `us-east-1`
- **AWS Access Key & Secret Key**: IAM credentials with S3 write permissions (and Glue permissions if using Iceberg)

### 3. Amazon Redshift (Destination)
- **Target Engine**: Serverless Workgroup or Provisioned Cluster
- **Workgroup / Cluster Name**: e.g., `default-workgroup`
- **Database**: e.g., `dev`
- **Target Schema**: e.g., `public` (local schemas required for standard COPY)
- **IAM Role ARN**: ARN attached to Redshift with permissions to read from your S3 bucket

---

## 🔄 Usage Workflow

1. **Table Discovery (Tab 1)**:
   - Browse tables from Snowflake or Databricks.
   - Filter by size or row count.
   - Select tables, pick target format (`Parquet`, `CSV`, or `Iceberg`), and click **➕ Add Selected to Queue**.
2. **Execute Migration (Tab 2 / Tab 3)**:
   - Click **▶ Start Migration**.
   - Watch live extraction to S3 and automated loading into Redshift.
   - If an individual table encounters an issue, review the diagnostic card and click **Resume** to retry.
3. **Monitor & Review (Tab 4 & Tab 5)**:
   - Monitor aggregate throughput (MB/s) and download generated `COPY` commands.
4. **Validate Data (Tab 6)**:
   - Run row count parity, schema comparison, and bitwise MD5 checksum audits to verify 100% data integrity.
5. **Convert DDL & Queries (Tab 7 & Tab 8)**:
   - Batch-convert source tables into Redshift DDL with optimized `DISTKEY` / `SORTKEY`.
   - Translate application SQL and Stored Procedures using the Query Migrator.
6. **Executive Insights (Tab 9)**:
   - Review composite health scores, compute utilization, and download board-ready CSV reports.

---

## ❓ Troubleshooting & FAQs

#### Q: Getting `OSError: [Errno 22] Invalid argument` on Windows?
**A**: This was caused by `print(..., flush=True)` with certain Windows terminal handles. The codebase includes a Windows-safe console wrapper that pre-encodes strings and catches `EINVAL` exceptions.

#### Q: Getting Snowflake Error `100083: Max file size (16777216) exceeded for unload single file mode`?
**A**: Snowflake enforces a 16 MB limit when `SINGLE=TRUE` is specified without `MAX_FILE_SIZE`. The accelerator now specifies `MAX_FILE_SIZE=5368709120` (5 GB) and includes an automatic fallback to multi-part unload if needed.

#### Q: Getting `Target schema 'awsdatacatalog' is an external schema` during Redshift COPY?
**A**: Redshift `COPY` cannot write directly to AWS Glue external schemas.
- **Solution 1**: Select a local schema such as `public` as the destination.
- **Solution 2**: Choose **Apache Iceberg** format. The accelerator will register the Iceberg table directly in AWS Glue, making it immediately queryable via Redshift Spectrum (`SELECT * FROM "awsdatacatalog"."table"`) without needing a `COPY` step.

#### Q: Application Control / WDAC policy blocks DLL import for `mmh3`?
**A**: The accelerator includes `mmh3_compat.py`, a pure-Python 128-bit MurmurHash3 implementation that automatically kicks in if the native C-extension is blocked by enterprise security policies.

---

## 📄 License & Attribution

Designed for enterprise cloud data warehouse modernization workflows. Built with [Streamlit](https://streamlit.io/), [SQLGlot](https://github.com/tobymao/sqlglot), [PyIceberg](https://py.iceberg.apache.org/), and [Snowflake Connector](https://github.com/snowflakedb/snowflake-connector-python).
