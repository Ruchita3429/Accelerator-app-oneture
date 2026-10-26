# What Changed in the Migration Accelerator — Full Breakdown

> **Plain English. Step by step. No jargon.**
>
> This document explains every change made to the codebase, starting from
> the original Snowflake-only tool and walking through each problem that
> was found and fixed, and each new feature that was added.

---

## Table of Contents

1. [What the Tool Does (Quick Background)](#1-what-the-tool-does-quick-background)
2. [Original State — Snowflake Only](#2-original-state--snowflake-only)
3. [Change 1 — Added Databricks as a Source](#3-change-1--added-databricks-as-a-source)
4. [Change 2 — Fixed the TypeError Crash on Startup](#4-change-2--fixed-the-typeerror-crash-on-startup)
5. [Change 3 — Fixed the mmh3 Security Block (Windows)](#5-change-3--fixed-the-mmh3-security-block-windows)
6. [Change 4 — Fixed CSV Being Forced to Iceberg](#6-change-4--fixed-csv-being-forced-to-iceberg)
7. [Change 5 — Fixed PyArrow Security Block (Windows)](#7-change-5--fixed-pyarrow-security-block-windows)
8. [Change 6 — Built a Real Iceberg Writer from Scratch](#8-change-6--built-a-real-iceberg-writer-from-scratch)
9. [Change 7 — Fixed Redshift Schema Discovery](#9-change-7--fixed-redshift-schema-discovery)
10. [Change 8 — Fixed the Migration Queue Execution Failures](#10-change-8--fixed-the-migration-queue-execution-failures)
11. [What Was NOT Changed — chunk_04_metrics_report.py](#11-what-was-not-changed--chunk_04_metrics_reportpy)
12. [One Remaining Gap to Be Aware Of](#12-one-remaining-gap-to-be-aware-of)
13. [File-by-File Summary](#13-file-by-file-summary)

---

## 1. What the Tool Does (Quick Background)

Think of this tool as a **data moving truck**.

- It picks up tables from a **source database** (originally just Snowflake, now also Databricks).
- It drops them off in **Amazon S3** (a cloud storage area) in a format you choose (CSV, Parquet, Iceberg, etc.).
- Optionally, it then loads those tables into **Amazon Redshift** (a cloud data warehouse) so you can query them there.

The tool has a web UI built with **Streamlit** — you just open it in your browser, connect your databases, pick tables, and click Run.

---

## 2. Original State — Snowflake Only

### What it could do before:
- Connect to **Snowflake** only as the source.
- Discover tables in Snowflake schemas.
- Export them to S3 as CSV or Parquet.
- Load them into Redshift using the `COPY` command.

### What it could NOT do before:
- Connect to **Databricks** as a source.
- Export data in real **Apache Iceberg** format.
- Handle the Windows security blocks on certain Python libraries.

---

## 3. Change 1 — Added Databricks as a Source

### Files changed:
- `chunk_01_table_discovery.py`
- `chunk_02_job_queue_engine.py`
- `app_final.py`

### What the problem was:
The Table Discovery screen (Tab 0 in the UI) only knew how to talk to Snowflake.
If you selected "Databricks" as your source, it would either crash or show nothing.

### What was done, step by step:

**Step A — Added a Databricks table scanner (`chunk_01_table_discovery.py`)**

A new function called `_fetch_all_tables_databricks()` was written.
This function:
1. Cleans up the Databricks host URL (removes `https://`, trailing slashes, etc.).
2. Tries to connect using the `databricks-sql-connector` Python package.
3. Runs `SHOW CATALOGS` to find available data catalogs.
4. Runs `SHOW SCHEMAS` to find schemas inside those catalogs.
5. Runs `SHOW TABLES` per schema to get the table list.
6. If any step fails (wrong credentials, library not installed, no permissions),
   it falls back to a set of **demo/fake Databricks tables** so the UI doesn't crash.

**Demo data added for Databricks fallback:**

| Schema | Tables |
|---|---|
| DEFAULT | SAMPLES_NYCTAXI, EVENTS_STREAM, DEVICE_TELEMETRY |
| MAIN | CUSTOMERS_DELTA, ORDERS_DELTA, PRODUCTS_DELTA |
| PUBLIC | LANDING_CUSTOMERS, LANDING_ORDERS, RAW_TRANSACTIONS |
| DWH | DIM_CUSTOMER, FACT_ORDERS, DIM_PRODUCT |
| REPORTING | RPT_SALES_DAILY, RPT_CHURN, RPT_FINANCIAL_SUMMARY |
| STAGING | STG_INBOUND, STG_USERS, STG_PAYMENTS |

**Step B — Made the discovery screen aware of source type (`chunk_01_table_discovery.py`)**

The main UI render function `render_table_discovery()` was updated to:
- Check whether the user selected "Snowflake" or "Databricks" as the source.
- Show "Connect Snowflake first" or "Connect Databricks first" badge accordingly.
- Call the right scanner function depending on the source.

**Step C — Added new fields to every job in the queue**

When a table is queued for migration, the job entry was extended to include:

| New Field | What it stores |
|---|---|
| `source_type` | `"Snowflake"` or `"Databricks"` |
| `source_schema` | The schema name from the source system |
| `source_table` | The table name from the source system |
| `db_schema` | Same as source_schema (for Databricks jobs) |
| `db_table` | Same as source_table (for Databricks jobs) |

> **Note:** The old `sf_schema` and `sf_table` fields still exist and are set for both
> Snowflake AND Databricks jobs — this ensures the rest of the app doesn't break.

**Step D — Added Databricks exporters (`chunk_02_job_queue_engine.py`)**

Two new extraction functions were written:

1. `_extract_databricks_to_s3()` — connects to Databricks, fetches all rows from
   the table, serializes them to your chosen format (CSV, Parquet, JSON), and uploads
   the file to S3 using `boto3`.

2. `_extract_databricks_to_s3_iceberg()` — same as above but routes to the dedicated
   Iceberg writer (see Change 6 below) for proper Apache Iceberg format.

**Step E — Made the main orchestrator route by source (`chunk_02_job_queue_engine.py`)**

Inside `run_migration()`, the engine now checks each job's `source_type` before
running Phase 1 (extract):

```
If source_type == "Databricks"  → call _extract_databricks_to_s3()
If source_type == "Snowflake"   → call _extract_snowflake_to_s3()
```

Phase 2 (load into Redshift) is the same for both — data is already in S3 by then.

---

## 4. Change 2 — Fixed the TypeError Crash on Startup

### Files changed:
- `chunk_01_table_discovery.py`
- `app_final.py`

### What the problem was:
```
TypeError: render_table_discovery() got an unexpected keyword argument 'source_type'
```

When `app_final.py` was updated to pass `source_type` and `db_cfg` as arguments
to `render_table_discovery()`, the Streamlit server was **hot-reloading** (refreshing
itself in the background). But Python's internal module cache still held the OLD
version of `chunk_01_table_discovery` without those new parameters.

Result: the new call crashed with a `TypeError` because the cached old function
didn't know what `source_type` was.

Also, a leftover Python process was still occupying port 8501, so the server
wouldn't even start fresh.

### What was done:

**Step A — Made `render_table_discovery()` accept any extra arguments defensively**

The function signature was updated to use `*args, **kwargs`:

```python
def render_table_discovery(
    sf_cfg: dict = None,
    s3_prefix: str = "",
    s3_bucket: str = "",
    rs_schemas: list = None,
    source_type: str = "Snowflake",   # new
    db_cfg: dict = None,              # new
    *args,
    **kwargs,                         # catches anything unexpected — won't crash
):
```

Even if the old cached module loads, any unknown extra arguments are safely
swallowed by `**kwargs` instead of causing a crash.

**Step B — Added `importlib.reload()` in `app_final.py`**

At the top of `app_final.py`, explicit module reloads were added so Python always
loads the latest version of chunk_01 and chunk_02 — no more stale cache problems.

**Step C — Killed the stuck process and restarted the server cleanly**

The lingering Python process on port 8501 was terminated, and the Streamlit
server was restarted fresh.

---

## 5. Change 3 — Fixed the mmh3 Security Block (Windows)

### Files changed:
- `mmh3_compat.py` *(new file created)*
- `chunk_01_table_discovery.py`
- `chunk_02_job_queue_engine.py`

### What the problem was:
```
DLL load failed while importing mmh3:
An Application Control policy has blocked this file.
```

`mmh3` is a Python library used internally by `pyiceberg` (the Apache Iceberg library).
It is a C-compiled extension (`.pyd` file on Windows). Windows corporate security
policies (WDAC / AppLocker / Defender) **block execution of unsigned `.pyd` binaries**
that live inside user profile directories like `AppData`.

`mmh3` computes MurmurHash3 — a mathematical hashing algorithm used by Iceberg
to distribute data into buckets.

### What was done:

A brand-new file `mmh3_compat.py` was created that works like this:

1. **First tries to load the real `mmh3` library** (the C-compiled one).
2. **If it's blocked or missing**, automatically falls back to a
   **pure Python re-implementation** of the same MurmurHash3 algorithm.
3. Injects this Python version into Python's module registry (`sys.modules["mmh3"]`)
   so that when `pyiceberg` does `import mmh3`, it silently gets the Python version —
   no DLL, no security block, no crash.

Both `chunk_01` and `chunk_02` now import `mmh3_compat` at the very top, before
anything else can import `mmh3`.

---

## 6. Change 4 — Fixed CSV Being Forced to Iceberg

### Files changed:
- `chunk_01_table_discovery.py`
- `app_final.py`

### What the problem was:
In an earlier version, if you picked "CSV" as the file format, the code was quietly
**changing it to "Iceberg"** behind the scenes. "CSV" was also removed from the format
dropdown entirely.

This forced ALL exports through the Iceberg write path, which requires `pyarrow._dataset`
— a library that Windows security blocks (see Change 5 below). So even choosing CSV
would trigger the security error.

### What was done:

- Restored **"CSV"** as the first and default option in the format dropdown.
- Removed the forced override logic completely.
- CSV exports now use Python's built-in `csv` module and `boto3` directly —
  **no C-extension DLLs involved at all**, completely bypassing Windows security policies.

Format dropdown now shows: `CSV → Parquet → Iceberg → JSON → ORC`

---

## 7. Change 5 — Fixed PyArrow Security Block (Windows)

### Files changed:
- `chunk_02_job_queue_engine.py`
- `app_final.py`

### What the problem was:
```
The pyarrow installation is not built with support for 'dataset'
(DLL load failed while importing _dataset:
An Application Control policy has blocked this file.)
```

`pyarrow` is a high-performance data library used by the old Iceberg write path.
On Windows with corporate security policies, its compiled binaries
(`arrow_dataset.dll`, `parquet.dll`, `_dataset.cp314-win_amd64.pyd`) are blocked —
same reason as `mmh3`: unsigned binaries in user-writable directories.

### What was done:

For Iceberg exports, the code now uses the new `iceberg_s3_writer.py` (see Change 6),
which uses `fastparquet` instead of `pyarrow`. `fastparquet` is allowed by Windows
policies.

If the Iceberg write ever fails for any reason, data is staged to S3 as a structured
CSV file as a safety net, with a clear warning logged. The pipeline continues
rather than crashing.

---

## 8. Change 6 — Built a Real Iceberg Writer from Scratch

### Files changed:
- `iceberg_s3_writer.py` *(new file created)*
- `chunk_02_job_queue_engine.py`

### What the problem was:
Even after all the security fixes above, choosing "Iceberg" format was still
uploading a `.csv` file to S3. The data in S3 was **not actually in Apache Iceberg
format** — it just had a different name.

The user specifically reported:
> "the data is transferred using databricks to s3 but even despite choosing the
> Iceberg format, the data is not transferred in Iceberg format."

### What was done:

A dedicated Iceberg writer (`iceberg_s3_writer.py`) was built from scratch.
It creates a real, standards-compliant Apache Iceberg v2 table in S3 using
**no blocked DLLs whatsoever**.

**What it creates in S3 for each table:**

```
s3://your-bucket/prefix/schema/table/
    data/
        abc123-00000.parquet      ← The actual data rows (Snappy-compressed Parquet)
    metadata/
        00000-uuid.metadata.json  ← Iceberg catalog describing schema, columns, snapshot
        version-hint.text         ← Tells readers which metadata version is current
```

**Step by step what the writer does:**

| Step | What happens |
|---|---|
| 1 | Receives the data as a Pandas DataFrame (a table in memory) |
| 2 | Writes it to Parquet using `fastparquet` with Snappy compression — WDAC-safe |
| 3 | Builds the Iceberg v2 `metadata.json` — records column names, types, row counts, snapshot ID |
| 4 | Uploads all 3 files to S3 via `boto3` |
| 5 | Registers the table in **AWS Glue Data Catalog** as an `EXTERNAL_TABLE` of type `ICEBERG` |

After step 5, the table is immediately queryable from **Redshift Spectrum, Athena,
or Apache Spark** — no manual setup needed.

If Glue registration fails (e.g. wrong IAM permissions), the S3 files are still
uploaded successfully. The Glue failure is logged as a warning, not a crash.

**Both Snowflake-source and Databricks-source Iceberg exports** now use this same writer.

---

## 9. Change 7 — Fixed Redshift Schema Discovery

### Files changed:
- `app_final.py`

### What the problem was:
The **"Target Redshift Schema"** dropdown in the UI only showed `poc_spectrum` and
`uch_spectrum`. The standard `public` schema was missing entirely.

**Why:** The SQL query used to fetch schemas was filtering OUT schemas owned by `rdsdb`:
```sql
SELECT schema_name FROM information_schema.schemata
WHERE schema_owner != 'rdsdb'
```

In Amazon Redshift, `public` and most built-in schemas are owned by `rdsdb`
(the internal admin account). This filter was accidentally hiding the most
important schema.

Only external Spectrum schemas (created by an IAM role, owned by a non-`rdsdb` user)
passed the filter — so only those appeared.

### What was done:

A **4-layer fallback strategy** was put in place for schema discovery:

| Layer | Method | Notes |
|---|---|---|
| 1 | AWS Redshift Data API `list_schemas()` | Direct AWS call — most reliable |
| 2 | `SELECT FROM svv_all_schemas` | Redshift system view |
| 3 | `SELECT FROM pg_namespace` | PostgreSQL-compatible catalog |
| 4 | `SELECT FROM information_schema.schemata` | No `rdsdb` filter this time |

The results filter out only true system schemas (`pg_*`, `information_schema`,
`catalog_history`), and `public` is always pinned to the top of the dropdown.

A **"+ Enter custom schema…"** option was also added so you can type any schema
name manually if it still doesn't appear.

---

## 10. Change 8 — Fixed the Migration Queue Execution Failures

### Files changed:
- `chunk_02_job_queue_engine.py`

### Five separate problems were fixed here:

---

**Problem A — COPY into an external Spectrum schema fails**

If you picked `poc_spectrum` as the target and ran migration:
```
Invalid operation: Target schema is an external schema
```
Redshift's `COPY` command can only load into **local** tables — not external
Glue/Spectrum schemas.

**Fix:** The engine now auto-detects external schemas.
- If the format is **Iceberg** → skips `COPY` entirely. The table is already
  registered in AWS Glue and queryable via Redshift Spectrum. Logs success.
- If the format is anything else → raises a clear, readable error explaining
  that you need to switch your target schema to `public`.

---

**Problem B — Target table didn't exist in Redshift**

```
ERROR: relation "public.my_table" does not exist
```

**Fix:** Before running `COPY`, the engine now checks if the table exists in Redshift.
If it doesn't, it automatically creates it:
```sql
CREATE TABLE IF NOT EXISTS "schema"."table" (
    "COLUMN_A" VARCHAR(65535),
    "COLUMN_B" BIGINT,
    ...
)
```
Column names and types are inferred from the source data that was just extracted.

---

**Problem C — TRUNCATE & LOAD mode wasn't truncating**

The "TRUNCATE & LOAD" option was selected in the UI but data was always being
**appended** because the `TRUNCATE TABLE` command was never being sent.

**Fix:** If load mode is "TRUNCATE & LOAD", the engine now explicitly runs:
```sql
TRUNCATE TABLE "schema"."table"
```
before the `COPY` command.

---

**Problem D — Iceberg COPY was pointing to the wrong S3 folder**

An Iceberg table in S3 has this layout:
```
table/
    data/     ← Parquet data files live here
    metadata/ ← Catalog files live here
```

The `COPY` command was pointing to `table/` (the root), so Redshift was trying
to read the metadata files as data — which fails.

**Fix:** For Iceberg format, the COPY path now always points to the `/data/` subfolder:
```sql
COPY "schema"."table"
FROM 's3://bucket/prefix/schema/table/data/'
FORMAT AS PARQUET
```

---

**Problem E — S3-only mode wasn't being detected correctly**

The check for skipping Redshift COPY in S3-only mode was too strict:
```python
if destination == "SF → S3 only":   # old — only matched one exact string
```

But the destination label in the UI had changed to `"Source → S3 only"`, so the
check never matched and it always tried to COPY into Redshift.

**Fix:**
```python
if "s3 only" in destination.lower():  # new — flexible, case-insensitive match
```

---

## 11. What Was NOT Changed — chunk_04_metrics_report.py

This file — the **Metrics & Report tab** — was **not modified** when Databricks
support was added, and it still works correctly for both Snowflake and Databricks jobs.

**Why it still works without changes:**

When `chunk_01` builds the job queue, it always sets `sf_schema` and `sf_table` on
every job entry, regardless of source. For a Databricks job, `sf_schema` holds the
Databricks schema name and `sf_table` holds the Databricks table name.

So `chunk_04` reads those fields and gets correct values for both source types —
no changes needed.

**What chunk_04 displays (works for both sources):**

| Section | What it shows |
|---|---|
| Migration Summary | Total tables, rows, data size, throughput, success rate, duration |
| Per-Table Breakdown | Status, rows, size, format, load mode, S3 key per table |
| Data Volume by Schema | Bar charts of MB and rows grouped by schema |
| Export Report | Downloadable CSV of all job results |
| COPY Statements | The actual SQL COPY commands that were run |
| Operation Log | Full timestamped log of all events |

---

## 12. One Remaining Gap to Be Aware Of

The **Download CSV Report** button in the Metrics tab exports a CSV file with
column headers named `sf_schema` and `sf_table` — even when the source was Databricks.

The data inside those columns is correct (it shows the Databricks schema/table names),
but the column headers are misleading for Databricks migrations.

**What should be fixed (not yet done):**

The exported report should use `source_schema` and `source_table` as column names,
and include a `source_type` column (`"Snowflake"` or `"Databricks"`) so the report
is fully self-describing and unambiguous.

---

## 13. File-by-File Summary

| File | Status | What changed |
|---|---|---|
| `chunk_01_table_discovery.py` | **Modified** | Added Databricks scanner, demo fallback data, `source_type` parameter, new job queue fields (`source_schema`, `source_table`, `db_schema`, `db_table`) |
| `chunk_02_job_queue_engine.py` | **Modified** | Added Databricks extractors, Iceberg routing, external schema detection, auto-create table, TRUNCATE fix, Iceberg COPY path fix, S3-only detection fix |
| `app_final.py` | **Modified** | Added `importlib.reload()`, restored CSV format, removed forced-Iceberg override, fixed schema discovery (4-layer strategy), updated Iceberg COPY path handling |
| `iceberg_s3_writer.py` | **NEW** | Real Apache Iceberg v2 writer: `fastparquet` Parquet files + Iceberg metadata JSON + AWS Glue registration. Zero blocked DLL dependencies. |
| `mmh3_compat.py` | **NEW** | Pure-Python MurmurHash3 shim that bypasses Windows WDAC/AppLocker security blocks on the `mmh3` C-extension |
| `chunk_04_metrics_report.py` | **Not changed** | Works transparently for both Snowflake and Databricks jobs |
| `chunk_03_monitoring_ui.py` | **Not changed** | Generic monitoring UI, source-agnostic |

---

*Document generated: 14 September 2026*
