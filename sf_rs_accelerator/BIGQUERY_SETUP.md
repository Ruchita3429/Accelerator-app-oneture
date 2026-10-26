# BigQuery migration POC

The existing DataBridge app now accepts BigQuery as a source. Snowflake extraction and loading remain separate and retain their existing implementations.

For complete service-account JSON and Application Default Credentials setup, see [BIGQUERY_CONNECTION_GUIDE.md](BIGQUERY_CONNECTION_GUIDE.md).

## Routes

| Pipeline destination | Formats | Final destination |
| --- | --- | --- |
| GCP (BigQuery) → S3 only | Iceberg, Parquet, JSON, ORC | S3; Iceberg registered in AWS Glue |
| GCP (BigQuery) → S3 → Redshift | Parquet, JSON, ORC | Existing Redshift table via COPY |

No Google Cloud Storage bucket or BigQuery Storage API setup is needed. Python reads the query result via the BigQuery REST client.

## Setup

1. Install project dependencies: `venv/bin/python -m pip install -r requirements.txt`.
2. Start the application: `venv/bin/python -m streamlit run app_final.py`.
3. Select a GCP route in **Pipeline Destination**.
4. Expand **Google Cloud — BigQuery** in the sidebar. Enter the source project ID. Upload a service-account JSON key or choose Application Default Credentials. For local ADC, configure it beforehand with `gcloud auth application-default login`.
5. Click **Connect BigQuery**. The account needs permission to list/read datasets and tables and create query jobs (typically BigQuery Data Viewer on the source datasets and BigQuery Job User on the query project).
6. Fill in the existing AWS S3 bucket, prefix, region, access key and secret key inputs. Credentials must allow writing/listing the destination. Iceberg additionally needs AWS Glue database/table read/create/update permissions and S3 reads. Redshift preflight also calls `GetBucketLocation`.
7. For the Redshift route, fill in the existing workgroup/database/region inputs and click **Connect Redshift**. The AWS identity needs Redshift Data API execution/result permissions, and the database identity needs access to the target table and the chosen load operations. The bucket must be in the same AWS region as Redshift.
8. In **Table Discovery**, select datasets and click **Discover BigQuery tables**. Choose tables, format, optional filters/limits, and target options, then build the queue.
9. In **BigQuery → S3**, either run the queue under **Advanced — Job Queue Engine**, or select tables/settings and start a migration directly. Both entry points use the same backend.
10. Inspect **Monitor** and **Metrics & Logs** for completion, errors and the migration report.

## S3 layout

The dataset occupies the position used by a Snowflake schema:

- Files: `s3://bucket/prefix/dataset/table_timestamp.parquet` (or `.json`, `.orc`). A single-table export can use a custom filename.
- GZIP JSON: `.json.gz`. Parquet compression is inside the Parquet file; it is not a gzip wrapper.
- Iceberg: `s3://bucket/prefix/dataset/table/data/` and `metadata/`, with dataset/table names lowercase.
- Redshift loads also write a small manifest under `prefix/dataset/_manifests/` to identify the exact data object and length.

Iceberg uses the existing `databridge_glue` catalog approach. The Glue table is `dataset.table`. BigQuery-written tables have a `databridge.source` ownership property containing `bigquery:project.dataset.table`. Existing tables from another source/project or another S3 location are not overwritten. Unregistered objects at the proposed Iceberg location also block creation. Use distinct dataset/table names for demo sources that would otherwise collide.

## Supported data and load behavior

- Ordinary BigQuery tables; supported flat columns: INT64, FLOAT64, NUMERIC/DECIMAL, BOOL, STRING, DATE, DATETIME and TIMESTAMP.
- Nested/repeated fields, BIGNUMERIC and other unsupported types fail before S3 writes. A custom SELECT can project or cast them explicitly.
- Decimal precision/scale and nulls are preserved. JSON decimals are numeric tokens, not floating-point approximations. Non-finite JSON numbers fail explicitly.
- Iceberg preserves timestamp timezone semantics. For Redshift, timestamp data is normalized to UTC values in a timezone-free Arrow timestamp and targets `TIMESTAMP`.
- Results are downloaded in pages. POC caps are 100,000 result rows, 1 GiB maximum billed query bytes and 256 MiB of accumulated Arrow buffers. Apply a row limit or filter for larger tables. A row limit does not necessarily reduce BigQuery scan bytes.
- A custom SELECT applies to one selected table and overrides its filter. The selected dataset provides the query location. The row limit is applied to custom SQL too.
- Iceberg creates or replaces a table through an overwrite snapshot. This version writes unpartitioned Iceberg tables. It does not migrate BigQuery partition specifications.
- Redshift target tables must exist. Column names/order and supported types are checked before truncation. Expected targets: BIGINT, DOUBLE PRECISION, BOOLEAN, VARCHAR, DATE, TIMESTAMP and DECIMAL with the same precision/scale as the exported Arrow schema. VARCHAR capacity is checked in UTF-8 bytes.
- `TRUNCATE & LOAD` truncates once per table. `APPEND` preserves existing rows. An empty replacement clears the target; an empty append adds nothing. Truncate and COPY are separate operations, so a failed replacement COPY can leave an empty target.
- Final target counts are compared with exported rows (plus the initial target count for APPEND). Use demo targets without concurrent writers for this check.
- The queue holds IDs of submitted statements. On timeout, Resume checks the same COPY rather than submitting it again. Explicitly failed COPY statements may be retried. Exported files are reused on a load retry.
- State is session-local. Resume is not durable across a server restart. After losing the session, reconcile the target before repeating APPEND.
- Changing connection/destination settings requires rebuilding the BigQuery queue. Reset & Start Over intentionally starts a new run and can append data again if APPEND is selected.
- Iceberg byte metrics count current snapshot data files, excluding catalog/metadata overhead.

The separate Snowflake DDL/query migration and account health features retain their Snowflake scope. BigQuery data movement does not require the missing optional Snowflake DDL/validation modules.

## Offline verification

Run `venv/bin/python -m unittest discover -s tests -v`.

Tests cover real Parquet/ORC/JSON serialization and readback, Arrow/Iceberg schema conversion, catalog ownership, query constraints, queue routing, statement timeout recovery, and Streamlit interaction. Cloud clients are mocked; no credentials are required and no cloud data is changed by these tests. On sandboxed macOS, ORC readback may require running the test command outside the sandbox because Arrow queries CPU cache metadata with sysctl.

Live acceptance still requires running a small representative table through each route with the user's cloud connections and checking the S3/Glue/Redshift results.
