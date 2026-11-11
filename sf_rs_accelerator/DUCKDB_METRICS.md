# Durable DuckDB Migration Metrics

The migration monitor continues to use Streamlit session state for live updates. A shared metrics repository writes run and per-job attempt history to a file-backed DuckDB database so completed and failed runs remain available after a browser session ends.

## Database location and deployment

The default database is:

```text
.migration_state/metrics.duckdb
```

Set `DATABRIDGE_METRICS_DB_PATH` to an absolute path on persistent storage when deploying the app. For containers or cloud hosts, mount a durable volume at that path. The directory is excluded from Git because it contains operational data.

The application uses a process-wide repository with one writer thread and one DuckDB connection. All metric operations are queued and committed transactionally by that writer. Run a single application process against a database file; do not point multiple Streamlit worker processes or app replicas at the same DuckDB file. For a multi-process or multi-host deployment, move metrics behind a dedicated single-writer service or a multi-writer database rather than sharing the file.

Runs that were still marked `running` when the app stopped are marked `interrupted` on the next database open. Their stored attempts remain available for review; the existing session-only job checkpoint is not automatically restored across a restart.

## Recorded fields and semantics

Each run and job attempt records source, target, start/end timestamps, duration, status, stage, row counts, bytes processed, validation outcome, optional watermark/checkpoint, and sanitized error details. Retries create separate attempts; manual export/load operations are tracked separately. Run totals use the latest attempt per job and operation so retries do not inflate counts or erase the export metrics when a separate load follows.

- `rows_read` counts source rows successfully read/exported.
- `rows_written` counts rows written to the configured destination (S3 for S3-only routes; Redshift for full routes).
- `rows_rejected` remains `NULL` when a connector cannot report rejected rows; it is not guessed as zero.
- `rows_written` remains `NULL` for manual S3-to-Redshift loads unless the load path returns a verified row count.
- `bytes_processed` counts bytes uploaded to S3 by an export attempt.
- `watermark` is `NULL` unless a route provides a meaningful checkpoint value.
- `validation_result` is `passed`, `failed`, or `not_run`; checks not performed are not treated as passing.
- Error text is length-limited and common credential/token patterns are redacted before persistence.

## S3 metric snapshots

After a run is finalized, the repository checkpoints DuckDB and exports an immutable JSON snapshot containing that run and its attempts. The app uploads it using the configured S3 credentials to:

```text
<configured-prefix>/_databridge_metrics/YYYY/MM/DD/<UTC timestamp>_<run-id>.json
```

Each snapshot has a unique key so prior exports are retained. The app uses the configured migration bucket and prefix; the AWS identity must be allowed to write objects under `_databridge_metrics/` in that prefix. No public bucket access or website CORS configuration is needed. Enable S3 bucket versioning as well if AWS-managed object versions are required. A snapshot upload failure is shown in the monitor and does not change the migration outcome. Use the run-history export action to retry a failed or interrupted run's snapshot.

## Verification

Run the offline DuckDB and application tests from this directory:

```powershell
.\.venv\Scripts\python.exe -m unittest discover -s tests -v
```

The DuckDB repository tests use temporary database files and do not contact AWS.
