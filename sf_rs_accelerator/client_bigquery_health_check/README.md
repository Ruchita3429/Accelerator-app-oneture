# Client-run GCP BigQuery health check

This is the **BigQuery** kit. It runs in the client's environment, authenticates
with the client's identity, and produces results the client can review and share.
It does not require access to our Streamlit application or sharing credentials.
The kit is independent of the earlier Snowflake collector.

## What it checks

Twenty metadata queries cover these areas:

| Area | Observations |
|---|---|
| Inventory | Datasets, tables and views |
| Query workload | Job counts, state and failures |
| Usage | Daily processed/billed bytes, slot milliseconds, largest scans |
| Performance | Slow queries, start delays, cache observations, resource warnings |
| Capacity | Observed workload grouped by reservation ID |
| Pipelines | Load/copy/extract jobs; jobs carrying scheduled-query labels |
| Storage | Dataset totals, largest tables, last observed storage writes |
| Table design | Partition columns, clustering columns, selected table options |
| Access observations | Principal workload, access-denied job reasons, top-level column policy tags |

These are BigQuery signals, not renamed Snowflake warehouse/credit checks.
There is no arbitrary overall health score, fixed-dollar cost estimate, or
automatic remediation. Each query includes interpretation notes.

## Choose the scope first

Each run assesses **one project and one BigQuery location**. Use `US`, `EU`, or
a region such as `asia-south1`. The query execution location must match the
INFORMATION_SCHEMA region. `US` is not the same scope as `us-central1`.
Run once per relevant project/location; the script does not discover other
locations or silently fall back to them.

Job observations cover jobs submitted in the assessed project, not all jobs in
other projects that happen to read its tables. Regional views require project
level metadata/history permissions; dataset-only access is not sufficient for
full collection. The default job lookback is 7 days, configurable from 1 to 180.
Storage and inventory are current snapshots, not historical snapshots for that
window. The Python collector fixes the job-window end at run start and excludes
its own labeled collection jobs. Metadata can lag, and there is no transactional
snapshot across queries.

## Option A: SQL in the BigQuery console (recommended client workflow)

Use this option when the client wants to run every check inside their own
BigQuery environment. Python, Streamlit, AWS access and credential sharing are
not required. The short handoff instructions are also available in
`SQL_CONSOLE_QUICKSTART.md`.

1. Open `bigquery_health_check.sql` in a text editor.
2. Replace every `your-project-id` with the project being assessed.
3. Replace every `region-us` if the target location differs. Examples:
   `region-eu`, `region-us-central1`, or `region-asia-south1`.
4. Open **Google Cloud console → BigQuery → SQL workspace**.
5. Confirm the query execution project in the Google Cloud project selector.
6. Open **More → Query settings** and set the processing location to the same
   location used in the SQL. Configure maximum bytes billed if the client uses
   an on-demand billing limit.
7. Copy and run one complete `-- CHECK:` statement at a time. Each check begins
   with a comment such as `-- CHECK: dataset_inventory | inventory` and ends at
   its semicolon.
8. Export each result as CSV using the check ID as its filename, for example
   `dataset_inventory.csv` and `job_summary.csv`.
9. Record the project, location, run time, lookback, omitted/failed checks and
   any changed row limits in a small `run_notes.txt` file.
10. Review the CSVs for operational identifiers and principal email addresses,
    then ZIP the reviewed files and share that ZIP.

The worksheet is intentionally split into independent queries because the
checks return different column shapes. Running and exporting them separately
keeps each result readable. The default lookback is 7 days and each SELECT is
limited to 1,001 rows; the extra row indicates that the result may have been
truncated. Change `INTERVAL 7 DAY` or `LIMIT 1001` only when the client approves
the broader query scope.

The SQL reads BigQuery `INFORMATION_SCHEMA` metadata. It does not read business
table rows, create permanent tables, change IAM, or modify datasets. Query jobs
can still consume slots or incur BigQuery processing charges. The client should
keep the maximum-bytes setting and their own billing model in mind.

### Required permissions for the SQL workflow

The person running the worksheet normally needs:

- BigQuery Job User (`roles/bigquery.jobUser`) on the execution project.
- BigQuery Resource Viewer (`roles/bigquery.resourceViewer`) on the assessed
  project for project-wide job history.
- BigQuery Metadata Viewer (`roles/bigquery.metadataViewer`) on the assessed
  project for dataset, table, column and storage metadata.

If a check returns `Access Denied`, preserve the error and check ID in the run
notes rather than requesting broad administrator access just to make every row
populate.

## Option B: optional Python collector and result packager

Unzip the kit and open a terminal in `client_bigquery_health_check`. Use Python
3.10 or newer supported by the supplied Google library.

```bash
python3 -m venv .venv
source .venv/bin/activate
python -m pip install -r requirements.txt
```

On Windows PowerShell, use `py -m venv .venv` then
`.venv\Scripts\Activate.ps1`. No pandas, Streamlit or AWS packages are required.
Use your organization's approved package source if needed.

Authenticate using existing Application Default Credentials (ADC). For local
development, if Google Cloud CLI is installed:

```bash
gcloud auth application-default login
```

On a GCP environment with an attached authorized service account, ADC can use
that identity without this browser login. An existing ADC configuration such as
`GOOGLE_APPLICATION_CREDENTIALS` is also supported. Alternatively pass
`--credentials /local/path/service-account.json` for an approved service-account
key. Never send authentication files with the results.

Run the collector, replacing the project and location:

```bash
python collect_bigquery_health_check.py \
  --project YOUR_PROJECT_ID \
  --location US \
  --days 7 \
  --output bigquery_results
```

The script creates **`bigquery_results/`** and **`bigquery_results.zip`**. Review
the directory, then share the results ZIP. Existing output directories/ZIPs are
never overwritten; use a new name for a rerun. Omit `--output` for a unique name.

Optional first step: validate with BigQuery without collecting data:

```bash
python collect_bigquery_health_check.py \
  --project YOUR_PROJECT_ID --location US \
  --dry-run --output bigquery_validation
```

This connects to BigQuery and performs API dry runs, producing validation status
and available byte estimates. It creates no health-result rows. A validated query
is not a live execution test or a health assessment; runtime failures can still
occur. Omit `--dry-run` for the actual collection. `--sql-only` is entirely local
and does not validate SQL with BigQuery.

Other useful commands:

```bash
# Offline inventory of available checks.
python collect_bigquery_health_check.py --list

# Collect only a few checks, with a smaller output cap.
python collect_bigquery_health_check.py --project YOUR_PROJECT_ID --location EU \
  --checks dataset_inventory,job_summary,failed_jobs --max-rows 100 \
  --output bigquery_small

# Generate project/location-specific SQL without connecting.
python collect_bigquery_health_check.py --project YOUR_PROJECT_ID --location asia-south1 \
  --sql-only client_checks.sql
```

`--execution-project OTHER_PROJECT_ID` can submit/bill the collector queries in
a separate client project while reading metadata from `--project`. The default
execution project is the assessed project. This does not change data location.

## Limits, costs and failures

- Default: 1,000 exported rows per check, a 120-second query-result wait and a
  1 GiB maximum-bytes-billed setting per query. Change using `--max-rows`,
  `--timeout` and `--max-bytes-billed` (bytes). The collector requests one extra
  row to detect truncated output. A result cap does not cap bytes scanned.
- BigQuery metadata queries can incur charges or consume reserved slots. Bytes
  billed is not a total money budget, especially with capacity pricing. These
  settings apply to each query separately, not the whole run. API dry-run byte
  estimates are not a promise of final billing.
- The collector requests a server job timeout and attempts cancellation after
  a failed/waiting query. Job timeouts/cancellation are best effort; API retries
  and result retrieval mean `--timeout` is not a strict whole-check wall-clock
  bound. Inspect the recorded job ID if cancellation is uncertain.
- A failed check does not stop the remaining checks. Its error type/code and
  job ID are recorded; exception text is omitted. Permission failures, wrong
  locations, unavailable fields and byte-cap errors remain visible as failures.
- `empty` means the query returned no rows, not that the project is healthy.
  Aggregates may return null values where observations are unavailable.
- Exit `0`: all selected checks executed or validated; `2`: partial run with
  failed checks (also used for invalid CLI arguments); `1`: setup/output failure.

## Results to share

| File | Contents |
|---|---|
| `summary.md` | Scope and check execution coverage, including failures/caps |
| `manifest.json` | Scope/window, query IDs, statuses, SQL, limits and notes |
| `results.json` | Successful result rows and columns, preserving nulls |
| `csv/*.csv` | One file per successful check; empty results include column headers |
| `executed_checks.sql` | Exact queries using the Python run's fixed window end |
| `README.txt` | Sharing and interpretation notes |

No results are automatically uploaded. Connector credentials/files are not copied
into the archive. Results contain operational identifiers and principal email
addresses; they are **not anonymized**. SQL literals, free-text error messages
and warning details are omitted by default. Add `--include-sensitive-text` only
when the client chooses to include them, and review that content before sharing.
Supplied checks do not read customer/business-table rows or view definitions.

JSON is the authoritative representation for analysis. CSV represents null as an
empty cell and nested values as JSON text; formula-like strings receive a leading
apostrophe. JSON preserves the original strings, and decimals use strings to avoid
precision loss. Failed checks are absent from `results.json`, not replaced by zeros.

The static SQL worksheet uses a rolling 7-day window at each statement's
execution. The Python collector fixes one window-end timestamp for the whole
run, labels its jobs, applies per-query API settings, records failures and builds
the ZIP automatically. It is included for repeatable or larger assessments, but
it is not required for the client-run BigQuery console workflow.

## Permissions (client administrator)

The client retains control of all access. No grants are made by the script.
Typical predefined roles are:

| Role | Grant scope | Purpose |
|---|---|---|
| BigQuery Job User (`roles/bigquery.jobUser`) | Execution project | Submit collector query jobs |
| BigQuery Resource Viewer (`roles/bigquery.resourceViewer`) | Assessed project | Read project-wide job history (`bigquery.jobs.listAll`) |
| BigQuery Metadata Viewer (`roles/bigquery.metadataViewer`) | Assessed project | Read dataset/table/column/storage metadata |

Custom roles with the equivalent permissions can be used. Metadata views need
permissions including `bigquery.datasets.get`, `bigquery.tables.get/list` and,
for TABLES, `bigquery.routines.get/list`. ADC quota-project usage can additionally
require `serviceusage.services.use` on the quota project, depending on setup.
An administrator should approve the scope; neither BigQuery Admin nor access to
business-table rows is required by these supplied checks.

The BigQuery API must be available in the execution project. Any billing,
organization-policy or service-perimeter requirements remain under client control.

## Interpretation boundaries

- This is a BigQuery workload/metadata assessment, not a complete GCP audit.
- Billing exports and pricing agreements are needed for actual dollar costs.
  Query bytes and slot milliseconds alone do not provide those costs.
- Reservation IDs from jobs do not inventory purchased capacity, autoscaling,
  assignments in an admin project, or idle-slot utilization.
- Scheduled-query labels are a detection hint, not proof that all schedules or
  missed runs were found. Streaming and transfer-service configuration need
  additional APIs. No activity in a window does not establish an unused pipeline.
- Partition/clustering settings do not establish pruning efficiency. Cache-hit
  ratios do not account for eligibility; start delay is not proof of slot contention.
- Storage metadata may lag. Clone/snapshot storage attribution can differ from
  billing; this kit does not report historical growth. Last write time is not an
  application-specific freshness guarantee.
- User activity/policy tags are not effective IAM permissions, nested-field tag
  coverage, row-access-policy coverage, failed-login monitoring or a compliance score.

## Official references and validation

- [Regional INFORMATION_SCHEMA scope and pricing](https://docs.cloud.google.com/bigquery/docs/information-schema-intro)
- [Project job history, fields and permissions](https://docs.cloud.google.com/bigquery/docs/information-schema-jobs)
- [Dataset metadata](https://docs.cloud.google.com/bigquery/docs/information-schema-datasets-schemata)
- [Table storage metadata](https://docs.cloud.google.com/bigquery/docs/information-schema-table-storage)
- [Column metadata](https://docs.cloud.google.com/bigquery/docs/information-schema-columns)
- [Table options](https://docs.cloud.google.com/bigquery/docs/information-schema-table-options)
- [Local ADC authentication](https://docs.cloud.google.com/docs/authentication/set-up-adc-local-dev-environment)

Validated offline using SQL parsing and mocked Google API results. The tests cover
scope selection, query settings, failures, timeouts/cancellation, dry runs, data
serialization, privacy defaults and ZIP contents. No live client project was
queried during development. Run API dry-run validation in the intended client
scope, then perform the real collection to confirm runtime behavior.

The kit runs without the Streamlit project. Upload/import of result ZIPs into
the existing app is not implemented.
