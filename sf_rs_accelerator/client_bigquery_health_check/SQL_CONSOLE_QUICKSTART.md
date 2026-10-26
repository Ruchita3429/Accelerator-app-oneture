# BigQuery health check: SQL console quick start

Use `bigquery_health_check.sql` when the client will run the assessment directly
inside the Google Cloud BigQuery console. No Python installation and no
credential sharing are required.

## Prepare the worksheet

1. Make a working copy of `bigquery_health_check.sql`.
2. Replace every `your-project-id` with the project being assessed.
3. Replace every `region-us` when necessary. For example:
   - Multi-region `EU` becomes `region-eu`.
   - Region `us-central1` becomes `region-us-central1`.
   - Region `asia-south1` becomes `region-asia-south1`.
4. Leave the 7-day lookback and 1,001-row limit unchanged for the first run.

The selected BigQuery processing location must match the `region-*` qualifier.
`US` and `us-central1` are different locations.

## Run the checks

1. Open **Google Cloud console → BigQuery → SQL workspace**.
2. Select the intended query execution project.
3. In **More → Query settings**, set the processing location and any client
   approved maximum-bytes-billed limit.
4. Copy one complete block beginning with `-- CHECK:` and ending at its
   semicolon into the editor.
5. Run the query.
6. Export the result as CSV using the check ID in the comment. For example,
   export `-- CHECK: job_summary | workload` as `job_summary.csv`.
7. Repeat for the remaining checks.

Run the checks separately because their result columns are different. A check
that reaches 1,001 rows may be truncated. Record this instead of treating the
first 1,000 rows as complete coverage.

## Record and share the run

Create `run_notes.txt` containing:

```text
Project: YOUR_PROJECT_ID
Location: US
Run time (UTC): YYYY-MM-DD HH:MM
Lookback: 7 days
Completed checks: ...
Failed or omitted checks: ...
Changed limits/settings: ...
```

Review the CSV files before sharing. Results can contain project, dataset,
table, column and KMS/policy-tag identifiers, job and reservation IDs, and
principal email addresses. The supplied queries omit raw SQL text and free-text
error/warning messages by default.

ZIP the reviewed CSV files together with `run_notes.txt`. Do not include Google
credentials, browser downloads containing keys, or ADC files.

## Permissions

Typical predefined roles are:

- BigQuery Job User (`roles/bigquery.jobUser`) on the execution project.
- BigQuery Resource Viewer (`roles/bigquery.resourceViewer`) on the assessed
  project for project-wide job history.
- BigQuery Metadata Viewer (`roles/bigquery.metadataViewer`) on the assessed
  project for inventory and storage metadata.

The queries read `INFORMATION_SCHEMA`; they do not read business-table rows or
create permanent objects. Query jobs can still consume slots or incur charges.
If a query is denied, record the check ID and error rather than broadening access
without the client's IAM approval.
