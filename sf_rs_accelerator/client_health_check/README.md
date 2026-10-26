# Client-run Snowflake health check

Run the checks inside your own environment using your existing Snowflake access.
Review the generated files and share the results ZIP with the assessment team.
You do not need to give the team account access or credentials, install Streamlit,
configure AWS, or deploy a Snowflake stored procedure.

This kit contains 45 distinct queries from the POC health-check tab. The repeated
"Idle Pipes" diagnostic is collected once. Executive KPI queries are included;
the dashboard's heuristic composite score and recommendations are not generated.
The application and migration flows are unchanged.

## Option A: Python collector (one results ZIP)

Use Python 3.10 or newer supported by the supplied Snowflake connector. Unzip the
kit, open a terminal in `client_health_check`, and create an isolated environment:

```bash
python3 -m venv .venv
source .venv/bin/activate
python -m pip install -r requirements.txt
```

On Windows, use `py -m venv .venv` and `.venv\Scripts\Activate.ps1` in PowerShell.
Install dependencies through your organization's approved package source if needed.

If you already have a local Snowflake `connections.toml` profile, use it:

```bash
python collect_health_check.py --connection-name client_health --output results_demo
```

The profile must select a warehouse and an authorized role (or provide them using
`--warehouse` and `--role`). It may use the organization's existing SSO, key-pair,
or other supported authentication settings. This kit does not contain a profile
or keys and does not copy your profile into the output.

For interactive browser SSO:

```bash
python collect_health_check.py \
  --account YOUR_ORG-YOUR_ACCOUNT \
  --user YOUR_USER \
  --warehouse YOUR_WAREHOUSE \
  --role YOUR_APPROVED_ROLE \
  --output results_demo
```

Sign in to your organization's identity provider when the browser opens. Browser
authentication requires an interactive machine and working SSO configuration.
For headless runs, use an approved connection profile, such as one with key-pair
authentication. Password authentication is available with `--authenticator
snowflake` if account policy permits it; the script prompts without echoing the
password, or reads `SNOWFLAKE_PASSWORD`. There is no command-line password flag.

Useful options:

```bash
# List the check IDs without connecting or importing the Snowflake connector.
python collect_health_check.py --list

# Start with a small subset, using an existing local connection profile.
python collect_health_check.py --connection-name client_health \
  --checks account_information,total_credits_7d,slow_queries_last_7_days \
  --max-rows 100 --timeout 120 --output results_small

# Produce a SQL worksheet without connecting to Snowflake.
python collect_health_check.py --sql-only my_checks.sql
```

Each output directory must be new. The script refuses to overwrite an existing
directory or ZIP. With no `--output`, it generates a unique timestamped name.

The collector executes checks sequentially. Default limits are 120 seconds and
1,000 exported rows per check. It requests one extra row to detect truncation.
Some original queries already have a smaller top-50/top-100 limit. Resource
monitors use SHOW and are subject to Snowflake's visibility and result limits.
Rows are capped for transfer/output, not for warehouse scanning or billing.
Queries can use warehouse credits; the script does not resize or create a warehouse.
It sets query tags and timeouts only for its own connection.

## What to share

After collection, review `results_demo/summary.md` and `results_demo/manifest.json`.
Then share **`results_demo.zip`**, which contains:

| File | Contents |
|---|---|
| `summary.md` | Execution coverage, failures and capped results; not a health score |
| `manifest.json` | Query IDs, statuses, duration, columns, executed SQL and interpretation notes |
| `results.json` | Successful results with explicit columns and rows, preserving nulls |
| `csv/*.csv` | One file per successful check, including header-only files for empty results |
| `executed_checks.sql` | The queries used for this run |
| `README.txt` | Sharing and interpretation notes |

Missing privileges, timeouts or unavailable views fail only the affected check;
other checks continue. Failed results are absent from `results.json`, and their
manifest rows are marked `failed`, never converted to zero. Empty results are
marked `empty`, not "healthy". The client administrator can use the recorded query
ID/error code to investigate inside Snowflake. Connector exception text is not
included in the archive.

Exit codes: `0` = all selected checks executed; `2` = partial collection with one
or more failed checks; `1` = setup/connection/output failure (the argument parser
also uses `2` for invalid arguments). A successful collection does not prove full
account visibility or good health.

Credentials and connection profiles are not copied by the collector. Results do
include account/user/object names, query IDs, resource-monitor configuration and
operational metrics. They are **not anonymized**. Raw query text and source error
messages are excluded by default; error codes are exported instead. To include
that text explicitly, add `--include-sensitive-text`; it can contain SQL literals
or other sensitive content and needs review before sharing. No files are uploaded
automatically. Share through your existing agreed channel.

CSV nulls appear as empty cells; use JSON when distinguishing null from an empty
string matters. Decimal values are JSON strings to preserve exact precision.
CSV cells that resemble formulas receive a leading apostrophe; JSON retains the
original values. No customer/business tables are read by the supplied checks.

## Option B: SQL worksheet (no Python installation)

1. Open `snowflake_health_check.sql` in Snowsight.
2. Select your approved role and warehouse in the worksheet.
3. Execute each statement beneath its `-- CHECK:` label.
4. Download that statement's result as CSV, naming it `<check_id>.csv`.
5. Record any failed check IDs/error codes in a separate text file; include the
   collection time and any checks you chose not to run.
6. Review and ZIP the CSVs and status note, then share them.

The worksheet uses only SELECT and SHOW statements. It does not create objects,
grant privileges, or write data. It does not automatically export local files or
build a ZIP; use Python for that. SELECT output is capped at 1,001 rows (the same
extra-row limit used by the default Python collector); reaching that limit means
the output may be incomplete. Configure a statement timeout through your approved
worksheet/session settings if required. Select-only worksheets cannot enforce
the Python collector's per-query timeout.

## Access requirements (client administrator)

Use an existing approved role with USAGE on the selected warehouse and access to
the relevant `SNOWFLAKE.ACCOUNT_USAGE` views. No ACCOUNTADMIN runtime role is
required by this script. An administrator can grant the appropriate database
roles to a dedicated account role; the collector never grants them itself:

| Snowflake database role | Checks that use it |
|---|---|
| `SNOWFLAKE.OBJECT_VIEWER` | Database, table and view metadata |
| `SNOWFLAKE.USAGE_VIEWER` | Warehouse load, metering and pipe usage |
| `SNOWFLAKE.GOVERNANCE_VIEWER` | Query history metrics |
| `SNOWFLAKE.SECURITY_VIEWER` | Users, roles and login history |

These are broad view-access groupings; the administrator should select access
appropriate to the checks the client agrees to run. `SHOW RESOURCE MONITORS` has
separate visibility rules, such as MONITOR privileges on the relevant monitors;
the database roles alone do not guarantee those results. A role without security
view access can still return the permitted usage checks, with failures recorded.

Official references:

- [Snowflake database roles and view mapping](https://docs.snowflake.com/en/sql-reference/snowflake-db-roles)
- [Python connector authentication and connection profiles](https://docs.snowflake.com/en/developer-guide/python-connector/python-connector-connect)
- [Account Usage latency and visibility](https://docs.snowflake.com/en/sql-reference/account-usage)
- [Resource monitor visibility](https://docs.snowflake.com/en/sql-reference/sql/show-resource-monitors)

## Interpretation and parity with the POC

- Existing 7-day and 30-day windows are preserved. Several original checks have
  no reporting-window filter and scan the available Account Usage history.
  Each affected query has a note. Results reflect collection time, view latency
  and the running role; the 45 queries do not share a transactional snapshot.
- "Query Cost Breakdown" assigns an entire warehouse hour's credits times USD 2
  to each matching query. It is a legacy approximation, **not a per-query cost**;
  do not sum it or use it as a bill. "Idle Warehouse Credits" measures compute
  credits and does not calculate idle consumption.
- "Table Growth" and "Schema Growth" are current metadata snapshots. Clustering
  checks list keys, not clustering quality. Warehouse load thresholds are POC
  heuristics, not CPU percentages or proven sizing recommendations.
- LAST_ALTERED is not a data-freshness guarantee. Some original table/pipe
  aggregations group by name without full database/schema qualification and can
  combine objects. Metadata queries may include deleted objects. The standalone
  queries preserve these original filters and grouping rules.
- The account-name/locator output aliases are corrected in this kit. In the
  default export, query text is omitted and source error text becomes error codes.
  Check definitions and source line references are in `checks.json`.
- Executive KPI result tables are supplied for analysis. No composite health
  score is calculated: missing observations must not appear as healthy scores.
- Results are intended for offline review. Importing a results ZIP back into the
  existing Streamlit dashboard is not part of this kit.

## Local validation

The kit was tested with simulated connector results, including permission errors,
timeouts, empty/capped data, exact decimal export and ZIP content checks. SQL was
parsed using the Snowflake dialect offline. It has not been executed against a
live client Snowflake account; actual permissions/view availability must be
verified in the client's environment.
