# BigQuery to Redshift DDL Migrator

Select either BigQuery pipeline route and open **BigQuery DDL Migrator**. The converter runs locally and does not execute SQL in BigQuery or Redshift.

## Input modes

- **Paste or upload DDL** accepts one or more GoogleSQL `CREATE TABLE` statements. Optional JSON mappings send an exact `project.dataset.table` source to a chosen `schema.table` target.
- **Connected BigQuery tables** reads table schema, partition-field, and clustering-field metadata through the existing BigQuery connection. It does not scan table rows or issue a data query.

No downloadable SQL is produced when any statement or selected table fails conversion. This avoids presenting a partial schema migration as a complete batch.

## Automatic conversions

| BigQuery | Redshift |
|---|---|
| `INT64` / integer aliases | `BIGINT` |
| `FLOAT64` | `DOUBLE PRECISION` |
| `NUMERIC` | `DECIMAL(38,9)` |
| `NUMERIC(p,s)` | `DECIMAL(38,9)` for compatibility with the BigQuery client's Arrow/Parquet physical type; a review warning records that the parameter constraint is not retained |
| `BOOL` | `BOOLEAN` |
| `STRING` | configured `VARCHAR` length, default `VARCHAR(65535)` |
| `STRING(n)` | `VARCHAR(n)` when `n <= 65535` |
| `DATE` | `DATE` |
| `DATETIME` | `TIMESTAMP` |
| `TIMESTAMP` | `TIMESTAMP`, matching the existing UTC-normalized Parquet/COPY path |

`NOT NULL`, `IF NOT EXISTS`, and literal defaults are retained. BigQuery `OPTIONS` metadata is omitted with an explicit review warning.

The output always includes a Redshift distribution style. `AUTO` is the default. `KEY` requires a valid column. A compound sort key can be entered manually or seeded from a BigQuery partition column followed by clustering columns. That source layout is only a starting suggestion because BigQuery and Redshift optimize data differently.

## Manual-design cases

The POC rejects nested or repeated fields, `JSON`, `BIGNUMERIC`, `BYTES`, `TIME`, `GEOGRAPHY`, `INTERVAL`, and `RANGE`. These types are also outside the current BigQuery data-extraction subset or require an explicit target representation.

It also rejects table constraints, computed/non-literal defaults, temporary tables, `CREATE OR REPLACE`, `CREATE TABLE AS SELECT`, and `LIKE`. Use the Query Migrator for a CTAS query and decide separately how the target table should be deployed.

The **Check Redshift Syntax** action is a parser check. It does not verify permissions, object conflicts, target engine version, physical-design quality, or data equivalence. Run reviewed output in a disposable Redshift schema before using it for a migration.
