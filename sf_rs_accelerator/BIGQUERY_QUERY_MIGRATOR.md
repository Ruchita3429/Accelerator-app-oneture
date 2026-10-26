# BigQuery → Redshift query migrator

The Query Migrator now switches its source with Pipeline Destination. Both GCP
BigQuery routes display the BigQuery converter. Snowflake routes retain the
existing Snowflake query/procedure converter.

This POC translates GoogleSQL SELECT queries into a Redshift SQL draft for review.
Conversion is local: it does not require cloud credentials, execute queries,
create target tables or change the data-migration pipeline.

## Use in the app

1. Choose a **GCP (BigQuery)** pipeline destination.
2. Open **BigQuery Query Migrator**.
3. Paste BigQuery SQL, or upload a UTF-8 `.sql` file (up to 1 MiB). A file may
   contain multiple SELECT statements separated by semicolons.
4. Set **Target Redshift schema**, matching where your data was migrated. The
   default is `public`.
5. Optionally enter exact table mappings as JSON:

   ```json
   {
     "my-project.sales.orders": "public.orders",
     "my-project.archive.orders": "archive.orders"
   }
   ```

6. Click **Convert BigQuery Query**. Review the SQL, table mappings, changes,
   warnings and differences.
7. **Check Redshift Syntax** performs a local parser check. It does not validate
   permissions, table/column existence, source types or result equivalence.
8. Download the Redshift SQL. Review notes are included as comments in the file.

Changing input SQL, the target schema or mappings hides the old output until you
convert again. Unsupported statements produce explicit errors. If any statement
in a file fails conversion, no partial SQL file is offered; split the input if
you want to convert supported queries separately.

## Example

BigQuery input:

```sql
SELECT customer_id,
       IFNULL(region, 'Unknown') AS region,
       COUNTIF(amount > 0) AS paid_orders
FROM `my-project.sales.orders`
GROUP BY customer_id, region;
```

With target schema `public`, the generated query is equivalent to:

```sql
SELECT customer_id,
       COALESCE(region, 'Unknown') AS region,
       COUNT(CASE WHEN amount > 0 THEN 1 END) AS paid_orders
FROM "public"."orders" AS "orders"
GROUP BY customer_id, region;
```

## Supported POC scope

- Flat-table SELECTs, filters, joins, subqueries, CTEs and set operations.
- Common aggregates, conditional/string/math functions, casts and ranking windows.
- `IFNULL` → `COALESCE`; `IF` → `CASE`.
- `COUNTIF` → `COUNT(CASE ...)`, including empty-input and window behavior.
- `SAFE_CAST` → `TRY_CAST`, with an explicit compatibility warning.
- `SAFE_DIVIDE` and ordinary division through SQLGlot, with precision/overflow notes.
- `DATE_ADD`, `DATE_SUB`, `DATE_DIFF` and `DATE_TRUNC` for supported calendar units;
  explicit DATE casts preserve date-returning arithmetic/truncation.
- Fixed-duration timestamp arithmetic. `TIMESTAMP_DIFF` uses elapsed microseconds
  divided by BIGINT unit lengths, rather than counting crossed date boundaries.
- BigQuery `NUMERIC` casts use `DECIMAL(38,9)` when no precision/scale is specified;
  STRING casts use `VARCHAR(65535)`.
- Explicit NULL propagation for `GREATEST`/`LEAST`.
- QUALIFY with relation aliases, plus explicit compatible ROWS window frames.
- SQLGlot conversions for STRING_AGG, regex matching and date formatting with
  review notes about engine differences.

The exact reviewed function set is in `bigquery_query_migrator.py`. Unknown/UDF
functions are blocked rather than passed through with an assumed equivalent.
This is intentionally a bounded POC, not a complete GoogleSQL implementation.

## Table mapping rules

Default mapping is `project.dataset.TableName` → `target_schema.tablename`.
Project/dataset qualifiers are removed, but CTE references are recognized and
retained. Physical-table aliases and qualified column references are adjusted
together. Explicit mappings override this default and use the exact source name
as written, without backticks. Unused mappings produce a warning.

Two different source tables cannot silently map to one target table. Supply
distinct overrides to resolve collisions. Target schema/table names must use
simple unquoted-identifier characters; generated target identifiers are quoted
and lowercase. Map consistently to the actual Redshift table names. Column
names are not inferred or renamed; verify case-sensitive quoted columns.

## Manual migration cases

DDL, INSERT/UPDATE/DELETE/MERGE, BigQuery scripting and stored procedures are
outside this query feature. GCP mode does not show the Snowflake procedure form.
Also blocked: arrays/STRUCT/UNNEST and nested access, JSON-specific operations,
BIGNUMERIC/GEOGRAPHY representations, wildcard/decorated/time-travel tables,
partition pseudo-columns, metadata tables, parameters, SELECT * EXCEPT/REPLACE,
recursive CTEs, set operations BY NAME and unsupported functions.
Explicit timezone arguments and CAST FORMAT clauses require manual conversion.

Ordered aggregate/value windows without an explicit frame and RANGE windows need
manual treatment: substituting ROWS can change results for duplicate sort keys.
Calendar WEEK/ISO/custom units also need explicit boundary-rule review.

SAFE_CAST is not perfectly interchangeable with Redshift TRY_CAST: blank strings
and some type pairs still fail. Division can introduce DOUBLE PRECISION, and string
lengths, regex dialects, date formats, list aggregation and leap/calendar edges
can differ. Timestamp conversion assumes UTC-aligned target types/session settings.
Review warnings and compare representative results, including nulls and edge
cases. Parsing successfully is not a live Redshift validation.

## Python API and validation

```python
from bigquery_query_migrator import BigQueryQueryMigrator

result = BigQueryQueryMigrator(target_schema="public").migrate(source_sql)
if result.errors:
    print(result.errors)
else:
    print(result.sql)
    print(result.changes)
    print(result.warnings)
```

Uses the project's existing SQLGlot dependency; no new cloud service is required.
Offline tests cover SQL transformations, table/CTE/correlated references, rejected
constructs, multi-statement files, source-specific UI and Snowflake conversion
regression. SQLite is used only to exercise selected generated expressions for
empty/null input and integer-duration arithmetic; it is not a substitute for
executing the SQL on Redshift. No live cloud queries were run for this change.

References:

- [SQLGlot transpilation and unsupported expressions](https://sqlglot.com/sqlglot.html)
- [Redshift TRY_CAST limitations](https://docs.aws.amazon.com/redshift/latest/dg/r_TRY_CAST.html)
- [Redshift QUALIFY alias requirements](https://docs.aws.amazon.com/redshift/latest/dg/r_QUALIFY_clause.html)
