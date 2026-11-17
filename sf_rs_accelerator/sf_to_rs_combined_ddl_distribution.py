"""Snowflake -> Redshift DDL conversion with DISTKEY / SORTKEY analysis.

Used by the DDL Migrator tab in app_final.py. The tab calls these functions:
convert_create_statement, convert_datatypes, fix_timestamp_columns, clean_misc,
extract_table_name, fetch_best_keys and finalize_table. It also temporarily
updates SNOWFLAKE_CREDS with the connected account's settings.

No credentials live in this file. For the optional S3 batch mode at the bottom
(process_schema), set these environment variables:

    SNOWFLAKE_USER, SNOWFLAKE_PASSWORD, SNOWFLAKE_ACCOUNT, SNOWFLAKE_WAREHOUSE
    DDL_S3_BUCKET, DDL_S3_OUTPUT_BASE, DDL_S3_INPUT_PREFIX
    AWS_REGION (default ap-south-1); AWS keys come from the normal boto3 chain.
"""
import os
import re
from collections import defaultdict

import snowflake.connector
import sqlglot
from sqlglot import exp

# =============================================================================
# CONFIGURATION (from environment; nothing secret is stored here)
# =============================================================================

SNOWFLAKE_CREDS = {
    "user": os.environ.get("SNOWFLAKE_USER", ""),
    "password": os.environ.get("SNOWFLAKE_PASSWORD", ""),
    "account": os.environ.get("SNOWFLAKE_ACCOUNT", ""),
    "warehouse": os.environ.get("SNOWFLAKE_WAREHOUSE", ""),
    "database": "SNOWFLAKE",
    "schema": "ACCOUNT_USAGE",
}

AWS_REGION = os.environ.get("AWS_REGION", "ap-south-1")
BUCKET = os.environ.get("DDL_S3_BUCKET", "")
OUTPUT_BASE = os.environ.get("DDL_S3_OUTPUT_BASE", "")
INPUT_PATHS = {"STANDARD": os.environ.get("DDL_S3_INPUT_PREFIX", "")}

# How many recent queries to pull from Snowflake history per table
QUERY_HISTORY_LIMIT = 100


def _s3():
    """Create the S3 client only when batch mode is used."""
    import boto3
    return boto3.client("s3", region_name=AWS_REGION)


# =============================================================================
# STEP A — DDL CONVERSION (Snowflake → Redshift syntax & data types)
# =============================================================================

def convert_create_statement(sql: str, schema: str) -> str:
    """Replace Snowflake CREATE variants with Redshift-compatible CREATE."""
    sql = re.sub(
        r"create\s+or\s+replace\s+transient\s+table\s+(\w+)",
        rf"DROP TABLE IF EXISTS {schema}.\1;\n\nCREATE TABLE {schema}.\1",
        sql, flags=re.IGNORECASE,
    )
    sql = re.sub(
        r"create\s+or\s+replace\s+table\s+(\w+)",
        rf"DROP TABLE IF EXISTS {schema}.\1;\n\nCREATE TABLE {schema}.\1",
        sql, flags=re.IGNORECASE,
    )
    return sql


def convert_datatypes(sql: str) -> str:
    """
    Map every Snowflake-only data type to its closest Redshift equivalent.

    Numeric
      NUMBER / NUMBER(p,s)       → NUMERIC / NUMERIC(p,s)
      BYTEINT                    → SMALLINT

    String / semi-structured
      VARCHAR(n > 65535)         → SUPER
      VARIANT / OBJECT / ARRAY   → SUPER
      STRING / TEXT              → VARCHAR(65535)
      NVARCHAR / NCHAR           → VARCHAR / CHAR

    Date / Time
      TIMESTAMP_NTZ(n)           → TIMESTAMP
      TIMESTAMP_TZ(n) / LTZ(n)   → TIMESTAMPTZ
      DATETIME                   → TIMESTAMP
      TIME(n)                    → TIME

    Binary
      BINARY / VARBINARY         → VARBYTE
    """
    # ── Numeric ──────────────────────────────────────────────────────────────
    sql = re.sub(r"\bNUMBER\b", "NUMERIC", sql, flags=re.IGNORECASE)
    sql = re.sub(r"\bBYTEINT\b", "SMALLINT", sql, flags=re.IGNORECASE)

    # ── String / semi-structured ──────────────────────────────────────────────
    sql = re.sub(
        r"VARCHAR\((\d+)\)",
        lambda m: "SUPER" if int(m.group(1)) > 65535 else f"VARCHAR({m.group(1)})",
        sql, flags=re.IGNORECASE,
    )
    sql = re.sub(r"\bVARIANT\b", "SUPER", sql, flags=re.IGNORECASE)
    sql = re.sub(r"\bOBJECT\b", "SUPER", sql, flags=re.IGNORECASE)
    sql = re.sub(r"\bARRAY\b", "SUPER", sql, flags=re.IGNORECASE)
    sql = re.sub(r"\bSTRING\b", "VARCHAR(65535)", sql, flags=re.IGNORECASE)
    sql = re.sub(r"\bTEXT\b", "VARCHAR(65535)", sql, flags=re.IGNORECASE)
    sql = re.sub(r"\bNVARCHAR\b", "VARCHAR", sql, flags=re.IGNORECASE)
    sql = re.sub(r"\bNCHAR\b", "CHAR", sql, flags=re.IGNORECASE)

    # ── Date / Time ───────────────────────────────────────────────────────────
    sql = re.sub(r"TIMESTAMP_NTZ\(\d+\)", "TIMESTAMP", sql, flags=re.IGNORECASE)
    sql = re.sub(r"TIMESTAMP_TZ\(\d+\)", "TIMESTAMPTZ", sql, flags=re.IGNORECASE)
    sql = re.sub(r"TIMESTAMP_LTZ\(\d+\)", "TIMESTAMPTZ", sql, flags=re.IGNORECASE)
    sql = re.sub(r"\bDATETIME\b", "TIMESTAMP", sql, flags=re.IGNORECASE)
    sql = re.sub(r"\bTIME\(\d+\)\b", "TIME", sql, flags=re.IGNORECASE)

    # ── Binary ────────────────────────────────────────────────────────────────
    sql = re.sub(r"\bVARBINARY\b", "VARBYTE", sql, flags=re.IGNORECASE)
    sql = re.sub(r"\bBINARY\b", "VARBYTE", sql, flags=re.IGNORECASE)

    return sql


def fix_timestamp_columns(sql: str) -> str:
    """Strip unsupported DEFAULT expressions from TIMESTAMP columns."""
    cleaned_lines = []
    for line in sql.splitlines():
        if "TIMESTAMP" in line.upper() and "DEFAULT" in line.upper():
            col = line.strip().split()[0]
            comma = "," if line.strip().endswith(",") else ""
            line = f"\t{col} TIMESTAMP{comma}"
        cleaned_lines.append(line)
    return "\n".join(cleaned_lines)


def clean_misc(sql: str) -> str:
    """Remove Snowflake-only clauses that Redshift does not understand."""
    sql = re.sub(r"CLUSTER BY\s*\([^)]+\)", "", sql, flags=re.IGNORECASE)
    sql = re.sub(r"COMMENT\s+'[^']*'", "", sql, flags=re.IGNORECASE)
    sql = re.sub(r"\bCOPY GRANTS\b", "", sql, flags=re.IGNORECASE)
    sql = re.sub(r"\bENABLE_SCHEMA_EVOLUTION\s*=\s*\w+\b", "", sql, flags=re.IGNORECASE)
    sql = re.sub(r"\bDATA_RETENTION_TIME_IN_DAYS\s*=\s*\d+\b", "", sql, flags=re.IGNORECASE)
    return sql


# =============================================================================
# STEP B — INTELLIGENT DISTKEY / SORTKEY ANALYSIS VIA SNOWFLAKE QUERY HISTORY
# =============================================================================

def fetch_best_keys(table_name: str):
    """
    Connect to Snowflake ACCOUNT_USAGE.QUERY_HISTORY, parse the last N SELECT
    queries that touch *table_name*, and return the best (distkey, sortkey)
    candidates based on JOIN and WHERE column frequency.
    Returns (None, None) when Snowflake cannot be reached.
    """
    print(f"\n   [Key Analysis] Connecting to Snowflake for table: {table_name}")
    join_scores = defaultdict(int)
    where_scores = defaultdict(int)

    try:
        conn = snowflake.connector.connect(**SNOWFLAKE_CREDS)
        cursor = conn.cursor()

        history_query = f"""
            SELECT query_text
            FROM snowflake.account_usage.query_history
            WHERE query_type        = 'SELECT'
              AND execution_status  = 'SUCCESS'
              AND query_text ILIKE '%{table_name}%'
            ORDER BY start_time DESC
            LIMIT {QUERY_HISTORY_LIMIT};
        """
        cursor.execute(history_query)
        query_logs = cursor.fetchall()
        cursor.close()
        conn.close()

        print(f"   [Key Analysis] Fetched {len(query_logs)} queries. Parsing ASTs...")

        for (sql_text,) in query_logs:
            try:
                parsed = sqlglot.parse_one(sql_text, read="snowflake")

                for where in parsed.find_all(exp.Where):
                    for col in where.find_all(exp.Column):
                        where_scores[col.name.lower()] += 1

                for join in parsed.find_all(exp.Join):
                    for col in join.find_all(exp.Column):
                        join_scores[col.name.lower()] += 1

            except Exception:
                continue  # skip unparseable queries

    except Exception as e:
        print(f"   [Key Analysis] WARNING: Could not connect to Snowflake ({e}). Falling back to AUTO.")
        return None, None

    best_distkey = (
        sorted(join_scores.items(), key=lambda x: x[1], reverse=True)[0][0]
        if join_scores else None
    )
    best_sortkey = (
        sorted(where_scores.items(), key=lambda x: x[1], reverse=True)[0][0]
        if where_scores else None
    )

    if best_distkey:
        print(f"   [Key Analysis] → DISTKEY candidate : {best_distkey} (score: {join_scores[best_distkey]})")
    if best_sortkey:
        print(f"   [Key Analysis] → SORTKEY candidate : {best_sortkey} (score: {where_scores[best_sortkey]})")

    return best_distkey, best_sortkey


# =============================================================================
# STEP C — FINALIZE DDL (append distribution / sort strategy)
# =============================================================================

def finalize_table(sql: str, distkey, sortkey) -> str:
    """Append DISTSTYLE / DISTKEY and SORTKEY clauses, then close the statement."""
    sql = sql.rstrip(";\n").rstrip()

    if distkey:
        sql += f"\nDISTSTYLE KEY\nDISTKEY ({distkey})"
    else:
        sql += "\nDISTSTYLE AUTO"

    if sortkey:
        sql += f"\nSORTKEY ({sortkey});\n"
    else:
        sql += "\nSORTKEY AUTO;\n"

    return sql


def extract_table_name(ddl: str):
    """Best-effort extraction of the bare table name from a CREATE statement."""
    match = re.search(
        r"create\s+(?:or\s+replace\s+)?(?:transient\s+)?table\s+(?:\w+\.)?(\w+)",
        ddl, flags=re.IGNORECASE,
    )
    return match.group(1) if match else None


# =============================================================================
# STEP D — OPTIONAL BATCH MODE: read from S3 → convert → optimise → upload
# =============================================================================

def process_schema(schema: str, prefix: str) -> None:
    if not BUCKET:
        raise RuntimeError("Set DDL_S3_BUCKET (and DDL_S3_OUTPUT_BASE) to use S3 batch mode.")
    s3 = _s3()
    print(f"\nProcessing schema : {schema}\nS3 prefix         : {prefix}")

    for page in s3.get_paginator("list_objects_v2").paginate(Bucket=BUCKET, Prefix=prefix):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if not key.endswith(".txt"):
                continue
            print(f"\n--- File: {key}")
            raw_ddl = s3.get_object(Bucket=BUCKET, Key=key)["Body"].read().decode("utf-8")

            ddl = convert_create_statement(raw_ddl, schema)
            ddl = convert_datatypes(ddl)
            ddl = fix_timestamp_columns(ddl)
            ddl = clean_misc(ddl)

            table_name = extract_table_name(ddl) or extract_table_name(raw_ddl)
            distkey, sortkey = fetch_best_keys(table_name) if table_name else (None, None)
            ddl = finalize_table(ddl, distkey, sortkey)

            output_key = OUTPUT_BASE + schema + "_DDL/" + key.split("/")[-1]
            s3.put_object(Bucket=BUCKET, Key=output_key, Body=ddl.encode("utf-8"))
            print(f"   Uploaded → s3://{BUCKET}/{output_key}")


if __name__ == "__main__":
    for _schema, _path in INPUT_PATHS.items():
        process_schema(_schema, _path)
    print("\n✓ Snowflake → Redshift conversion completed successfully.")
