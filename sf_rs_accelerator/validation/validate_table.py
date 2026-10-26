#!/usr/bin/env python3
"""
validate_table.py
-----------------
Cross-platform table validator using the `snowflake-data-validation` (sdv) CLI.
Supports Redshift → Snowflake and Snowflake → Snowflake validation modes.

Usage examples
--------------
# Redshift source → Snowflake target
python validate_table.py \
    --source redshift \
    --table "analytics_db.public.fact_sales" \
    --source-host redshift-cluster.us-east-1.redshift.amazonaws.com \
    --source-port 5439 \
    --source-user admin \
    --source-password "s3cr3t" \
    --source-database analytics_db \
    --sf-connection my_snowflake_conn \
    --output-dir ./reports

# Snowflake → Snowflake
python validate_table.py \
    --source snowflake \
    --table "mydb.myschema.orders" \
    --sf-connection my_snowflake_conn \
    --output-dir ./reports

# Optional flags
    --target-schema  PUBLIC          # override target schema
    --target-database ANALYTICS_PROD # override target database
    --where-clause "created_date >= '2024-01-01'"
    --row-validation                 # enable row-level validation (expensive)
    --threads 8
    --tolerance 0.01
"""

import argparse
import csv
import os
import random
import subprocess
import sys
import textwrap
import time
from datetime import datetime
from pathlib import Path

import yaml  # pip install pyyaml


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

SUPPORTED_SOURCES = ("redshift", "snowflake")

REPORT_COLUMNS = [
    "VALIDATION_TYPE", "TABLE", "COLUMN_VALIDATED",
    "EVALUATION_CRITERIA", "SOURCE_VALUE", "SNOWFLAKE_VALUE",
    "STATUS", "COMMENTS",
]

STATUS_EMOJI = {
    "SUCCESS":          "✅",
    "FAILURE":          "❌",
    "WARNING":          "⚠️ ",
    "NOT_FOUND_SOURCE": "🔍",
    "NOT_FOUND_TARGET": "🔍",
}

# ---------------------------------------------------------------------------
# Demo data  (used when --demo flag is set)
# ---------------------------------------------------------------------------

DEMO_ROWS = [
    # ── Schema checks ──────────────────────────────────────────────────────
    ("SCHEMA", "", "COLUMN_COUNT",  "42",              "42",              "SUCCESS", ""),
    ("SCHEMA", "sale_id",      "DATA_TYPE",    "INTEGER",         "NUMBER(38,0)",    "SUCCESS", "Snowflake uses NUMBER for INTEGER columns"),
    ("SCHEMA", "sale_date",    "DATA_TYPE",    "DATE",            "DATE",            "SUCCESS", ""),
    ("SCHEMA", "customer_id",  "DATA_TYPE",    "INTEGER",         "NUMBER(38,0)",    "SUCCESS", ""),
    ("SCHEMA", "product_id",   "DATA_TYPE",    "INTEGER",         "NUMBER(38,0)",    "SUCCESS", ""),
    ("SCHEMA", "region",       "DATA_TYPE",    "VARCHAR(50)",     "TEXT",            "SUCCESS", "Snowflake normalises VARCHAR to TEXT"),
    ("SCHEMA", "sales_amount", "DATA_TYPE",    "DECIMAL(18,2)",   "NUMBER(18,2)",    "SUCCESS", ""),
    ("SCHEMA", "discount_pct", "DATA_TYPE",    "FLOAT",           "FLOAT",           "SUCCESS", ""),
    ("SCHEMA", "is_returned",  "DATA_TYPE",    "BOOLEAN",         "BOOLEAN",         "SUCCESS", ""),
    ("SCHEMA", "created_at",   "DATA_TYPE",    "TIMESTAMP",       "TIMESTAMP_NTZ(9)","WARNING", "Snowflake stores as TIMESTAMP_NTZ; verify timezone handling"),
    ("SCHEMA", "promo_code",   "DATA_TYPE",    "VARCHAR(20)",     "TEXT",            "SUCCESS", ""),
    ("SCHEMA", "store_id",     "DATA_TYPE",    "SMALLINT",        "NUMBER(38,0)",    "WARNING", "SMALLINT precision differs; confirm no overflow"),
    # ── Metrics checks ─────────────────────────────────────────────────────
    ("METRICS", "",             "ROW_COUNT",    "99999911",        "100000000",       "FAILURE", "Row count mismatch of 89 rows; investigate missing records in Snowflake target"),
    ("METRICS", "sale_id",      "NULL_COUNT",   "0",               "0",               "SUCCESS", ""),
    ("METRICS", "customer_id",  "NULL_COUNT",   "0",               "0",               "SUCCESS", ""),
    ("METRICS", "sales_amount", "NULL_COUNT",   "142",             "142",             "SUCCESS", ""),
    ("METRICS", "discount_pct", "NULL_COUNT",   "9823",            "9823",            "SUCCESS", ""),
    ("METRICS", "promo_code",   "NULL_COUNT",   "11204",           "11205",           "FAILURE", "1 extra NULL in Snowflake target; possible ETL trim issue on load"),
    ("METRICS", "sales_amount", "SUM",          "4823917642.87",   "4823917642.87",   "SUCCESS", ""),
    ("METRICS", "discount_pct", "AVG",          "0.1423",          "0.1423",          "SUCCESS", ""),
    ("METRICS", "sales_amount", "MIN",          "0.01",            "0.01",            "SUCCESS", ""),
    ("METRICS", "sales_amount", "MAX",          "99999.99",        "99999.99",        "SUCCESS", ""),
    ("METRICS", "sales_amount", "AVG",          "261.14",          "261.14",          "SUCCESS", ""),
    ("METRICS", "sale_date",    "MIN",          "2019-01-01",      "2019-01-01",      "SUCCESS", ""),
    ("METRICS", "sale_date",    "MAX",          "2026-04-30",      "2026-04-30",      "SUCCESS", ""),
    ("METRICS", "region",       "DISTINCT_COUNT","12",             "12",              "SUCCESS", ""),
    ("METRICS", "channel",      "DISTINCT_COUNT","5",              "5",               "SUCCESS", ""),
    ("METRICS", "store_id",     "DISTINCT_COUNT","318",            "318",             "SUCCESS", ""),
    ("METRICS", "is_returned",  "SUM",          "203847",          "203847",          "SUCCESS", ""),
    ("METRICS", "created_at",   "MAX",          "2026-05-04 23:58:11","2026-05-04 23:58:11","SUCCESS",""),
    ("METRICS", "sales_amount", "STDDEV",       "187.4312",        "187.4318",        "FAILURE", "Stddev deviation of 0.0006 exceeds tolerance 0.0001; minor floating-point rounding during migration"),
]


# ---------------------------------------------------------------------------
# Config builder
# ---------------------------------------------------------------------------

def build_config(args: argparse.Namespace) -> dict:
    """Return a YAML-serialisable config dict for the sdv CLI."""

    # ── Source connection ──────────────────────────────────────────────────
    if args.source == "redshift":
        source_connection = {
            "mode": "credentials",
            "host": args.source_host,
            "port": args.source_port,
            "username": args.source_user,
            "password": args.source_password,
            "database": args.source_database,
        }
        source_platform = "Redshift"
    else:  # snowflake → snowflake
        source_connection = {
            "mode": "name",
            "name": args.sf_connection,
        }
        source_platform = "Snowflake"

    # ── Target connection (always Snowflake) ───────────────────────────────
    if args.sf_connection:
        target_connection = {"mode": "name", "name": args.sf_connection}
    else:
        target_connection = {"mode": "default"}

    # ── Table entry ────────────────────────────────────────────────────────
    table_entry: dict = {
        "fully_qualified_name": args.table,
        "use_column_selection_as_exclude_list": False,
        "column_selection_list": [],
    }

    if args.target_database:
        table_entry["target_database"] = args.target_database
    if args.target_schema:
        table_entry["target_schema"] = args.target_schema
    if args.target_name:
        table_entry["target_name"] = args.target_name
    if args.where_clause:
        table_entry["where_clause"] = args.where_clause
        table_entry["target_where_clause"] = args.where_clause
    if args.row_validation:
        table_entry["chunk_number"] = 10   # sensible default for row-level

    # ── Assemble final config ──────────────────────────────────────────────
    config = {
        "source_platform": source_platform,
        "target_platform": "Snowflake",
        "output_directory_path": str(args.output_dir),
        "max_threads": args.threads if args.threads else "auto",
        "source_connection": source_connection,
        "target_connection": target_connection,
        "validation_configuration": {
            "schema_validation": True,
            "metrics_validation": True,
            "row_validation": args.row_validation,
            "max_failed_rows_number": args.max_failed_rows,
            "exclude_metrics": False,
            "apply_metric_column_modifier": False,
        },
        "comparison_configuration": {
            "tolerance": args.tolerance,
        },
        "logging_configuration": {
            "level": "INFO",
            "console_level": "WARNING",
            "file_level": "DEBUG",
        },
        "tables": [table_entry],
    }

    return config


# ---------------------------------------------------------------------------
# SDV runner
# ---------------------------------------------------------------------------

def run_sdv(config_path: Path, source: str) -> int:
    """Invoke the sdv CLI and return its exit code."""
    cmd = [
        "sdv", source,
        "run-validation",
        "--data-validation-config-file", str(config_path),
    ]

    print(f"\n▶  Running: {' '.join(cmd)}\n{'─' * 60}")
    result = subprocess.run(cmd)
    return result.returncode


# ---------------------------------------------------------------------------
# Report renderer
# ---------------------------------------------------------------------------

def _latest_report(output_dir: Path) -> Path | None:
    """Find the most recently created validation_report.csv in output_dir."""
    reports = sorted(
        output_dir.glob("*validation_report.csv"),
        key=lambda p: p.stat().st_mtime,
        reverse=True,
    )
    return reports[0] if reports else None


def _wait_with_spinner(label: str, demo: bool = False) -> None:
    """Show an animated spinner with a label for 10-15 s (demo) or skip."""
    if not demo:
        return
    duration = random.uniform(10, 15)
    frames = ["⠋", "⠙", "⠹", "⠸", "⠼", "⠴", "⠦", "⠧", "⠇", "⠏"]
    end_time = time.time() + duration
    i = 0
    while time.time() < end_time:
        elapsed = duration - (end_time - time.time())
        print(f"\r  {frames[i % len(frames)]}  {label}  ({elapsed:.1f}s) ", end="", flush=True)
        time.sleep(0.1)
        i += 1
    print(f"\r  ✔  {label} — done{' ' * 20}")


def render_report(output_dir: Path, table_name: str, demo: bool = False) -> None:
    """Read the CSV report and print a formatted summary to stdout."""

    report_path = _latest_report(output_dir)
    if report_path is None:
        print("\n⚠️  No validation_report.csv found in output directory.")
        return

    rows = []
    with open(report_path, newline="", encoding="utf-8") as fh:
        reader = csv.DictReader(fh)
        for row in reader:
            rows.append(row)

    if not rows:
        print("\nℹ️  Report is empty – no validation rows found.")
        return

    # ── Aggregate counters ─────────────────────────────────────────────────
    totals: dict[str, int] = {}
    for row in rows:
        status = row.get("STATUS", "UNKNOWN").strip()
        totals[status] = totals.get(status, 0) + 1

    # ── Header banner ──────────────────────────────────────────────────────
    run_ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print("\n" + "═" * 70)
    print(f"  DATA VALIDATION REPORT")
    print(f"  Table  : {table_name}")
    print(f"  Source : {report_path.name}")
    print(f"  Run at : {run_ts}")
    print("═" * 70)

    _wait_with_spinner("Connecting to Redshift source", demo)
    _wait_with_spinner("Connecting to Snowflake target", demo)

    # ── Summary section ────────────────────────────────────────────────────
    _wait_with_spinner("Running schema validation checks", demo)
    print("\n📊  SUMMARY")
    print("─" * 40)
    for status, count in sorted(totals.items()):
        emoji = STATUS_EMOJI.get(status, "  ")
        print(f"  {emoji}  {status:<22} {count:>5}")
    print(f"\n  Total checks : {len(rows)}")
    passed = totals.get("SUCCESS", 0)
    failed = len(rows) - passed
    pct = (passed / len(rows) * 100) if rows else 0
    print(f"  Pass rate    : {pct:.1f}%  ({passed} / {len(rows)})")

    # ── Failures detail ────────────────────────────────────────────────────
    failures = [
        r for r in rows
        if r.get("STATUS", "").strip() not in ("SUCCESS",)
    ]

    _wait_with_spinner("Running metrics & row-count validation", demo)

    if failures:
        print("\n\n🔎  ISSUES DETECTED")
        print("─" * 70)
        for r in failures:
            status  = r.get("STATUS", "").strip()
            vtype   = r.get("VALIDATION_TYPE", "").strip()
            col     = r.get("COLUMN_VALIDATED", "").strip()
            crit    = r.get("EVALUATION_CRITERIA", "").strip()
            src_val = r.get("SOURCE_VALUE", "").strip()
            sf_val  = r.get("SNOWFLAKE_VALUE", "").strip()
            comment = r.get("COMMENTS", "").strip()

            emoji = STATUS_EMOJI.get(status, "  ")
            print(f"\n  {emoji} [{vtype}]  Column: {col or '(table-level)'}  |  Check: {crit}")
            print(f"       Source    : {src_val or '–'}")
            print(f"       Snowflake : {sf_val  or '–'}")
            if comment:
                wrapped = textwrap.fill(comment, width=60,
                                        initial_indent="       Note      : ",
                                        subsequent_indent="                   ")
                print(wrapped)
    else:
        print("\n\n✅  All validation checks passed!")

    # ── Footer ─────────────────────────────────────────────────────────────
    print("\n" + "═" * 70)
    print(f"  Full report : {report_path}")
    print("═" * 70 + "\n")


# ---------------------------------------------------------------------------
# Demo report seeder
# ---------------------------------------------------------------------------

def seed_demo_report(output_dir: Path, table: str) -> Path:
    """Write DEMO_ROWS into a fresh validation_report CSV and return its path."""
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    report_path = output_dir / f"{ts}_validation_report.csv"

    with open(report_path, "w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=REPORT_COLUMNS)
        writer.writeheader()
        for (vtype, col, crit, src, sf, status, comment) in DEMO_ROWS:
            writer.writerow({
                "VALIDATION_TYPE":    vtype,
                "TABLE":              table,
                "COLUMN_VALIDATED":   col,
                "EVALUATION_CRITERIA": crit,
                "SOURCE_VALUE":       src,
                "SNOWFLAKE_VALUE":    sf,
                "STATUS":             status,
                "COMMENTS":           comment,
            })

    print(f"✅  Demo report seeded → {report_path}")
    return report_path


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Validate a table from Redshift or Snowflake against a Snowflake target.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=textwrap.dedent(__doc__),
    )

    # Required (not needed in demo mode)
    parser.add_argument(
        "--source", choices=SUPPORTED_SOURCES,
        help="Source platform: 'redshift' or 'snowflake'. Not required with --demo.",
    )
    parser.add_argument(
        "--table",
        help=(
            "Fully qualified source table name, e.g. "
            "'analytics_db.public.fact_sales'. Not required with --demo."
        ),
    )

    # Redshift connection (required only when --source redshift)
    rs = parser.add_argument_group("Redshift source connection")
    rs.add_argument("--source-host",     help="Redshift cluster endpoint.")
    rs.add_argument("--source-port",     type=int, default=5439)
    rs.add_argument("--source-user",     help="Redshift username.")
    rs.add_argument("--source-password", help="Redshift password.")
    rs.add_argument("--source-database", help="Redshift database name.")

    # Snowflake target / source
    sf = parser.add_argument_group("Snowflake connection")
    sf.add_argument(
        "--sf-connection",
        help=(
            "Named Snowflake connection (from ~/.snowflake/connections.toml). "
            "Omit to use the default connection."
        ),
    )

    # Target overrides
    tgt = parser.add_argument_group("Target overrides (optional)")
    tgt.add_argument("--target-database", help="Override target database name.")
    tgt.add_argument("--target-schema",   help="Override target schema name.")
    tgt.add_argument("--target-name",     help="Override target table name.")

    # Validation options
    val = parser.add_argument_group("Validation options")
    val.add_argument(
        "--where-clause",
        help="SQL WHERE clause (no 'WHERE' keyword) applied to both source and target.",
    )
    val.add_argument(
        "--row-validation", action="store_true",
        help="Enable row-level MD5 validation (resource-intensive).",
    )
    val.add_argument(
        "--max-failed-rows", type=int, default=100,
        help="Max failed rows to report in row validation (default: 100).",
    )
    val.add_argument(
        "--tolerance", type=float, default=0.01,
        help="Metrics comparison tolerance, e.g. 0.01 = 1%% (default: 0.01).",
    )
    val.add_argument(
        "--threads", type=int, default=None,
        help="Number of parallel threads (default: auto).",
    )

    # Output
    parser.add_argument(
        "--output-dir", type=Path, default=Path("./validation_reports"),
        help="Directory for reports and config files (default: ./validation_reports).",
    )
    parser.add_argument(
        "--config-only", action="store_true",
        help="Write the YAML config file and exit without running sdv.",
    )
    parser.add_argument(
        "--demo", action="store_true",
        help=(
            "Demo mode: seed a realistic dummy report for sales_fact_table "
            "and render it. No DB credentials or sdv install required."
        ),
    )

    return parser.parse_args()


def validate_args(args: argparse.Namespace) -> None:
    """Raise if required arguments for the chosen source are missing."""
    if args.demo:
        return  # demo mode needs no credentials

    if not args.source:
        sys.exit("❌  --source is required (use --demo to run without credentials).")
    if not args.table:
        sys.exit("❌  --table is required (use --demo to run without credentials).")

    if args.source == "redshift":
        missing = [
            f for f in ("source_host", "source_user", "source_password", "source_database")
            if not getattr(args, f)
        ]
        if missing:
            sys.exit(
                f"❌  Redshift source requires: {', '.join('--' + m.replace('_', '-') for m in missing)}"
            )


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main() -> None:
    args = parse_args()
    validate_args(args)

    # Ensure output directory exists
    args.output_dir.mkdir(parents=True, exist_ok=True)

    # ── Demo mode: seed dummy report and render, then exit ─────────────────
    if args.demo:
        demo_table = args.table or "analytics_db.public.sales_fact_table"
        print(f"\n🎬  Demo mode — seeding dummy report for: {demo_table}")
        print(f"    Source  : Redshift  →  Target: Snowflake")
        print(f"    Output  : {args.output_dir}\n")
        seed_demo_report(args.output_dir, demo_table)
        render_report(args.output_dir, demo_table, demo=True)
        return

    # Build and write config
    config = build_config(args)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    safe_table = args.table.replace(".", "_")
    config_path = args.output_dir / f"config_{safe_table}_{ts}.yaml"

    with open(config_path, "w", encoding="utf-8") as fh:
        yaml.dump(config, fh, default_flow_style=False, sort_keys=False)

    print(f"✅  Config written → {config_path}")

    if args.config_only:
        print("\nℹ️  --config-only flag set. Skipping sdv execution.")
        return

    # Check sdv is installed
    probe = subprocess.run(["sdv", "--version"], capture_output=True)
    if probe.returncode != 0:
        sys.exit(
            "❌  'sdv' command not found.\n"
            "    Install with: pip install snowflake-data-validation[redshift]"
        )

    print(f"sdv {probe.stdout.decode().strip()}")

    # Run validation
    exit_code = run_sdv(config_path, args.source)

    # Render human-readable report
    render_report(args.output_dir, args.table)

    sys.exit(exit_code)


if __name__ == "__main__":
    main()