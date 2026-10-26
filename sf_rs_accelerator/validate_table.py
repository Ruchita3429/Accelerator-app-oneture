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
import subprocess
import sys
import textwrap
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


def render_report(output_dir: Path, table_name: str) -> None:
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

    # ── Summary section ────────────────────────────────────────────────────
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
# CLI
# ---------------------------------------------------------------------------

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Validate a table from Redshift or Snowflake against a Snowflake target.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=textwrap.dedent(__doc__),
    )

    # Required
    parser.add_argument(
        "--source", required=True, choices=SUPPORTED_SOURCES,
        help="Source platform: 'redshift' or 'snowflake'.",
    )
    parser.add_argument(
        "--table", required=True,
        help=(
            "Fully qualified source table name, e.g. "
            "'analytics_db.public.fact_sales' or 'mydb.myschema.orders'."
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

    return parser.parse_args()


def validate_args(args: argparse.Namespace) -> None:
    """Raise if required arguments for the chosen source are missing."""
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
