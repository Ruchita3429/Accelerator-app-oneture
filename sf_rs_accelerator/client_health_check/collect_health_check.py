#!/usr/bin/env python3
"""Run the POC Snowflake health checks locally; never upload the results."""
import argparse
import base64
import csv
from datetime import date, datetime, timezone
from decimal import Decimal
import getpass
import json
import os
from pathlib import Path
import re
import time
import uuid
import zipfile


CATALOG_PATH = Path(__file__).with_name("checks.json")
RESULT_NOTICE = """These are diagnostic observations, not a certification of account health.
Failed or unavailable checks are not zero values. Empty results can reflect role
visibility, Account Usage latency, or no matching activity. Review manifest.json
for each check's status, SQL, limitations, row cap and query ID.

The package contains account/object/user names, query IDs and operational metrics.
Raw query text and source error messages are excluded unless explicitly enabled.
Credentials and client connection configuration are never included by this tool.
No business-table rows are queried. No results are uploaded automatically.
Review the contents before sharing through your agreed channel.

Legacy dashboard calculations are preserved except for the corrected account
name/locator labels and optional removal of sensitive text. In particular, hourly
warehouse cost is NOT per-query cost; load thresholds are heuristics; metadata
snapshots are NOT growth; LAST_ALTERED is NOT a data-freshness guarantee. The
collector does not generate the dashboard's composite health score.
"""


def load_checks():
    checks = json.loads(CATALOG_PATH.read_text(encoding="utf-8"))["checks"]
    if len({c["id"] for c in checks}) != len(checks):
        raise ValueError("Duplicate check IDs")
    return checks


def positive_int(value):
    value = int(value)
    if value < 1:
        raise argparse.ArgumentTypeError("Must be greater than zero")
    return value


def rendered_sql(check, max_rows=1000, include_sensitive_text=False):
    sql = check["sql"].strip().rstrip(";")
    if not include_sensitive_text:
        # Only change the selected columns; keep diagnostic predicates intact.
        sql = re.sub(r"SELECT QUERY_ID, QUERY_TEXT\b", "SELECT QUERY_ID", sql)
        sql = sql.replace("SELECT USER_NAME, EVENT_TIMESTAMP, ERROR_MESSAGE",
                          "SELECT USER_NAME, EVENT_TIMESTAMP, ERROR_CODE")
        sql = sql.replace("SELECT QUERY_ID, ERROR_MESSAGE", "SELECT QUERY_ID, ERROR_CODE")
    if sql.upper() == "SHOW RESOURCE MONITORS":
        return sql
    if not re.match(r"^(SELECT|WITH)\b", sql, re.I):
        raise ValueError(f"Unexpected statement for {check['id']}")
    # One extra result lets the collector explicitly report truncation.
    return f"SELECT * FROM (\n{sql}\n) AS health_check_result LIMIT {max_rows + 1}"


def sql_script(checks, max_rows=1000, include_sensitive_text=False):
    parts = ["-- DataBridge client-run Snowflake health checks\n"
             "-- Select your approved role and warehouse in Snowsight first.\n"
             "-- Run each named statement and download its result as CSV.\n"
             "-- No tables, grants or account settings are modified.\n"
             "-- Original 7/30-day windows are preserved. Some checks scan all available history.\n"
             "-- SELECT results are capped at max_rows + 1; SHOW uses Snowflake's own limits.\n"
             "-- An empty result does not certify health. Record any failed statements.\n"
             "-- Operational names/IDs are included; review results before sharing.\n"
             f"-- Raw query/error text included: {include_sensitive_text}\n"]
    for check in checks:
        parts.append(f"-- CHECK: {check['id']} | {check['section']} | {check['label']}")
        parts.extend("-- NOTE: " + note for note in check["notes"])
        parts.append(rendered_sql(check, max_rows, include_sensitive_text) + ";\n")
    return "\n".join(parts)


def serializable(value):
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, Decimal):
        return str(value)  # Avoid silently rounding monetary/credit values.
    if isinstance(value, bytes):
        return {"base64": base64.b64encode(value).decode("ascii")}
    raise TypeError(f"Unsupported result type: {type(value).__name__}")


def csv_cell(value):
    if value is None:
        return ""
    if isinstance(value, (dict, list, tuple)):
        value = json.dumps(value, default=serializable, ensure_ascii=False)
    elif isinstance(value, (date, datetime, Decimal, bytes)):
        value = serializable(value)
        if isinstance(value, dict):
            value = json.dumps(value)
    # Keep operational names/query text from becoming formulas in spreadsheets.
    if isinstance(value, str) and value.lstrip().startswith(("=", "+", "-", "@")):
        return "'" + value
    return value


def write_json(path, data):
    path.write_text(json.dumps(data, indent=2, ensure_ascii=False,
                               default=serializable, allow_nan=False) + "\n", encoding="utf-8")


def collect(connection, checks, output, max_rows=1000, timeout=120,
            include_sensitive_text=False):
    """Continue after individual query errors and package only this new run."""
    output = Path(output)
    output.mkdir(parents=True, exist_ok=False)
    (output / "csv").mkdir()
    started = datetime.now(timezone.utc).isoformat()
    results, manifest_checks = {}, []
    for check in checks:
        sql = rendered_sql(check, max_rows, include_sensitive_text)
        item = {k: v for k, v in check.items() if k != "sql"}
        item.update(sql=sql, status="failed", row_count=None, truncated=False,
                    query_id=None, error=None)
        cursor = None
        start = time.monotonic()
        try:
            cursor = connection.cursor()
            cursor.execute(sql, timeout=timeout)
            item["query_id"] = getattr(cursor, "sfqid", None)
            columns = [d[0] for d in cursor.description or []]
            rows = cursor.fetchmany(max_rows + 1)
            item["truncated"] = len(rows) > max_rows
            rows = rows[:max_rows]
            # Normalize before writing either result file, so unsupported values
            # make a failed check rather than an apparently successful export.
            rows = json.loads(json.dumps(rows, default=serializable, allow_nan=False))
            with (output / "csv" / (check["id"] + ".csv")).open("w", newline="", encoding="utf-8") as handle:
                writer = csv.writer(handle)
                writer.writerow([csv_cell(c) for c in columns])
                writer.writerows([[csv_cell(v) for v in row] for row in rows])
            results[check["id"]] = {"columns": columns, "rows": rows}
            item.update(status="ok" if rows else "empty", row_count=len(rows), columns=columns)
        except Exception as exc:
            # Don't put connector exception text (which can contain SQL or
            # connection information) in the client's shareable archive.
            item["query_id"] = getattr(cursor, "sfqid", None) if cursor is not None else None
            item["error"] = {"type": type(exc).__name__, "code": getattr(exc, "errno", None),
                             "sqlstate": getattr(exc, "sqlstate", None),
                             "hint": "Client administrator: inspect this query ID in Snowflake Query History; check role access, view availability and timeout."}
        finally:
            if cursor is not None:
                try:
                    cursor.close()
                except Exception:
                    pass
        item["duration_seconds"] = round(time.monotonic() - start, 3)
        manifest_checks.append(item)
        print(f"{check['id']}: {item['status']}" + (" (row cap reached)" if item["truncated"] else ""))

    failed = sum(c["status"] == "failed" for c in manifest_checks)
    manifest = {"format_version": 1, "started_at_utc": started,
                "finished_at_utc": datetime.now(timezone.utc).isoformat(),
                "collection_status": "partial" if failed else "complete",
                "selected_checks": len(checks), "failed_checks": failed,
                "max_rows_per_check": max_rows, "timeout_seconds_per_check": timeout,
                "include_sensitive_text": include_sensitive_text,
                "checks": manifest_checks}
    write_json(output / "manifest.json", manifest)
    write_json(output / "results.json", results)
    (output / "executed_checks.sql").write_text(sql_script(checks, max_rows, include_sensitive_text), encoding="utf-8")
    (output / "README.txt").write_text(RESULT_NOTICE, encoding="utf-8")
    lines = ["# Snowflake health-check collection", "",
             f"Collection: **{manifest['collection_status']}**. {len(checks) - failed}/{len(checks)} checks executed successfully.",
             "This is execution coverage, not a health score.", "",
             "| Check | Status | Exported rows | Capped |", "|---|---|---:|---|"]
    for c in manifest_checks:
        lines.append(f"| {c['id']} | {c['status']} | {c['row_count'] if c['row_count'] is not None else 'unavailable'} | {c['truncated']} |")
    lines.extend(["", RESULT_NOTICE])
    (output / "summary.md").write_text("\n".join(lines), encoding="utf-8")
    archive = output.parent / (output.name + ".zip")
    with zipfile.ZipFile(archive, "x", zipfile.ZIP_DEFLATED) as bundle:
        for path in sorted(output.rglob("*")):
            if path.is_file():
                bundle.write(path, arcname=str(Path(output.name) / path.relative_to(output)))
    return manifest, archive


def connect(args):
    import snowflake.connector

    params = {"login_timeout": 60, "network_timeout": args.timeout,
              "session_parameters": {"QUERY_TAG": "databridge_client_health_check_v1",
                                     "STATEMENT_TIMEOUT_IN_SECONDS": args.timeout,
                                     "STATEMENT_QUEUED_TIMEOUT_IN_SECONDS": args.timeout}}
    if args.connection_name:
        params["connection_name"] = args.connection_name
    else:
        if not args.account or not args.user:
            raise ValueError("Supply --connection-name, or --account and --user")
        params.update(account=args.account, user=args.user, authenticator=args.authenticator)
        if args.authenticator == "snowflake":
            params["password"] = os.environ.get("SNOWFLAKE_PASSWORD") or getpass.getpass("Snowflake password: ")
    for key in ("warehouse", "role"):
        if getattr(args, key):
            params[key] = getattr(args, key)
    return snowflake.connector.connect(**params)


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--connection-name", help="Existing local Snowflake connections.toml profile (including key-pair/SSO settings)")
    parser.add_argument("--account", default=os.environ.get("SNOWFLAKE_ACCOUNT"))
    parser.add_argument("--user", default=os.environ.get("SNOWFLAKE_USER"))
    parser.add_argument("--warehouse", default=os.environ.get("SNOWFLAKE_WAREHOUSE"))
    parser.add_argument("--role", default=os.environ.get("SNOWFLAKE_ROLE"))
    parser.add_argument("--authenticator", choices=["externalbrowser", "snowflake"], default="externalbrowser")
    parser.add_argument("--output", type=Path, help="New output directory; existing directories are never overwritten")
    parser.add_argument("--max-rows", type=positive_int, default=1000)
    parser.add_argument("--timeout", type=positive_int, default=120, help="Seconds per check (default: 120)")
    parser.add_argument("--checks", help="Comma-separated check IDs; omit to run all")
    parser.add_argument("--include-sensitive-text", action="store_true", help="Include source query text/error messages; review before sharing")
    parser.add_argument("--list", action="store_true", help="List checks without connecting")
    parser.add_argument("--sql-only", type=Path, help="Write a SQL worksheet without connecting; refuses to overwrite")
    args = parser.parse_args(argv)
    checks = load_checks()
    if args.checks:
        requested = {v.strip() for v in args.checks.split(",")}
        unknown = requested - {c["id"] for c in checks}
        if unknown:
            parser.error("Unknown check IDs: " + ", ".join(sorted(unknown)))
        checks = [c for c in checks if c["id"] in requested]
    if args.list:
        for check in checks:
            print(f"{check['id']:35} {check['section']} | {check['label']}")
        return 0
    if args.sql_only:
        with args.sql_only.open("x", encoding="utf-8") as handle:
            handle.write(sql_script(checks, args.max_rows, args.include_sensitive_text))
        print(f"SQL worksheet: {args.sql_only}")
        return 0
    output = args.output or Path("health_check_" + datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ_") + uuid.uuid4().hex[:8])
    if output.exists() or (output.parent / (output.name + ".zip")).exists():
        parser.error("Output directory or ZIP already exists; choose a new --output")
    connection = None
    try:
        connection = connect(args)
        manifest, archive = collect(connection, checks, output, args.max_rows, args.timeout, args.include_sensitive_text)
    except (ImportError, ValueError) as exc:
        print(f"Setup failed: {exc}")
        return 1
    except Exception as exc:
        print(f"Collection stopped: {type(exc).__name__} (code {getattr(exc, 'errno', 'unavailable')}). Check local setup, authentication and output permissions.")
        return 1
    finally:
        if connection is not None:
            try:
                connection.close()
            except Exception:
                pass
    print(f"Review locally: {output / 'summary.md'}")
    print(f"Share after review: {archive}")
    return 2 if manifest["failed_checks"] else 0


if __name__ == "__main__":
    raise SystemExit(main())
