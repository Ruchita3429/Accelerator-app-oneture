#!/usr/bin/env python3
"""Collect BigQuery health observations locally and package results for review."""
import argparse
import csv
from datetime import date, datetime, timezone
from decimal import Decimal
import json
from pathlib import Path
import re
import time
import uuid
import zipfile

try:
    from .checks import CHECKS
except ImportError:
    from checks import CHECKS


NOTICE = """BigQuery diagnostic observations; not a health or security certification.
Coverage is one project and one location. Job history covers the requested window;
inventory/storage are current metadata snapshots and may have reporting delays.
Failed checks are unavailable, not zero. Empty results are not proof of health.
Usage bytes and slot milliseconds are not billed currency or slot utilization.

Results include project/dataset/table/column names, principal emails, job IDs,
reservation IDs and KMS/policy-tag resource names. They are not anonymized.
Raw query/error/warning text is excluded unless explicitly requested. Such text
can contain sensitive literals. Review all output before sharing.
The collector does not copy authentication files or upload results. Supplied SQL
reads metadata, not business-table rows. Query jobs can incur BigQuery charges.

This kit does not audit IAM bindings, login events, networking, billing exports,
reservation capacity/commitments, streaming ingestion or transfer configuration.
It does not compute a composite health score or modify the migration application.
"""


def positive_int(value):
    value = int(value)
    if value < 1:
        raise argparse.ArgumentTypeError("Must be greater than zero")
    return value


def validate_scope(project, location):
    # Accept standard GCP project IDs; reject SQL identifiers/injection syntax.
    if not re.fullmatch(r"[a-z][a-z0-9-]{4,28}[a-z0-9]", project or ""):
        raise ValueError("Use a standard GCP project ID (not its display name or project number)")
    region = (location or "").lower()
    if not re.fullmatch(r"(?:us|eu|[a-z]+(?:-[a-z]+)+[0-9]+)", region):
        raise ValueError("Supply a location such as US, EU, us-central1 or asia-south1 (without region-)")
    return region


def render_sql(check, project, location, days=7, max_rows=1000,
               include_sensitive_text=False, as_of=None):
    region = validate_scope(project, location)
    if not 1 <= days <= 180 or max_rows < 1:
        raise ValueError("Days must be 1–180 and max_rows must be positive")
    end = "CURRENT_TIMESTAMP()"
    if as_of is not None:
        if as_of.tzinfo is None:
            raise ValueError("as_of must have a timezone")
        end = "TIMESTAMP('" + as_of.astimezone(timezone.utc).isoformat() + "')"
    sql = check['sql'].format(
        project=project, region=region, days=days, as_of=end,
        query_text=", query AS query_text" if include_sensitive_text else "",
        error_text=", error_result.message AS error_message" if include_sensitive_text else "",
        warning_text=", query_info.resource_warning AS resource_warning" if include_sensitive_text else "",
    ).strip()
    # The additional row detects truncation without downloading all results.
    return sql + f"\nLIMIT {max_rows + 1}"


def worksheet(checks, project, location, days=7, max_rows=1000,
              include_sensitive_text=False, as_of=None):
    parts = ["-- PRIMARY CLIENT WORKFLOW: run these SQL checks in the BigQuery console; Python is not required.",
             f"-- BigQuery client health check: project={project}, location={location}, days={days}",
             "-- Set the BigQuery editor execution location to the location above.",
             "-- If using the supplied template, replace your-project-id and region-us before running.",
             "-- Run each CHECK separately and export its result as <check_id>.csv.",
             "-- SELECT-only metadata queries. Results contain operational names and identities.",
             "-- Row limit is max_rows + 1. Reaching it indicates possible truncation.",
             "-- Query editor jobs are charged according to the execution project's pricing model.",
             "-- Set maximum bytes billed in Query settings; this file does not set that option.",
             f"-- Include sensitive text: {include_sensitive_text}", ""]
    for check in checks:
        parts.extend([f"-- CHECK: {check['id']} | {check['section']}",
                      "-- NOTE: " + check['note'],
                      render_sql(check, project, location, days, max_rows, include_sensitive_text, as_of) + ";", ""])
    return "\n".join(parts)


def json_value(value):
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, Decimal):
        return str(value)
    raise TypeError(f"Unsupported result type: {type(value).__name__}")


def write_json(path, value):
    path.write_text(json.dumps(value, default=json_value, indent=2,
                               ensure_ascii=False, allow_nan=False) + "\n", encoding="utf-8")


def csv_cell(value):
    if value is None:
        return ""
    if isinstance(value, (list, dict)):
        value = json.dumps(value, ensure_ascii=False)
    if isinstance(value, str) and value.lstrip().startswith(('=', '+', '-', '@')):
        return "'" + value
    return value


def collect(client, checks, project, location, output, *, days=7, max_rows=1000,
            timeout=120, max_bytes_billed=1024**3, include_sensitive_text=False,
            dry_run=False):
    from google.cloud import bigquery

    validate_scope(project, location)
    output = Path(output)
    output.mkdir(parents=True, exist_ok=False)
    (output / 'csv').mkdir()
    as_of = datetime.now(timezone.utc)
    run_id = uuid.uuid4().hex
    results, entries = {}, []
    for check in checks:
        sql = render_sql(check, project, location, days, max_rows, include_sensitive_text, as_of)
        submitted_id = f"databridge_hc_{run_id}_{check['id']}"
        entry = {'id': check['id'], 'section': check['section'], 'note': check['note'],
                 'sql': sql, 'status': 'failed', 'row_count': None, 'truncated': False,
                 'submitted_job_id': submitted_id, 'job_id': None, 'error': None}
        job = None
        start = time.monotonic()
        try:
            config = bigquery.QueryJobConfig(
                use_legacy_sql=False, use_query_cache=False, dry_run=dry_run,
                maximum_bytes_billed=max_bytes_billed, job_timeout_ms=timeout * 1000,
                labels={'databridge_health_check': 'v1'})
            job = client.query(sql, job_config=config, location=location,
                               job_id=submitted_id, timeout=30, job_retry=None)
            entry['job_id'] = job.job_id
            if dry_run:
                entry['status'] = 'validated'
                entry['estimated_bytes_processed'] = job.total_bytes_processed
            else:
                iterator = job.result(timeout=timeout, max_results=max_rows + 1, page_size=min(1000, max_rows + 1), job_retry=None)
                columns = [field.name for field in iterator.schema]
                raw_rows = [dict(row.items()) for row in iterator]
                entry['truncated'] = len(raw_rows) > max_rows
                rows = json.loads(json.dumps(raw_rows[:max_rows], default=json_value, allow_nan=False))
                with (output / 'csv' / (check['id'] + '.csv')).open('w', encoding='utf-8', newline='') as handle:
                    writer = csv.writer(handle)
                    writer.writerow(columns)
                    writer.writerows([[csv_cell(row.get(c)) for c in columns] for row in rows])
                results[check['id']] = {'columns': columns, 'rows': rows}
                entry.update(status='ok' if rows else 'empty', row_count=len(rows), columns=columns,
                             total_bytes_processed=job.total_bytes_processed,
                             total_bytes_billed=job.total_bytes_billed)
        except Exception as exc:
            code = getattr(exc, 'code', None)
            if callable(code):
                code = code()
            entry['error'] = {'type': type(exc).__name__, 'code': str(code) if code is not None else None,
                              'hint': 'Client administrator: inspect the job ID in the execution project/location; check metadata permissions, location, bytes cap and timeout.'}
            # A local timeout does not prove the server stopped. Attempt to cancel
            # only this collector's named job and report the acknowledgement.
            if not dry_run:
                try:
                    client.cancel_job(submitted_id, project=client.project, location=location, timeout=10)
                    entry['cancellation_requested'] = True
                except Exception:
                    entry['cancellation_requested'] = False
            # Never export exception text: it can contain query or auth details.
        entry['duration_seconds'] = round(time.monotonic() - start, 3)
        entries.append(entry)
        print(f"{check['id']}: {entry['status']}" + (' (capped)' if entry['truncated'] else ''))

    failed = sum(c['status'] == 'failed' for c in entries)
    manifest = {'format_version': 1, 'platform': 'bigquery', 'project': project,
                'execution_project': client.project, 'location': location, 'lookback_days': days,
                'window_end_utc': as_of.isoformat(), 'finished_at_utc': datetime.now(timezone.utc).isoformat(),
                'collection_status': 'partial' if failed else ('validated_only' if dry_run else 'complete'),
                'dry_run': dry_run, 'include_sensitive_text': include_sensitive_text,
                'max_rows_per_check': max_rows, 'timeout_seconds_per_check': timeout,
                'maximum_bytes_billed_per_check': max_bytes_billed,
                'failed_checks': failed, 'selected_checks': len(checks), 'checks': entries}
    write_json(output / 'manifest.json', manifest)
    write_json(output / 'results.json', results)
    (output / 'executed_checks.sql').write_text(worksheet(checks, project, location, days, max_rows, include_sensitive_text, as_of), encoding='utf-8')
    (output / 'README.txt').write_text(NOTICE, encoding='utf-8')
    lines = ['# BigQuery health-check collection', '',
             f"Project: `{project}` · Location: `{location}` · Lookback: {days} days", '',
             f"Collection: **{manifest['collection_status']}**. {len(checks) - failed}/{len(checks)} checks {'validated' if dry_run else 'executed'} successfully.",
             'This is collection coverage, not a health score.', '',
             '| Check | Status | Exported rows | Capped |', '|---|---|---:|---|']
    for c in entries:
        lines.append(f"| {c['id']} | {c['status']} | {c['row_count'] if c['row_count'] is not None else 'unavailable'} | {c['truncated']} |")
    (output / 'summary.md').write_text('\n'.join(lines) + '\n\n' + NOTICE, encoding='utf-8')
    archive = output.parent / (output.name + '.zip')
    with zipfile.ZipFile(archive, 'x', zipfile.ZIP_DEFLATED) as bundle:
        for path in sorted(output.rglob('*')):
            if path.is_file():
                bundle.write(path, arcname=str(Path(output.name) / path.relative_to(output)))
    return manifest, archive


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--project', help='Project being assessed; required except with --list')
    parser.add_argument('--location', default='US', help='US, EU, us-central1, asia-south1, etc. (default: US)')
    parser.add_argument('--execution-project', help='Optional separate project in which to submit/bill collector queries')
    parser.add_argument('--credentials', type=Path, help='Optional local service-account JSON key; otherwise use ADC')
    parser.add_argument('--days', type=positive_int, default=7)
    parser.add_argument('--max-rows', type=positive_int, default=1000)
    parser.add_argument('--timeout', type=positive_int, default=120)
    parser.add_argument('--max-bytes-billed', type=positive_int, default=1024**3)
    parser.add_argument('--include-sensitive-text', action='store_true')
    parser.add_argument('--output', type=Path, help='New output directory; existing results are never overwritten')
    parser.add_argument('--checks', help='Comma-separated check IDs; omit to run all')
    parser.add_argument('--list', action='store_true')
    parser.add_argument('--sql-only', type=Path, help='Generate a worksheet without connecting')
    parser.add_argument('--dry-run', action='store_true', help='Ask BigQuery to validate queries without collecting results')
    args = parser.parse_args(argv)
    checks = CHECKS
    if args.checks:
        requested = {v.strip() for v in args.checks.split(',')}
        unknown = requested - {c['id'] for c in checks}
        if unknown:
            parser.error('Unknown check IDs: ' + ', '.join(sorted(unknown)))
        checks = [c for c in checks if c['id'] in requested]
    if args.list:
        for check in checks:
            print(f"{check['id']:32} {check['section']} | {check['note']}")
        return 0
    try:
        validate_scope(args.project, args.location)
        if args.execution_project:
            validate_scope(args.execution_project, args.location)
        if args.days > 180:
            raise ValueError('Lookback cannot exceed the 180-day job history window')
    except ValueError as exc:
        parser.error(str(exc))
    if args.sql_only:
        with args.sql_only.open('x', encoding='utf-8') as handle:
            handle.write(worksheet(checks, args.project, args.location, args.days, args.max_rows, args.include_sensitive_text))
        print(f'SQL worksheet: {args.sql_only}')
        return 0
    output = args.output or Path('bigquery_health_' + datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ_') + uuid.uuid4().hex[:8])
    if output.exists() or (output.parent / (output.name + '.zip')).exists():
        parser.error('Output directory or ZIP already exists; choose a new --output')
    client = None
    try:
        from google.cloud import bigquery
        credentials = None
        if args.credentials:
            from google.oauth2 import service_account
            credentials = service_account.Credentials.from_service_account_file(str(args.credentials))
        client = bigquery.Client(project=args.execution_project or args.project, credentials=credentials)
        manifest, archive = collect(client, checks, args.project, args.location, output,
                                    days=args.days, max_rows=args.max_rows, timeout=args.timeout,
                                    max_bytes_billed=args.max_bytes_billed,
                                    include_sensitive_text=args.include_sensitive_text, dry_run=args.dry_run)
    except Exception as exc:
        print(f'Setup/collection stopped: {type(exc).__name__}. Check local authentication, dependencies and output permissions. No exception text is exported.')
        return 1
    finally:
        if client is not None:
            client.close()
    print(f'Review: {output / "summary.md"}')
    print(f'Share after review: {archive}')
    return 2 if manifest['failed_checks'] else 0


if __name__ == '__main__':
    raise SystemExit(main())
