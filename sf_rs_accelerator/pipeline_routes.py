"""Canonical pipeline routes shared by the UI and executor."""

SF_REDSHIFT = "SF → S3 → Redshift"
SF_S3 = "SF → S3 only"
BQ_REDSHIFT = "GCP (BigQuery) → S3 → Redshift"
BQ_S3 = "GCP (BigQuery) → S3 only"
DB_REDSHIFT = "Databricks → S3 → Redshift"
DB_S3 = "Databricks → S3 only"
ROUTES = [SF_REDSHIFT, SF_S3, BQ_REDSHIFT, BQ_S3, DB_REDSHIFT, DB_S3]


def resolve_route(destination, source_type="Snowflake"):
    """Resolve the main UI's generic destination or a legacy route label."""
    if destination in ROUTES:
        return destination
    source = (source_type or "Snowflake").lower()
    routes = {"snowflake": (SF_REDSHIFT, SF_S3),
              "databricks": (DB_REDSHIFT, DB_S3),
              "bigquery": (BQ_REDSHIFT, BQ_S3)}
    if source not in routes or destination not in ("Source → S3 → Redshift", "Source → S3 only"):
        raise ValueError("Unknown pipeline source or destination. Select a supported route.")
    return routes[source][destination == "Source → S3 only"]


def is_bigquery(route):
    return route in (BQ_REDSHIFT, BQ_S3)


def s3_only(route):
    return route in (SF_S3, BQ_S3, DB_S3)


def formats_for(route):
    return ["Parquet", "JSON", "ORC"] if route == BQ_REDSHIFT else ["Iceberg", "Parquet", "JSON", "ORC"]


def source_name(job):
    if job.get("source_type") == "bigquery":
        return f"{job['bq_project']}.{job['bq_dataset']}.{job['bq_table']}"
    return f"{job['sf_schema']}.{job['sf_table']}"


def source_schema(job):
    return job.get("bq_dataset", job.get("sf_schema", ""))


def reset_queue(state):
    state.update(job_queue=[], checkpoint={}, migration_done=False,
                 migration_running=False, migration_summary={})
