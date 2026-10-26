"""BigQuery-specific metadata checks. No business-table data is queried."""

JOBS = """WITH jobs AS (
  SELECT * FROM `{project}.region-{region}.INFORMATION_SCHEMA.JOBS_BY_PROJECT`
  WHERE creation_time >= TIMESTAMP_SUB({as_of}, INTERVAL {days} DAY)
    AND creation_time < {as_of}
    AND statement_type IS DISTINCT FROM 'SCRIPT'
    AND NOT EXISTS (
      SELECT 1 FROM UNNEST(labels) AS label
      WHERE label.key = 'databridge_health_check'
    )
)
"""


def check(ident, section, sql, note, jobs=False):
    return {"id": ident, "section": section, "sql": (JOBS if jobs else "") + sql,
            "note": note, "history": jobs}


CHECKS = [
    check("dataset_inventory", "inventory", """
SELECT catalog_name AS project_id, schema_name AS dataset_id, location,
       creation_time, last_modified_time
FROM `{project}.region-{region}.INFORMATION_SCHEMA.SCHEMATA`
ORDER BY dataset_id
""", "Metadata visible in the selected project/location only; dataset modification time is not table-data freshness."),
    check("table_inventory", "inventory", """
SELECT table_catalog, table_schema, table_name, table_type, creation_time,
       is_insertable_into
FROM `{project}.region-{region}.INFORMATION_SCHEMA.TABLES`
ORDER BY table_schema, table_name
""", "Table/view inventory; definitions and business rows are not exported."),
    check("job_summary", "workload", """
SELECT job_type, state, COUNT(*) AS job_count,
       COUNTIF(error_result IS NOT NULL) AS failed_jobs,
       COUNTIF(error_result.reason = 'accessDenied') AS access_denied_jobs
FROM jobs GROUP BY job_type, state ORDER BY job_count DESC
""", "Jobs submitted in this project and created in the reporting window. Script parents are excluded to avoid counting their child work twice.", True),
    check("daily_query_usage", "usage", """
SELECT DATE(creation_time) AS day_utc, COUNT(*) AS query_count,
       COUNTIF(error_result IS NOT NULL) AS failed_queries,
       SUM(total_bytes_processed) AS total_bytes_processed,
       SUM(total_bytes_billed) AS total_bytes_billed,
       SUM(total_slot_ms) AS total_slot_ms,
       COUNTIF(total_bytes_billed IS NULL) AS queries_with_unknown_billed_bytes
FROM jobs WHERE job_type = 'QUERY' AND state = 'DONE'
GROUP BY day_utc ORDER BY day_utc
""", "Bytes and slot milliseconds are usage signals, not a currency bill. Some statistics can be unavailable, including for row-level security." , True),
    check("slow_queries", "performance", """
SELECT job_id, user_email, creation_time, start_time, end_time,
       TIMESTAMP_DIFF(end_time, start_time, MILLISECOND) AS execution_ms,
       total_slot_ms, total_bytes_processed, cache_hit, reservation_id{query_text}
FROM jobs WHERE job_type = 'QUERY' AND state = 'DONE'
ORDER BY execution_ms DESC, job_id
""", "Ranks elapsed execution time; long duration alone does not establish a problem. Raw SQL is opt-in.", True),
    check("query_queue_delay", "performance", """
SELECT job_id, user_email, priority, creation_time, start_time,
       TIMESTAMP_DIFF(start_time, creation_time, MILLISECOND) AS start_delay_ms,
       reservation_id
FROM jobs WHERE job_type = 'QUERY' AND start_time IS NOT NULL
ORDER BY start_delay_ms DESC, job_id
""", "Creation-to-start delay is an observation, not proof of slot starvation; batch scheduling and other causes can contribute.", True),
    check("largest_query_scans", "usage", """
SELECT job_id, user_email, creation_time, total_bytes_processed,
       total_bytes_billed, total_slot_ms, reservation_id{query_text}
FROM jobs WHERE job_type = 'QUERY' AND state = 'DONE'
ORDER BY total_bytes_processed DESC, job_id
""", "Large scans are review candidates. Correct cost interpretation depends on the pricing model; no fixed per-TiB price is assumed.", True),
    check("query_cache_observations", "performance", """
SELECT COUNT(*) AS completed_queries,
       COUNTIF(cache_hit IS TRUE) AS cache_hits,
       COUNTIF(cache_hit IS FALSE) AS cache_misses,
       COUNTIF(cache_hit IS NULL) AS cache_unknown,
       SAFE_DIVIDE(COUNTIF(cache_hit IS TRUE), COUNTIF(cache_hit IS NOT NULL)) AS observed_cache_hit_ratio
FROM jobs WHERE job_type = 'QUERY' AND state = 'DONE' AND error_result IS NULL
""", "Not all queries are cache eligible; the observed ratio is not a cache-eligibility-adjusted efficiency score.", True),
    check("principal_workload", "access_observations", """
SELECT user_email, COUNT(*) AS query_count,
       SUM(total_bytes_processed) AS total_bytes_processed,
       SUM(total_slot_ms) AS total_slot_ms,
       COUNTIF(error_result IS NOT NULL) AS failed_queries
FROM jobs WHERE job_type = 'QUERY'
GROUP BY user_email ORDER BY total_slot_ms DESC, user_email
""", "Includes principal email addresses. Shows observed job activity, not IAM memberships or a complete access audit.", True),
    check("failed_jobs", "reliability", """
SELECT job_id, job_type, user_email, creation_time,
       error_result.reason AS error_reason{error_text}{query_text}
FROM jobs WHERE error_result IS NOT NULL
ORDER BY creation_time DESC, job_id
""", "Failure reasons are included by default; error messages and SQL text are opt-in. Failed logins and policy changes require Cloud Audit Logs.", True),
    check("resource_warnings", "performance", """
SELECT job_id, user_email, creation_time,
       query_info.resource_warning IS NOT NULL AS has_resource_warning,
       total_slot_ms, total_bytes_processed{warning_text}{query_text}
FROM jobs WHERE query_info.resource_warning IS NOT NULL
ORDER BY creation_time DESC, job_id
""", "Resource-warning presence is exported by default; free-text warning details are opt-in.", True),
    check("reservation_workload", "capacity", """
SELECT reservation_id, COUNT(*) AS query_count,
       SUM(total_slot_ms) AS total_slot_ms,
       SUM(TIMESTAMP_DIFF(end_time, start_time, MILLISECOND)) AS summed_execution_ms
FROM jobs WHERE job_type = 'QUERY' AND state = 'DONE'
GROUP BY reservation_id ORDER BY total_slot_ms DESC
""", "Observed workload by reservation. Summed execution durations overlap; these values do not measure purchased-slot utilization, idle slots, or reservation spend.", True),
    check("pipeline_jobs", "pipeline", """
SELECT job_type, state, COUNT(*) AS job_count,
       COUNTIF(error_result IS NOT NULL) AS failed_jobs,
       MAX(creation_time) AS latest_job_created_at,
       MAX(IF(error_result IS NULL AND state = 'DONE', end_time, NULL)) AS latest_success_end_time
FROM jobs WHERE job_type IN ('LOAD', 'COPY', 'EXTRACT')
GROUP BY job_type, state ORDER BY job_type, state
""", "Covers load/copy/extract jobs only. Streaming ingestion and Data Transfer Service schedule/run configuration require additional APIs.", True),
    check("scheduled_query_labels", "pipeline", """
SELECT job_id, creation_time, state, user_email, error_result.reason AS error_reason,
       (SELECT ANY_VALUE(value) FROM UNNEST(labels) WHERE key = 'data_source_id') AS data_source_id
FROM jobs
WHERE EXISTS (SELECT 1 FROM UNNEST(labels)
              WHERE key = 'data_source_id' AND value = 'scheduled_query')
ORDER BY creation_time DESC, job_id
""", "Only jobs carrying the scheduled_query data-source label are detected. This is not a complete inventory of schedules or missed runs.", True),
    check("storage_by_dataset", "storage", """
SELECT table_schema, COUNT(*) AS table_count,
       SUM(total_rows) AS total_rows,
       SUM(total_logical_bytes) AS logical_bytes,
       SUM(total_physical_bytes) AS physical_bytes,
       SUM(time_travel_physical_bytes) AS time_travel_physical_bytes,
       SUM(fail_safe_physical_bytes) AS fail_safe_physical_bytes
FROM `{project}.region-{region}.INFORMATION_SCHEMA.TABLE_STORAGE`
WHERE deleted = FALSE GROUP BY table_schema ORDER BY logical_bytes DESC, table_schema
""", "Current non-deleted storage snapshot, not historical growth or a bill. Clone/snapshot attribution and metadata delay affect interpretation."),
    check("largest_tables", "storage", """
SELECT table_catalog, table_schema, table_name, table_type, total_rows,
       total_logical_bytes, total_physical_bytes, total_partitions,
       storage_last_modified_time
FROM `{project}.region-{region}.INFORMATION_SCHEMA.TABLE_STORAGE`
WHERE deleted = FALSE ORDER BY total_logical_bytes DESC, table_schema, table_name
""", "Ranks current visible storage; excludes deleted tables."),
    check("table_write_recency", "storage", """
SELECT table_catalog, table_schema, table_name, total_rows,
       storage_last_modified_time,
       TIMESTAMP_DIFF({as_of}, storage_last_modified_time, DAY) AS days_since_storage_write
FROM `{project}.region-{region}.INFORMATION_SCHEMA.TABLE_STORAGE`
WHERE deleted = FALSE
ORDER BY storage_last_modified_time ASC, table_schema, table_name
""", "Storage-write recency is not a business-data freshness SLA. NULL can mean no data; static reference tables may be intentionally unchanged."),
    check("partitioning_and_clustering", "table_design", """
SELECT table_catalog, table_schema, table_name,
       STRING_AGG(IF(is_partitioning_column = 'YES', column_name, NULL), ', ' ORDER BY ordinal_position) AS partition_columns,
       STRING_AGG(IF(clustering_ordinal_position IS NOT NULL, column_name, NULL), ', ' ORDER BY clustering_ordinal_position) AS clustering_columns
FROM `{project}.region-{region}.INFORMATION_SCHEMA.COLUMNS`
GROUP BY table_catalog, table_schema, table_name ORDER BY table_schema, table_name
""", "Lists design metadata, including ingestion-time partitioning columns when exposed. Does not measure pruning quality or recommend partitioning every table."),
    check("table_options", "table_design", """
SELECT table_catalog, table_schema, table_name, option_name, option_value
FROM `{project}.region-{region}.INFORMATION_SCHEMA.TABLE_OPTIONS`
WHERE option_name IN ('require_partition_filter', 'expiration_timestamp',
                      'partition_expiration_days', 'kms_key_name')
ORDER BY table_schema, table_name, option_name
""", "Selected settings only. An absent option can mean default behavior; missing CMEK does not mean unencrypted data. KMS resource names are included."),
    check("column_policy_tags", "access_observations", """
SELECT table_catalog, table_schema, table_name, column_name, data_type, policy_tags
FROM `{project}.region-{region}.INFORMATION_SCHEMA.COLUMNS`
WHERE ARRAY_LENGTH(policy_tags) > 0 ORDER BY table_schema, table_name, ordinal_position
""", "Top-level column tag metadata only; not effective permissions, nested-field coverage, row-access-policy coverage or an IAM compliance verdict."),
]
