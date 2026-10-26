"""BigQuery access for small POC exports. No credentials are written to disk."""
import json


def create_client(project, credential_json=None):
    from google.cloud import bigquery
    from google.oauth2 import service_account

    if not project or not project.strip():
        raise ValueError("Enter a BigQuery project ID.")
    credentials = None
    if credential_json:
        info = json.loads(credential_json)
        if info.get("type") != "service_account":
            raise ValueError("Upload a service-account JSON key.")
        credentials = service_account.Credentials.from_service_account_info(info)
    return bigquery.Client(project=project.strip(), credentials=credentials)


def list_datasets(client):
    return sorted(d.dataset_id for d in client.list_datasets(client.project))


def discover_tables(client, datasets):
    result = {}
    for dataset in datasets:
        location = client.get_dataset(f"{client.project}.{dataset}").location
        result[dataset] = []
        for item in client.list_tables(f"{client.project}.{dataset}"):
            if item.table_type != "TABLE":
                continue
            table = client.get_table(item.reference)
            partition_field = (table.time_partitioning.field
                               if table.time_partitioning is not None else None)
            result[dataset].append({"table": table.table_id, "rows": table.num_rows,
                                    "bytes": table.num_bytes, "location": location,
                                    "partition_field": partition_field,
                                    "clustering_fields": list(table.clustering_fields or []),
                                    "schema": [{
                                        "Column": f.name,
                                        "Type": f.field_type,
                                        "Mode": f.mode,
                                        "Precision": f.precision,
                                        "Scale": f.scale,
                                        "Max Length": f.max_length,
                                    } for f in table.schema]})
        result[dataset].sort(key=lambda t: t["table"])
    return result


def build_query(job):
    import sqlglot
    from sqlglot import exp

    ref = f"{job['bq_project']}.{job['bq_dataset']}.{job['bq_table']}"
    if "`" in ref:
        raise ValueError("Invalid BigQuery table identifier.")
    custom = job.get("source_query", "").strip()
    if custom:
        statements = sqlglot.parse(custom, read="bigquery")
        if len(statements) != 1 or not isinstance(statements[0], exp.Query):
            raise ValueError("Custom SQL must be a single SELECT query.")
        query = custom.rstrip().rstrip(";")
    else:
        query = f"SELECT * FROM `{ref}`"
        if job.get("filter_expression", "").strip():
            query += " WHERE " + job["filter_expression"].strip()
        statements = sqlglot.parse(query, read="bigquery")
        if len(statements) != 1 or not isinstance(statements[0], exp.Select):
            raise ValueError("Filter must be a single WHERE expression.")
    limit = int(job.get("row_limit", 0))
    if limit < 0:
        raise ValueError("Row limit cannot be negative.")
    return f"SELECT * FROM ({query}) AS export_rows LIMIT {limit}" if limit else query


def read_table(client, job):
    from google.cloud import bigquery

    query = build_query(job)
    config = bigquery.QueryJobConfig(maximum_bytes_billed=int(job.get("max_query_bytes", 1024**3)))
    query_job = client.query(query, location=job["bq_location"], job_config=config)
    job["bq_query_id"] = query_job.job_id
    result = query_job.result(timeout=300, page_size=1000)
    if result.total_rows > int(job.get("max_rows", 100000)):
        raise ValueError("Result exceeds the POC row cap. Set a row limit or narrow the filter.")
    allowed = {"INTEGER", "INT64", "FLOAT", "FLOAT64", "NUMERIC", "DECIMAL",
               "BOOLEAN", "BOOL", "STRING", "DATE", "DATETIME", "TIMESTAMP"}
    unsupported = [f"{f.name} ({f.field_type}{' REPEATED' if f.mode == 'REPEATED' else ''})"
                   for f in result.schema if f.field_type not in allowed or f.mode == "REPEATED"]
    if unsupported:
        raise ValueError("Unsupported POC columns: " + ", ".join(unsupported))
    # REST download keeps the POC setup free of Storage API requirements.
    import pyarrow as pa
    batches = []
    size = 0
    for batch in result.to_arrow_iterable():
        size += batch.nbytes
        if size > 256 * 1024**2:
            raise ValueError("Result exceeds the 256 MiB POC memory cap. Reduce the row limit.")
        batches.append(batch)
    if batches:
        return pa.Table.from_batches(batches)
    # Empty results still need the declared BigQuery schema.
    return result.to_arrow(create_bqstorage_client=False)
