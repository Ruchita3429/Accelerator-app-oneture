"""BigQuery export writers and format-aware Redshift Data API loading."""
import io
import hashlib
import json
import math
import time
from datetime import date, datetime
from decimal import Decimal

from pipeline_routes import BQ_REDSHIFT, formats_for, source_name


def iceberg_schema(arrow_schema):
    import pyarrow as pa
    from pyiceberg.schema import Schema
    from pyiceberg.types import (NestedField, LongType, DoubleType, BooleanType,
                                StringType, DateType, TimestampType, TimestamptzType, DecimalType)
    fields = []
    for index, field in enumerate(arrow_schema, 1):
        t = field.type
        if pa.types.is_integer(t): target = LongType()
        elif pa.types.is_floating(t): target = DoubleType()
        elif pa.types.is_boolean(t): target = BooleanType()
        elif pa.types.is_string(t) or pa.types.is_large_string(t): target = StringType()
        elif pa.types.is_date(t): target = DateType()
        elif pa.types.is_timestamp(t): target = TimestamptzType() if t.tz else TimestampType()
        elif pa.types.is_decimal(t) and t.precision <= 38: target = DecimalType(t.precision, t.scale)
        else: raise ValueError(f"Unsupported Iceberg column: {field.name} ({t})")
        fields.append(NestedField(index, field.name, target, required=False))
    return Schema(*fields)


def write_iceberg(table, job, s3_cfg):
    from pyiceberg.catalog import load_catalog
    from pyiceberg.io.pyarrow import schema_to_pyarrow

    schema = iceberg_schema(table.schema)
    warehouse = f"s3://{s3_cfg['bucket']}"
    prefix = s3_cfg.get("prefix", "").strip("/")
    if prefix:
        warehouse += "/" + prefix
    namespace, name = job['bq_dataset'].lower(), job['bq_table'].lower()
    identifier = (namespace, name)
    location = f"{warehouse}/{namespace}/{name}"
    catalog = load_catalog("databridge_glue", **{
        "type": "glue", "warehouse": warehouse,
        "client.region": s3_cfg['region'],
        "client.access-key-id": s3_cfg['access_key'],
        "client.secret-access-key": s3_cfg['secret_key'],
    })
    # Ownership prevents this route overwriting a same-named Snowflake/other-project table.
    owner = "bigquery:" + source_name(job)
    if catalog.table_exists(identifier):
        target = catalog.load_table(identifier)
        if target.properties.get("databridge.source") != owner or target.location().rstrip('/') != location:
            raise ValueError(f"Glue table {namespace}.{name} belongs to another source/location. Choose another dataset or catalog table.")
        existing = target.schema()
        if [(f.name, f.field_type) for f in existing.fields] != [(f.name, f.field_type) for f in schema.fields]:
            raise ValueError("Existing Iceberg schema differs from the BigQuery result.")
    else:
        # Also check for a pre-existing, unregistered location before claiming it.
        import boto3
        s3 = boto3.client('s3', region_name=s3_cfg['region'],
                          aws_access_key_id=s3_cfg['access_key'], aws_secret_access_key=s3_cfg['secret_key'])
        key = '/'.join(p for p in (prefix, namespace, name) if p) + '/'
        if s3.list_objects_v2(Bucket=s3_cfg['bucket'], Prefix=key, MaxKeys=1).get('KeyCount', 0):
            raise ValueError("Iceberg destination already contains objects without a matching Glue registration.")
        if not catalog.namespace_exists(namespace):
            catalog.create_namespace(namespace)
        target = catalog.create_table(identifier, schema=schema, location=location,
                                      properties={"databridge.source": owner})
    # Explicit schema cast keeps empty/all-null columns and decimal/timestamp types.
    table = table.cast(schema_to_pyarrow(target.schema(), include_field_ids=False))
    target.overwrite(table)
    snapshot = target.current_snapshot()
    job.update(iceberg_table=f"{namespace}.{name}",
               iceberg_snapshot_id=snapshot.snapshot_id if snapshot else None,
               s3_key='/'.join(p for p in (prefix, namespace, name) if p) + '/')
    # Data-file sizes in the committed snapshot, not the Arrow in-memory size.
    tasks = list(target.scan().plan_files())
    if sum(task.file.record_count for task in tasks) != table.num_rows:
        raise ValueError('Iceberg snapshot row count does not match the exported data.')
    return sum(task.file.file_size_in_bytes for task in tasks)


def _json_value(value):
    if isinstance(value, Decimal):
        if not value.is_finite(): raise ValueError("JSON export cannot represent non-finite decimals.")
        return format(value, 'f')
    if isinstance(value, float) and not math.isfinite(value):
        raise ValueError("JSON export cannot represent NaN or Infinity.")
    if isinstance(value, (datetime, date)):
        value = value.isoformat()
    return json.dumps(value, ensure_ascii=False, allow_nan=False)


def serialize_table(table, fmt, compression=False):
    import pyarrow.parquet as pq
    import pyarrow.orc as orc
    buffer = io.BytesIO()
    if fmt == 'Parquet':
        pq.write_table(table, buffer, compression='gzip' if compression else 'snappy',
                       coerce_timestamps='us', version='1.0')
    elif fmt == 'ORC':
        orc.write_table(table, buffer)
    elif fmt == 'JSON':
        for row in table.to_pylist():
            line = '{' + ','.join(json.dumps(k) + ':' + _json_value(v) for k, v in row.items()) + '}\n'
            buffer.write(line.encode('utf-8'))
        if compression:
            import gzip
            return gzip.compress(buffer.getvalue())
    else:
        raise ValueError(f"Unsupported file format: {fmt}")
    return buffer.getvalue()


def extract_to_s3(job, client, s3_cfg):
    import boto3
    import pyarrow as pa
    from bigquery_connector import read_table

    if job['file_format'] not in formats_for(job['destination']):
        raise ValueError("Iceberg is only available for BigQuery → S3 only.")
    for field in ('bucket', 'access_key', 'secret_key', 'region'):
        if not s3_cfg.get(field): raise ValueError(f"Missing S3 setting: {field}")
    start = time.time()
    job['phase'] = 'reading'
    table = read_table(client, job)
    if len({n.lower() for n in table.column_names}) != len(table.column_names):
        raise ValueError("Column names collide when normalized for Redshift.")
    if job['destination'] == BQ_REDSHIFT:
        # Redshift COPY columnar timestamps use UTC without Arrow timezone metadata.
        fields = [pa.field(f.name, pa.timestamp('us') if pa.types.is_timestamp(f.type) else f.type)
                  for f in table.schema]
        table = table.cast(pa.schema(fields))
    job['source_columns'] = [(f.name, str(f.type)) for f in table.schema]
    job['string_bytes'] = {f.name: max((len(v.encode('utf-8')) for v in table[f.name].to_pylist() if v is not None), default=0)
                           for f in table.schema if pa.types.is_string(f.type) or pa.types.is_large_string(f.type)}
    job['phase'] = 'writing S3'
    if job['file_format'] == 'Iceberg':
        size = write_iceberg(table, job, s3_cfg)
    else:
        body = serialize_table(table, job['file_format'], job.get('compression', False))
        if job['file_format'] == 'JSON' and job.get('compression') and not job['s3_key'].endswith('.gz'):
            job['s3_key'] += '.gz'
        s3 = boto3.client('s3', region_name=s3_cfg['region'],
                          aws_access_key_id=s3_cfg['access_key'], aws_secret_access_key=s3_cfg['secret_key'])
        s3.put_object(Bucket=s3_cfg['bucket'], Key=job['s3_key'], Body=body)
        size = len(body)
    job['export_complete'] = True
    return {'rows': table.num_rows, 'bytes': size, 'duration_secs': round(time.time()-start, 2)}


def quote_identifier(value):
    if not value: raise ValueError("Target schema and table are required.")
    return '"' + value.replace('"', '""') + '"'


def sql_literal(value):
    return "'" + str(value).replace("'", "''") + "'"


def wait_statement(client, statement_id, timeout=300):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        result = client.describe_statement(Id=statement_id)
        if result['Status'] == 'FINISHED': return result
        if result['Status'] in ('FAILED', 'ABORTED'):
            raise RuntimeError(result.get('Error', 'Redshift statement failed.'))
        time.sleep(1)
    raise TimeoutError(f"Redshift statement {statement_id} is still pending; Resume will check this same statement.")


def run_sql(client, cfg, sql):
    statement_id = client.execute_statement(WorkgroupName=cfg['workgroup'].split('.')[0].strip(),
                                            Database=cfg['database'], Sql=sql)['Id']
    result = wait_statement(client, statement_id)
    rows = []
    if result.get('HasResultSet'):
        kwargs = {'Id': statement_id}
        while True:
            page = client.get_statement_result(**kwargs)
            rows.extend([[None if col.get('isNull') else next(iter(col.values())) for col in row]
                         for row in page.get('Records', [])])
            if not page.get('NextToken'): break
            kwargs['NextToken'] = page['NextToken']
    return rows


def build_copy_sql(job, s3_cfg):
    fmt = job['file_format']
    if fmt not in ('Parquet', 'ORC', 'JSON'):
        raise ValueError("Redshift COPY supports Parquet, ORC or JSON on the BigQuery route.")
    target = quote_identifier(job['rs_schema']) + '.' + quote_identifier(job['rs_table'])
    credentials = f"aws_access_key_id={s3_cfg['access_key']};aws_secret_access_key={s3_cfg['secret_key']}"
    clause = "JSON 'auto ignorecase'" if fmt == 'JSON' else fmt.upper()
    key = job.get('manifest_key', job['s3_key'])
    manifest = ' MANIFEST' if job.get('manifest_key') else ''
    return (f"COPY {target} FROM {sql_literal('s3://' + s3_cfg['bucket'] + '/' + key)} "
            f"CREDENTIALS {sql_literal(credentials)} FORMAT AS {clause}{manifest} STATUPDATE ON;")


def validate_target_columns(job, columns):
    import re
    expected = [n.lower() for n, _ in job['source_columns']]
    if [c[0].lower() for c in columns] != expected:
        raise ValueError('Target column names/order do not match the exported data.')
    for (name, source), target in zip(job['source_columns'], columns):
        kind = target[1].lower()
        valid = False
        if source == 'int64': valid = kind == 'bigint'
        elif source == 'double': valid = kind == 'double precision'
        elif source == 'bool': valid = kind == 'boolean'
        elif source.startswith('date32'): valid = kind == 'date'
        elif source.startswith('timestamp'): valid = kind == 'timestamp without time zone'
        elif source in ('string', 'large_string'):
            valid = kind in ('character varying', 'text') and (target[4] is None or
                    job.get('string_bytes', {}).get(name, 0) <= int(target[4]))
        elif source.startswith('decimal'):
            match = re.search(r'\((\d+),\s*(\d+)\)', source)
            if match and kind in ('numeric', 'decimal') and target[2] is not None and target[3] is not None:
                precision, scale = map(int, match.groups())
                # Columnar COPY requires an identical physical decimal schema.
                valid = (int(target[2]), int(target[3])) == (precision, scale)
        if not valid:
            raise ValueError(f'Target type/size does not match {name}: source {source}, target {kind}.')


def load_to_redshift(job, s3_cfg, rs_cfg):
    import boto3

    if job['file_format'] == 'Iceberg': raise ValueError("Iceberg cannot be used on the Redshift route.")
    if s3_cfg['region'] != rs_cfg.get('region'):
        raise ValueError("Use an S3 bucket in the same region as Redshift for this POC route.")
    client = boto3.client('redshift-data', region_name=rs_cfg['region'],
                          aws_access_key_id=rs_cfg['access_key'], aws_secret_access_key=rs_cfg['secret_key'])
    target = quote_identifier(job['rs_schema']) + '.' + quote_identifier(job['rs_table'])
    start = time.time()
    if not job.get('load_statement_id'):
        s3 = boto3.client('s3', region_name=s3_cfg['region'], aws_access_key_id=s3_cfg['access_key'],
                          aws_secret_access_key=s3_cfg['secret_key'])
        actual_region = s3.get_bucket_location(Bucket=s3_cfg['bucket']).get('LocationConstraint') or 'us-east-1'
        if actual_region == 'EU': actual_region = 'eu-west-1'
        if actual_region != rs_cfg['region']:
            raise ValueError('The S3 bucket is not in the Redshift region.')
        columns = run_sql(client, rs_cfg,
            "SELECT column_name, data_type, numeric_precision, numeric_scale, character_maximum_length FROM information_schema.columns WHERE table_schema = "
            + sql_literal(job['rs_schema']) + " AND table_name = " + sql_literal(job['rs_table']) + " ORDER BY ordinal_position")
        if not columns: raise ValueError(f"Target {target} does not exist or is not visible. Create it before loading.")
        validate_target_columns(job, columns)
        if 'target_rows_before' not in job:
            job['target_rows_before'] = int(run_sql(client, rs_cfg, f'SELECT COUNT(*) FROM {target}')[0][0])
        if job['load_mode'] == 'TRUNCATE & LOAD':
            if not job.get('truncate_statement_id'):
                job['truncate_statement_id'] = client.execute_statement(
                    WorkgroupName=rs_cfg['workgroup'].split('.')[0].strip(), Database=rs_cfg['database'],
                    Sql=f'TRUNCATE TABLE {target}', ClientToken=_statement_token(job, 'truncate'))['Id']
            try:
                wait_statement(client, job['truncate_statement_id'])
            except RuntimeError:
                job.pop('truncate_statement_id', None)
                job['truncate_attempt'] = job.get('truncate_attempt', 0) + 1
                raise
        # Empty exports still implement the selected load mode, but need no COPY.
        if job['rows']:
            s3 = boto3.client('s3', region_name=s3_cfg['region'], aws_access_key_id=s3_cfg['access_key'],
                              aws_secret_access_key=s3_cfg['secret_key'])
            directory, _, filename = job['s3_key'].rpartition('/')
            job['manifest_key'] = f'{directory}/_manifests/{filename}.manifest'
            manifest = {'entries': [{'url': f"s3://{s3_cfg['bucket']}/{job['s3_key']}",
                                     'mandatory': True, 'meta': {'content_length': job['bytes']}}]}
            s3.put_object(Bucket=s3_cfg['bucket'], Key=job['manifest_key'], Body=json.dumps(manifest).encode())
            sql = build_copy_sql(job, s3_cfg)
            job['copy_sql'] = sql.replace(s3_cfg['access_key'], '<access-key>').replace(s3_cfg['secret_key'], '<secret-key>')
            job['load_statement_id'] = client.execute_statement(
                WorkgroupName=rs_cfg['workgroup'].split('.')[0].strip(), Database=rs_cfg['database'], Sql=sql,
                ClientToken=_statement_token(job, 'copy'))['Id']
    if job.get('load_statement_id'):
        try:
            wait_statement(client, job['load_statement_id'])
        except RuntimeError:
            # Server explicitly failed: COPY was rolled back; a retry may submit it again.
            job.pop('load_statement_id', None)
            job['copy_attempt'] = job.get('copy_attempt', 0) + 1
            raise
    job['phase'] = 'validating'
    count = int(run_sql(client, rs_cfg, f'SELECT COUNT(*) FROM {target}')[0][0])
    expected = job['rows'] + (job['target_rows_before'] if job['load_mode'] == 'APPEND' else 0)
    job['target_rows_after'] = count
    if count != expected:
        raise ValueError(f"Target row count {count} differs from expected {expected}. Check concurrent target writes.")
    return {'rs_rows': job['rows'], 'rs_duration_secs': round(time.time()-start, 2)}


def _statement_token(job, stage):
    attempt = job.get(stage + '_attempt', 0)
    return hashlib.sha256(f"{job['run_id']}:{job['id']}:{stage}:{attempt}".encode()).hexdigest()
