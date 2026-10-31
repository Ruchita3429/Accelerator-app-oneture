"""
CHUNK 2 — Job Queue Engine
===========================
Standalone module: chunk_02_job_queue_engine.py

PURPOSE
-------
Consumes st.session_state.job_queue built by Chunk 1 and executes each
table migration sequentially:

    Snowflake → (extract) → S3 → (COPY) → Redshift

For each table it:
  1. Updates job status to "running" in session state
  2. Extracts from Snowflake and uploads to S3
  3. Issues COPY into Redshift
  4. Marks job "done" or "failed" with metrics
  5. Records a checkpoint in session state for retries in the same session

PLUG-IN point (for Assembly — Chunk 5)
---------------------------------------
Replace the run button + execution block in Tab 1 / Tab 2 with:
    from chunk_02_job_queue_engine import render_execution_controls, run_migration

Call render_execution_controls() to draw the Run / Pause / Resume buttons.
The engine writes all state back to st.session_state.job_queue in-place,
so Chunk 3 (monitoring UI) and Chunk 4 (metrics) just read from there.

SESSION STATE KEYS READ
------------------------
  st.session_state.job_queue          : list[dict]  — from Chunk 1
  st.session_state.sf_connected       : bool
  st.session_state.rs_connected       : bool

SESSION STATE KEYS WRITTEN
---------------------------
  st.session_state.job_queue          : updated in-place (status, rows, bytes, etc.)
  st.session_state.migration_running  : bool
  st.session_state.migration_done     : bool
  st.session_state.checkpoint         : dict  — {table_id: "done"|"failed"}
  st.session_state.migration_summary  : dict  — aggregate stats after completion

CHECKPOINT LOGIC
----------------
A checkpoint dict is maintained in session state keyed by job id.
On resume, any job whose id is already "done" in the checkpoint is skipped.
Checkpoints do not survive an application restart. Source and load errors
are reported as failures; source errors never generate replacement data.
"""

# ── Compatibility Shims ───────────────────────────────────────────────────────
import mmh3_compat      # Pure-Python mmh3 fallback (WDAC blocks mmh3.pyd)
import pyarrow_compat   # Stubs pyarrow._compute + pyarrow._dataset before pandas loads them

import streamlit as st
import time
from datetime import datetime

from pipeline_routes import is_bigquery, s3_only, source_name, formats_for, resolve_route


# ─────────────────────────────────────────────────────────────────────────────
# Internal helpers
# ─────────────────────────────────────────────────────────────────────────────

def _log(msg: str, level: str = "info"):
    if "logs" not in st.session_state:
        st.session_state.logs = []
    ts = datetime.now().strftime("%H:%M:%S")
    st.session_state.logs.append((ts, level, msg))
    try:
        safe_msg = str(msg).encode("ascii", errors="replace").decode("ascii")
        print(f"[{ts}] [{level.upper()}] {safe_msg}")
    except (Exception, OSError):
        pass


def _format_bytes(b: int) -> str:
    if b < 1024:        return f"{b} B"
    if b < 1024 ** 2:   return f"{b / 1024:.1f} KB"
    if b < 1024 ** 3:   return f"{b / 1024 ** 2:.2f} MB"
    return f"{b / 1024 ** 3:.3f} GB"


def _update_job(job_id: str, **kwargs):
    """Update fields on a specific job in the queue by id."""
    for job in st.session_state.job_queue:
        if job["id"] == job_id:
            job.update(kwargs)
            break


def _init_checkpoint():
    if "checkpoint" not in st.session_state:
        st.session_state.checkpoint = {}


def _mark_checkpoint(job_id: str, status: str):
    st.session_state.checkpoint[job_id] = status


def _is_checkpointed(job_id: str) -> bool:
    return st.session_state.checkpoint.get(job_id) == "done"


def record_manual_transfer(stage, *, source_schema=None, source_table=None,
                           s3_key=None, error="", **metrics):
    """Keep the existing queue/monitor in sync with the two manual transfer tabs."""
    queue = st.session_state.get("job_queue", [])
    source = st.session_state.get("source_type", "Snowflake")
    route = resolve_route(st.session_state.get("pipeline_destination", "Source → S3 → Redshift"), source)
    matched = False
    for job in queue:
        if stage == "export":
            matches = (job.get("source_type", source).lower() == source.lower()
                       and str(job.get("source_schema", job.get("sf_schema", ""))).casefold() == str(source_schema).casefold()
                       and str(job.get("source_table", job.get("sf_table", ""))).casefold() == str(source_table).casefold())
        else:
            matches = (job.get("s3_key") == s3_key
                       and (not job.get("s3_bucket") or job["s3_bucket"] == metrics.get("s3_bucket")))
        if not matches:
            continue
        matched = True
        _init_checkpoint()
        if error:
            job.update(status="failed", phase=f"{stage} failed", error=error)
            if stage == "export":
                job.pop("export_complete", None)
            _mark_checkpoint(job["id"], "failed")
        else:
            if stage == "export":
                # A new export invalidates any previous load/checkpoint state.
                for key in ("load_statement_id", "truncate_statement_id", "target_rows_before", "target_rows_after"):
                    job.pop(key, None)
                job.update(export_complete=True)
            complete = stage == "load" or s3_only(route)
            if stage == "load":
                job["duration_secs"] = round(job.get("duration_secs", 0) + metrics.get("rs_duration_secs", 0), 2)
            job.update(metrics, s3_key=s3_key, status="done" if complete else "pending",
                       phase="done" if complete else "exported to S3; awaiting Redshift", error="")
            _mark_checkpoint(job["id"], "done" if complete else "pending")
    if matched:
        done = [j for j in queue if j.get("status") == "done"]
        failed = sum(j.get("status") == "failed" for j in queue)
        seconds = sum(j.get("duration_secs", 0) for j in queue)
        size = sum(j.get("bytes", 0) for j in done)
        st.session_state.migration_done = len(done) == len(queue)
        st.session_state.migration_summary = dict(
            total=len(queue), completed=len(done), failed=failed,
            pending=len(queue) - len(done) - failed,
            total_rows=sum(j.get("rows", 0) for j in done), total_bytes=size,
            total_secs=seconds, throughput=int(size / seconds) if seconds else 0,
            finished_at=datetime.now().strftime("%Y-%m-%d %H:%M:%S"))


# ─────────────────────────────────────────────────────────────────────────────
# REAL CONNECTOR HOOKS — replace these for production
# ─────────────────────────────────────────────────────────────────────────────

def _extract_snowflake_to_s3_iceberg(job: dict, sf_cfg: dict, s3_cfg: dict) -> dict:
    """
    Export a Snowflake table to S3 in Apache Iceberg format.

    Flow:
      1. Fetch rows from Snowflake via the Python connector (cursor.fetchall)
      2. Convert to a PyArrow Table
      3. Register and write the table through AWS Glue using pyiceberg

    S3 output layout:
      s3://<bucket>/<prefix>/<SCHEMA>/<TABLE>/
          metadata/
              00000-<uuid>.metadata.json   ← Iceberg snapshot catalog
              snap-<id>-<uuid>.avro        ← manifest list
          data/
              <uuid>.parquet               ← actual Parquet data files

    Requires:
      pip install "pyiceberg[glue,pyarrow]" pyarrow pandas
    """
    import time
    import snowflake.connector
    import pandas as pd
    import pyarrow as pa
    import mmh3_compat
    from pyiceberg.catalog import load_catalog

    start_time = time.time()
    sf_schema  = job["sf_schema"]
    sf_table   = job["sf_table"]

    # ── Step 1: fetch data from Snowflake ─────────────────────────────────────
    _log(f"Iceberg export: connecting to Snowflake for {sf_schema}.{sf_table}")
    conn   = snowflake.connector.connect(**sf_cfg)
    cursor = conn.cursor()

    use_filters = job.get("use_filters", False)
    filter_col  = job.get("filter_col", "")

    if job.get("source_query"):
        query = job["source_query"]
    elif use_filters and filter_col:
        query = f'SELECT * FROM "{sf_schema}"."{sf_table}" WHERE {filter_col.strip()}'
        _log(f"Iceberg export: filter applied — WHERE {filter_col.strip()}")
    else:
        query = f'SELECT * FROM "{sf_schema}"."{sf_table}"'

    cursor.execute(query)
    rows      = cursor.fetchall()
    col_names = [desc[0] for desc in cursor.description]
    cursor.close()
    conn.close()

    total_rows = len(rows)
    _log(f"Iceberg export: fetched {total_rows:,} rows from Snowflake")

    # ── Step 2: Write Apache Iceberg table to S3 & register with Glue ───────────
    df = pd.DataFrame(rows, columns=col_names)
    job["columns"] = list(df.columns)
    job["dtypes"] = {c: str(df[c].dtype) for c in df.columns}
    from iceberg_s3_writer import write_iceberg_table_to_s3
    res = write_iceberg_table_to_s3(
        df=df,
        s3_cfg=s3_cfg,
        schema_name=sf_schema,
        table_name=sf_table,
        log_fn=_log,
    )
    job["s3_key"] = res["s3_key"]
    return res


def _extract_databricks_to_s3_iceberg(job: dict, db_cfg: dict, s3_cfg: dict) -> dict:
    """
    Export a Databricks table to S3 in Apache Iceberg format.

    Flow:
      1. Fetch rows from Databricks (cursor.fetchall); stop on source errors
      2. Convert to a PyArrow Table
      3. Register and write the table through AWS Glue using pyiceberg
    """
    import time
    import pandas as pd
    import pyarrow as pa
    import mmh3_compat
    from pyiceberg.catalog import load_catalog

    start_time = time.time()
    db_schema  = job.get("source_schema") or job.get("sf_schema", "")
    db_table   = job.get("source_table") or job.get("sf_table", "")

    _log(f"Iceberg export: connecting to Databricks for {db_schema}.{db_table}")
    rows, col_names = [], []
    try:
        from databricks import sql as dbsql
        conn = dbsql.connect(
            server_hostname=db_cfg.get("host"),
            http_path=db_cfg.get("http_path"),
            access_token=db_cfg.get("token"),
        )
        cur = conn.cursor()
        cat = (db_cfg.get("catalog") or "").strip()
        if not cat or cat.lower() == "hive_metastore":
            active_cat = ""
            try:
                import streamlit as _st
                active_cat = _st.session_state.get("db_active_catalog", "") or _st.session_state.get("db_catalog", "")
            except Exception:
                pass
            cat = active_cat.strip()

        if cat and cat.lower() != "hive_metastore":
            tbl_ident = f"`{cat}`.`{db_schema}`.`{db_table}`"
        else:
            tbl_ident = f"`{db_schema}`.`{db_table}`"

        use_filters = job.get("use_filters", False)
        filter_col  = job.get("filter_col", "")

        if job.get("source_query"):
            query = job["source_query"]
        elif use_filters and filter_col:
            query = f"SELECT * FROM {tbl_ident} WHERE {filter_col.strip()}"
            _log(f"Iceberg export: filter applied — WHERE {filter_col.strip()}")
        else:
            query = f"SELECT * FROM {tbl_ident}"

        try:
            cur.execute(query)
        except Exception as q_err:
            if "UC_HIVE_METASTORE_DISABLED_EXCEPTION" in str(q_err) or "hive_metastore" in str(q_err).lower():
                fallback_ident = f"`{db_schema}`.`{db_table}`"
                if use_filters and filter_col:
                    query = f"SELECT * FROM {fallback_ident} WHERE {filter_col.strip()}"
                else:
                    query = f"SELECT * FROM {fallback_ident}"
                cur.execute(query)
            else:
                raise
        rows      = cur.fetchall()
        col_names = [desc[0] for desc in cur.description] if cur.description else []
        cur.close()
        conn.close()
    except Exception as exc:
        raise RuntimeError(f"Databricks read failed for {db_schema}.{db_table}: {exc}") from exc

    total_rows = len(rows)
    _log(f"Iceberg export: fetched {total_rows:,} rows from Databricks")

    # ── Step 2: Write Apache Iceberg table to S3 & register with Glue ───────────
    df = pd.DataFrame(rows, columns=col_names)
    job["columns"] = list(df.columns)
    job["dtypes"] = {c: str(df[c].dtype) for c in df.columns}
    from iceberg_s3_writer import write_iceberg_table_to_s3
    res = write_iceberg_table_to_s3(
        df=df,
        s3_cfg=s3_cfg,
        schema_name=db_schema,
        table_name=db_table,
        log_fn=_log,
    )
    job["s3_key"] = res["s3_key"]
    return res


def _extract_databricks_to_s3(job: dict, db_cfg: dict, s3_cfg: dict) -> dict:
    """
    Export a Databricks table to S3 in the requested format (Iceberg, Parquet, CSV, JSON, ORC).
    """
    fmt = job.get("file_format", "").upper()
    if fmt == "ICEBERG":
        return _extract_databricks_to_s3_iceberg(job, db_cfg, s3_cfg)

    import time
    import io
    import boto3
    import pandas as pd

    start_time = time.time()
    db_schema  = job.get("source_schema") or job.get("sf_schema", "")
    db_table   = job.get("source_table") or job.get("sf_table", "")

    rows, col_names = [], []
    try:
        from databricks import sql as dbsql
        conn = dbsql.connect(
            server_hostname=db_cfg.get("host"),
            http_path=db_cfg.get("http_path"),
            access_token=db_cfg.get("token"),
        )
        cur = conn.cursor()
        cat = (db_cfg.get("catalog") or "").strip()
        if not cat or cat.lower() == "hive_metastore":
            active_cat = ""
            try:
                import streamlit as _st
                active_cat = _st.session_state.get("db_active_catalog", "") or _st.session_state.get("db_catalog", "")
            except Exception:
                pass
            cat = active_cat.strip()

        if cat and cat.lower() != "hive_metastore":
            tbl_ident = f"`{cat}`.`{db_schema}`.`{db_table}`"
        else:
            tbl_ident = f"`{db_schema}`.`{db_table}`"

        query = f"SELECT * FROM {tbl_ident}"
        try:
            cur.execute(query)
        except Exception as q_err:
            if "UC_HIVE_METASTORE_DISABLED_EXCEPTION" in str(q_err) or "hive_metastore" in str(q_err).lower():
                query = f"SELECT * FROM `{db_schema}`.`{db_table}`"
                cur.execute(query)
            else:
                raise
        rows      = cur.fetchall()
        col_names = [desc[0] for desc in cur.description] if cur.description else []
        cur.close()
        conn.close()
    except Exception as exc:
        raise RuntimeError(f"Databricks read failed for {db_schema}.{db_table}: {exc}") from exc

    df = pd.DataFrame(rows, columns=col_names)
    job["columns"] = list(df.columns)
    job["dtypes"] = {c: str(df[c].dtype) for c in df.columns}
    if fmt == "PARQUET":
        import pyarrow as pa
        import pyarrow.parquet as pq
        pq_buf = io.BytesIO()
        pq.write_table(pa.Table.from_pandas(df, preserve_index=False), pq_buf,
                       compression="snappy", coerce_timestamps="us", version="1.0")
        body = pq_buf.getvalue()
    elif fmt == "ORC":
        import pyarrow as pa
        import pyarrow.orc as orc
        orc_buf = io.BytesIO()
        table = pa.Table.from_pandas(df, preserve_index=False)
        table = table.cast(pa.schema([pa.field(f.name, pa.string() if pa.types.is_null(f.type) else f.type)
                                      for f in table.schema]))
        orc.write_table(table, orc_buf)
        body = orc_buf.getvalue()
    elif fmt == "JSON":
        body = df.to_json(orient="records", lines=True, date_format="iso", date_unit="us").encode("utf-8")
    else:
        csv_buf = io.StringIO()
        df.to_csv(csv_buf, index=False)
        body = csv_buf.getvalue().encode("utf-8")

    s3_client = boto3.client(
        "s3",
        aws_access_key_id=s3_cfg["access_key"],
        aws_secret_access_key=s3_cfg["secret_key"],
        region_name=s3_cfg["region"],
    )
    s3_client.put_object(
        Bucket=s3_cfg["bucket"],
        Key=job["s3_key"],
        Body=body,
    )

    duration = round(time.time() - start_time, 2)
    return {
        "rows":          len(df),
        "bytes":         len(body),
        "duration_secs": duration,
    }


def _extract_snowflake_to_s3(job: dict, sf_cfg: dict, s3_cfg: dict) -> dict:
    """
    Use Snowflake's COPY INTO to unload data directly into S3.

    Supports two optional modes driven by job flags set from the metadata table:
      use_partitions — builds a hierarchical S3 path (col=val/col=val/...)
                       by running one COPY per distinct partition key combination.
      use_filters    — injects a WHERE clause via a subquery SELECT.

    If the table has no metadata row the flags are False and export is plain.

    Iceberg is the Snowflake-to-S3 destination. Legacy CSV jobs are also
    delegated to the Iceberg writer so they cannot create CSV objects in S3.
    """
    # ── Route Iceberg exports to the dedicated function ───────────────────────
    if job.get("file_format", "").upper() == "ICEBERG":
        return _extract_snowflake_to_s3_iceberg(job, sf_cfg, s3_cfg)

    import snowflake.connector
    import time

    conn   = snowflake.connector.connect(**sf_cfg)
    cursor = conn.cursor()

    _fmt = job["file_format"].upper()
    if _fmt == "CSV":
        _ff = (
            "FILE_FORMAT=("
            "TYPE=CSV "
            "COMPRESSION=NONE "
            "FIELD_DELIMITER=',' "
            "FIELD_OPTIONALLY_ENCLOSED_BY=NONE "
            "NULL_IF=('') "
            "EMPTY_FIELD_AS_NULL=TRUE"
            ")"
        )
    else:
        _ff = f"FILE_FORMAT=(TYPE={_fmt} COMPRESSION=NONE)"

    _creds = (
        f"CREDENTIALS=("
        f"AWS_KEY_ID='{s3_cfg['access_key']}' "
        f"AWS_SECRET_KEY='{s3_cfg['secret_key']}')"
    )
    _header_opt = "HEADER=TRUE" if _fmt == "CSV" else ""

    def _parse_copy_result(result, description):
        if not result:
            return 0, 0
        t_rows = 0
        t_bytes = 0
        if description:
            col_names = [str(d[0]).lower() for d in description]
            row_col_idx = None
            byte_col_idx = None
            for cand in ("rows_unloaded", "row_count", "rows"):
                if cand in col_names:
                    row_col_idx = col_names.index(cand)
                    break
            for cand in ("output_bytes", "file_size", "bytes", "input_bytes"):
                if cand in col_names:
                    byte_col_idx = col_names.index(cand)
                    break
            if row_col_idx is not None and byte_col_idx is not None:
                for r in result:
                    try:
                        t_rows += int(r[row_col_idx] or 0)
                    except (ValueError, TypeError):
                        pass
                    try:
                        t_bytes += int(r[byte_col_idx] or 0)
                    except (ValueError, TypeError):
                        pass
                return t_rows, t_bytes

        for r in result:
            nums = [int(v) for v in r if isinstance(v, (int, float)) or (isinstance(v, str) and v.isdigit())]
            if len(nums) >= 2:
                t_rows += nums[0]
                t_bytes += nums[-1]
            elif len(nums) == 1:
                t_rows += nums[0]
        return t_rows, t_bytes

    def _execute_unload(s3_dest, source_query):
        # 1. Primary: SINGLE=TRUE with MAX_FILE_SIZE=5368709120 (5 GB)
        copy_single = f"""
            COPY INTO '{s3_dest}'
            FROM {source_query}
            {_creds}
            {_ff}
            OVERWRITE=TRUE
            {_header_opt}
            SINGLE=TRUE
            MAX_FILE_SIZE=5368709120;
        """
        try:
            cursor.execute(copy_single)
            res = cursor.fetchall()
            return _parse_copy_result(res, cursor.description)
        except Exception as copy_err:
            err_text = str(copy_err)
            if "100083" in err_text or "single file mode" in err_text.lower() or "max file size" in err_text.lower():
                _log(
                    f"Single-file limit (5GB) reached for {job.get('sf_schema')}.{job.get('sf_table')}; "
                    f"retrying with multi-part unload (SINGLE=FALSE, MAX_FILE_SIZE=5368709120)...",
                    "warn"
                )
                copy_multi = f"""
                    COPY INTO '{s3_dest}'
                    FROM {source_query}
                    {_creds}
                    {_ff}
                    OVERWRITE=TRUE
                    {_header_opt}
                    SINGLE=FALSE
                    MAX_FILE_SIZE=5368709120;
                """
                cursor.execute(copy_multi)
                res = cursor.fetchall()
                return _parse_copy_result(res, cursor.description)
            else:
                raise

    use_partitions = job.get("use_partitions", False)
    use_filters    = job.get("use_filters",    False)
    filter_col     = job.get("filter_col",     "")
    partition_cols = [c.strip() for c in job.get("partition_cols", "").split(",") if c.strip()]

    sf_schema = job["sf_schema"]
    sf_table  = job["sf_table"]
    s3_base   = f"s3://{s3_cfg['bucket']}/{job['s3_key'].rsplit('/', 1)[0]}"  # strip filename

    start_time  = time.time()
    total_rows  = 0
    total_bytes = 0

    if use_partitions and partition_cols:
        part_cols_upper = [c.upper() for c in partition_cols]
        part_col_csv    = ", ".join(part_cols_upper)

        if use_filters and filter_col:
            keys_sql = (
                f'SELECT DISTINCT {part_col_csv} '
                f'FROM "{sf_schema}"."{sf_table}" '
                f'WHERE {filter_col.strip()} '
                f'ORDER BY {part_col_csv}'
            )
        else:
            keys_sql = (
                f'SELECT DISTINCT {part_col_csv} '
                f'FROM "{sf_schema}"."{sf_table}" '
                f'ORDER BY {part_col_csv}'
            )

        _log(f"Fetching distinct partition keys for {sf_schema}.{sf_table}: [{part_col_csv}]")
        cursor.execute(keys_sql)
        partition_combos = cursor.fetchall()
        col_names = [desc[0] for desc in cursor.description]
        _log(f"Found {len(partition_combos)} partition combination(s) — running individual COPYs")

        for combo in partition_combos:
            part_path_parts = [f"{col}={val}" for col, val in zip(col_names, combo)]
            part_suffix     = "/".join(str(p) for p in part_path_parts)
            s3_part_path    = f"{s3_base}/{part_suffix}/"

            part_where_parts = []
            for col, val in zip(col_names, combo):
                safe_val = str(val).replace("'", "''")
                part_where_parts.append(f"{col} = '{safe_val}'")
            part_where = " AND ".join(part_where_parts)

            if use_filters and filter_col:
                where_clause = f"WHERE {part_where} AND {filter_col.strip()}"
            else:
                where_clause = f"WHERE {part_where}"

            _source = f'(SELECT * FROM "{sf_schema}"."{sf_table}" {where_clause})'
            p_rows, p_bytes = _execute_unload(s3_part_path, _source)
            total_rows  += p_rows
            total_bytes += p_bytes

    else:
        s3_path = f"s3://{s3_cfg['bucket']}/{job['s3_key']}"

        if use_filters and filter_col:
            _source = f'(SELECT * FROM "{sf_schema}"."{sf_table}" WHERE {filter_col.strip()})'
            _log(f"Filter enabled — WHERE {filter_col.strip()}")
        else:
            _source = f'"{sf_schema}"."{sf_table}"'

        total_rows, total_bytes = _execute_unload(s3_path, _source)

    cursor.close()
    conn.close()

    return {
        "rows":          total_rows,
        "bytes":         total_bytes,
        "duration_secs": round(time.time() - start_time, 2),
    }
def _load_s3_to_redshift(job: dict, s3_cfg: dict, rs_cfg: dict) -> dict:
    """
    COPY data from S3 into Redshift for one table.

    Parameters
    ----------
    job    : dict containing job details (e.g., table names, file format, etc.)
    s3_cfg : S3 connection config dict
    rs_cfg : Redshift connection config dict

    Returns
    -------
    dict with rs_rows and rs_duration_secs
    """
    import boto3
    import time

    rs_schema = (job.get("rs_schema") or "public").strip()
    rs_table  = (job.get("rs_table") or job.get("source_table") or job.get("sf_table") or "table").strip()
    fmt       = (job.get("file_format") or "CSV").upper()
    load_mode = job.get("load_mode", "TRUNCATE & LOAD")

    client = boto3.client(
        "redshift-data",
        region_name=rs_cfg.get("region") or s3_cfg.get("region") or "ap-south-1",
        aws_access_key_id=rs_cfg.get("access_key") or s3_cfg.get("access_key"),
        aws_secret_access_key=rs_cfg.get("secret_key") or s3_cfg.get("secret_key"),
    )

    def _execute_sync(sql: str, timeout: int = 120, statement_key=None):
        s_id = job.get(statement_key) if statement_key else None
        if not s_id:
            resp = client.execute_statement(
                WorkgroupName=rs_cfg["workgroup"],
                Database=rs_cfg["database"],
                Sql=sql,
            )
            s_id = resp["Id"]
            if statement_key:
                job[statement_key] = s_id
        dl = time.time() + timeout
        while time.time() < dl:
            d = client.describe_statement(Id=s_id)
            stt = d["Status"]
            if stt == "FINISHED":
                return d
            if stt in ("FAILED", "ABORTED"):
                if statement_key:
                    job.pop(statement_key, None)
                raise RuntimeError(f"{d.get('Error', 'Unknown statement error')}")
            time.sleep(1)
        raise TimeoutError(f"Redshift statement timed out after {timeout}s: {sql[:60]}")

    # ── Special Handling for External Redshift Schemas (Glue / Spectrum) ──────
    is_external = (
        rs_schema.lower().endswith("_spectrum")
        or rs_schema.lower().startswith("spectrum_")
        or rs_schema.lower() in ("awsdatacatalog", "spectrum")
        or "datacatalog" in rs_schema.lower()
    )

    if not is_external:
        try:
            chk_ext = client.execute_statement(
                WorkgroupName=rs_cfg["workgroup"],
                Database=rs_cfg["database"],
                Sql=f"SELECT 1 FROM svv_external_schemas WHERE LOWER(schemaname) = '{rs_schema.lower()}'",
            )
            eid = chk_ext["Id"]
            for _ in range(10):
                ed = client.describe_statement(Id=eid)
                if ed["Status"] == "FINISHED":
                    if ed.get("HasResultSet", False):
                        r = client.get_statement_result(Id=eid)
                        if r.get("Records"):
                            is_external = True
                    break
                if ed["Status"] in ("FAILED", "ABORTED"):
                    break
                time.sleep(0.5)
        except Exception:
            pass

    if is_external:
        if fmt == "ICEBERG":
            _log(
                f"Schema '{rs_schema}' is an external AWS Glue / Redshift Spectrum catalog. "
                f"Table '{rs_schema}.{rs_table}' is registered in AWS Glue Data Catalog "
                f"and is queryable directly via Redshift Spectrum (SELECT * FROM \"{rs_schema}\".\"{rs_table}\").",
                "ok"
            )
            return {"rs_rows": job.get("rows", 0), "rs_duration_secs": 0.1}
        else:
            raise RuntimeError(
                f"Target schema '{rs_schema}' is an external AWS Glue / Redshift Spectrum schema. "
                f"Redshift COPY only loads into standard local schemas (such as 'public'). "
                f"Please rebuild the queue with 'public' as the Target Redshift Schema, or export in Iceberg format."
            )

    # ── Ensure target table exists in Redshift (Auto-create if needed) ────────
    table_exists = False
    try:
        chk_sql = (
            f"SELECT 1 FROM information_schema.tables "
            f"WHERE table_schema = '{rs_schema.lower()}' AND table_name = '{rs_table.lower()}'"
        )
        resp = client.execute_statement(
            WorkgroupName=rs_cfg["workgroup"],
            Database=rs_cfg["database"],
            Sql=chk_sql,
        )
        cid = resp["Id"]
        for _ in range(20):
            d = client.describe_statement(Id=cid)
            if d["Status"] == "FINISHED":
                if d.get("HasResultSet", False):
                    res = client.get_statement_result(Id=cid)
                    if res.get("Records"):
                        table_exists = True
                break
            if d["Status"] in ("FAILED", "ABORTED"):
                break
            time.sleep(1)
    except Exception:
        pass

    cols = job.get("columns", [])
    dtypes = job.get("dtypes", {})
    if not table_exists and cols:
        type_map = {
            "int64": "BIGINT",
            "int32": "INTEGER",
            "int16": "SMALLINT",
            "float64": "DOUBLE PRECISION",
            "float32": "REAL",
            "bool": "BOOLEAN",
            "datetime64[ns]": "TIMESTAMP",
            "timestamp": "TIMESTAMP",
            "date": "DATE",
        }
        col_defs = []
        for c in cols:
            dt = dtypes.get(c, "object").lower()
            rs_type = "VARCHAR(65535)"
            for k, v in type_map.items():
                if k in dt:
                    rs_type = v
                    break
            col_defs.append(f'"{c}" {rs_type}')
        create_sql = f'CREATE TABLE IF NOT EXISTS "{rs_schema}"."{rs_table}" ({", ".join(col_defs)});'
        _log(f"Target table '{rs_schema}.{rs_table}' not found in Redshift — auto-creating...", "info")
        try:
            _execute_sync(create_sql, timeout=30)
            table_exists = True
            _log(f"Created table '{rs_schema}.{rs_table}' successfully", "ok")
        except Exception as c_err:
            raise RuntimeError(f"Target table creation failed: {c_err}") from c_err

    target = f'"{rs_schema}"."{rs_table}"'

    def _target_count():
        desc = _execute_sync(f'SELECT COUNT(*) FROM {target}', timeout=30)
        result = client.get_statement_result(Id=desc["Id"])
        return int(result["Records"][0][0]["longValue"])

    if "target_rows_before" not in job:
        job["target_rows_before"] = _target_count()

    # ── Handle TRUNCATE & LOAD ────────────────────────────────────────────────
    if load_mode == "TRUNCATE & LOAD" and not job.get("load_statement_id"):
        try:
            _execute_sync(f'TRUNCATE TABLE {target}', timeout=30, statement_key="truncate_statement_id")
            _log(f"Truncated existing table '{rs_schema}.{rs_table}' before loading", "info")
        except Exception as t_err:
            raise RuntimeError(f"Target truncate failed; COPY was not started: {t_err}") from t_err

    # ── Construct Redshift COPY command ───────────────────────────────────────
    s3_bucket = s3_cfg["bucket"]
    s3_key = job["s3_key"]
    ak = s3_cfg["access_key"]
    sk = s3_cfg["secret_key"]
    s3_region = s3_cfg.get("region") or rs_cfg.get("region") or "ap-south-1"

    if fmt == "ICEBERG":
        # Redshift COPY ingests the parquet data files under /data/
        s3_path = f"s3://{s3_bucket}/{s3_key.rstrip('/')}/data/"
        format_clause = "FORMAT AS PARQUET"
    elif fmt == "PARQUET":
        s3_path = f"s3://{s3_bucket}/{s3_key}"
        format_clause = "FORMAT AS PARQUET"
    elif fmt == "CSV":
        s3_path = f"s3://{s3_bucket}/{s3_key}"
        delim = job.get("delimiter", ",")
        if len(delim) > 1:
            delim_map = {",  (comma)": ",", "|  (pipe)": "|", "\\t  (tab)": "\t", ";  (semicolon)": ";"}
            delim = delim_map.get(delim, ",")
        hdr_clause = "IGNOREHEADER 1" if job.get("include_header", True) else ""
        gzip_clause = "GZIP" if job.get("compression", False) else ""
        format_clause = f"""
        FORMAT AS CSV
        DELIMITER '{delim}'
        {hdr_clause}
        {gzip_clause}
        NULL AS ''
        TRUNCATECOLUMNS
        ACCEPTINVCHARS '?'
        TIMEFORMAT 'auto'
        DATEFORMAT 'auto'
        EMPTYASNULL
        BLANKSASNULL"""
    else:
        s3_path = f"s3://{s3_bucket}/{s3_key}"
        format_clause = f"FORMAT AS {fmt}"

    copy_command = f"""
        COPY "{rs_schema}"."{rs_table}"
        FROM '{s3_path}'
        ACCESS_KEY_ID '{ak}'
        SECRET_ACCESS_KEY '{sk}'
        REGION '{s3_region}'
        {format_clause}
        MAXERROR 0
        COMPUPDATE ON STATUPDATE ON;
    """

    _log(f"COPY path: {s3_path}")

    start_time = time.time()
    try:
        _execute_sync(copy_command, timeout=3600, statement_key="load_statement_id")
    except Exception as copy_err:
        err_str = str(copy_err)
        if "external schema" in err_str.lower():
            raise RuntimeError(
                f"Target schema '{rs_schema}' is an external Redshift Spectrum / AWS Glue schema. "
                f"Redshift COPY only loads into standard local schemas (such as 'public'). "
                f"Please rebuild the queue targeting 'public', or export in Iceberg format."
            )
        if "relation" in err_str.lower() and "does not exist" in err_str.lower():
            raise RuntimeError(
                f"Target table '{rs_schema}.{rs_table}' does not exist in Redshift. "
                f"Please create the table in Redshift first or verify column metadata."
            )
        raise RuntimeError(f"COPY failed: {err_str}")

    duration = round(time.time() - start_time, 2)

    # Separate Data API calls do not share pg_last_copy_count() session state.
    try:
        job["target_rows_after"] = _target_count()
    except Exception as exc:
        raise RuntimeError(f"COPY finished but target row-count verification failed: {exc}") from exc
    baseline = job["target_rows_before"] if load_mode == "APPEND" else 0
    rs_rows = job["target_rows_after"] - baseline
    if rs_rows != job["rows"]:
        raise RuntimeError(f"Target row-count mismatch: expected {job['rows']} loaded rows, found {rs_rows}.")

    _log(f"Loaded {rs_rows:,} rows into Redshift table '{rs_schema}.{rs_table}' in {duration}s", "ok")
    return {
        "rs_rows": rs_rows,
        "rs_duration_secs": duration,
    }


# ─────────────────────────────────────────────────────────────────────────────
# Core executor
# ─────────────────────────────────────────────────────────────────────────────

def run_migration(
    sf_cfg:   dict,
    s3_cfg:   dict,
    rs_cfg:   dict,
    progress_placeholder,   # st.empty() — for overall progress bar
    status_placeholder,     # st.empty() — for current table status text
    db_cfg:   dict = None,
    source_type: str = None,
):
    """
    Sequential executor — iterates job_queue and runs each table migration.
    Skips already-checkpointed (done) jobs to support resume.

    Parameters
    ----------
    sf_cfg               : Snowflake connection config dict
    s3_cfg               : {"bucket": str, "prefix": str, "access_key": str,
                             "secret_key": str, "region": str}
    rs_cfg               : Redshift connection config dict
    progress_placeholder : st.empty() for overall progress bar
    status_placeholder   : st.empty() for live status text
    db_cfg               : Databricks connection config dict
    source_type          : "Snowflake" | "Databricks" | "BigQuery"
    """
    _init_checkpoint()
    queue = st.session_state.job_queue
    total = len(queue)
    st.session_state.migration_summary = {}
    st.session_state.migration_done = False

    if total == 0:
        st.warning("No jobs in queue. Go to the Table Discovery tab first.")
        return

    active_source = source_type or st.session_state.get("source_type", "Snowflake")
    route = resolve_route(st.session_state.get("pipeline_destination", "Source → S3 → Redshift"), active_source)
    bq_run = is_bigquery(route)
    if any(j.get("source_type", active_source).lower() != active_source.lower() for j in queue):
        st.error("The queue belongs to another source. Rebuild it for the selected source.")
        return False
    if active_source.lower() == "databricks" and st.session_state.get("db_is_demo"):
        st.error("Demo tables cannot be exported. Connect to a live Databricks source first.")
        return False

    # ── BigQuery-specific pre-flight validation ──────────────────────────────
    if bq_run:
        # Ensure queue was built for this route, not a leftover SF/DBX queue
        if any((j.get("source_type") == "bigquery") != bq_run
               or j.get("destination", route) != route for j in queue):
            st.error("The queue belongs to another route. Build it again for the selected destination.")
            return False
        # Validate connection + config fingerprint are unchanged since queue build
        try:
            from bigquery_ui import config_fingerprint
            if any(j.get("bq_connection_id") != st.session_state.get("bq_connection_id") or
                   j.get("config_fingerprint") != config_fingerprint(s3_cfg, rs_cfg, route)
                   for j in queue):
                st.error("Connection or destination settings changed. Rebuild the BigQuery queue.")
                return False
        except ImportError:
            pass
        # Iceberg is unavailable on the BigQuery → Redshift route
        if any(j["file_format"] not in formats_for(route) for j in queue):
            st.error("Iceberg is unavailable for the BigQuery → Redshift route.")
            return False

    st.session_state.migration_running = True
    st.session_state.migration_done    = False

    completed = 0
    failed    = 0
    total_rows  = 0
    total_bytes = 0
    total_secs  = 0.0

    run_start = time.time()
    _log(f"Migration started · {total} tables queued", "info")

    for idx, job in enumerate(queue):

        # ── Skip already done (checkpoint resume) ──
        if _is_checkpointed(job["id"]):
            _log(f"[{idx+1}/{total}] SKIP (checkpointed) · {job['id']}", "info")
            completed += 1
            total_rows  += job.get("rows", 0)
            total_bytes += job.get("bytes", 0)
            continue

        # ── Mark running ──
        _update_job(job["id"], status="running")
        _src_label = source_name(job) if bq_run else (
            f"{job.get('source_schema', job.get('sf_schema',''))}.{job.get('source_table', job.get('sf_table',''))}"
        )
        _dst_label = "S3" if s3_only(route) else f"{job.get('rs_schema','')}.{job.get('rs_table','')}"
        status_placeholder.markdown(
            f"<small style='color:#5b8dee'>⟳ &nbsp;[{idx+1}/{total}] &nbsp;"
            f"{_src_label} → {_dst_label}</small>",
            unsafe_allow_html=True,
        )

        job_start = time.time()

        # ── Phase 1: Source → S3 ──
        try:
            _log(f"[{idx+1}/{total}] Extract · {job['id']}", "info")
            job_src = job.get("source_type") or source_type or st.session_state.get("source_type", "Snowflake")

            if job.get("export_complete"):
                sf_result = {"rows": job["rows"], "bytes": job["bytes"]}
            elif job_src.lower() == "bigquery" or bq_run:
                # BigQuery → S3 export via bigquery_migration
                from bigquery_migration import extract_to_s3 as _bq_extract
                sf_result = _bq_extract(job, st.session_state.get("bq_client"), s3_cfg)
            elif job_src.lower() == "databricks":
                sf_result = _extract_databricks_to_s3(job, db_cfg or {}, s3_cfg)
            else:
                sf_result = _extract_snowflake_to_s3(job, sf_cfg or {}, s3_cfg)

            _update_job(
                job["id"],
                rows=sf_result["rows"],
                bytes=sf_result["bytes"],
                export_complete=True,
            )
        except Exception as e:
            err = f"{'BigQuery' if (bq_run or job.get('source_type') == 'bigquery') else 'Source'}→S3 failed: {e}"
            _update_job(job["id"], status="failed", error=err)
            _mark_checkpoint(job["id"], "failed")
            _log(f"[{idx+1}/{total}] ERROR · {job['id']} · {err}", "err")
            failed += 1
            overall_pct = int(((idx + 1) / total) * 100)
            progress_placeholder.progress(overall_pct, text=f"Processing… {idx+1}/{total}")
            continue

        # ── Phase 2: S3 → Redshift (skipped when pipeline_destination is S3-only) ──
        if s3_only(route):
            _log(f"[{idx+1}/{total}] S3-only mode — Redshift COPY skipped · {job['id']}", "info")
            rs_result = {"rs_rows": 0, "rs_duration_secs": 0}
        else:
            try:
                _log(f"[{idx+1}/{total}] Load  · {job['id']}", "info")
                if bq_run or job.get("source_type") == "bigquery":
                    from bigquery_migration import load_to_redshift as _bq_load
                    job["phase"] = "loading Redshift"
                    rs_result = _bq_load(job, s3_cfg, rs_cfg)
                else:
                    rs_result = _load_s3_to_redshift(job, s3_cfg, rs_cfg)
            except Exception as e:
                err = f"S3→RS failed: {e}"
                _update_job(job["id"], status="failed", error=err)
                _mark_checkpoint(job["id"], "failed")
                _log(f"[{idx+1}/{total}] ERROR · {job['id']} · {err}", "err")
                failed += 1
                overall_pct = int(((idx + 1) / total) * 100)
                progress_placeholder.progress(overall_pct, text=f"Processing… {idx+1}/{total}")
                break

        # ── Mark done ──
        job_secs = round(time.time() - job_start, 2)
        _update_job(
            job["id"],
            status="done",
            phase="done",
            duration_secs=job_secs,
            error="",
        )
        _mark_checkpoint(job["id"], "done")

        total_rows  += sf_result["rows"]
        total_bytes += sf_result["bytes"]
        total_secs  += job_secs
        completed   += 1

        _log(
            f"[{idx+1}/{total}] ✓ {job['id']} · "
            f"{sf_result['rows']:,} rows · {_format_bytes(sf_result['bytes'])} · {job_secs}s",
            "ok",
        )

        # ── Overall progress ──
        overall_pct = int(((idx + 1) / total) * 100)
        progress_placeholder.progress(
            overall_pct,
            text=f"Migrating… {idx+1}/{total} tables · {completed} done · {failed} failed",
        )

    # ── Wrap up ──────────────────────────────────────────────────────────────
    # Include checkpointed jobs later in the queue when a failure stops a retry.
    done_jobs = [j for j in queue if _is_checkpointed(j["id"])]
    completed = len(done_jobs)
    failed = sum(j.get("status") == "failed" for j in queue)
    total_rows = sum(j.get("rows", 0) for j in done_jobs)
    total_bytes = sum(j.get("bytes", 0) for j in done_jobs)
    wall_secs = round(time.time() - run_start, 1)
    st.session_state.migration_running = False
    st.session_state.migration_done    = failed == 0 and completed == total

    st.session_state.migration_summary = {
        "total":        total,
        "completed":    completed,
        "failed":       failed,
        "pending":      total - completed - failed,
        "total_rows":   total_rows,
        "total_bytes":  total_bytes,
        "total_secs":   wall_secs,
        "throughput":   int(total_bytes / wall_secs) if wall_secs > 0 else 0,
        "finished_at":  datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    }

    _log(
        f"Migration {'complete' if st.session_state.migration_done else 'incomplete'} · {completed}/{total} succeeded · "
        f"{failed} failed · {_format_bytes(total_bytes)} · {wall_secs}s total",
        "ok" if failed == 0 else "warn",
    )

    status_placeholder.empty()
    if st.session_state.migration_done:
        progress_placeholder.progress(100, text="Migration complete ✓")
    else:
        progress_placeholder.progress(int((completed + failed) / total * 100),
                                      text=f"Migration incomplete · {failed} failed · {total - completed - failed} pending")
    return st.session_state.migration_done


# ─────────────────────────────────────────────────────────────────────────────
# UI — Execution controls
# ─────────────────────────────────────────────────────────────────────────────

def render_execution_controls(
    sf_cfg: dict,
    s3_cfg: dict,
    rs_cfg: dict,
    db_cfg: dict = None,
    source_type: str = None,
):
    """
    Renders Run / Resume / Reset controls and fires run_migration() on click.

    Place this at the bottom of the pipeline configuration section,
    replacing the old single-table run buttons from Tab 1 & Tab 2.
    """
    queue    = st.session_state.get("job_queue", [])
    ckpt     = st.session_state.get("checkpoint", {})
    is_done  = st.session_state.get("migration_done", False)

    _active_src = source_type or st.session_state.get("source_type", "Snowflake")
    _route = resolve_route(st.session_state.get("pipeline_destination", "Source → S3 → Redshift"), _active_src)
    _bq = is_bigquery(_route)
    is_db = (_active_src == "Databricks")
    if _bq:
        src_name = "BigQuery"
        src_ok   = st.session_state.get("bq_connected", False)
    elif is_db:
        src_name = "Databricks"
        src_ok   = st.session_state.get("db_connected", False)
    else:
        src_name = "Snowflake"
        src_ok   = st.session_state.get("sf_connected", False)
    rs_ok  = st.session_state.get("rs_connected", False)

    # How many jobs are already checkpointed as done
    already_done = sum(1 for j in queue if ckpt.get(j["id"]) == "done")
    remaining    = len(queue) - already_done
    # Failed first runs must remain resumable for every source.
    is_resume    = remaining > 0 and (
        already_done > 0 or any(j["status"] == "failed" for j in queue)
    )

    st.markdown("---")
    st.markdown('<div class="card-title">🚀 Execute Migration</div>', unsafe_allow_html=True)

    _dest_mode = st.session_state.get("pipeline_destination", "Source → S3 → Redshift")
    _s3_only = s3_only(_route)

    col_r1, col_r2, col_r3, col_r4 = st.columns([1, 1, 1, 3])

    # ── Run / Resume ──
    with col_r1:
        btn_label    = f"▶ Resume ({remaining} left)" if is_resume else f"🚀 Run All ({len(queue)} tables)"
        # Redshift connection not required when destination is S3-only
        btn_disabled = (not queue) or (not src_ok) or (not rs_ok and not _s3_only) or is_done

        run_clicked = st.button(btn_label, key="btn_run_migration", disabled=btn_disabled)

    # ── Reset checkpoint (start over) ──
    with col_r2:
        reset_clicked = st.button("↺ Reset & Start Over", key="btn_reset_migration", disabled=not queue)

    # ── Clear queue ──
    with col_r3:
        clear_clicked = st.button("🗑 Clear Queue", key="btn_clear_queue", disabled=not queue)

    # ── Status badge ──
    with col_r4:
        if not queue:
            st.markdown('<span class="badge badge-pending">⚠ Build queue first (Step 1)</span>', unsafe_allow_html=True)
        elif not src_ok or (not rs_ok and not _s3_only):
            _conn_msg = f"⚠ Connect {src_name} first" if (src_ok and _s3_only) else f"⚠ Connect {src_name} and Redshift first"
            st.markdown(f'<span class="badge badge-pending">{_conn_msg}</span>', unsafe_allow_html=True)
        elif is_done:
            summary = st.session_state.get("migration_summary", {})
            c = summary.get("completed", 0)
            f = summary.get("failed", 0)
            cls = "badge-connected" if f == 0 else "badge-pending"
            st.markdown(f'<span class="badge {cls}">✓ Done · {c} succeeded · {f} failed</span>', unsafe_allow_html=True)
        elif is_resume:
            st.markdown(
                f'<span class="badge badge-pending">↺ Resumable · {already_done} done · {remaining} remaining</span>',
                unsafe_allow_html=True,
            )
        else:
            st.markdown(
                f'<span class="badge badge-connected">✓ Ready · {len(queue)} tables queued</span>',
                unsafe_allow_html=True,
            )

    # ── Handle button actions ──────────────────────────────────────────────

    if reset_clicked:
        st.session_state.checkpoint       = {}
        st.session_state.migration_done   = False
        st.session_state.migration_running= False
        st.session_state.migration_summary= {}
        # Reset all job statuses back to pending
        for job in st.session_state.get("job_queue", []):
            job["status"]        = "pending"
            job["rows"]          = 0
            job["bytes"]         = 0
            job["duration_secs"] = 0.0
            job["error"]         = ""
            for _k in ("export_complete", "load_statement_id", "truncate_statement_id",
                       "target_rows_before", "target_rows_after"):
                job.pop(_k, None)
            # Clear BigQuery-specific idempotency keys so Redshift Data API retries cleanly
            if job.get("source_type") == "bigquery":
                import uuid
                job["run_id"] = uuid.uuid4().hex
                job["phase"] = "pending"
                for _k in ("export_complete", "load_statement_id", "truncate_statement_id",
                            "target_rows_before", "target_rows_after", "copy_sql",
                            "copy_attempt", "truncate_attempt", "manifest_key"):
                    job.pop(_k, None)
        _log("Migration reset — all jobs back to pending", "warn")
        st.rerun()

    if clear_clicked:
        st.session_state.job_queue         = []
        st.session_state.checkpoint        = {}
        st.session_state.migration_done    = False
        st.session_state.migration_running = False
        st.session_state.migration_summary = {}
        _log("Job queue cleared", "warn")
        st.rerun()

    if run_clicked:
        progress_ph = st.progress(0, text="Starting migration…")
        status_ph   = st.empty()
        outcome = run_migration(
            sf_cfg, s3_cfg, rs_cfg, progress_ph, status_ph,
            db_cfg=db_cfg, source_type=_active_src,
        )
        if outcome is False and not st.session_state.get("migration_summary"):
            return

        # Final outcome banner
        summary = st.session_state.get("migration_summary", {})
        failed  = summary.get("failed", 0)
        done    = summary.get("completed", 0)
        total   = summary.get("total", 0)
        secs    = summary.get("total_secs", 0)
        rows    = summary.get("total_rows", 0)
        size    = _format_bytes(summary.get("total_bytes", 0))

        if st.session_state.get("migration_done"):
            st.success(
                f"✅ All {total} tables migrated successfully — "
                f"**{rows:,} rows** · **{size}** · **{secs}s**"
            )
        else:
            st.warning(
                f"⚠ Migration finished with issues — "
                f"{done} succeeded, {failed} failed, {summary.get('pending', 0)} pending. "
                f"See below and check the Monitoring tab for details. Click Resume to retry failed tables."
            )
            # Render detailed error cards directly on the active screen for immediate visibility
            for j in queue:
                if j.get("status") == "failed":
                    err_msg = j.get("error") or "Unknown error"
                    st.error(f"❌ **Failed Table: `{j['id']}`**\n\n`{err_msg}`")
                    if any(k in err_msg.lower() for k in ("external", "spectrum", "awsdatacatalog")):
                        st.info(
                            "💡 **Root Cause & Fix:** Target schema is an external schema (e.g. `awsdatacatalog` or Spectrum). "
                            "Amazon Redshift `COPY` only loads into standard local tables (such as `public`).\n\n"
                            "**How to resolve:**\n"
                            "1. Go to the **Table Discovery** tab.\n"
                            "2. Under Step 3, set **Target Redshift Schema** to **`public`** (or select **Iceberg** format).\n"
                            "3. Click **📋 Build Migration Queue**, then re-run."
                        )
                    elif "relation" in err_msg.lower() and "does not exist" in err_msg.lower():
                        st.info(
                            f"💡 **Root Cause & Fix:** The target table does not exist in Redshift.\n\n"
                            f"**How to resolve:** Create the target table in schema `{j.get('rs_schema') or 'public'}` in Redshift before loading, or ensure source discovery detected columns."
                        )
                    elif "100083" in err_msg or "max file size" in err_msg.lower():
                        st.info(
                            "💡 **Root Cause & Fix:** Snowflake single-file limit (16MB default) was reached. "
                            "Unload parameters have been automatically upgraded to 5GB max file size with multi-part fallback. "
                            "Click **Resume** to re-run the failed table."
                        )
                    elif "source→s3" in err_msg.lower():
                        st.info(
                            "💡 **Root Cause & Fix:** Data export from Source to S3 failed. Check source connectivity, query permissions, or S3 bucket write permissions."
                        )
