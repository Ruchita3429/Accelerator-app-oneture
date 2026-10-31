
# ── Compatibility Shims ───────────────────────────────────────────────────────
import mmh3_compat  # Pure-Python mmh3 fallback if C-extension is blocked by WDAC / AppLocker

import streamlit as st
import time
from datetime import datetime


# ── Demo data (fallback when connectors aren't installed / creds absent) ──────
_DEMO_DATA = {
    "PUBLIC":    ["CUSTOMERS", "ORDERS", "PRODUCTS", "INVOICES", "PAYMENTS"],
    "ANALYTICS": ["DAILY_REVENUE", "USER_RETENTION", "FUNNEL_METRICS", "COHORT_ANALYSIS"],
    "RAW":       ["EVENTS_RAW", "CLICKSTREAM", "LOGS_INGESTION", "WEBHOOK_EVENTS"],
    "STAGING":   ["STG_CUSTOMERS", "STG_ORDERS", "STG_PRODUCTS", "STG_PAYMENTS"],
    "MARTS":     ["DIM_DATE", "FACT_SALES", "DIM_CUSTOMER", "DIM_PRODUCT", "FACT_EVENTS"],
    "FINANCE":   ["GL_TRANSACTIONS", "AP_INVOICES", "AR_INVOICES", "BUDGET_ACTUALS"],
    "OPS":       ["WAREHOUSE_INVENTORY", "SHIPMENTS", "RETURNS", "SUPPORT_TICKETS"],
}

_DEMO_DATA_DB = {
    "DEFAULT":   ["SAMPLES_NYCTAXI", "EVENTS_STREAM", "DEVICE_TELEMETRY"],
    "MAIN":      ["CUSTOMERS_DELTA", "ORDERS_DELTA", "PRODUCTS_DELTA"],
    "PUBLIC":    ["LANDING_CUSTOMERS", "LANDING_ORDERS", "RAW_TRANSACTIONS", "CLICKSTREAM"],
    "DWH":       ["DIM_CUSTOMER", "FACT_ORDERS", "DIM_PRODUCT", "FACT_INVENTORY"],
    "REPORTING": ["RPT_SALES_DAILY", "RPT_CHURN", "RPT_FINANCIAL_SUMMARY"],
    "STAGING":   ["STG_INBOUND", "STG_USERS", "STG_PAYMENTS"],
}


# ── Real Snowflake fetch (graceful fallback) ──────────────────────────────────
def _fetch_all_tables_snowflake(sf_cfg: dict) -> dict:
    """
    Returns dict[schema_name -> list[table_name]].
    Falls back to demo data if connector unavailable or auth fails.
    """
    try:
        import snowflake.connector
        conn = snowflake.connector.connect(**sf_cfg)
        cur  = conn.cursor()

        # Get schemas
        cur.execute("SHOW SCHEMAS")
        schemas = [r[1] for r in cur.fetchall() if r[1] not in ("INFORMATION_SCHEMA",)]

        result = {}
        for schema in schemas:
            try:
                cur.execute(f"SHOW TABLES IN SCHEMA {sf_cfg['database']}.{schema}")
                result[schema] = [r[1] for r in cur.fetchall()]
            except Exception:
                result[schema] = []

        conn.close()
        return result

    except Exception:
        # Demo fallback — clearly labelled in UI
        return _DEMO_DATA


# ── Real Databricks fetch (multi-catalog fallback & live scan) ────────────────
def _fetch_all_tables_databricks(db_cfg: dict) -> tuple[dict, str | None]:
    """
    Returns (dict[schema_name -> list[table_name]], error_msg | None) from Databricks.
    Executes live queries against Databricks with robust fallbacks.
    Falls back to demo data only if connector is absent, credentials missing, or queries fail.
    """
    import re
    cleaned = dict(db_cfg or {})
    host = str(cleaned.get("host") or "").strip()
    host = re.sub(r"^https?://", "", host).split("/")[0].split("?")[0].strip()
    cleaned["host"] = host

    http_path = str(cleaned.get("http_path") or "").strip()
    if http_path and not http_path.startswith("/"):
        http_path = "/" + http_path
    cleaned["http_path"] = http_path

    cleaned["token"] = str(cleaned.get("token") or "").strip()
    user_catalog = str(cleaned.get("catalog") or "").strip()

    if not host or not http_path or not cleaned["token"]:
        return _DEMO_DATA_DB, "Databricks credentials missing; showing demo tables."

    try:
        from databricks import sql as dbsql
    except ImportError:
        return _DEMO_DATA_DB, "databricks-sql-connector not installed; showing demo tables."

    try:
        conn = dbsql.connect(
            server_hostname=host,
            http_path=http_path,
            access_token=cleaned["token"],
        )
    except Exception as ce:
        return _DEMO_DATA_DB, f"Connection failed: {ce}"

    try:
        cur = conn.cursor()
        schemas = []
        effective_catalog = user_catalog

        # 1. Discover Catalogs
        available_catalogs = []
        try:
            cur.execute("SHOW CATALOGS")
            for r in cur.fetchall():
                cname = str(r[0]).strip()
                if cname.lower() not in ("system", "__databricks_internal"):
                    available_catalogs.append(cname)
        except Exception:
            pass

        # 2. Query schemas
        # Strategy A: Try specified catalog
        if user_catalog:
            try:
                cur.execute(f"SHOW SCHEMAS IN `{user_catalog}`")
                schemas = [
                    str(r[0]).strip() for r in cur.fetchall()
                    if str(r[0]).strip().lower() not in ("information_schema",)
                ]
                if schemas:
                    effective_catalog = user_catalog
            except Exception:
                schemas = []

        # Strategy B: Try default warehouse catalog (SHOW SCHEMAS)
        if not schemas:
            try:
                cur.execute("SHOW SCHEMAS")
                schemas = [
                    str(r[0]).strip() for r in cur.fetchall()
                    if str(r[0]).strip().lower() not in ("information_schema",)
                ]
                if schemas:
                    effective_catalog = ""
            except Exception:
                schemas = []

        # Strategy C: Try available catalogs
        if not schemas and available_catalogs:
            for cat in available_catalogs:
                if cat == user_catalog:
                    continue
                try:
                    cur.execute(f"SHOW SCHEMAS IN `{cat}`")
                    cat_schemas = [
                        str(r[0]).strip() for r in cur.fetchall()
                        if str(r[0]).strip().lower() not in ("information_schema",)
                    ]
                    if cat_schemas:
                        schemas = cat_schemas
                        effective_catalog = cat
                        break
                except Exception:
                    continue

        if not schemas:
            cur.close()
            conn.close()
            return _DEMO_DATA_DB, "Connected to Databricks, but found 0 schemas (or permission denied)."

        # 3. Query tables per schema
        result = {}
        for schema in schemas:
            tbls = []
            if effective_catalog:
                try:
                    cur.execute(f"SHOW TABLES IN `{effective_catalog}`.`{schema}`")
                    for r in cur.fetchall():
                        tname = getattr(r, "tableName", None) or (r[1] if len(r) > 1 else r[0])
                        if tname:
                            tbls.append(str(tname).strip())
                except Exception:
                    pass

            if not tbls:
                try:
                    cur.execute(f"SHOW TABLES IN `{schema}`")
                    for r in cur.fetchall():
                        tname = getattr(r, "tableName", None) or (r[1] if len(r) > 1 else r[0])
                        if tname:
                            tbls.append(str(tname).strip())
                except Exception:
                    pass

            if not tbls:
                try:
                    query = f"SELECT table_name FROM information_schema.tables WHERE table_schema = '{schema}'"
                    if effective_catalog:
                        query = f"SELECT table_name FROM `{effective_catalog}`.information_schema.tables WHERE table_schema = '{schema}'"
                    cur.execute(query)
                    for r in cur.fetchall():
                        if r and r[0]:
                            tbls.append(str(r[0]).strip())
                except Exception:
                    pass

            result[schema] = sorted(list(set(tbls)))

        cur.close()
        conn.close()

        if result and any(result.values()):
            return result, None
        return result, "Schemas found, but no tables exist or permission denied on tables."

    except Exception as e:
        try:
            conn.close()
        except Exception:
            pass
        return _DEMO_DATA_DB, f"Query error: {e}"


# ── Build S3 key for a given table ───────────────────────────────────────────
def _build_s3_key(s3_prefix: str, schema: str, table: str, fmt: str, ts: str) -> str:
    if fmt.upper() == "ICEBERG":
        # An Iceberg table is a directory containing data/ and metadata/; it is
        # not a single export file.
        path_parts = [s3_prefix.strip("/"), schema.lower(), table.lower()]
        return "/".join(part for part in path_parts if part) + "/"
    ext = {"CSV": "csv", "Parquet": "parquet", "JSON": "json", "ORC": "orc"}.get(fmt, "csv")
    prefix = s3_prefix.rstrip("/")
    return f"{prefix}/{schema}/{table}_{ts}.{ext}"


# ── Build job queue from selections ──────────────────────────────────────────
def _build_job_queue(
    selected: dict,      # {schema: [table, ...]}
    bulk_defaults: dict, # format, delimiter, header, compression, rs_schema, load_mode
    s3_prefix: str,
    source_type: str = "Snowflake",
) -> list:
    ts  = datetime.now().strftime("%Y%m%d_%H%M%S")
    queue = []
    for schema, tables in selected.items():
        for table in tables:
            rs_schema = bulk_defaults.get("rs_schema") or schema
            fmt = bulk_defaults.get("file_format") or bulk_defaults.get("format", "CSV")
            queue.append({
                "id":             f"{schema}.{table}",
                "source_type":    source_type,
                "source_schema":  schema,
                "source_table":   table,
                "db_schema":      schema,
                "db_table":       table,
                "sf_schema":      schema,
                "sf_table":       table,
                "s3_key":         _build_s3_key(s3_prefix, schema, table, fmt, ts),
                "file_format":    fmt,
                "delimiter":      bulk_defaults.get("delimiter", ","),
                "include_header": bulk_defaults.get("include_header", True),
                "compression":    bulk_defaults.get("compression", False),
                "rs_schema":      rs_schema,
                "rs_table":       table,   # same name by default
                "load_mode":      bulk_defaults.get("load_mode", "TRUNCATE & LOAD"),
                "status":         "pending",
                "rows":           0,
                "bytes":          0,
                "duration_secs":  0.0,
                "error":          "",
            })
    return queue


# ── Main render function ──────────────────────────────────────────────────────
def render_table_discovery(
    sf_cfg: dict = None,
    s3_prefix: str = "",
    s3_bucket: str = "",
    rs_schemas: list = None,
    source_type: str = "Snowflake",
    db_cfg: dict = None,
    *args,
    **kwargs,
):
    """
    Renders the full Table Discovery & Selection UI.
    Supports both Snowflake and Databricks sources.

    Parameters
    ----------
    sf_cfg      : dict  — Snowflake connection config
    s3_prefix   : str   — S3 prefix from sidebar
    s3_bucket   : str   — S3 bucket from sidebar
    rs_schemas  : list  — Redshift schemas list
    source_type : str   — "Snowflake" | "Databricks"
    db_cfg      : dict  — Databricks connection config
    """
    if sf_cfg is None:
        sf_cfg = kwargs.get("sf_cfg", {})
    if db_cfg is None:
        db_cfg = kwargs.get("db_cfg", {})
    if rs_schemas is None:
        rs_schemas = kwargs.get("rs_schemas", [])

    _active_src = kwargs.get("source_type", source_type) or st.session_state.get("source_type", "Snowflake")
    if _active_src == "BigQuery":
        from bigquery_ui import render_discovery
        render_discovery(kwargs.get("s3_cfg", {}), kwargs.get("rs_cfg", {}))
        return
    is_db = (_active_src == "Databricks")
    source_name = "Databricks" if is_db else "Snowflake"
    is_connected = st.session_state.get("db_connected", False) if is_db else st.session_state.get("sf_connected", False)

    # ── Step 1: Discover button ───────────────────────────────────────────────
    col_disc, col_disc_status = st.columns([1, 3])

    with col_disc:
        discover_clicked = st.button(
            "🔍 Discover Tables",
            key="btn_discover_tables",
            disabled=not is_connected,
        )

    all_tables_cached = st.session_state.get("_all_tables") or (
        st.session_state.get("_db_tables_map") if is_db else st.session_state.get("_sf_all_tables")
    ) or {}

    with col_disc_status:
        if not is_connected:
            st.markdown(f'<span class="badge badge-pending">⚠ Connect {source_name} first</span>', unsafe_allow_html=True)
        elif all_tables_cached:
            total = sum(len(v) for v in all_tables_cached.values())
            schemas_count = len(all_tables_cached)
            st.markdown(
                f'<span class="badge badge-connected">✓ {total} tables across {schemas_count} schemas</span>',
                unsafe_allow_html=True,
            )
        else:
            st.markdown('<span class="badge badge-pending">Click Discover to load tables</span>', unsafe_allow_html=True)

    # Run discovery
    if discover_clicked:
        with st.spinner(f"Scanning {source_name} schemas and tables…"):
            time.sleep(0.4)
            if is_db:
                result, err = _fetch_all_tables_databricks(db_cfg)
                if err:
                    st.warning(f"Databricks scan notice: {err}")
                    if "logs" not in st.session_state:
                        st.session_state["logs"] = []
                    ts = datetime.now().strftime("%H:%M:%S")
                    st.session_state["logs"].append((ts, "warn", f"Databricks scan: {err}"))
                else:
                    if "logs" not in st.session_state:
                        st.session_state["logs"] = []
                    ts = datetime.now().strftime("%H:%M:%S")
                    total = sum(len(v) for v in result.values())
                    st.session_state["logs"].append((ts, "ok", f"Discovery complete ({source_name}) · {len(result)} schemas · {total} tables"))
                st.session_state["_db_tables_map"] = result
                st.session_state["db_schemas"] = list(result.keys())
            else:
                result = _fetch_all_tables_snowflake(sf_cfg)
                if "logs" not in st.session_state:
                    st.session_state["logs"] = []
                ts = datetime.now().strftime("%H:%M:%S")
                total = sum(len(v) for v in result.values())
                st.session_state["logs"].append((ts, "ok", f"Discovery complete ({source_name}) · {len(result)} schemas · {total} tables"))
            st.session_state["_all_tables"] = result
            st.session_state["_sf_all_tables"] = result  # backward compat
            st.session_state["discovery_done"] = True

    # ── Step 2: Schema-grouped browser ───────────────────────────────────────
    all_tables: dict = st.session_state.get("_all_tables") or (
        st.session_state.get("_db_tables_map") if is_db else st.session_state.get("_sf_all_tables", {})
    ) or {}

    if not all_tables:
        st.info(f"Click **Discover Tables** to browse available {source_name} tables.")
        return

    st.markdown("---")
    st.markdown('<div class="card-title">🗂 Select Tables to Migrate</div>', unsafe_allow_html=True)

    # Track selections in session state
    if "table_selections" not in st.session_state:
        st.session_state.table_selections = {}

    # Global select/deselect all
    col_selall, col_deselall, col_count = st.columns([1, 1, 4])
    with col_selall:
        if st.button("✅ Select All", key="btn_select_all"):
            for schema, tables in all_tables.items():
                st.session_state.table_selections[schema] = list(tables)
    with col_deselall:
        if st.button("⬜ Deselect All", key="btn_deselect_all"):
            st.session_state.table_selections = {}

    total_selected = sum(len(v) for v in st.session_state.table_selections.values())
    with col_count:
        if total_selected > 0:
            st.markdown(
                f'<span class="badge badge-connected">✓ {total_selected} tables selected</span>',
                unsafe_allow_html=True,
            )
        else:
            st.markdown(
                '<span class="badge badge-pending">No tables selected</span>',
                unsafe_allow_html=True,
            )

    st.markdown('<div style="height:10px"></div>', unsafe_allow_html=True)

    # Per-schema expanders
    for schema, tables in all_tables.items():
        if not tables:
            continue

        current_selection = st.session_state.table_selections.get(schema, [])
        selected_count    = len(current_selection)
        total_count       = len(tables)

        label = f"**{schema}** &nbsp; `{selected_count}/{total_count} selected`"

        with st.expander(label, expanded=(selected_count > 0)):
            col_sa, col_sda, _ = st.columns([1, 1, 5])
            with col_sa:
                if st.button(f"All", key=f"selall_{schema}"):
                    st.session_state.table_selections[schema] = list(tables)
                    st.rerun()
            with col_sda:
                if st.button(f"None", key=f"deselall_{schema}"):
                    st.session_state.table_selections[schema] = []
                    st.rerun()

            # Multi-select for this schema
            selected = st.multiselect(
                f"Tables in {schema}",
                options=tables,
                default=st.session_state.table_selections.get(schema, []),
                key=f"multisel_{schema}",
                label_visibility="collapsed",
            )
            st.session_state.table_selections[schema] = selected

    # ── Step 3: Bulk defaults ─────────────────────────────────────────────────
    st.markdown("---")
    st.markdown('<div class="card-title">⚙️ Bulk Defaults — applies to all selected tables</div>', unsafe_allow_html=True)

    col_d1, col_d2, col_d3 = st.columns(3)

    with col_d1:
        bulk_format = st.selectbox(
            "File Format",
            ["CSV", "Parquet", "Iceberg", "JSON", "ORC"],
            key="bulk_file_format",
        )
        if bulk_format == "CSV":
            bulk_delimiter = st.selectbox(
                "Delimiter",
                [",  (comma)", "|  (pipe)", "\\t  (tab)", ";  (semicolon)"],
                key="bulk_delimiter",
            )
            delim_map = {",  (comma)": ",", "|  (pipe)": "|", "\\t  (tab)": "\t", ";  (semicolon)": ";"}
            actual_delim = delim_map[bulk_delimiter]
        else:
            actual_delim = ","

    with col_d2:
        bulk_header      = st.checkbox("Include header row", value=True,  key="bulk_header")
        bulk_compression = st.checkbox("GZIP compression",   value=False, key="bulk_compression")
        bulk_load_mode   = st.selectbox(
            "Redshift Load Mode",
            ["TRUNCATE & LOAD", "APPEND"],
            key="bulk_load_mode",
        )

    with col_d3:
        schemas_list = list(rs_schemas) if rs_schemas else []
        non_public = [s for s in schemas_list if s.lower() != "public"]
        rs_schema_opts = ["public"] + non_public + ["+ Enter custom schema…"]

        bulk_rs_schema_sel = st.selectbox(
            "Target Redshift Schema",
            rs_schema_opts,
            key="bulk_rs_schema_sel",
            help="All tables will land in this Redshift schema. Table names are preserved.",
        )
        if bulk_rs_schema_sel == "+ Enter custom schema…":
            bulk_rs_schema = st.text_input("Custom Target Schema", value="public", key="bulk_rs_custom_schema")
        else:
            bulk_rs_schema = bulk_rs_schema_sel

        st.markdown(
            f'<small style="color:#4a5068">Table names are preserved from {source_name} unless overridden per-table in Chunk 2.</small>',
            unsafe_allow_html=True,
        )

    # ── Step 4: Build Queue button ────────────────────────────────────────────
    st.markdown('<div style="height:8px"></div>', unsafe_allow_html=True)

    col_bq, col_bq_status = st.columns([1, 3])

    with col_bq:
        build_queue = st.button(
            "📋 Build Migration Queue",
            key="btn_build_queue",
            disabled=(total_selected == 0),
        )

    if build_queue:
        selected_tables = {
            schema: tables
            for schema, tables in st.session_state.table_selections.items()
            if tables
        }
        resolved_schema = bulk_rs_schema.strip() if bulk_rs_schema and bulk_rs_schema not in ("— connect Redshift —", "+ Enter custom schema…") else "public"
        bulk_defaults = {
            "file_format":    bulk_format,
            "delimiter":      actual_delim,
            "include_header": bulk_header,
            "compression":    bulk_compression,
            "load_mode":      bulk_load_mode,
            "rs_schema":      resolved_schema,
        }
        queue = _build_job_queue(selected_tables, bulk_defaults, s3_prefix, source_type=source_name)
        st.session_state.job_queue = queue

        ts = datetime.now().strftime("%H:%M:%S")
        st.session_state.logs.append((ts, "ok", f"Job queue built · {len(queue)} tables queued"))

    with col_bq_status:
        queue = st.session_state.get("job_queue", [])
        if queue:
            st.markdown(
                f'<span class="badge badge-connected">✓ Queue ready · {len(queue)} jobs</span>',
                unsafe_allow_html=True,
            )

    # ── Step 5: Queue preview table ───────────────────────────────────────────
    queue = st.session_state.get("job_queue", [])
    if queue:
        st.markdown("---")
        st.markdown('<div class="card-title">📋 Migration Queue Preview</div>', unsafe_allow_html=True)

        import pandas as pd
        src_label = f"Source ({'DB' if is_db else 'SF'})"
        preview_df = pd.DataFrame([
            {
                src_label:       f"{j.get('source_schema', j['sf_schema'])}.{j.get('source_table', j['sf_table'])}",
                "Format":        j["file_format"],
                "S3 Key":        j["s3_key"],
                "Target (RS)":   f"{j['rs_schema']}.{j['rs_table']}",
                "Load Mode":     j["load_mode"],
                "Status":        j["status"].upper(),
            }
            for j in queue
        ])
        st.dataframe(
            preview_df,
            use_container_width=True,
            hide_index=True,
            height=min(400, 40 + len(queue) * 36),
        )


# ── Summary helper (used by Chunk 3 / metrics tab) ───────────────────────────
def get_job_queue_summary() -> dict:
    """
    Returns a summary dict of the current job queue state.
    Call this from the metrics tab to get aggregate counts.
    """
    queue = st.session_state.get("job_queue", [])
    if not queue:
        return {"total": 0, "pending": 0, "running": 0, "done": 0, "failed": 0}

    from collections import Counter
    counts = Counter(j["status"] for j in queue)
    return {
        "total":   len(queue),
        "pending": counts.get("pending", 0),
        "running": counts.get("running", 0),
        "done":    counts.get("done", 0),
        "failed":  counts.get("failed", 0),
    }
