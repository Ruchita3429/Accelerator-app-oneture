"""
Snowflake → S3 → Redshift Data Pipeline
A CXO-demo-ready Streamlit app for orchestrating bulk data movement.

Integrates:
  chunk_01_table_discovery   — schema-grouped multi-table browser + job queue builder
  chunk_02_job_queue_engine  — sequential executor with resume / checkpoint support
  chunk_03_monitoring_ui     — live per-table status grid + schema rollup
  chunk_04_metrics_report    — aggregate metrics, charts, CSV export, COPY statements
"""

import streamlit as st
import pandas as pd
import time
import io
import json
from datetime import datetime

# ── Chunk module imports ──────────────────────────────────────────────────────
from chunk_01_table_discovery import render_table_discovery, get_job_queue_summary
import chunk_02_job_queue_engine as _chunk02
from chunk_02_job_queue_engine import render_execution_controls, run_migration
from chunk_03_monitoring_ui   import render_monitoring_tab
from chunk_04_metrics_report  import render_metrics_tab
from tab_data_validation import render_validation_tab  # new cross-cloud validator
import sf_to_rs_combined_ddl_distribution as ddl_migrator  # DDL conversion + distkey/sortkey analysis

# ── S3-only gate: patch chunk_02's run_migration so COPY is skipped when
#    pipeline_destination == "SF → S3 only".  chunk_02 reads st.session_state
#    so no signature change is needed — we just store the flag there.
#    render_execution_controls internally calls run_migration (or the same COPY
#    helpers), so storing the flag before render is sufficient.  As a belt-and-
#    suspenders measure, we also wrap run_migration itself. ──────────────────────
_orig_run_migration = run_migration

def _run_migration_gated(*args, **kwargs):
    """Wrap run_migration to skip the Redshift COPY leg in SF→S3 only mode."""
    import streamlit as _st
    if _st.session_state.get("pipeline_destination") == "SF → S3 only":
        # Patch the COPY helper inside chunk_02 to be a no-op for this run
        _orig_copy = getattr(_chunk02, "_do_redshift_copy", None) or getattr(_chunk02, "do_redshift_copy", None)
        def _noop_copy(*a, **kw):
            return None  # silently skip COPY
        # Temporarily replace the COPY function inside chunk_02's namespace
        for _attr in dir(_chunk02):
            if "copy" in _attr.lower() and callable(getattr(_chunk02, _attr, None)):
                setattr(_chunk02, _attr, _noop_copy)
        try:
            return _orig_run_migration(*args, **kwargs)
        finally:
            # Restore original copy function
            if _orig_copy:
                for _attr in dir(_chunk02):
                    if "copy" in _attr.lower() and callable(getattr(_chunk02, _attr, None)):
                        setattr(_chunk02, _attr, _orig_copy)
    else:
        return _orig_run_migration(*args, **kwargs)

run_migration = _run_migration_gated
_chunk02.run_migration = _run_migration_gated  # patch module-level ref too
try:
    from st_to_rs_bi_query_migration import StoredProcedureMigrator, BIQueryMigrator
except ImportError:
    StoredProcedureMigrator = None
    BIQueryMigrator = None

try:
    from snowflake.snowpark import Session as SnowparkSession
except ImportError:
    SnowparkSession = None

# ── Page config ──────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="DataBridge · Pipeline Studio",
    page_icon="🔀",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ── Custom CSS ────────────────────────────────────────────────────────────────
st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=DM+Sans:wght@300;400;500;600&family=DM+Mono:wght@400;500&display=swap');

/* ── Reset & Base ── */
html, body, [class*="css"] {
    font-family: 'DM Sans', sans-serif;
}

/* ── Hide Streamlit default header/toolbar (white strip) ── */
header[data-testid="stHeader"] {
    display: none !important;
    height: 0 !important;
}
#MainMenu, footer, header { visibility: hidden; height: 0; }
.stAppDeployButton { display: none !important; }
[data-testid="stToolbar"] { display: none !important; }
.stApp > header { display: none !important; }

/* ── Query migrator — match output code block height to input textarea ── */
.qm-output-code pre {
    height: 340px !important;
    overflow-y: auto !important;
    font-family: 'DM Mono', monospace !important;
    font-size: 13px !important;
}

/* ── Background ── */
.stApp {
    background: #161b27;
    color: #e8eaf0;
}

/* ── Sidebar ── */
section[data-testid="stSidebar"] {
    background: #1c2233;
    border-right: 1px solid #2a3148;
}
section[data-testid="stSidebar"] * {
    color: #d0d4e0 !important;
}

/* ── Header brand ── */
.brand-header {
    display: flex;
    align-items: center;
    gap: 12px;
    padding: 8px 0 24px 0;
    border-bottom: 1px solid #2a3148;
    margin-bottom: 24px;
}
.brand-logo {
    width: 36px; height: 36px;
    background: linear-gradient(135deg, #5b8dee 0%, #a78bfa 100%);
    border-radius: 10px;
    display: flex; align-items: center; justify-content: center;
    font-size: 18px;
}
.brand-name {
    font-size: 20px;
    font-weight: 600;
    color: #f0f2ff !important;
    letter-spacing: -0.3px;
}
.brand-sub {
    font-size: 11px;
    color: #6b7080 !important;
    letter-spacing: 0.5px;
    text-transform: uppercase;
}

/* ── Section headers in sidebar ── */
.sidebar-section {
    font-size: 10px;
    font-weight: 600;
    letter-spacing: 1.2px;
    text-transform: uppercase;
    color: #4a5068 !important;
    margin: 20px 0 10px 0;
}

/* ── Cards ── */
.card {
    background: #1c2233;
    border: 1px solid #2a3148;
    border-radius: 14px;
    padding: 24px;
    margin-bottom: 16px;
}
.card-title {
    font-size: 13px;
    font-weight: 600;
    letter-spacing: 0.8px;
    text-transform: uppercase;
    color: #6b7080;
    margin-bottom: 18px;
    display: flex;
    align-items: center;
    gap: 8px;
}

/* ── Status badges ── */
.badge {
    display: inline-flex;
    align-items: center;
    gap: 6px;
    padding: 4px 12px;
    border-radius: 20px;
    font-size: 12px;
    font-weight: 500;
}
.badge-connected   { background: #0f2e1e; color: #34d399; border: 1px solid #1a5c3a; }
.badge-disconnected{ background: #2e1010; color: #f87171; border: 1px solid #7f1d1d; }
.badge-pending     { background: #2a2010; color: #fbbf24; border: 1px solid #78350f; }

/* ── Metric cards ── */
.metric-row { display: flex; gap: 12px; margin-bottom: 16px; }
.metric-card {
    flex: 1;
    background: #1c2233;
    border: 1px solid #2a3148;
    border-radius: 10px;
    padding: 16px;
    text-align: center;
}
.metric-value {
    font-family: 'DM Mono', monospace;
    font-size: 26px;
    font-weight: 500;
    color: #5b8dee;
    line-height: 1.2;
}
.metric-label {
    font-size: 11px;
    color: #4a5068;
    text-transform: uppercase;
    letter-spacing: 0.8px;
    margin-top: 4px;
}

/* ── Progress bar ── */
.stProgress > div > div { background: linear-gradient(90deg, #5b8dee, #a78bfa); border-radius: 4px; }

/* ── Buttons ── */
.stButton > button {
    background: linear-gradient(135deg, #5b8dee 0%, #7c6ee0 100%) !important;
    color: #fff !important;
    border: none !important;
    border-radius: 10px !important;
    padding: 10px 28px !important;
    font-family: 'DM Sans', sans-serif !important;
    font-weight: 500 !important;
    font-size: 14px !important;
    letter-spacing: 0.2px !important;
    transition: opacity 0.2s !important;
    width: 100%;
}
.stButton > button:hover { opacity: 0.85 !important; }

/* ── Inputs ── */
.stTextInput input, .stSelectbox > div > div, .stNumberInput input {
    background: #131825 !important;
    border: 1px solid #2a3148 !important;
    border-radius: 8px !important;
    color: #e8eaf0 !important;
    font-family: 'DM Mono', monospace !important;
    font-size: 13px !important;
}

/* ── Log box ── */
.log-box {
    background: #131825;
    border: 1px solid #2a3148;
    border-radius: 10px;
    padding: 16px;
    font-family: 'DM Mono', monospace;
    font-size: 12px;
    color: #6b7080;
    height: 200px;
    overflow-y: auto;
    line-height: 1.8;
}
.log-ok   { color: #34d399; }
.log-info { color: #5b8dee; }
.log-warn { color: #fbbf24; }
.log-err  { color: #f87171; }

/* ── Pipeline flow diagram ── */
.pipeline-flow {
    display: flex;
    align-items: center;
    justify-content: center;
    gap: 0;
    padding: 20px 0;
    margin-bottom: 24px;
}
.flow-node {
    text-align: center;
    padding: 14px 22px;
    border-radius: 12px;
    min-width: 110px;
}
.flow-node-sf  { background: #162040; border: 1px solid #2a4080; }
.flow-node-s3  { background: #1e1600; border: 1px solid #6b4800; }
.flow-node-rs  { background: #102418; border: 1px solid #1a4a30; }
.flow-node-icon { font-size: 24px; }
.flow-node-name { font-size: 11px; font-weight: 600; letter-spacing: 0.5px; margin-top: 4px; color: #c8cad4; }
.flow-arrow {
    color: #3a3f55;
    font-size: 20px;
    padding: 0 8px;
}
.flow-arrow-active { color: #5b8dee; }

/* ── Tabs ── */
.stTabs [data-baseweb="tab-list"] {
    background: #1c2233;
    border-radius: 10px;
    padding: 5px 6px;
    gap: 2px;
    border: 1px solid #2a3148;
}
.stTabs [data-baseweb="tab"] {
    color: #8891a8 !important;
    border-radius: 7px !important;
    font-size: 13px !important;
    font-weight: 500 !important;
    padding: 7px 14px !important;
    white-space: nowrap !important;
}
.stTabs [aria-selected="true"] {
    background: #243050 !important;
    color: #7aa8f5 !important;
    box-shadow: 0 1px 3px rgba(0,0,0,0.3) !important;
}
.stTabs [data-baseweb="tab"]:hover:not([aria-selected="true"]) {
    background: #212840 !important;
    color: #b0b8cc !important;
}

/* ── Divider ── */
hr { border-color: #2a3148 !important; }

/* ── Expander ── */
.streamlit-expanderHeader {
    background: #1c2233 !important;
    border-radius: 8px !important;
    color: #d0d4e0 !important;
}
</style>
""", unsafe_allow_html=True)


# ── Session State Init ────────────────────────────────────────────────────────
def init_state():
    defaults = {
        "sf_connected": False,
        "rs_connected": False,
        "sf_schemas": [],
        "sf_tables": [],
        "rs_schemas": [],
        "rs_tables": [],
        "logs": [],
        "last_run": None,
        "pipeline_stage": 0,   # 0=idle, 1=sf→s3, 2=s3→rs, 3=done
        "pipeline_destination": "SF → S3 → Redshift",  # or "SF → S3 only"
        # Chunk 1
        "job_queue": [],
        # Partition / filter feature
        "migration_metadata_table": "MIGRATION_METADATA",
        "global_use_partitions": False,
        "global_use_filters": False,
        "migration_metadata_cache": {},   # {table_name: {filter_col, partition_cols}}
        "table_export_flags": {},         # {table_name: {use_partitions, use_filters}}
        "discovery_done": False,
        "_sf_all_tables": {},
        "table_selections": {},
        # Chunk 2
        "migration_running": False,
        "migration_done": False,
        "checkpoint": {},
        "migration_summary": {},
        # Validation tab
        # val2_ keys are managed by tab_data_validation.py _init()
        "val2_stage": "idle",
        # DDL Migrator tab — single paste mode
        "ddl_input": "",
        "ddl_output": "",
        "ddl_running": False,
        "ddl_logs": [],
        "ddl_distkey": None,
        "ddl_sortkey": None,
        # DDL Migrator tab — bulk Snowflake browser mode
        "ddl_bulk_schema": "",
        "ddl_bulk_tables": [],
        "ddl_bulk_selected": [],
        "ddl_bulk_results": [],
        "ddl_bulk_logs": [],
        "ddl_bulk_running": False,
        # Query Migrator tab
        "qm_query_input": "",
        "qm_query_output": "",
        "qm_query_changes": [],
        "qm_proc_input": "",
        "qm_proc_output": "",
        "qm_running": False,
        # Syntax validation & diff results
        "ddl_validate_result": None,   # None | dict{ok, errors, warnings}
        "ddl_diff_html": "",           # highlighted HTML diff for DDL paste tab
        "qm_query_validate_result": None,
        "qm_query_diff_html": "",
        "qm_proc_validate_result": None,
        "qm_proc_diff_html": "",
        # Health Check tab
        "hc_session": None,
        "hc_connected": False,
        "hc_error": None,
        # S3 → Redshift tab
        # S3→Redshift browser state
        "t2_bucket_val": "",
        "t2_browser_prefix": "",
        "t2_browser_folders": [],
        "t2_browser_files": [],
        "t2_selected_files": [],
    }
    for k, v in defaults.items():
        if k not in st.session_state:
            st.session_state[k] = v

init_state()


# ── Helpers ───────────────────────────────────────────────────────────────────
def add_log(msg: str, level: str = "info"):
    ts = datetime.now().strftime("%H:%M:%S")
    st.session_state.logs.append((ts, level, msg))

def render_logs():
    html = '<div class="log-box">'
    for ts, level, msg in st.session_state.logs[-80:]:
        cls = {"ok": "log-ok", "info": "log-info", "warn": "log-warn", "err": "log-err"}.get(level, "log-info")
        html += f'<div><span style="color:#2a2d3e">{ts}</span> &nbsp;<span class="{cls}">{msg}</span></div>'
    html += "</div>"
    st.markdown(html, unsafe_allow_html=True)

def format_bytes(b: int) -> str:
    if b < 1024: return f"{b} B"
    if b < 1024**2: return f"{b/1024:.1f} KB"
    if b < 1024**3: return f"{b/1024**2:.2f} MB"
    return f"{b/1024**3:.3f} GB"

def fake_schemas_tables(source: str):
    """Returns mock schema/table lists — replace with real connector calls."""
    if source == "snowflake":
        schemas = ["PUBLIC", "ANALYTICS", "RAW", "STAGING", "MARTS"]
        tables  = {
            "PUBLIC":    ["CUSTOMERS", "ORDERS", "PRODUCTS", "INVOICES"],
            "ANALYTICS": ["DAILY_REVENUE", "USER_RETENTION", "FUNNEL_METRICS"],
            "RAW":       ["EVENTS_RAW", "CLICKSTREAM", "LOGS_INGESTION"],
            "STAGING":   ["STG_CUSTOMERS", "STG_ORDERS"],
            "MARTS":     ["DIM_DATE", "FACT_SALES", "DIM_CUSTOMER"],
        }
    else:
        schemas = ["PUBLIC", "DWH", "REPORTING", "STAGING"]
        tables  = {
            "PUBLIC":    ["LANDING_CUSTOMERS", "LANDING_ORDERS"],
            "DWH":       ["DIM_CUSTOMER", "FACT_ORDERS", "DIM_PRODUCT"],
            "REPORTING": ["RPT_SALES_DAILY", "RPT_CHURN"],
            "STAGING":   ["STG_INBOUND"],
        }
    return schemas, tables


# ── Try real connectors (graceful fallback) ───────────────────────────────────
def get_snowflake_schemas(cfg):
    try:
        import snowflake.connector
        conn = snowflake.connector.connect(**cfg)
        cur  = conn.cursor()
        cur.execute(f"SELECT SCHEMA_NAME FROM {cfg['database']}.INFORMATION_SCHEMA.SCHEMATA ORDER BY SCHEMA_NAME")
        schemas = [r[0] for r in cur.fetchall() if r[0] not in ("INFORMATION_SCHEMA",)]
        # Also pre-fetch all tables grouped by schema
        cur.execute(f"SELECT TABLE_SCHEMA, TABLE_NAME FROM {cfg['database']}.INFORMATION_SCHEMA.TABLES ORDER BY TABLE_SCHEMA, TABLE_NAME")
        tables_map = {}
        for schema, table in cur.fetchall():
            tables_map.setdefault(schema, []).append(table)
        conn.close()
        return schemas, tables_map
    except Exception as e:
        return None, None

def get_snowflake_tables(cfg, schema):
    try:
        import snowflake.connector
        conn = snowflake.connector.connect(**cfg)
        cur  = conn.cursor()
        cur.execute(f"SHOW TABLES IN SCHEMA {cfg['database']}.{schema}")
        tables = [r[1] for r in cur.fetchall()]
        conn.close()
        return tables
    except Exception:
        return None

def _rs_data_api_client(cfg):
    import boto3
    return boto3.client(
        "redshift-data",
        region_name=cfg.get("region", "ap-south-1"),
        aws_access_key_id=cfg.get("access_key"),
        aws_secret_access_key=cfg.get("secret_key"),
    )

def _rs_run_sql(cfg, sql, timeout=60):
    """
    Execute SQL via Redshift Data API and return rows.
    For DML/DDL (COPY, TRUNCATE, INSERT, CREATE, etc.) there is no result set —
    the API marks them FINISHED but get_statement_result raises ResourceNotFoundException.
    We check HasResultSet from describe_statement before fetching results.
    """
    import time
    client = _rs_data_api_client(cfg)
    resp = client.execute_statement(
        WorkgroupName=cfg["workgroup"],
        Database=cfg["database"],
        Sql=sql,
    )
    stmt_id = resp["Id"]
    deadline = time.time() + timeout
    desc = {}
    while time.time() < deadline:
        desc = client.describe_statement(Id=stmt_id)
        status = desc["Status"]
        if status == "FINISHED":
            break
        if status in ("FAILED", "ABORTED"):
            raise RuntimeError(f"Redshift Data API error: {desc.get('Error', 'Unknown')}")
        time.sleep(1)
    else:
        raise TimeoutError(f"Redshift Data API timed out after {timeout}s")
    # COPY / TRUNCATE / DDL: FINISHED but no result set — return empty list, not an error
    if not desc.get("HasResultSet", False):
        return []
    result = client.get_statement_result(Id=stmt_id)
    rows = []
    for record in result.get("Records", []):
        rows.append([list(col.values())[0] for col in record])
    return rows

def get_redshift_schemas(cfg):
    try:
        rows = _rs_run_sql(cfg,
            "SELECT schema_name FROM information_schema.schemata WHERE schema_owner != 'rdsdb'"
        )
        return [r[0] for r in rows] or None
    except Exception:
        return None

def get_redshift_tables(cfg, schema, _raise=False):
    """
    Try multiple catalog queries in order — Redshift Serverless restricts some views.
    SVV_ALL_TABLES is the most reliable for serverless user-created tables.
    """
    schema_lc = schema.lower()
    queries = [
        # Best for Redshift Serverless — sees all user-created tables
        (f"SELECT table_name FROM svv_all_tables "
         f"WHERE schema_name = '{schema_lc}' "
         f"AND table_type = 'TABLE' ORDER BY table_name"),
        # Standard ANSI — works when user has USAGE on schema
        (f"SELECT table_name FROM information_schema.tables "
         f"WHERE table_schema = '{schema_lc}' "
         f"AND table_type = 'BASE TABLE' ORDER BY table_name"),
        # pg catalog — works even without information_schema grants
        (f"SELECT tablename FROM pg_tables "
         f"WHERE schemaname = '{schema_lc}' ORDER BY tablename"),
    ]
    errors = []
    for q in queries:
        try:
            rows = _rs_run_sql(cfg, q)
            if rows is not None:  # empty list is valid (schema exists but no tables)
                return [r[0] for r in rows]
        except Exception as e:
            errors.append(f"{q[:40]}…: {e}")
            continue
    # All queries failed — surface the real errors
    combined = " | ".join(errors)
    if _raise:
        raise RuntimeError(f"All table-list queries failed: {combined}")
    return None


# ── Sidebar ───────────────────────────────────────────────────────────────────
with st.sidebar:
    st.markdown("""
    <div class="brand-header">
        <div class="brand-logo">🔀</div>
        <div>
            <div class="brand-name">DataBridge</div>
            <div class="brand-sub">Pipeline Studio</div>
        </div>
    </div>
    """, unsafe_allow_html=True)

    # ── SNOWFLAKE ──
    st.markdown('<div class="sidebar-section">❄️ &nbsp;Snowflake</div>', unsafe_allow_html=True)
    sf_account  = st.text_input("Account",   placeholder="xyz.us-east-1", key="sf_account")
    sf_user     = st.text_input("User",       placeholder="data_user",     key="sf_user")
    sf_password = st.text_input("Password",   type="password",             key="sf_password")
    sf_database = st.text_input("Database",   placeholder="PROD_DB",       key="sf_database")
    sf_warehouse= st.text_input("Warehouse",  placeholder="COMPUTE_WH",    key="sf_warehouse")
    sf_role     = st.text_input("Role",       placeholder="SYSADMIN",      key="sf_role", value="")

    if st.button("Connect Snowflake", key="btn_sf_connect"):
        with st.spinner("Connecting…"):
            time.sleep(0.8)
            cfg = {
                "account":   sf_account,
                "user":      sf_user,
                "password":  sf_password,
                "database":  sf_database,
                "warehouse": sf_warehouse,
            }
            if sf_role: cfg["role"] = sf_role
            schemas, tables_map = get_snowflake_schemas(cfg)
            if schemas:
                st.session_state.sf_schemas = schemas
                if tables_map:
                    st.session_state._sf_tables_map = tables_map
                st.session_state.sf_connected = True
                add_log(f"Snowflake connected · {len(schemas)} schemas found", "ok")
            else:
                # Demo mode
                s, t = fake_schemas_tables("snowflake")
                st.session_state.sf_schemas = s
                st.session_state._sf_tables_map = t
                st.session_state.sf_connected = True
                add_log("Snowflake connected (demo mode)", "ok")

    sf_status = ("badge-connected", "● Connected") if st.session_state.sf_connected else ("badge-disconnected", "○ Not connected")
    st.markdown(f'<span class="badge {sf_status[0]}">{sf_status[1]}</span>', unsafe_allow_html=True)

    st.markdown("---")

    # ── AWS S3 ──
    st.markdown('<div class="sidebar-section">☁️ &nbsp;AWS S3</div>', unsafe_allow_html=True)
    s3_bucket      = st.text_input("Bucket",          placeholder="my-data-lake",    key="s3_bucket")
    s3_prefix      = st.text_input("Prefix / Path",   placeholder="pipeline/exports", key="s3_prefix")
    s3_access_key  = st.text_input("Access Key ID",   placeholder="AKIA…",           key="s3_access_key")
    s3_secret_key  = st.text_input("Secret Access Key", type="password",             key="s3_secret_key")
    s3_region      = st.text_input("Region",          placeholder="us-east-1",       key="s3_region", value="us-east-1")

    st.markdown("---")

    # ── REDSHIFT ──
    st.markdown('<div class="sidebar-section">🔴 &nbsp;Redshift</div>', unsafe_allow_html=True)
    rs_workgroup= st.text_input("Workgroup name only e.g. defaultworkgroup", placeholder="defaultworkgroup", key="rs_workgroup", help="Just the short name — not the full endpoint URL")
    rs_db       = st.text_input("Database", placeholder="dev",        key="rs_db")
    rs_region   = st.text_input("Region",   placeholder="ap-south-1", key="rs_region")
    rs_host     = ""  # kept for backward compat, not used with Data API
    rs_port     = 5439
    rs_user     = ""
    rs_password = ""

    if st.button("Connect Redshift", key="btn_rs_connect"):
        with st.spinner("Connecting…"):
            cfg = {"workgroup": rs_workgroup, "database": rs_db, "region": rs_region, "access_key": s3_access_key, "secret_key": s3_secret_key}
            try:
                # Strip full endpoint URL if pasted — extract just workgroup name
                cfg["workgroup"] = cfg["workgroup"].split(".")[0].strip()
                # Lightweight ping — just checks credentials and workgroup reachability
                ping_rows = _rs_run_sql(cfg, "SELECT 1 AS ok", timeout=20)
                st.session_state.rs_connected = True
                # Now fetch schemas (best-effort — fall back to demo if denied)
                schemas = get_redshift_schemas(cfg)
                if schemas:
                    st.session_state.rs_schemas = schemas
                    add_log(f"Redshift connected · {len(schemas)} schemas found", "ok")
                else:
                    s, t = fake_schemas_tables("redshift")
                    st.session_state.rs_schemas = s
                    st.session_state._rs_tables_map = t
                    add_log("Redshift connected · schema fetch fell back to demo mode", "ok")
            except Exception as _e:
                st.session_state.rs_connected = False
                add_log(f"Redshift connect failed: {_e}", "err")
                st.error(f"Could not connect: {_e}")
        st.rerun()

    rs_status = ("badge-connected", "● Connected") if st.session_state.rs_connected else ("badge-disconnected", "○ Not connected")
    st.markdown(f'<span class="badge {rs_status[0]}">{rs_status[1]}</span>', unsafe_allow_html=True)


# ── Connection config dicts (passed to chunk functions) ──────────────────────
_sf_cfg = {
    "account":   sf_account,
    "user":      sf_user,
    "password":  sf_password,
    "database":  sf_database,
    "warehouse": sf_warehouse,
}
if sf_role:
    _sf_cfg["role"] = sf_role

_s3_cfg = {
    "bucket":     s3_bucket,
    "prefix":     s3_prefix,
    "access_key": s3_access_key,
    "secret_key": s3_secret_key,
    "region":     s3_region,
}

_rs_cfg = {
    "workgroup":  rs_workgroup,
    "database":   rs_db,
    "region":     rs_region,
    "access_key": s3_access_key,
    "secret_key": s3_secret_key,
}


# ── Main area ─────────────────────────────────────────────────────────────────
st.markdown("## Pipeline Configuration")

# ── Destination selector ──────────────────────────────────────────────────────
_dest_opts = ["SF → S3 → Redshift", "SF → S3 only"]
_dest_col, _dest_info_col = st.columns([2, 5])
with _dest_col:
    _selected_dest = st.selectbox(
        "🎯 Pipeline Destination",
        options=_dest_opts,
        index=_dest_opts.index(st.session_state.get("pipeline_destination", "SF → S3 → Redshift")),
        key="pipeline_destination_sel",
        help="Choose whether data lands in S3 only, or continues into Redshift via COPY.",
    )
    if _selected_dest != st.session_state.get("pipeline_destination"):
        st.session_state["pipeline_destination"] = _selected_dest
        st.rerun()

_dest_mode = st.session_state.get("pipeline_destination", "SF → S3 → Redshift")
_s3_only   = (_dest_mode == "SF → S3 only")

with _dest_info_col:
    if _s3_only:
        st.markdown(
            '<span class="badge badge-pending" style="margin-top:28px;display:inline-flex">'
            '◌ S3-only mode — tables will be exported to S3 and the Redshift COPY step will be skipped'
            '</span>',
            unsafe_allow_html=True,
        )
    else:
        st.markdown(
            '<span class="badge badge-connected" style="margin-top:28px;display:inline-flex">'
            '● Full pipeline — Snowflake → S3 → Redshift'
            '</span>',
            unsafe_allow_html=True,
        )

# Flow diagram — dim the S3→Redshift leg when S3-only
arrow1_cls = "flow-arrow-active" if st.session_state.pipeline_stage >= 1 else "flow-arrow"
arrow2_cls = "flow-arrow-active" if (st.session_state.pipeline_stage >= 2 and not _s3_only) else "flow-arrow"
rs_node_style = ' style="opacity:0.35;filter:grayscale(1)"' if _s3_only else ""

st.markdown(f"""
<div class="pipeline-flow">
  <div class="flow-node flow-node-sf">
    <div class="flow-node-icon">❄️</div>
    <div class="flow-node-name">SNOWFLAKE</div>
  </div>
  <div class="flow-arrow {arrow1_cls}">──▶</div>
  <div class="flow-node flow-node-s3">
    <div class="flow-node-icon">☁️</div>
    <div class="flow-node-name">Amazon S3</div>
  </div>
  <div class="flow-arrow {arrow2_cls}">{"──▶" if not _s3_only else "── ✕"}</div>
  <div class="flow-node flow-node-rs"{rs_node_style}>
    <div class="flow-node-icon">🔴</div>
    <div class="flow-node-name">REDSHIFT</div>
  </div>
</div>
""", unsafe_allow_html=True)

# ── Tab order: Table Discovery · Snowflake→S3 · S3→Redshift · Monitor · Metrics & Logs ──
tab_discovery, tab1, tab2, tab_monitor, tab3, tab_validate, tab_ddl, tab_qm, tab_hc = st.tabs([
    "🗂 Table Discovery",
    "📤 Snowflake → S3",
    "📥 S3 → Redshift",
    "📡 Monitor",
    "📊 Metrics & Logs",
    "🔍 Data Validation",
    "🏗 DDL Migrator",
    "🔄 Query Migrator",
    "❄️ SF Health Check",
])


# ─────────────────────────────────────────────────────────────────────────────
# TAB 0 · Table Discovery  (Chunk 1)
# ─────────────────────────────────────────────────────────────────────────────
with tab_discovery:
    # ── Destination mode reminder ─────────────────────────────────────────────
    if _s3_only:
        st.markdown(
            '<div style="background:#2a2010;border:1px solid #78350f;border-radius:10px;'
            'padding:10px 16px;color:#fbbf24;font-size:13px;margin-bottom:12px">'
            '◌ <b>S3-only mode</b> — the queue will export to S3 and stop there. '
            'The Redshift COPY step will be skipped. '
            'Change the <b>Pipeline Destination</b> dropdown above to enable full pipeline.</div>',
            unsafe_allow_html=True,
        )
    else:
        st.markdown(
            '<div style="background:#0f2e1e;border:1px solid #1a5c3a;border-radius:10px;'
            'padding:10px 16px;color:#34d399;font-size:13px;margin-bottom:12px">'
            '● <b>Full pipeline mode</b> — queue will run Snowflake → S3 → Redshift COPY.</div>',
            unsafe_allow_html=True,
        )
    # ── Partition / Filter configuration ─────────────────────────────────────
    st.markdown("---")
    st.markdown('<div class="card-title">⚙️ Export Options — Partitioning & Filtering</div>',
                unsafe_allow_html=True)

    pf_col1, pf_col2, pf_col3 = st.columns([2, 1, 1])
    with pf_col1:
        _meta_tbl = st.text_input(
            "Metadata Table",
            value=st.session_state.get("migration_metadata_table", "MIGRATION_METADATA"),
            key="meta_table_input",
            help="Snowflake table with columns: TABLE_NAME, FILTER_COLUMN_NAME, PARTITION_COLUMNS",
            placeholder="e.g. ADMIN.MIGRATION_METADATA",
        )
        st.session_state["migration_metadata_table"] = _meta_tbl

    with pf_col2:
        _g_partitions = st.checkbox(
            "🗂 Enable Partitions",
            value=st.session_state.get("global_use_partitions", False),
            key="global_partitions_chk",
            help="Use PARTITION_COLUMNS from metadata to build hierarchical S3 paths",
        )
        st.session_state["global_use_partitions"] = _g_partitions

    with pf_col3:
        _g_filters = st.checkbox(
            "🔍 Apply Filters",
            value=st.session_state.get("global_use_filters", False),
            key="global_filters_chk",
            help="Add WHERE clause to export using FILTER_COLUMN_NAME from metadata",
        )
        st.session_state["global_use_filters"] = _g_filters

    if _g_partitions or _g_filters:
        st.markdown(
            '<div style="background:#13161f;border:1px solid #2a3148;border-radius:8px;'
            'padding:8px 14px;color:#6b7080;font-size:12px;margin-bottom:8px">'
            'ℹ Tables not found in the metadata table will be exported without partitioning or '
            'filtering — no errors will be raised for missing rows.</div>',
            unsafe_allow_html=True,
        )

        # Load / refresh metadata from Snowflake
        _meta_load_col, _ = st.columns([2, 5])
        with _meta_load_col:
            if st.button("↻ Load Metadata", key="btn_load_metadata", use_container_width=True):
                if not st.session_state.sf_connected:
                    st.error("Connect to Snowflake first.")
                else:
                    try:
                        import snowflake.connector as _sfc
                        _conn = _sfc.connect(**_sf_cfg)
                        _cur  = _conn.cursor()
                        # Use the table name as typed — support unqualified, schema.table,
                        # or db.schema.table. Wrap each part in double-quotes to preserve case.
                        _meta_tbl_quoted = ".".join(
                            f'"{p.strip()}"' for p in _meta_tbl.split(".")
                        )
                        # Column order matches the INSERT:
                        # TABLE_NAME, PARTITION_COLUMNS, FILTER_COLUMN_NAME
                        _cur.execute(
                            f"SELECT TABLE_NAME, PARTITION_COLUMNS, FILTER_COLUMN_NAME "
                            f"FROM {_meta_tbl_quoted}"
                        )
                        _rows = _cur.fetchall()
                        _cur.close(); _conn.close()
                        _cache = {}
                        for _row in _rows:
                            _tname = str(_row[0]).upper().strip()
                            _cache[_tname] = {
                                "partition_cols": _row[1] or "",
                                "filter_col":     _row[2] or "",
                            }
                        st.session_state["migration_metadata_cache"] = _cache
                        st.success(f"Loaded metadata for {len(_cache)} tables.")
                        st.rerun()
                    except Exception as _e:
                        _err_str = str(_e)
                        if "does not exist or not authorized" in _err_str:
                            st.error(
                                f"Table not found: **{_meta_tbl}**\n\n"
                                f"Use a fully qualified name — e.g. `MY_DB.PUBLIC.MIGRATION_METADATA`. "
                                f"The Snowflake role connected must have SELECT privilege on this table.\n\n"
                                f"Raw error: {_err_str}"
                            )
                        else:
                            st.error(f"Failed to load metadata: {_err_str}")

        # Show loaded metadata
        _cache = st.session_state.get("migration_metadata_cache", {})
        if _cache:
            _meta_rows = [
                {"Table": k, "Partition Columns": v["partition_cols"], "Filter Column": v["filter_col"]}
                for k, v in _cache.items()
            ]
            st.dataframe(_meta_rows, use_container_width=True, height=180)

    st.markdown("---")

    render_table_discovery(
        sf_cfg=_sf_cfg,
        s3_prefix=s3_prefix,
        s3_bucket=s3_bucket,
        rs_schemas=st.session_state.rs_schemas,
    )

    # ── After queue is built, stamp partition/filter flags onto each job ───────
    # chunk_01 builds the queue; we post-process here to inject the flags
    # so chunk_02 can read them without needing chunk_01 changes.
    _queue = st.session_state.get("job_queue", [])
    _meta_cache = st.session_state.get("migration_metadata_cache", {})
    _g_part = st.session_state.get("global_use_partitions", False)
    _g_filt = st.session_state.get("global_use_filters", False)

    for _job in _queue:
        _tname = str(_job.get("sf_table", "")).upper().strip()
        _meta  = _meta_cache.get(_tname, {})
        # Only apply flags if metadata row exists; silently skip if missing
        _has_meta = bool(_meta)
        _job["use_partitions"]   = _g_part and _has_meta and bool(_meta.get("partition_cols", "").strip())
        _job["use_filters"]      = _g_filt and _has_meta and bool(_meta.get("filter_col", "").strip())
        _job["partition_cols"]   = _meta.get("partition_cols", "") if _job["use_partitions"] else ""
        _job["filter_col"]       = _meta.get("filter_col", "")    if _job["use_filters"]     else ""


# ─────────────────────────────────────────────────────────────────────────────
# TAB 1 · Snowflake → S3
# ─────────────────────────────────────────────────────────────────────────────
with tab1:
    # ── Destination mode banner ───────────────────────────────────────────────
    if _s3_only:
        st.markdown(
            '<div style="background:#2a2010;border:1px solid #78350f;border-radius:10px;'
            'padding:10px 16px;color:#fbbf24;font-size:13px;margin-bottom:16px">'
            '◌ <b>S3-only mode active</b> — export will stop at S3. '
            'The Redshift COPY step (Tab 2) and Job Queue Redshift leg will be skipped.</div>',
            unsafe_allow_html=True,
        )
    else:
        st.markdown(
            '<div style="background:#0f2e1e;border:1px solid #1a5c3a;border-radius:10px;'
            'padding:10px 16px;color:#34d399;font-size:13px;margin-bottom:16px">'
            '● <b>Full pipeline mode</b> — after S3 export, the Job Queue will also run Redshift COPY.</div>',
            unsafe_allow_html=True,
        )

    col_left, col_right = st.columns([1, 1], gap="large")

    with col_left:
        st.markdown('<div class="card-title">🗂 Source — Snowflake</div>', unsafe_allow_html=True)

        # Schema picker
        sf_schema_opts = st.session_state.sf_schemas if st.session_state.sf_connected else ["— connect first —"]
        sf_schema = st.selectbox("Schema", sf_schema_opts, key="sf_schema_sel")

        # Tables for selected schema — multiselect
        if st.session_state.sf_connected and sf_schema and sf_schema not in ("— connect first —",):
            tables_map = st.session_state.get("_sf_tables_map", {})
            if tables_map:
                sf_table_opts = tables_map.get(sf_schema, [])
            else:
                result = get_snowflake_tables(_sf_cfg, sf_schema)
                sf_table_opts = result if result else []
            st.session_state.sf_tables = sf_table_opts
        else:
            sf_table_opts = []

        sf_tables_selected = st.multiselect(
            "Tables  (select one or more)",
            options=sf_table_opts,
            default=st.session_state.get("sf_tables_selected", []),
            placeholder="Choose tables…",
            key="sf_table_multisel",
        )
        st.session_state["sf_tables_selected"] = sf_tables_selected

        # For backwards-compat with single-table references elsewhere
        sf_table = sf_tables_selected[0] if sf_tables_selected else "— select table —"

        if len(sf_tables_selected) > 1:
            st.markdown(
                f'<span class="badge badge-pending">◌ Batch mode — {len(sf_tables_selected)} tables · same export settings applied to all</span>',
                unsafe_allow_html=True,
            )

        st.markdown('<div style="height:8px"></div>', unsafe_allow_html=True)

        custom_query = st.checkbox("Use custom SQL query", key="sf_custom_sql", disabled=len(sf_tables_selected) > 1,
                                   help="Custom SQL is only available when a single table is selected.")
        if custom_query and len(sf_tables_selected) == 1:
            sql_query = st.text_area(
                "SQL Query",
                value=f"SELECT * FROM {sf_schema}.{sf_table}\nLIMIT 1000000",
                height=100,
                key="sf_sql_query",
            )
        else:
            sql_query = None  # batch: each table gets SELECT * FROM schema.table

        row_limit = st.number_input("Row limit per table (0 = all rows)", min_value=0, value=0, key="sf_row_limit")

    with col_right:
        st.markdown('<div class="card-title">☁️ Target — Amazon S3</div>', unsafe_allow_html=True)

        file_format = st.selectbox("File Format", ["CSV", "Parquet", "JSON", "ORC"], key="s3_file_format")

        col_a, col_b = st.columns(2)
        with col_a:
            if file_format == "CSV":
                delimiter = st.selectbox("Field Delimiter", [",  (comma)", "|  (pipe)", "\\t  (tab)", ";  (semicolon)"], key="s3_delimiter")
                delim_map = {",  (comma)": ",", "|  (pipe)": "|", "\\t  (tab)": "\t", ";  (semicolon)": ";"}
                actual_delimiter = delim_map[delimiter]
            else:
                st.info(f"{file_format} uses native encoding.")
        with col_b:
            skip_header = st.checkbox("Include header row", value=True, key="s3_header")
            compress    = st.checkbox("GZIP compression (Parquet/JSON only)", value=False, key="s3_gzip", help="CSV compression disabled — Redshift requires uncompressed CSV for reliable COPY")

        quote_char = st.selectbox("Quote Character", ['"  (double)', "'  (single)", "None"], key="s3_quote") if file_format == "CSV" else None

        if len(sf_tables_selected) <= 1:
            s3_filename = st.text_input(
                "Output filename",
                value=f"{sf_table.lower() if sf_table not in ['— select table —','— connect first —'] else 'output'}_{datetime.now().strftime('%Y%m%d')}.{file_format.lower()}",
                key="s3_filename",
            )
        else:
            st.info(f"Batch mode: each table will be exported as `<table_name>_{datetime.now().strftime('%Y%m%d')}.{file_format.lower()}` under the configured S3 prefix.")
            s3_filename = None

        encoding = st.selectbox("Encoding", ["UTF-8", "UTF-16", "ISO-8859-1"], key="s3_encoding") if file_format == "CSV" else None
        null_string = st.text_input("NULL represented as", value="", placeholder='e.g. \\N or empty', key="s3_null_str") if file_format == "CSV" else None

    st.markdown("---")

    # ── Run button ────────────────────────────────────────────────────────────
    run_col, status_col = st.columns([2, 5])
    with run_col:
        run_sf_export = st.button("▶ Export to S3", key="btn_sf_export", use_container_width=True)

    if run_sf_export:
        if not sf_tables_selected:
            st.error("Please select at least one table before running.")
        elif not st.session_state.sf_connected:
            st.error("Snowflake is not connected. Please connect via the sidebar first.")
        else:
            add_log(f"Starting export · {len(sf_tables_selected)} table(s) → S3", "info")
            progress = st.progress(0, text="Preparing export…")
            all_ok = True

            for i, tbl in enumerate(sf_tables_selected):
                progress.progress((i) / len(sf_tables_selected), text=f"Exporting {tbl}…")
                try:
                    import snowflake.connector, boto3, csv, io

                    # Build query for this table
                    if sql_query and len(sf_tables_selected) == 1:
                        query = sql_query
                    else:
                        query = f'SELECT * FROM {sf_schema}."{tbl}"'
                        if row_limit and row_limit > 0:
                            query += f" LIMIT {row_limit}"

                    # Fetch from Snowflake
                    conn = snowflake.connector.connect(**_sf_cfg)
                    cur = conn.cursor()
                    cur.execute(query)
                    rows = cur.fetchall()
                    col_names = [d[0] for d in cur.description]
                    cur.close()
                    conn.close()

                    # Determine output filename
                    ts_now = datetime.now().strftime('%Y%m%d_%H%M%S')
                    fname = s3_filename if (s3_filename and len(sf_tables_selected) == 1) else \
                        f"{tbl.upper()}_{ts_now}.{file_format.lower()}"
                    # Serialise
                    if file_format == "CSV":
                        buf = io.StringIO()
                        # Always use comma delimiter and full quoting so Redshift COPY can parse reliably
                        writer = csv.writer(
                            buf,
                            delimiter=",",
                            quotechar='"',
                            quoting=csv.QUOTE_ALL,
                            lineterminator="\n",
                        )
                        # Always write header — Redshift COPY uses IGNOREHEADER 1
                        writer.writerow(col_names)
                        for row in rows:
                            # Normalise None → empty string so nulls load cleanly
                            writer.writerow(["" if v is None else v for v in row])
                        body = buf.getvalue().encode("utf-8")
                    elif file_format == "Parquet":
                        import pandas as pd
                        df = pd.DataFrame(rows, columns=col_names)
                        pq_buf = io.BytesIO()
                        df.to_parquet(pq_buf, index=False)
                        body = pq_buf.getvalue()
                    else:
                        import json, pandas as pd
                        df = pd.DataFrame(rows, columns=col_names)
                        body = df.to_json(orient="records", lines=True).encode("utf-8")

                    if compress and file_format != "CSV":
                        import gzip
                        body = gzip.compress(body)
                        if not fname.endswith(".gz"):
                            fname += ".gz"

                    # Upload to S3
                    s3_client = boto3.client(
                        "s3",
                        aws_access_key_id=_s3_cfg["access_key"],
                        aws_secret_access_key=_s3_cfg["secret_key"],
                        region_name=_s3_cfg["region"],
                    )
                    s3_key = f"{_s3_cfg['prefix'].rstrip('/')}/{sf_schema}/{fname}"
                    s3_client.put_object(Bucket=_s3_cfg["bucket"], Key=s3_key, Body=body)

                    # Sync s3_key back into job_queue so COPY uses the exact uploaded path
                    for _job in st.session_state.get("job_queue", []):
                        if _job.get("sf_table", "").upper() == tbl.upper():
                            _job["s3_key"] = s3_key
                            break

                    add_log(f"✓ {tbl} → s3://{_s3_cfg['bucket']}/{s3_key} ({len(rows):,} rows)", "ok")

                except Exception as exc:
                    add_log(f"✗ {tbl}: {exc}", "err")
                    all_ok = False

            progress.progress(1.0, text="Done!")
            if all_ok:
                st.success(f"✅ {len(sf_tables_selected)} table(s) exported to S3 successfully.")
            else:
                st.warning("Export completed with some errors — check the logs.")

    # ── Execution controls (Chunk 2 — job queue) ──────────────────────────────
    _jq_label = (
        "⚙️ Advanced — Job Queue Engine  (SF → S3 only · Redshift COPY skipped)"
        if _s3_only else
        "⚙️ Advanced — Job Queue Engine  (SF → S3 → Redshift)"
    )
    with st.expander(_jq_label, expanded=False):
        if _s3_only:
            st.markdown(
                '<div style="background:#2a2010;border:1px solid #78350f;border-radius:8px;'
                'padding:8px 14px;color:#fbbf24;font-size:12px;margin-bottom:10px">'
                '◌ <b>S3-only mode</b> — the queue runner will execute the Snowflake → S3 export '
                'for each job and <b>skip the Redshift COPY step</b>. '
                'Switch the Pipeline Destination to "SF → S3 → Redshift" to enable COPY.</div>',
                unsafe_allow_html=True,
            )
        render_execution_controls(sf_cfg=_sf_cfg, s3_cfg=_s3_cfg, rs_cfg=_rs_cfg)


# ─────────────────────────────────────────────────────────────────────────────
# TAB 2 · S3 → Redshift
# ─────────────────────────────────────────────────────────────────────────────

# ── Extension → format map ───────────────────────────────────────────────────
_EXT_FMT = {
    ".csv": "CSV", ".csv.gz": "CSV",
    ".parquet": "Parquet", ".parq": "Parquet",
    ".json": "JSON", ".jsonl": "JSON", ".ndjson": "JSON", ".json.gz": "JSON",
    ".orc": "ORC",
    ".avro": "Avro",
}

def _detect_fmt(key: str) -> str:
    k = key.lower()
    for ext, fmt in sorted(_EXT_FMT.items(), key=lambda x: -len(x[0])):
        if k.endswith(ext):
            return fmt
    return "Unknown"

def _s3_list_prefix(bucket, prefix, access_key, secret_key, region):
    """
    List one level of an S3 prefix — returns:
      folders: list of prefix strings (common prefixes / virtual directories)
      files:   list of (key, size, fmt) for actual objects at this level
    Returns Exception on error.
    """
    try:
        import boto3
        s3 = boto3.client("s3", aws_access_key_id=access_key,
                          aws_secret_access_key=secret_key, region_name=region)
        resp = s3.list_objects_v2(Bucket=bucket, Prefix=prefix, Delimiter="/")
        folders = [cp["Prefix"] for cp in resp.get("CommonPrefixes", [])]
        files   = [
            (obj["Key"], obj["Size"], _detect_fmt(obj["Key"]))
            for obj in resp.get("Contents", [])
            if obj["Key"] != prefix  # exclude the prefix key itself
        ]
        return folders, files
    except Exception as exc:
        return exc

def _rs_get_table_columns(rs_cfg, schema, table):
    try:
        rows = _rs_run_sql(
            rs_cfg,
            f"SELECT column_name FROM information_schema.columns "
            f"WHERE table_schema = '{schema.lower()}' AND table_name = '{table.lower()}' "
            f"ORDER BY ordinal_position",
            timeout=30,
        )
        return [r[0] for r in rows]
    except Exception:
        return None

def _peek_file_columns(bucket, key, fmt, access_key, secret_key, region):
    try:
        import boto3, io, csv as _csv
        s3 = boto3.client("s3", aws_access_key_id=access_key,
                          aws_secret_access_key=secret_key, region_name=region)
        if fmt == "Parquet":
            import pandas as pd
            raw = s3.get_object(Bucket=bucket, Key=key)["Body"].read()
            return list(pd.read_parquet(io.BytesIO(raw)).columns)
        raw = s3.get_object(Bucket=bucket, Key=key, Range="bytes=0-131071")["Body"].read()
        if fmt == "CSV":
            if key.endswith(".gz"):
                import gzip; raw = gzip.decompress(raw)
            header = next(_csv.reader(io.StringIO(raw.decode("utf-8", errors="replace"))), None)
            return [c.strip().strip('"') for c in header] if header else []
        if fmt == "JSON":
            import json
            if key.endswith(".gz"):
                import gzip; raw = gzip.decompress(raw)
            for line in raw.decode("utf-8", errors="replace").splitlines():
                line = line.strip()
                if line:
                    return list(json.loads(line).keys())
        return []
    except Exception:
        return []

def _do_redshift_copy(rs_cfg, s3_cfg, s3_key, rs_schema, rs_table, fmt, load_mode,
                      rs_delimiter=",", rs_skip_header=1, rs_null_as="",
                      rs_encoding="UTF8", rs_date_fmt="YYYY-MM-DD",
                      rs_max_error=10, rs_compupdate=True, rs_statupdate=True):
    s3_uri = f"s3://{s3_cfg['bucket']}/{s3_key}"
    ak, sk  = s3_cfg["access_key"], s3_cfg["secret_key"]
    if load_mode == "TRUNCATE & LOAD":
        _rs_run_sql(rs_cfg, f'TRUNCATE TABLE "{rs_schema}"."{rs_table}"', timeout=30)
    delim_map = {",  (comma)": ",", "|  (pipe)": "|", "\\t  (tab)": "\t", ";  (semicolon)": ";"}
    delim = delim_map.get(rs_delimiter, rs_delimiter)
    if fmt == "CSV":
        sql = f"""COPY "{rs_schema}"."{rs_table}" FROM '{s3_uri}'
            ACCESS_KEY_ID '{ak}' SECRET_ACCESS_KEY '{sk}'
            FORMAT AS CSV DELIMITER '{delim}' QUOTE '"' IGNOREHEADER {rs_skip_header}
            NULL AS '{rs_null_as}' ENCODING {rs_encoding} DATEFORMAT '{rs_date_fmt}'
            MAXERROR {rs_max_error} {"COMPUPDATE ON" if rs_compupdate else "COMPUPDATE OFF"}
            {"STATUPDATE ON" if rs_statupdate else "STATUPDATE OFF"} TIMEFORMAT 'auto'"""
    elif fmt == "Parquet":
        sql = f"""COPY "{rs_schema}"."{rs_table}" FROM '{s3_uri}'
            ACCESS_KEY_ID '{ak}' SECRET_ACCESS_KEY '{sk}'
            FORMAT AS PARQUET MAXERROR {rs_max_error}"""
    elif fmt == "ORC":
        sql = f"""COPY "{rs_schema}"."{rs_table}" FROM '{s3_uri}'
            ACCESS_KEY_ID '{ak}' SECRET_ACCESS_KEY '{sk}'
            FORMAT AS ORC MAXERROR {rs_max_error}"""
    else:
        sql = f"""COPY "{rs_schema}"."{rs_table}" FROM '{s3_uri}'
            ACCESS_KEY_ID '{ak}' SECRET_ACCESS_KEY '{sk}'
            FORMAT AS JSON 'auto' MAXERROR {rs_max_error}
            {"COMPUPDATE ON" if rs_compupdate else "COMPUPDATE OFF"}"""
    _rs_run_sql(rs_cfg, sql.strip(), timeout=300)


with tab2:
    st.markdown('<div class="card-title">📥 S3 → Redshift Loader</div>', unsafe_allow_html=True)

    # ── Destination mode banner ───────────────────────────────────────────────
    if _s3_only:
        st.markdown(
            '<div style="background:#2e1010;border:1px solid #7f1d1d;border-radius:10px;'
            'padding:14px 18px;color:#f87171;font-size:14px;font-weight:600;margin-bottom:16px">'
            '✗ Redshift COPY is disabled in <b>S3-only mode</b>. '
            'Use the <b>Pipeline Destination</b> dropdown above and switch to '
            '"SF → S3 → Redshift" to enable this tab.</div>',
            unsafe_allow_html=True,
        )
        _tab2_skip = True
    else:
        _tab2_skip = False

    if not _tab2_skip:
        t2_col_src, t2_col_tgt = st.columns([1, 1], gap="large")

        # ─────────────────────────────────────────────────────────────────────────
        # LEFT COLUMN — S3 bucket browser
        # ─────────────────────────────────────────────────────────────────────────
        with t2_col_src:
            st.markdown('<div class="card-title">☁️ Source — Amazon S3</div>', unsafe_allow_html=True)

            t2_bucket = st.text_input(
                "S3 Bucket",
                value=st.session_state.get("t2_bucket_val", s3_bucket or ""),
                placeholder="my-data-lake",
                key="t2_bucket_input",
            )

            # ── Browse button — lists top level of current path ───────────────────
            if st.button("🔍 Browse", key="btn_t2_browse", use_container_width=True):
                if not t2_bucket.strip():
                    st.warning("Enter a bucket name first.")
                else:
                    bucket_to_use = t2_bucket.strip()
                    current_prefix = st.session_state.get("t2_browser_prefix", "")
                    with st.spinner(f"Listing s3://{bucket_to_use}/{current_prefix} …"):
                        res = _s3_list_prefix(bucket_to_use, current_prefix,
                                              s3_access_key, s3_secret_key, s3_region)
                    if isinstance(res, Exception):
                        st.error(f"Cannot access bucket: {res}")
                    else:
                        st.session_state["t2_bucket_val"]     = bucket_to_use
                        st.session_state["t2_browser_folders"] = res[0]
                        st.session_state["t2_browser_files"]   = res[1]
                        st.session_state["t2_selected_files"]  = []   # reset selection
                    st.rerun()

            # ── Navigation breadcrumb + up button ────────────────────────────────
            _cur_prefix = st.session_state.get("t2_browser_prefix", "")
            if _cur_prefix:
                bc_parts   = ["(root)"] + [p for p in _cur_prefix.rstrip("/").split("/") if p]
                bc_display = " / ".join(bc_parts)
                nav_c1, nav_c2 = st.columns([4, 1])
                with nav_c1:
                    st.caption(f"📂 {bc_display}")
                with nav_c2:
                    if st.button("⬆ Up", key="btn_t2_up", use_container_width=True):
                        parts = _cur_prefix.rstrip("/").split("/")
                        new_prefix = "/".join(parts[:-1])
                        st.session_state["t2_browser_prefix"]  = (new_prefix + "/") if new_prefix else ""
                        st.session_state["t2_browser_folders"] = []
                        st.session_state["t2_browser_files"]   = []
                        st.session_state["t2_selected_files"]  = []
                        st.rerun()

            # ── Folder list ───────────────────────────────────────────────────────
            _folders = st.session_state.get("t2_browser_folders", [])
            if _folders:
                folder_names = [f.rstrip("/").split("/")[-1] for f in _folders]
                chosen_folder = st.selectbox(
                    f"📁 Folders  ({len(_folders)} found — select to drill in)",
                    options=["— stay here —"] + folder_names,
                    key="t2_folder_sel",
                )
                if chosen_folder != "— stay here —":
                    if st.button(f"Open  📁 {chosen_folder}", key="btn_t2_open_folder", use_container_width=True):
                        selected_full = _folders[folder_names.index(chosen_folder)]
                        with st.spinner(f"Opening {chosen_folder}/ …"):
                            res2 = _s3_list_prefix(
                                st.session_state["t2_bucket_val"], selected_full,
                                s3_access_key, s3_secret_key, s3_region,
                            )
                        if isinstance(res2, Exception):
                            st.error(str(res2))
                        else:
                            st.session_state["t2_browser_prefix"]  = selected_full
                            st.session_state["t2_browser_folders"] = res2[0]
                            st.session_state["t2_browser_files"]   = res2[1]
                            st.session_state["t2_selected_files"]  = []
                        st.rerun()

            # ── File list with auto-detected format ───────────────────────────────
            _browser_files = st.session_state.get("t2_browser_files", [])
            if _browser_files:
                _file_display = [
                    f"{k.split('/')[-1]}  ·  {_detect_fmt(k)}  ·  {format_bytes(sz)}"
                    for k, sz, fmt in _browser_files
                ]
                _prev_sel = st.session_state.get("t2_selected_files", _file_display)
                chosen_file_labels = st.multiselect(
                    f"Files  ({len(_browser_files)} found — format auto-detected)",
                    options=_file_display,
                    default=[l for l in _prev_sel if l in _file_display],
                    key="t2_file_multisel",
                )
                st.session_state["t2_selected_files"] = chosen_file_labels
                _label_to_entry = dict(zip(_file_display, _browser_files))
                t2_selected_entries = [_label_to_entry[l] for l in chosen_file_labels]
                t2_selected_keys    = [k for k, _, _ in t2_selected_entries]

                if t2_selected_keys:
                    _fmts_present = list({fmt for _, _, fmt in t2_selected_entries if fmt != "Unknown"})
                    _fmt_badge = " · ".join(_fmts_present) if _fmts_present else "Unknown"
                    st.markdown(
                        f'<span class="badge badge-connected">✓ {len(t2_selected_keys)} file(s) selected · {_fmt_badge}</span>',
                        unsafe_allow_html=True,
                    )
            elif st.session_state.get("t2_browser_files") is not None:
                st.info("No files in this folder. Open a sub-folder above.")
                t2_selected_entries = []
                t2_selected_keys    = []
            else:
                st.caption("Click Browse to explore the bucket.")
                t2_selected_entries = []
                t2_selected_keys    = []

            # ── CSV options (only shown when CSV files selected) ──────────────────
            _has_csv = any(fmt == "CSV" for _, _, fmt in t2_selected_entries) if t2_selected_entries else False
            if _has_csv:
                st.markdown("---")
                st.markdown('<div class="card-title">📄 CSV Options</div>', unsafe_allow_html=True)
                _tc1, _tc2 = st.columns(2)
                with _tc1:
                    t2_delimiter   = st.selectbox("Delimiter", [",  (comma)", "|  (pipe)", "\\t  (tab)", ";  (semicolon)"], key="t2_delimiter")
                    t2_skip_header = st.number_input("Skip header rows", 0, 5, 1, key="t2_skip_header")
                with _tc2:
                    t2_null_as  = st.text_input("NULL as", value="", placeholder="\\N", key="t2_null_as")
                    t2_encoding = st.selectbox("Encoding", ["UTF8", "UTF16", "LATIN1"], key="t2_encoding")
                t2_date_fmt = st.text_input("Date format", value="YYYY-MM-DD", key="t2_date_fmt")
            else:
                t2_delimiter = t2_skip_header = t2_null_as = t2_encoding = t2_date_fmt = None

            t2_max_error  = st.number_input("MAXERROR tolerance", 0, 10000, 10, key="t2_maxerror")
            t2_compupdate = st.checkbox("COMPUPDATE ON", value=True, key="t2_compupdate")
            t2_statupdate = st.checkbox("STATUPDATE ON", value=True, key="t2_statupdate")

        # ─────────────────────────────────────────────────────────────────────────
        # RIGHT COLUMN — Redshift target
        # ─────────────────────────────────────────────────────────────────────────
        with t2_col_tgt:
            st.markdown('<div class="card-title">🔴 Target — Redshift</div>', unsafe_allow_html=True)

            _rscfg_t2 = {
                "workgroup": rs_workgroup,
                "database":  rs_db,
                "region":    rs_region,
                "access_key": s3_access_key,
                "secret_key": s3_secret_key,
            }

            # ── Schema selector ───────────────────────────────────────────────────
            t2_rs_schema_opts = st.session_state.rs_schemas if st.session_state.rs_connected else ["— connect first —"]
            t2_rs_schema = st.selectbox("Target Schema", t2_rs_schema_opts, key="t2_rs_schema")

            # ── Table list ────────────────────────────────────────────────────────
            # Cache key is per-schema so switching schemas auto-fetches fresh list.
            # A None value means "never fetched"; [] means "fetched but empty".
            _t2_cache_key  = f"_t2_tbls_{t2_rs_schema}"
            _t2_err_key    = f"_t2_tbls_err_{t2_rs_schema}"

            if st.session_state.rs_connected and t2_rs_schema not in ("— connect first —", ""):
                # Always re-fetch when cache key absent (schema switched or after refresh)
                if _t2_cache_key not in st.session_state:
                    with st.spinner(f"Fetching tables in {t2_rs_schema}…"):
                        try:
                            _live = get_redshift_tables(_rscfg_t2, t2_rs_schema, _raise=True)
                            st.session_state[_t2_cache_key] = _live if _live is not None else []
                            st.session_state.pop(_t2_err_key, None)
                        except Exception as _te:
                            st.session_state[_t2_cache_key] = []
                            st.session_state[_t2_err_key]   = str(_te)

                t2_rs_table_opts_base = st.session_state.get(_t2_cache_key, [])
                _t2_fetch_err         = st.session_state.get(_t2_err_key)

                # Show error with full message so we know which query failed
                if _t2_fetch_err:
                    st.error(f"⚠ Could not fetch tables: {_t2_fetch_err}")
                    st.caption("Check that your Redshift user has USAGE on this schema and SELECT on SVV_ALL_TABLES.")
                elif not t2_rs_table_opts_base:
                    st.warning(f"No tables found in schema '{t2_rs_schema}'. Schema may be empty or user lacks SELECT privilege.")
                else:
                    st.caption(f"✓ {len(t2_rs_table_opts_base)} table(s) in {t2_rs_schema}")

                if st.button("↺ Refresh tables", key="btn_t2_refresh_tables", use_container_width=True):
                    st.session_state.pop(_t2_cache_key, None)
                    st.session_state.pop(_t2_err_key, None)
                    st.rerun()
            else:
                t2_rs_table_opts_base = []

            t2_target_mode = st.radio(
                "Target table", ["Existing table", "New table name"],
                horizontal=True, key="t2_target_mode",
            )
            if t2_target_mode == "Existing table":
                _tbl_opts = t2_rs_table_opts_base if t2_rs_table_opts_base else ["— no tables found —"]
                t2_rs_table = st.selectbox("Table", _tbl_opts, key="t2_rs_table_sel")
            else:
                t2_rs_table = st.text_input("New table name", placeholder="MY_NEW_TABLE", key="t2_rs_new_table")

            t2_load_mode = st.selectbox("Load Mode", ["APPEND", "TRUNCATE & LOAD"], key="t2_load_mode")

            st.markdown("---")
            st.markdown('<div class="card-title">📋 Schema Validation</div>', unsafe_allow_html=True)
            st.caption("At export time, each file's columns are compared against the Redshift table. Mismatched files are skipped and listed.")
            t2_strict = st.checkbox(
                "Strict — skip if file has extra columns too",
                value=True, key="t2_strict_schema",
            )

        # ── Export button ─────────────────────────────────────────────────────────
        st.markdown("---")
        exp_col, _ = st.columns([2, 5])
        with exp_col:
            run_t2 = st.button("▶ Load to Redshift", key="btn_t2_load", use_container_width=True)

        if run_t2:
            if not st.session_state.rs_connected:
                st.error("Redshift is not connected — connect via the sidebar first.")
            elif not t2_selected_keys:
                st.error("No files selected. Browse the bucket and pick files first.")
            elif not t2_rs_schema or t2_rs_schema == "— connect first —":
                st.error("Select a target schema first.")
            elif not t2_rs_table or t2_rs_table in ("— no tables found —", "— select schema —"):
                st.error("Select or enter a target table name.")
            else:
                _rscfg_load = _rscfg_t2
                _s3cfg_load = {
                    "bucket":     st.session_state.get("t2_bucket_val", t2_bucket),
                    "access_key": s3_access_key,
                    "secret_key": s3_secret_key,
                    "region":     s3_region,
                }

                # Fetch RS columns once for validation
                rs_cols = _rs_get_table_columns(_rscfg_load, t2_rs_schema, t2_rs_table)
                rs_cols_lower = {c.lower() for c in rs_cols} if rs_cols else None

                skipped, succeeded, failed = [], [], []
                progress_bar = st.progress(0, text="Starting…")

                for idx, (s3_key, _sz, file_fmt) in enumerate(t2_selected_entries):
                    fname = s3_key.split("/")[-1]
                    progress_bar.progress(idx / len(t2_selected_entries), text=f"Processing {fname}…")

                    # ── Schema validation (only at export time) ───────────────────
                    if rs_cols_lower is not None:
                        file_cols = _peek_file_columns(
                            _s3cfg_load["bucket"], s3_key, file_fmt,
                            s3_access_key, s3_secret_key, s3_region,
                        )
                        if file_cols:
                            fcl = {c.lower() for c in file_cols}
                            missing = rs_cols_lower - fcl
                            extra   = fcl - rs_cols_lower
                            mismatch = bool(missing or extra) if t2_strict else bool(missing)
                            if mismatch:
                                parts = []
                                if missing: parts.append(f"missing cols: {', '.join(sorted(missing))}")
                                if extra and t2_strict: parts.append(f"extra cols: {', '.join(sorted(extra))}")
                                reason = "; ".join(parts)
                                skipped.append((fname, reason))
                                add_log(f"⚠ SKIPPED {fname}: {reason}", "warn")
                                continue

                    # ── COPY ──────────────────────────────────────────────────────
                    try:
                        _do_redshift_copy(
                            _rscfg_load, _s3cfg_load, s3_key,
                            t2_rs_schema, t2_rs_table, file_fmt, t2_load_mode,
                            rs_delimiter=t2_delimiter or ",  (comma)",
                            rs_skip_header=t2_skip_header if t2_skip_header is not None else 1,
                            rs_null_as=t2_null_as or "",
                            rs_encoding=t2_encoding or "UTF8",
                            rs_date_fmt=t2_date_fmt or "YYYY-MM-DD",
                            rs_max_error=t2_max_error,
                            rs_compupdate=t2_compupdate,
                            rs_statupdate=t2_statupdate,
                        )
                        succeeded.append(fname)
                        add_log(f"✓ {fname} → {t2_rs_schema}.{t2_rs_table}", "ok")
                    except Exception as copy_err:
                        failed.append((fname, str(copy_err)))
                        add_log(f"✗ COPY failed {fname}: {copy_err}", "err")

                progress_bar.progress(1.0, text="Done!")

                # ── Results summary ───────────────────────────────────────────────
                st.markdown("---")
                st.markdown('<div class="card-title">📋 Load Results</div>', unsafe_allow_html=True)
                rc1, rc2, rc3 = st.columns(3)
                with rc1:
                    st.markdown(f'<div class="metric-card"><div class="metric-value" style="color:#34d399">{len(succeeded)}</div><div class="metric-label">Loaded</div></div>', unsafe_allow_html=True)
                with rc2:
                    st.markdown(f'<div class="metric-card"><div class="metric-value" style="color:#fbbf24">{len(skipped)}</div><div class="metric-label">Skipped</div></div>', unsafe_allow_html=True)
                with rc3:
                    st.markdown(f'<div class="metric-card"><div class="metric-value" style="color:#f87171">{len(failed)}</div><div class="metric-label">Failed</div></div>', unsafe_allow_html=True)

                if succeeded:
                    st.success(f"✅ {len(succeeded)} file(s) loaded into {t2_rs_schema}.{t2_rs_table}.")
                if skipped:
                    st.warning(f"⚠️ {len(skipped)} file(s) skipped — schema mismatch:")
                    st.dataframe(pd.DataFrame(skipped, columns=["File", "Reason"]), use_container_width=True, hide_index=True)
                if failed:
                    st.error(f"❌ {len(failed)} file(s) failed:")
                    st.dataframe(pd.DataFrame(failed, columns=["File", "Error"]), use_container_width=True, hide_index=True)
    # ─────────────────────────────────────────────────────────────────────────────
    # TAB 3 · Monitor  (Chunk 3)
    # ─────────────────────────────────────────────────────────────────────────────
with tab_monitor:
    render_monitoring_tab()


# ─────────────────────────────────────────────────────────────────────────────
# TAB 4 · Metrics & Logs  (Chunk 4 — replaces old Tab 3 content entirely)
# ─────────────────────────────────────────────────────────────────────────────
with tab3:
    render_metrics_tab(s3_bucket=s3_bucket)


# ─────────────────────────────────────────────────────────────────────────────
# TAB 5 · Data Validation  (tab_data_validation.py)
# ─────────────────────────────────────────────────
with tab_validate:
    render_validation_tab(rs_cfg=_rs_cfg, sf_cfg=_sf_cfg)

# TAB 6 · DDL Migrator  (sf_to_rs_combined_ddl_distribution.py)
# ─────────────────────────────────────────────────────────────────────────────

# ── Shared DDL conversion helper (used by both sub-tabs) ─────────────────────
def _run_ddl_conversion(raw_sql, target_schema, use_sf_analysis=False,
                        manual_distkey="", manual_sortkey=""):
    """Convert a single raw Snowflake DDL → Redshift DDL.
    Returns (converted_sql, distkey, sortkey, log_lines).  Raises on hard error."""
    logs = []
    converted = ddl_migrator.convert_create_statement(raw_sql, target_schema)
    converted = ddl_migrator.convert_datatypes(converted)
    converted = ddl_migrator.fix_timestamp_columns(converted)
    converted = ddl_migrator.clean_misc(converted)
    logs.append(("ok", "Syntax & type conversion complete"))

    table_name = (ddl_migrator.extract_table_name(converted)
                  or ddl_migrator.extract_table_name(raw_sql))

    _META = {"QUERY_TEXT","QUERY_ID","USER_NAME","WAREHOUSE_NAME",
             "START_TIME","END_TIME","EXECUTION_STATUS"}

    def _ddl_cols(sql):
        return {
            ln.strip().split()[0].strip('"').upper()
            for ln in sql.splitlines()
            if ln.strip()
            and not ln.strip().upper().startswith(
                ("CREATE","(",")","-","DISTKEY","SORTKEY","ENCODE","DISTSTYLE"))
            and len(ln.strip().split()) >= 2
        }

    # DISTKEY
    if manual_distkey.strip():
        distkey = manual_distkey.strip()
        logs.append(("info", f"Manual DISTKEY override: {distkey}"))
    elif use_sf_analysis and table_name:
        logs.append(("info", f"Querying Snowflake history for: {table_name}"))
        _orig = ddl_migrator.SNOWFLAKE_CREDS.copy()
        ddl_migrator.SNOWFLAKE_CREDS.update({**_sf_cfg, "schema": "ACCOUNT_USAGE"})
        distkey, _ = ddl_migrator.fetch_best_keys(table_name)
        ddl_migrator.SNOWFLAKE_CREDS.update(_orig)
        cols = _ddl_cols(converted)
        if distkey and (distkey.upper() in _META or distkey.upper() not in cols):
            logs.append(("warn", f"DISTKEY '{distkey}' not a table column — ignored"))
            distkey = None
        logs.append(("ok" if distkey else "warn",
                     f"DISTKEY from history: {distkey or 'none found → AUTO'}"))
    else:
        distkey = None

    # SORTKEY
    if manual_sortkey.strip():
        sortkey = manual_sortkey.strip()
        logs.append(("info", f"Manual SORTKEY override: {sortkey}"))
    elif use_sf_analysis and table_name and not manual_distkey.strip():
        _orig2 = ddl_migrator.SNOWFLAKE_CREDS.copy()
        ddl_migrator.SNOWFLAKE_CREDS.update({**_sf_cfg, "schema": "ACCOUNT_USAGE"})
        _, sortkey = ddl_migrator.fetch_best_keys(table_name)
        ddl_migrator.SNOWFLAKE_CREDS.update(_orig2)
        cols2 = _ddl_cols(converted)
        if sortkey and (sortkey.upper() in _META or sortkey.upper() not in cols2):
            logs.append(("warn", f"SORTKEY '{sortkey}' not a table column — ignored"))
            sortkey = None
        logs.append(("ok" if sortkey else "warn",
                     f"SORTKEY from history: {sortkey or 'none found → AUTO'}"))
    else:
        sortkey = None

    converted = ddl_migrator.finalize_table(converted, distkey, sortkey)
    logs.append(("ok", "DDL finalised — DISTKEY/SORTKEY appended"))
    return converted, distkey, sortkey, logs


def _validate_redshift_sql(sql: str) -> dict:
    """
    Validate SQL against the Redshift dialect using sqlglot.
    Returns dict: {ok: bool, errors: list[str], warnings: list[str], dialect_notes: list[str]}
    """
    result = {"ok": False, "errors": [], "warnings": [], "dialect_notes": []}
    if not sql or not sql.strip():
        result["errors"].append("Empty SQL — nothing to validate.")
        return result
    try:
        import sqlglot
        import sqlglot.errors as sge
        # Parse with Redshift dialect — raises or returns error tokens
        try:
            expressions = sqlglot.parse(sql, dialect="redshift", error_level=sge.ErrorLevel.WARN)
        except sge.ParseError as pe:
            for err in pe.errors:
                result["errors"].append(f"Parse error: {err.get('description', str(err))}")
            return result

        # Collect any parse-time warnings sqlglot embedded in the tree
        for expr in expressions:
            if expr is None:
                result["warnings"].append("One statement could not be fully parsed (returned None).")
                continue
            # Check for unsupported nodes
            for node in expr.walk():
                node_type = type(node).__name__
                if node_type in ("Anonymous", "Placeholder"):
                    result["warnings"].append(
                        f"Unrecognised token/function in statement: check manually."
                    )
                    break

        # Snowflake-specific constructs that are NOT valid in Redshift
        sf_patterns = [
            ("VARIANT",          "VARIANT type is Snowflake-only — use SUPER in Redshift"),
            ("OBJECT_CONSTRUCT", "OBJECT_CONSTRUCT() is Snowflake-only — use JSON_BUILD_OBJECT() or SUPER"),
            ("ARRAY_CONSTRUCT",  "ARRAY_CONSTRUCT() is Snowflake-only — use ARRAY[] or SUPER"),
            ("FLATTEN(",         "FLATTEN() is Snowflake-only — use Redshift SUPER unnesting"),
            ("QUALIFY",          "QUALIFY clause is Snowflake-only — rewrite with a subquery"),
            ("SAMPLE(",          "TABLESAMPLE/SAMPLE is Snowflake syntax — use Redshift TABLESAMPLE"),
            ("TRANSIENT",        "TRANSIENT table keyword not supported in Redshift — remove it"),
            ("CLUSTER BY",       "CLUSTER BY is Snowflake syntax — use SORTKEY/DISTKEY in Redshift"),
            ("TIMESTAMP_NTZ",    "TIMESTAMP_NTZ is Snowflake-only — use TIMESTAMP or TIMESTAMPTZ"),
            ("TIMESTAMP_LTZ",    "TIMESTAMP_LTZ is Snowflake-only — use TIMESTAMPTZ"),
            ("TIMESTAMP_TZ",     "TIMESTAMP_TZ is Snowflake-only — use TIMESTAMPTZ"),
            ("NUMBER(",          "NUMBER(p,s) type — ensure converted to DECIMAL/NUMERIC for Redshift"),
        ]
        sql_upper = sql.upper()
        for kw, note in sf_patterns:
            if kw.upper() in sql_upper:
                result["warnings"].append(f"⚠ Possible Snowflake remnant: {note}")

        if not result["errors"]:
            result["ok"] = True
            if not result["warnings"]:
                result["dialect_notes"].append("✓ SQL parsed cleanly against the Redshift dialect.")
            else:
                result["dialect_notes"].append(
                    "⚠ SQL parsed but has warnings — review before executing."
                )
    except ImportError:
        result["errors"].append(
            "sqlglot is not installed. Run: pip install sqlglot"
        )
    except Exception as exc:
        result["errors"].append(f"Unexpected validation error: {exc}")
    return result


def _diff_sql_changes(original: str, converted: str) -> list[dict]:
    """
    Return a structured list of changes between original and converted SQL.
    Each item: {type: 'added'|'removed'|'changed', description: str, before: str, after: str}
    Uses token-level diffing via sqlglot normalisation + line-level difflib.
    """
    import difflib, re

    changes = []
    if not original or not converted:
        return changes

    def _normalise(sql):
        # Collapse whitespace, uppercase keywords for comparison
        return re.sub(r'\s+', ' ', sql).strip()

    orig_lines  = [l.rstrip() for l in original.strip().splitlines()]
    conv_lines  = [l.rstrip() for l in converted.strip().splitlines()]

    differ = difflib.SequenceMatcher(None, orig_lines, conv_lines, autojunk=False)

    for tag, i1, i2, j1, j2 in differ.get_opcodes():
        if tag == "equal":
            continue
        orig_chunk = "\n".join(orig_lines[i1:i2])
        conv_chunk = "\n".join(conv_lines[j1:j2])
        if tag == "replace":
            changes.append({"type": "changed", "before": orig_chunk, "after": conv_chunk})
        elif tag == "delete":
            changes.append({"type": "removed", "before": orig_chunk, "after": ""})
        elif tag == "insert":
            changes.append({"type": "added",   "before": "",          "after": conv_chunk})

    # Also detect high-level semantic changes by keyword scanning
    sf_to_rs_rewrites = [
        (r'\bTRANSIENT\b',       "Removed Snowflake TRANSIENT keyword"),
        (r'\bCLUSTER\s+BY\b',    "Removed Snowflake CLUSTER BY → converted to SORTKEY"),
        (r'\bVARIANT\b',         "Converted VARIANT → SUPER"),
        (r'\bTIMESTAMP_NTZ\b',   "Converted TIMESTAMP_NTZ → TIMESTAMP"),
        (r'\bTIMESTAMP_LTZ\b',   "Converted TIMESTAMP_LTZ → TIMESTAMPTZ"),
        (r'\bTIMESTAMP_TZ\b',    "Converted TIMESTAMP_TZ → TIMESTAMPTZ"),
        (r'\bNUMBER\s*\(',       "Converted NUMBER(p,s) → DECIMAL/NUMERIC"),
        (r'\bFLOAT8\b',          "Converted FLOAT8 → FLOAT"),
        (r'\bDATEADD\s*\(',      "Converted DATEADD() → DATEADD (Redshift compatible)"),
        (r'\bNVL\s*\(',          "NVL() → COALESCE() (Redshift prefers COALESCE)"),
        (r'\bIFF\s*\(',          "Converted IFF() → CASE WHEN"),
        (r'\bQUALIFY\b',         "Rewrote QUALIFY clause as subquery"),
        (r'DISTKEY\s*\(',        "Added DISTKEY distribution key"),
        (r'SORTKEY\s*\(',        "Added SORTKEY sort key"),
        (r'DISTSTYLE\s+\w+',     "Set DISTSTYLE for distribution"),
        (r'ENCODE\s+\w+',        "Added column ENCODE compression"),
    ]
    import re
    orig_upper = original.upper()
    conv_upper = converted.upper()
    for pattern, desc in sf_to_rs_rewrites:
        in_orig = bool(re.search(pattern, orig_upper))
        in_conv = bool(re.search(pattern, conv_upper))
        # "Added" patterns — present in output but not input
        if pattern in (r'DISTKEY\s*\(', r'SORTKEY\s*\(', r'DISTSTYLE\s+\w+', r'ENCODE\s+\w+'):
            if in_conv and not in_orig:
                changes.append({"type": "added", "before": "", "after": desc})
        else:
            # Removed/converted — present in input but handled in output
            if in_orig and not in_conv:
                changes.append({"type": "changed", "before": pattern.replace(r'\b','').replace(r'\s*\(','()').replace(r'\s+\w+',''), "after": desc})

    # Deduplicate descriptions
    seen = set()
    unique = []
    for c in changes:
        key = (c["type"], c.get("after",""), c.get("before",""))
        if key not in seen:
            seen.add(key)
            unique.append(c)
    return unique


def _render_changes_panel(changes: list[dict]) -> str:
    """Render a styled HTML panel showing the diff changes."""
    if not changes:
        return ""
    type_cfg = {
        "changed": ("#1a2540", "#5b8dee", "#2a3a60", "↔ Changed"),
        "added":   ("#0f2e1e", "#34d399", "#1a5c3a", "+ Added"),
        "removed": ("#2e1010", "#f87171", "#7f1d1d", "− Removed"),
    }
    html = '<div style="display:flex;flex-direction:column;gap:6px;margin-top:8px">'
    for ch in changes[:30]:  # cap at 30 to avoid giant panels
        bg, fg, border, label = type_cfg.get(ch["type"], type_cfg["changed"])
        before = ch.get("before", "").strip()
        after  = ch.get("after",  "").strip()
        if before and after:
            detail = (f'<span style="color:#6b7080;font-size:11px">'
                      f'<code style="color:#f87171;background:#1e0808;padding:1px 5px;border-radius:3px">{_esc(before[:80])}</code>'
                      f' → '
                      f'<code style="color:#34d399;background:#081e10;padding:1px 5px;border-radius:3px">{_esc(after[:80])}</code>'
                      f'</span>')
        elif after:
            detail = (f'<code style="color:#34d399;background:#081e10;font-size:11px;'
                      f'padding:1px 5px;border-radius:3px">{_esc(after[:100])}</code>')
        elif before:
            detail = (f'<code style="color:#f87171;background:#1e0808;font-size:11px;'
                      f'padding:1px 5px;border-radius:3px">{_esc(before[:100])}</code>')
        else:
            continue
        html += (f'<div style="background:{bg};border:1px solid {border};border-radius:8px;'
                 f'padding:8px 12px;display:flex;gap:10px;align-items:flex-start">'
                 f'<span style="color:{fg};font-size:11px;font-weight:600;'
                 f'white-space:nowrap;min-width:64px">{label}</span>'
                 f'{detail}</div>')
    html += "</div>"
    return html


def _esc(s: str) -> str:
    """HTML-escape a string for safe inline rendering."""
    return (s.replace("&", "&amp;").replace("<", "&lt;")
             .replace(">", "&gt;").replace('"', "&quot;"))


def _render_validate_result(result: dict) -> str:
    """Render validation result as styled HTML."""
    if result is None:
        return ""
    html = ""
    if result["ok"] and not result["errors"]:
        icon, bg, border, fg = "✓", "#0f2e1e", "#1a5c3a", "#34d399"
        msg = "Syntax valid — Redshift dialect"
    else:
        icon, bg, border, fg = "✗", "#2e1010", "#7f1d1d", "#f87171"
        msg = "Syntax errors detected"
    html += (f'<div style="background:{bg};border:1px solid {border};border-radius:10px;'
             f'padding:10px 16px;color:{fg};font-size:13px;font-weight:600;margin-bottom:6px">'
             f'{icon} {msg}</div>')
    for err in result["errors"]:
        html += (f'<div style="background:#2e1010;border:1px solid #7f1d1d;border-radius:8px;'
                 f'padding:7px 12px;color:#f87171;font-size:12px;margin-bottom:4px">✗ {_esc(err)}</div>')
    for warn in result["warnings"]:
        html += (f'<div style="background:#2a2010;border:1px solid #78350f;border-radius:8px;'
                 f'padding:7px 12px;color:#fbbf24;font-size:12px;margin-bottom:4px">{_esc(warn)}</div>')
    for note in result.get("dialect_notes", []):
        html += (f'<div style="background:#13161f;border:1px solid #2a3148;border-radius:8px;'
                 f'padding:7px 12px;color:#6b7080;font-size:12px;margin-bottom:4px">{_esc(note)}</div>')
    return html


def _render_log_box(log_lines):
    html = '<div class="log-box">'
    icons = {"ok": "✓", "info": "ℹ", "warn": "⚠", "err": "✗"}
    cls_map = {"ok": "log-ok", "info": "log-info", "warn": "log-warn", "err": "log-err"}
    for level, msg in log_lines:
        html += (f'<div><span class="{cls_map.get(level,"log-info")}">'
                 f'{icons.get(level,"ℹ")} {msg}</span></div>')
    html += "</div>"
    return html


with tab_ddl:
    st.markdown('<div class="card-title">🏗 Snowflake → Redshift DDL Converter</div>',
                unsafe_allow_html=True)
    st.caption("Convert Snowflake CREATE TABLE statements to Redshift DDL — "
               "paste a single statement, or browse your Snowflake account to "
               "batch-convert entire schemas into one file.")
    st.markdown("---")

    ddl_sub_paste, ddl_sub_bulk = st.tabs([
        "✏️  Paste & Convert",
        "🗄️  Bulk Convert from Snowflake",
    ])

    # ══════════════════════════════════════════════════════════════════════════
    # SUB-TAB A · Paste & Convert  (original single-DDL flow — unchanged)
    # ══════════════════════════════════════════════════════════════════════════
    with ddl_sub_paste:
        ddl_col_l, ddl_col_r = st.columns([1, 1], gap="large")

        with ddl_col_l:
            st.markdown('<div class="card-title">❄️ Snowflake DDL Input</div>',
                        unsafe_allow_html=True)
            ddl_input_text = st.text_area(
                "Paste Snowflake CREATE TABLE DDL",
                height=380,
                placeholder="""CREATE OR REPLACE TRANSIENT TABLE ORDERS (
    ORDER_ID    NUMBER(38,0),
    CUSTOMER_ID NUMBER(38,0),
    ORDER_DATE  TIMESTAMP_NTZ(9),
    STATUS      VARCHAR(50),
    AMOUNT      FLOAT8,
    METADATA    VARIANT
)
CLUSTER BY (ORDER_DATE);""",
                key="ddl_input_area",
                label_visibility="collapsed",
            )

        with ddl_col_r:
            st.markdown('<div class="card-title">🔴 Redshift DDL Output</div>',
                        unsafe_allow_html=True)
            if st.session_state.ddl_output:
                st.code(st.session_state.ddl_output, language="sql")
                # ── Validate Syntax button ──
                val_ddl_col, _ddl_val_spacer = st.columns([2, 3])
                with val_ddl_col:
                    if st.button("✔ Validate Syntax", key="btn_validate_ddl", use_container_width=True):
                        with st.spinner("Validating against Redshift dialect…"):
                            st.session_state.ddl_validate_result = _validate_redshift_sql(
                                st.session_state.ddl_output
                            )
                            st.session_state.ddl_diff_html = _render_changes_panel(
                                _diff_sql_changes(
                                    st.session_state.get("ddl_input_area", ""),
                                    st.session_state.ddl_output,
                                )
                            )
                        st.rerun()
                if st.session_state.get("ddl_validate_result") is not None:
                    st.markdown(
                        _render_validate_result(st.session_state.ddl_validate_result),
                        unsafe_allow_html=True,
                    )
            else:
                st.markdown(
                    '<div style="color:#4a5068;font-family:\'DM Mono\',monospace;'
                    'font-size:13px;padding:12px 0">— converted DDL will appear here —</div>',
                    unsafe_allow_html=True,
                )

        st.markdown("---")
        st.markdown('<div class="card-title">⚙️ Distribution & Sort Key Options</div>',
                    unsafe_allow_html=True)
        ddl_opt_col1, ddl_opt_col2, ddl_opt_col3 = st.columns(3)
        with ddl_opt_col1:
            ddl_use_sf_analysis = st.checkbox(
                "Analyse Snowflake query history for keys",
                value=False, key="ddl_use_sf_analysis",
                help="Connects to Snowflake ACCOUNT_USAGE.QUERY_HISTORY and parses ASTs "
                     "to recommend DISTKEY / SORTKEY.",
            )
        with ddl_opt_col2:
            ddl_manual_distkey = st.text_input(
                "Override DISTKEY column", placeholder="e.g. customer_id",
                key="ddl_manual_distkey",
            )
        with ddl_opt_col3:
            ddl_manual_sortkey = st.text_input(
                "Override SORTKEY column", placeholder="e.g. order_date",
                key="ddl_manual_sortkey",
            )

        st.markdown("---")
        ddl_schema_col, ddl_run_col, _ddl_spacer = st.columns([2, 2, 1])
        with ddl_schema_col:
            ddl_target_schema = st.text_input(
                "Target Redshift Schema", placeholder="public", value="public",
                key="ddl_target_schema",
            )
        with ddl_run_col:
            run_ddl = st.button("▶ Convert DDL", key="btn_run_ddl",
                                use_container_width=True)

        if run_ddl:
            raw = ddl_input_text.strip()
            if not raw:
                st.error("Please paste a Snowflake DDL statement first.")
            else:
                st.session_state.ddl_running = True
                st.session_state.ddl_logs = []
                with st.spinner("Converting DDL…"):
                    try:
                        converted, distkey, sortkey, logs = _run_ddl_conversion(
                            raw, ddl_target_schema,
                            use_sf_analysis=ddl_use_sf_analysis,
                            manual_distkey=ddl_manual_distkey,
                            manual_sortkey=ddl_manual_sortkey,
                        )
                        st.session_state.ddl_output  = converted
                        st.session_state.ddl_distkey = distkey
                        st.session_state.ddl_sortkey = sortkey
                        st.session_state.ddl_logs    = logs
                        st.session_state.ddl_validate_result = None  # reset on new conversion
                        st.session_state.ddl_diff_html = ""
                        tbl = (ddl_migrator.extract_table_name(converted) or "(unknown)")
                        add_log(f"DDL converted → table: {tbl}", "ok")
                    except Exception as exc:
                        st.session_state.ddl_logs.append(("err", f"Error: {exc}"))
                        add_log(f"DDL migration error: {exc}", "err")
                st.session_state.ddl_running = False
                st.rerun()

        if st.session_state.ddl_logs:
            st.markdown("---")
            st.markdown('<div class="card-title">📋 Conversion Log</div>',
                        unsafe_allow_html=True)
            st.markdown(_render_log_box(st.session_state.ddl_logs),
                        unsafe_allow_html=True)

        # ── What Changed panel ───────────────────────────────────────────────
        if st.session_state.ddl_output and st.session_state.get("ddl_diff_html"):
            st.markdown("---")
            st.markdown('<div class="card-title">🔍 What Changed (Snowflake → Redshift)</div>',
                        unsafe_allow_html=True)
            st.markdown(st.session_state.ddl_diff_html, unsafe_allow_html=True)
        elif st.session_state.ddl_output and not st.session_state.get("ddl_diff_html"):
            # Auto-compute diff on first render after conversion (no validate pressed yet)
            _auto_diff = _render_changes_panel(
                _diff_sql_changes(
                    st.session_state.get("ddl_input_area", ""),
                    st.session_state.ddl_output,
                )
            )
            if _auto_diff:
                st.markdown("---")
                st.markdown('<div class="card-title">🔍 What Changed (Snowflake → Redshift)</div>',
                            unsafe_allow_html=True)
                st.markdown(_auto_diff, unsafe_allow_html=True)

        if st.session_state.ddl_output:
            st.markdown("---")
            dl_c1, dl_c2 = st.columns([3, 1])
            with dl_c2:
                st.download_button(
                    "⬇️ Download DDL (.sql)",
                    data=st.session_state.ddl_output.encode(),
                    file_name=f"redshift_ddl_{datetime.now().strftime('%Y%m%d_%H%M%S')}.sql",
                    mime="text/plain",
                    use_container_width=True,
                )
            with dl_c1:
                _parts = []
                _parts.append(
                    f'<span class="badge badge-connected">● DISTKEY: {st.session_state.ddl_distkey}</span>'
                    if st.session_state.ddl_distkey else
                    '<span class="badge badge-pending">◌ DISTSTYLE AUTO</span>')
                _parts.append(
                    f'<span class="badge badge-connected">● SORTKEY: {st.session_state.ddl_sortkey}</span>'
                    if st.session_state.ddl_sortkey else
                    '<span class="badge badge-pending">◌ SORTKEY AUTO</span>')
                st.markdown("&nbsp;&nbsp;".join(_parts), unsafe_allow_html=True)

    # ══════════════════════════════════════════════════════════════════════════
    # SUB-TAB B · Bulk Convert from Snowflake
    # ══════════════════════════════════════════════════════════════════════════
    with ddl_sub_bulk:
        st.markdown(
            '<div class="card-title">🗄️ Bulk DDL Converter — Browse Snowflake & Convert Multiple Tables</div>',
            unsafe_allow_html=True,
        )
        st.caption(
            "Select a schema, pick one or more tables, and click **▶ Fetch & Convert**. "
            "The tool calls `GET_DDL()` for each table, runs the full conversion pipeline, "
            "and bundles everything into a single `.sql` download."
        )
        st.markdown("---")

        if not st.session_state.sf_connected:
            st.markdown(
                '<span class="badge badge-disconnected">'
                '○ Connect Snowflake via the sidebar first — credentials are used to call GET_DDL()'
                '</span>',
                unsafe_allow_html=True,
            )
        else:
            # ── Step 1 · Schema + table picker ───────────────────────────────
            bulk_top_l, bulk_top_r = st.columns([1, 1], gap="large")

            with bulk_top_l:
                st.markdown('<div class="card-title">❄️ Step 1 — Choose Schema & Tables</div>',
                            unsafe_allow_html=True)

                bulk_schema_opts = (st.session_state.sf_schemas
                                    if st.session_state.sf_schemas
                                    else ["— connect first —"])
                bulk_schema = st.selectbox(
                    "Snowflake Schema", bulk_schema_opts, key="ddl_bulk_schema_sel",
                )

                # Resolve table list (from cache or live fetch)
                _bulk_tbl_opts = []
                if bulk_schema and bulk_schema != "— connect first —":
                    _tmap = st.session_state.get("_sf_tables_map", {})
                    _bulk_tbl_opts = _tmap.get(bulk_schema, [])

                _ref_l, _ref_r = st.columns([3, 1])
                with _ref_r:
                    if st.button("↺ Refresh", key="btn_bulk_refresh",
                                 use_container_width=True,
                                 help="Re-fetch table list from Snowflake"):
                        if bulk_schema and bulk_schema != "— connect first —":
                            with st.spinner("Fetching tables…"):
                                _live = get_snowflake_tables(_sf_cfg, bulk_schema)
                                if _live is not None:
                                    _bulk_tbl_opts = _live
                                    _m = st.session_state.get("_sf_tables_map", {})
                                    _m[bulk_schema] = _live
                                    st.session_state["_sf_tables_map"] = _m
                            st.rerun()

                if _bulk_tbl_opts:
                    _sel_all = st.checkbox(
                        f"Select all ({len(_bulk_tbl_opts)} tables)",
                        key="ddl_bulk_sel_all",
                    )
                    _default_sel = (_bulk_tbl_opts if _sel_all
                                    else st.session_state.get("ddl_bulk_selected", []))
                    bulk_selected = st.multiselect(
                        "Tables to convert",
                        options=_bulk_tbl_opts,
                        default=_default_sel,
                        placeholder="Choose one or more tables…",
                        key="ddl_bulk_table_sel",
                    )
                    st.session_state["ddl_bulk_selected"] = bulk_selected
                    if bulk_selected:
                        st.markdown(
                            f'<span class="badge badge-pending">'
                            f'◌ {len(bulk_selected)} table(s) selected</span>',
                            unsafe_allow_html=True,
                        )
                elif bulk_schema and bulk_schema != "— connect first —":
                    st.warning(f"No tables found in schema '{bulk_schema}'.")
                    bulk_selected = []
                else:
                    bulk_selected = []

            with bulk_top_r:
                st.markdown('<div class="card-title">⚙️ Step 2 — Conversion Options</div>',
                            unsafe_allow_html=True)

                bulk_target_schema = st.text_input(
                    "Target Redshift Schema", placeholder="public", value="public",
                    key="ddl_bulk_target_schema",
                )
                bulk_use_sf_analysis = st.checkbox(
                    "Analyse query history for DISTKEY / SORTKEY",
                    value=False, key="ddl_bulk_use_sf_analysis",
                    help="Runs a QUERY_HISTORY lookup per table. Slower but better keys.",
                )
                bulk_add_header = st.checkbox(
                    "Add section header comments in output file",
                    value=True, key="ddl_bulk_add_header",
                    help="Inserts a -- ── TABLE: <name> comment block before each DDL.",
                )
                bulk_skip_errors = st.checkbox(
                    "Skip failed tables and continue batch",
                    value=True, key="ddl_bulk_skip_errors",
                )

                st.markdown("---")
                st.markdown(
                    '<div style="color:#6b7080;font-size:12px;line-height:1.9">'
                    '📌 <b style="color:#c8cad4">How it works</b><br>'
                    '1. Calls <code>GET_DDL(\'TABLE\', \'db.schema.table\')</code> per table<br>'
                    '2. Runs full type conversion + clause cleanup pipeline<br>'
                    '3. Optionally resolves DISTKEY / SORTKEY from ACCOUNT_USAGE<br>'
                    '4. Bundles all statements into one <code>.sql</code> download'
                    '</div>',
                    unsafe_allow_html=True,
                )

            # ── Step 3 · Run ──────────────────────────────────────────────────
            st.markdown("---")
            _run_c1, _run_c2 = st.columns([2, 5])
            with _run_c1:
                run_bulk = st.button(
                    "▶ Fetch & Convert",
                    key="btn_run_bulk_ddl",
                    use_container_width=True,
                    disabled=(not bool(bulk_selected)),
                )

            if run_bulk and bulk_selected:
                st.session_state.ddl_bulk_results = []
                st.session_state.ddl_bulk_logs    = []
                _progress = st.progress(0, text="Initialising…")
                _n = len(bulk_selected)

                try:
                    import snowflake.connector as _sf_mod
                    _conn = _sf_mod.connect(**_sf_cfg)
                    _cur  = _conn.cursor()
                    _conn_ok = True
                except Exception as _ce:
                    st.error(f"Cannot open Snowflake connection: {_ce}")
                    _conn_ok = False

                if _conn_ok:
                    for _idx, _tbl in enumerate(bulk_selected):
                        _progress.progress(_idx / _n,
                                           text=f"Processing {_tbl} ({_idx+1}/{_n})…")
                        _tbl_logs = []
                        try:
                            # Build fully-qualified name
                            _db   = st.session_state.get("sf_database", "")
                            _qual = (f"{_db}.{bulk_schema}.{_tbl}"
                                     if _db else f"{bulk_schema}.{_tbl}")
                            _cur.execute(f"SELECT GET_DDL('TABLE', '{_qual}')")
                            _raw_ddl = _cur.fetchone()[0]
                            _tbl_logs.append(("ok",
                                              f"GET_DDL succeeded — {len(_raw_ddl):,} chars"))

                            _conv, _dk, _sk, _clogs = _run_ddl_conversion(
                                _raw_ddl, bulk_target_schema,
                                use_sf_analysis=bulk_use_sf_analysis,
                            )
                            _tbl_logs.extend(_clogs)
                            st.session_state.ddl_bulk_results.append({
                                "table":   _tbl,
                                "sql":     _conv,
                                "distkey": _dk,
                                "sortkey": _sk,
                                "status":  "ok",
                                "logs":    _tbl_logs,
                            })
                            add_log(f"Bulk DDL: {_tbl} converted ✓", "ok")

                        except Exception as _te:
                            _tbl_logs.append(("err", f"Failed: {_te}"))
                            st.session_state.ddl_bulk_results.append({
                                "table":  _tbl,
                                "sql":    "",
                                "status": "err",
                                "logs":   _tbl_logs,
                            })
                            add_log(f"Bulk DDL: {_tbl} failed — {_te}", "err")
                            if not bulk_skip_errors:
                                _progress.progress(1.0, text="Stopped on error.")
                                break

                    try:
                        _cur.close()
                        _conn.close()
                    except Exception:
                        pass
                    _progress.progress(1.0, text=f"Done — {len(st.session_state.ddl_bulk_results)} table(s) processed.")
                    st.rerun()

            # ── Results ───────────────────────────────────────────────────────
            _bulk_res = st.session_state.get("ddl_bulk_results", [])
            if _bulk_res:
                _ok_res  = [r for r in _bulk_res if r["status"] == "ok"]
                _err_res = [r for r in _bulk_res if r["status"] == "err"]

                # Summary metrics row
                _mc1, _mc2, _mc3 = st.columns(3)
                with _mc1:
                    st.markdown(
                        f'<div style="background:#13161f;border:1px solid #1e2130;border-radius:10px;'
                        f'padding:16px;text-align:center">'
                        f'<div style="font-family:\'DM Mono\',monospace;font-size:32px;'
                        f'font-weight:600;color:#34d399">{len(_ok_res)}</div>'
                        f'<div style="font-size:11px;color:#6b7080;margin-top:4px;'
                        f'text-transform:uppercase;letter-spacing:0.8px">Converted</div></div>',
                        unsafe_allow_html=True,
                    )
                with _mc2:
                    st.markdown(
                        f'<div style="background:#13161f;border:1px solid #1e2130;border-radius:10px;'
                        f'padding:16px;text-align:center">'
                        f'<div style="font-family:\'DM Mono\',monospace;font-size:32px;'
                        f'font-weight:600;color:#f87171">{len(_err_res)}</div>'
                        f'<div style="font-size:11px;color:#6b7080;margin-top:4px;'
                        f'text-transform:uppercase;letter-spacing:0.8px">Failed</div></div>',
                        unsafe_allow_html=True,
                    )
                with _mc3:
                    st.markdown(
                        f'<div style="background:#13161f;border:1px solid #1e2130;border-radius:10px;'
                        f'padding:16px;text-align:center">'
                        f'<div style="font-family:\'DM Mono\',monospace;font-size:32px;'
                        f'font-weight:600;color:#5b8dee">{len(_bulk_res)}</div>'
                        f'<div style="font-size:11px;color:#6b7080;margin-top:4px;'
                        f'text-transform:uppercase;letter-spacing:0.8px">Total</div></div>',
                        unsafe_allow_html=True,
                    )

                st.markdown("---")

                # ── Build combined SQL file ───────────────────────────────────
                _ts = datetime.now().strftime("%Y%m%d_%H%M%S")
                _bschema = st.session_state.get("ddl_bulk_schema_sel",
                                                bulk_selected[0] if bulk_selected else "schema")
                _lines = [
                    "-- ════════════════════════════════════════════════════════════",
                    "-- Redshift DDL — Bulk Converted from Snowflake",
                    f"-- Source schema : {bulk_schema}",
                    f"-- Target schema : {bulk_target_schema}",
                    f"-- Generated     : {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
                    f"-- Tables        : {len(_ok_res)} converted · {len(_err_res)} failed",
                    "-- ════════════════════════════════════════════════════════════",
                    "",
                ]
                for _r in _ok_res:
                    if bulk_add_header:
                        _dk_n = f"DISTKEY={_r['distkey']}" if _r.get("distkey") else "DISTSTYLE AUTO"
                        _sk_n = f"SORTKEY={_r['sortkey']}" if _r.get("sortkey") else "SORTKEY AUTO"
                        _lines += [
                            "",
                            f"-- ── TABLE: {_r['table'].upper()} {'─'*max(1,48-len(_r['table']))}",
                            f"--    {_dk_n}  ·  {_sk_n}",
                            f"-- {'─'*60}",
                        ]
                    _lines.append(_r["sql"])
                    _lines.append("")

                if _err_res:
                    _lines += ["", "-- ════ FAILED TABLES (not converted) ════"]
                    for _r in _err_res:
                        _emsg = next((m for _, m in _r["logs"] if "Failed" in m), "unknown error")
                        _lines.append(f"-- ✗  {_r['table']}: {_emsg}")

                _combined = "\n".join(_lines)

                # Download + status bar
                _dl1, _dl2 = st.columns([4, 1])
                with _dl2:
                    st.download_button(
                        "⬇️ Download All DDL (.sql)",
                        data=_combined.encode("utf-8"),
                        file_name=f"redshift_bulk_{bulk_schema}_{_ts}.sql",
                        mime="text/plain",
                        use_container_width=True,
                    )
                with _dl1:
                    if _ok_res:
                        st.markdown(
                            f'<span class="badge badge-connected">● {len(_ok_res)} table(s) ready · '
                            f'{len(_combined):,} chars · schema: {bulk_target_schema}</span>',
                            unsafe_allow_html=True,
                        )
                    if _err_res:
                        st.markdown(
                            f'<span class="badge badge-disconnected">'
                            f'✗ {len(_err_res)} table(s) failed — see per-table details below'
                            f'</span>',
                            unsafe_allow_html=True,
                        )

                st.markdown("---")

                # ── Per-table accordion ───────────────────────────────────────
                st.markdown('<div class="card-title">📋 Per-Table Conversion Details</div>',
                            unsafe_allow_html=True)
                for _r in _bulk_res:
                    _icon  = "✓" if _r["status"] == "ok" else "✗"
                    _dk_b  = f'  DISTKEY={_r["distkey"]}' if _r.get("distkey") else ""
                    _sk_b  = f'  SORTKEY={_r["sortkey"]}' if _r.get("sortkey") else ""
                    _label = f'{_icon}  {_r["table"]}{_dk_b}{_sk_b}'
                    with st.expander(_label, expanded=(_r["status"] == "err")):
                        st.markdown(_render_log_box(_r["logs"]), unsafe_allow_html=True)
                        if _r["sql"]:
                            st.code(_r["sql"], language="sql")

# ─────────────────────────────────────────────────────────────────────────────
# TAB 7 · Query Migrator  (query_migrator_app_v2.py / BIQueryMigrator)
# ─────────────────────────────────────────────────────────────────────────────
with tab_qm:
    st.markdown('<div class="card-title">🔄 Snowflake → Redshift Query & Procedure Migrator</div>', unsafe_allow_html=True)
    st.caption("Convert Snowflake SELECT queries and stored procedures to Redshift-compatible SQL.")

    st.markdown("---")

    qm_tab_query, qm_tab_proc = st.tabs(["📊 Query Migration", "🔧 Stored Procedure Migration"])

    # ── Query Migration ───────────────────────────────────────────────────────
    with qm_tab_query:
        qm_col_l, qm_col_r = st.columns([1, 1], gap="large")

        with qm_col_l:
            st.markdown('<div class="card-title">❄️ Snowflake Query</div>', unsafe_allow_html=True)
            qm_query_input = st.text_area(
                "Snowflake SQL",
                height=340,
                placeholder="""SELECT
    customer_id,
    NVL(region, 'Unknown') AS region,
    COUNT(*) AS order_count,
    SUM(amount) AS total_amount
FROM orders
WHERE order_date >= DATEADD(month, -3, CURRENT_DATE())
GROUP BY customer_id, region""",
                key="qm_query_area",
                label_visibility="collapsed",
            )

        with qm_col_r:
            st.markdown('<div class="card-title">🔴 Redshift Query</div>', unsafe_allow_html=True)
            st.text_area(
                "Redshift SQL Output",
                value=st.session_state.get("qm_query_output", ""),
                height=340,
                placeholder="— converted query will appear here —",
                label_visibility="collapsed",
            )
            if st.session_state.get("qm_query_output"):
                val_q_col, _val_q_sp = st.columns([2, 3])
                with val_q_col:
                    if st.button("✔ Validate Syntax", key="btn_validate_qm_query", use_container_width=True):
                        with st.spinner("Validating…"):
                            st.session_state.qm_query_validate_result = _validate_redshift_sql(
                                st.session_state.qm_query_output
                            )
                            st.session_state.qm_query_diff_html = _render_changes_panel(
                                _diff_sql_changes(
                                    st.session_state.get("qm_query_area", ""),
                                    st.session_state.qm_query_output,
                                )
                            )
                        st.rerun()
                if st.session_state.get("qm_query_validate_result") is not None:
                    st.markdown(
                        _render_validate_result(st.session_state.qm_query_validate_result),
                        unsafe_allow_html=True,
                    )

        st.markdown("---")

        qm_run_col, qm_status_col = st.columns([2, 5])
        with qm_run_col:
            run_qm_query = st.button("▶ Convert Query", key="btn_run_qm_query", use_container_width=True)

        if run_qm_query:
            if not qm_query_input.strip():
                st.error("Please paste a Snowflake query first.")
            elif BIQueryMigrator is None:
                st.error("BIQueryMigrator not available — ensure `st_to_rs_bi_query_migration.py` is on the Python path.")
            else:
                with st.spinner("Converting query…"):
                    try:
                        migrator = BIQueryMigrator()
                        converted_q = migrator.migrate_query(qm_query_input, preview=False)
                        st.session_state.qm_query_output = converted_q
                        st.session_state.qm_query_changes = ["✓ Converted Snowflake → Redshift dialect"]
                        st.session_state.qm_query_validate_result = None  # reset on new conversion
                        st.session_state.qm_query_diff_html = ""
                        add_log("Query migrated successfully", "ok")
                    except Exception as exc:
                        import traceback
                        st.session_state.qm_query_output = ""
                        st.session_state.qm_query_changes = []
                        add_log(f"Query migration error: {exc}", "err")
                        st.error(f"Conversion error: {exc}")
                        st.code(traceback.format_exc(), language="python")
                st.rerun()

        if st.session_state.qm_query_changes:
            st.markdown("---")
            st.markdown('<div class="card-title">📋 Changes Applied</div>', unsafe_allow_html=True)
            changes_html = '<div class="log-box">'
            for ch in st.session_state.qm_query_changes:
                cls = "log-warn" if "⚠" in ch else "log-ok"
                changes_html += f'<div><span class="{cls}">{ch}</span></div>'
            changes_html += "</div>"
            st.markdown(changes_html, unsafe_allow_html=True)

        # ── What Changed diff panel ──────────────────────────────────────────
        if st.session_state.get("qm_query_output"):
            _qm_diff = st.session_state.get("qm_query_diff_html") or _render_changes_panel(
                _diff_sql_changes(
                    st.session_state.get("qm_query_area", ""),
                    st.session_state.qm_query_output,
                )
            )
            if _qm_diff:
                st.markdown("---")
                st.markdown('<div class="card-title">🔍 What Changed (Snowflake → Redshift)</div>',
                            unsafe_allow_html=True)
                st.markdown(_qm_diff, unsafe_allow_html=True)

        if st.session_state.qm_query_output:
            col_qdl1, col_qdl2 = st.columns([5, 1])
            with col_qdl2:
                st.download_button(
                    "⬇️ Download (.sql)",
                    data=st.session_state.qm_query_output.encode(),
                    file_name=f"redshift_query_{datetime.now().strftime('%Y%m%d_%H%M%S')}.sql",
                    mime="text/plain",
                    use_container_width=True,
                )

    # ── Stored Procedure Migration ────────────────────────────────────────────
    with qm_tab_proc:
        if StoredProcedureMigrator is None:
            st.markdown(
                '<span class="badge badge-disconnected">○ StoredProcedureMigrator unavailable — '
                'ensure st_to_rs_bi_query_migration.py is present</span>',
                unsafe_allow_html=True,
            )
        else:
            qm_proc_col_l, qm_proc_col_r = st.columns([1, 1], gap="large")

            with qm_proc_col_l:
                st.markdown('<div class="card-title">❄️ Snowflake Procedure</div>', unsafe_allow_html=True)
                qm_proc_input = st.text_area(
                    "Snowflake Stored Procedure",
                    height=340,
                    placeholder="""CREATE OR REPLACE PROCEDURE sp_process_orders(p_date DATE)
RETURNS VARCHAR AS
$$
DECLARE
    v_count INTEGER;
BEGIN
    SELECT COUNT(*) INTO v_count FROM orders WHERE order_date = p_date;
    RETURN 'Processed: ' || v_count;
END;
$$
LANGUAGE SQL;""",
                    key="qm_proc_area",
                    label_visibility="collapsed",
                )

            with qm_proc_col_r:
                st.markdown('<div class="card-title">🔴 Redshift Procedure</div>', unsafe_allow_html=True)
                st.text_area(
                    "Redshift Procedure Output",
                    value=st.session_state.get("qm_proc_output", ""),
                    height=340,
                    placeholder="— converted procedure will appear here —",
                    label_visibility="collapsed",
                )
                if st.session_state.get("qm_proc_output"):
                    val_p_col, _val_p_sp = st.columns([2, 3])
                    with val_p_col:
                        if st.button("✔ Validate Syntax", key="btn_validate_qm_proc", use_container_width=True):
                            with st.spinner("Validating…"):
                                st.session_state.qm_proc_validate_result = _validate_redshift_sql(
                                    st.session_state.qm_proc_output
                                )
                                st.session_state.qm_proc_diff_html = _render_changes_panel(
                                    _diff_sql_changes(
                                        st.session_state.get("qm_proc_area", ""),
                                        st.session_state.qm_proc_output,
                                    )
                                )
                            st.rerun()
                    if st.session_state.get("qm_proc_validate_result") is not None:
                        st.markdown(
                            _render_validate_result(st.session_state.qm_proc_validate_result),
                            unsafe_allow_html=True,
                        )

            st.markdown("---")

            qm_proc_run_col, qm_proc_status_col = st.columns([2, 5])
            with qm_proc_run_col:
                run_qm_proc = st.button("▶ Convert Procedure", key="btn_run_qm_proc", use_container_width=True)

            if run_qm_proc:
                if not qm_proc_input.strip():
                    st.error("Please paste a Snowflake stored procedure first.")
                else:
                    with st.spinner("Converting procedure…"):
                        try:
                            proc_migrator = StoredProcedureMigrator()
                            if proc_migrator.is_stored_procedure(qm_proc_input):
                                converted_proc = proc_migrator.migrate_procedure(qm_proc_input, preview=False)
                                st.session_state.qm_proc_output = converted_proc
                                st.session_state.qm_proc_validate_result = None  # reset on new conversion
                                st.session_state.qm_proc_diff_html = ""
                                add_log("Stored procedure migrated successfully", "ok")
                            else:
                                st.error("Input does not appear to be a Snowflake stored procedure. Ensure it starts with CREATE [OR REPLACE] PROCEDURE.")
                        except Exception as exc:
                            st.session_state.qm_proc_output = ""
                            add_log(f"Procedure migration error: {exc}", "err")
                            st.error(f"Conversion error: {exc}")
                    st.rerun()

            if st.session_state.qm_proc_output:
                st.markdown("---")
                st.markdown(
                    '<span class="badge badge-pending">◌ Review error handling and transaction control before deploying</span>',
                    unsafe_allow_html=True,
                )
                col_pdl1, col_pdl2 = st.columns([5, 1])
                with col_pdl2:
                    st.download_button(
                        "⬇️ Download (.sql)",
                        data=st.session_state.qm_proc_output.encode(),
                        file_name=f"redshift_proc_{datetime.now().strftime('%Y%m%d_%H%M%S')}.sql",
                        mime="text/plain",
                        use_container_width=True,
                    )

                # ── What Changed diff panel ──────────────────────────────────
                _proc_diff = st.session_state.get("qm_proc_diff_html") or _render_changes_panel(
                    _diff_sql_changes(
                        st.session_state.get("qm_proc_area", ""),
                        st.session_state.qm_proc_output,
                    )
                )
                if _proc_diff:
                    st.markdown("---")
                    st.markdown('<div class="card-title">🔍 What Changed (Snowflake → Redshift)</div>',
                                unsafe_allow_html=True)
                    st.markdown(_proc_diff, unsafe_allow_html=True)









st.markdown("---")
st.markdown("""
<div style="text-align:center; color:#3a4255; font-size:12px; font-family:'DM Mono',monospace; padding:8px 0">
    DataBridge Pipeline Studio &nbsp;·&nbsp; Snowflake ❄️ → S3 ☁️ → Redshift 🔴
</div>
""", unsafe_allow_html=True)
# ─────────────────────────────────────────────────────────────────────────────
# TAB 9 · Snowflake Health Check
# ─────────────────────────────────────────────────────────────────────────────
with tab_hc:
    st.markdown('<div class="card-title">❄️ Snowflake Account Health Check</div>', unsafe_allow_html=True)
    st.caption("Live diagnostics pulled from SNOWFLAKE.ACCOUNT_USAGE — warehouse load, query performance, security, pipeline health and more.")
    st.markdown("---")

    # ── Connection banner ─────────────────────────────────────────────────────
    hc_col_conn, hc_col_btn = st.columns([4, 1])
    with hc_col_conn:
        if SnowparkSession is None:
            st.markdown('<span class="badge badge-disconnected">○ snowflake-snowpark-python not installed</span>', unsafe_allow_html=True)
        elif not st.session_state.hc_connected:
            st.markdown('<span class="badge badge-pending">◌ Uses sidebar Snowflake credentials — click Connect below</span>', unsafe_allow_html=True)
        else:
            st.markdown('<span class="badge badge-connected">● Snowpark session active</span>', unsafe_allow_html=True)

    with hc_col_btn:
        if SnowparkSession is not None and not st.session_state.hc_connected:
            if st.button("▶ Connect", key="btn_hc_connect", use_container_width=True):
                with st.spinner("Opening Snowpark session…"):
                    try:
                        params = {
                            "account":   sf_account,
                            "user":      sf_user,
                            "password":  sf_password,
                            "database":  sf_database,
                            "warehouse": sf_warehouse,
                        }
                        if sf_role:
                            params["role"] = sf_role
                        _hc_sess = SnowparkSession.builder.configs(params).create()
                        st.session_state.hc_session = _hc_sess
                        st.session_state.hc_connected = True
                        st.session_state.hc_error = None
                        add_log("Snowpark session opened for Health Check", "ok")
                    except Exception as _e:
                        st.session_state.hc_error = str(_e)
                        add_log(f"Health Check connect error: {_e}", "err")
                st.rerun()
        elif st.session_state.hc_connected:
            if st.button("⏹ Disconnect", key="btn_hc_disconnect", use_container_width=True):
                try:
                    st.session_state.hc_session.close()
                except Exception:
                    pass
                st.session_state.hc_session = None
                st.session_state.hc_connected = False
                st.rerun()

    if st.session_state.get("hc_error"):
        st.error(f"Connection error: {st.session_state.hc_error}")

    st.markdown("---")

    # ── Helper: run query and render themed dataframe ─────────────────────────
    def hc_run_query(query, label=None):
        if not st.session_state.hc_connected or st.session_state.hc_session is None:
            st.markdown(
                f'<span class="badge badge-pending">◌ {label or "Query"} — connect first</span>',
                unsafe_allow_html=True,
            )
            return None
        try:
            df = st.session_state.hc_session.sql(query).to_pandas()
            if label:
                st.markdown(f'<div class="card-title">📋 {label}</div>', unsafe_allow_html=True)
            num_cols = df.select_dtypes(include="number").columns.tolist()
            styled = df.style.set_properties(**{
                "font-size": "13px",
                "font-family": "'DM Mono', monospace",
                "text-align": "center",
            })
            if num_cols:
                styled = (
                    styled
                    .background_gradient(cmap="Blues", subset=num_cols)
                    .highlight_max(color="#1a3a5c", subset=num_cols, axis=0)
                    .highlight_min(color="#e18484", subset=num_cols, axis=0)
                )
            st.dataframe(styled, use_container_width=True)
            return df
        except Exception as exc:
            st.markdown(
                f'<span class="badge badge-disconnected">✗ {label}: {exc}</span>',
                unsafe_allow_html=True,
            )
            add_log(f"Health check query failed [{label}]: {exc}", "err")
            return None

    # ── Sub-tabs ──────────────────────────────────────────────────────────────
    hc_t1, hc_t2, hc_t3, hc_t4, hc_t5, hc_t6, hc_t7 = st.tabs([
        "📊 Account Overview",
        "🏢 Warehouse Health",
        "🔍 Query Performance",
        "📦 Table & Schema",
        "🔄 Pipeline Health",
        "🔒 Security & RBAC",
        "🎯 Executive Insights",
    ])

    # ── 1 · Account Overview ──────────────────────────────────────────────────
    with hc_t1:
        st.markdown('<div class="card-title">🌐 Account Overview</div>', unsafe_allow_html=True)
        ov_c1, ov_c2 = st.columns(2)
        with ov_c1:
            hc_run_query("""
                SELECT CURRENT_ACCOUNT() AS ACCOUNT_NAME,
                       CURRENT_REGION() AS REGION,
                       CURRENT_VERSION() AS SNOWFLAKE_VERSION,
                       CURRENT_ACCOUNT_NAME() AS ACCOUNT_LOCATOR
            """, "Account Information")
        with ov_c2:
            hc_run_query("""
                SELECT
                  (SELECT COUNT(*) FROM SNOWFLAKE.ACCOUNT_USAGE.DATABASES) AS DATABASES,
                  (SELECT COUNT(*) FROM SNOWFLAKE.ACCOUNT_USAGE.TABLES)    AS TABLES,
                  (SELECT COUNT(*) FROM SNOWFLAKE.ACCOUNT_USAGE.VIEWS)     AS VIEWS,
                  (SELECT COUNT(*) FROM SNOWFLAKE.ACCOUNT_USAGE.USERS)     AS USERS,
                  (SELECT COUNT(*) FROM SNOWFLAKE.ACCOUNT_USAGE.ROLES)     AS ROLES
            """, "Object Counts")
        st.markdown("---")
        hc_run_query("""
            SELECT USER_NAME, EVENT_TIMESTAMP, ERROR_MESSAGE
            FROM SNOWFLAKE.ACCOUNT_USAGE.LOGIN_HISTORY
            WHERE IS_SUCCESS = 'NO'
            ORDER BY EVENT_TIMESTAMP DESC
        """, "Login Failures")
        hc_run_query("""
            SELECT DATE_TRUNC('DAY', START_TIME) AS DAY,
                   SUM(CREDITS_USED) AS TOTAL_CREDITS
            FROM SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY
            GROUP BY 1 ORDER BY 1 DESC
        """, "Cost Summary (Daily)")
        hc_run_query("""
            SELECT SERVICE_TYPE, SUM(CREDITS_USED) AS CREDITS
            FROM SNOWFLAKE.ACCOUNT_USAGE.METERING_HISTORY
            GROUP BY SERVICE_TYPE ORDER BY CREDITS DESC
        """, "Credits by Feature")
        with st.expander("💰 Query Cost Analysis (Last 7 Days)"):
            st.markdown("""
**Assumptions:**
- 1 Snowflake credit = **$2.00 USD** (fixed multiplier)
- Cost estimation = **Credits used per hour × 2.0**

**Note:** This is an approximation — Snowflake does not expose per-query cost directly.
            """)
            hc_run_query("""
                WITH account_info AS (
                    SELECT CURRENT_ACCOUNT() AS account_name, CURRENT_REGION() AS account_region
                ),
                query_usage AS (
                    SELECT QUERY_ID, USER_NAME, WAREHOUSE_NAME,
                           TOTAL_ELAPSED_TIME AS elapsed_ms, START_TIME
                    FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
                    WHERE START_TIME >= DATEADD(DAY, -7, CURRENT_TIMESTAMP())
                ),
                warehouse_cost AS (
                    SELECT WAREHOUSE_NAME, START_TIME, CREDITS_USED
                    FROM SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY
                    WHERE START_TIME >= DATEADD(DAY, -7, CURRENT_TIMESTAMP())
                )
                SELECT q.QUERY_ID, q.USER_NAME, a.account_name,
                    CASE
                        WHEN a.account_region ILIKE '%AWS_US_EAST_1%' THEN 'US East (N. Virginia)'
                        WHEN a.account_region ILIKE '%AWS_US_WEST_2%' THEN 'US West (Oregon)'
                        WHEN a.account_region ILIKE '%AWS_EU_WEST_1%' THEN 'EU (Ireland)'
                        WHEN a.account_region ILIKE '%AZURE%' THEN 'Azure Region'
                        WHEN a.account_region ILIKE '%GCP%'   THEN 'GCP Region'
                        ELSE 'Other Region'
                    END AS region_name,
                    COALESCE(w.CREDITS_USED, 0) * 2.0 AS estimated_cost_usd,
                    q.elapsed_ms
                FROM query_usage q
                LEFT JOIN warehouse_cost w
                    ON q.WAREHOUSE_NAME = w.WAREHOUSE_NAME
                    AND DATE_TRUNC('HOUR', q.START_TIME) = DATE_TRUNC('HOUR', w.START_TIME)
                JOIN account_info a ON TRUE
                ORDER BY q.START_TIME DESC LIMIT 100
            """, "Query Cost Breakdown")

    # ── 2 · Warehouse Health ──────────────────────────────────────────────────
    with hc_t2:
        st.markdown('<div class="card-title">🏢 Warehouse Health</div>', unsafe_allow_html=True)
        wh_c1, wh_c2 = st.columns(2)
        with wh_c1:
            hc_run_query("""
                SELECT WAREHOUSE_NAME,
                       AVG(AVG_RUNNING) AS AVG_LOAD,
                       MAX(AVG_RUNNING) AS PEAK_LOAD
                FROM SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_LOAD_HISTORY
                GROUP BY 1
            """, "Warehouse Utilization")
        with wh_c2:
            hc_run_query("""
                SELECT WAREHOUSE_NAME,
                       ROUND(AVG(AVG_RUNNING), 3)     AS AVG_RUNNING,
                       ROUND(MAX(AVG_RUNNING), 3)     AS PEAK_RUNNING,
                       ROUND(AVG(AVG_QUEUED_LOAD), 3) AS AVG_QUEUE_LOAD,
                       ROUND(MAX(AVG_QUEUED_LOAD), 3) AS PEAK_QUEUE_LOAD
                FROM SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_LOAD_HISTORY
                WHERE START_TIME >= DATEADD(DAY, -7, CURRENT_TIMESTAMP())
                GROUP BY WAREHOUSE_NAME
            """, "Queue & Blocking (7 days)")
        st.markdown("---")
        hc_run_query("""
            SELECT WAREHOUSE_NAME,
                   ROUND(AVG(AVG_RUNNING), 3) AS AVG_RUNNING,
                   CASE WHEN AVG(AVG_RUNNING) < 0.2  THEN 'OVER-PROVISIONED'
                        WHEN AVG(AVG_RUNNING) <= 0.7 THEN 'OPTIMAL'
                        ELSE 'UNDER-PROVISIONED' END AS UTILIZATION_STATUS,
                   ROUND(MAX(AVG_QUEUED_LOAD), 3) AS PEAK_QUEUE,
                   CASE WHEN MAX(AVG_QUEUED_LOAD) > 0 THEN 'CONCURRENCY ISSUE'
                        ELSE 'NO QUEUE' END AS QUEUE_STATUS
            FROM SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_LOAD_HISTORY
            WHERE START_TIME >= DATEADD(DAY, -7, CURRENT_TIMESTAMP())
            GROUP BY WAREHOUSE_NAME
        """, "Utilization Status")
        hc_run_query("""
            SELECT WAREHOUSE_NAME,
                   SUM(CREDITS_USED) - SUM(CREDITS_USED_CLOUD_SERVICES) AS COMPUTE_CREDITS
            FROM SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY
            GROUP BY 1
        """, "Idle Warehouse Credits")

    # ── 3 · Query Performance ─────────────────────────────────────────────────
    with hc_t3:
        st.markdown('<div class="card-title">🔍 Query Performance</div>', unsafe_allow_html=True)
        hc_run_query("""
            SELECT QUERY_ID, USER_NAME, WAREHOUSE_NAME, EXECUTION_STATUS,
                   TOTAL_ELAPSED_TIME/1000      AS EXEC_TIME_SEC,
                   COMPILATION_TIME/1000        AS COMPILATION_SEC,
                   BYTES_SCANNED/1024/1024/1024 AS GB_SCANNED
            FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
            WHERE START_TIME >= DATEADD(DAY, -7, CURRENT_TIMESTAMP())
            ORDER BY EXEC_TIME_SEC DESC LIMIT 50
        """, "Slow Queries (Last 7 Days)")
        st.markdown("---")
        hc_run_query("""
            SELECT QUERY_ID, QUERY_TEXT
            FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
            WHERE QUERY_TEXT ILIKE '%SELECT *%'
               OR QUERY_TEXT ILIKE '%CROSS JOIN%'
            LIMIT 100
        """, "Query Antipatterns")
        hc_run_query("""
            SELECT QUERY_ID,
                   BYTES_SPILLED_TO_LOCAL_STORAGE,
                   BYTES_SPILLED_TO_REMOTE_STORAGE
            FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
            WHERE BYTES_SPILLED_TO_REMOTE_STORAGE > 0
            ORDER BY BYTES_SPILLED_TO_REMOTE_STORAGE DESC
        """, "Query Spills")

    # ── 4 · Table & Schema ────────────────────────────────────────────────────
    with hc_t4:
        st.markdown('<div class="card-title">📦 Table & Schema</div>', unsafe_allow_html=True)
        ts_c1, ts_c2 = st.columns(2)
        with ts_c1:
            hc_run_query("""
                SELECT TABLE_SCHEMA, TABLE_NAME,
                       BYTES/1024/1024/1024 AS SIZE_GB
                FROM SNOWFLAKE.ACCOUNT_USAGE.TABLES
                ORDER BY SIZE_GB DESC LIMIT 50
            """, "Largest Tables")
        with ts_c2:
            hc_run_query("""
                SELECT TABLE_NAME,
                       DATE_TRUNC('DAY', CREATED) AS CREATED_DATE,
                       BYTES/1024/1024/1024 AS SIZE_GB
                FROM SNOWFLAKE.ACCOUNT_USAGE.TABLES
                ORDER BY CREATED_DATE DESC
            """, "Table Growth")
        st.markdown("---")
        hc_run_query("""
            SELECT TABLE_NAME, CLUSTERING_KEY
            FROM SNOWFLAKE.ACCOUNT_USAGE.TABLES
            WHERE CLUSTERING_KEY IS NOT NULL
        """, "Clustering Health")
        hc_run_query("""
            SELECT QUERY_ID, PARTITIONS_SCANNED, PARTITIONS_TOTAL,
                   (PARTITIONS_SCANNED/NULLIF(PARTITIONS_TOTAL,0))*100 AS SCAN_PERCENT
            FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
            WHERE PARTITIONS_SCANNED > 0
            ORDER BY SCAN_PERCENT DESC
        """, "Poor Pruning Queries")
        hc_run_query("""
            SELECT TABLE_SCHEMA,
                   SUM(BYTES)/1024/1024/1024 AS SIZE_GB
            FROM SNOWFLAKE.ACCOUNT_USAGE.TABLES
            GROUP BY TABLE_SCHEMA ORDER BY SIZE_GB DESC
        """, "Schema Growth")

    # ── 5 · Pipeline Health ───────────────────────────────────────────────────
    with hc_t5:
        st.markdown('<div class="card-title">🔄 Pipeline Health</div>', unsafe_allow_html=True)
        ph_c1, ph_c2 = st.columns(2)
        with ph_c1:
            hc_run_query("""
                SELECT PIPE_NAME, SUM(CREDITS_USED) AS TOTAL_CREDITS,
                       SUM(BYTES_INSERTED)/1024/1024/1024 AS GB_INSERTED
                FROM SNOWFLAKE.ACCOUNT_USAGE.PIPE_USAGE_HISTORY
                WHERE START_TIME >= DATEADD(DAY, -7, CURRENT_TIMESTAMP())
                GROUP BY PIPE_NAME ORDER BY TOTAL_CREDITS DESC
            """, "Pipe Cost (7 days)")
        with ph_c2:
            hc_run_query("""
                SELECT PIPE_NAME,
                       AVG(BYTES_INSERTED/NULLIF(FILES_INSERTED,0)) AS AVG_FILE_SIZE_BYTES,
                       SUM(FILES_INSERTED) AS TOTAL_FILES
                FROM SNOWFLAKE.ACCOUNT_USAGE.PIPE_USAGE_HISTORY
                WHERE START_TIME >= DATEADD(DAY, -7, CURRENT_TIMESTAMP())
                GROUP BY PIPE_NAME
                HAVING AVG_FILE_SIZE_BYTES < 10*1024*1024
            """, "Pipe Small Files")
        st.markdown("---")
        hc_run_query("""
            SELECT PIPE_NAME,
                   DATE_TRUNC('HOUR', START_TIME) AS HOUR,
                   SUM(BYTES_INSERTED)/1024/1024/1024 AS GB_LOADED
            FROM SNOWFLAKE.ACCOUNT_USAGE.PIPE_USAGE_HISTORY
            WHERE START_TIME >= DATEADD(DAY, -7, CURRENT_TIMESTAMP())
            GROUP BY PIPE_NAME, HOUR
        """, "Pipe Trend")
        hc_run_query("""
            SELECT PIPE_NAME, MAX(END_TIME) AS LAST_LOAD_TIME
            FROM SNOWFLAKE.ACCOUNT_USAGE.PIPE_USAGE_HISTORY
            GROUP BY PIPE_NAME
            HAVING LAST_LOAD_TIME < DATEADD(DAY, -1, CURRENT_TIMESTAMP())
        """, "Idle Pipes")

    # ── 6 · Security & RBAC ───────────────────────────────────────────────────
    with hc_t6:
        st.markdown('<div class="card-title">🔒 Security & RBAC</div>', unsafe_allow_html=True)
        sec_c1, sec_c2 = st.columns(2)
        with sec_c1:
            hc_run_query("""
                SELECT NAME, LAST_SUCCESS_LOGIN
                FROM SNOWFLAKE.ACCOUNT_USAGE.USERS
                WHERE LAST_SUCCESS_LOGIN < DATEADD(DAY, -30, CURRENT_TIMESTAMP())
            """, "Unused Users (30+ days)")
        with sec_c2:
            hc_run_query("""
                SELECT PIPE_NAME, MAX(END_TIME) AS LAST_LOAD_TIME
                FROM SNOWFLAKE.ACCOUNT_USAGE.PIPE_USAGE_HISTORY
                GROUP BY PIPE_NAME
                HAVING LAST_LOAD_TIME < DATEADD(DAY, -1, CURRENT_TIMESTAMP())
            """, "Idle Pipes")
        st.markdown("---")
        hc_run_query("""
            SELECT QUERY_ID, ERROR_MESSAGE
            FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
            WHERE ERROR_MESSAGE IS NOT NULL AND QUERY_TYPE = 'CALL'
        """, "Stored Procedure Failures")
        hc_run_query("""
            SELECT TABLE_NAME, MAX(LAST_ALTERED) AS LAST_UPDATED
            FROM SNOWFLAKE.ACCOUNT_USAGE.TABLES
            GROUP BY TABLE_NAME
            HAVING LAST_UPDATED < DATEADD(HOUR, -24, CURRENT_TIMESTAMP())
        """, "Data Staleness (24+ hours)")
        hc_run_query("SHOW RESOURCE MONITORS", "Resource Monitors")
        st.markdown("---")
        st.markdown(
            '<span class="badge badge-connected">✓ Health Check Complete</span>',
            unsafe_allow_html=True,
        )
    # ── 7 · Executive Insights ────────────────────────────────────────────────
    with hc_t7:
        st.markdown('<div class="card-title">🎯 Executive Insights — Snowflake Account Health Summary</div>', unsafe_allow_html=True)
        st.caption("Aggregated signals from all health-check dimensions, distilled into board-ready KPIs, risk flags, and actionable recommendations.")
        st.markdown("---")

        if not st.session_state.hc_connected or st.session_state.hc_session is None:
            st.markdown(
                '<span class="badge badge-pending">◌ Connect Snowflake via the sidebar, then click ▶ Connect above to load Executive Insights</span>',
                unsafe_allow_html=True,
            )
        else:
            # ── Helper: safe query → scalar / df ─────────────────────────────
            def _hc_scalar(sql, fallback=None):
                try:
                    df = st.session_state.hc_session.sql(sql).to_pandas()
                    if df.empty:
                        return fallback
                    return df.iloc[0, 0]
                except Exception:
                    return fallback

            def _hc_df(sql):
                try:
                    return st.session_state.hc_session.sql(sql).to_pandas()
                except Exception:
                    return pd.DataFrame()

            # ── Fetch all KPI data ────────────────────────────────────────────
            with st.spinner("Pulling account-wide signals…"):
                # Cost
                total_credits_7d = _hc_scalar("""
                    SELECT ROUND(SUM(CREDITS_USED), 2)
                    FROM SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY
                    WHERE START_TIME >= DATEADD(DAY, -7, CURRENT_TIMESTAMP())
                """, 0)
                est_cost_7d = round((total_credits_7d or 0) * 2.0, 2)

                total_credits_30d = _hc_scalar("""
                    SELECT ROUND(SUM(CREDITS_USED), 2)
                    FROM SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY
                    WHERE START_TIME >= DATEADD(DAY, -30, CURRENT_TIMESTAMP())
                """, 0)
                est_cost_30d = round((total_credits_30d or 0) * 2.0, 2)

                # Warehouse efficiency
                over_prov_count = _hc_scalar("""
                    SELECT COUNT(*)
                    FROM (
                        SELECT WAREHOUSE_NAME, AVG(AVG_RUNNING) AS AVG_RUN
                        FROM SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_LOAD_HISTORY
                        WHERE START_TIME >= DATEADD(DAY, -7, CURRENT_TIMESTAMP())
                        GROUP BY WAREHOUSE_NAME
                        HAVING AVG_RUN < 0.2
                    )
                """, 0)

                under_prov_count = _hc_scalar("""
                    SELECT COUNT(*)
                    FROM (
                        SELECT WAREHOUSE_NAME, AVG(AVG_RUNNING) AS AVG_RUN
                        FROM SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_LOAD_HISTORY
                        WHERE START_TIME >= DATEADD(DAY, -7, CURRENT_TIMESTAMP())
                        GROUP BY WAREHOUSE_NAME
                        HAVING AVG_RUN > 0.7
                    )
                """, 0)

                wh_queue_issues = _hc_scalar("""
                    SELECT COUNT(*)
                    FROM (
                        SELECT WAREHOUSE_NAME
                        FROM SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_LOAD_HISTORY
                        WHERE START_TIME >= DATEADD(DAY, -7, CURRENT_TIMESTAMP())
                        GROUP BY WAREHOUSE_NAME
                        HAVING MAX(AVG_QUEUED_LOAD) > 0.5
                    )
                """, 0)

                # Query health
                slow_query_count = _hc_scalar("""
                    SELECT COUNT(*)
                    FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
                    WHERE START_TIME >= DATEADD(DAY, -7, CURRENT_TIMESTAMP())
                      AND TOTAL_ELAPSED_TIME > 300000
                """, 0)

                spill_count = _hc_scalar("""
                    SELECT COUNT(*)
                    FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
                    WHERE BYTES_SPILLED_TO_REMOTE_STORAGE > 0
                      AND START_TIME >= DATEADD(DAY, -7, CURRENT_TIMESTAMP())
                """, 0)

                antipattern_count = _hc_scalar("""
                    SELECT COUNT(*)
                    FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
                    WHERE START_TIME >= DATEADD(DAY, -7, CURRENT_TIMESTAMP())
                      AND (QUERY_TEXT ILIKE '%SELECT *%' OR QUERY_TEXT ILIKE '%CROSS JOIN%')
                """, 0)

                # Security
                unused_users = _hc_scalar("""
                    SELECT COUNT(*)
                    FROM SNOWFLAKE.ACCOUNT_USAGE.USERS
                    WHERE LAST_SUCCESS_LOGIN < DATEADD(DAY, -30, CURRENT_TIMESTAMP())
                       OR LAST_SUCCESS_LOGIN IS NULL
                """, 0)

                login_failures_7d = _hc_scalar("""
                    SELECT COUNT(*)
                    FROM SNOWFLAKE.ACCOUNT_USAGE.LOGIN_HISTORY
                    WHERE IS_SUCCESS = 'NO'
                      AND EVENT_TIMESTAMP >= DATEADD(DAY, -7, CURRENT_TIMESTAMP())
                """, 0)

                sp_failures = _hc_scalar("""
                    SELECT COUNT(*)
                    FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
                    WHERE ERROR_MESSAGE IS NOT NULL AND QUERY_TYPE = 'CALL'
                      AND START_TIME >= DATEADD(DAY, -7, CURRENT_TIMESTAMP())
                """, 0)

                # Storage & tables
                total_tables = _hc_scalar("""
                    SELECT COUNT(*) FROM SNOWFLAKE.ACCOUNT_USAGE.TABLES
                """, 0)

                stale_tables = _hc_scalar("""
                    SELECT COUNT(*)
                    FROM (
                        SELECT TABLE_NAME, MAX(LAST_ALTERED) AS LAST_UPDATED
                        FROM SNOWFLAKE.ACCOUNT_USAGE.TABLES
                        GROUP BY TABLE_NAME
                        HAVING LAST_UPDATED < DATEADD(HOUR, -72, CURRENT_TIMESTAMP())
                    )
                """, 0)

                clustered_tables = _hc_scalar("""
                    SELECT COUNT(*)
                    FROM SNOWFLAKE.ACCOUNT_USAGE.TABLES
                    WHERE CLUSTERING_KEY IS NOT NULL
                """, 0)

                # Pipeline
                idle_pipes = _hc_scalar("""
                    SELECT COUNT(*)
                    FROM (
                        SELECT PIPE_NAME, MAX(END_TIME) AS LAST_LOAD
                        FROM SNOWFLAKE.ACCOUNT_USAGE.PIPE_USAGE_HISTORY
                        GROUP BY PIPE_NAME
                        HAVING LAST_LOAD < DATEADD(DAY, -7, CURRENT_TIMESTAMP())
                    )
                """, 0)

                small_file_pipes = _hc_scalar("""
                    SELECT COUNT(*)
                    FROM (
                        SELECT PIPE_NAME,
                               AVG(BYTES_INSERTED / NULLIF(FILES_INSERTED, 0)) AS AVG_FILE_SIZE
                        FROM SNOWFLAKE.ACCOUNT_USAGE.PIPE_USAGE_HISTORY
                        WHERE START_TIME >= DATEADD(DAY, -7, CURRENT_TIMESTAMP())
                        GROUP BY PIPE_NAME
                        HAVING AVG_FILE_SIZE < 1024 * 1024
                    )
                """, 0)

                # Top cost warehouse
                df_top_wh = _hc_df("""
                    SELECT WAREHOUSE_NAME, ROUND(SUM(CREDITS_USED), 2) AS CREDITS
                    FROM SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY
                    WHERE START_TIME >= DATEADD(DAY, -7, CURRENT_TIMESTAMP())
                    GROUP BY WAREHOUSE_NAME ORDER BY CREDITS DESC LIMIT 5
                """)

                # Daily spend trend (7d)
                df_daily_spend = _hc_df("""
                    SELECT DATE_TRUNC('DAY', START_TIME) AS DAY,
                           ROUND(SUM(CREDITS_USED), 2) AS CREDITS
                    FROM SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY
                    WHERE START_TIME >= DATEADD(DAY, -7, CURRENT_TIMESTAMP())
                    GROUP BY 1 ORDER BY 1 ASC
                """)

                # Top spenders
                df_top_users = _hc_df("""
                    SELECT USER_NAME, COUNT(*) AS QUERY_COUNT,
                           ROUND(SUM(TOTAL_ELAPSED_TIME)/1000/60, 1) AS TOTAL_EXEC_MIN
                    FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
                    WHERE START_TIME >= DATEADD(DAY, -7, CURRENT_TIMESTAMP())
                    GROUP BY USER_NAME ORDER BY TOTAL_EXEC_MIN DESC LIMIT 8
                """)

            # ── Score computation ─────────────────────────────────────────────
            # Each dimension scored 0–100. Lower risk = higher score.
            score_cost       = 100 if (total_credits_7d or 0) < 500  else (70 if (total_credits_7d or 0) < 2000 else 40)
            score_wh         = max(0, 100 - int(over_prov_count or 0) * 15 - int(under_prov_count or 0) * 20 - int(wh_queue_issues or 0) * 10)
            score_query      = max(0, 100 - int(slow_query_count or 0) * 2 - int(spill_count or 0) * 3 - int(antipattern_count or 0))
            score_security   = max(0, 100 - int(unused_users or 0) * 5 - int(login_failures_7d or 0) * 2 - int(sp_failures or 0) * 10)
            score_pipeline   = max(0, 100 - int(idle_pipes or 0) * 20 - int(small_file_pipes or 0) * 15)
            score_storage    = max(0, 100 - int(stale_tables or 0) * 2)
            overall_score    = round((score_cost + score_wh + score_query + score_security + score_pipeline + score_storage) / 6)

            def _score_color(s):
                if s >= 80: return "#34d399"
                if s >= 55: return "#fbbf24"
                return "#f87171"

            def _score_label(s):
                if s >= 80: return "HEALTHY"
                if s >= 55: return "NEEDS ATTENTION"
                return "AT RISK"

            # ── Overall Health Score banner ───────────────────────────────────
            oc = _score_color(overall_score)
            ol = _score_label(overall_score)
            st.markdown(f"""
            <div style="background:#1c2233;border:1px solid #2a3148;border-radius:16px;padding:28px 32px;margin-bottom:24px;display:flex;align-items:center;gap:32px">
                <div style="text-align:center;min-width:110px">
                    <div style="font-family:'DM Mono',monospace;font-size:56px;font-weight:600;color:{oc};line-height:1">{overall_score}</div>
                    <div style="font-size:10px;letter-spacing:1.2px;color:#4a5068;text-transform:uppercase;margin-top:4px">Overall Score</div>
                </div>
                <div style="flex:1">
                    <div style="font-size:22px;font-weight:600;color:{oc};margin-bottom:6px">{ol}</div>
                    <div style="font-size:13px;color:#6b7080;line-height:1.7">
                        Composite health score across <strong style="color:#a0aabb">Cost</strong>,
                        <strong style="color:#a0aabb">Warehouse Efficiency</strong>,
                        <strong style="color:#a0aabb">Query Performance</strong>,
                        <strong style="color:#a0aabb">Security</strong>,
                        <strong style="color:#a0aabb">Pipeline Health</strong> and
                        <strong style="color:#a0aabb">Data Freshness</strong>.
                        Score above 80 = healthy · 55–79 = needs attention · below 55 = at risk.
                    </div>
                </div>
                <div style="display:flex;flex-direction:column;gap:8px;min-width:160px;text-align:right">
                    <div style="font-size:11px;color:#6b7080">Estimated Cost (7d)</div>
                    <div style="font-family:'DM Mono',monospace;font-size:26px;color:#5b8dee;font-weight:500">${est_cost_7d:,.2f}</div>
                    <div style="font-size:11px;color:#6b7080">Est. Cost (30d): <span style="color:#a78bfa">${est_cost_30d:,.2f}</span></div>
                </div>
            </div>
            """, unsafe_allow_html=True)

            # ── Dimension score cards ─────────────────────────────────────────
            st.markdown('<div class="card-title">📐 Health Dimensions</div>', unsafe_allow_html=True)
            dims = [
                ("💰 Cost Efficiency",      score_cost,     f"{total_credits_7d} credits used (7d)"),
                ("🏢 Warehouse",            score_wh,       f"{int(over_prov_count or 0)} over-prov · {int(under_prov_count or 0)} under-prov · {int(wh_queue_issues or 0)} queue issues"),
                ("🔍 Query Health",         score_query,    f"{int(slow_query_count or 0)} slow · {int(spill_count or 0)} spills · {int(antipattern_count or 0)} antipatterns"),
                ("🔒 Security",             score_security, f"{int(unused_users or 0)} unused users · {int(login_failures_7d or 0)} login fails · {int(sp_failures or 0)} SP errors"),
                ("🔄 Pipeline",             score_pipeline, f"{int(idle_pipes or 0)} idle pipes · {int(small_file_pipes or 0)} small-file pipes"),
                ("📦 Data Freshness",       score_storage,  f"{int(stale_tables or 0)} stale tables (72h+) of {int(total_tables or 0)} total"),
            ]
            dim_cols = st.columns(3)
            for i, (name, score, detail) in enumerate(dims):
                c = _score_color(score)
                with dim_cols[i % 3]:
                    st.markdown(f"""
                    <div style="background:#13161f;border:1px solid #1e2130;border-radius:12px;padding:18px;margin-bottom:12px">
                        <div style="font-size:12px;font-weight:600;color:#6b7080;text-transform:uppercase;letter-spacing:0.8px;margin-bottom:10px">{name}</div>
                        <div style="font-family:'DM Mono',monospace;font-size:32px;font-weight:600;color:{c};line-height:1">{score}</div>
                        <div style="font-size:11px;color:#4a5068;margin-top:6px">{detail}</div>
                    </div>
                    """, unsafe_allow_html=True)

            st.markdown("---")

            # ── Two-column: charts + risk flags ──────────────────────────────
            ins_left, ins_right = st.columns([3, 2], gap="large")

            with ins_left:
                # Daily spend trend
                if not df_daily_spend.empty:
                    st.markdown('<div class="card-title">📈 7-Day Credit Spend Trend</div>', unsafe_allow_html=True)
                    try:
                        import plotly.graph_objects as go
                        fig_spend = go.Figure()
                        days  = df_daily_spend.iloc[:, 0].astype(str).tolist()
                        creds = df_daily_spend.iloc[:, 1].tolist()
                        avg_c = sum(creds) / len(creds) if creds else 0
                        colors_bar = ["#f87171" if c > avg_c * 1.2 else "#5b8dee" for c in creds]
                        fig_spend.add_trace(go.Bar(x=days, y=creds, marker_color=colors_bar, name="Daily Credits"))
                        fig_spend.add_trace(go.Scatter(x=days, y=[avg_c] * len(days),
                                                        mode="lines", line=dict(color="#fbbf24", dash="dash", width=2),
                                                        name="Avg"))
                        fig_spend.update_layout(
                            paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
                            font=dict(color="#6b7080", family="DM Sans"), height=260,
                            margin=dict(l=0, r=0, t=10, b=0),
                            legend=dict(bgcolor="rgba(0,0,0,0)", font=dict(size=11)),
                            xaxis=dict(showgrid=False, tickfont=dict(size=11)),
                            yaxis=dict(gridcolor="#1e2130", tickfont=dict(size=11)),
                        )
                        st.plotly_chart(fig_spend, use_container_width=True)
                    except ImportError:
                        st.dataframe(df_daily_spend, use_container_width=True, hide_index=True)

                # Top warehouses by credit
                if not df_top_wh.empty:
                    st.markdown('<div class="card-title">🏢 Top Credit Consumers (Warehouses, 7d)</div>', unsafe_allow_html=True)
                    try:
                        import plotly.graph_objects as go
                        wh_names  = df_top_wh.iloc[:, 0].tolist()
                        wh_creds  = df_top_wh.iloc[:, 1].tolist()
                        max_c = max(wh_creds) if wh_creds else 1
                        bar_colors = ["#f87171" if c == max_c else "#5b8dee" for c in wh_creds]
                        fig_wh = go.Figure(go.Bar(
                            x=wh_creds[::-1], y=wh_names[::-1],
                            orientation="h",
                            marker_color=bar_colors[::-1],
                            text=[f"{c} cr" for c in wh_creds[::-1]],
                            textposition="outside",
                            textfont=dict(size=11, color="#e8eaf0"),
                        ))
                        fig_wh.update_layout(
                            paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
                            font=dict(color="#6b7080", family="DM Sans"), height=220,
                            margin=dict(l=0, r=60, t=10, b=0),
                            xaxis=dict(showgrid=False, visible=False),
                            yaxis=dict(showgrid=False, tickfont=dict(size=12, color="#c8cad4")),
                        )
                        st.plotly_chart(fig_wh, use_container_width=True)
                    except ImportError:
                        st.dataframe(df_top_wh, use_container_width=True, hide_index=True)

                # Top users by exec time
                if not df_top_users.empty:
                    st.markdown('<div class="card-title">👤 Top Users by Execution Time (7d)</div>', unsafe_allow_html=True)
                    try:
                        import plotly.graph_objects as go
                        users = df_top_users["USER_NAME"].tolist() if "USER_NAME" in df_top_users.columns else df_top_users.iloc[:, 0].tolist()
                        mins  = df_top_users["TOTAL_EXEC_MIN"].tolist() if "TOTAL_EXEC_MIN" in df_top_users.columns else df_top_users.iloc[:, 2].tolist()
                        fig_usr = go.Figure(go.Bar(
                            x=users, y=mins,
                            marker_color="#a78bfa",
                            text=[f"{m}m" for m in mins],
                            textposition="outside",
                            textfont=dict(size=10, color="#e8eaf0"),
                        ))
                        fig_usr.update_layout(
                            paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
                            font=dict(color="#6b7080", family="DM Sans"), height=220,
                            margin=dict(l=0, r=0, t=10, b=0),
                            xaxis=dict(showgrid=False, tickangle=-30, tickfont=dict(size=11)),
                            yaxis=dict(gridcolor="#1e2130", tickfont=dict(size=11), title="Exec Minutes"),
                        )
                        st.plotly_chart(fig_usr, use_container_width=True)
                    except ImportError:
                        st.dataframe(df_top_users, use_container_width=True, hide_index=True)

            with ins_right:
                # ── Risk flags ────────────────────────────────────────────────
                st.markdown('<div class="card-title">🚨 Risk Flags</div>', unsafe_allow_html=True)
                flags = []
                if int(over_prov_count or 0) > 0:
                    flags.append(("amber", f"{int(over_prov_count)} warehouse(s) over-provisioned", "Downsize to reduce idle spend"))
                if int(under_prov_count or 0) > 0:
                    flags.append(("red", f"{int(under_prov_count)} warehouse(s) under-provisioned", "Queries are being throttled — scale up"))
                if int(wh_queue_issues or 0) > 0:
                    flags.append(("red", f"{int(wh_queue_issues)} warehouse(s) with queue buildup", "Concurrency issue — increase cluster size or add MCW"))
                if int(slow_query_count or 0) > 0:
                    flags.append(("amber", f"{int(slow_query_count)} queries > 5 min (7d)", "Review query plans and clustering keys"))
                if int(spill_count or 0) > 0:
                    flags.append(("red", f"{int(spill_count)} queries spilling to remote storage", "Increase warehouse size or rewrite heavy joins"))
                if int(antipattern_count or 0) > 0:
                    flags.append(("amber", f"{int(antipattern_count)} SELECT * / CROSS JOIN queries", "Enforce column pruning in query standards"))
                if int(unused_users or 0) > 0:
                    flags.append(("amber", f"{int(unused_users)} users inactive 30+ days", "Review and disable to tighten security perimeter"))
                if int(login_failures_7d or 0) > 10:
                    flags.append(("red", f"{int(login_failures_7d)} login failures in 7 days", "Investigate potential brute-force or misconfigured clients"))
                if int(sp_failures or 0) > 0:
                    flags.append(("red", f"{int(sp_failures)} stored procedure failures", "Review SP error logs immediately"))
                if int(idle_pipes or 0) > 0:
                    flags.append(("amber", f"{int(idle_pipes)} pipe(s) idle > 7 days", "Decommission or investigate broken ingestion"))
                if int(small_file_pipes or 0) > 0:
                    flags.append(("red", f"{int(small_file_pipes)} pipe(s) loading files < 1 MB", "Micro-files inflate metadata costs — batch files to ≥10 MB"))
                if int(stale_tables or 0) > 0:
                    flags.append(("amber", f"{int(stale_tables)} tables stale > 72 hours", "Verify ETL pipelines are running on schedule"))

                if not flags:
                    st.markdown("""
                    <div style="background:#0f2e1e;border:1px solid #1a5c3a;border-radius:10px;padding:16px 20px;color:#34d399;font-size:13px">
                        ✅ No critical risk flags detected. Account health looks good.
                    </div>
                    """, unsafe_allow_html=True)
                else:
                    color_map = {"red": ("#2e1010", "#f87171", "#7f1d1d"), "amber": ("#2a2010", "#fbbf24", "#78350f")}
                    for severity, msg, fix in flags:
                        bg, fg, border = color_map.get(severity, color_map["amber"])
                        st.markdown(f"""
                        <div style="background:{bg};border:1px solid {border};border-radius:10px;padding:12px 16px;margin-bottom:8px">
                            <div style="color:{fg};font-size:12px;font-weight:600">{msg}</div>
                            <div style="color:#8891a8;font-size:11px;margin-top:4px">→ {fix}</div>
                        </div>
                        """, unsafe_allow_html=True)

                st.markdown("---")

                # ── Quick stats panel ─────────────────────────────────────────
                st.markdown('<div class="card-title">📋 Account Snapshot</div>', unsafe_allow_html=True)
                snap_items = [
                    ("Total Tables",         str(int(total_tables or 0)),           "#5b8dee"),
                    ("Clustered Tables",     f"{int(clustered_tables or 0)} / {int(total_tables or 1)}", "#a78bfa"),
                    ("Credits (7d)",         str(total_credits_7d or 0),             "#fbbf24"),
                    ("Credits (30d)",        str(total_credits_30d or 0),            "#fbbf24"),
                    ("Est. Cost (7d)",       f"${est_cost_7d:,.2f}",                 "#34d399"),
                    ("Login Failures (7d)",  str(int(login_failures_7d or 0)),       "#f87171" if (login_failures_7d or 0) > 10 else "#34d399"),
                    ("Stale Tables",         str(int(stale_tables or 0)),            "#f87171" if (stale_tables or 0) > 0 else "#34d399"),
                    ("Idle Pipes",           str(int(idle_pipes or 0)),              "#fbbf24" if (idle_pipes or 0) > 0 else "#34d399"),
                ]
                for label, val, color in snap_items:
                    st.markdown(f"""
                    <div style="display:flex;justify-content:space-between;align-items:center;padding:7px 0;border-bottom:1px solid #1e2130">
                        <span style="font-size:12px;color:#6b7080">{label}</span>
                        <span style="font-family:'DM Mono',monospace;font-size:13px;font-weight:600;color:{color}">{val}</span>
                    </div>
                    """, unsafe_allow_html=True)

            st.markdown("---")

            # ── Recommendations table ─────────────────────────────────────────
            st.markdown('<div class="card-title">💡 Top Recommendations for Stakeholders</div>', unsafe_allow_html=True)

            recommendations = []
            priority_counter = 1

            if int(over_prov_count or 0) > 0:
                recommendations.append({
                    "Priority": f"P{priority_counter}", "Category": "💰 Cost",
                    "Finding": f"{int(over_prov_count)} over-provisioned warehouse(s)",
                    "Impact": "High — idle compute credits",
                    "Action": "Downsize warehouse tier; enable AUTO_SUSPEND < 60s"
                }); priority_counter += 1

            if int(under_prov_count or 0) > 0 or int(wh_queue_issues or 0) > 0:
                recommendations.append({
                    "Priority": f"P{priority_counter}", "Category": "⚡ Performance",
                    "Finding": f"{int(under_prov_count)} under-provisioned, {int(wh_queue_issues)} with queue issues",
                    "Impact": "High — user-facing query slowdown",
                    "Action": "Upsize or enable Multi-Cluster Warehouse (MCW)"
                }); priority_counter += 1

            if int(spill_count or 0) > 0:
                recommendations.append({
                    "Priority": f"P{priority_counter}", "Category": "⚡ Performance",
                    "Finding": f"{int(spill_count)} queries spilling to remote storage",
                    "Impact": "High — 10–100× slower queries",
                    "Action": "Increase warehouse size; rewrite joins with clustering keys"
                }); priority_counter += 1

            if int(login_failures_7d or 0) > 10:
                recommendations.append({
                    "Priority": f"P{priority_counter}", "Category": "🔒 Security",
                    "Finding": f"{int(login_failures_7d)} login failures in 7 days",
                    "Impact": "High — potential breach attempt",
                    "Action": "Enable MFA enforcement; block flagged IPs via network policy"
                }); priority_counter += 1

            if int(sp_failures or 0) > 0:
                recommendations.append({
                    "Priority": f"P{priority_counter}", "Category": "🔄 Reliability",
                    "Finding": f"{int(sp_failures)} stored procedure failures",
                    "Impact": "Medium — broken data workflows",
                    "Action": "Review SP error logs; add EXCEPTION blocks and alerting"
                }); priority_counter += 1

            if int(antipattern_count or 0) > 0:
                recommendations.append({
                    "Priority": f"P{priority_counter}", "Category": "⚡ Performance",
                    "Finding": f"{int(antipattern_count)} SELECT * / CROSS JOIN queries",
                    "Impact": "Medium — unnecessary data scan cost",
                    "Action": "Enforce column projection; use query governance policies"
                }); priority_counter += 1

            if int(unused_users or 0) > 0:
                recommendations.append({
                    "Priority": f"P{priority_counter}", "Category": "🔒 Security",
                    "Finding": f"{int(unused_users)} inactive users (30+ days)",
                    "Impact": "Medium — attack surface risk",
                    "Action": "Disable accounts; implement periodic access review process"
                }); priority_counter += 1

            if int(small_file_pipes or 0) > 0:
                recommendations.append({
                    "Priority": f"P{priority_counter}", "Category": "🔄 Pipeline",
                    "Finding": f"{int(small_file_pipes)} pipe(s) ingesting micro-files < 1 MB",
                    "Impact": "Medium — metadata overhead, increased pipe credits",
                    "Action": "Buffer upstream writes to produce files ≥ 10 MB before loading"
                }); priority_counter += 1

            if int(stale_tables or 0) > 0:
                recommendations.append({
                    "Priority": f"P{priority_counter}", "Category": "📦 Data Quality",
                    "Finding": f"{int(stale_tables)} tables not updated in 72+ hours",
                    "Impact": "Medium — stale reports reaching end users",
                    "Action": "Review ETL schedule; add DATA_METRIC_FUNCTION staleness checks"
                }); priority_counter += 1

            clustered_ratio = (clustered_tables or 0) / max(total_tables or 1, 1)
            if clustered_ratio < 0.2 and (total_tables or 0) > 10:
                recommendations.append({
                    "Priority": f"P{priority_counter}", "Category": "⚡ Performance",
                    "Finding": f"Only {round(clustered_ratio * 100)}% of tables have clustering keys",
                    "Impact": "Low-Medium — poor partition pruning on large tables",
                    "Action": "Profile top scanned tables; apply CLUSTER BY on join/filter columns"
                }); priority_counter += 1

            if not recommendations:
                recommendations.append({
                    "Priority": "—", "Category": "✅ All Clear",
                    "Finding": "No significant issues detected in the past 7 days",
                    "Impact": "—",
                    "Action": "Continue monitoring — schedule next review in 30 days"
                })

            df_recs = pd.DataFrame(recommendations)

            def _style_recs(df):
                def row_style(row):
                    p = str(row.get("Priority", ""))
                    if p == "P1": bg = "background-color:#2e1010;color:#f87171"
                    elif p in ("P2", "P3"): bg = "background-color:#2a2010;color:#fbbf24"
                    else: bg = "background-color:#0d0f14;color:#e8eaf0"
                    return [bg] * len(row)
                return df.style.apply(row_style, axis=1).set_properties(**{
                    "font-size": "12px",
                    "font-family": "'DM Sans', sans-serif",
                })

            st.dataframe(_style_recs(df_recs), use_container_width=True, hide_index=True)

            # ── Export button ─────────────────────────────────────────────────
            st.markdown("---")
            exp_c1, exp_c2 = st.columns([4, 1])
            with exp_c2:
                csv_export = df_recs.to_csv(index=False).encode("utf-8")
                st.download_button(
                    "⬇️ Export Report (.csv)",
                    data=csv_export,
                    file_name=f"sf_exec_health_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                    mime="text/csv",
                    use_container_width=True,
                )
            with exp_c1:
                st.markdown(
                    f'<span class="badge badge-connected">✓ Report generated · {len(recommendations)} recommendation(s) · Score: {overall_score}/100</span>',
                    unsafe_allow_html=True,
                )