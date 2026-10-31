"""
CHUNK 3 — Bulk Monitoring UI
=============================
Standalone module: chunk_03_monitoring_ui.py

PURPOSE
-------
Renders a live status grid for all N tables in the job queue.
Shows per-table status (pending / running / done / failed),
rows, size, duration, and error drill-down for failures.

PLUG-IN POINT (for Assembly — Chunk 5)
---------------------------------------
Add a new tab "📊 Migration Monitor" in the main script and call:
    from chunk_03_monitoring_ui import render_monitoring_tab
    render_monitoring_tab()

SESSION STATE KEYS READ
------------------------
  st.session_state.job_queue          : list[dict]  — from Chunk 1, updated by Chunk 2
  st.session_state.migration_running  : bool
  st.session_state.migration_done     : bool
  st.session_state.migration_summary  : dict
"""

import streamlit as st
import pandas as pd
from datetime import datetime


def _format_bytes(b: int) -> str:
    if b < 1024:        return f"{b} B"
    if b < 1024 ** 2:   return f"{b / 1024:.1f} KB"
    if b < 1024 ** 3:   return f"{b / 1024 ** 2:.2f} MB"
    return f"{b / 1024 ** 3:.3f} GB"


def _status_icon(status: str) -> str:
    return {
        "pending": "⬜ PENDING",
        "running": "⟳ RUNNING",
        "done":    "✅ DONE",
        "failed":  "❌ FAILED",
        "skipped": "⏭ SKIPPED",
    }.get(status, status.upper())


# ── Overall progress summary bar ─────────────────────────────────────────────
def _render_summary_bar(queue: list):
    total   = len(queue)
    done    = sum(1 for j in queue if j["status"] == "done")
    failed  = sum(1 for j in queue if j["status"] == "failed")
    running = sum(1 for j in queue if j["status"] == "running")
    pending = sum(1 for j in queue if j["status"] == "pending")

    pct = int((done / total) * 100) if total else 0
    exported = sum(j.get("export_complete", False) and j["status"] == "pending" for j in queue)

    col1, col2, col3, col4, col5 = st.columns(5)
    with col1:
        st.markdown(f"""
        <div class="metric-card">
            <div class="metric-value">{total}</div>
            <div class="metric-label">Total Tables</div>
        </div>""", unsafe_allow_html=True)
    with col2:
        st.markdown(f"""
        <div class="metric-card">
            <div class="metric-value" style="color:#34d399">{done}</div>
            <div class="metric-label">Completed</div>
        </div>""", unsafe_allow_html=True)
    with col3:
        st.markdown(f"""
        <div class="metric-card">
            <div class="metric-value" style="color:#f87171">{failed}</div>
            <div class="metric-label">Failed</div>
        </div>""", unsafe_allow_html=True)
    with col4:
        st.markdown(f"""
        <div class="metric-card">
            <div class="metric-value" style="color:#5b8dee">{running}</div>
            <div class="metric-label">Running</div>
        </div>""", unsafe_allow_html=True)
    with col5:
        st.markdown(f"""
        <div class="metric-card">
            <div class="metric-value" style="color:#4a5068">{pending}</div>
            <div class="metric-label">Pending</div>
        </div>""", unsafe_allow_html=True)

    st.markdown('<div style="height:12px"></div>', unsafe_allow_html=True)
    text = f"Overall progress — {done}/{total} tables complete ({pct}%)"
    if exported:
        text += f" · {exported} exported to S3; awaiting Redshift"
    st.progress(pct, text=text)


# ── Filter bar ────────────────────────────────────────────────────────────────
def _render_filter_bar() -> str:
    col_f1, col_f2, _ = st.columns([1, 1, 3])
    with col_f1:
        status_filter = st.selectbox(
            "Filter by status",
            ["All", "pending", "running", "done", "failed"],
            key="monitor_status_filter",
            label_visibility="collapsed",
        )
    with col_f2:
        search = st.text_input(
            "Search table",
            placeholder="Search schema.table…",
            key="monitor_search",
            label_visibility="collapsed",
        )
    return status_filter, search


# ── Main status grid ──────────────────────────────────────────────────────────
def _render_status_grid(queue: list, status_filter: str, search: str):
    # Apply filters
    filtered = queue
    if status_filter != "All":
        filtered = [j for j in filtered if j["status"] == status_filter]
    if search:
        s = search.lower()
        filtered = [j for j in filtered if s in j["id"].lower()]

    if not filtered:
        st.info("No tables match the current filter.")
        return

    # Build display dataframe
    rows = []
    for j in filtered:
        rows.append({
            "Status":       "📦 S3 exported; awaiting Redshift" if j["status"] == "pending" and j.get("export_complete") else _status_icon(j["status"]),
            "Source (SF)":  f"{j['sf_schema']}.{j['sf_table']}",
            "Target (RS)":  f"{j['rs_schema']}.{j['rs_table']}",
            "Format":       j["file_format"],
            "Rows":         f"{j['rows']:,}"         if j["rows"]  else "—",
            "Size":         _format_bytes(j["bytes"]) if j["bytes"] else "—",
            "Duration":     f"{j['duration_secs']}s"  if j["duration_secs"] else "—",
            "Error":        j["error"][:60] + "…"     if len(j.get("error","")) > 60 else j.get("error",""),
        })

    df = pd.DataFrame(rows)
    st.dataframe(
        df,
        use_container_width=True,
        hide_index=True,
        height=min(600, 40 + len(filtered) * 38),
        column_config={
            "Status":  st.column_config.TextColumn("Status",  width="small"),
            "Error":   st.column_config.TextColumn("Error",   width="medium"),
            "Rows":    st.column_config.TextColumn("Rows",    width="small"),
            "Size":    st.column_config.TextColumn("Size",    width="small"),
            "Duration":st.column_config.TextColumn("Duration",width="small"),
        },
    )
    st.caption(f"Showing {len(filtered)} of {len(queue)} tables")


# ── Failed table drill-down ───────────────────────────────────────────────────
def _render_failed_drilldown(queue: list):
    failed_jobs = [j for j in queue if j["status"] == "failed"]
    if not failed_jobs:
        return

    st.markdown("---")
    st.markdown('<div class="card-title">❌ Failed Tables — Error Detail</div>', unsafe_allow_html=True)

    for j in failed_jobs:
        with st.expander(f"❌ {j['id']}"):
            col_a, col_b = st.columns(2)
            with col_a:
                st.markdown(f"""
                | Field | Value |
                |-------|-------|
                | Source | `{j['sf_schema']}.{j['sf_table']}` |
                | Target | `{j['rs_schema']}.{j['rs_table']}` |
                | Format | `{j['file_format']}` |
                | Load Mode | `{j['load_mode']}` |
                """)
            with col_b:
                err_text = j['error'] or 'Unknown error'
                st.error(f"**Error:** {err_text}")
                if any(k in err_text.lower() for k in ("external", "spectrum", "awsdatacatalog")):
                    st.info("💡 **Fix:** Target schema is external (`awsdatacatalog`/Spectrum). Rebuild queue with **Target Redshift Schema: `public`** or choose **Iceberg** format.")
                elif "relation" in err_text.lower() and "does not exist" in err_text.lower():
                    st.info(f"💡 **Fix:** Table `{j.get('rs_schema') or 'public'}.{j.get('rs_table')}` does not exist in Redshift. Create it before loading.")
                st.markdown(
                    "<small style='color:#4a5068'>This table will be retried "
                    "on Resume. Fix the underlying issue first if needed.</small>",
                    unsafe_allow_html=True,
                )


# ── Auto-refresh notice (when running) ───────────────────────────────────────
def _render_live_notice():
    st.markdown(
        """<div style="display:flex;align-items:center;gap:8px;padding:10px 14px;
        background:#0d1e3a;border:1px solid #1e4080;border-radius:8px;margin-bottom:16px">
        <span style="color:#5b8dee;font-size:18px">⟳</span>
        <span style="color:#8aabf0;font-size:13px">
        Migration is running — refresh the page or re-click this tab to update status.
        </span></div>""",
        unsafe_allow_html=True,
    )


# ── Main render entry point ───────────────────────────────────────────────────
def render_monitoring_tab():
    """
    Full monitoring tab renderer.
    Call this inside a `with tab:` block in the main script.
    """
    queue   = st.session_state.get("job_queue", [])
    running = st.session_state.get("migration_running", False)
    done    = st.session_state.get("migration_done", False)

    st.markdown('<div class="card-title">📡 Migration Monitor</div>', unsafe_allow_html=True)

    if not queue:
        st.info("No migration queued yet. Complete Table Discovery first.")
        return

    if running:
        _render_live_notice()

    # Summary bar
    _render_summary_bar(queue)

    st.markdown("---")

    # Filter + grid
    st.markdown('<div class="card-title">📋 Table Status</div>', unsafe_allow_html=True)
    status_filter, search = _render_filter_bar()
    st.markdown('<div style="height:8px"></div>', unsafe_allow_html=True)
    _render_status_grid(queue, status_filter, search)

    # Failed drill-down
    _render_failed_drilldown(queue)

    # Schema-level rollup
    st.markdown("---")
    st.markdown('<div class="card-title">🗂 Schema Rollup</div>', unsafe_allow_html=True)

    schema_stats = {}
    for j in queue:
        s = j["sf_schema"]
        if s not in schema_stats:
            schema_stats[s] = {"total": 0, "done": 0, "failed": 0, "rows": 0, "bytes": 0}
        schema_stats[s]["total"]  += 1
        schema_stats[s]["rows"]   += j["rows"]
        schema_stats[s]["bytes"]  += j["bytes"]
        if j["status"] == "done":   schema_stats[s]["done"]   += 1
        if j["status"] == "failed": schema_stats[s]["failed"] += 1

    rollup_rows = []
    for schema, stats in schema_stats.items():
        pct = int((stats["done"] / stats["total"]) * 100) if stats["total"] else 0
        rollup_rows.append({
            "Schema":    schema,
            "Tables":    stats["total"],
            "Done":      stats["done"],
            "Failed":    stats["failed"],
            "Progress":  pct,
            "Rows":      f"{stats['rows']:,}"          if stats["rows"]  else "—",
            "Data Size": _format_bytes(stats["bytes"]) if stats["bytes"] else "—",
        })

    rollup_df = pd.DataFrame(rollup_rows)
    st.dataframe(
        rollup_df,
        use_container_width=True,
        hide_index=True,
        column_config={
            "Progress": st.column_config.ProgressColumn(
                "Progress %", min_value=0, max_value=100, format="%d%%"
            ),
        },
    )
