"""
CHUNK 4 — Aggregated Metrics & Migration Report
=================================================
Standalone module: chunk_04_metrics_report.py

PURPOSE
-------
Replaces the single-table metrics in the original Tab 3 with:
  - Aggregate stats across all N tables
  - Per-table metrics breakdown
  - Schema-level throughput chart
  - Exportable CSV migration report
  - Generated COPY statements for all tables

PLUG-IN POINT (for Assembly — Chunk 5)
---------------------------------------
Replace the contents of Tab 3 (Metrics & Logs) in the main script with:
    from chunk_04_metrics_report import render_metrics_tab
    render_metrics_tab()

SESSION STATE KEYS READ
------------------------
  st.session_state.job_queue          : list[dict]  — from Chunk 1, updated by Chunk 2
  st.session_state.migration_summary  : dict        — from Chunk 2
  st.session_state.logs               : list        — existing log list
"""

import streamlit as st
import pandas as pd
from datetime import datetime


def _format_bytes(b: int) -> str:
    if b < 1024:        return f"{b} B"
    if b < 1024 ** 2:   return f"{b / 1024:.1f} KB"
    if b < 1024 ** 3:   return f"{b / 1024 ** 2:.2f} MB"
    return f"{b / 1024 ** 3:.3f} GB"


# ── Top-level aggregate metrics ───────────────────────────────────────────────
def _render_aggregate_metrics(summary: dict):
    st.markdown('<div class="card-title">📈 Migration Summary</div>', unsafe_allow_html=True)

    total     = summary.get("total", 0)
    completed = summary.get("completed", 0)
    failed    = summary.get("failed", 0)
    rows      = summary.get("total_rows", 0)
    size      = summary.get("total_bytes", 0)
    secs      = summary.get("total_secs", 0)
    throughput= summary.get("throughput", 0)
    finished  = summary.get("finished_at", "—")

    success_rate = round((completed / total) * 100, 1) if total else 0

    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.markdown(f"""
        <div class="metric-card">
            <div class="metric-value">{completed}<span style="font-size:14px;color:#4a5068">/{total}</span></div>
            <div class="metric-label">Tables Migrated</div>
        </div>""", unsafe_allow_html=True)
    with col2:
        st.markdown(f"""
        <div class="metric-card">
            <div class="metric-value">{rows:,}</div>
            <div class="metric-label">Total Rows</div>
        </div>""", unsafe_allow_html=True)
    with col3:
        st.markdown(f"""
        <div class="metric-card">
            <div class="metric-value">{_format_bytes(size)}</div>
            <div class="metric-label">Total Data Moved</div>
        </div>""", unsafe_allow_html=True)
    with col4:
        st.markdown(f"""
        <div class="metric-card">
            <div class="metric-value" style="font-size:20px">{_format_bytes(throughput)}/s</div>
            <div class="metric-label">Avg Throughput</div>
        </div>""", unsafe_allow_html=True)

    st.markdown('<div style="height:10px"></div>', unsafe_allow_html=True)

    col5, col6, col7, col8 = st.columns(4)
    with col5:
        st.markdown(f"""
        <div class="metric-card">
            <div class="metric-value" style="font-size:22px">{secs}s</div>
            <div class="metric-label">Total Wall Time</div>
        </div>""", unsafe_allow_html=True)
    with col6:
        color = "#34d399" if success_rate == 100 else "#fbbf24" if success_rate >= 80 else "#f87171"
        st.markdown(f"""
        <div class="metric-card">
            <div class="metric-value" style="color:{color}">{success_rate}%</div>
            <div class="metric-label">Success Rate</div>
        </div>""", unsafe_allow_html=True)
    with col7:
        avg_secs = round(secs / completed, 1) if completed else 0
        st.markdown(f"""
        <div class="metric-card">
            <div class="metric-value" style="font-size:22px">{avg_secs}s</div>
            <div class="metric-label">Avg per Table</div>
        </div>""", unsafe_allow_html=True)
    with col8:
        st.markdown(f"""
        <div class="metric-card">
            <div class="metric-value" style="font-size:14px;color:#6b7080">{finished}</div>
            <div class="metric-label">Completed At</div>
        </div>""", unsafe_allow_html=True)


# ── Per-table metrics breakdown ───────────────────────────────────────────────
def _render_per_table_breakdown(queue: list):
    st.markdown("---")
    st.markdown('<div class="card-title">📋 Per-Table Breakdown</div>', unsafe_allow_html=True)

    done_jobs = [j for j in queue if j["status"] in ("done", "failed")]
    if not done_jobs:
        st.info("No tables have been processed yet.")
        return

    rows = []
    for j in done_jobs:
        rows.append({
            "Table":        j["id"],
            "Status":       "✅ Done" if j["status"] == "done" else "❌ Failed",
            "Rows":         j["rows"],
            "Size":         j["bytes"],
            "Duration (s)": j["duration_secs"],
            "Format":       j["file_format"],
            "Load Mode":    j["load_mode"],
            "S3 Key":       j["s3_key"],
        })

    df = pd.DataFrame(rows)

    # Display with formatted columns
    display_df = df.copy()
    display_df["Rows"]  = display_df["Rows"].apply(lambda x: f"{x:,}")
    display_df["Size"]  = display_df["Size"].apply(_format_bytes)

    st.dataframe(
        display_df,
        use_container_width=True,
        hide_index=True,
        height=min(500, 40 + len(display_df) * 38),
    )


# ── Schema-level chart ────────────────────────────────────────────────────────
def _render_schema_chart(queue: list):
    done_jobs = [j for j in queue if j["status"] == "done" and j["bytes"] > 0]
    if not done_jobs:
        return

    st.markdown("---")
    st.markdown('<div class="card-title">📊 Data Volume by Schema</div>', unsafe_allow_html=True)

    schema_bytes = {}
    schema_rows  = {}
    for j in done_jobs:
        s = j["sf_schema"]
        schema_bytes[s] = schema_bytes.get(s, 0) + j["bytes"]
        schema_rows[s]  = schema_rows.get(s, 0)  + j["rows"]

    chart_df = pd.DataFrame({
        "Schema":    list(schema_bytes.keys()),
        "Data (MB)": [round(v / 1024 ** 2, 2) for v in schema_bytes.values()],
        "Rows (K)":  [round(v / 1000, 1) for v in schema_rows.values()],
    })

    col_c1, col_c2 = st.columns(2)
    with col_c1:
        st.markdown("**Data volume (MB) per schema**")
        st.bar_chart(chart_df.set_index("Schema")["Data (MB)"], height=250)
    with col_c2:
        st.markdown("**Rows (thousands) per schema**")
        st.bar_chart(chart_df.set_index("Schema")["Rows (K)"], height=250)


# ── Export CSV report ─────────────────────────────────────────────────────────
def _render_export(queue: list, summary: dict):
    st.markdown("---")
    st.markdown('<div class="card-title">💾 Export Migration Report</div>', unsafe_allow_html=True)

    col_e1, col_e2 = st.columns([1, 3])

    with col_e1:
        if st.button("📥 Download CSV Report", key="btn_download_report"):
            rows = []
            for j in queue:
                rows.append({
                    "table_id":       j["id"],
                    "sf_schema":      j["sf_schema"],
                    "sf_table":       j["sf_table"],
                    "rs_schema":      j["rs_schema"],
                    "rs_table":       j["rs_table"],
                    "file_format":    j["file_format"],
                    "load_mode":      j["load_mode"],
                    "s3_key":         j["s3_key"],
                    "status":         j["status"],
                    "rows_migrated":  j["rows"],
                    "bytes_migrated": j["bytes"],
                    "size_human":     _format_bytes(j["bytes"]) if j["bytes"] else "",
                    "duration_secs":  j["duration_secs"],
                    "error":          j["error"],
                    "report_generated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                })
            report_df  = pd.DataFrame(rows)
            csv_bytes  = report_df.to_csv(index=False).encode("utf-8")
            fname = f"migration_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
            st.download_button(
                label="⬇ Save CSV",
                data=csv_bytes,
                file_name=fname,
                mime="text/csv",
                key="btn_save_csv",
            )

    with col_e2:
        total     = summary.get("total", len(queue))
        completed = summary.get("completed", 0)
        failed    = summary.get("failed", 0)
        st.markdown(
            f"<small style='color:#6b7080'>Report includes all {total} tables — "
            f"{completed} completed, {failed} failed. "
            f"Generated at {datetime.now().strftime('%Y-%m-%d %H:%M')}.</small>",
            unsafe_allow_html=True,
        )


# ── Generated COPY statements ─────────────────────────────────────────────────
def _render_copy_statements(queue: list, s3_bucket: str, iam_role: str = "arn:aws:iam::ACCOUNT:role/RedshiftS3Role"):
    done_jobs = [j for j in queue if j["status"] == "done"]
    if not done_jobs:
        return

    st.markdown("---")
    st.markdown('<div class="card-title">📋 Actual COPY Statements Executed</div>', unsafe_allow_html=True)

    # Show first 5 with expander for the rest
    preview_jobs = done_jobs[:5]
    remaining    = done_jobs[5:]

    for j in preview_jobs:
        copy_sql = j.get("copy_sql", "— COPY statement not recorded for this table —")
        with st.expander(f"`{j['rs_schema']}.{j['rs_table']}`"):
            st.code(copy_sql, language="sql")

    if remaining:
        with st.expander(f"+ {len(remaining)} more COPY statements…"):
            for j in remaining:
                copy_sql = j.get("copy_sql", "— COPY statement not recorded for this table —")
                st.code(copy_sql, language="sql")


# ── Log renderer (carried over from original script) ─────────────────────────
def render_logs():
    logs = st.session_state.get("logs", [])
    html = '<div class="log-box">'
    for ts, level, msg in logs[-80:]:
        cls = {"ok": "log-ok", "info": "log-info", "warn": "log-warn", "err": "log-err"}.get(level, "log-info")
        html += f'<div><span style="color:#2a2d3e">{ts}</span> &nbsp;<span class="{cls}">{msg}</span></div>'
    html += "</div>"
    st.markdown(html, unsafe_allow_html=True)


# ── Main render entry point ───────────────────────────────────────────────────
def render_metrics_tab(s3_bucket: str = ""):
    """
    Full metrics & report tab renderer.
    Replaces the old Tab 3 content in the main script.

    Parameters
    ----------
    s3_bucket : str — passed in from sidebar input for COPY statement generation
    """
    queue   = st.session_state.get("job_queue", [])
    summary = st.session_state.get("migration_summary", {})

    if not queue:
        st.info("No migration data yet. Complete Table Discovery and run the pipeline first.")
        return

    # Aggregate metrics — only show if migration has run
    if summary:
        _render_aggregate_metrics(summary)
    else:
        st.markdown(
            '<span class="badge badge-pending">⚠ Run migration to see metrics</span>',
            unsafe_allow_html=True,
        )

    # Per-table breakdown
    _render_per_table_breakdown(queue)

    # Schema chart
    _render_schema_chart(queue)

    # Export
    _render_export(queue, summary)

    # COPY statements
    _render_copy_statements(queue, s3_bucket)

    # Operation log
    st.markdown("---")
    st.markdown('<div class="card-title">🖥 Operation Log</div>', unsafe_allow_html=True)
    col_log, col_logbtn = st.columns([5, 1])
    with col_logbtn:
        if st.button("Clear logs", key="btn_clear_logs"):
            st.session_state.logs = []
    render_logs()