"""Client-run BigQuery health-check downloads for the Streamlit application."""
from pathlib import Path

import streamlit as st


KIT = Path(__file__).resolve().parent / "client_bigquery_health_check"


def render_bigquery_health_check():
    st.markdown("### BigQuery Client-Run Health Check")
    st.caption("The client runs the supplied SQL in their own BigQuery console and shares only reviewed results.")
    st.info(
        "Use the SQL worksheet as the primary workflow. DataBridge does not need the client's "
        "BigQuery credentials, and the worksheet does not read business-table rows or create permanent objects."
    )

    st.markdown("#### Client workflow")
    st.markdown(
        """
1. Download the SQL-only kit.
2. Replace `your-project-id` and, when required, `region-us` in the SQL file.
3. Set the matching processing location in BigQuery Query settings.
4. Run each `-- CHECK:` statement separately.
5. Export each result using the check ID as the CSV filename.
6. Review and ZIP the CSV files with the project, location, run time and failed checks before sharing.
"""
    )

    quickstart = (KIT / "SQL_CONSOLE_QUICKSTART.md").read_bytes()
    worksheet = (KIT / "bigquery_health_check.sql").read_bytes()
    sql_zip = Path(__file__).resolve().parent / "client_bigquery_health_check_sql_kit.zip"

    left, middle, right = st.columns(3)
    with left:
        st.download_button(
            "Download SQL-only kit", data=sql_zip.read_bytes(),
            file_name="client_bigquery_health_check_sql_kit.zip",
            mime="application/zip", key="bq_hc_download_sql_kit",
            use_container_width=True,
        )
    with middle:
        st.download_button(
            "Download SQL worksheet", data=worksheet,
            file_name="bigquery_health_check.sql", mime="text/plain",
            key="bq_hc_download_sql", use_container_width=True,
        )
    with right:
        st.download_button(
            "Download quick start", data=quickstart,
            file_name="SQL_CONSOLE_QUICKSTART.md", mime="text/markdown",
            key="bq_hc_download_quickstart", use_container_width=True,
        )

    st.markdown("#### Coverage")
    st.write(
        "The 20 checks cover inventory, query workload, processed/billed bytes, slot usage, "
        "slow queries, start delays, cache observations, resource warnings, reservations, "
        "pipeline jobs, storage, partitioning/clustering metadata, selected table options, "
        "access-denied job reasons and column policy tags."
    )
    st.warning(
        "Results can contain operational object names, job/reservation IDs and principal email addresses. "
        "The client should review every exported file before sharing it. Query jobs can consume slots or incur charges."
    )

    with st.expander("Optional automated collector"):
        st.write(
            "The full kit also contains a Python runner for repeatable collection, fixed timestamps, "
            "failure manifests and automatic ZIP packaging. It executes the same SQL checks and is optional."
        )
        full_zip = Path(__file__).resolve().parent / "client_bigquery_health_check_kit.zip"
        st.download_button(
            "Download full SQL + optional Python kit", data=full_zip.read_bytes(),
            file_name="client_bigquery_health_check_kit.zip", mime="application/zip",
            key="bq_hc_download_full_kit",
        )
