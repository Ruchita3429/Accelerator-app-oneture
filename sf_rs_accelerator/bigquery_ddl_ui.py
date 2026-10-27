"""Streamlit UI for the BigQuery-to-Redshift DDL migrator."""
from datetime import datetime
import difflib
import hashlib
import json

import streamlit as st
from ui_theme import tab_loading

from bigquery_ddl_migrator import BigQueryDDLMigrator, validate_redshift_ddl


EXAMPLE = """CREATE TABLE IF NOT EXISTS `my-project.sales.orders` (
  order_id INT64 NOT NULL,
  customer_id INT64,
  order_date DATE,
  created_at TIMESTAMP,
  status STRING(50),
  amount NUMERIC(18,2),
  is_paid BOOL
)
PARTITION BY order_date
CLUSTER BY customer_id, status
OPTIONS(description='Sales order demo table');"""


def _migration_settings(prefix, allow_mappings=True):
    left, middle, right = st.columns(3)
    with left:
        target_schema = st.text_input("Target Redshift schema", value="public",
                                      key=f"{prefix}_schema")
        string_length = st.number_input(
            "Default VARCHAR length", min_value=1, max_value=65535,
            value=65535, step=1, key=f"{prefix}_varchar",
            help="Used for unparameterized BigQuery STRING columns.",
        )
    with middle:
        diststyle = st.selectbox("Redshift distribution style",
                                 ["AUTO", "EVEN", "ALL", "KEY"],
                                 key=f"{prefix}_diststyle")
        distkey = st.text_input("DISTKEY column", key=f"{prefix}_distkey",
                                disabled=diststyle != "KEY",
                                placeholder="customer_id")
    with right:
        sort_label = st.selectbox(
            "Redshift sort-key design",
            ["No explicit sort key", "Use BigQuery layout", "Manual"],
            key=f"{prefix}_sort_mode",
            help="BigQuery partition/clustering columns can be used as an initial compound SORTKEY suggestion.",
        )
        manual = st.text_input("SORTKEY columns (comma-separated)",
                               key=f"{prefix}_sortkeys", disabled=sort_label != "Manual",
                               placeholder="order_date, customer_id")
    mappings = {}
    mapping_text = "{}"
    if allow_mappings:
        with st.expander("Optional table mappings"):
            mapping_text = st.text_area(
                "Source-to-target mappings (JSON)", value="{}",
                key=f"{prefix}_mappings",
                placeholder='{"my-project.sales.orders": "analytics.orders"}',
            )
            st.caption("Map an exact BigQuery project.dataset.table name to schema.table. This also resolves same-name collisions in multi-statement files.")
    try:
        mappings = json.loads(mapping_text or "{}")
    except json.JSONDecodeError as exc:
        mappings = exc
    mode = {"No explicit sort key": "none", "Use BigQuery layout": "source",
            "Manual": "manual"}[sort_label]
    settings = {
        "target_schema": target_schema,
        "table_mapping": mappings,
        "string_length": int(string_length),
        "diststyle": diststyle,
        "distkey": distkey,
        "sortkey_mode": mode,
        "sortkeys": [part.strip() for part in manual.split(",") if part.strip()],
    }
    return settings


def _render_result(result, source, fingerprint, prefix, filename):
    if not result:
        return
    if result.get("fingerprint") != fingerprint:
        st.info("The input or conversion settings changed. Convert again to refresh the output.")
        return
    for error in result["errors"]:
        st.error(error)
    if result["errors"]:
        st.caption("No combined SQL file is produced until every selected statement or table converts successfully.")
        return
    st.markdown("#### Redshift DDL")
    st.code(result["sql"], language="sql")
    st.caption(f"{result['statement_count']} table(s) converted. The DDL has not been executed.")
    with st.expander("Changes applied", expanded=True):
        for change in result["changes"]:
            st.write(change)
    for warning in result["warnings"]:
        st.warning(warning)
    if source:
        with st.expander("SQL differences"):
            diff = "\n".join(difflib.unified_diff(
                source.splitlines(), result["sql"].splitlines(),
                fromfile="bigquery.sql", tofile="redshift.sql", lineterm="",
            ))
            st.code(diff, language="diff")
    if st.button("Check Redshift Syntax", key=f"{prefix}_validate"):
        st.session_state[f"{prefix}_validation"] = validate_redshift_ddl(result["sql"])
    validation = st.session_state.get(f"{prefix}_validation")
    if validation:
        (st.success if validation["valid"] else st.error)(validation["message"])
    notes = "\n".join("-- Review: " + warning.replace("\n", " ")
                      for warning in result["warnings"])
    st.download_button(
        "Download Redshift DDL", data=notes + "\n\n" + result["sql"] + "\n",
        file_name=filename, mime="text/plain", key=f"{prefix}_download",
    )


def _paste_tab():
    source_mode = st.radio("DDL input", ["Paste SQL", "Upload .sql file"],
                           horizontal=True, key="bq_ddl_source_mode")
    source = ""
    if source_mode == "Paste SQL":
        source = st.text_area("BigQuery CREATE TABLE DDL", height=330,
                              placeholder=EXAMPLE, key="bq_ddl_input")
    else:
        upload = st.file_uploader("BigQuery DDL file", type=["sql"],
                                  key="bq_ddl_upload")
        if upload is not None:
            if upload.size > 1024 * 1024:
                st.error("For this POC, upload a SQL file no larger than 1 MiB.")
                return
            try:
                source = upload.getvalue().decode("utf-8-sig")
            except UnicodeDecodeError:
                st.error("Save the SQL file as UTF-8 and upload it again.")
                return
            st.code(source, language="sql")
    settings = _migration_settings("bq_ddl")
    fingerprint = hashlib.sha256(json.dumps([source, settings], sort_keys=True, default=str).encode()).hexdigest()
    if st.button("Convert BigQuery DDL", key="bq_ddl_convert", type="primary"):
        st.session_state.pop("bq_ddl_validation", None)
        try:
            if isinstance(settings["table_mapping"], Exception):
                raise ValueError(f"Table mappings must be valid JSON: {settings['table_mapping']}")
            result = BigQueryDDLMigrator(**settings).migrate(source).to_dict()
            result["fingerprint"] = fingerprint
            st.session_state.bq_ddl_result = result
        except (TypeError, ValueError) as exc:
            st.session_state.bq_ddl_result = {
                "sql": "", "changes": [], "warnings": [],
                "errors": [f"Conversion settings: {exc}"],
                "statement_count": 0, "fingerprint": fingerprint,
            }
    _render_result(st.session_state.get("bq_ddl_result"), source, fingerprint,
                   "bq_ddl", "bigquery_to_redshift_ddl.sql")


def _metadata_tab():
    st.caption("This mode reads schema metadata only. It does not scan BigQuery rows or execute the generated DDL.")
    if not st.session_state.get("bq_connected"):
        st.info("Connect BigQuery in the sidebar first.")
        return
    datasets = st.session_state.get("bq_datasets", [])
    if not datasets:
        st.warning("No BigQuery datasets are visible to the connected identity.")
        return
    dataset = st.selectbox("BigQuery dataset", datasets, key="bq_ddl_dataset")
    if st.button("Load table schemas", key="bq_ddl_load_tables"):
        try:
            from bigquery_connector import discover_tables
            with st.spinner("Reading BigQuery table metadata…"):
                st.session_state.bq_ddl_tables = discover_tables(
                    st.session_state.bq_client, [dataset]
                )
            st.session_state.pop("bq_ddl_metadata_result", None)
        except Exception as exc:
            st.error(f"Could not load BigQuery table metadata: {exc}")
    table_map = st.session_state.get("bq_ddl_tables", {})
    tables = table_map.get(dataset, [])
    names = [item["table"] for item in tables]
    if not names:
        st.info("Load table schemas for the selected dataset.")
        return
    selected = st.multiselect("Tables to convert", names, key="bq_ddl_metadata_selected")
    if selected:
        preview = []
        for name in selected:
            item = next(table for table in tables if table["table"] == name)
            preview.append({
                "Table": name,
                "Columns": len(item.get("schema", [])),
                "Partition field": item.get("partition_field") or "—",
                "Clustering fields": ", ".join(item.get("clustering_fields") or []) or "—",
            })
        st.dataframe(preview, hide_index=True, use_container_width=True)
    settings = _migration_settings("bq_ddl_meta", allow_mappings=False)
    snapshot = [{key: item.get(key) for key in ("table", "schema", "partition_field", "clustering_fields")}
                for item in tables if item["table"] in selected]
    fingerprint = hashlib.sha256(json.dumps([dataset, snapshot, settings], sort_keys=True, default=str).encode()).hexdigest()
    if st.button("Generate Redshift DDL", key="bq_ddl_metadata_convert",
                 type="primary", disabled=not selected):
        st.session_state.pop("bq_ddl_meta_validation", None)
        try:
            migrator = BigQueryDDLMigrator(**settings)
            results = []
            for name in selected:
                item = next(table for table in tables if table["table"] == name)
                results.append(migrator.migrate_metadata_table(
                    st.session_state.bq_client.project, dataset, name,
                    item.get("schema", []), item.get("partition_field"),
                    item.get("clustering_fields", []),
                ))
            errors = [f"{name}: {error}" for name, result in zip(selected, results)
                      for error in result.errors]
            combined = {
                "sql": "\n\n".join(result.sql for result in results) if not errors else "",
                "changes": [change for result in results for change in result.changes],
                "warnings": list(dict.fromkeys(warning for result in results for warning in result.warnings)),
                "errors": errors,
                "statement_count": len(results),
                "fingerprint": fingerprint,
            }
            st.session_state.bq_ddl_metadata_result = combined
        except (TypeError, ValueError) as exc:
            st.session_state.bq_ddl_metadata_result = {
                "sql": "", "changes": [], "warnings": [],
                "errors": [f"Conversion settings: {exc}"],
                "statement_count": 0, "fingerprint": fingerprint,
            }
    filename = f"bigquery_{dataset}_to_redshift_{datetime.now().strftime('%Y%m%d_%H%M%S')}.sql"
    _render_result(st.session_state.get("bq_ddl_metadata_result"), "", fingerprint,
                   "bq_ddl_meta", filename)


def render_ddl_migrator():
    st.markdown("### BigQuery → Redshift DDL Migrator")
    st.caption("Convert flat BigQuery CREATE TABLE definitions to Redshift DDL. A Redshift connection is not required.")
    st.info("The generated schema matches the scalar types supported by this POC's BigQuery → Parquet → Redshift path. Nested/repeated fields, BIGNUMERIC, CTAS, replacement DDL and table constraints are reported for manual design.")
    paste, metadata = st.tabs(["Paste or upload DDL", "Connected BigQuery tables"])
    with paste, tab_loading():
        _paste_tab()
    with metadata, tab_loading():
        _metadata_tab()
