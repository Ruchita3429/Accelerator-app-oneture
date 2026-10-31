"""Pipeline routing and failure regressions; all cloud clients are mocked."""
import sys
import types
import unittest
from unittest.mock import Mock, patch

import chunk_02_job_queue_engine as engine
from pipeline_routes import (resolve_route, is_bigquery, s3_only,
                             SF_REDSHIFT, SF_S3, BQ_REDSHIFT, BQ_S3,
                             DB_REDSHIFT, DB_S3)
from bigquery_ui import build_queue


class State(dict):
    __getattr__ = dict.__getitem__
    __setattr__ = dict.__setitem__


def job(source="Snowflake", name="orders"):
    return dict(id=name, source_type=source, sf_schema="public", sf_table=name,
                rs_schema="public", rs_table=name, file_format="CSV",
                s3_key=name + ".csv", load_mode="TRUNCATE & LOAD",
                rows=2, bytes=10, status="pending")


S3 = dict(bucket="test", access_key="test-key", secret_key="test-secret", region="us-east-1")
RS = dict(workgroup="test", database="dev", region="us-east-1")


class DataAPI:
    def __init__(self, counts=(5, 2), fail=None, table_exists=True):
        self.sql = {}
        self.counts = iter(counts)
        self.fail = fail
        self.table_exists = table_exists

    def execute_statement(self, **kwargs):
        statement_id = str(len(self.sql))
        self.sql[statement_id] = kwargs["Sql"]
        return {"Id": statement_id}

    def describe_statement(self, Id):
        sql = self.sql[Id]
        if self.fail and self.fail in sql:
            return dict(Id=Id, Status="FAILED", Error="permission denied")
        return dict(Id=Id, Status="FINISHED", HasResultSet=sql.lstrip().startswith("SELECT"))

    def get_statement_result(self, Id):
        sql = self.sql[Id]
        if "svv_external_schemas" in sql:
            return {"Records": []}
        if "information_schema.tables" in sql:
            return {"Records": [[{"longValue": 1}]] if self.table_exists else []}
        if "COUNT(*)" in sql:
            value = next(self.counts)
            if isinstance(value, Exception):
                raise value
            return {"Records": [[{"longValue": value}]]}
        raise AssertionError(sql)


class PipelineFailures(unittest.TestCase):
    def test_bigquery_panels_build_queue_and_destination_change_clears_it(self):
        from pathlib import Path
        from streamlit.testing.v1 import AppTest
        app = AppTest.from_file(str(Path(__file__).resolve().parents[1] / "app_final.py"), default_timeout=60).run()
        app.selectbox(key="source_type_sel").select("BigQuery").run()
        app.session_state["bq_connected"] = True
        app.session_state["bq_client"] = types.SimpleNamespace(project="project")
        app.session_state["bq_datasets"] = ["dataset"]
        app.session_state["bq_tables"] = {"dataset": [dict(table="orders", location="US", schema=[])]}
        app.run()
        self.assertFalse(app.exception)
        app.multiselect(key="bq_queue_tables").select("dataset.orders").run()
        app.button(key="bq_queue_build").click().run()
        self.assertFalse(app.exception)
        self.assertEqual(BQ_REDSHIFT, app.session_state["job_queue"][0]["destination"])
        app.selectbox(key="pipeline_destination_sel").select("Source → S3 only").run()
        self.assertFalse(app.exception)
        self.assertEqual([], app.session_state["job_queue"])
        app.multiselect(key="bq_queue_tables").select("dataset.orders").run()
        app.button(key="bq_queue_build").click().run()
        self.assertFalse(app.exception)
        self.assertEqual(BQ_S3, app.session_state["job_queue"][0]["destination"])

    def test_generic_destinations_resolve_for_all_sources(self):
        for source, full, only in [("Snowflake", SF_REDSHIFT, SF_S3),
                                   ("Databricks", DB_REDSHIFT, DB_S3),
                                   ("BigQuery", BQ_REDSHIFT, BQ_S3)]:
            self.assertEqual(full, resolve_route("Source → S3 → Redshift", source))
            self.assertEqual(only, resolve_route("Source → S3 only", source))
            self.assertTrue(s3_only(only))
            self.assertFalse(s3_only(full))
            self.assertEqual(source == "BigQuery", is_bigquery(full))
            self.assertEqual(full, resolve_route(full, source))
        with self.assertRaises(ValueError):
            resolve_route("unknown")

    def test_bigquery_queue_normalizes_generic_destination(self):
        settings = dict(destination="Source → S3 → Redshift", file_format="Parquet", rs_schema="public")
        queue = build_queue("project", [("dataset", "orders")],
                            {"dataset": [dict(table="orders", location="US")]},
                            settings, S3, RS, "identity")
        self.assertEqual(BQ_REDSHIFT, queue[0]["destination"])
        with self.assertRaises(ValueError):
            build_queue("project", [], {}, {**settings, "file_format": "Iceberg"}, S3, RS, "identity")

    def test_databricks_read_failure_never_writes_sample_data(self):
        databricks = types.ModuleType("databricks")
        databricks.sql = Mock()
        databricks.sql.connect.side_effect = RuntimeError("source unavailable")
        for exporter in [engine._extract_databricks_to_s3, engine._extract_databricks_to_s3_iceberg]:
            with self.subTest(exporter=exporter.__name__), \
                    patch.dict(sys.modules, {"databricks": databricks}), \
                    patch("boto3.client") as aws, patch("iceberg_s3_writer.write_iceberg_table_to_s3") as iceberg:
                with self.assertRaisesRegex(RuntimeError, "Databricks read failed"):
                    exporter(job("Databricks"), {}, S3)
                aws.assert_not_called()
                iceberg.assert_not_called()

    def test_databricks_parquet_preserves_dates_decimals_and_nulls(self):
        from datetime import date, datetime
        from decimal import Decimal
        import pyarrow as pa
        import pyarrow.parquet as pq

        rows = [(date(2026, 10, 25), Decimal("12.50"), datetime(2026, 10, 25, 12, 30)),
                (None, None, None)]
        columns = ["ORDER_DATE", "AMOUNT", "UPDATED_AT"]
        databricks = types.ModuleType("databricks")
        databricks.sql = Mock()
        cursor = databricks.sql.connect.return_value.cursor.return_value
        cursor.description = [(name,) for name in columns]
        cursor.fetchall.return_value = rows
        with patch.dict(sys.modules, {"databricks": databricks}), patch("boto3.client") as aws:
            result = engine._extract_databricks_to_s3(
                {**job("Databricks"), "file_format": "Parquet"}, {"catalog": "main"}, S3)
            body = aws.return_value.put_object.call_args.kwargs["Body"]
        table = pq.read_table(pa.BufferReader(body))
        self.assertEqual(2, result["rows"])
        self.assertEqual([dict(zip(columns, row)) for row in rows], table.to_pylist())
        self.assertTrue(pa.types.is_date32(table.schema.field("ORDER_DATE").type))
        self.assertTrue(pa.types.is_decimal(table.schema.field("AMOUNT").type))

    def load(self, api, item=None):
        with patch("boto3.client", return_value=api), patch.object(engine, "_log"):
            return engine._load_s3_to_redshift(item if item is not None else job(), S3, RS)

    def test_truncate_failure_prevents_copy(self):
        api = DataAPI(fail="TRUNCATE")
        with self.assertRaisesRegex(RuntimeError, "truncate failed"):
            self.load(api)
        self.assertFalse(any(sql.lstrip().startswith("COPY") for sql in api.sql.values()))

    def test_create_failure_prevents_copy(self):
        api = DataAPI(fail="CREATE TABLE", table_exists=False)
        item = {**job(), "columns": ["id"], "dtypes": {"id": "int64"}}
        with self.assertRaisesRegex(RuntimeError, "creation failed"):
            self.load(api, item)
        self.assertFalse(any(sql.lstrip().startswith("COPY") for sql in api.sql.values()))

    def test_copy_failure_is_reported(self):
        api = DataAPI(fail="COPY")
        with self.assertRaisesRegex(RuntimeError, "COPY failed"):
            self.load(api)

    def test_verified_load_and_append_counts(self):
        for mode, counts in [("TRUNCATE & LOAD", (5, 2)), ("APPEND", (5, 7))]:
            api = DataAPI(counts=counts)
            result = self.load(api, {**job(), "load_mode": mode})
            self.assertEqual(2, result["rs_rows"])
            copy = next(sql for sql in api.sql.values() if sql.lstrip().startswith("COPY"))
            self.assertIn("MAXERROR 0", copy)

    def test_count_failure_is_not_reported_as_success_and_resume_does_not_copy_again(self):
        api = DataAPI(counts=(5, RuntimeError("count unavailable"), 2))
        item = job()
        with self.assertRaisesRegex(RuntimeError, "verification failed"):
            self.load(api, item)
        self.assertEqual(2, self.load(api, item)["rs_rows"])
        self.assertEqual(1, sum(sql.lstrip().startswith("COPY") for sql in api.sql.values()))
        self.assertEqual(1, sum(sql.lstrip().startswith("TRUNCATE") for sql in api.sql.values()))

    def test_count_mismatch_fails(self):
        with self.assertRaisesRegex(RuntimeError, "row-count mismatch"):
            self.load(DataAPI(counts=(5, 1)))

    def test_load_failure_stops_queue_and_remains_resumable(self):
        for source in ["Snowflake", "Databricks", "BigQuery"]:
            items = [job(source, "first"), job(source, "second")]
            state = State(job_queue=items, source_type=source,
                          pipeline_destination="Source → S3 → Redshift", checkpoint={})
            if source == "BigQuery":
                from bigquery_ui import config_fingerprint
                for item in items:
                    item.update(source_type="bigquery", bq_project="project", bq_dataset="public",
                                bq_table=item["id"], destination=BQ_REDSHIFT, file_format="Parquet",
                                config_fingerprint=config_fingerprint(S3, RS, BQ_REDSHIFT))
            with self.subTest(source=source), patch.object(engine, "st", Mock(session_state=state)), \
                    patch.object(engine, "_extract_snowflake_to_s3", return_value=dict(rows=2, bytes=10)), \
                    patch.object(engine, "_extract_databricks_to_s3", return_value=dict(rows=2, bytes=10)), \
                    patch("bigquery_migration.extract_to_s3", return_value=dict(rows=2, bytes=10)), \
                    patch.object(engine, "_load_s3_to_redshift", side_effect=RuntimeError("COPY denied")) as load, \
                    patch("bigquery_migration.load_to_redshift", side_effect=RuntimeError("COPY denied")) as bq_load:
                progress = Mock()
                self.assertFalse(engine.run_migration({}, S3, RS, progress, Mock()))
                self.assertFalse(state.migration_done)
                self.assertFalse(state.migration_running)
                self.assertEqual(["failed", "pending"], [j["status"] for j in items])
                self.assertEqual(1, state.migration_summary["pending"])
                self.assertNotIn("complete ✓", progress.progress.call_args.kwargs["text"])
                load.side_effect = bq_load.side_effect = None
                self.assertTrue(engine.run_migration({}, S3, RS, progress, Mock()))
                self.assertEqual(2, state.migration_summary["completed"])

    def test_generic_s3_only_never_loads_redshift(self):
        state = State(job_queue=[job()], source_type="Snowflake", checkpoint={},
                      pipeline_destination="Source → S3 only")
        with patch.object(engine, "st", Mock(session_state=state)), \
                patch.object(engine, "_extract_snowflake_to_s3", return_value=dict(rows=2, bytes=10)), \
                patch.object(engine, "_load_s3_to_redshift") as load:
            self.assertTrue(engine.run_migration({}, S3, RS, Mock(), Mock()))
            load.assert_not_called()


if __name__ == "__main__":
    unittest.main()
