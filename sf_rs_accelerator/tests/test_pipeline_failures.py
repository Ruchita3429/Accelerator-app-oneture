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
    def setUp(self):
        # Pipeline tests use placeholder AWS credentials. Snapshot behavior is
        # covered separately with a mocked S3 client in test_duckdb_metrics.
        metrics = patch.object(engine, "_metrics_call", return_value="test-attempt")
        patcher = patch.object(engine, "_metrics_finish_run")
        metrics.start()
        patcher.start()
        self.addCleanup(metrics.stop)
        self.addCleanup(patcher.stop)

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

    def test_classify_failure_buckets(self):
        fatal = [
            "S3→RS failed: COPY denied",
            "permission denied for schema public",
            "Target schema is an external schema",
            "ExpiredToken: the security token included in the request is expired",
            "could not connect to workgroup",
        ]
        per_table = [
            "Target row-count mismatch: expected 100 loaded rows, found 400.",
            "String length exceeds DDL length",
            "Invalid digit, value 'x'",
            "error 100083: max file size exceeded",
            "COPY finished but target row-count verification failed",
            "ERROR: permission denied for relation employees",
            "ERROR: must be owner of relation orders",
            "COPY failed: ERROR: Spectrum Scan Error code: 15007 Unmatched number of columns "
            "between table and file. Table columns: 10, Data columns: 5",
        ]
        for msg in fatal:
            self.assertEqual("fatal", engine._classify_failure(msg), msg)
        for msg in per_table:
            self.assertEqual("table", engine._classify_failure(msg), msg)
        # Unknown errors default to fatal (safe stop-on-failure behaviour).
        self.assertEqual("fatal", engine._classify_failure("something totally unexpected"))

    def test_short_error_trims_noise_and_url(self):
        raw = ("COPY failed: ERROR: Spectrum Scan Error code: 15007 context: Unmatched number "
               "of columns between table and file. Table columns: 10, Data columns: 5, "
               "File name: https://s3.ap-south-1.amazonaws.com/bucket/public/employees/data/x.parquet "
               "query: 25793426 location: dory_util.cpp:2029 process: worker_thread [pid=18637]")
        short = engine._short_error(raw)
        self.assertNotIn("https://", short)
        self.assertNotIn("worker_thread", short)
        self.assertLessEqual(len(short), 260)
        self.assertIn("15007", short)

    def test_per_table_failure_continues_and_remaining_tables_succeed(self):
        items = [job("Snowflake", "first"), job("Snowflake", "second")]
        state = State(job_queue=items, source_type="Snowflake",
                      pipeline_destination="Source → S3 → Redshift", checkpoint={})
        calls = {"n": 0}

        def load_side_effect(item, *_a, **_k):
            calls["n"] += 1
            if item["id"] == "first":
                raise RuntimeError("Target row-count mismatch: expected 2 loaded rows, found 8.")
            return dict(rs_rows=2, rs_duration_secs=0.1)

        with patch.object(engine, "st", Mock(session_state=state)), \
                patch.object(engine, "_extract_snowflake_to_s3", return_value=dict(rows=2, bytes=10)), \
                patch.object(engine, "_load_s3_to_redshift", side_effect=load_side_effect):
            engine.run_migration({}, S3, RS, Mock(), Mock())

        # First skipped (per-table), second still migrated — queue did not stop.
        self.assertEqual(["failed", "done"], [j["status"] for j in items])
        self.assertEqual("table", items[0]["fail_kind"])
        self.assertEqual(2, calls["n"])
        self.assertEqual(1, state.migration_summary["completed"])
        self.assertEqual(0, state.migration_summary["pending"])

    def test_per_relation_permission_skips_one_but_escalates_when_repeated(self):
        # A single per-relation permission error skips just that table.
        items = [job("Snowflake", "blocked"), job("Snowflake", "ok")]
        state = State(job_queue=items, source_type="Snowflake",
                      pipeline_destination="Source → S3 → Redshift", checkpoint={})

        def one_block(item, *_a, **_k):
            if item["id"] == "blocked":
                raise RuntimeError("ERROR: permission denied for relation blocked")
            return dict(rs_rows=2, rs_duration_secs=0.1)

        with patch.object(engine, "st", Mock(session_state=state)), \
                patch.object(engine, "_extract_snowflake_to_s3", return_value=dict(rows=2, bytes=10)), \
                patch.object(engine, "_load_s3_to_redshift", side_effect=one_block):
            engine.run_migration({}, S3, RS, Mock(), Mock())
        self.assertEqual(["failed", "done"], [j["status"] for j in items])
        self.assertEqual("table", items[0]["fail_kind"])

        # Four tables all blocked by permission — should stop after the limit
        # instead of attempting every one.
        many = [job("Snowflake", f"t{i}") for i in range(5)]
        state2 = State(job_queue=many, source_type="Snowflake",
                       pipeline_destination="Source → S3 → Redshift", checkpoint={})
        calls = {"n": 0}

        def all_block(item, *_a, **_k):
            calls["n"] += 1
            raise RuntimeError("ERROR: permission denied for relation " + item["id"])

        with patch.object(engine, "st", Mock(session_state=state2)), \
                patch.object(engine, "_extract_snowflake_to_s3", return_value=dict(rows=2, bytes=10)), \
                patch.object(engine, "_load_s3_to_redshift", side_effect=all_block):
            engine.run_migration({}, S3, RS, Mock(), Mock())
        # Stops at the 3rd permission failure; tables 4 and 5 never attempted.
        self.assertEqual(3, calls["n"])
        self.assertEqual(2, state2.migration_summary["pending"])

    def test_redshift_types_follow_the_exported_data_not_dtype_guesses(self):
        from datetime import date, datetime
        from decimal import Decimal
        import pandas as pd
        df = pd.DataFrame({
            "ID": [1, 2],
            "SALARY": [Decimal("10.50"), Decimal("20.25")],
            "JOINED": [date(2026, 1, 1), date(2026, 1, 2)],
            "PAID_AT": [datetime(2026, 1, 1, 12, 30), datetime(2026, 1, 2, 8, 15)],
            "NAME": ["a", "b"],
        })
        types = engine._infer_redshift_types(df)
        self.assertEqual("BIGINT", types["ID"])
        self.assertEqual("DECIMAL(4,2)", types["SALARY"])
        self.assertEqual("DATE", types["JOINED"])
        self.assertEqual("TIMESTAMP", types["PAID_AT"])
        self.assertEqual("VARCHAR(65535)", types["NAME"])
        # Regression: pandas 3 timestamp dtypes must never be mistaken for DATE.
        for dt in ("datetime64[ns]", "datetime64[us]", "datetime64[s]", "datetime64[ms]"):
            self.assertEqual("TIMESTAMP", engine._redshift_type_from_dtype(dt), dt)
        self.assertEqual("BIGINT", engine._redshift_type_from_dtype("int64"))
        self.assertEqual("VARCHAR(65535)", engine._redshift_type_from_dtype("object"))

    def _mismatch_api(self, counts):
        class API(DataAPI):
            copy_calls = 0

            def describe_statement(self, Id):
                if self.sql[Id].lstrip().startswith("COPY"):
                    API.copy_calls += 1
                    if API.copy_calls == 1:
                        return dict(Id=Id, Status="FAILED", Error=(
                            "Spectrum Scan Error code: 15007 has an incompatible Parquet schema for column"))
                return super().describe_statement(Id)

            def get_statement_result(self, Id):
                if "pg_tables" in self.sql[Id]:
                    return {"Records": [[{"longValue": 1}]]}
                return super().get_statement_result(Id)

        return API(counts=counts)

    def test_type_mismatch_rebuilds_empty_owned_table_and_retries_once(self):
        api = self._mismatch_api(counts=(0, 0, 2))
        item = {**job(), "columns": ["ID", "SALARY"], "dtypes": {"ID": "int64", "SALARY": "object"},
                "rs_types": {"ID": "BIGINT", "SALARY": "DECIMAL(4,2)"}}
        result = self.load(api, item)
        self.assertEqual(2, result["rs_rows"])
        statements = list(api.sql.values())
        self.assertEqual(2, sum(s.lstrip().startswith("COPY") for s in statements))
        create = next(s for s in statements if s.lstrip().startswith("CREATE TABLE"))
        self.assertIn('"SALARY" DECIMAL(4,2)', create)
        self.assertTrue(any(s.lstrip().startswith("DROP TABLE") for s in statements))

    def test_type_mismatch_never_drops_a_table_that_holds_data(self):
        api = self._mismatch_api(counts=(5, 5))
        item = {**job(), "load_mode": "APPEND", "columns": ["ID"], "dtypes": {"ID": "int64"}}
        with self.assertRaisesRegex(RuntimeError, "COPY failed"):
            self.load(api, item)
        self.assertFalse(any(s.lstrip().startswith("DROP TABLE") for s in api.sql.values()))

    def test_copy_statement_is_valid_for_every_format(self):
        expectations = {
            "CSV": ("FORMAT AS CSV", True),
            "PARQUET": ("FORMAT AS PARQUET", False),
            "ORC": ("FORMAT AS ORC", False),
            "JSON": ("FORMAT AS JSON 'auto ignorecase'", True),
            "ICEBERG": ("FORMAT AS PARQUET", False),
        }
        for fmt, (clause, allows_compupdate) in expectations.items():
            with self.subTest(fmt=fmt):
                api = DataAPI(counts=(5, 2))
                item = {**job(), "file_format": fmt}
                if fmt == "ICEBERG":
                    item.update(s3_key="p/public/orders/", iceberg_data_key="p/public/orders/data/x-00000.parquet")
                self.load(api, item)
                copy = next(s for s in api.sql.values() if s.lstrip().startswith("COPY"))
                self.assertIn(clause, copy)
                self.assertEqual(allows_compupdate, "COMPUPDATE" in copy)
                if fmt == "ICEBERG":
                    self.assertIn("data/x-00000.parquet", copy)

    def test_snowflake_orc_fails_fast_and_missing_table_is_per_table(self):
        # Server-side unload (used only for partitioned exports) cannot write ORC.
        with self.assertRaisesRegex(RuntimeError, "cannot unload partitioned data to ORC"):
            engine._extract_snowflake_to_s3(
                {**job(), "file_format": "ORC", "use_partitions": True, "partition_cols": "region"}, {}, S3)
        self.assertEqual("table", engine._classify_failure(
            "Target table 'public.x' does not exist in Redshift. Please create the table first."))
        self.assertEqual("fatal", engine._classify_failure('schema "nope" does not exist'))

    def test_snowflake_non_partitioned_export_reads_in_python_with_known_types(self):
        from datetime import datetime, timezone
        import pyarrow as pa
        import pyarrow.parquet as pq
        rows = [(1, datetime(2026, 1, 1, 12, 30, tzinfo=timezone.utc)),
                (2, datetime(2026, 1, 2, 8, 15, tzinfo=timezone.utc))]
        for fmt in ("PARQUET", "CSV", "JSON", "ORC"):
            with self.subTest(fmt=fmt):
                cursor = Mock(description=[("ID",), ("PAID_AT",)])
                cursor.fetchall.return_value = rows
                conn = Mock()
                conn.cursor.return_value = cursor
                item = {**job(), "file_format": fmt, "s3_key": "x/payments." + fmt.lower()}
                with patch("snowflake.connector.connect", return_value=conn), \
                        patch("boto3.client") as aws, patch.object(engine, "_log"):
                    result = engine._extract_snowflake_to_s3(item, {}, S3)
                body = aws.return_value.put_object.call_args.kwargs["Body"]
                self.assertEqual(2, result["rows"])
                self.assertEqual(len(body), result["bytes"])
                self.assertEqual(["ID", "PAID_AT"], item["columns"])
                # TIMESTAMP_LTZ/TZ values become TIMESTAMPTZ; no Snowflake Parquet limit applies.
                self.assertEqual("TIMESTAMPTZ", item["rs_types"]["PAID_AT"])
                self.assertEqual("BIGINT", item["rs_types"]["ID"])
                if fmt == "PARQUET":
                    self.assertEqual(2, pq.read_table(pa.BufferReader(body)).num_rows)

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
