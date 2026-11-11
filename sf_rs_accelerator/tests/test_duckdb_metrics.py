"""Durable migration metrics storage tests; no AWS services are contacted."""
from concurrent.futures import ThreadPoolExecutor
import json
from pathlib import Path
import tempfile
import unittest
from datetime import datetime, timezone

from metrics_repository import MetricsRepository, sanitize_error
import chunk_02_job_queue_engine as engine
from unittest.mock import Mock, patch


class State(dict):
    __getattr__ = dict.__getitem__
    __setattr__ = dict.__setitem__


class DuckDBMetricsTests(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.path = Path(self.temp_dir.name) / "metrics.duckdb"
        self.repository = MetricsRepository(self.path)

    def tearDown(self):
        self.repository.close()
        self.temp_dir.cleanup()

    def _finished_attempt(self, run_id, job_id, rows, size, status="done"):
        attempt = self.repository.start_attempt(
            run_id, job_id, "Snowflake.PUBLIC", "Redshift.PUBLIC", "source_to_s3"
        )
        self.repository.finish_attempt(
            attempt,
            status=status,
            current_stage="completed" if status == "done" else "failed",
            started_at=datetime.now(timezone.utc).isoformat(),
            rows_read=rows,
            rows_written=rows if status == "done" else None,
            bytes_processed=size,
            validation_result="passed" if status == "done" else "not_run",
            error_details="upload failed" if status != "done" else None,
        )

    def test_run_and_attempts_persist_after_reopen_and_checkpoint(self):
        self.repository.start_run("run-1", "Snowflake", "Redshift")
        self._finished_attempt("run-1", "PUBLIC.ORDERS", 4, 80)
        self.repository.finish_run("run-1", status="completed", current_stage="completed")
        snapshot = json.loads(self.repository.checkpoint_snapshot("run-1"))
        self.assertEqual("run-1", snapshot["run"][0]["run_id"])
        self.assertEqual(4, snapshot["run"][0]["rows_written"])

        self.repository.close()
        self.repository = MetricsRepository(self.path)
        run = self.repository.recent_runs()[0]
        self.assertEqual("completed", run["status"])
        self.assertEqual(4, run["rows_read"])
        self.assertEqual(80, run["bytes_processed"])
        self.assertIsNone(run["rows_rejected"])
        self.assertEqual(1, len(self.repository.attempts_for_run("run-1")))

    def test_retry_aggregates_latest_attempt_without_double_counting(self):
        self.repository.start_run("run-retry", "Databricks", "S3")
        self._finished_attempt("run-retry", "PUBLIC.EVENTS", 9, 900, "failed")
        self._finished_attempt("run-retry", "PUBLIC.EVENTS", 5, 500)
        self.repository.finish_run("run-retry", status="completed", current_stage="completed")
        run = self.repository.recent_runs()[0]
        self.assertEqual(5, run["rows_read"])
        self.assertEqual(5, run["rows_written"])
        self.assertEqual(500, run["bytes_processed"])
        self.assertEqual("passed", run["validation_result"])

    def test_manual_export_and_load_metrics_roll_up_without_losing_source_rows(self):
        self.repository.start_run("run-manual", "Snowflake", "Redshift")
        export_attempt = self.repository.start_attempt(
            "run-manual", "PUBLIC.ORDERS", "PUBLIC.ORDERS", "public.orders",
            "export", operation="export",
        )
        self.repository.finish_attempt(
            export_attempt,
            status="done",
            current_stage="export_complete",
            started_at=datetime.now(timezone.utc).isoformat(),
            rows_read=12,
            rows_written=None,
            bytes_processed=240,
        )
        load_attempt = self.repository.start_attempt(
            "run-manual", "PUBLIC.ORDERS", "S3", "public.orders",
            "load", operation="load",
        )
        self.repository.finish_attempt(
            load_attempt,
            status="done",
            current_stage="load_complete",
            started_at=datetime.now(timezone.utc).isoformat(),
            rows_written=12,
            bytes_processed=None,
        )
        self.repository.finish_run("run-manual", status="completed", current_stage="completed")
        run = self.repository.recent_runs()[0]
        self.assertEqual(12, run["rows_read"])
        self.assertEqual(12, run["rows_written"])
        self.assertEqual(240, run["bytes_processed"])
        self.assertEqual({"export", "load"}, {
            attempt["operation"] for attempt in self.repository.attempts_for_run("run-manual")
        })

    def test_concurrent_callers_are_serialized_by_single_writer(self):
        self.repository.start_run("run-concurrent", "BigQuery", "S3")

        def write(index):
            self._finished_attempt("run-concurrent", f"dataset.table_{index}", 1, 10)

        with ThreadPoolExecutor(max_workers=8) as executor:
            list(executor.map(write, range(16)))

        self.repository.finish_run(
            "run-concurrent", status="completed", current_stage="completed"
        )
        run = self.repository.recent_runs()[0]
        self.assertEqual(16, run["rows_read"])
        self.assertEqual(16, run["rows_written"])
        self.assertEqual(16, len(self.repository.attempts_for_run("run-concurrent")))

    def test_running_records_are_marked_interrupted_after_restart(self):
        self.repository.start_run("run-interrupted", "Snowflake", "S3")
        self.repository.start_attempt(
            "run-interrupted", "PUBLIC.OPEN", "PUBLIC.OPEN", "S3", "source_to_s3"
        )
        self.repository.close()
        self.repository = MetricsRepository(self.path)
        run = next(run for run in self.repository.recent_runs() if run["run_id"] == "run-interrupted")
        attempt = self.repository.attempts_for_run("run-interrupted")[0]
        self.assertEqual("interrupted", run["status"])
        self.assertIsNotNone(run["end_time"])
        self.assertEqual("interrupted", attempt["status"])
        self.assertIsNotNone(attempt["end_time"])

    def test_existing_database_schema_upgrades_operation_field(self):
        import duckdb

        self.repository.close()
        connection = duckdb.connect(str(self.path))
        connection.execute("ALTER TABLE migration_attempts DROP COLUMN operation")
        connection.close()
        self.repository = MetricsRepository(self.path)
        columns = self.repository._submit(
            lambda connection: [
                column[0] for column in connection.execute(
                    "DESCRIBE migration_attempts"
                ).fetchall()
            ]
        )
        self.assertIn("operation", columns)

    def test_failed_transaction_rolls_back_and_writer_remains_usable(self):
        self.repository.start_run("run-rollback", "Snowflake", "S3")

        def broken_write(connection):
            connection.execute(
                "UPDATE migration_runs SET status='corrupt' WHERE run_id='run-rollback'"
            )
            raise RuntimeError("force rollback")

        with self.assertRaisesRegex(RuntimeError, "force rollback"):
            self.repository._submit(broken_write)
        self.assertEqual("running", self.repository.run_status("run-rollback")[0])
        self.repository.start_run("run-after-rollback", "Snowflake", "S3")
        self.assertEqual("running", self.repository.run_status("run-after-rollback")[0])

    def test_error_details_redact_credentials_and_bound_length(self):
        error = sanitize_error(
            "Access denied password=hidden AKIA1234567890ABCDEF "
            "token=abc Bearer eyJhbGciOiJIUzI1NiJ9",
            secrets=["hidden"],
        )
        self.assertNotIn("hidden", error)
        self.assertNotIn("AKIA1234567890ABCDEF", error)
        self.assertNotIn("eyJhbGciOiJIUzI1NiJ9", error)
        self.assertLessEqual(len(sanitize_error("x" * 2000)), 1000)

    def test_pipeline_writes_run_metrics_and_uploads_versioned_snapshot(self):
        state = State({
            "job_queue": [{
                "id": "PUBLIC.ORDERS",
                "sf_schema": "PUBLIC",
                "sf_table": "ORDERS",
                "rs_schema": "public",
                "rs_table": "orders",
                "file_format": "Parquet",
                "s3_key": "exports/PUBLIC/ORDERS.parquet",
                "status": "pending",
                "rows": 0,
                "bytes": 0,
                "duration_secs": 0,
                "error": "",
            }],
            "source_type": "Snowflake",
            "pipeline_destination": "Source → S3 only",
            "checkpoint": {},
            "migration_running": False,
            "migration_done": False,
            "migration_summary": {},
            "logs": [],
        })
        s3_cfg = {
            "bucket": "metrics-test-bucket",
            "prefix": "exports",
            "region": "us-east-1",
            "access_key": "test-key",
            "secret_key": "test-secret",
        }
        with patch("metrics_repository._repository", self.repository), \
             patch.object(engine, "st", Mock(session_state=state)), \
             patch.object(engine, "_extract_snowflake_to_s3",
                          return_value={"rows": 7, "bytes": 140}), \
             patch("boto3.client") as aws:
            succeeded = engine.run_migration({}, s3_cfg, {}, Mock(), Mock())

        self.assertTrue(succeeded)
        run = self.repository.recent_runs()[0]
        self.assertEqual("completed", run["status"])
        self.assertEqual(7, run["rows_read"])
        self.assertEqual(7, run["rows_written"])
        self.assertEqual(140, run["bytes_processed"])
        self.assertEqual("not_run", run["validation_result"])
        uploaded = aws.return_value.put_object.call_args.kwargs
        self.assertEqual("metrics-test-bucket", uploaded["Bucket"])
        self.assertIn("exports/_databridge_metrics/", uploaded["Key"])
        self.assertTrue(uploaded["Key"].endswith(".json"))

    def test_failed_pipeline_persists_redacted_error(self):
        state = State({
            "job_queue": [{
                "id": "PUBLIC.BAD",
                "sf_schema": "PUBLIC",
                "sf_table": "BAD",
                "rs_schema": "public",
                "rs_table": "bad",
                "file_format": "Parquet",
                "s3_key": "exports/PUBLIC/BAD.parquet",
                "status": "pending",
                "rows": 0,
                "bytes": 0,
                "duration_secs": 0,
                "error": "",
            }],
            "source_type": "Snowflake",
            "pipeline_destination": "Source → S3 only",
            "checkpoint": {},
            "migration_running": False,
            "migration_done": False,
            "migration_summary": {},
            "logs": [],
        })
        s3_cfg = {
            "bucket": "metrics-test-bucket",
            "prefix": "exports",
            "region": "us-east-1",
            "access_key": "test-key",
            "secret_key": "private-secret",
        }
        with patch("metrics_repository._repository", self.repository), \
             patch.object(engine, "st", Mock(session_state=state)), \
             patch.object(engine, "_extract_snowflake_to_s3",
                          side_effect=RuntimeError("password=private-secret denied")), \
             patch("boto3.client"):
            self.assertFalse(engine.run_migration({}, s3_cfg, {}, Mock(), Mock()))

        attempt = self.repository.attempts_for_run(state["migration_run_id"])[0]
        self.assertEqual("failed", attempt["status"])
        self.assertNotIn("private-secret", attempt["error_details"])
        self.assertIn("[REDACTED]", attempt["error_details"])

    def test_snapshot_upload_failure_is_visible_and_retryable(self):
        state = State(logs=[])
        self.repository.start_run("run-backup-retry", "BigQuery", "S3")
        self.repository.finish_run(
            "run-backup-retry", status="completed", current_stage="completed"
        )
        aws = Mock()
        aws.return_value.put_object.side_effect = [
            RuntimeError("AccessDenied"),
            None,
        ]
        s3_cfg = {
            "bucket": "metrics-test-bucket",
            "prefix": "exports",
            "region": "us-east-1",
            "access_key": "test-key",
            "secret_key": "test-secret",
        }
        with patch("metrics_repository._repository", self.repository), \
             patch.object(engine, "st", Mock(session_state=state)), \
             patch("boto3.client", aws):
            self.assertFalse(engine.retry_metrics_snapshot("run-backup-retry", s3_cfg))
            self.assertIn("AccessDenied", state["metrics_backup_error"])
            self.assertTrue(engine.retry_metrics_snapshot("run-backup-retry", s3_cfg))
        self.assertNotIn("metrics_backup_error", state)
        self.assertIn("s3://metrics-test-bucket/", state["metrics_backup_status"])
        self.assertEqual(2, aws.return_value.put_object.call_count)


if __name__ == "__main__":
    unittest.main()
