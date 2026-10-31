"""Manual-tab transfers must update the same records displayed by Monitor."""
import unittest
from unittest.mock import Mock, patch

import chunk_02_job_queue_engine as engine
from chunk_01_table_discovery import _build_job_queue


class State(dict):
    __getattr__ = dict.__getitem__
    __setattr__ = dict.__setitem__


def state_for(destination):
    queue = _build_job_queue({"PUBLIC": [f"ORDERS_{i}" for i in range(9)]},
                             {"file_format": "Parquet", "rs_schema": "public"}, "exports")
    return State(job_queue=queue, source_type="Snowflake", pipeline_destination=destination)


def export(job, **kwargs):
    engine.record_manual_transfer("export", source_schema=job["sf_schema"], source_table=job["sf_table"],
                                  s3_key=job["s3_key"], s3_bucket="test", rows=1000, bytes=2000,
                                  duration_secs=1, **kwargs)


class ManualMonitoring(unittest.TestCase):
    def test_nine_s3_only_exports_complete_the_monitor_queue(self):
        state = state_for("Source → S3 only")
        with patch.object(engine, "st", Mock(session_state=state)):
            for job in state.job_queue:
                export(job)
        self.assertEqual(["done"] * 9, [j["status"] for j in state.job_queue])
        self.assertTrue(state.migration_done)
        self.assertEqual(9, state.migration_summary["completed"])
        self.assertEqual(9000, state.migration_summary["total_rows"])
        self.assertTrue(all(state.checkpoint[j["id"]] == "done" for j in state.job_queue))

    def test_full_pipeline_waits_for_redshift_and_then_completes(self):
        state = state_for("Source → S3 → Redshift")
        with patch.object(engine, "st", Mock(session_state=state)):
            for job in state.job_queue:
                export(job)
            self.assertFalse(state.migration_done)
            self.assertTrue(all(j["export_complete"] for j in state.job_queue))
            self.assertTrue(all(j["phase"] == "exported to S3; awaiting Redshift" for j in state.job_queue))
            for job in state.job_queue:
                engine.record_manual_transfer("load", s3_key=job["s3_key"], s3_bucket="test", rs_duration_secs=2)
        self.assertTrue(state.migration_done)
        self.assertEqual(9, state.migration_summary["completed"])
        self.assertEqual(27, state.migration_summary["total_secs"])

    def test_failed_export_updates_only_matching_schema(self):
        state = state_for("Source → S3 only")
        duplicate = dict(state.job_queue[0], id="OTHER.ORDERS_0", sf_schema="OTHER", source_schema="OTHER")
        state.job_queue.append(duplicate)
        with patch.object(engine, "st", Mock(session_state=state)):
            first = state.job_queue[0]
            export(first)
            engine.record_manual_transfer("export", source_schema="PUBLIC", source_table="ORDERS_0", error="upload denied")
        self.assertEqual("failed", first["status"])
        self.assertNotIn("export_complete", first)
        self.assertEqual("pending", duplicate["status"])
        self.assertFalse(state.migration_done)

    def test_different_bucket_does_not_complete_a_queued_load(self):
        state = state_for("Source → S3 → Redshift")
        with patch.object(engine, "st", Mock(session_state=state)):
            first = state.job_queue[0]
            export(first)
            engine.record_manual_transfer("load", s3_key=first["s3_key"], s3_bucket="other")
        self.assertEqual("pending", first["status"])

    def test_monitor_displays_export_stage_without_claiming_redshift_completion(self):
        from streamlit.testing.v1 import AppTest
        app = AppTest.from_string('''
import streamlit as st
from chunk_01_table_discovery import _build_job_queue
from chunk_02_job_queue_engine import record_manual_transfer
from chunk_03_monitoring_ui import render_monitoring_tab
st.session_state.source_type = "Snowflake"
st.session_state.pipeline_destination = "Source → S3 → Redshift"
st.session_state.job_queue = _build_job_queue({"PUBLIC": [f"ORDERS_{i}" for i in range(9)]}, {}, "exports")
for job in st.session_state.job_queue:
    record_manual_transfer("export", source_schema="PUBLIC", source_table=job["sf_table"],
                           s3_key=job["s3_key"], rows=1000, bytes=2000)
render_monitoring_tab()
''').run()
        self.assertFalse(app.exception)
        self.assertIn("9 exported to S3; awaiting Redshift", app.get("progress")[0].proto.text)
        self.assertEqual(0, app.get("progress")[0].proto.value)
        self.assertTrue(all("awaiting Redshift" in status for status in app.dataframe[0].value["Status"]))


if __name__ == "__main__":
    unittest.main()
