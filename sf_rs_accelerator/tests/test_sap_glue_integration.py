"""Local integration-contract tests for the SAP Streamlit -> AWS Glue service.

These tests mock boto3 and never contact AWS or SAP.
Run from the project root with:
    python -m unittest discover -s tests -p 'test_*.py' -v
"""
import unittest
from unittest.mock import Mock, patch

from services.sap.extraction_service import (
    get_sap_glue_job_status,
    start_sap_glue_job,
)


class TestSapGlueIntegration(unittest.TestCase):
    def setUp(self):
        self.sap_config = {
            "token_url": "https://sap.example/oauth/token",
            "client_id": "test-client-id",
            "client_secret": "test-client-secret",
            "odata_base_url": "https://sap.example/odata/v2/service",
            "entity_set": "Products",
        }
        self.glue = Mock()

    @patch("services.sap.extraction_service.boto3.client")
    def test_start_job_passes_expected_arguments(self, boto_client):
        boto_client.return_value = self.glue
        self.glue.start_job_run.return_value = {"JobRunId": "jr_mock_123"}

        run_id = start_sap_glue_job(
            glue_job_name="sap-migration-test",
            sap_config=self.sap_config,
            aws_access_key="test-access-key",
            aws_secret_key="test-aws-secret",
            s3_base_path="s3://demo-bucket/migrations/sap",
            region="ap-south-1",
            page_size=1000,
            load_type="FULL",
            write_mode="overwrite",
            output_format="parquet",
        )

        self.assertEqual(run_id, "jr_mock_123")
        boto_client.assert_called_once_with(
            "glue",
            region_name="ap-south-1",
            aws_access_key_id="test-access-key",
            aws_secret_access_key="test-aws-secret",
        )
        call = self.glue.start_job_run.call_args.kwargs
        self.assertEqual(call["JobName"], "sap-migration-test")
        self.assertEqual(
            set(call["Arguments"]),
            {
                "--s3_base_path", "--token_url", "--client_id",
                "--client_secret", "--odata_base_url", "--entity_set",
                "--region", "--page_size", "--load_type",
                "--write_mode", "--output_format",
            },
        )
        self.assertEqual(call["Arguments"]["--entity_set"], "Products")
        self.assertEqual(call["Arguments"]["--client_secret"], "test-client-secret")
        self.assertEqual(call["Arguments"]["--s3_base_path"], "s3://demo-bucket/migrations/sap")

    @patch("services.sap.extraction_service.boto3.client")
    def test_status_retrieval_returns_glue_state(self, boto_client):
        boto_client.return_value = self.glue
        self.glue.get_job_run.return_value = {
            "JobRun": {
                "JobRunState": "SUCCEEDED",
                "StartedOn": "start-time",
                "CompletedOn": "end-time",
                "ErrorMessage": None,
            }
        }

        result = get_sap_glue_job_status(
            "sap-migration-test", "jr_mock_123", "test-access-key",
            "test-aws-secret", "ap-south-1",
        )

        self.assertEqual(result["status"], "SUCCEEDED")
        self.assertEqual(result["job_run_id"], "jr_mock_123")
        self.assertEqual(result["started_on"], "start-time")
        self.assertEqual(result["completed_on"], "end-time")

    @patch("services.sap.extraction_service.boto3.client")
    def test_missing_selected_entity_is_rejected_before_aws_call(self, boto_client):
        incomplete_config = dict(self.sap_config, entity_set="__DISCOVERY__")
        with self.assertRaisesRegex(ValueError, "real SAP OData entity"):
            start_sap_glue_job(
                "sap-migration-test", incomplete_config,
                "test-access-key", "test-aws-secret",
                "s3://demo-bucket/migrations/sap",
            )
        boto_client.assert_not_called()

    @patch("services.sap.extraction_service.boto3.client")
    def test_invalid_s3_path_is_rejected_before_aws_call(self, boto_client):
        with self.assertRaisesRegex(ValueError, "S3 destination"):
            start_sap_glue_job(
                "sap-migration-test", self.sap_config,
                "test-access-key", "test-aws-secret", "demo-bucket/path",
            )
        boto_client.assert_not_called()


if __name__ == "__main__":
    unittest.main()
