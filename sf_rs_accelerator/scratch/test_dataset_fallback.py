"""
Test export flow under blocked pyarrow.dataset
"""
import sys, os
sys.path.insert(0, os.path.abspath("."))

import mmh3_compat
import chunk_02_job_queue_engine as c2

# Test fallback logic in _extract_databricks_to_s3_iceberg
job = {
    "source_schema": "default",
    "source_table": "customers",
    "sf_schema": "default",
    "sf_table": "customers",
    "file_format": "Iceberg",
    "s3_key": "pipeline/default/customers/",
}

db_cfg = {"host": "test.cloud.databricks.com", "http_path": "/test", "token": "dapi123"}
s3_cfg = {
    "bucket": "my-test-bucket",
    "prefix": "pipeline",
    "access_key": "AKIAIOSFODNN7EXAMPLE",
    "secret_key": "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
    "region": "us-east-1",
}

import unittest.mock as mock
with mock.patch("boto3.client") as mock_boto, \
     mock.patch("pyiceberg.catalog.load_catalog") as mock_cat_loader, \
     mock.patch.dict("sys.modules", {"databricks.sql": mock.MagicMock(connect=mock.MagicMock(side_effect=Exception("Simulated offline")))}):
    mock_s3 = mock.MagicMock()
    mock_boto.return_value = mock_s3
    
    mock_catalog = mock.MagicMock()
    mock_cat_loader.return_value = mock_catalog
    mock_catalog.namespace_exists.return_value = True
    mock_catalog.table_exists.return_value = True
    
    mock_tbl = mock.MagicMock()
    mock_catalog.load_table.return_value = mock_tbl
    mock_tbl.overwrite.side_effect = ImportError("The pyarrow installation is not built with support for 'dataset' (DLL load failed while importing _dataset: An Application Control policy has blocked this file.)")

    # Run _extract_databricks_to_s3_iceberg which will hit pyarrow._dataset WDAC block
    res = c2._extract_databricks_to_s3_iceberg(job, db_cfg, s3_cfg)
    print("Databricks Iceberg result under WDAC block:", res)
    assert res.get("fallback") is True
    assert mock_s3.put_object.called
    print("put_object called with key:", mock_s3.put_object.call_args[1]["Key"])
    print("ALL TESTS PASSED SUCCESSFULLY!")
