"""Round-trip export types without connecting to S3 or Glue."""
import csv
import io
import json
import sys
import types
import unittest
from datetime import date, datetime
from decimal import Decimal
from unittest.mock import Mock, patch

import chunk_02_job_queue_engine as engine
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pyarrow.orc as orc
from iceberg_s3_writer import write_iceberg_table_to_s3


S3 = dict(bucket="test", prefix="exports", access_key="test-key",
          secret_key="test-secret", region="us-east-1")
COLUMNS = ["ORDER_DATE", "AMOUNT", "UPDATED_AT", "EMPTY"]
ROWS = [(date(2026, 10, 25), Decimal("1234567890123.45"), datetime(2026, 10, 25, 12, 30), None),
        (None, None, None, None)]


class FormatExports(unittest.TestCase):
    def test_iceberg_data_and_catalog_use_the_same_types(self):
        s3, glue = Mock(), Mock()
        uploads = {}
        s3.put_object.side_effect = lambda **kw: uploads.update({kw["Key"]: kw["Body"]})
        with patch("iceberg_s3_writer.boto3.client", side_effect=lambda service, **kw: s3 if service == "s3" else glue):
            result = write_iceberg_table_to_s3(pd.DataFrame(ROWS, columns=COLUMNS), S3, "public", "orders")

        table = pq.read_table(pa.BufferReader(uploads[result["data_key"]]))
        self.assertEqual([dict(zip(COLUMNS, row)) for row in ROWS], table.to_pylist())
        self.assertTrue(pa.types.is_date32(table.schema.field("ORDER_DATE").type))
        self.assertTrue(pa.types.is_decimal(table.schema.field("AMOUNT").type))
        self.assertEqual(b"1", table.schema.field("ORDER_DATE").metadata[b"PARQUET:field_id"])
        metadata = json.loads(uploads[result["metadata_key"]])
        fields = {f["name"]: f["type"] for f in metadata["schemas"][0]["fields"]}
        self.assertEqual("date", fields["ORDER_DATE"])
        self.assertEqual("decimal(15,2)", fields["AMOUNT"])
        self.assertEqual("timestamp", fields["UPDATED_AT"])
        self.assertEqual("string", fields["EMPTY"])
        columns = glue.create_table.call_args.kwargs["TableInput"]["StorageDescriptor"]["Columns"]
        self.assertEqual("date", next(c["Type"] for c in columns if c["Name"] == "ORDER_DATE"))
        self.assertEqual("decimal(15,2)", next(c["Type"] for c in columns if c["Name"] == "AMOUNT"))

    def test_iceberg_unsupported_nested_values_fail_before_upload(self):
        with patch("iceberg_s3_writer.boto3.client") as aws:
            with self.assertRaisesRegex(ValueError, "Unsupported Iceberg column type"):
                write_iceberg_table_to_s3(pd.DataFrame({"nested": [{"id": 1}]}), S3, "public", "orders")
            aws.assert_not_called()

    def test_csv_json_and_orc_export_dates_and_nulls(self):
        databricks = types.ModuleType("databricks")
        databricks.sql = Mock()
        cursor = databricks.sql.connect.return_value.cursor.return_value
        cursor.description = [(name,) for name in COLUMNS]
        cursor.fetchall.return_value = ROWS
        for fmt in ("CSV", "JSON", "ORC"):
            with self.subTest(format=fmt), patch.dict(sys.modules, {"databricks": databricks}), patch("boto3.client") as aws:
                item = dict(file_format=fmt, source_schema="public", source_table="orders", s3_key="orders." + fmt.lower())
                result = engine._extract_databricks_to_s3(item, {"catalog": "main"}, S3)
                body = aws.return_value.put_object.call_args.kwargs["Body"]
                self.assertEqual(2, result["rows"])
                if fmt == "ORC":
                    table = orc.ORCFile(pa.BufferReader(body)).read()
                    self.assertEqual([dict(zip(COLUMNS, row)) for row in ROWS], table.to_pylist())
                elif fmt == "JSON":
                    rows = [json.loads(line) for line in body.decode().splitlines()]
                    self.assertTrue(rows[0]["ORDER_DATE"].startswith("2026-10-25"))
                    self.assertTrue(rows[0]["UPDATED_AT"].startswith("2026-10-25T12:30:00"))
                    self.assertIsNone(rows[1]["ORDER_DATE"])
                else:
                    rows = list(csv.DictReader(io.StringIO(body.decode())))
                    self.assertEqual("2026-10-25", rows[0]["ORDER_DATE"])
                    self.assertEqual("1234567890123.45", rows[0]["AMOUNT"])
                    self.assertEqual("", rows[1]["ORDER_DATE"])


if __name__ == "__main__":
    unittest.main()
