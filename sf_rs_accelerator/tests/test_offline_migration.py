"""Exercise real converters without credentials, cloud calls or SQL execution."""
import unittest

from bigquery_ddl_migrator import BigQueryDDLMigrator, validate_redshift_ddl
from bigquery_query_migrator import BigQueryQueryMigrator, validate_redshift_syntax


class OfflineMigrationTests(unittest.TestCase):
    def test_ddl_conversion_and_invalid_input(self):
        migrator = BigQueryDDLMigrator()
        result = migrator.migrate(
            "CREATE TABLE `project.dataset.orders` (id INT64, label STRING, total NUMERIC);"
        )
        self.assertFalse(result.errors, result.errors)
        self.assertTrue(validate_redshift_ddl(result.sql)["valid"])
        self.assertIn("BIGINT", result.sql.upper())
        self.assertIn("VARCHAR", result.sql.upper())
        self.assertTrue(migrator.migrate("").errors)
        self.assertTrue(migrator.migrate("DROP TABLE orders").errors)
        with self.assertRaises(ValueError):
            BigQueryDDLMigrator(diststyle="KEY")

    def test_query_conversion_and_rejected_writes(self):
        migrator = BigQueryQueryMigrator()
        result = migrator.migrate(
            "SELECT id, IFNULL(label, 'unknown') AS label FROM `project.dataset.orders`"
        )
        self.assertFalse(result.errors, result.errors)
        self.assertTrue(validate_redshift_syntax(result.sql)["valid"])
        self.assertIn("COALESCE", result.sql.upper())
        self.assertTrue(migrator.migrate("").errors)
        rejected = migrator.migrate("DELETE FROM `project.dataset.orders` WHERE id = 1")
        self.assertTrue(rejected.errors)
        self.assertEqual("", rejected.sql)
