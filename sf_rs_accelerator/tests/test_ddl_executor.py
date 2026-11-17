"""Offline tests for the DDL executor guard rails. No cloud access."""
import unittest

import ddl_executor as ex


class SplitAndGuardTests(unittest.TestCase):
    def test_split_ignores_semicolons_in_quotes_and_comments(self):
        sql = """
        -- header; with a semicolon
        CREATE TABLE public.a (note VARCHAR(10) DEFAULT 'x;y');  /* block; comment */
        CREATE TABLE public."we;ird" (id INT);
        """
        parts = ex.split_statements(sql)
        self.assertEqual(2, len(parts))
        self.assertIn("'x;y'", parts[0])
        self.assertIn('"we;ird"', parts[1])
        self.assertNotIn("header", parts[0])

    def test_only_create_table_and_schema_are_allowed(self):
        allowed = ["CREATE TABLE public.t (id INT)",
                   "create table if not exists public.t (id INT)",
                   "CREATE SCHEMA IF NOT EXISTS analytics"]
        for sql in allowed:
            self.assertIsNone(ex.block_reason(sql), sql)
        blocked = ["DROP TABLE public.t", "TRUNCATE TABLE public.t", "DELETE FROM public.t",
                   "INSERT INTO public.t VALUES (1)", "GRANT ALL ON public.t TO x",
                   "ALTER TABLE public.t ADD COLUMN c INT", "CREATE EXTERNAL SCHEMA s FROM DATA CATALOG",
                   "CREATE TABLE public.t AS SELECT * FROM public.other",
                   "CREATE TABLE public.t AS (SELECT 1)", "SELECT 1"]
        for sql in blocked:
            self.assertIsNotNone(ex.block_reason(sql), sql)

    def test_hidden_second_statement_is_still_checked(self):
        plan = ex.prepare("CREATE TABLE public.t (id INT); DROP TABLE public.important;")
        self.assertEqual(2, len(plan["statements"]))
        self.assertEqual(1, len(plan["blocked"]))
        self.assertIn("DROP", plan["blocked"][0][0].upper())

    def test_comment_cannot_disguise_a_statement(self):
        plan = ex.prepare("/* CREATE TABLE x (id INT) */ DROP TABLE public.t")
        self.assertEqual(["DROP TABLE public.t"], plan["statements"])
        self.assertTrue(plan["blocked"])

    def test_labels(self):
        self.assertEqual("CREATE TABLE public.orders",
                         ex.statement_label('CREATE TABLE IF NOT EXISTS "public"."orders" (id INT)'))


class ConverterOutputTests(unittest.TestCase):
    """Run the real converter on Snowflake GET_DDL output, then the executor on it."""

    SNOWFLAKE_DDL = """create or replace TABLE ORDERS (
\tORDER_ID NUMBER(38,0),
\tCUSTOMER_ID NUMBER(38,0),
\tORDER_DATE TIMESTAMP_NTZ(9),
\tMETADATA VARIANT,
\tNOTE VARCHAR(50) COMMENT 'free text'
) CLUSTER BY (ORDER_DATE);"""

    def convert(self, schema="public"):
        import sf_to_rs_combined_ddl_distribution as conv
        sql = conv.convert_create_statement(self.SNOWFLAKE_DDL, schema)
        sql = conv.clean_misc(conv.fix_timestamp_columns(conv.convert_datatypes(sql)))
        return conv.finalize_table(sql, None, None)

    def test_converter_output_uses_redshift_types_and_a_replace_pair(self):
        sql = self.convert()
        for expected in ("NUMERIC(38,0)", "TIMESTAMP", "SUPER", "DISTSTYLE AUTO", "DROP TABLE IF EXISTS public.ORDERS"):
            self.assertIn(expected, sql)
        for gone in ("NUMBER", "VARIANT", "CLUSTER BY", "COMMENT"):
            self.assertNotIn(gone, sql)

    def test_default_removes_the_drop_so_existing_tables_are_safe(self):
        plan = ex.prepare(self.convert())
        self.assertEqual([], plan["blocked"])
        self.assertEqual(1, plan["drops_removed"])
        self.assertEqual(["CREATE TABLE public.ORDERS"], [ex.statement_label(s) for s in plan["statements"]])
        calls = []
        ex.run(plan["statements"], calls.append)
        self.assertFalse(any("DROP" in c.upper() for c in calls))

    def test_replace_mode_runs_the_drop_then_the_create(self):
        plan = ex.prepare(self.convert(), allow_replace=True)
        self.assertEqual([], plan["blocked"])
        self.assertEqual(["public.orders"], plan["replace"])
        calls = []
        results = ex.run(plan["statements"], calls.append, allow_replace=True)
        self.assertEqual(["dropped", "created"], [r["status"] for r in results])
        self.assertTrue(calls[0].startswith("DROP TABLE IF EXISTS") and calls[1].startswith("CREATE TABLE"))

    def test_failed_drop_means_the_create_is_not_attempted(self):
        plan = ex.prepare(self.convert(), allow_replace=True)

        def runner(sql):
            if sql.startswith("DROP"):
                raise RuntimeError("must be owner of table orders")
        results = ex.run(plan["statements"], runner, allow_replace=True, continue_on_error=True)
        self.assertEqual(["failed", "not run"], [r["status"] for r in results])

    def test_other_drops_are_never_allowed_even_in_replace_mode(self):
        for sql in ("DROP TABLE IF EXISTS public.a CASCADE; CREATE TABLE public.a (id INT);",
                    "DROP TABLE IF EXISTS public.a; CREATE TABLE public.b (id INT);",
                    "DROP TABLE public.a; CREATE TABLE public.a (id INT);",
                    "DROP SCHEMA public CASCADE;"):
            plan = ex.prepare(sql, allow_replace=True)
            self.assertTrue(plan["blocked"], sql)


class BigQueryHookTests(unittest.TestCase):
    """The BigQuery DDL tab offers 'Run on Redshift' only for valid, current DDL."""

    SCRIPT = """
import streamlit as st
import bigquery_ddl_ui as ui
ui.RESULT_ACTION = lambda sql, key: st.write("HOOK " + key + " :: " + sql)
good = {"fingerprint": "f", "errors": [], "sql": "CREATE TABLE public.t (id BIGINT);",
        "statement_count": 1, "changes": [], "warnings": []}
ui._render_result(good, "", CURRENT, "bq_ddl", "x.sql")
"""

    def hook_lines(self, result_fingerprint, current, errors=()):
        from streamlit.testing.v1 import AppTest
        script = self.SCRIPT.replace('"fingerprint": "f"', f'"fingerprint": "{result_fingerprint}"') \
                            .replace('"errors": []', f'"errors": {list(errors)!r}') \
                            .replace("CURRENT", repr(current))
        app = AppTest.from_string(script, default_timeout=30).run()
        self.assertFalse(app.exception)
        return [m.value for m in app.markdown if str(m.value).startswith("HOOK")]

    def test_hook_runs_for_valid_current_ddl(self):
        lines = self.hook_lines("f", "f")
        self.assertEqual(["HOOK bq_ddl :: CREATE TABLE public.t (id BIGINT);"], lines)

    def test_hook_is_skipped_for_stale_or_failed_ddl(self):
        self.assertEqual([], self.hook_lines("old", "new"))
        self.assertEqual([], self.hook_lines("f", "f", errors=["bad column"]))


class AppPanelTests(unittest.TestCase):
    """The panel renders in the real app and only runs after confirmation."""

    def app(self, connected=True):
        from pathlib import Path
        from streamlit.testing.v1 import AppTest
        app = AppTest.from_file(str(Path(__file__).resolve().parents[1] / "app_final.py"),
                                default_timeout=60)
        app.session_state["ddl_output"] = "CREATE TABLE public.orders (id BIGINT);"
        app.session_state["rs_connected"] = connected
        return app.run()

    def test_run_button_needs_connection_and_confirmation(self):
        app = self.app(connected=False)
        self.assertFalse(app.exception)
        self.assertEqual([], [b for b in app.button if b.key == "ddl_paste_exec_run"])

        app = self.app(connected=True)
        self.assertFalse(app.exception)
        button = next(b for b in app.button if b.key == "ddl_paste_exec_run")
        self.assertTrue(button.disabled)
        next(c for c in app.checkbox if c.key == "ddl_paste_exec_confirm").check().run()
        button = next(b for b in app.button if b.key == "ddl_paste_exec_run")
        self.assertFalse(button.disabled)

    def test_blocked_statement_hides_the_run_button(self):
        from pathlib import Path
        from streamlit.testing.v1 import AppTest
        app = AppTest.from_file(str(Path(__file__).resolve().parents[1] / "app_final.py"), default_timeout=60)
        app.session_state["ddl_output"] = "CREATE TABLE public.a (id INT); DROP TABLE public.b;"
        app.session_state["rs_connected"] = True
        app.run()
        self.assertFalse(app.exception)
        self.assertEqual([], [b for b in app.button if b.key == "ddl_paste_exec_run"])
        self.assertTrue(any("cannot be run" in e.value for e in app.error))


class RunTests(unittest.TestCase):
    def runner(self, fail_on=None, exists_on=None):
        calls = []

        def run(sql):
            calls.append(sql)
            if fail_on and fail_on in sql:
                raise RuntimeError("ERROR: permission denied for schema public")
            if exists_on and exists_on in sql:
                raise RuntimeError('ERROR: relation "orders" already exists')
        run.calls = calls
        return run

    STATEMENTS = ["CREATE TABLE public.a (id INT)", "CREATE TABLE public.b (id INT)",
                  "CREATE TABLE public.c (id INT)"]

    def test_all_created(self):
        runner = self.runner()
        results = ex.run(self.STATEMENTS, runner)
        self.assertEqual(["created"] * 3, [r["status"] for r in results])
        self.assertEqual(3, len(runner.calls))

    def test_existing_table_is_skipped_not_replaced(self):
        runner = self.runner(exists_on="public.b")
        results = ex.run(self.STATEMENTS, runner)
        self.assertEqual(["created", "skipped", "created"], [r["status"] for r in results])
        self.assertFalse(any("DROP" in c.upper() for c in runner.calls))

    def test_failure_stops_the_rest_by_default(self):
        runner = self.runner(fail_on="public.b")
        results = ex.run(self.STATEMENTS, runner)
        self.assertEqual(["created", "failed", "not run"], [r["status"] for r in results])
        self.assertEqual(2, len(runner.calls))
        self.assertIn("permission denied", results[1]["message"])

    def test_continue_on_error_runs_everything(self):
        runner = self.runner(fail_on="public.b")
        results = ex.run(self.STATEMENTS, runner, continue_on_error=True)
        self.assertEqual(["created", "failed", "created"], [r["status"] for r in results])
        self.assertEqual({"created": 2, "dropped": 0, "skipped": 0, "failed": 1, "not run": 0},
                         ex.summarize(results))

    def test_schemas_are_created_first_except_public(self):
        runner = self.runner()
        ex.run(self.STATEMENTS[:1], runner, create_schemas=["analytics", "PUBLIC"])
        self.assertEqual('CREATE SCHEMA IF NOT EXISTS "analytics"', runner.calls[0])
        self.assertEqual(2, len(runner.calls))

    def test_schemas_referenced_reads_them_from_the_ddl(self):
        statements = ['CREATE TABLE analytics.orders (id INT)',
                      'CREATE TABLE "Sales"."t" (id INT)',
                      'CREATE TABLE IF NOT EXISTS db.analytics.x (id INT)',
                      'CREATE TABLE public.a (id INT)', 'CREATE TABLE bare (id INT)']
        self.assertEqual(["analytics", "Sales"], ex.schemas_referenced(statements))

    def test_bad_schema_name_is_rejected_before_running_anything(self):
        runner = self.runner()
        with self.assertRaises(ValueError):
            ex.run(self.STATEMENTS, runner, create_schemas=['x"; DROP TABLE y; --'])
        self.assertEqual([], runner.calls)

    def test_run_refuses_a_blocked_statement_even_without_prepare(self):
        runner = self.runner()
        results = ex.run(["DROP TABLE public.t"], runner)
        self.assertEqual("failed", results[0]["status"])
        self.assertEqual([], runner.calls)


if __name__ == "__main__":
    unittest.main()
