"""Selection controls must change visible dropdown chips and queue input together."""
import unittest
from streamlit.testing.v1 import AppTest


class DiscoverySelection(unittest.TestCase):
    def test_select_deselect_and_manual_selection_stay_synchronized(self):
        app = AppTest.from_string('''
import streamlit as st
from chunk_01_table_discovery import render_table_discovery
st.session_state.sf_connected = True
st.session_state.setdefault("logs", [])
st.session_state._all_tables = {
    "PUBLIC": [f"ORDERS_{i}" for i in range(9)],
    "OTHER": ["CUSTOMERS", "SALES"],
}
render_table_discovery(rs_schemas=["public"])
''').run(timeout=10)
        self.assertFalse(app.exception)
        self.assertFalse(any(b.label in ("All", "None") for b in app.button))
        app.button(key="btn_select_all").click().run(timeout=10)
        self.assertFalse(app.exception)
        self.assertEqual(9, len(app.multiselect(key="multisel_PUBLIC").value))
        self.assertEqual(["CUSTOMERS", "SALES"], app.multiselect(key="multisel_OTHER").value)
        self.assertEqual(11, sum(map(len, app.session_state["table_selections"].values())))
        self.assertTrue(any("11 tables selected" in m.value for m in app.markdown))
        app.run(timeout=10)
        self.assertEqual(9, len(app.multiselect(key="multisel_PUBLIC").value))
        app.button(key="btn_deselect_all").click().run(timeout=10)
        self.assertFalse(app.exception)
        self.assertEqual([], app.multiselect(key="multisel_PUBLIC").value)
        self.assertEqual([], app.multiselect(key="multisel_OTHER").value)
        self.assertEqual(0, sum(map(len, app.session_state["table_selections"].values())))
        self.assertTrue(app.button(key="btn_build_queue").disabled)
        app.multiselect(key="multisel_PUBLIC").select("ORDERS_0").run(timeout=10)
        self.assertFalse(app.exception)
        self.assertEqual(["ORDERS_0"], app.session_state["table_selections"]["PUBLIC"])
        self.assertTrue(any("1 tables selected" in m.value for m in app.markdown))
        app.button(key="btn_select_all").click().run(timeout=10)
        app.session_state["migration_run_id"] = "previous-run"
        app.session_state["checkpoint"] = {"PUBLIC.ORDERS_0": "done"}
        app.button(key="btn_build_queue").click().run(timeout=10)
        self.assertFalse(app.exception)
        self.assertEqual(11, len(app.session_state["job_queue"]))
        self.assertIsNone(app.session_state["migration_run_id"])
        self.assertEqual({}, app.session_state["checkpoint"])


if __name__ == "__main__":
    unittest.main()
