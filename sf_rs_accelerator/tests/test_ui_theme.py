"""Offline UI regression checks; never connect to a cloud service.

Run: .venv/Scripts/python.exe -m unittest discover -s tests -v
These checks do not replace browser verification of CSS and native sidebar state.
"""
import ast
from pathlib import Path
import sys
import unittest

from google.protobuf.json_format import MessageToDict
from streamlit.testing.v1 import AppTest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from ui_theme import PALETTES, THEME_KEY, theme_css


def contrast(a, b):
    def luminance(value):
        rgb = [int(value[i:i + 2], 16) / 255 for i in (1, 3, 5)]
        rgb = [v / 12.92 if v <= .04045 else ((v + .055) / 1.055) ** 2.4 for v in rgb]
        return sum(v * weight for v, weight in zip(rgb, (.2126, .7152, .0722)))
    light, dark = sorted((luminance(a), luminance(b)), reverse=True)
    return (light + .05) / (dark + .05)


def widget_contract(app):
    """Retain widget order, identity, options, defaults, help and disabled flags."""
    result = []
    kinds = {"text_input", "text_area", "number_input", "selectbox", "multiselect",
             "checkbox", "radio", "button", "date_input", "time_input",
             "file_uploader", "download_button", "slider", "tab", "expander"}
    for element in app:
        if element.type not in kinds or getattr(element, "key", None) == "databridge_appearance_button":
            continue
        proto = MessageToDict(element.proto)
        # Streamlit may emit a value synchronization flag after an ordinary
        # rerun. Compare actual widget values instead of transport flags.
        proto.pop("setValue", None)
        proto.pop("value", None)
        result.append((element.type, proto, getattr(element, "value", None)))
    return result


class ThemeTests(unittest.TestCase):
    def test_palette_contrast(self):
        for name, palette in PALETTES.items():
            for fg, bg in [("text", "bg"), ("text", "input"), ("muted", "surface"),
                           ("accent", "accent-soft"), ("green", "green-bg"),
                           ("amber", "amber-bg"), ("red", "red-bg"), ("on-accent", "accent")]:
                with self.subTest(mode=name, foreground=fg, background=bg):
                    self.assertGreaterEqual(contrast(palette[fg], palette[bg]), 4.5)

    def test_css_preserves_native_sidebar_and_tab_order(self):
        for mode in PALETTES:
            css = theme_css(mode)
            self.assertIn("stExpandSidebarButton", css)
            self.assertIn("stSidebarCollapseButton", css)
            self.assertNotIn("visibility: hidden", css)
            self.assertNotIn("position: fixed", css)
            self.assertNotRegex(css, r"(?<![\w-])order\s*:")
            self.assertIn('flex-wrap: wrap', css)
            self.assertIn('prefers-reduced-motion', css)

    def test_all_python_sources_parse(self):
        for source in ROOT.glob("*.py"):
            with self.subTest(file=source.name):
                ast.parse(source.read_text(encoding="utf-8"), filename=str(source))

    def test_sources_destinations_and_widget_preservation(self):
        for source in ("Snowflake", "Databricks", "BigQuery"):
            app = AppTest.from_file(str(ROOT / "app_final.py"), default_timeout=60).run()
            self.assertFalse(app.exception)
            if source != "Snowflake":
                app.selectbox(key="source_type_sel").select(source).run()
            for destination in ("Source → S3 → Redshift", "Source → S3 only"):
                with self.subTest(source=source, destination=destination):
                    app.selectbox(key="pipeline_destination_sel").select(destination).run()
                    self.assertFalse(app.exception)
                    before = widget_contract(app)
                    original_mode = app.session_state[THEME_KEY]
                    app.button(key="databridge_appearance_button").click().run()
                    self.assertFalse(app.exception)
                    self.assertNotEqual(original_mode, app.session_state[THEME_KEY])
                    self.assertEqual(before, widget_contract(app))
                    mode = app.session_state[THEME_KEY]
                    app.run()
                    self.assertFalse(app.exception)
                    self.assertEqual(mode, app.session_state[THEME_KEY])
                    self.assertEqual(before, widget_contract(app))
                    self.assertEqual(source, app.session_state["source_type"])
                    self.assertEqual(destination, app.session_state["pipeline_destination"])
                    self.assertEqual([], app.session_state["job_queue"])
                    app.button(key="databridge_appearance_button").click().run()
                    self.assertEqual(original_mode, app.session_state[THEME_KEY])

    def test_input_value_survives_theme_and_rerun(self):
        app = AppTest.from_file(str(ROOT / "app_final.py"), default_timeout=60).run()
        app.text_input(key="sf_database").set_value("UI_PRESERVATION_CHECK").run()
        app.button(key="databridge_appearance_button").click().run()
        app.run()
        self.assertFalse(app.exception)
        self.assertEqual("UI_PRESERVATION_CHECK", app.text_input(key="sf_database").value)
        self.assertFalse(app.session_state["sf_connected"])

    def test_standalone_query_app(self):
        app = AppTest.from_file(str(ROOT / "query_migrator_app_v2.py"), default_timeout=60).run()
        self.assertFalse(app.exception)
        before = widget_contract(app)
        self.assertEqual(2, len(app.tabs))
        app.button(key="databridge_appearance_button").click().run()
        self.assertFalse(app.exception)
        self.assertEqual(before, widget_contract(app))


if __name__ == "__main__":
    unittest.main()
