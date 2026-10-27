# DataBridge presentation layer

The main entry point is `app_final.py`. Its nine existing main tabs, nested tabs,
widget declarations, connection settings, session keys and workflow calls are
retained. The request referred to ten destinations; this checkout defines nine,
so no extra destination was created. `query_migrator_app_v2.py` is a separate
entry point and shares the same styling.

## Implementation

- `ui_theme.py`: two semantic palettes, one native Streamlit button, and the
  namespaced `databridge_appearance` session key. Its callback changes only that
  appearance key. It survives normal reruns and source/destination changes;
  browser-session loss/server restart may reset it to light.
- `ui_theme.css`: shared typography, surfaces, native controls, focus treatment,
  wrapping tabs, statuses, overlays, tables, charts, logs and existing HTML panels.
  No JavaScript, downloaded fonts, extra package or replacement widget is used.
- `.streamlit/config.toml`: a known native light base and minimal toolbar.
- The two entry points replace their old stylesheet blocks with calls to the
  shared presentation layer. The main theme button is above the original title,
  outside all existing forms. No original widget arguments were changed.

The sidebar defect was caused by rules hiding `stHeader`, all `header` elements,
and `stToolbar`. The native expand control belongs to that header/toolbar.
Those rules were removed. Native sidebar state, placement, collapse behavior and
recovery are left to Streamlit; only control colors and borders are styled.

The main tabs remain native `st.tabs` buttons in their original DOM order. They
wrap onto additional lines instead of squeezing their labels. The BaseWeb
single-row moving underline is hidden; a border and tinted surface indicate the
selected tab on every row. Keyboard focus has a separate visible outline.

Existing literal HTML colors are mapped through narrowly scoped CSS attribute
selectors. This avoids rewriting status decisions, generated SQL, report data,
or existing HTML text. Validation's existing CSS variable names are supplied.
The existing icon spaces use the supplied BigQuery SVG, Redshift PNG and S3
JPEG from `assets/brands`, plus the supplied inline DataBridge, Snowflake and
Databricks paths. SVG and image elements have intrinsic width/height attributes
and inline maximum dimensions so they cannot flash at their natural size before
the stylesheet arrives. Existing navigation labels and routing are retained.

`tab_loading()` provides a temporary, accessible skeleton during each main and
nested tab body render, including the standalone query and BigQuery DDL views.
A `finally` block clears it on success or failure. It adds no artificial delay,
overlay, replacement controls or animation. Already-rendered tab switches need
no loader; the skeleton cannot precede Streamlit mounting the Python output.

## Streamlit constraints

This implementation targets the installed Streamlit 1.63.0. Its public
`st.set_option` does not support changing theme options per session. The code
therefore does not mutate private/global configuration from a button callback.
Native widgets use CSS palettes; Streamlit-specific surfaces use `data-testid`
selectors and React Aria roles/component attributes, not generated Emotion
class names. Recheck these selectors when upgrading Streamlit.

The input styling defect came from obsolete BaseWeb selectors: the current
React Aria control wrappers retained light backgrounds while their child inputs
were dark. Styling now covers the actual outer input, number, textarea, select
and multiselect surfaces, native icon buttons and portaled option highlights.
Hover/focus change only color and shadow, preserving native geometry. Light-mode
controls use subtle borders/shadows; both palettes retain validation states.

Glide data grids and Vega canvas charts draw pixels rather than HTML. A scoped
dark-mode filter adapts their native light canvas without replacing tables,
data, selection, sorting, scrolling or editing. This also affects custom cell
colors. Plotly text is themed separately. Canvas colors, selection overlays,
chart hover labels, dataframe fullscreen and conditional formatting need visual
acceptance with representative connected data; the offline tests cannot prove
their appearance. A native per-session theme API would be preferable if a
future Streamlit release supplies one.

## Verification

Run from the project root:

```powershell
.\.venv\Scripts\python.exe -m unittest discover -s tests -v
```

The tests parse root Python sources, check palette text contrast (at least 4.5:1
for the tested semantic pairs), and execute the real Streamlit scripts through
AppTest. They cover all three source selections, both destination selections,
both appearances, ordinary reruns, input-value preservation and the standalone
query UI. Widget protocols and ordering are compared across appearance changes;
transient Streamlit value-sync flags are excluded, while actual values are checked.
No cloud connection, migration, authentication submission, demo activation or
remote write is performed.

The local Streamlit server started successfully on `127.0.0.1:8502`. All eight
unit/AppTest checks passed. Additional checks cover intrinsic icon sizes and
skeleton cleanup on successful and failing renders.

Live browser verification confirmed both themes, dark dropdown surfaces and
keyboard option focus, the empty multiselect menu, native password show/hide,
disabled-field text, SQL textareas, and skeleton appearance/cleanup. The inspected
text input retained its 40 px root height, padding and radius across both themes
and hover/focus. Supplied images loaded at 18 px in the sidebar and 28 px in the
pipeline. Screenshots are saved in `ui-verification/`. Populated multiselect tags,
date/time popovers and connected-data displays were not exercised.

Before release, check in both modes at desktop and narrow widths:

1. Collapse/expand the sidebar repeatedly, switch tabs, trigger a harmless rerun,
   and switch appearance; confirm the native recovery control remains reachable.
2. Check all nine main tabs and existing nested tabs, including the long source
   health labels. Confirm native arrow-key and Tab navigation remain usable.
3. Check dropdowns, multiselects, uploaders, disabled controls, tooltips and date
   popovers; confirm no overlay is clipped.
4. With authorized representative data, inspect monitoring, validation, charts,
   conditional table colors, query output, logs, errors and health panels.

Cloud-dependent execution was inspected but not exercised. Existing missing
optional modules or connection-dependent failures are outside this UI change.

## Cached-module recovery and regression check (2026-10-27)

A running server retained the older `ui_theme` module without `brand_image` or
`tab_loading`, causing the reported ImportError. The main entry point now checks
for the required presentation exports before importing dependent UI modules and
reloads only an outdated presentation module. It does not clear session state or
reload migration modules as part of this fix. A regression test removes those
exports from the cached module and verifies recovery, input/theme persistence,
and subsequent BigQuery rendering.

All 11 unit/AppTest tests pass, including real offline BigQuery DDL/query
conversion, invalid-input rejection and the previous UI/state checks. The original
server on localhost:8501 was verified to render successfully. The existing mmh3 /
Iceberg bucket-transform smoke test passed. The validation scratch assertion was
updated from the obsolete `only_in_sf` key to the current `only_in_src` contract;
its module/type-normalization and mocked schema-comparison checks passed.

The environment does not have `snowflake-snowpark-python`, so Snowflake health
checks requiring Snowpark remain unavailable. Cloud authentication, live discovery,
S3 writes, Redshift COPY, monitoring with live jobs and live validation were not
executed. Passing offline checks does not establish end-to-end cloud correctness.

## Consistent technology logos

`ui_brands.py` is the shared renderer for existing logo slots in the sidebar,
pipeline, footer, S3/Redshift cards, DDL/query/procedure headings, health heading,
validation selectors and Iceberg format banner. Supplied assets and inline paths
are reused; the official Iceberg wordmark is bundled locally with provenance in
`assets/brands/README.md`. The footer now follows BigQuery selection correctly.
Plain prose, native widget labels, routing and workflow icons remain unchanged.
Backup files under `bkp/` are historical and are not part of the active UI.

The 11 regression tests passed after centralizing the renderer; a subsequent
asset check verified the bundled Iceberg SVG. Browser inspection confirmed the
footer's three logos load at 18 px. Connected-only panels were checked statically,
not with cloud credentials.
