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
The pipeline illustration uses local, neutral database/cloud glyphs because no
licensed official brand assets are bundled. Existing navigation labels/icons
are retained.

## Streamlit constraints

This implementation targets the installed Streamlit 1.63.0. Its public
`st.set_option` does not support changing theme options per session. The code
therefore does not mutate private/global configuration from a button callback.
Native widgets use CSS palettes; Streamlit-specific surfaces use `data-testid`
selectors and BaseWeb controls use their component attributes, not generated
Emotion class names. Recheck these selectors when upgrading Streamlit.

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

The local Streamlit server started successfully on `127.0.0.1:8502` after local
socket access was allowed outside the sandbox. Browser verification was blocked:
the in-app browser could not bind its tab, and Windows computer use stopped
because it could not confidently determine the browser URL. Consequently,
repeated sidebar collapse/recovery, tab wrapping at different widths, popup
placement and live screenshots have **not** been visually verified.

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
