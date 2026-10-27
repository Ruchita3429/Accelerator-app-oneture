"""Presentation-only theme for the Streamlit application.

No application state is read or changed except the namespaced appearance key.
Streamlit does not expose a public per-session theme setter; native widgets stay
native and CSS supplies the two palettes. Never mutate global Streamlit config.
"""
from pathlib import Path
import base64
from contextlib import contextmanager
from functools import lru_cache

import streamlit as st


THEME_KEY = "databridge_appearance"


@lru_cache(maxsize=4)
def brand_image(name):
    """Embed supplied local assets with intrinsic sizing before CSS arrives."""
    assets = {"iceberg": ("iceberg.svg", "image/svg+xml"),
              "bigquery": ("bigquery.svg", "image/svg+xml"),
              "s3": ("s3.jpg", "image/jpeg"),
              "redshift": ("redshift.png", "image/png")}
    if name not in assets:
        return None
    filename, mime = assets[name]
    data = (Path(__file__).parent / "assets" / "brands" / filename).read_bytes()
    encoded = base64.b64encode(data).decode("ascii")
    return (f'<img class="brand-icon brand-icon-{name}" width="18" height="18" '
            f'alt="" aria-hidden="true" style="object-fit:contain;max-width:28px;max-height:28px" '
            f'src="data:{mime};base64,{encoded}">')


@contextmanager
def tab_loading():
    """A real render-lifecycle placeholder; never delays or masks controls.

    Streamlit renders all tab bodies during a run. Fast tabs may complete before
    a browser paints the skeleton; slow bodies retain it until completion.
    Clear in finally so errors and st.stop cannot strand a loading indicator.
    """
    placeholder = st.empty()
    placeholder.markdown(
        '<div class="tab-loading" role="status" aria-label="Loading tab content">'
        '<span class="tab-loading-label">Loading…</span>'
        '<span class="tab-loading-bar"></span>'
        '<span class="tab-loading-bar"></span></div>',
        unsafe_allow_html=True,
    )
    try:
        yield
    finally:
        placeholder.empty()

PALETTES = {
    "light": {
        "bg": "#f4f6f9",
        "surface": "#fafbfd",
        "raised": "#eef2f7",
        "input": "#fcfcfd",
        "text": "#243247",
        "muted": "#58677c",
        "border": "#c5ceda",
        "accent": "#285eb2",
        "accent-soft": "#e4edf9",
        "green": "#196845",
        "green-bg": "#e8f3ed",
        "amber": "#835409",
        "amber-bg": "#fbf1dc",
        "red": "#b13743",
        "red-bg": "#f9e9eb",
        "purple": "#6d4caf",
        "on-accent": "#ffffff",
        "canvas-filter": "none",

        # Always-visible input borders and subtle elevation
        "control-bg": "#fcfcfd",
        "control-border": "#aebac9",
        "control-hover": "#647995",
        "control-shadow": "0 1px 3px rgb(36 50 71 / 10%)",
    },

    "dark": {
        "bg": "#151d2a",
        "surface": "#1c2737",
        "raised": "#263448",
        "input": "#182332",
        "text": "#e3eaf3",
        "muted": "#a7b5c9",
        "border": "#45566d",
        "accent": "#90b8f5",
        "accent-soft": "#293e5b",
        "green": "#83d4ab",
        "green-bg": "#203d34",
        "amber": "#eac27d",
        "amber-bg": "#403728",
        "red": "#f1a0a9",
        "red-bg": "#452c36",
        "purple": "#c6b1ee",
        "on-accent": "#15263f",

        # Visible against the dark surface, even when idle
        "control-bg": "#202d3e",
        "control-border": "#61738a",
        "control-hover": "#819ec1",
        "control-shadow": "0 1px 3px rgb(0 0 0 / 22%)",
        "canvas-filter": "invert(.88) hue-rotate(180deg)",
    },
}

# Existing HTML uses literal colors throughout the health/monitoring panels.
# Map only those exact inline declarations, scoped to markdown. This avoids
# altering strings, calculations, queries, status decisions or widget content.
LEGACY_COLORS = {
    "bg": ["#0d0f14", "#13161f", "#131825", "#161b27"],
    "surface": ["#1c2233", "#f0f2f6"],
    "raised": ["#212840", "#1a2540"],
    "border": ["#1e2130", "#2a3148", "#2a2d3e", "#3a4255", "#3a3f55"],
    "text": ["#e8eaf0", "#f0f2ff", "#d0d4e0", "#c8cad4", "#b0b8cc", "#a0aabb", "#fff"],
    "muted": ["#4a5068", "#6b7080", "#8891a8"],
    "accent": ["#5b8dee", "#7aa8f5", "#8aabf0", "#0066cc"],
    "accent-soft": ["#0d1e3a", "#162040", "#243050", "#1a3a5c", "#2a3a60", "#2a4080", "#1e4080", "#e8f4f8"],
    "green": ["#34d399", "#28a745"],
    "green-bg": ["#081e10", "#0f2e1e", "#102418", "#1a4a30", "#1a5c3a"],
    "amber": ["#fbbf24", "#ffc107", "#ff5a1f"],
    "amber-bg": ["#1e1600", "#2a2010", "#3a1a00", "#6b4800", "#78350f", "#fff3cd"],
    "red": ["#f87171", "#e18484"],
    "red-bg": ["#1e0808", "#2e1010", "#7f1d1d"],
    "purple": ["#a78bfa", "#7c6ee0"],
}


def _toggle_appearance():
    st.session_state[THEME_KEY] = (
        "light" if st.session_state[THEME_KEY] == "dark" else "dark"
    )


def theme_css(mode):
    """Build deterministic CSS; kept separate for offline verification."""
    palette = PALETTES[mode]
    tokens = ";".join(f"--{name}:{value}" for name, value in palette.items())
    rules = [f":root {{{tokens}; --blue:var(--accent); color-scheme:{mode};}}"]
    rules.append(Path(__file__).with_name("ui_theme.css").read_text(encoding="utf-8"))
    for token, colors in LEGACY_COLORS.items():
        for color in colors:
            # Exact declaration endings avoid matching color #fff in #fff3cd,
            # or a foreground declaration within background-color.
            for prop in ("color", "background", "background-color", "border-color"):
                selectors = []
                for spacing in ("", " "):
                    value = f"{prop}:{spacing}{color}"
                    for start in (f'[style^="{value}', f'[style*=";{value}', f'[style*="; {value}'):
                        selectors.append(f'[data-testid="stMarkdownContainer"] {start};" i]')
                    selectors.append(f'[data-testid="stMarkdownContainer"] [style="{value}" i]')
                    selectors.append(f'[data-testid="stMarkdownContainer"] [style$=";{value}" i]')
                    selectors.append(f'[data-testid="stMarkdownContainer"] [style$="; {value}" i]')
                rules.append(",\n".join(selectors) + f" {{{prop}:var(--{token}) !important;}}")
    return "\n".join(rules)


def apply_theme():
    st.session_state.setdefault(THEME_KEY, "light")
    st.html("<style>" + theme_css(st.session_state[THEME_KEY]) + "</style>")


def render_theme_toggle():
    """The only new control; placed above the existing main title/forms."""
    dark = st.session_state[THEME_KEY] == "dark"
    with st.container(key="databridge_appearance_control", horizontal=True, horizontal_alignment="right"):
        st.button(
            "Light mode" if dark else "Dark mode",
            icon=":material/light_mode:" if dark else ":material/dark_mode:",
            key="databridge_appearance_button", on_click=_toggle_appearance,
            help="Switch appearance. Your choice stays selected during this session.",
        )
