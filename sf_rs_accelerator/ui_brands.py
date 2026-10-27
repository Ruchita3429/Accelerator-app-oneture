"""Shared technology marks for all presentation surfaces."""
from ui_theme import brand_image

_BRAND_MARKS = {
    "databridge": '<path d="M3 19h18M5 19V9l7-5 7 5v10M8 19v-5h8v5M5 10h14" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round"/>',
    "snowflake": '<path d="M12 2v20M3.34 7l17.32 10M3.34 17 20.66 7M8 4l4 4 4-4M8 20l4-4 4 4M3 11l5 1-1 5M21 13l-5-1 1-5M3 13l5-1-1-5M21 11l-5 1 1 5" fill="none" stroke="currentColor" stroke-width="1.7" stroke-linecap="round" stroke-linejoin="round"/>',
    "databricks": '<path d="M.95 14.184 12 20.403l9.919-5.55v2.21L12 22.662l-10.484-5.96-.565.308v.77L12 24l11.05-6.218v-4.317l-.515-.309L12 19.118l-9.867-5.653v-2.21L12 16.805l11.05-6.218V6.32l-.515-.308L12 11.974 2.647 6.681 12 1.388l7.76 4.368.668-.411v-.566L12 0 .95 6.27v.72L12 13.207l9.919-5.55v2.26L12 15.52 1.516 9.56l-.565.308Z"/>',
    "bigquery": '<path d="M4 3h5v18H4zM10 3h5v18h-5zM16 3h4v18h-4z"/>',
    "s3": '<path d="M4 7h16l-1.2 13H5.2L4 7Z" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linejoin="round"/><path d="M7 4c3-2 7-2 10 0M8 13c2 1 6 1 8 0" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round"/><path d="m15 11 2 2-2 2" fill="none" stroke="currentColor" stroke-width="1.5" stroke-linecap="round" stroke-linejoin="round"/>',
    "redshift": '<ellipse cx="12" cy="5" rx="8" ry="3" fill="none" stroke="currentColor" stroke-width="1.8"/><path d="M4 5v13c0 1.7 3.6 3 8 3s8-1.3 8-3V5M4 11c0 1.7 3.6 3 8 3s8-1.3 8-3" fill="none" stroke="currentColor" stroke-width="1.8"/><path d="M9 8h6" stroke="currentColor" stroke-width="1.8" stroke-linecap="round"/>',
}


def brand_icon(name):
    """Return a supplied asset or a dimensioned inline SVG brand mark."""
    image = brand_image(name)
    if image is not None:
        return image
    return (
        f'<svg class="brand-icon brand-icon-{name}" viewBox="0 0 24 24" '
        f'width="18" height="18" fill="currentColor" '
        f'style="max-width:28px;max-height:28px" '
        f'aria-hidden="true">{_BRAND_MARKS[name]}</svg>'
    )


