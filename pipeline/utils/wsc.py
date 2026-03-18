"""WSC (Watershed Code) utilities — zero external dependencies."""

from __future__ import annotations

import re

_WSC_TRIM_RE = re.compile(r"(-000000)+$")


def trim_wsc(code: str) -> str:
    """Strip trailing ``-000000`` padding from an FWA watershed code.

    FWA codes are fixed-length hierarchical strings where trailing zero
    groups are meaningless padding::

        930-508366-413291-000000-...-000000  →  930-508366-413291

    Non-FWA keys (e.g. integer waterbody_key for lakes) pass through
    unchanged.  Idempotent — safe to call on already-trimmed values.
    """
    if not code:
        return ""
    return _WSC_TRIM_RE.sub("", code)
