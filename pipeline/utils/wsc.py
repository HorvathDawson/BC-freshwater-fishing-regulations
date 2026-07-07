"""WSC (Watershed Code) utilities — zero external dependencies."""

from __future__ import annotations

import math
import re

_WSC_TRIM_RE = re.compile(r"(-000000)+$")

# 1:50K Watershed Atlas (WSA) code group widths (sum = 45 digits).
# Canonical layout example: 900-569800-08600-00000-0000-0000-000-000-000-000-000-000
_WSC_50K_GROUPS = (3, 6, 5, 5, 4, 4, 3, 3, 3, 3, 3, 3)


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


def format_wsc_50k(code) -> str:
    """Format a 1:50K Watershed Atlas (WSA) watershed code with dashes.

    Source values are stored as an undelimited 45-digit number (leading
    zeros may be dropped when persisted as a numeric type).  This groups
    them into the canonical WSA layout::

        915765500702004240031700…  →  915-765500-70200-42400-3170-0000-…

    Empty/None inputs and the all-nines "undefined" sentinel return "".
    Trailing all-zero groups are trimmed (mirroring ``trim_wsc`` for the
    modern FWA code).  Always returns a string.
    """
    if code is None:
        return ""
    if isinstance(code, float):
        if math.isnan(code):
            return ""
        code = str(int(code))
    digits = str(code).strip()
    if not digits or digits == "nan":
        return ""
    if len(digits) < 45:
        digits = digits.zfill(45)
    if len(digits) != 45 or not digits.isdigit():
        return ""
    if set(digits) == {"9"}:  # WSA "no watershed" sentinel
        return ""
    parts = []
    i = 0
    for width in _WSC_50K_GROUPS:
        parts.append(digits[i : i + width])
        i += width
    # Trim trailing all-zero groups (meaningless padding, like trim_wsc).
    while len(parts) > 1 and set(parts[-1]) == {"0"}:
        parts.pop()
    return "-".join(parts)
