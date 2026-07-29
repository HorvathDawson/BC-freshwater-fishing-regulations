"""WSC (Watershed Code) utilities — zero external dependencies.

Copied verbatim from ``pipeline/utils/wsc.py`` so the v2 stream_sections package is
self-contained (no import back into the legacy pipeline). Keep the two in sync if either changes.

- ``trim_wsc``      : drop trailing ``-000000`` padding from a modern (1:20K) FWA watershed code.
- ``format_wsc_50k``: turn the undelimited 45-digit 1:50K Watershed-Atlas code (how streams store
                      ``WATERSHED_CODE_50K``) into the canonical dashed, zero-trimmed form
                      (``910290700999004150000…`` → ``910-290700-99900-41500``). This is the proper
                      way to reconcile the obstacles layer's dashed code with the streams' packed
                      one — both go through ``format_wsc_50k`` to the same key.
"""

from __future__ import annotations

import math
import re

_WSC_TRIM_RE = re.compile(r"(-000000)+$")

# 1:50K Watershed Atlas (WSA) code group widths (sum = 45 digits).
# Canonical layout example: 900-569800-08600-00000-0000-0000-000-000-000-000-000-000
_WSC_50K_GROUPS = (3, 6, 5, 5, 4, 4, 3, 3, 3, 3, 3, 3)


def trim_wsc(code: str) -> str:
    """Strip trailing ``-000000`` padding from an FWA watershed code. Idempotent."""
    if not code:
        return ""
    return _WSC_TRIM_RE.sub("", code)


def format_wsc_50k(code) -> str:
    """Format a 1:50K Watershed Atlas code (undelimited 45 digits) into the canonical dashed,
    trailing-zero-trimmed layout. Empty/None and the all-nines sentinel return "". To normalise
    the obstacles layer's already-dashed code, strip its dashes first: ``format_wsc_50k(v.replace('-',''))``."""
    if code is None:
        return ""
    if isinstance(code, float):
        if math.isnan(code):
            return ""
        code = str(int(code))
    digits = str(code).strip().replace("-", "")
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
    while len(parts) > 1 and set(parts[-1]) == {"0"}:
        parts.pop()
    return "-".join(parts)
