"""WSC (Watershed Code) utilities — zero external dependencies.

THE ONE OWNER OF HOW A WATERSHED CODE IS READ (T117, owner 2026-10-22: one decision = one function).
FWA codes nest: a tributary's code is its river's code plus one more `-`-separated group. Before this
module there were more than ten spellings of "is this code inside that one" across the pipeline — with a
dash guard and without, an empty parent read as yes in one place and no in another. Every one now asks
`relation` (or a name built on it), and `test_wsc_relation.py` pins that no other module slices or
prefix-compares a code.

What each answer means where a code is MISSING (`""`) is the caller's call, made in the open: `relation`
says `UNKNOWN`, and the caller decides whether unknown is a yes (the atlas's edge filter keeps an edge it
cannot judge) or a no (a gauge shed claims nothing it cannot prove).
"""

from __future__ import annotations

import math
import re

_WSC_TRIM_RE = re.compile(r"(-000000)+$")

#: `relation(a, b)`: how the water coded `a` stands to the water coded `b`.
SAME = "same"          # one code: the same water (or a side channel of it, which FWA codes alike)
BELOW = "below"        # `a` extends `b`: `a` drains into `b` (a tributary, at any depth)
ABOVE = "above"        # `b` extends `a`: `a` is a water `b` drains into
APART = "apart"        # neither: two branches
UNKNOWN = "unknown"    # a code is missing: the code cannot say

#: FWA's raw "no watershed code" (`999-999999-…`): a fid carrying it belongs to no watershed.
FWA_NO_CODE = "999-999999"
#: The first group FWA gives a water it cannot place in a watershed.
UNKNOWN_ROOT = "999"
#: The Coastal Rivers drainages: first groups 900, 905, … 995.
COASTAL_ROOTS = tuple(str(p) for p in range(900, 1000, 5))


def groups(code: str | None) -> list[str]:
    """The code's groups, outermost first (`"100-077501"` -> `["100", "077501"]`); `[]` for none."""
    return code.split("-") if code else []


def root(code: str | None) -> str:
    """The first group — the drainage a code sits in (`"100"` for the Fraser); `""` for none."""
    return code.split("-", 1)[0] if code else ""


def parent(code: str | None) -> str:
    """The code one level up (the river this water joins); the code itself for a root."""
    return code.rsplit("-", 1)[0] if code and "-" in code else (code or "")


def ancestors(code: str | None) -> tuple[str, ...]:
    """Every STRICT ancestor of `code`, outermost first (`"100-1-2"` -> `("100", "100-1")`)."""
    g = groups(code)
    return tuple("-".join(g[:i]) for i in range(1, len(g)))


def head(code: str | None, n: int) -> str:
    """The code's first `n` groups (`head("910-665062-1", 2)` -> `"910-665062"`)."""
    return "-".join(groups(code)[:n])


def join(*parts: str) -> str:
    """Groups (or codes) joined into one code."""
    return "-".join(parts)


def relation(a: str | None, b: str | None) -> str:
    """How the water coded `a` stands to the water coded `b`: `SAME`, `BELOW` (a tributary of `b`, at
    any depth), `ABOVE` (a water `b` drains into), `APART`, or `UNKNOWN` when either code is missing.
    Dash-guarded: `100-0259560` is never below `100-025956`."""
    if not a or not b:
        return UNKNOWN
    if a == b:
        return SAME
    if a.startswith(b + "-"):
        return BELOW
    if b.startswith(a + "-"):
        return ABOVE
    return APART


def is_at_or_below(code: str | None, parent_code: str | None) -> bool:
    """Is `code` the water `parent_code` names or one draining into it? False where either is missing."""
    return relation(code, parent_code) in (SAME, BELOW)


def is_strictly_below(code: str | None, parent_code: str | None) -> bool:
    """Is `code` a tributary (at any depth) of `parent_code` — never the water itself? False where either
    is missing."""
    return relation(code, parent_code) == BELOW


def is_unknown(code: str | None) -> bool:
    """A code that places nothing: missing, or FWA's `999` (no watershed)."""
    return not code or root(str(code)) == UNKNOWN_ROOT


def is_no_code(raw: str | None) -> bool:
    """A RAW (untrimmed) FWA code that is FWA's own "no watershed code" sentinel (`999-999999-…`)."""
    return bool(raw) and str(raw).startswith(FWA_NO_CODE)


def is_coastal(code: str | None) -> bool:
    """In the Coastal Rivers drainage — a 9xx first group of the coastal set."""
    return root(trim_wsc(code or "")) in COASTAL_ROOTS


def in_nine_hundreds(code: str | None) -> bool:
    """A first group in the 9xx range: the coastal drainages and FWA's `999` alike."""
    return root(code).startswith("9")


def has_prefix(code: str | None, prefixes) -> bool:
    """Does `code` start with one of the CONFIGURED `prefixes` (`fwa_exclude`, the tidal leaf-prune
    prefixes) — raw text prefixes as the config writes them, not codes compared as waters."""
    p = tuple(prefixes or ())
    return bool(code and p) and code.startswith(p)


def group_after(code: str, river: str) -> str | None:
    """The group that follows `river`'s code in `code` — where the water coded `code` joins `river`
    (or the branch of `river`'s basin it lies in). None when `code` is `river` or not below it."""
    if not is_strictly_below(code, river):
        return None
    return code[len(river) + 1:].split("-", 1)[0]

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
