"""WATERSHED MEMBERSHIP BY FWA CODE — `area:basin:<code prefix>`.

A watershed IS a prefix of the FWA watershed code: every stream, lake and wetland draining to the
Fraser carries a code starting `100-`, to the Chilcotin `100-342455-`, to the Peace `200-948755-`.
So "the Fraser River watershed in Region 6" is a set, not a walk. The tributary walk from a named
river cannot reach water with no mapped outflow or water behind a connector, and a zone rule printed
as a WHOLE WATERSHED must still cover it (user ruling, 2026-09-24).

ONE DEFINITION, TWO USERS. The registry build mints the MAJOR basins (`area:basin:100-` …) as
registry items, keyed by the code's first group; the reach resolver answers a basin the registry did
not mint (a sub-basin such as the Chilcotin's) from the graph with `in_basin`, which for a one-group
code is that same test — so a minted basin and a resolved one hold the same water
(`test_basin_areas` pins it). Minting every sub-basin would add more registry items than there are
waters.

The graph stores codes TRIMMED of their trailing zero groups (`100-342455`, the Fraser `100`), so a
member either IS the prefix's code or starts with the prefix.
"""

from __future__ import annotations

import re

BASIN = "area:basin:"

#: `100-`, `100-342455-`, `200-948755-837217-`: three-digit head, six-digit groups, trailing dash.
_CODE = re.compile(r"^\d{3}-(?:\d{6}-)*$")

#: THE RIVER EACH NAMED BASIN DRAINS TO — so a reader is told "Fraser River watershed", never
#: "100-". These are FWA facts (the river whose own code is the prefix), pinned against the graph by
#: `test_basin_areas.test_every_named_basin_is_its_rivers_code` (slow). A basin in the corpus with
#: no name here is refused by `test_every_basin_the_corpus_names_has_a_name`.
BASIN_NAMES = {
    "100-": "Fraser River watershed",
    "100-342455-": "Chilcotin River watershed",
    "200-948755-": "Peace River watershed",
    "400-": "Skeena River watershed",
    "500-": "Nass River watershed",
}

#: The gnis item of the river each named basin is the watershed of (for the slow pin).
BASIN_RIVERS = {
    "100-": "gnis:39325",
    "100-342455-": "gnis:13744",
    "200-948755-": "gnis:14619",
    "400-": "gnis:2936",
    "500-": "gnis:3206",
}


def basin_code(area_id: str) -> str | None:
    """`area:basin:100-342455-` -> `100-342455-`; None when `area_id` is not a basin id or its
    code is malformed (a malformed code must fail, never match nothing in silence)."""
    if not str(area_id).startswith(BASIN):
        return None
    code = str(area_id)[len(BASIN):]
    return code if _CODE.match(code) else None


def in_basin(wsc: str | None, code: str) -> bool:
    """Does a node whose (trimmed) watershed code is `wsc` drain to basin `code`?"""
    if not wsc:
        return False
    return wsc == code[:-1] or wsc.startswith(code)


def basin_members(nodes, code: str) -> list[str]:
    """The node ids of `nodes` (StreamNode-likes with `wsc` and `node_id`) inside basin `code`."""
    return [n.node_id for n in nodes if in_basin(getattr(n, "wsc", "") or "", code)]


def basin_name(area_id: str) -> str | None:
    """The reader's name for a basin id, or None."""
    code = basin_code(area_id)
    return BASIN_NAMES.get(code) if code else None
