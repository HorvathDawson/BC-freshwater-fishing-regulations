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
    "100-382626-": "Williams Lake River watershed",
    "200-948755-": "Peace River watershed",
    "400-": "Skeena River watershed",
    "500-": "Nass River watershed",
}

#: The gnis item of the river each named basin is the watershed of (for the slow pin).
BASIN_RIVERS = {
    "100-": "gnis:39325",
    "100-342455-": "gnis:13744",
    "100-382626-": "gnis:27764",
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


def node_basin_code(n) -> str:
    """The code a node's WATERSHED membership is read from: its own FWA code, or — for a lake FWA
    gives none (`999`) — the code of the named watershed containing it (`derive_basin_wsc`)."""
    return (getattr(n, "wsc", "") or "") or (getattr(n, "basin_wsc", "") or "")


def basin_members(nodes, code: str) -> list[str]:
    """The node ids of `nodes` (StreamNode-likes with `wsc` and `node_id`) inside basin `code`."""
    return [n.node_id for n in nodes if in_basin(node_basin_code(n), code)]


def derive_basin_wsc(graph, polys: dict, watersheds) -> dict[str, int]:
    """Give every LAKE node with no FWA code the code of the smallest FWA named-watershed polygon
    containing it. Mutates `graph.nodes`; returns counts.

    WHY. FWA codes a waterbody its 1:20k network does not connect to anything `999-…` — kettle
    ponds, dugouts, closed depressions, ponds whose outlet channel is too small to map (83,853 lake
    nodes; province-wide the largest is 24 ha, the median 0.11 ha). With no code, no watershed rule
    could see them: Four Lakes (2.2 ha, in the Babine) got every Region 6 trout rule except "release
    lake trout from the Fraser and Skeena watersheds", because `in_basin 400-` had nothing to match.

    HOW, and not otherwise. The book's watershed is "all the streams and lakes that drain the land
    into a named waterbody" (p86); FWA's named-watershed polygon IS that land for its named water,
    so the pond is in the watershed whose land it sits on. The test is the polygon's REPRESENTATIVE
    POINT (always inside it) against the named watersheds, and the smallest container wins — the
    deepest named basin. NOT the nearest coded water: it disagrees with the polygon across divides
    (lake:329027507 is 1 km from a Nechako stream but inside the Skeena's Big Loon Creek watershed)
    and the user rejected it. A lake on no named watershed (the coastal fronts, ~10,900) keeps none.

    `polys` is {wbk: polygon}; `watersheds` an iterable of (trimmed code, area, polygon). Kept in
    `basin_wsc`, never `wsc`: the graph's hydrology is not touched, only membership.
    """
    import shapely
    from pipeline.common.models import NodeKind

    ws = [(c, a, g) for c, a, g in watersheds if c and g is not None and not g.is_empty]
    todo = [nid for nid, n in graph.nodes.items()
            if n.kind == NodeKind.lake and not (n.wsc or "") and n.wbk in polys
            and polys[n.wbk] is not None and not polys[n.wbk].is_empty]
    counts = {"codeless_lakes": sum(1 for n in graph.nodes.values()
                                    if n.kind == NodeKind.lake and not (n.wsc or "")),
              "given": 0}
    if not ws or not todo:
        return counts
    tree = shapely.STRtree([g for _c, _a, g in ws])
    pts = shapely.points([shapely.get_coordinates(polys[graph.nodes[nid].wbk]
                                                  .representative_point())[0] for nid in todo])
    pi, wi = tree.query(pts, predicate="within")
    best: dict[int, tuple[float, str]] = {}
    for p, w in zip(pi.tolist(), wi.tolist()):
        code, area = ws[w][0], float(ws[w][1] or 0.0)
        cur = best.get(p)
        if cur is None or (area, code) < cur:
            best[p] = (area, code)
    from dataclasses import replace
    for p, (_area, code) in sorted(best.items()):
        nid = todo[p]
        graph.nodes[nid] = replace(graph.nodes[nid], basin_wsc=code)
        counts["given"] += 1
    return counts


def basin_name(area_id: str) -> str | None:
    """The reader's name for a basin id, or None."""
    code = basin_code(area_id)
    return BASIN_NAMES.get(code) if code else None
