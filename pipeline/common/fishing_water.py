"""NOT FISHING WATER (T152): FWA polygons that hold water and are not a place anyone fishes.

The FWA `manmade` layer puts sewage settling basins, mine tailing ponds and dumps beside the
reservoirs, so the atlas minted them as lake nodes and the tiles drew them as lakes: 275 of them,
12 Fort Nelson sewage basins under the "Liard River watershed" area rule and four tailing ponds of
93-231 ha on named creeks (Alkali, Pukaist, MacDonald, Wilfred), visible from z8. The app told an
angler these were lakes with the region's rules (T148 census, table B).

DECIDED ONCE, AT THE ATLAS, FROM THE FEATURE CODE: the build stamps the code on the polygon's node
(`StreamNode.not_fishing`) and every consumer reads that one field — the reach takes the node out
of every binding (`reach.classify.apply_limits`, limit `not_fishing_water`: no zone, area or water
rule, no licensing record), and the tiles draw it in no water layer (the decorative
`not_fishing_water` layer instead, carrying only its `kind` for the tap sheet, T292). None is named, so none is an item or searchable. The node stays in the graph: a creek routed through
a tailing pond keeps its topology.
"""

from __future__ import annotations

#: FWA FEATURE_CODE -> what it is. Pinned (`test_not_fishing_water.py`); a code is added here only
#: with the census row that shows it is no fishing water.
NOT_FISHING_WATER: dict[str, str] = {
    "EA26700110": "sewage settling basin",
    "AP90300100": "mine tailing pond",
    "AP09200000": "dump",
}

#: FWA FEATURE_CODE -> the tile's `kind` (T292): one short word per code, the only thing the map
#: carries about one. The app turns it into the tap sheet's words (`@app/core` `notFishingSheet`)
#: and refuses a value it does not know. Same keys as `NOT_FISHING_WATER` (pinned).
NOT_FISHING_KIND: dict[str, str] = {
    "EA26700110": "sewage",
    "AP90300100": "tailings",
    "AP09200000": "dump",
}


def is_not_fishing(feature_code: str | None) -> bool:
    return (feature_code or "").strip().upper() in NOT_FISHING_WATER


def not_fishing_kind(feature_code: str | None) -> str:
    """The tile `kind` for a stamped code; raises on a code that is not NOT FISHING WATER."""
    return NOT_FISHING_KIND[(feature_code or "").strip().upper()]


def not_fishing_sections(graph) -> frozenset[str]:
    """Every node the atlas stamped not fishing water. Cached on the graph (one scan of ~1.96M
    nodes; the review app asks per rule)."""
    if graph is None:
        return frozenset()
    cached = getattr(graph, "_not_fishing_cache", None)
    if cached is not None:
        return cached
    got = frozenset(nid for nid, n in graph.nodes.items() if getattr(n, "not_fishing", ""))
    try:
        setattr(graph, "_not_fishing_cache", got)
    except (AttributeError, TypeError):
        pass
    return got
