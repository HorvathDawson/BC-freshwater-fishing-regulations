"""The exact-key join: FWA 50K group code -> the lake nodes carrying it.

WHY THIS IS BUILT HERE AND NOT ON THE GRAPH. The graph's lake nodes already carry `wbk`
(WATERBODY_KEY), and the lakes layer is keyed on the same column — so the group code can be
looked up at bundle time from the source, in one pass over one column, rather than being
propagated through an 18-minute, 9 GB graph rebuild (AGENTS rule 17). It also stays fresh
with the source instead of being frozen at whenever the graph was last built.

WHAT THE KEY IS. `WATERBODY_KEY_GROUP_CODE_50K` in `FWA_LAKES_POLY` carries the same string
FIDQ publishes as `WATERBODY_IDENTIFIER` (`02322SAJR`). That is what makes stocking an exact
join rather than a name search, and v1's own notes record it resolving the large majority of
rows on its own.

ONE CODE MAY COVER SEVERAL WATERS — the FWA's own multi-part waterbody grouping, or a
genuine collision — so this returns a LIST per code and the caller breaks the tie on distance
to FIDQ's anchor point. Collapsing it to one here would hide the ambiguity at the only place
it can still be resolved honestly.
"""

from __future__ import annotations

from pathlib import Path

LAYER = "lakes"
KEY = "WATERBODY_KEY"
CODE = "WATERBODY_KEY_GROUP_CODE_50K"


def build_identifier_index(gpkg: Path, graph) -> dict[str, list[str]]:
    """``{group_code: [node_id, ...]}`` for every lake node whose wbk carries a code.

    Nodes are returned sorted, so a rebuild that changed nothing produces identical bytes.
    """
    import fiona

    if not gpkg.exists():
        return {}

    # wbk -> the lake nodes with that key. Several nodes may share one: a lake split into
    # closure areas is still one waterbody.
    nodes_by_wbk: dict[str, list[str]] = {}
    for nid, n in graph.nodes.items():
        if getattr(n, "wbk", "") and str(n.kind).endswith("lake"):
            nodes_by_wbk.setdefault(str(n.wbk), []).append(nid)

    out: dict[str, list[str]] = {}
    with fiona.open(gpkg, layer=LAYER) as src:
        if CODE not in src.schema["properties"]:
            return {}                # an older extract; the caller falls back to names
        for feat in src:
            p = feat["properties"]
            code = (p.get(CODE) or "").strip()
            wbk = p.get(KEY)
            if not code or wbk is None:
                continue
            for nid in nodes_by_wbk.get(str(wbk), ()):
                out.setdefault(code, []).append(nid)

    return {c: sorted(set(v)) for c, v in sorted(out.items())}
