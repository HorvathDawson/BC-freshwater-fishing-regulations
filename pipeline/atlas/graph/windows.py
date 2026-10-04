"""WHERE A POLYGON SITS ON ITS BLUE LINE — one reader of the graph, no stored field.

A waterbody node (`lake:{wbk}`) has `blk=""` and no measures: FWA cut the line at the polygon and
the graph joins the polygon to the pieces either side by `lake_in` / `lake_out` edges, whose
`at_measure` is the route measure on the piece's blue line where the water enters and leaves. For
a river's OWN polygon (`registry.flowing`: the Stellako's wide reach, a slough's polygons) that gap
IS the river's measure window: `lo` = where the line leaves the polygon downstream (`lake_out`,
the next piece's `down_m`), `hi` = where it enters upstream (`lake_in`, the previous piece's
`up_m`). Measured on the Stellako: 2,615.9-2,998.1 closes the gap between `356362768:2125` and
`:2998` exactly; every East Gribbell polygon fills its gap.

ONE FUNCTION, NOT A FIELD (FREV/sloughs F5): the reach's measure placement (`extent._by_measure`)
and the bundle's spans (`spans.compute`) both read it from the same graph, so there is no second
artifact to keep in step.
"""
from __future__ import annotations


def polygon_window(graph, section_id: str, blk: str | None = None):
    """`(blk, lo, hi)` for a waterbody node on blue line `blk` (default: its through-line — the
    line it both enters and leaves, else the one it leaves by, else the one it enters from), or
    None when it touches no line. `lo` is None for a polygon no line leaves (it is the line's
    mouth end), `hi` None for one no line enters (its head: nothing lies above it on that line)."""
    n = graph.nodes.get(section_id)
    if n is None or not graph.edges:
        return None
    outs: dict[str, float] = {}             # blk -> the lowest measure the line leaves at
    ins: dict[str, float] = {}              # blk -> the highest measure the line enters at
    for ei in graph.down_adj.get(section_id, ()):
        e = graph.edges[ei]
        to = graph.nodes.get(e.to_node)
        if to is not None and to.blk and e.kind == "lake_out":
            m = float(e.at_measure)
            outs[to.blk] = min(outs.get(to.blk, m), m)
    for ei in graph.up_adj.get(section_id, ()):
        e = graph.edges[ei]
        fr = graph.nodes.get(e.from_node)
        if fr is not None and fr.blk and e.kind == "lake_in":
            m = float(e.at_measure)
            ins[fr.blk] = max(ins.get(fr.blk, m), m)
    if blk is None:
        both = sorted(set(outs) & set(ins))
        blk = both[0] if both else (sorted(outs)[0] if outs else (sorted(ins)[0] if ins else None))
    if blk is None or (blk not in outs and blk not in ins):
        return None
    return blk, outs.get(blk), ins.get(blk)
