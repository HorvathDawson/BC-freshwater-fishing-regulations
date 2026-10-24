"""Where a water that touches nothing lies along its river — by geometry, for `Extent.watershed`.

A WATERSHED PART is decided by FWA code position (`extent._watershed_part`): a member whose code
joins the river at group `p` is placed by `p` against the cut. A member coded to the RIVER ITSELF
has no group, and goes where the water it touches goes. The floodplain lakes of the Fraser in
Region 5 touch nothing — no stream flows in or out of them in FWA — and carry the Fraser's own code
(`basin_wsc` '100', from the Fraser's named-watershed polygon). 151 of them fell out of BOTH white
sturgeon rules (closed upstream of the Williams Lake River, catch and release below it), and out of
the Fraser's Region 5 row and the sturgeon licence (SP-9).

Their side of the cut is WHERE THEY ARE: the river piece nearest to the lake's outline, among the
river's own pieces the measure cut placed. A lake beside the Fraser above the Williams Lake River
is on the upstream side; one below it, on the downstream side. The nearest piece is the containing
position the code would have given, had FWA coded the lake to a tributary.

The geometry is the atlas's own (`waterbody_polys.pkl`, `geometries.pkl` — the files the region
homes are measured on, `registry.regions`), read the first time a watershed part asks, and only
for the lakes it asks about. `attach` names the build on the graph; a graph with none (a unit-test
fixture) places no lake this way, never guesses.
"""

from __future__ import annotations

from pathlib import Path

_ATTR = "_position_build"
_CACHE_ATTR = "_position_geoms"


def attach(graph, build_dir) -> None:
    """Name the atlas build whose geometry positions this graph's code-less waters."""
    try:
        setattr(graph, _ATTR, str(Path(build_dir)))
    except (AttributeError, TypeError):
        pass


def _geoms(graph):
    got = getattr(graph, _CACHE_ATTR, None)
    if got is not None:
        return got
    build = getattr(graph, _ATTR, None)
    if not build:
        return None
    from pipeline.common.io.serialize import read_artifact
    got = (read_artifact(str(Path(build) / "waterbody_polys.pkl")),
           read_artifact(str(Path(build) / "geometries.pkl")))
    try:
        setattr(graph, _CACHE_ATTR, got)
    except (AttributeError, TypeError):
        pass
    return got


def nearest_pieces(graph, waters, pieces) -> dict[str, str]:
    """`{water: the piece of `pieces` nearest to its outline}` — for the waters with an outline,
    when the graph has a build attached (`attach`); `{}` otherwise. Ties go to the lower id, so
    the answer never depends on iteration order."""
    geoms = _geoms(graph)
    if geoms is None or not waters or not pieces:
        return {}
    outlines, lines = geoms
    from shapely.strtree import STRtree
    ids = sorted(p for p in pieces if lines.get(p) is not None and not lines[p].is_empty)
    if not ids:
        return {}
    tree = STRtree([lines[p] for p in ids])
    out: dict[str, str] = {}
    for w in sorted(waters):
        shape = outlines.get(w)
        if shape is None or shape.is_empty:
            continue
        i = tree.nearest(shape)
        if i is None:
            continue
        best = lines[ids[int(i)]].distance(shape)
        # `nearest` returns one of several at the same distance; take the lowest id among them.
        ties = [ids[int(j)] for j in tree.query(shape, predicate="dwithin", distance=best + 1e-6)
                if lines[ids[int(j)]].distance(shape) <= best + 1e-6]
        out[w] = min(ties) if ties else ids[int(i)]
    return out
