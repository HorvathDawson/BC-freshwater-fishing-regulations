"""THE SHORE HELPERS of the coast cut (T678 / T685, `atlas.splits.coast`; `build.cut_shores`).

The coast cut's own record and id family live here because every atlas since T660 carries them: `RECORD`
(`shore_cuts.json`, every line the cut looked at, cut or why not) and the cut family `SOURCE` (`shore:<blk>:<measure>`,
label `LABEL`). The helpers read a built graph the way the coast cut needs it: its blue lines as chains
(`graph_lines`), the boundaries already standing on each line (`standing_boundaries`), the lines a rank-1 limit governs
(`owner_limited`) and where an edge leaves its line (`edge_leave_measures`).

RETIRED (T678, 2026-11-20, after the first coast-cut atlas was promoted and its `.prev` is coast-cut too): the T665
polygon rule (`polygon_rule`, `polygon_inside`, the `HELD_*` lists — the owner's rulings are `waters/shore_held.json` —,
`atlas_tidal`, `reach_tidal`) and its sidecar `inside_tidal.json`. No served atlas was cut by it; every reader refuses an
atlas without the one tidal record (`coast.TIDAL_RECORD`).
"""
from __future__ import annotations

from typing import Mapping

from pipeline.common.models import NodeKind, StreamGraph

#: the cut family (`SplitPoint.source`, `sectionizer.AUTO_SOURCES`) and its id prefix
SOURCE = "shore"
#: two FWA segments whose measures differ by less than this are one boundary (float slop between segment rows)
_JOIN_M = 0.01
#: the atlas sidecar: every line the coast cut looked at, cut or why not; its `rule` names the rule that made it
RECORD = "shore_cuts.json"
#: every shore cut's label
LABEL = "shore"
_EPS = _JOIN_M


def graph_lines(graph: StreamGraph, geoms: Mapping[str, object]) -> list:
    """The graph's blue lines as chains, mouth first: the line the coast cut reads a water's edge on (`build.cut_shores`;
    never a merged `BlkChain`, see there). The graph cuts a line at its lake runs, so each run of touching stream pieces is one chain; where
    it meets a lake its inside interval ends at the middle of the lake run (`lo_end` / `hi_end`), as
    `clean_cut.stretches` ends a `BlkChain`'s."""
    from types import SimpleNamespace

    import shapely
    from shapely.geometry import LineString
    by_blk: dict[str, list] = {}
    for nid in sorted(graph.nodes):
        n = graph.nodes[nid]
        if n.kind == NodeKind.stream and n.blk:
            by_blk.setdefault(n.blk, []).append(n)
    out = []
    for blk in sorted(by_blk):
        ns = sorted(by_blk[blk], key=lambda n: (n.down_m, n.up_m))
        groups: list[list] = []
        for n in ns:
            if groups and abs(n.down_m - groups[-1][-1].up_m) <= _JOIN_M:
                groups[-1].append(n)
            else:
                groups.append([n])
        for gi, grp in enumerate(groups):
            xy: list = []
            for n in grp:
                g = geoms.get(n.node_id)
                if g is None or g.is_empty:
                    continue
                c = list(shapely.force_2d(g).coords)
                xy.extend(c[1:] if xy and c and c[0] == xy[-1] else c)
            if len(xy) < 2:
                continue
            lo_end = None if gi == 0 else (groups[gi - 1][-1].up_m + grp[0].down_m) / 2.0
            hi_end = None if gi == len(groups) - 1 else (grp[-1].up_m + groups[gi + 1][0].down_m) / 2.0
            out.append(SimpleNamespace(blk=blk, geometry=LineString(xy), mouth_measure=grp[0].down_m,
                                       waterbody_runs=(), lo_end=lo_end, hi_end=hi_end))
    return out


def standing_boundaries(graph: StreamGraph, skip_prefix: str = f"split:{SOURCE}:") -> dict[str, list[float]]:
    """`{blk: measures}` of the boundaries already standing inside each blue line (curated cuts, areas, lengths, lake
    edges), for `clean_cut.onto_existing`. A boundary that is only a shore cut (`skip_prefix`) is not standing: the
    coast cut replaces those."""
    out: dict[str, set] = {}
    for n in graph.nodes.values():
        if n.kind != NodeKind.stream or not n.blk:
            continue
        for b in (n.lower_bound, n.upper_bound):
            if b is None or b.route_measure is None:
                continue
            ids = [str(x) for x in (b.boundary_id, *b.aliases)]
            if ids and all(x.startswith(skip_prefix) for x in ids):
                continue
            if b.boundary_id in ("outlet", "headwaters"):
                continue
            out.setdefault(n.blk, set()).add(float(b.route_measure))
    return {k: sorted(v) for k, v in out.items()}


def owner_limited(graph: StreamGraph, tidal, limit_rows, waters=()) -> frozenset:
    """The blks the coast cut never cuts because the owner's own limit governs them: every line of a WATER (same gnis,
    or one registry water: `waters`, each the set of blks of one stream item) one of whose lines carries a `split` or
    `whole` row (`limit_rows`: `blk`, `limit`, as dicts or `tidal.Limit`) or holds a section of `tidal`. The registry
    waters are what joins an added line with no gnis to its water (T685: Port Moody's Turner Creek line is Noons Creek,
    which the owner's railway-bridge boundary governs; by gnis alone the coast rule cut it a second time)."""
    def val(r, k):
        return r.get(k) if isinstance(r, Mapping) else getattr(r, k, None)
    gnis_of: dict[str, str] = {}
    for n in graph.nodes.values():
        if n.kind == NodeKind.stream and n.blk and n.gnis_id:
            gnis_of.setdefault(n.blk, str(n.gnis_id))
    by_gnis: dict[str, set] = {}
    for b, g in gnis_of.items():
        by_gnis.setdefault(g, set()).add(b)
    lim = {str(val(r, "blk")) for r in limit_rows if val(r, "blk") and val(r, "limit") in ("split", "whole")}
    lim |= {graph.nodes[s].blk for s in tidal if s in graph.nodes and graph.nodes[s].blk}
    out = set(lim)
    for b in lim:
        if b in gnis_of:
            out |= by_gnis[gnis_of[b]]
    for w in waters:
        if not lim.isdisjoint(w):
            out |= set(w)
    return frozenset(out)


def edge_leave_measures(graph: StreamGraph, geoms, view=None):
    """`leaves_at` for `coast.coast_rule`: a function edge index -> the route measure, on the edge's FROM line, where it
    leaves that line (`braid_sweep.touch_measure`: the receiving line's point at the confluence, projected onto the
    from line), or None when it cannot be placed. `view`: a `braid_sweep.View` of this graph to reuse."""
    from pipeline.atlas.splits import braid_sweep as _bs
    view = view or _bs.View(graph, geoms)

    def leaves_at(i: int):
        e = graph.edges[i]
        f = graph.nodes.get(e.from_node)
        if f is None or f.kind != NodeKind.stream or not f.blk or e.to_node not in graph.nodes:
            return None
        m = _bs.touch_measure(view, e.to_node, i, e.from_node, f.blk)
        return None if m is None or m != m else float(m)
    return leaves_at
