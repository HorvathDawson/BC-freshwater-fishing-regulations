"""T660 (owner 2026-11-11): THE SHORE CUT — a stream's line ends where it leaves the water it enters.

FWA draws a creek on from its shore straight across a double-line river or an estuary to the receiving main flow; those
lines carry their own edge type (`fwa_edge_types.WATER_CROSSING`). Below the Skeena's tidal limit 38 creeks crossed the
estuary that way, so the creek's lowest section ran from its first confluence on land out over the tidal water to the
river's line, and every rule binding the creek coloured the estuary (the owner, on the T632 tidal sheets: "collapsed
to the boundary").

THE CUT. On every blue line whose MOUTH (its lowest stream piece) begins with a run of crossing lines lying IN TIDAL
WATER — at least `MIN_SHARE` of the run's length inside the DFO tidal-waters polygon (`tidal_boundary` in the build
gpkg, the polygon the generic tidal method reads: `tools.audit.tidal_candidates`, the map's `tidal_water`), sampled
every `SAMPLE_M` — and continues above it with any other line, the atlas cuts at the top of that run: the FWA segment
boundary where the creek's own line begins, an exact measure (no snapping). The piece below is the crossing; the piece
above is the creek. A crossing that is the whole line is not cut (nothing is above it). A run whose either piece would
be shorter than `SLIVER_M` is not cut. Inland, the same lines cross double-line rivers and lakes (329,452 of them,
53,877 km; 476 km in the polygon): they are never cut here.

THE ATLAS DOES NOT DECIDE TIDAL WATER HERE. It only gives a crossing in tidal water a section of its own. Whether that
section is tidal is the reach run's question (`reach.outside.tidal_crossing_owner`): exactly when it ends in tidal
water off the map. A creek crossing a river the owner's limit keeps freshwater to its mouth (the Bear, the Kemano) is
cut the same way and stays freshwater; `shore_cuts.json` lists every cut and every run not cut, with its share, for the
owner. The cut's id is `shore:<blk>:<int measure>` (minted, like `length:`), its source `shore`, its label `LABEL`
("shore": true whether the water it enters is tidal or not).
"""
from __future__ import annotations

from typing import Callable, Mapping

from pipeline.common import fwa_edge_types as _et
from pipeline.common.models import AnchorType, NodeKind, SplitPoint, StreamGraph
from pipeline.common.tolerances import SLIVER_M

#: the cut family (`SplitPoint.source`, `sectionizer.AUTO_SOURCES`) and its id prefix
SOURCE = "shore"
#: two FWA segments whose measures differ by less than this are one boundary (float slop between segment rows)
_JOIN_M = 0.01
#: a crossing run is IN tidal water when at least this share of its length lies in the polygon. MEASURED 2026-11-11 at
#: the mouths: 158 runs ending in the run's tidal water at >= 50 % (120.2 km), 3 under it (0.7 km)
MIN_SHARE = 0.5
#: the polygon is sampled along each line every this many metres
SAMPLE_M = 10.0
#: the atlas sidecar listing every crossing run at a mouth that touches tidal water: cut, or why not
RECORD = "shore_cuts.json"
#: every shore cut's label: where the creek's own line meets the water it enters (tidal or not: the reach decides)
LABEL = "shore"
RECORD_ABOUT = ("T660: every creek mouth whose FWA water-crossing run touches the DFO tidal polygon. A row without "
                "`not_cut` was cut at `m` (the top of the crossing); the reach run takes the piece below as tidal only "
                "when it ends in tidal water. Written by the atlas build (`atlas.splits.shore`); read by people.")


def crossing_run(node, fid_index: Mapping[str, tuple], crossing_fids) -> tuple[list[str], float, bool] | None:
    """The run of water-crossing lines at the foot of `node`: (its fids, the measure of its top, is anything above
    it). None when the node does not start with one. `fid_index` is fid -> (down_m, up_m, ...); `crossing_fids` the
    fids whose edge type is `fwa_edge_types.WATER_CROSSING`."""
    segs = sorted(((fid_index[f][0], fid_index[f][1], f) for f in node.member_fids if f in fid_index),
                  key=lambda r: (r[0], r[1], r[2]))
    if not segs or segs[0][2] not in crossing_fids or abs(segs[0][0] - node.down_m) > _JOIN_M:
        return None
    run, top = [segs[0][2]], segs[0][1]
    for lo, hi, f in segs[1:]:
        if f in crossing_fids and abs(lo - top) <= _JOIN_M:
            run.append(f)
            top = hi
            continue
        return run, top, True
    return run, top, False


#: POLICY (T660, the length cap's precedent `length_splits.junction_cuts`): a shore a braid of the line's OWN water
#: spans is never cut. Measured on the 2026-11-10 atlas: 97 of 629 shores, every one a side channel crossing back into
#: its own river (Fraser, Kemano, Bella Coola, San Juan …), none of the 163 crossings that end in tidal water. Cutting
#: there would make the braid sweep cut the river itself across its width (403 braid cuts, build 27's stop). The run
#: stays whole and freshwater, listed `not_cut` — never widened to tidal. False is the mutation pin.
A_SHORE_A_BRAID_SPANS_IS_NOT_CUT = True
#: the `not_cut` reason a spanned shore carries
SPANNED = "a braid of its own water spans the shore"


def shore_cuts(graph: StreamGraph, fid_index: Mapping[str, tuple], crossing_fids, share_of: Callable[[list], float],
               record: list | None = None, spanned: Callable[[str, float], bool] | None = None) -> list[SplitPoint]:
    """One SplitPoint per blue line whose mouth piece starts with a crossing run in tidal water (`share_of(run fids)`
    at least `MIN_SHARE`) and continues above it: at the top of that run. Sorted by blk, so the same graph gives the
    same list. `record` collects one row per crossing run at a mouth with a share above 0 — cut or not, and why.
    `spanned(blk, m)` (the braid sweep's gate, `braid_sweep.spans_a_braid`): a shore it answers True for is not cut
    (`A_SHORE_A_BRAID_SPANS_IS_NOT_CUT`)."""
    lowest: dict[str, str] = {}
    for nid in sorted(graph.nodes):
        n = graph.nodes[nid]
        if n.kind != NodeKind.stream or not n.blk:
            continue
        cur = lowest.get(n.blk)
        if cur is None or n.down_m < graph.nodes[cur].down_m:
            lowest[n.blk] = nid
    out: list[SplitPoint] = []
    for blk in sorted(lowest):
        n = graph.nodes[lowest[blk]]
        got = crossing_run(n, fid_index, crossing_fids)
        if got is None:
            continue
        run, top, above = got
        share = float(share_of(run))
        if share <= 0.0:
            continue
        why = ("" if share >= MIN_SHARE and above and top - n.down_m >= SLIVER_M and n.up_m - top >= SLIVER_M else
               f"under {MIN_SHARE:.0%} in tidal water" if share < MIN_SHARE else
               "the whole line is the crossing" if not above else f"a piece under {SLIVER_M:g} m")
        if not why and spanned is not None and A_SHORE_A_BRAID_SPANS_IS_NOT_CUT and spanned(blk, top):
            why = SPANNED
        if record is not None:
            record.append({"section": n.node_id, "blk": blk, "name": n.display_name, "m": round(top, 2),
                           "crossing_m": round(top - n.down_m, 1), "share": round(share, 3),
                           **({"not_cut": why} if why else {})})
        if why:
            continue
        out.append(SplitPoint(split_id=f"{SOURCE}:{blk}:{int(top)}", blk=blk, route_measure=top, fid="",
                              label=LABEL, anchor_type=AnchorType.point, source=SOURCE))
    return out


def share_in(polygon, geometries: Mapping[str, object]) -> Callable[[list], float]:
    """`share_of` for `shore_cuts`: the share of a run's length inside `polygon` (prepared once), from points every
    `SAMPLE_M` along each line, both ends included — deterministic, and fast on a coast-long polygon an exact
    intersection is not."""
    import numpy as np
    import shapely
    shapely.prepare(polygon)

    def share(fids: list) -> float:
        tot = inside = 0.0
        for f in fids:
            g = geometries.get(f)
            if g is None or g.is_empty:
                continue
            g = shapely.force_2d(g)
            n = max(2, int(g.length // SAMPLE_M) + 1)
            pts = shapely.line_interpolate_point(g, np.linspace(0.0, g.length, n))
            hit = shapely.contains_xy(polygon, shapely.get_x(pts), shapely.get_y(pts))
            tot += g.length
            inside += g.length * float(hit.mean())
        return inside / tot if tot else 0.0
    return share


def read_tidal_polygon(gpkg: str, bbox=None):
    """The DFO tidal-waters polygon (`tidal_boundary`, EPSG:3005), one geometry. A gpkg without it is refused (AGENTS
    37): a build that cut no shore would look healthy."""
    import pyogrio
    import shapely
    df = pyogrio.read_dataframe(gpkg, layer="tidal_boundary", bbox=bbox)
    geoms = [g for g in df.geometry if g is not None and not g.is_empty]
    if not geoms and bbox is None:
        raise SystemExit(f"shore: {gpkg} layer tidal_boundary holds no polygon")
    return shapely.union_all(geoms) if geoms else None
