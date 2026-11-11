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


# ------------------------------------------------------------------------------------------------
# T665 (owner 2026-11-11, design `handoff/PLAN/app-integration/T665-design.md`): THE POLYGON RULE. Not wired into the
# build yet: the in-memory dry run (`T665-dryrun.md`) reads it on the live atlas; `build.cut_shores` takes it in the
# next atlas rebuild (with T664 and the nine DFO cuts), when `shore_cuts` above goes.
#
# One pass of the clean cutter (`clean_cut.decide_chain`, the parks' cutter: exact to the polygon edge, dips under
# `clean_cut.DIP_M` ignored, a run that short at a line end taken as the end) gives each blue line its stretches inside
# the DFO tidal polygon. Two rules read them:
#   A. a line wholly inside the polygon is tidal water (not drawn, bound by nothing) — named or not;
#   B. a creek whose mouth is inside and whose water, followed down while inside, reaches tidal water off the map (a
#      piece below a curated `split`/`cut`, a `whole` tidal water, or a rule-A line) is cut ONCE, at the top of the
#      inside stretch that starts at its mouth (outside runs up to `BRIDGE_M` bridged: owner D3, the first real exit).
#      Whether the piece below goes tidal stays the reach's question (`reach.outside.tidal_crossing_owner`).
# ------------------------------------------------------------------------------------------------

#: POLICY (ticket ruling 2): every line wholly inside the polygon is tidal water. False is the mutation pin
A_LINE_WHOLLY_INSIDE_THE_POLYGON_IS_TIDAL = True
#: POLICY (ticket ruling 1): a creek joining tidal water is cut where it leaves the polygon. False is the mutation pin
A_CREEK_INTO_TIDAL_WATER_IS_CUT_AT_ITS_POLYGON_EXIT = True
#: POLICY (owner D4, 2026-11-11): a creek with the owner's `mouth` row in `tidal_limits.json` keeps its limit at the END
#: of its mapped line ("line end keeps it consistent"): no polygon-edge cut on it. False is the mutation pin
AN_OWNER_MOUTH_ROW_KEEPS_THE_LINE_END = True
#: owner D3: an outside run of at most this many metres between two inside stretches does not end the creek's tidal
#: stretch (the first REAL exit); a longer one does, and what lies inside higher up stays freshwater
BRIDGE_M = 25.0
#: the not-cut reasons (each lands in the record: none is silent)
OWNER_MOUTH = "owner mouth row: the limit stays at the end of the line (D4)"
EXIT_AT_TOP = "the exit is the line's top: the whole line is tidal"
LAKE_BEFORE_EXIT = "a lake on the line before the exit (lakes are never cut)"
MOUTH_OUTSIDE = "mouth outside the polygon (D6)"
FRESH_RECEIVER = "joins fresh water (D1: no shore cut)"
OWNER_LIMIT = "the owner's split/whole limit for this water governs it"
#: POLICY (dry run 2026-11-11): a line of a water the owner gave a `split` or `whole` limit, or holding tidal water off
#: the map already, is never cut by the polygon: its curated limit is where its tidal water ends (unguarded, the rule
#: cut the Pitt 3.4 km and the Nitinat 23.9 km above their limits). False is the mutation pin
THE_OWNERS_LIMIT_GOVERNS_ITS_OWN_WATER = True
_EPS = _JOIN_M


def _iv_contains(ivs, lo: float, hi: float) -> bool:
    return any(a <= lo + _EPS and b >= hi - _EPS for a, b in ivs)


def _iv_at(ivs, m: float) -> bool:
    return any(a <= m + _EPS and m < b for a, b in ivs)


def bridged(ivs, gap_m: float = BRIDGE_M) -> list[tuple[float, float]]:
    """Inside intervals (sorted, absolute) joined across every outside run of at most `gap_m`."""
    out: list[list[float]] = []
    for a, b in sorted(ivs):
        if out and a - out[-1][1] <= gap_m:
            out[-1][1] = max(out[-1][1], b)
        else:
            out.append([a, b])
    return [(a, b) for a, b in out]


def polygon_chunks(poly):
    """The polygon's edge in short runs (`border.outline_chunks`) behind an STRtree: an exact crossing test against a
    601,582-vertex coast is then local to each line."""
    from shapely.strtree import STRtree

    from pipeline.atlas.splits.border import outline_chunks
    chunks = outline_chunks(poly)
    return chunks, STRtree(chunks)


def chain_inside(chain, poly, chunks=None) -> list[tuple[float, float]]:
    """One blue line's stretches inside `poly`: `clean_cut.decide_chain` with dips (and outside runs) under
    `clean_cut.DIP_M` ignored. Absolute route measures; ±inf where a stretch reaches the line's own mouth or source, the
    middle of the lake run where it reaches a lake (`clean_cut.stretches`)."""
    from shapely.geometry import MultiLineString

    from pipeline.atlas.splits import clean_cut as _cc
    chunks = chunks or polygon_chunks(poly)
    parts, tree = chunks
    boundary = MultiLineString([parts[i] for i in sorted(tree.query(chain.geometry, predicate="intersects"))])
    _cuts, inside, _dips = _cc.decide_chain(chain, poly, boundary, 0.0, rejoin_m=_cc.DIP_M)
    return sorted(inside)


def polygon_inside(chains, poly) -> dict[str, list[tuple[float, float]]]:
    """`{blk: inside intervals}` for every chain meeting `poly` (`chain_inside`); a chain the polygon does not reach is
    absent. Chains are any objects with `blk`, `geometry` (mouth first), `mouth_measure`, `waterbody_runs`
    (`BlkChain`, or `graph_lines` for a built atlas). Deterministic: by blk."""
    import math

    import numpy as np
    import shapely
    shapely.prepare(poly)
    chunks = polygon_chunks(poly)
    keep = [c for c in chains if c.geometry is not None and not c.geometry.is_empty]
    if not keep:
        return {}
    arr = np.array([c.geometry for c in keep], dtype=object)
    meets = np.zeros(len(keep), dtype=bool)
    meets[np.unique(chunks[1].query(arr, predicate="intersects")[0])] = True
    first = shapely.get_point(arr, 0)
    meets |= shapely.contains_xy(poly, shapely.get_x(first), shapely.get_y(first))
    out: dict[str, list] = {}
    for i in sorted(np.flatnonzero(meets), key=lambda i: (keep[i].blk, keep[i].mouth_measure)):
        c = keep[i]
        ivs = chain_inside(c, poly, chunks)
        lo_end, hi_end = getattr(c, "lo_end", None), getattr(c, "hi_end", None)
        ivs = [(lo_end if (a == -math.inf and lo_end is not None) else a,
                hi_end if (b == math.inf and hi_end is not None) else b) for a, b in ivs]
        if ivs:
            out.setdefault(c.blk, []).extend(ivs)
    return {b: sorted(v) for b, v in out.items()}


def graph_lines(graph: StreamGraph, geoms: Mapping[str, object]) -> list:
    """A BUILT atlas's blue lines as chains for `polygon_inside` (the sidecar path and the dry run: the build itself
    has `BlkChain`s). The graph cuts a line at its lake runs, so each run of touching stream pieces is one chain; where
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
    polygon rule replaces those."""
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


def owner_limited(graph: StreamGraph, tidal, limit_rows) -> tuple[frozenset, frozenset]:
    """(limited, mouth) blk sets for `polygon_rule` from the curated `tidal_limits.json` rows (`blk`, `limit`): every line
    of a WATER (same gnis) one of whose lines carries a `split` or `whole` row or holds a `tidal` section is limited;
    every line of a water with a `mouth` row (and no split/whole) keeps its line end (D4, by water, so a river's side
    channels follow its mainstem)."""
    gnis_of: dict[str, str] = {}
    for n in graph.nodes.values():
        if n.kind == NodeKind.stream and n.blk and n.gnis_id:
            gnis_of.setdefault(n.blk, n.gnis_id)
    by_gnis: dict[str, set] = {}
    for b, g in gnis_of.items():
        by_gnis.setdefault(g, set()).add(b)

    def water(blks):
        out = set(blks)
        for b in blks:
            if b in gnis_of:
                out |= by_gnis[gnis_of[b]]
        return out
    lim = {str(r["blk"]) for r in limit_rows if r.get("blk") and r.get("limit") in ("split", "whole")}
    lim |= {graph.nodes[s].blk for s in tidal if s in graph.nodes and graph.nodes[s].blk}
    limited = water(lim)
    mouth = water({str(r["blk"]) for r in limit_rows if r.get("blk") and r.get("limit") == "mouth"}) - limited
    return frozenset(limited), frozenset(mouth)


def polygon_rule(graph: StreamGraph, inside: Mapping[str, list], tidal, mouth_rows=frozenset(),
                 existing: Mapping[str, list] | None = None, limited=frozenset()) -> tuple[list[SplitPoint], list[dict], list[dict]]:
    """THE T665 RULE over `inside` (`polygon_inside`). `tidal`: the section ids of tidal water OFF THE MAP under the
    curated limits (`reach.outside.is_cut_owner` owners other than crossings). `mouth_rows`: the blks the owner gave a
    `mouth` limit (D4). `existing`: `standing_boundaries`. `limited`: lines whose own curated limit governs
    (`owner_limited`).

    Returns (cut points, rule-A lines, rows): one row per line whose mouth is inside the polygon or that meets it and
    joins tidal water — cut or why not. Sorted by blk."""
    from pipeline.atlas.splits import clean_cut as _cc
    existing = existing or {}
    tidal = frozenset(tidal)
    lines: dict[str, list] = {}
    for nid in sorted(graph.nodes):
        n = graph.nodes[nid]
        if n.kind == NodeKind.stream and n.blk:
            lines.setdefault(n.blk, []).append(n)
    for v in lines.values():
        v.sort(key=lambda n: (n.down_m, n.up_m))

    # RULE A: every stream piece of the line inside (a line already wholly tidal water is not counted again)
    rule_a: dict[str, dict] = {}
    if A_LINE_WHOLLY_INSIDE_THE_POLYGON_IS_TIDAL:
        for blk in sorted(inside):
            ns = lines.get(blk) or []
            if not ns or not all(_iv_contains(inside[blk], n.down_m, n.up_m) for n in ns):
                continue
            if all(n.node_id in tidal for n in ns):
                continue
            rule_a[blk] = {"blk": blk, "name": next((n.display_name for n in ns if n.display_name), ""),
                           "sections": [n.node_id for n in ns], "length_m": round(sum(n.length_m for n in ns), 1),
                           "gnis": sorted({n.gnis_id for n in ns if n.gnis_id})}

    def first_stretch(blk):
        """(mouth node, exit measure, re-enters) for a line whose mouth is inside, else None."""
        ns = lines[blk]
        ivs = bridged(inside.get(blk) or ())
        if not ivs or not _iv_at(ivs, ns[0].down_m):
            return None
        return ns[0], ivs[0][1], len(ivs) > 1

    stretch = {blk: first_stretch(blk) for blk in sorted(inside) if blk in lines}

    def below_exit(n) -> bool:
        st = stretch.get(n.blk)
        return st is not None and n.down_m < st[1] - _EPS

    # the candidate filter: from the line's outlets, down the network while still inside the polygon, to tidal water
    def reaches_tidal(start) -> str | None:
        seen, todo = set(), list(start)
        while todo:
            d = todo.pop()
            if d in seen:
                continue
            seen.add(d)
            if d in tidal:
                return "curated"
            n = graph.nodes.get(d)
            if n is None:
                continue
            if n.blk and n.blk in rule_a:
                return "inside line"
            if n.kind != NodeKind.stream or not n.blk or not _iv_at(inside.get(n.blk) or (), n.down_m):
                continue
            todo.extend(graph.edges[i].to_node for i in graph.down_adj.get(d, ()))
        return None

    def outlets(n):
        return sorted(graph.edges[i].to_node for i in graph.down_adj.get(n.node_id, ()))

    pts: list[SplitPoint] = []
    rows: list[dict] = []
    cut_blks: dict[str, float] = {}
    for blk in sorted(lines):
        if blk in rule_a or blk not in inside:
            continue
        ns = lines[blk]
        mouth = ns[0]
        outs = outlets(mouth)
        st = stretch.get(blk)
        via = reaches_tidal(outs) if outs else None
        top = ns[-1].up_m
        row = {"blk": blk, "name": next((n.display_name for n in ns if n.display_name), ""), "section": mouth.node_id,
               "mouth_m": round(mouth.down_m, 2), "top_m": round(top, 2), "outlets": outs, "joins": via}
        if st is None:
            if via is None:
                continue
            rows.append({**row, "not_cut": MOUTH_OUTSIDE})
            continue
        _, exit_m, reenters = st
        lake_below = any(b.down_m - a.up_m > _EPS for a, b in zip(ns, ns[1:]) if b.down_m < exit_m)
        row.update(exit_m=round(min(exit_m, top), 2), inside_m=round(min(exit_m, top) - mouth.down_m, 1),
                   reenters=reenters, **({"lake_below_exit": True} if lake_below else {}))
        if via is None:
            rows.append({**row, "not_cut": FRESH_RECEIVER})
            continue
        why = ""
        if THE_OWNERS_LIMIT_GOVERNS_ITS_OWN_WATER and blk in limited:
            why = OWNER_LIMIT
        elif AN_OWNER_MOUTH_ROW_KEEPS_THE_LINE_END and blk in mouth_rows:
            why = OWNER_MOUTH
        elif exit_m >= top - _cc.DIP_M:
            why = EXIT_AT_TOP
        elif not any(n.down_m - _EPS <= exit_m <= n.up_m + _EPS for n in ns):
            why = LAKE_BEFORE_EXIT
        elif not A_CREEK_INTO_TIDAL_WATER_IS_CUT_AT_ITS_POLYGON_EXIT:
            why = "the polygon rule is off"
        if why:
            rows.append({**row, "not_cut": why})
            continue
        at = _cc.onto_existing(exit_m, existing.get(blk, ()), _cc.DIP_M)
        cut_blks[blk] = at
        rows.append({**row, "cut_m": round(at, 2), **({"onto_existing": True} if at != exit_m else {})})
        pts.append(SplitPoint(split_id=f"{SOURCE}:{blk}:{int(at)}", blk=blk, route_measure=at, fid="", label=LABEL,
                              anchor_type=AnchorType.point, source=SOURCE))

    # what the reach will decide for each cut: the piece below is tidal when every outlet is tidal water off the map
    memo: dict[str, bool] = {}

    def tidal_like(d, stack=()) -> bool:
        if d in tidal:
            return True
        n = graph.nodes.get(d)
        if n is None or not n.blk:
            return False
        if n.blk in rule_a:
            return True
        if n.blk not in cut_blks or n.down_m >= cut_blks[n.blk] - _EPS or n.blk in stack:
            return False
        if n.blk not in memo:
            m0 = lines[n.blk][0]
            o = outlets(m0)
            memo[n.blk] = bool(o) and all(tidal_like(x, stack + (n.blk,)) for x in o)
        return memo[n.blk]
    for r in rows:
        if "cut_m" in r:
            r["below_cut_tidal"] = tidal_like(r["section"]) if r["blk"] in cut_blks else False
    return pts, [rule_a[b] for b in sorted(rule_a)], rows
