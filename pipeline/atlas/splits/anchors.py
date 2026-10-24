"""Split anchor resolution (04): an authored SplitDef -> concrete SplitPoint(s) on blue line(s).

Every anchor normalizes to an absolute DOWNSTREAM_ROUTE_MEASURE on a target BLK, using geometry
(so it runs before/independent of the graph). Target scope selects the eligible blue lines:
`blk` (one), `gnis_id` (all with that gnis), or `wsc` (river + side channels) — a point/line/mu
cut may thus land on several BLKs (main + side channels), one SplitPoint each.

- point       : project the coord onto the target blue line (kept iff within `proximity_m`).
                An optional `offset_m`/`offset_dir` then shifts the cut that many metres up/down
                the channel (e.g. "100 m downstream of the falls"), clamped to the channel ends.
- line        : intersect the cut line with the target blue line; measure the crossing.
- confluence  : the tributary (by `tributary_blk` or `tributary_wsc`) has a mouth; project it
                onto the target (parent) blue line -> the confluence measure. `tributary_wsc` is
                preferred: the tributary's WSC must be a strict descendant of the parent's WSC
                (self-validating; a failed check keeps the split but records a `concern`). The
                parent may be unnamed (author supplies `label` + a name override elsewhere).
- lake        : the target's waterbody-run for `wbk` -> its boundary measure. NOTE lakes already
                split the BLK in the graph build, so this is a no-op split; it exists so a
                lake-anchored reg resolves to the (already-present) boundary for matching.
- mu_boundary : the shared boundary line of two ADJACENT MUs (`mu_a`,`mu_b`) intersected with the
                target blue line -> a SINGLE crossing measure (collapsed to one even where the
                river runs along the boundary; multi-crossing is deduped + flagged). Needs
                `mu_polys` context.
"""

from __future__ import annotations

from collections import defaultdict
from functools import lru_cache
from typing import Optional

from shapely.geometry import LineString, Point

from pipeline.common.models import AnchorType, BlkChain, SplitDef, SplitPoint


@lru_cache(maxsize=1)
def _to_3005():
    from pyproj import Transformer
    return Transformer.from_crs(4326, 3005, always_xy=True)   # lon/lat -> BC Albers


def _pt(coord, is_lonlat: bool) -> Point:
    if is_lonlat:
        x, y = _to_3005().transform(coord[0], coord[1])
        return Point(x, y)
    return Point(coord)


def _line(coords, is_lonlat: bool) -> LineString:
    return LineString([_pt(c, is_lonlat).coords[0] for c in coords])


from pipeline.common.utils.wsc import trim_wsc as _trim_wsc   # shared util (pipeline/wsc.py); prefix-safe WSC trim
from pipeline.common.utils import wsc as _wsc


def _target_blks(sd: SplitDef, chains: list[BlkChain], descendants: bool = False) -> list[str]:
    if sd.blk:
        return [sd.blk]
    if sd.gnis_id:
        return [c.blk for c in chains if c.gnis_id == sd.gnis_id]
    if sd.wsc:
        w = _trim_wsc(sd.wsc)
        if descendants:
            # the whole WSC subtree: the trunk itself + everything draining into it (prefix match,
            # dash-guarded so '100-025956' never matches a sibling like '100-0259560').
            return [c.blk for c in chains
                    if _wsc.is_at_or_below(_trim_wsc(c.fwa_watershed_code), w)]
        return [c.blk for c in chains if _trim_wsc(c.fwa_watershed_code) == w]
    return []


def _points(geom) -> list[Point]:
    """Extract crossing points from a stream ∩ boundary result (Point/MultiPoint/Line/collection).
    For an overlap LineString (river running along the boundary) use its endpoints (enter/exit)."""
    if geom is None or geom.is_empty:
        return []
    t = geom.geom_type
    if t == "Point":
        return [geom]
    if t in ("MultiPoint", "GeometryCollection", "MultiLineString"):
        out: list[Point] = []
        for g in geom.geoms:
            out.extend(_points(g))
        return out
    if t == "LineString":
        c = list(geom.coords)
        return [Point(c[0]), Point(c[-1])]
    return []


def _cross_measure(g, a, b, boundary) -> float:
    """Along-``g`` measure where segment a->b crosses the polygon boundary (fallback: the b vertex)."""
    pts = _points(LineString([a, b]).intersection(boundary))
    return g.project(pts[0] if pts else Point(b))


def _area_transition_measures(g, poly, boundary) -> list[float]:
    """First-enter + last-exit cut measures for a stream crossing an area polygon. Absorbs
    boundary-following oscillation (a stream weaving along the edge): cut once where it FIRST enters
    the polygon and once where it LAST exits — everything between is treated as inside. <=2 cuts (1 if
    it runs to its headwater inside or starts inside at the mouth; 0 if it never enters)."""
    from shapely.prepared import prep
    coords = list(g.coords)
    pp = prep(poly)
    inside = [pp.contains(Point(xy)) for xy in coords]
    if not any(inside):
        # A SMALL POLYGON CAN BE CROSSED BETWEEN TWO VERTICES.
        #
        # Containment is tested per VERTEX, which is exact for a park — kilometres across, far
        # wider than the spacing of the points describing a river. It fails silently for a small
        # one. The Landstrom Bar sign zone is 11.5 ha and clips the Fraser mainstem for 138 m; the
        # Fraser's chain is 1,407 km long and simply has no vertex in that 138 m, so every vertex
        # read as outside and the stream was never cut. The rule then bound the whole 10.6 km
        # gauge-to-gauge section it sits in — a closure spread over ten times the water it covers.
        #
        # The side channel beside it cut correctly, which is what makes this the dangerous kind of
        # bug: the area reports cuts, the membership flag is stamped, and only the one line that
        # matters is missing.
        #
        # So when no vertex is inside but the LINE still meets the polygon, take the enter/exit
        # straight off the intersection. Same first-enter/last-exit semantics as below.
        seg = g.intersection(poly)
        if seg.is_empty:
            return []
        parts = [q for q in (seg.geoms if hasattr(seg, "geoms") else [seg])
                 if not q.is_empty and getattr(q, "length", 0) > 0]
        ms = [g.project(Point(c)) for q in parts for c in (q.coords[0], q.coords[-1])]
        if not ms:
            return []
        lo, hi = min(ms), max(ms)
        return ([lo] if lo > 0 else []) + ([hi] if hi < g.length else [])
    first_in = inside.index(True)
    last_in = len(inside) - 1 - inside[::-1].index(True)
    out: list[float] = []
    if first_in > 0:                                   # mouth outside -> cut where it enters
        out.append(_cross_measure(g, coords[first_in - 1], coords[first_in], boundary))
    if last_in < len(coords) - 1:                      # source outside -> cut where it exits
        out.append(_cross_measure(g, coords[last_in], coords[last_in + 1], boundary))
    return out


def _apply_offset(d: float, length: float, anchor) -> tuple[float, str]:
    """Shift an along-channel distance ``d`` (metres from the mouth) by the anchor's authored
    offset, following the channel. +upstream / -downstream (geometry is mouth->source, so a larger
    along-distance is farther upstream). Clamped to [0, length]; a clamp records a concern."""
    if not anchor.offset_m:
        return d, ""
    m = d + anchor.offset_m if anchor.offset_dir == "upstream" else d - anchor.offset_m
    if m < 0.0:
        return 0.0, f"offset {anchor.offset_m:.0f}m {anchor.offset_dir} clamped at the mouth"
    if m > length:
        return length, f"offset {anchor.offset_m:.0f}m {anchor.offset_dir} clamped at the source"
    return m, ""


def _tributary_chain(anchor, by_blk, by_wsc) -> Optional[BlkChain]:
    if anchor.tributary_blk:
        return by_blk.get(anchor.tributary_blk)
    if anchor.tributary_wsc:
        cands = by_wsc.get(_trim_wsc(anchor.tributary_wsc), [])
        # the main channel of that WSC (side channels share it) = the largest.
        return max(cands, key=lambda c: (c.stream_magnitude or 0, c.length_m)) if cands else None
    return None


def resolve_split_defs(split_defs: list[SplitDef], chains: list[BlkChain],
                       mu_polys: Optional[dict] = None,
                       area_polys: Optional[dict] = None) -> list[SplitPoint]:
    by_blk = {c.blk: c for c in chains}
    by_wsc: dict[str, list[BlkChain]] = defaultdict(list)   # keyed by TRIMMED wsc (authored form)
    for c in chains:
        by_wsc[_trim_wsc(c.fwa_watershed_code)].append(c)
    out: list[SplitPoint] = []

    def _emit(sd, blk, m, concern="", offset=0.0):
        out.append(SplitPoint(split_id=sd.id, blk=blk, route_measure=float(m), fid="",
                              label=(sd.label or sd.id), anchor_type=sd.anchor.type,
                              offset_m=float(offset), proximity_m=sd.proximity_m,
                              concern=(concern or sd.concern),
                              anchor_offset_m=float(sd.anchor.offset_m or 0.0),
                              anchor_offset_dir=str(sd.anchor.offset_dir or ""),
                              source=("gauge" if sd.anchor.type == AnchorType.gauge
                                      else "curated")))

    for sd in split_defs:
        a = sd.anchor
        targets = [(blk, by_blk[blk]) for blk in _target_blks(sd, chains) if blk in by_blk]

        # A GAUGE ANCHOR IS A POINT ANCHOR WITH A PROVENANCE. Identical geometry — a
        # published coordinate, projected onto the scoped channel — and a distinct `anchor_type` so
        # a cut a curator wrote and a cut a station generated are told apart in
        # splits.resolved.json and in the gpkg.
        if a.type in (AnchorType.point, AnchorType.gauge) and a.coord is not None:
            # The authored coordinate names ONE channel — the one it sits on — and that channel is
            # cut by projection (exact). The side channels a cut must also cut are NOT decided here:
            # the braid sweep (`braid_sweep`, T67) does it on the graph, for every split kind, and
            # only for a channel that attaches to the host on both sides of the cut. The old
            # perpendicular sweep here (gauges and points only, scoped by gnis/wsc, no topology) is
            # gone: 4 of its gauge braid cuts were artefacts, one 3.1 km from 08KG001.
            p = _pt(a.coord, a.is_lonlat)
            host = None
            for blk, c in targets:
                g = c.geometry
                if g is None or g.is_empty:
                    continue
                d = g.project(p)
                dist = g.interpolate(d).distance(p)
                if dist <= sd.proximity_m and (host is None or dist < host[2]):
                    host = (blk, c, dist, d)
            if host is None:
                continue
            blk, c, _, d = host
            m, oc = _apply_offset(d, c.geometry.length, a)
            _emit(sd, blk, c.mouth_measure + m, concern=oc)

        elif a.type == AnchorType.line and a.coords:
            for blk, c in targets:
                g = c.geometry
                if g is None or g.is_empty:
                    continue
                for p in _points(g.intersection(_line(a.coords, a.is_lonlat))):
                    _emit(sd, blk, c.mouth_measure + g.project(p))

        elif a.type == AnchorType.confluence:
            trib = _tributary_chain(a, by_blk, by_wsc)
            if trib is None or trib.geometry is None:
                continue
            mouth = Point(trib.geometry.coords[0])
            # A braided parent reach (main stem + side channels sharing the gnis/WSC) would otherwise
            # yield one cut PER channel for a single confluence. Collect the candidate cuts, then emit
            # ONE — on the main channel (largest stream_magnitude, then longest) — so a confluence is a
            # single split point. (Single-channel targets are unaffected: one candidate in, one out.)
            cands: list[tuple] = []                    # (magnitude, length, blk, route_measure, off, concern)
            for blk, c in targets:
                if c.geometry is None:
                    continue
                # WSC self-validation: the tributary's WSC must be a strict descendant of the
                # parent's (a real confluence). If not, keep the split but flag it — the author
                # likely picked the wrong tributary/parent.
                pw, tw = _trim_wsc(c.fwa_watershed_code), _trim_wsc(trib.fwa_watershed_code)
                # valid iff the tributary is a strict descendant of the parent, OR tw == pw (the river's
                # OWN mouth into a larger river — "X to its confluence with Y", cut on X at measure 0).
                concern = "" if (tw == pw or _wsc.is_at_or_below(tw, pw)) else (
                    f"confluence WSC check failed: tributary {tw} is not a descendant of parent {pw}")
                d = c.geometry.project(mouth)
                off = c.geometry.interpolate(d).distance(mouth)
                if off <= sd.proximity_m:
                    m, oc = _apply_offset(d, c.geometry.length, a)
                    concern = "; ".join(x for x in (concern, oc) if x)
                    cands.append(((c.stream_magnitude or 0), c.length_m, blk,
                                  c.mouth_measure + m, off, concern))
            if cands:
                cands.sort(key=lambda t: (t[0], t[1]), reverse=True)   # main channel first
                _, _, blk, measure, off, concern = cands[0]
                _emit(sd, blk, measure, concern=concern, offset=off)

        elif a.type == AnchorType.lake:
            for blk, c in targets:
                # waterbody_runs are per-fid; consolidate to the lake's two boundaries on this
                # BLK (downstream entry, upstream exit).
                runs = [r for r in c.waterbody_runs if str(r.wbk) == a.wbk]
                if not runs:
                    continue
                enters, leaves = min(r.down_m for r in runs), max(r.up_m for r in runs)
                if not a.offset_m:
                    # No offset: both boundaries, as no-op splits (the lake already split the BLK) —
                    # emitted for matching / completeness.
                    _emit(sd, blk, enters)
                    _emit(sd, blk, leaves)
                    continue
                # OFFSET: one real cut, measured from the end of the lake the offset runs away from —
                # "100 m upstream of Mitchell Lake" starts where the river LEAVES the lake, "500 m
                # downstream" where it enters. This is what makes a cut that travels WITH the lake:
                # authored as a bare split it landed on the lake boundary itself, `_cut_at` resolved
                # both ids to that one boundary, and `between(lake, 100m_upstream)` collapsed to an
                # empty range. Offsetting from the lake keeps the two cuts distinct by construction.
                base = leaves if a.offset_dir == "upstream" else enters
                m, concern = _apply_offset(base - c.mouth_measure, c.length_m, a)
                _emit(sd, blk, c.mouth_measure + m, concern=concern, offset=a.offset_m)

        elif a.type == AnchorType.mu_boundary:
            if not mu_polys:
                continue
            pa, pb = mu_polys.get(a.mu_a), mu_polys.get(a.mu_b)
            if pa is None or pb is None:
                continue
            shared = pa.boundary.intersection(pb.boundary)
            if shared.is_empty:
                continue                              # MUs are not adjacent -> nothing to cut
            for blk, c in targets:
                if c.geometry is None:
                    continue
                # A region boundary should divide the river ONCE. Where the river runs ALONG the
                # boundary it may intersect several times; collapse to a single split: dedupe
                # crossings closer than proximity_m, and if several distinct ones remain keep the
                # median (deterministic) + flag a concern. Real data (Fraser 2-18/3-14) yields one.
                measures = sorted(c.geometry.project(p) for p in _points(c.geometry.intersection(shared)))
                if not measures:
                    continue
                deduped: list[float] = []
                for m in measures:
                    if not deduped or (m - deduped[-1]) > sd.proximity_m:
                        deduped.append(m)
                concern = ""
                if len(deduped) > 1:
                    concern = (f"MU boundary crossed the river {len(deduped)}x at measures "
                               f"{[round(c.mouth_measure + m) for m in deduped]}; kept the median")
                    chosen = deduped[len(deduped) // 2]
                else:
                    chosen = deduped[0]
                _emit(sd, blk, c.mouth_measure + chosen, concern=concern)

        elif a.type == AnchorType.area_boundary:
            # Cut the target water + (optionally) its WSC descendants where they cross the admin/park
            # polygon — but only at the FIRST entry and LAST exit (transition cutting), so a stream
            # weaving along the boundary yields <=2 cuts, not hundreds. The between-reach is flagged
            # inside later by border.mark_inside_area.
            poly = (area_polys or {}).get(a.area_name)
            if poly is None:
                continue
            boundary = poly.boundary
            # A CUT THAT WOULD LEAVE A SLIVER IS NOT MADE (BOUND round, 2026-10-06), as for the
            # blanket areas (`area_splits.resolve_area_splits`): one within `sliver_gate.SLIVER_M`
            # of the line's own mouth or source, a lake edge, or another cut of this split — the
            # Tweedsmuir split cut the Talchako 0.26 m and 3.7 m from its ends, Ape Creek 1.35 m.
            from pipeline.atlas.splits.sliver_gate import SLIVER_M
            scope = _target_blks(sd, chains, descendants=a.wsc_descendants)
            for blk in scope:
                c = by_blk.get(blk)
                if c is None or c.geometry is None or c.geometry.is_empty:
                    continue
                ms = _area_transition_measures(c.geometry, poly, boundary)
                ends = [0.0, c.geometry.length]
                ends += [e - c.mouth_measure for r in (c.waterbody_runs or ())
                         for e in (r.down_m, r.up_m)]
                for m in ms:
                    others = [x for x in ms if x != m]
                    if any(abs(e - m) < SLIVER_M for e in ends + others):
                        continue
                    _emit(sd, blk, c.mouth_measure + m)

    return out


def confluence_mouths(split_defs: list[SplitDef], chains: list[BlkChain]) -> dict[str, tuple[float, float]]:
    """{split id: (x, y)} — the tributary's mouth for every CONFLUENCE anchor with no offset, read by the
    braid sweep (`braid_sweep.confluence_frame`): a confluence's cut runs from the mouth straight across."""
    by_blk = {c.blk: c for c in chains}
    by_wsc: dict[str, list[BlkChain]] = defaultdict(list)
    for c in chains:
        by_wsc[_trim_wsc(c.fwa_watershed_code)].append(c)
    out: dict[str, tuple[float, float]] = {}
    for sd in split_defs:
        a = sd.anchor
        if a.type != AnchorType.confluence or a.offset_m:
            continue
        trib = _tributary_chain(a, by_blk, by_wsc)
        if trib is None or trib.geometry is None or trib.geometry.is_empty:
            continue
        x, y = trib.geometry.coords[0][:2]
        out[sd.id] = (float(x), float(y))
    return out
