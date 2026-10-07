"""CLEAN CUT — the cut mode for an area whose edge is where fishing stops (BOUND round, 2026-10-06).

An area in `areas.json` says how a blue line crossing it is cut (`"cut"`):

  * ``"first_last"`` (regions, MU groups, sign zones, restricted land) — cut where the line FIRST
    enters and LAST leaves (`anchors._area_transition_measures`); everything between is one stretch,
    and ``"membership": "both_sides"`` says a piece that still straddles the line is a member of
    every area it really reaches into (`border.mark_inside_areas`, the overlap test). For a region
    that is the ruling: a river wandering a region line can carry both regions' rule sets.

  * ``"clean"`` (national parks, ecological reserves, the Chilkoot Trail — closures) — this module,
    `decide_rejoin`. A STRADDLE IS INSIDE: the line's inside stretches are joined across every
    continuous outside run of at most the area's ``rejoin_m``; an EXIT happens only where it stays
    outside longer than that, and re-entry is a new entry. Cuts sit at the straddle's outer
    crossings (first entry, last exit). A joined inside stretch shorter than `DIP_M` is an isolated
    dip within positional error and is ignored, as is an outside run that short at a stretch end.
    A stretch ends at the line's mouth or source, a lake edge, or a border cut (`stretches`).

  * THE B.C. BORDER is cut by the older zone rule (`decide`, `border.BORDER_CROSSING_ZONE_M`):
    crossings closer than the zone are one place; a side change is one cut at the zone's
    representative (middle) crossing, a graze none, and a zone at a stretch end takes no side there.

    THE CUTTER ASSIGNS MEMBERSHIP. The same pass that decides the cuts decides which measure
    intervals of each blue line are inside (`CleanCut.inside`), and the membership step stamps the
    stream pieces lying in those intervals (`border.mark_inside_areas(cutter=)`), which does not
    test a stream piece against a clean area at all. Lakes and wetlands are never cut and keep the
    outline test.

Pure geometry over the FWA chains: no graph, no tolerance-based repair. The rejoin distance is the
one number, stated per area in `areas.json` next to the measurement that chose it.
"""

from __future__ import annotations

from dataclasses import dataclass, field

from shapely.geometry import Point

from pipeline.common.models import AnchorType, BlkChain, SplitPoint

#: Two computations of ONE intersection point (a chain against two polygons that share the edge
#: segment) agree to ~1e-9 m; this is that identity, not a positional tolerance.
SAME_PLACE_M = 1e-6


@dataclass
class CleanCut:
    """What the clean cutter decided for one area def: the cut points, and per area name the inside
    measure intervals per blk (absolute route measures, ascending, non-overlapping)."""
    points: list[SplitPoint] = field(default_factory=list)
    inside: dict[str, dict[str, list[tuple[float, float]]]] = field(default_factory=dict)
    grazes: list[tuple[str, str, float, float]] = field(default_factory=list)   # (area, blk, lo, hi)


def crossings(g, boundary) -> list[float]:
    """Measures along `g` (0 = mouth) where it meets `boundary`, ascending, de-duplicated.

    An overlap (the line running ON the edge) contributes both its ends; `zones` keeps the two in
    one zone however long the overlap is, because no side is defined along it."""
    inter = g.intersection(boundary)
    if inter.is_empty:
        return []
    parts = list(inter.geoms) if hasattr(inter, "geoms") else [inter]
    ms: list[float] = []
    for q in parts:
        if q.is_empty:
            continue
        if q.geom_type == "Point":
            ms.append(g.project(q))
        elif q.geom_type == "LineString":
            ms += [g.project(Point(q.coords[0])), g.project(Point(q.coords[-1]))]
    ms.sort()
    out: list[float] = []
    for m in ms:
        if not out or m - out[-1] > SAME_PLACE_M:
            out.append(m)
    return out


def _sides(g, poly, boundary, ms: list[float]) -> list[str]:
    """Side of each stretch between consecutive crossings (len(ms)+1 of them): in / out / on."""
    cuts = [0.0] + ms + [g.length]
    out = []
    for lo, hi in zip(cuts, cuts[1:]):
        mid = g.interpolate((lo + hi) / 2.0)
        if boundary.distance(mid) <= SAME_PLACE_M:
            out.append("on")
        else:
            out.append("in" if poly.contains(mid) else "out")
    return out


def representative(zm: list[float]) -> float:
    """THE CUT OF A STRADDLE: its most representative crossing, the one at its MIDDLE — the crossing
    nearest the midpoint of the straddle's measure span (user ruling 2026-10-06: "median" and
    "middle" are one concept). It sits halfway along the straddle and still exactly on the edge;
    crossings bunched at one end do not drag it there, as a count-median would. A tie takes the
    crossing nearer the mouth."""
    mid = (zm[0] + zm[-1]) / 2.0
    return min(zm, key=lambda x: (abs(x - mid), x))


def zones(ms: list[float], sides: list[str], zone_m: float) -> list[tuple[int, int]]:
    """Group crossing indexes into zones: consecutive crossings under `zone_m` apart, or joined by
    an `on` stretch (the line running along the edge), are one zone. Returns (first, last) index
    pairs, inclusive, in measure order."""
    out: list[tuple[int, int]] = []
    for i in range(len(ms)):
        joined = out and (ms[i] - ms[out[-1][1]] < zone_m or sides[i] == "on")
        if joined:
            out[-1] = (out[-1][0], i)
        else:
            out.append((i, i))
    return out


def decide(g, poly, boundary, zone_m: float, zones_out: dict | None = None
           ) -> tuple[list[float], list[tuple[float, float]], list[tuple[float, float]]]:
    """For one chain geometry: (cut measures, inside intervals, graze zones) — all relative to the
    chain's mouth (0 .. g.length). `zones_out` collects {cut: (first, last crossing of its zone)}."""
    L = g.length
    ms = crossings(g, boundary)
    if not ms:
        mid = g.interpolate(0.5, normalized=True)
        return [], ([(0.0, L)] if poly.contains(mid) else []), []
    sides = _sides(g, poly, boundary, ms)
    zs = zones(ms, sides, zone_m)
    # Each zone: the side before it is the stretch ending at its first crossing (index a), the side
    # after it the stretch starting at its last (index b + 1). A zone reaching a chain end has no
    # side there: the end takes the zone's other side.
    resolved: list[tuple[int, int, str, str]] = []
    for a, b in zs:
        before = None if ms[a] < zone_m else sides[a]
        after = None if L - ms[b] < zone_m else sides[b + 1]
        before = before if before != "on" else None
        after = after if after != "on" else None
        resolved.append((a, b, before, after))
    # the side of every stretch between zones, carried across the ends that have none
    known = [s for _a, _b, s0, s1 in resolved for s in (s0, s1) if s is not None]
    if not known:
        # ONE ZONE REACHING BOTH ENDS: the stretch is shorter than the edge's uncertainty on either
        # side of it, so no place on it can be the edge. It is not cut, and it lies on the side
        # holding most of its length (a 75 m creek crossing a line 39 m above its mouth).
        cuts_ = [0.0] + ms + [L]
        inside_len = sum(hi - lo for (lo, hi), sd in zip(zip(cuts_, cuts_[1:]), sides) if sd == "in")
        return [], ([(0.0, L)] if inside_len > L - inside_len else []), []
    cuts: list[float] = []
    grazes: list[tuple[float, float]] = []
    start_side = resolved[0][2] or resolved[0][3]
    side = start_side
    lo = 0.0
    inside: list[tuple[float, float]] = []
    for a, b, before, after in resolved:
        before = before or side
        after = after or before
        if before != after:
            zm = ms[a:b + 1]
            m = representative(zm)
            cuts.append(m)
            if zones_out is not None:
                zones_out[m] = (zm[0], zm[-1])
            if before == "in":
                inside.append((lo, m))
            lo, side = m, after
        else:
            grazes.append((ms[a], ms[b]))
            side = after
    if side == "in":
        inside.append((lo, L))
    return cuts, inside, grazes


#: A dip shorter than this, standing alone, is the two datasets' positional disagreement, not water
#: in or out of the area — and a piece this short could not be cut anyway (`sliver_gate.SLIVER_M`).
DIP_M = 5.0


def decide_rejoin(g, poly, boundary, rejoin_m: float, dip_m: float = DIP_M
                  ) -> tuple[list[float], list[tuple[float, float]], list[tuple[float, float]]]:
    """THE CLEAN-CUT RULE FOR A CLOSURE AREA (user ruling 2026-10-06): (cuts, inside intervals,
    ignored dips), relative to the line's mouth (0 .. g.length).

      * A STRADDLE IS INSIDE: the stretches of the line inside the area are joined across every
        continuous OUTSIDE run of at most `rejoin_m` — from the first crossing of a run of in-and-out
        to the last, the water is in the closure (the Beaverfoot and Kicking Horse along Yoho; the
        80 m out on Vladimir J. Krajina's creek).
      * AN EXIT happens only where the line stays outside continuously for more than `rejoin_m`;
        re-entry after that is a new entry.
      * The cuts sit at the straddle's OUTER crossings: its first entry and its last exit.
      * A joined inside stretch shorter than `dip_m` is an isolated DIP within the data's positional
        error: ignored — no entry, no cut (the Ospika's mouth 1.9 m inside Ospika Cones). An outside
        run at a stretch END shorter than `dip_m` is ignored the same way, so no cut lands a hair
        from the end."""
    L = g.length
    ms = crossings(g, boundary)
    if not ms:
        mid = g.interpolate(0.5, normalized=True)
        return [], ([(0.0, L)] if poly.contains(mid) else []), []
    sides = _sides(g, poly, boundary, ms)
    edges = [0.0] + ms + [L]
    ins: list[list[float]] = []
    for (lo, hi), sd in zip(zip(edges, edges[1:]), sides):
        if sd == "out":
            continue
        if ins and lo - ins[-1][1] <= rejoin_m:
            ins[-1][1] = hi
        else:
            ins.append([lo, hi])
    if ins and 0.0 < ins[0][0] < dip_m:
        ins[0][0] = 0.0
    if ins and L - dip_m < ins[-1][1] < L:
        ins[-1][1] = L
    dips = [(lo, hi) for lo, hi in ins if hi - lo < dip_m]
    inside = [(lo, hi) for lo, hi in ins if hi - lo >= dip_m]
    cuts = sorted([lo for lo, _ in inside if lo > 0.0] + [hi for _, hi in inside if hi < L])
    return cuts, inside, dips


def stretches(chain, gaps=()) -> list[tuple[float, float, float | None, float | None]]:
    """The chain's stretches to cut, relative to its mouth (0 .. geometry length): the line minus
    its waterbody runs and minus `gaps` (ABSOLUTE measure intervals not to cut — the stretches the
    border pass put outside B.C.). Each is (lo, hi, lo_end, hi_end): `*_end` is None at the line's
    own mouth or source, else the ABSOLUTE measure an inside interval ends at on that side — the
    middle of the lake run (the graph rounds a lake edge to 1e-5 m, so an interval ending exactly
    there would split hairs with it: Comox Lake Bluffs, 690.7232542 vs 690.72325), or the border
    cut itself (which IS the pieces' boundary). A lake edge or a border cut already bounds a stretch,
    so it is an END for the crossing-zone rule exactly like the line's own mouth or source: a
    reservoir's watershed boundary drawn 1.4 m below the dam (Coquitlam, Capilano, Seymour) is not
    a cut 1.4 m from the lake edge."""
    import math

    L = chain.geometry.length
    M0 = chain.mouth_measure
    holes = [(max(0.0, r.down_m - M0), min(L, r.up_m - M0),
              (r.down_m + r.up_m) / 2.0, (r.down_m + r.up_m) / 2.0)        # the run's middle
             for r in (chain.waterbody_runs or ())]
    for glo, ghi in gaps:
        a = -math.inf if glo == -math.inf else glo - M0
        b = math.inf if ghi == math.inf else ghi - M0
        holes.append((max(0.0, a), min(L, b), glo, ghi))
    holes.sort()
    out, lo, lo_end = [], 0.0, None
    for a, b, a_end, b_end in holes:
        if a > lo:
            out.append((lo, a, lo_end, a_end))
        if b >= lo:
            lo, lo_end = b, b_end
    if lo < L:
        out.append((lo, L, lo_end, None))
    return out


def decide_chain(chain, poly, boundary, zone_m: float, gaps=(), rejoin_m: float | None = None):
    """`decide` (the border's zone rule) or, with `rejoin_m`, `decide_rejoin` (a closure area's
    rule) over each stretch of `chain` (`stretches`). Returns (cuts, inside, grazes) in
    ABSOLUTE route measures: cuts as (measure, "enter" | "exit", zone first crossing, zone last
    crossing); an inside interval reaching the
    line's own end is open (±inf); one reaching a lake run or a gap ends where `stretches` says."""
    import math
    from shapely.ops import substring

    g = chain.geometry
    M0 = chain.mouth_measure
    cuts, inside, grazes = [], [], []
    for lo, hi, lo_end, hi_end in stretches(chain, gaps):
        if hi - lo <= SAME_PLACE_M:
            continue
        sub = g if (lo == 0.0 and hi >= g.length) else substring(g, lo, hi)
        if sub.is_empty or sub.geom_type != "LineString":
            continue
        zx: dict = {}
        if rejoin_m is not None:
            c, ins, gz = decide_rejoin(sub, poly, boundary, rejoin_m)
        else:
            c, ins, gz = decide(sub, poly, boundary, zone_m, zx)
        lo_abs = lo_end if lo_end is not None else -math.inf
        hi_abs = hi_end if hi_end is not None else math.inf
        for a, b in ins:
            A = lo_abs if a <= 0.0 else M0 + lo + a
            B = hi_abs if b >= sub.length else M0 + lo + b
            inside.append((A, B))
            if a > 0.0:
                z0, z1 = zx.get(a, (a, a))
                cuts.append((M0 + lo + a, "enter", M0 + lo + z0, M0 + lo + z1))
            if b < sub.length:
                z0, z1 = zx.get(b, (b, b))
                cuts.append((M0 + lo + b, "exit", M0 + lo + z0, M0 + lo + z1))
        grazes += [(M0 + lo + a, M0 + lo + b) for a, b in gz]
    cuts.sort()
    return cuts, inside, grazes


def outside_gaps(inside: list) -> list[tuple[float, float]]:
    """The complement of a line's inside intervals over (-inf, inf): what the border pass put
    outside B.C., as gaps for `stretches`."""
    import math
    out, at = [], -math.inf
    for lo, hi in sorted(inside):
        if lo > at:
            out.append((at, lo))
        at = max(at, hi)
    if at < math.inf:
        out.append((at, math.inf))
    return out


def onto_existing(M: float, existing, zone_m: float) -> float:
    """ONE PLACE PER CROSSING ZONE, whoever cut there first. An area's edge is known to its stated
    `crossing_zone_m`; a boundary already on the blue line within that distance (the border, a
    curated cut, another area's edge) is the same place: the area's cut IS it (same measure, so
    `sectionizer._coincident` makes the area an alias there). The Region 2/3 line meets the Fraser
    0.42 mm from the curated Spuzzum Creek confluence cut, because the line follows the creek; the
    clipped Rolla Canyon reserve meets a creek 1.9 mm from the border cut."""
    near = min(existing or (), key=lambda b: abs(b - M), default=None)
    return near if near is not None and abs(near - M) < zone_m else M


def resolve_clean_cuts(polys_by_name: dict, chains: list[BlkChain], rejoin_m: float,
                       label_of=None, scope: dict | None = None,
                       existing: dict | None = None, bc_inside: dict | None = None) -> CleanCut:
    """Clean-cut every chain meeting each polygon. `label_of(name, chain, measure)` labels a cut
    (default: the area's own name). `scope` narrows an area to one water, as in
    `area_splits.resolve_area_splits`.

    `existing` ({blk: [boundary measures]} already on the graph — the border, curated cuts, earlier
    areas): a cut of this area within its crossing zone of one IS that boundary (`onto_existing`),
    and its inside ends there. `bc_inside` ({blk: [inside-B.C. intervals]} from the border pass): an
    area is clipped to B.C., so it is cut only along what lies inside B.C. — Liumchen Creek's route
    measure runs 1.6 km into Washington past the end of its geometry, and an inside interval left
    open there gave that stretch the reserve's membership.

    Interval ends at a chain's mouth or source are open (±inf): a chain's geometric length and its
    route-measure span differ by millimetres, and the inside reaches the water's end either way."""
    import math

    import shapely
    from shapely.strtree import STRtree

    if not rejoin_m or rejoin_m <= 0:
        raise ValueError("a clean-cut area must state rejoin_m > 0")
    keep = [c for c in chains if c.geometry is not None and not c.geometry.is_empty]
    res = CleanCut()
    if not keep:
        return res
    tree = STRtree([c.geometry for c in keep])
    for name in sorted(polys_by_name):
        poly = polys_by_name[name]
        if poly is None or poly.is_empty:
            continue
        boundary = poly.boundary
        shapely.prepare(poly)
        shapely.prepare(boundary)
        want = (scope or {}).get(name)
        got: dict[str, list[tuple[float, float]]] = {}
        for i in sorted(tree.query(poly, predicate="intersects")):
            c = keep[i]
            if want and f"gnis:{c.gnis_id}" != want and f"wbk:{getattr(c, 'wbk', '')}" != want:
                continue
            gaps = outside_gaps(bc_inside[c.blk]) if bc_inside and c.blk in bc_inside else ()
            cuts, inside, grazes = decide_chain(c, poly, boundary, 0.0, gaps, rejoin_m=rejoin_m)
            ex = (existing or {}).get(c.blk, ())
            cut_at = {M for M, *_ in cuts}

            def at(M: float) -> float:
                """A cut's measure — or the existing boundary it is within the zone of. Lake-run
                middles and open ends pass through."""
                return onto_existing(M, ex, DIP_M) if M in cut_at else M
            for M, *_ in cuts:
                res.points.append(SplitPoint(
                    split_id=f"area:{name}", blk=c.blk, route_measure=at(M), fid="",
                    label=(label_of(name, c, M - c.mouth_measure) if label_of else name),
                    anchor_type=AnchorType.area_boundary, source="area"))
            # where the inside ends at a border cut, the area's edge IS the border: offer the area
            # there too, so the border cut carries its name (`sectionizer._coincident` aliases it)
            ends = {e for g0, g1 in gaps for e in (g0, g1) if e not in (-math.inf, math.inf)}
            for M in sorted({e for lo, hi in inside for e in (lo, hi)} & ends):
                res.points.append(SplitPoint(
                    split_id=f"area:{name}", blk=c.blk, route_measure=M, fid="",
                    label=(label_of(name, c, M - c.mouth_measure) if label_of else name),
                    anchor_type=AnchorType.area_boundary, source="area"))
            if inside:
                got[c.blk] = [(at(lo), at(hi)) for lo, hi in inside]
            res.grazes += [(name, c.blk, lo, hi) for lo, hi in grazes]
        res.inside[name] = got
    return res
