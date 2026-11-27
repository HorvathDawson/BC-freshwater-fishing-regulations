"""CLEAN CUT — the cut mode for an area whose edge is where fishing stops (BOUND round, 2026-10-06).

An area in `areas.json` says how a blue line crossing it is cut (`"cut"`):

  * ``"first_last"`` (regions, MU groups, sign zones, restricted land) — cut where the line FIRST
    enters and LAST leaves (`anchors._area_transition_measures`); everything between is one stretch,
    and ``"membership": "both_sides"`` says a piece that still straddles the line is a member of
    every area it really reaches into (`border.mark_inside_areas`, the overlap test). For a region
    that is the ruling: a river wandering a region line can carry both regions' rule sets.

  * ``"clean"`` (national parks, ecological reserves, the Chilkoot Trail — closures) — this module,
    `decide_area` (T1199, owner 2026-11-26: option D, rule C2). Cuts sit at the REAL crossings of the
    polygon's edge. EDGE WATER IS INSIDE: an outside run between two inside runs, or a tail to the
    line's end or a lake edge, stays inside only while it never strays farther than `EDGE_M` from
    the polygon (a boundary creek, the river along a reserve's island bank: closed like a boundary
    river); one that strays farther is an EXCURSION and is cut open at its crossings, however short.
    NO PIECE UNDER `MIN_PIECE_M`: a short outside piece joins the inside, a short inside piece grows
    outward, and a cut that close to another boundary moves outward onto it (or out to the floor
    from it). The inside never shrinks. A line that never crosses the edge is inside only when it
    lies inside; one beside the edge is never inside. A joined inside stretch shorter than `DIP_M`
    is an isolated dip within positional error and is ignored, as is an outside tail that short.
    A stretch ends at the line's mouth or source, a lake edge, or a border cut (`stretches`).

  * THE B.C. BORDER is cut by the older zone rule (`decide`, `border.BORDER_CROSSING_ZONE_M`):
    crossings closer than the zone are one place; a side change is one cut at the zone's
    representative (middle) crossing, a graze none, and a zone at a stretch end takes no side there.

    THE CUTTER ASSIGNS MEMBERSHIP. The same pass that decides the cuts decides which measure
    intervals of each blue line are inside (`CleanCut.inside`), and the membership step stamps the
    stream pieces lying in those intervals (`border.mark_inside_areas(cutter=)`), which does not
    test a stream piece against a clean area at all. Lakes and wetlands are never cut and keep the
    outline test.

Pure geometry over the FWA chains: no graph, no tolerance-based repair. The edge band and the floor are
the two numbers, declared once in `pipeline/common/tolerances.py` beside the measurement that chose
them; the area-cut gate (`area_cut_gate`) asserts both on the built pieces.
"""

from __future__ import annotations

from dataclasses import dataclass, field

from shapely.geometry import Point
from shapely.ops import substring

from pipeline.common import tolerances as _tol
from pipeline.common.models import AnchorType, BlkChain, SplitPoint

#: Two computations of ONE intersection point (a chain against two polygons that share the edge
#: segment) agree to ~1e-9 m; this is that identity, not a positional tolerance.
SAME_PLACE_M = _tol.SAME_PLACE_M


@dataclass
class CleanCut:
    """What the clean cutter decided for one area def: the cut points, and per area name the inside
    measure intervals per blk (absolute route measures, ascending, non-overlapping)."""
    points: list[SplitPoint] = field(default_factory=list)
    inside: dict[str, dict[str, list[tuple[float, float]]]] = field(default_factory=dict)
    grazes: list[tuple[str, str, float, float]] = field(default_factory=list)   # (area, blk, lo, hi)
    stats: dict[str, int] = field(default_factory=dict)       # what each part of the rule did (`decide_area`)


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
DIP_M = _tol.SLIVER_M     # 5 m (pipeline/common/tolerances.py)


#: THE EDGE BAND (`tolerances.AREA_EDGE_M`, 50 m): outside water that never strays farther than this from a closure
#: area's polygon is edge water and is closed with it; farther is an excursion, cut open at the real crossings.
EDGE_M = _tol.AREA_EDGE_M
#: THE FLOOR (`tolerances.AREA_MIN_PIECE_M`, 100 m): no area cut leaves a piece shorter than this.
MIN_PIECE_M = _tol.AREA_MIN_PIECE_M
#: Another boundary this close to a stretch's own end IS that end (a lake edge, the line's mouth): the floor's tail
#: rule already keeps every area cut at least `MIN_PIECE_M` from it.
_AT_END_M = _tol.AT_CUT_M
#: The edge band is a buffer of the polygon; its arcs are chords. 16 segments a quarter circle put a chord at most
#: 50 m x (1 - cos(pi/64)) = 6 cm inside the true band.
EDGE_QUAD_SEGS = 16

#: THE RULE'S THREE PARTS (owner 2026-11-26), each a switch a mutation test flips (`test_clean_cut.py`).
#: Edge water stays closed like a boundary river: an outside run or tail within `EDGE_M` joins the inside.
EDGE_WATER_IS_INSIDE = True
#: No piece under `MIN_PIECE_M`: a short outside piece joins the inside, a short inside piece grows outward.
NO_PIECE_UNDER_THE_FLOOR = True
#: An area cut within `MIN_PIECE_M` of another boundary moves outward onto it, or out to the floor from it.
A_CUT_NEAR_A_BOUNDARY_MOVES_ONTO_IT = True
#: A run inside shorter than `DIP_M` is positional error, dropped BEFORE the band joins anything: a line beside the
#: edge that grazes it by 2 m is still a line that never crosses (owner 2026-11-26 "Never inside"; review of T1199:
#: joined first, one graze closed 2 km of a creek hugging the edge 20 m out).
GRAZES_ARE_NOT_CROSSINGS = True


def edge_band(poly, edge_m: float = EDGE_M):
    """The polygon widened by the edge band, prepared: a line lies within `edge_m` of the polygon exactly when this
    covers it."""
    import shapely
    band = poly.buffer(edge_m, quad_segs=EDGE_QUAD_SEGS)
    shapely.prepare(band)
    return band


def _merge(iv: list) -> list[tuple[float, float]]:
    out: list[list[float]] = []
    for a, b in sorted(iv):
        if b - a <= SAME_PLACE_M:
            continue
        if out and a <= out[-1][1] + SAME_PLACE_M:
            out[-1][1] = max(out[-1][1], b)
        else:
            out.append([a, b])
    return [(a, b) for a, b in out]


#: A pass of the floor or the move onto a boundary only ever widens the inside, so it settles; this many passes
#: without settling is a defect in the rule, and it raises.
_MAX_PASSES = 100


def floor_pieces(iv: list, L: float, min_piece_m: float = MIN_PIECE_M, stats: dict | None = None
                 ) -> list[tuple[float, float]]:
    """NO PIECE UNDER THE FLOOR, on one stretch (0 .. L): an outside gap shorter than `min_piece_m` between two inside
    pieces joins them; an outside tail that short at a stretch end joins the inside; an inside piece that short grows
    outward, evenly both ways, the rest the other way at a stretch end. The inside never shrinks. A stretch shorter
    than the floor that holds any inside is inside whole."""
    iv = _merge(iv)
    if not iv:
        return iv
    for _ in range(_MAX_PASSES):
        out: list[list[float]] = []
        for a, b in iv:
            if out and a - out[-1][1] < min_piece_m:
                out[-1][1] = b
                _count(stats, "outside_gaps_joined")
            else:
                out.append([a, b])
        if 0.0 < out[0][0] < min_piece_m:
            out[0][0] = 0.0
            _count(stats, "tails_joined")
        if out[-1][1] < L and L - out[-1][1] < min_piece_m:
            out[-1][1] = L
            _count(stats, "tails_joined")
        grown = []
        for a, b in out:
            if b - a < min_piece_m:
                need = min_piece_m - (b - a)
                a, b = a - need / 2.0, b + need / 2.0
                if a < 0.0:
                    b, a = b - a, 0.0
                if b > L:
                    a, b = a - (b - L), L
                a = max(0.0, a)
                _count(stats, "pieces_grown")
            grown.append((a, b))
        new = _merge(grown)
        if new == iv:
            return new
        iv = new
    raise RuntimeError(f"clean_cut.floor_pieces: no settle after {_MAX_PASSES} passes on a {L:.1f} m stretch")


def onto_others(iv: list, L: float, others, min_piece_m: float = MIN_PIECE_M, stats: dict | None = None
                ) -> list[tuple[float, float]]:
    """NO PIECE UNDER THE FLOOR AGAINST ANOTHER BOUNDARY (a curated or gauge cut, the coast cut, a region or MU-group
    line, restricted land, another area's edge; `others`, relative measures on this stretch): an area cut closer than
    `min_piece_m` to one moves OUTWARD only — onto it when it lies outside the inside (the piece between closes), or
    out to the floor from it when it lies inside (the inside piece between reaches the floor). The nearest wins; a tie
    takes the one nearer the mouth. Then the floor again, until nothing moves."""
    iv = _merge(iv)
    others = sorted(others)
    if not others or not iv:
        return iv

    def nearest(x):
        near = [o for o in others if SAME_PLACE_M < abs(o - x) < min_piece_m]
        return min(near, key=lambda o: (abs(o - x), o)) if near else None
    for _ in range(_MAX_PASSES):
        out = []
        for a, b in iv:
            if a > 0.0:
                o = nearest(a)
                if o is not None:
                    _count(stats, "cuts_moved_onto_a_boundary" if o < a else "cuts_moved_to_the_floor")
                    a = o if o < a else max(0.0, o - min_piece_m)
            if b < L:
                o = nearest(b)
                if o is not None:
                    _count(stats, "cuts_moved_onto_a_boundary" if o > b else "cuts_moved_to_the_floor")
                    b = o if o > b else min(L, o + min_piece_m)
            out.append((a, b))
        new = floor_pieces(out, L, min_piece_m) if NO_PIECE_UNDER_THE_FLOOR else _merge(out)
        if new == iv:
            return new
        iv = new
    raise RuntimeError(f"clean_cut.onto_others: no settle after {_MAX_PASSES} passes on a {L:.1f} m stretch")


def _count(stats: dict | None, key: str) -> None:
    if stats is not None:
        stats[key] = stats.get(key, 0) + 1


def decide_area(g, poly, boundary, band=None, others=(), edge_m: float = EDGE_M,
                min_piece_m: float = MIN_PIECE_M, dip_m: float = DIP_M, stats: dict | None = None,
                forced_in=()) -> tuple[list[float], list[tuple[float, float]], list[tuple[float, float]]]:
    """THE CLEAN-CUT RULE FOR A CLOSURE AREA (T1199, owner 2026-11-26: option D, rule C2): (cuts, inside intervals,
    ignored dips), relative to the line's mouth (0 .. g.length). `band` is `edge_band(poly, edge_m)` (made here when
    not given); `others` the other boundaries on this stretch, relative.

      * CUTS AT REAL CROSSINGS of the polygon's edge.
      * EDGE WATER IS INSIDE: an outside run between two inside runs stays inside while it never strays farther than
        `edge_m` from the polygon, however long it is (Carmanah Creek hugging Pacific Rim at 21 m for 1.3 km; the
        Skeena 30-46 m off its reserve's island bank); one that strays farther is an excursion, cut open at its
        crossings, however short (the Skeena's loop 186 m out, the Beaverfoot's 968 m off Yoho). An outside tail to
        the stretch's end (the line's mouth or source, a lake edge, a border cut) is read the same way; one shorter
        than `dip_m` is ignored.
      * A run inside shorter than `dip_m` is a DIP within the data's positional error, dropped before anything is
        joined: no entry, no cut, and nothing the band can join to (the Ospika's mouth 1.9 m inside Ospika Cones; a
        creek beside the edge that grazes it by 2 m stays outside its whole length).
      * NO PIECE UNDER `min_piece_m` (`floor_pieces`), and none against another boundary (`onto_others`). The inside
        never shrinks.
      * A line that never crosses the edge is inside exactly when it lies inside: a river beside the edge is never
        inside (owner 2026-11-26, "Never inside").
      * `forced_in` (relative intervals): stretches the OWNER called edge water of this area
        (`area_edge_calls.json` `closed`), inside whatever the crossings say; the floor and the move apply to them."""
    L = g.length
    forced = [(max(0.0, a), min(L, b)) for a, b in forced_in if min(L, b) > max(0.0, a)]
    ms = crossings(g, boundary)
    if not ms and not forced:
        mid = g.interpolate(0.5, normalized=True)
        return [], ([(0.0, L)] if poly.contains(mid) else []), []
    if not ms:
        # a called stretch on a line that never crosses: the floor and the move apply to it as below (one path)
        inside, dips = ([(0.0, L)] if poly.contains(g.interpolate(0.5, normalized=True)) else []), []
    else:
        inside, dips = _by_crossings(g, poly, boundary, band, edge_m, dip_m, stats, ms)
    if forced:
        inside = _merge(inside + forced)
        _count(stats, "stretches_called_edge_water")
    if NO_PIECE_UNDER_THE_FLOOR:
        inside = floor_pieces(inside, L, min_piece_m, stats)
    if A_CUT_NEAR_A_BOUNDARY_MOVES_ONTO_IT:
        near = [o for o in others if _AT_END_M < o < L - _AT_END_M]
        inside = onto_others(inside, L, near, min_piece_m, stats)
    cuts = sorted([lo for lo, _ in inside if lo > 0.0] + [hi for _, hi in inside if hi < L])
    return cuts, inside, dips


def _by_crossings(g, poly, boundary, band, edge_m: float, dip_m: float, stats: dict | None, ms: list[float]
                  ) -> tuple[list[tuple[float, float]], list[tuple[float, float]]]:
    """`decide_area`'s inside from the real crossings `ms` (the band joins, the dips dropped), before the owner's
    called stretches, the floor and the move: (inside, dips), relative."""
    L = g.length
    band = edge_band(poly, edge_m) if band is None else band
    sides = _sides(g, poly, boundary, ms)
    edges = [0.0] + ms + [L]

    def hugs(lo: float, hi: float) -> bool:
        """The outside run lo..hi never strays farther than the edge band."""
        return EDGE_WATER_IS_INSIDE and band.covers(substring(g, lo, hi))
    ins: list[list[float]] = []
    dips: list[tuple[float, float]] = []
    for (lo, hi), sd in zip(zip(edges, edges[1:]), sides):
        if sd == "out":
            continue
        if GRAZES_ARE_NOT_CROSSINGS and hi - lo < dip_m:
            dips.append((lo, hi))      # positional error: no entry, and nothing for the band to join to
            continue
        if ins and hugs(ins[-1][1], lo):
            ins[-1][1] = hi
            _count(stats, "outside_runs_joined_by_the_band")
        else:
            if ins:
                _count(stats, "excursions_cut")
            ins.append([lo, hi])
    if ins and 0.0 < ins[0][0] and (ins[0][0] < dip_m or hugs(0.0, ins[0][0])):
        ins[0][0] = 0.0
        _count(stats, "tails_joined_by_the_band")
    if ins and ins[-1][1] < L and (L - ins[-1][1] < dip_m or hugs(ins[-1][1], L)):
        ins[-1][1] = L
        _count(stats, "tails_joined_by_the_band")
    dips += [(lo, hi) for lo, hi in ins if hi - lo < dip_m]
    return [(lo, hi) for lo, hi in ins if hi - lo >= dip_m], dips


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


def decide_chain(chain, poly, boundary, zone_m: float, gaps=(), band=None, existing=(),
                 stats: dict | None = None, forced=()):
    """`decide` (the border's zone rule) or, with `band` (`edge_band`), `decide_area` (a closure area's rule) over
    each stretch of `chain` (`stretches`). `existing`: the ABSOLUTE measures of the other boundaries on this blue line
    (`decide_area`'s `others`). Returns (cuts, inside, grazes) in ABSOLUTE route measures: cuts as (measure, "enter" |
    "exit", zone first crossing, zone last crossing); an inside interval reaching the line's own end is open (±inf);
    one reaching a lake run or a gap ends where `stretches` says."""
    import math

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
        if band is not None:
            rel = [e - M0 - lo for e in existing if M0 + lo < e < M0 + lo + sub.length]
            frel = [(a - M0 - lo, b - M0 - lo) for a, b in forced if b > M0 + lo and a < M0 + lo + sub.length]
            c, ins, gz = decide_area(sub, poly, boundary, band, others=rel, stats=stats, forced_in=frel)
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
    clipped Rolla Canyon reserve meets a creek 1.9 mm from the border cut. For a closure area the
    rule itself has already moved every cut onto a boundary within the floor (`onto_others`); this
    only makes the measure exactly that boundary's, past the float noise of the relative round trip."""
    near = min(existing or (), key=lambda b: abs(b - M), default=None)
    return near if near is not None and abs(near - M) < zone_m else M


def resolve_clean_cuts(polys_by_name: dict, chains: list[BlkChain], label_of=None,
                       scope: dict | None = None, existing: dict | None = None,
                       bc_inside: dict | None = None, edge_m: float = EDGE_M, forced: dict | None = None) -> CleanCut:
    """Clean-cut every chain meeting each polygon (`decide_area`). `label_of(name, chain, measure)` labels a cut
    (default: the area's own name). `scope` narrows an area to one water, as in
    `area_splits.resolve_area_splits`.

    `existing` ({blk: [boundary measures]} already on the graph — the border, curated and gauge cuts, the coast cut,
    the first/last areas, earlier closure areas): no cut of this area lands closer than the floor to one
    (`onto_others`); one moved onto it IS that boundary (`onto_existing`), and its inside ends there. The cuts of
    each polygon, in name order, are boundaries for the next (two reserves 60 m apart on one creek). `bc_inside`
    ({blk: [inside-B.C. intervals]} from the border pass): an area is clipped to B.C., so it is cut only along what
    lies inside B.C. — Liumchen Creek's route measure runs 1.6 km into Washington past the end of its geometry, and
    an inside interval left open there gave that stretch the reserve's membership.

    `forced` ({name: {blk: [(from_m, to_m)]}}, absolute measures): the stretches the owner called edge water of that
    area (`area_edge_calls.closed_by_area`), inside whatever the crossings say.

    Interval ends at a chain's mouth or source are open (±inf): a chain's geometric length and its
    route-measure span differ by millimetres, and the inside reaches the water's end either way."""
    import math

    import shapely
    from shapely.strtree import STRtree

    keep = [c for c in chains if c.geometry is not None and not c.geometry.is_empty]
    res = CleanCut()
    if not keep:
        return res
    ex_all: dict[str, list[float]] = {k: sorted(v) for k, v in (existing or {}).items()}
    tree = STRtree([c.geometry for c in keep])
    for name in sorted(polys_by_name):
        poly = polys_by_name[name]
        if poly is None or poly.is_empty:
            continue
        boundary = poly.boundary
        shapely.prepare(poly)
        shapely.prepare(boundary)
        band = edge_band(poly, edge_m)
        want = (scope or {}).get(name)
        got: dict[str, list[tuple[float, float]]] = {}
        made: dict[str, list[float]] = {}
        for i in sorted(tree.query(poly, predicate="intersects")):
            c = keep[i]
            if want and f"gnis:{c.gnis_id}" != want and f"wbk:{getattr(c, 'wbk', '')}" != want:
                continue
            gaps = outside_gaps(bc_inside[c.blk]) if bc_inside and c.blk in bc_inside else ()
            ex = ex_all.get(c.blk, ())
            cuts, inside, grazes = decide_chain(c, poly, boundary, 0.0, gaps, band=band, existing=ex,
                                                stats=res.stats, forced=((forced or {}).get(name) or {}).get(c.blk, ()))
            cut_at = {M for M, *_ in cuts}

            def at(M: float) -> float:
                """A cut's measure — or the existing boundary it was moved onto. Lake-run middles and open ends
                pass through."""
                return onto_existing(M, ex, DIP_M) if M in cut_at else M
            for M, *_ in cuts:
                res.points.append(SplitPoint(
                    split_id=f"area:{name}", blk=c.blk, route_measure=at(M), fid="",
                    label=(label_of(name, c, M - c.mouth_measure) if label_of else name),
                    anchor_type=AnchorType.area_boundary, source="area"))
                made.setdefault(c.blk, []).append(at(M))
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
        for blk, ms in made.items():
            ex_all[blk] = sorted(set(ex_all.get(blk, ())) | set(ms))
        res.inside[name] = got
    return res
