"""CLEAN CUT, one place per boundary, and the assert-only sliver gate (BOUND round, 2026-10-06).

The user's rulings: ecological reserves and parks get a CLEAN cut — crossings that cluster along the
edge collapse to ONE cut at the median crossing when the side changes, none when it does not (a
graze); a stream that wanders the edge, then enters, passes through and exits gets an entry AND an
exit cut; the cutter assigns membership. The B.C. border and a region edge are one geometry and one
cut. Slivers are prevented at the source; the build gate only asserts. Synthetic geometry, no gpkg.
"""
from __future__ import annotations

import pytest
from shapely.geometry import LineString, box

from pipeline.atlas.splits import clean_cut as CC
from pipeline.atlas.splits.area_splits import cut_mode
from pipeline.common.models import (AnchorType, BlkChain, BoundaryKind, NodeKind, SplitPoint,
                                    StreamGraph, StreamNode)

RESERVE = box(1000, 0, 2000, 1000)          # its west edge is x = 1000

# ---- THE ZONE RULE (`CC.decide`) — the B.C. border's (border.BORDER_CROSSING_ZONE_M) --------------


def _wander_then_through():
    """Along y = 500 from x = 0. At the west edge it wanders across it five times within a few metres
    (x 990..1012), then runs straight through the reserve and leaves by the east edge."""
    pts = [(0, 500), (990, 500), (1004, 503), (996, 506), (1008, 509), (995, 512), (1006, 515),
           (1012, 518), (1500, 518), (2500, 518)]
    return LineString(pts)


def test_a_graze_is_neither_cut_nor_member():
    """Trout Creek's shape: the creek dips 4 m into the reserve and comes back."""
    g = LineString([(0, 500), (998, 500), (1002, 504), (998, 508), (0, 508)])
    cuts, inside, grazes = CC.decide(g, RESERVE, RESERVE.boundary, zone_m=50.0)
    assert cuts == [] and inside == [] and len(grazes) == 1


def test_wander_then_through_gives_exactly_an_entry_and_an_exit_cut():
    g = _wander_then_through()
    ms = CC.crossings(g, RESERVE.boundary)
    assert len(ms) == 6, "five along the west edge, one at the east edge"
    cuts, inside, grazes = CC.decide(g, RESERVE, RESERVE.boundary, zone_m=50.0)
    assert len(cuts) == 2, cuts
    assert cuts[0] == CC.representative(ms[:5]), "the entry is the straddle's middle crossing"
    assert cuts[1] == ms[5]
    assert inside == [(cuts[0], cuts[1])]
    assert grazes == []


def test_every_crossing_would_cut_without_the_zone():
    """MUTATION: a zone narrower than the wander cuts at all six crossings (today's slivers)."""
    cuts, _inside, _ = CC.decide(_wander_then_through(), RESERVE, RESERVE.boundary, zone_m=1.0)
    assert len(cuts) == 6


def test_an_odd_zone_cuts_at_its_middle_crossing():
    g = LineString([(0, 500), (995, 500), (1005, 503), (995, 506), (1005, 509), (1500, 509)])
    ms = CC.crossings(g, RESERVE.boundary)
    cuts, inside, _ = CC.decide(g, RESERVE, RESERVE.boundary, zone_m=50.0)
    assert len(ms) == 3 and cuts == [ms[1]]
    assert inside == [(ms[1], g.length)]


def test_a_crossing_inside_the_zone_of_a_blue_lines_end_is_no_cut():
    """The Ospika's shape: the reserve's edge 1.9 m above the mouth. The mouth has no side, so the
    whole line takes the side above the zone — here outside — and nothing is cut."""
    g = LineString([(1001.9, 500), (0, 500)])               # mouth 1.9 m inside, runs west
    cuts, inside, grazes = CC.decide(g, RESERVE, RESERVE.boundary, zone_m=50.0)
    assert cuts == [] and inside == []
    g2 = LineString([(998.1, 500), (1900, 500)])            # mouth 1.9 m outside, runs in
    cuts, inside, _ = CC.decide(g2, RESERVE, RESERVE.boundary, zone_m=50.0)
    assert cuts == [] and inside == [(0.0, g2.length)]


def test_a_line_wholly_inside_is_inside_and_uncut():
    g = LineString([(1100, 100), (1900, 900)])
    assert CC.decide(g, RESERVE, RESERVE.boundary, 50.0) == ([], [(0.0, g.length)], [])


def test_resolve_emits_area_points_and_absolute_inside_intervals():
    g = _wander_then_through()
    chain = BlkChain(blk="X", fwa_watershed_code="100", fids=(), geometry=g, mouth_measure=10_000.0,
                     length_m=g.length, name_tuples=())
    res = CC.resolve_clean_cuts({"R": RESERVE}, [chain], rejoin_m=1000.0)
    assert [p.split_id for p in res.points] == ["area:R", "area:R"]
    assert all(p.anchor_type == AnchorType.area_boundary for p in res.points)
    (lo, hi), = res.inside["R"]["X"]
    assert (lo, hi) == (res.points[0].route_measure, res.points[1].route_measure)
    with pytest.raises(ValueError):
        CC.resolve_clean_cuts({"R": RESERVE}, [chain], rejoin_m=0)


# ---- the cutter assigns membership ---------------------------------------------------------------
def _piece(nid, blk, lo, hi):
    return StreamNode(node_id=nid, kind=NodeKind.stream, blk=blk, down_m=lo, up_m=hi, length_m=hi - lo)


def test_membership_of_a_clean_area_is_the_cutters_inside_not_an_overlap_test():
    """The Ospika widening (2026-10-05): a piece reaching 1.9 m into a reserve was flagged by the
    overlap test and the reserve's closure covered all 329 m of it. For a clean-cut area the overlap
    test is not run on stream pieces; the cutter's intervals are."""
    from pipeline.atlas.splits.border import mark_inside_areas
    g = StreamGraph()
    for n in (_piece("X:0", "X", 0, 100), _piece("X:100", "X", 100, 429), _piece("X:429", "X", 429, 900)):
        g.nodes[n.node_id] = n
    geoms = {"X:0": LineString([(0, 0), (100, 0)]), "X:100": LineString([(100, 0), (429, 0)]),
             "X:429": LineString([(429, 0), (900, 0)])}
    reserve = box(-50, -5, 101.9, 5)               # X:100 reaches 1.9 m in
    strad: list = []
    mark_inside_areas(g, geoms, {"reserve": reserve}, cutter={"reserve": {"X": [(0.0, 100.0)]}},
                      straddlers=strad)
    assert g.nodes["X:0"].in_areas == ("reserve",)
    assert g.nodes["X:100"].in_areas == () and g.nodes["X:429"].in_areas == ()
    assert strad == []
    # a cut the cutter asked for that did not happen: reported, never guessed
    g.nodes["X:0"] = g.nodes["X:0"].__class__(**{**g.nodes["X:0"].__dict__, "in_areas": ()})
    mark_inside_areas(g, geoms, {"reserve": reserve}, cutter={"reserve": {"X": [(0.0, 50.0)]}},
                      straddlers=strad)
    assert [x[:2] for x in strad] == [("reserve", "X:0")] and g.nodes["X:0"].in_areas == ()
    # MUTATION: the overlap test (no cutter) widens the reserve onto X:100
    g2 = StreamGraph()
    g2.nodes.update({k: _piece(k, "X", v.down_m, v.up_m) for k, v in g.nodes.items()})
    mark_inside_areas(g2, geoms, {"reserve": reserve})
    assert "reserve" in g2.nodes["X:100"].in_areas


# ---- one place, one cut ---------------------------------------------------------------------------
def _sp(sid, m, anchor):
    return SplitPoint(sid, "X", m, "", sid, anchor, source="t")


def test_a_region_cut_at_the_border_cut_is_its_alias_and_the_border_keeps_the_name():
    from pipeline.atlas.splits.sectionizer import split_graph_at
    from pipeline.deliver.bundle.spans import end_token
    from pipeline.tests.test_sectionizer import _graph

    for order in ("border_first", "region_first"):
        g, _ = _graph()
        n0 = sum(1 for n in g.nodes.values() if n.blk == "X")
        b = _sp("border:X:0", 120.0, AnchorType.border)
        r = _sp("area:7B", 120.0 + 3e-9, AnchorType.area_boundary)     # the same point, computed twice
        for sp in ((b, r) if order == "border_first" else (r, b)):
            split_graph_at(g, {}, [sp])
        xs = sorted((n for n in g.nodes.values() if n.blk == "X"), key=lambda n: n.down_m)
        assert len(xs) == n0 + 1, (order, "one cut, not two")
        bd = xs[0].upper_bound
        assert bd.boundary_id == "split:border:X:0" and bd.kind == BoundaryKind.border, order
        assert "split:area:7B" in bd.aliases and xs[1].lower_bound == bd
        assert end_token(bd, "up", lake_item=lambda _: None) == "bc_border"


def test_a_millimetre_is_two_places():
    """The identity is float noise on ONE point, not a tolerance: 1 mm apart is cut twice."""
    from pipeline.atlas.splits.sectionizer import split_graph_at
    from pipeline.tests.test_sectionizer import _graph
    g, _ = _graph()
    n0 = sum(1 for n in g.nodes.values() if n.blk == "X")
    split_graph_at(g, {}, [_sp("border:X:0", 120.0, AnchorType.border),
                           _sp("area:7B", 121.5, AnchorType.area_boundary)])
    assert sum(1 for n in g.nodes.values() if n.blk == "X") == n0 + 2


# ---- the gate asserts ------------------------------------------------------------------------------
def test_the_gate_names_a_sliver_a_cut_made_and_spares_fwas_own(tmp_path):
    from pipeline.atlas.splits import sliver_gate as SG
    from pipeline.atlas.splits.sectionizer import split_graph_at
    from pipeline.tests.test_sectionizer import _graph
    g, _ = _graph()
    before = SG.short_pieces(g)
    SG.check(g, before, [], tmp_path)                           # clean graph passes
    split_graph_at(g, {}, [_sp("border:X:0", 120.0, AnchorType.border),
                           _sp("area:5", 121.5, AnchorType.area_boundary)])
    with pytest.raises(SystemExit, match="sliver gate"):
        SG.check(g, before, [], tmp_path)
    got = (tmp_path / "sliver_gate.json").read_text()
    assert "X:120" in got and "split:border:X:0" in got and "split:area:5" in got
    assert SG.slivers(g, before | {"X:120"}) == [], "a piece FWA drew short is not a sliver"
    with pytest.raises(SystemExit):
        SG.check(StreamGraph(), set(), [("reserve", "X:0")], tmp_path)


def test_cut_mode_is_explicit():
    assert cut_mode({"id": "a", "cut": "clean", "rejoin_m": 1000}) == "clean"
    assert cut_mode({"id": "a", "cut": "first_last", "membership": "both_sides"}) == "first_last"
    assert cut_mode({"id": "a", "cut": False}) is None
    for bad in ({"cut": True}, {}, {"cut": "clean"}, {"cut": "first_last"}, {"cut": "every"},
                {"cut": "first_last", "membership": "both_sides", "crossing_zone_m": 60},
                {"cut": "first_last", "membership": "both_sides", "rejoin_m": 1000}):
        with pytest.raises(ValueError):
            cut_mode({"id": "a", **bad})


def test_areas_json_declares_the_ruled_modes():
    """User ruling 2026-10-06: reserves and parks clean, regions keep in/out marking."""
    from pipeline.atlas.splits.area_splits import load_area_split_defs
    modes = {a["id"]: cut_mode(a) for a in load_area_split_defs()}
    assert modes["ecological_reserves"] == modes["national_parks"] == modes["chilkoot_trail"] == "clean"
    assert modes["regions"] == "first_last"
    rejoin = {a["id"]: a.get("rejoin_m") for a in load_area_split_defs()}
    assert rejoin["ecological_reserves"] == rejoin["national_parks"] == rejoin["chilkoot_trail"] == 1000
    assert rejoin["regions"] is None, "regions are cut where they cross, as before (user ruling)"


def test_a_stretch_shorter_than_its_zone_lies_on_its_majority_side():
    """A 75 m creek crossing a line 39 m above its mouth: both ends are inside the 60 m zone, so no
    place on it can be the edge. It is not cut, and it lies where most of it lies — it is never
    left in NO area (which, for a region, reads as outside B.C.)."""
    g = LineString([(960.7, 500), (1035.7, 500)])           # 39.3 m out, 35.7 m in
    assert CC.decide(g, RESERVE, RESERVE.boundary, 60.0) == ([], [], [])
    g2 = LineString([(964.3, 500), (1039.3, 500)])          # 35.7 m out, 39.3 m in
    assert CC.decide(g2, RESERVE, RESERVE.boundary, 60.0) == ([], [(0.0, g2.length)], [])


# ---- the representative crossing (user ruling 2026-10-06) -------------------------------------------
def _zig(x0, n, step=10.0, amp=6.0, y0=0.0):
    """n crossings of the west edge (x = 1000) between y0 and y0 + n*step, alternating sides."""
    return [(1000 + (amp if i % 2 else -amp), y0 + step * i) for i in range(1, n + 1)]


def test_a_long_straddle_then_through_is_one_cut_at_its_middle_crossing():
    """Out for 500 m, straddling the edge with 9 crossings 10 m apart (no run on either side longer
    than the zone), then in for good: one entry cut, at the crossing nearest the straddle's middle."""
    pts = [(500, 0), (994, 0)] + _zig(1000, 9) + [(1006, 100), (1500, 100)]
    g = LineString(pts)
    ms = CC.crossings(g, RESERVE.boundary)
    cuts, inside, _ = CC.decide(g, RESERVE, RESERVE.boundary, 50.0)
    mid = (ms[0] + ms[-1]) / 2
    assert len(cuts) == 1 and cuts[0] == min(ms, key=lambda x: abs(x - mid))
    assert inside == [(cuts[0], g.length)]


def test_a_long_straddle_that_returns_is_no_cut():
    pts = [(500, 0), (994, 0)] + _zig(1000, 8) + [(994, 90), (500, 90)]
    g = LineString(pts)
    assert CC.decide(g, RESERVE, RESERVE.boundary, 50.0)[0] == []


def test_out_long_in_long_out_long_in_is_entry_exit_entry():
    """A real leave-then-re-enter: out, in for 300 m, out for 300 m, in again. Each long run bounds
    the straddles; each straddle between runs on different sides is exactly one cut."""
    pts = [(700, 0), (1300, 0), (1300, 20), (700, 20), (700, 40), (1300, 40)]
    g = LineString(pts)
    cuts, inside, _ = CC.decide(g, RESERVE, RESERVE.boundary, 50.0)
    assert len(cuts) == 3
    assert [round(x) for x in cuts] == [300, 920, 1540]
    assert len(inside) == 2


def test_bunched_crossings_cut_at_the_distance_middle_not_the_count_median():
    """Seven crossings bunched in the first 12 m of a 60 m straddle, then two more: the count-median
    would sit in the bunch; the representative crossing is the one nearest the span's middle."""
    zm = [0.0, 2.0, 4.0, 6.0, 8.0, 10.0, 12.0, 31.0, 60.0]
    assert CC.representative(zm) == 31.0
    assert zm[(len(zm) - 1) // 2] == 8.0, "the count-median it replaces"
    assert CC.representative([0.0, 10.0, 20.0]) == 10.0
    assert CC.representative([0.0, 10.0]) == 0.0, "a tie takes the crossing nearer the mouth"


# ---- THE CLOSURE RULE (`CC.decide_rejoin`): a straddle is inside; exit only after a long run out -----
def test_a_straddle_is_inside_from_its_first_crossing():
    """Vladimir J. Krajina's creek: enters at 207 m, out 413-493 m (80 m), in again. The 80 m out
    is straddle: one inside stretch from 207 m, ONE cut there."""
    g = LineString([(793, 500), (1000 + 206, 500), (1000 + 206, 520), (900, 520), (900, 600),
                    (1500, 600)])
    ms = CC.crossings(g, RESERVE.boundary)
    cuts, inside, _ = CC.decide_rejoin(g, RESERVE, RESERVE.boundary, 1000.0)
    assert len(ms) == 3 and cuts == [ms[0]] and inside == [(ms[0], g.length)]


def test_the_cuts_sit_at_the_straddles_outer_crossings():
    """A wander along the edge (five crossings), through the reserve and out the far side: entry at
    the FIRST crossing of the wander, exit at the far edge; the wander is inside."""
    g = _wander_then_through()
    ms = CC.crossings(g, RESERVE.boundary)
    cuts, inside, _ = CC.decide_rejoin(g, RESERVE, RESERVE.boundary, 1000.0)
    assert cuts == [ms[0], ms[-1]] and inside == [(ms[0], ms[-1])]


def test_an_exit_needs_a_continuous_run_out_longer_than_the_rejoin_distance():
    """In, out for 1,200 m, in again: an exit cut and a re-entry cut. The same with 800 m out: one
    straddle, no cut between."""
    big = box(1000, 0, 5000, 3000)
    long_out = LineString([(1500, 100), (1500, 500), (300, 500), (300, 900), (1500, 900)])
    ms = CC.crossings(long_out, big.boundary)
    cuts, inside, _ = CC.decide_rejoin(long_out, big, big.boundary, 1000.0)
    assert cuts == ms and len(inside) == 2, (cuts, inside)
    short_out = LineString([(1500, 100), (1500, 500), (700, 500), (700, 900), (1500, 900)])
    cuts, inside, _ = CC.decide_rejoin(short_out, big, big.boundary, 1000.0)
    assert cuts == [] and inside == [(0.0, short_out.length)]


def test_the_exit_sits_at_the_last_crossing_before_the_long_run_out():
    """FIX A1(a), user review 2026-10-06: the exit cut is where the stream LAST LEFT — walk back from
    the long outside run to the crossing that starts it — never R metres out, never the first
    crossing of the wander before it. Carmanah Creek's shape in Pacific Rim (live atlas, blk
    354154120): in 0-820, out 1,338 m, then in/out/in/out/in of 63, 7, 220, 8, 167 m, last crossing
    at 2,623.51, then 22 km out: cuts 819.88 (exit), 2,157.81 (re-entry), 2,623.51 (exit)."""
    big = box(0, -5000, 10000, 0)                     # the area: y < 0
    # mouth inside at (100,-820), up to the edge at 820 m, out 1,338 m, back in, then three 7-8 m dips
    # out across y = 0, then the long run out (5 km)
    pts = [(100, -820), (100, 0), (100, 669), (300, 669), (300, 0), (300, -63), (400, -63),
           (400, 3.65), (450, 3.65), (450, -220), (600, -220), (600, 3.95), (650, 3.95),
           (650, -167), (800, -167), (800, 0), (800, 5000)]
    g = LineString(pts)
    ms = CC.crossings(g, big.boundary)
    cuts, inside, _ = CC.decide_rejoin(g, big, big.boundary, 1000.0)
    assert len(cuts) == 3 and cuts[0] == ms[0] and cuts[1] == ms[1], (cuts, ms)
    assert cuts[2] == ms[-1], "the exit is the LAST crossing, where the stream last left"
    assert g.length - cuts[2] == pytest.approx(5000.0), "and nothing of the run out is inside"
    assert inside == [(0.0, ms[0]), (ms[1], ms[-1])]


@pytest.mark.parametrize("tail, exits", [(4.0, False), (5.0, True), (30.0, True), (700.0, True),
                                          (1500.0, True)])
def test_an_outside_run_to_the_headwaters_is_an_exit_however_short(tail, exits):
    """FIX A1(b), user review 2026-10-06: an outside run that never re-enters — it runs to the line's
    end (its headwaters) outside the area — is an EXIT however short; only a run under the
    positional error (5 m) is ignored. The rejoin distance joins only runs that come BACK."""
    g = LineString([(1500, 500), (1000 - tail, 500)])           # mouth 500 m inside, source `tail` out
    cuts, inside, _ = CC.decide_rejoin(g, RESERVE, RESERVE.boundary, 1000.0)
    if exits:
        assert cuts == [500.0] and inside == [(0.0, 500.0)]
    else:
        assert cuts == [] and inside == [(0.0, g.length)]


def test_an_outside_run_to_a_lake_edge_is_an_exit_however_short():
    """The same at a stretch END that is a lake edge (the run ends on the lake, `stretches`): a 40 m
    run out to the lake is an exit at the last crossing, in absolute measures."""
    from pipeline.common.models import WaterbodyRun
    line = LineString([(1500, 500), (960, 500), (0, 500)])      # mouth inside; lake from 540 to 1500 m
    ch = BlkChain(blk="L", fwa_watershed_code="100", fids=(), geometry=line, mouth_measure=0.0,
                  length_m=line.length, name_tuples=(),
                  waterbody_runs=(WaterbodyRun(wbk="1", down_m=540.0, up_m=1500.0, kind="lake"),))
    cuts, inside, _ = CC.decide_chain(ch, RESERVE, RESERVE.boundary, 0.0, rejoin_m=1000.0)
    assert [c[:2] for c in cuts] == [(500.0, "exit")]
    assert inside == [(float("-inf"), 500.0)]


def test_an_isolated_dip_under_the_positional_error_is_ignored():
    """Trout Creek's shape: 4 m in and back, nothing else near. No entry, no cut, no membership. And
    the Ospika's: its mouth 1.9 m inside the edge — no cut, the line is outside."""
    dip = LineString([(0, 500), (999, 500), (1001, 502), (999, 504), (0, 504)])   # 2.8 m in
    cuts, inside, dips = CC.decide_rejoin(dip, RESERVE, RESERVE.boundary, 1000.0)
    assert cuts == [] and inside == [] and len(dips) == 1
    mouth = LineString([(1001.9, 500), (0, 500)])
    assert CC.decide_rejoin(mouth, RESERVE, RESERVE.boundary, 1000.0)[:2] == ([], [])
    mouth_out = LineString([(998.1, 500), (1900, 500)])     # 1.9 m outside at the mouth, then in
    cuts, inside, _ = CC.decide_rejoin(mouth_out, RESERVE, RESERVE.boundary, 1000.0)
    assert cuts == [] and inside == [(0.0, mouth_out.length)]


# ---- first/last areas (regions): as before, plus the outline and the sliver -------------------------
def _chain(line, blk="X"):
    return BlkChain(blk=blk, fwa_watershed_code="100", fids=(), geometry=line, mouth_measure=0.0,
                    length_m=line.length, name_tuples=())


def test_regions_cut_first_enter_last_exit_as_before():
    """A river weaving the line between two regions is cut at its first entry and last exit, and
    the straddle between keeps both regions by overlap (unchanged by this round)."""
    from pipeline.atlas.splits.area_splits import resolve_area_splits
    A, B = box(-1000, -500, 1000, 500), box(1000, -500, 3000, 500)
    line = LineString([(500, 0)] + [(1000 + (15 if i % 2 else -15), 40 * i) for i in range(1, 12)]
                      + [(1500, 440), (2500, 440)])
    pts = resolve_area_splits({"A": A, "B": B}, [_chain(line)], term="Region")
    ms = CC.crossings(line, A.boundary)
    got = sorted((p.split_id, round(p.route_measure, 6)) for p in pts)
    assert got == [("area:A", round(ms[-1], 6)), ("area:B", round(ms[0], 6))], \
        "A's last exit and B's first entry: the 11-crossing straddle between lies in both"


def test_a_region_dip_that_would_leave_a_sliver_is_not_cut():
    """Contact Creek dips 2.8 m into Region 6; a region line meets the West Road 0.3 m below its
    source. Neither is cut: each would leave a piece under the gate."""
    from pipeline.atlas.splits.area_splits import resolve_area_splits
    A, B = box(-1000, -500, 1000, 500), box(1000, -500, 3000, 500)
    dip = LineString([(500, 0), (998.6, 0), (1001.4, 1), (998.6, 2), (500, 2)])      # 2.8 m in B
    assert resolve_area_splits({"A": A, "B": B}, [_chain(dip)], term="Region") == []
    end = LineString([(500, 0), (1000.3, 0)])                                       # 0.3 m into B
    assert resolve_area_splits({"A": A, "B": B}, [_chain(end)], term="Region") == []
    ok = LineString([(500, 0), (1300, 0)])                                          # a real crossing
    assert len(resolve_area_splits({"A": A, "B": B}, [_chain(ok)], term="Region")) == 2


def test_a_region_crossing_on_the_outline_follows_the_border():
    """A region's edge on the B.C. outline IS the border: where the border cut there, the region's
    cut takes that measure (it becomes the border's alias); where the border cut nothing, neither
    does the region."""
    from pipeline.atlas.splits.area_splits import resolve_area_splits
    bc = box(-1000, -500, 2000, 500)
    B = box(1000, -500, 2000, 500)
    line = LineString([(1500, 0), (2500, 0)])               # leaves B (and B.C.) at x = 2000
    out_bd = bc.boundary
    pts = resolve_area_splits({"B": B}, [_chain(line)], border=(out_bd, {"X": [500.0 + 1e-9]}))
    assert [p.route_measure for p in pts] == [500.0 + 1e-9], "the border's own measure"
    pts = resolve_area_splits({"B": B}, [_chain(line)], border=(out_bd, {}))
    assert pts == [], "the border cut nothing here: neither does the region"

