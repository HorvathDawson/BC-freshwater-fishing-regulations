"""The owner's calls on edge water the closure cutter leaves open (`waters/area_edge_calls.json`, T1199 review).

A boundary river drawn wiggling across the edge (every crossing a graze) is left open by the cutter and named by the
area-cut gate's G6. A `proposed` row is no acceptance; `closed` makes the cutter close it; `open` accepts it open; an
accepted row matching nothing is stale. Synthetic geometry, no data.
"""
from __future__ import annotations

import json

import pytest
from shapely.geometry import LineString, box

from pipeline.atlas.splits import area_cut_gate as G
from pipeline.atlas.splits import area_edge_calls as E
from pipeline.atlas.splits import clean_cut as CC
from pipeline.common.models import NodeKind, StreamGraph, StreamNode

AREA = box(1000, 0, 3000, 3000)
AID, NAME = "area:national_parks:p", "P"


def _wiggle():
    """+-0.5 m across the edge x = 1000, a crossing every 4 m, 2,062 m long (every run in or out under the dip)."""
    return LineString([(1000 + (0.5 if k % 2 else -0.5), 100 + 4.0 * k) for k in range(501)])


def _built(line, inside):
    """The line cut into pieces at the cutter's inside ends; members of the area where the cutter put them."""
    ends = sorted({0.0, line.length} | {e for a, b in inside for e in (a, b)})
    g, geoms = StreamGraph(), {}
    for lo, hi in zip(ends, ends[1:]):
        mid = (lo + hi) / 2
        member = any(a <= mid <= b for a, b in inside)
        nid = f"W:{lo:.1f}"
        g.nodes[nid] = StreamNode(node_id=nid, kind=NodeKind.stream, blk="W", down_m=lo, up_m=hi, length_m=hi - lo,
                                  in_areas=(AID,) if member else ())
        from shapely.ops import substring
        geoms[nid] = substring(line, lo, hi)
    return g, geoms


def _call(call, lo=0.0, hi=2062.0, area=AID):
    return E.EdgeCall(area=area, blk="W", water="W River", from_m=lo, to_m=hi, call=call, note="test")


def _doc(accepted=(), proposed=()):
    return {"_about": "test", "accepted": list(accepted), "proposed": list(proposed)}


ROW = {"area": AID, "blk": "W", "water": "W River", "from_m": 0.0, "to_m": 2000.0, "call": "open", "note": "x"}


def test_the_wiggling_line_is_left_open_and_g6_stops_the_build_with_a_proposed_row_only(tmp_path):
    p = tmp_path / "calls.json"
    p.write_text(json.dumps(_doc(proposed=[ROW])))
    assert E.load(p) == [], "a proposal is no acceptance"
    line = _wiggle()
    g, geoms = _built(line, CC.decide_area(line, AREA, AREA.boundary)[1])
    with pytest.raises(SystemExit, match="G6 1"):
        G.check(g, geoms, [(AID, NAME, AREA)], tmp_path, calls=E.load(p))


def test_closed_makes_the_stretch_a_member_and_every_check_passes():
    line = _wiggle()
    cuts, inside, _ = CC.decide_area(line, AREA, AREA.boundary, forced_in=[(0.0, line.length)])
    assert inside == [(0.0, line.length)] and cuts == []
    g, geoms = _built(line, inside)
    res = G.measure(g, geoms, [(AID, NAME, AREA)], calls=[_call("closed")])
    assert all(n.in_areas == (AID,) for n in g.nodes.values())
    assert set(res["counts"].values()) == {0}, res["counts"]
    assert res["calls_used"][0]["call"] == "closed"


def test_the_cutter_takes_closed_calls_by_area_and_measure_through_resolve():
    from pipeline.common.models import BlkChain
    line = _wiggle()
    ch = BlkChain(blk="W", fwa_watershed_code="1", fids=(), geometry=line, mouth_measure=0.0, length_m=line.length,
                  name_tuples=())
    assert CC.resolve_clean_cuts({NAME: AREA}, [ch]).inside[NAME] == {}
    res = CC.resolve_clean_cuts({NAME: AREA}, [ch], forced={NAME: {"W": [(0.0, line.length)]}})
    assert res.inside[NAME]["W"] == [(float("-inf"), float("inf"))]
    assert E.closed_by_area([_call("closed"), _call("open")]) == {AID: {"W": [(0.0, 2062.0)]}}


def test_open_passes_g6_and_leaves_it_open():
    line = _wiggle()
    g, geoms = _built(line, CC.decide_area(line, AREA, AREA.boundary)[1])
    res = G.measure(g, geoms, [(AID, NAME, AREA)], calls=[_call("open")])
    assert set(res["counts"].values()) == {0}, res["counts"]
    assert all(n.in_areas == () for n in g.nodes.values())


def test_an_accepted_call_matching_nothing_is_stale_and_stops_the_build(tmp_path):
    line = _wiggle()
    g, geoms = _built(line, CC.decide_area(line, AREA, AREA.boundary)[1])
    stale = _call("open", 5000.0, 6000.0)
    res = G.measure(g, geoms, [(AID, NAME, AREA)], calls=[_call("open"), stale])
    assert res["counts"]["G6"] == 0 and [r["from_m"] for r in res["checks"]["G6_stale"]] == [5000.0]
    with pytest.raises(SystemExit, match="stale calls 1"):
        G.check(g, geoms, [(AID, NAME, AREA)], tmp_path, calls=[_call("open"), stale])


def test_the_file_is_checked():
    assert E.problems(_doc(accepted=[ROW])) == ["area_edge_calls.accepted[0]: an accepted row needs accepted_by and "
                                                "accepted_on"]
    assert E.problems(_doc(proposed=[{**ROW, "call": "shut"}]))[0].endswith("got 'shut'")
    assert E.problems(_doc(proposed=[{**ROW, "from_m": 9.0, "to_m": 1.0}])) == [
        "area_edge_calls.proposed[0]: from_m must be below to_m"]
    assert E.problems(_doc(accepted=[{**ROW, "accepted_by": "owner", "accepted_on": "2026-11-27"}])) == []
    assert E.problems({"accepted": []})[0].endswith("with an `_about`")
    assert E.MATCH_M == 5.0


def test_the_curated_file_is_well_formed_and_holds_the_owners_two_closed_calls():
    """Owner 2026-11-27 (T1199): "Merge with the list; close both edge stretches" — both G6 stretches closed."""
    from pipeline.tests import under_test
    p = under_test.curated().waters.area_edge_calls
    assert E.problems(json.loads(p.read_text())) == []
    got = {(c.blk, c.from_m, c.to_m, c.call) for c in E.load(p)}
    assert got >= {("356566041", 11976.2, 12099.2, "closed"), ("354154906", 318.3, 421.6, "closed")}


def _hug_two_grazes():
    """The real two's shape (Beaverfoot / Klanawa, T1199 round 3): an open line 5-10 m out of the edge x = 1000 for
    over 100 m within the band, with exactly TWO grazes of 0.3 m into the area."""
    return LineString([(0, 100), (995, 100), (1000.3, 101), (995, 102), (990, 120), (990, 200), (995, 220),
                       (1000.3, 221), (995, 222), (0, 222)])


def test_g6_names_a_hug_with_exactly_two_grazes(monkeypatch):
    line = _hug_two_grazes()
    inside = CC.decide_area(line, AREA, AREA.boundary)[1]
    assert inside == [], "both grazes are under the dip: the cutter leaves it open"
    g, geoms = _built(line, inside)
    got = G.measure(g, geoms, [(AID, NAME, AREA)])["checks"]["G6"]
    assert [r["grazes"] for r in got] == [2] and G.GRAZES_TO_LOOK_AT == 2
    monkeypatch.setattr(G, "GRAZES_TO_LOOK_AT", 3)
    assert G.measure(g, geoms, [(AID, NAME, AREA)])["counts"]["G6"] == 0, "the pin is what names two"


def test_an_open_call_holds_a_stretch_moved_under_match_m_and_goes_stale_past_it():
    line = _hug_two_grazes()
    g, geoms = _built(line, [])
    row = G.measure(g, geoms, [(AID, NAME, AREA)])["checks"]["G6"][0]
    for off, stale in ((E.MATCH_M - 1.0, 0), (E.MATCH_M + 1.0, 1)):
        c = _call("open", row["from_m"] + off, row["to_m"])
        n = G.measure(g, geoms, [(AID, NAME, AREA)], calls=[c])["counts"]
        assert (n["G6"], n["G6_stale"]) == (stale, stale), off


def test_a_called_stretch_on_a_line_that_never_crosses_moves_onto_a_boundary_within_the_floor():
    """The floor AND the move apply to a called stretch, crossings or none: its end 60 m short of another boundary
    moves onto it (round 3: the no-crossing path ran the floor only)."""
    line = LineString([(980, 0), (980, 2000)])
    stats: dict = {}
    cuts, inside, _ = CC.decide_area(line, AREA, AREA.boundary, others=[760.0], forced_in=[(500.0, 700.0)],
                                     stats=stats)
    assert inside == [(500.0, 760.0)] and cuts == [500.0, 760.0]
    assert stats["stretches_called_edge_water"] == 1


def test_a_closed_call_whose_stretch_left_the_band_is_stale():
    """A `closed` call is applied by measure; if the line under it no longer lies within the edge band (FWA redrew
    it), it is no longer within the stretch G6 would name: stale, named, never applied blind."""
    line = LineString([(990, 100), (990, 150), (940, 160), (940, 175), (990, 185), (990, 300)])
    g, geoms = _built(line, [(0.0, line.length)])
    res = G.measure(g, geoms, [(AID, NAME, AREA)], calls=[_call("closed", 0.0, line.length)])
    assert res["counts"]["G6_stale"] == 1 and "not within the stretch" in res["checks"]["G6_stale"][0]["why"]
    assert res["counts"]["G1"] == 0, "under G1's allowance: only the call's own check sees it"
    ok = LineString([(990, 100), (990, 300)])
    g, geoms = _built(ok, [(0.0, ok.length)])
    assert G.measure(g, geoms, [(AID, NAME, AREA)], calls=[_call("closed", 0.0, ok.length)])["counts"]["G6_stale"] == 0


def test_a_closed_call_and_an_open_line_in_one_area_are_both_judged():
    """Round 3: the call loop once rebound the area's count record (`c`), and the next open piece crashed the gate."""
    ok = LineString([(990, 100), (990, 300)])
    g, geoms = _built(ok, [(0.0, ok.length)])
    g.nodes["V:0"] = StreamNode(node_id="V:0", kind=NodeKind.stream, blk="V", down_m=0.0, up_m=500.0, length_m=500.0,
                                in_areas=())
    geoms["V:0"] = LineString([(500, 1000), (1003, 1000), (500, 1001)])
    res = G.measure(g, geoms, [(AID, NAME, AREA)], calls=[_call("closed", 0.0, ok.length)])
    assert set(res["counts"].values()) == {0} and res["areas"][AID]["near_outside"] == 1


def _klanawa():
    """The Klanawa creek's shape: 200 m inside the area, out across the edge, two grazes early on a hug 10 m out,
    then away past the band. Without a call the cutter closes the inside only and G6 names the hug."""
    line = LineString([(1200, 100), (1000, 100), (995, 105), (990, 115), (1000.3, 125), (990, 135), (1000.3, 145),
                       (990, 155), (990, 260), (940, 300), (700, 400)])
    inside = CC.decide_area(line, AREA, AREA.boundary)[1]
    g, geoms = _built(line, inside)
    row = G.measure(g, geoms, [(AID, NAME, AREA)])["checks"]["G6"][0]
    return line, row


def _closed_gate(line, lo, hi):
    inside = CC.decide_area(line, AREA, AREA.boundary, forced_in=[(lo, hi)])[1]
    g, geoms = _built(line, inside)
    return inside, G.measure(g, geoms, [(AID, NAME, AREA)], calls=[_call("closed", lo, hi)])["counts"]


def test_the_whole_stretch_or_a_shortened_row_closes_to_its_end_and_the_rest_is_judged_by_g6():
    """Owner 2026-11-27 (T1199): a `closed` row may close PART of the stretch (option 2 on the Klanawa creek: the row
    shortened to where today's closure ends); what it leaves open is under the floor with no graze, so G6 passes it."""
    line, row = _klanawa()
    assert row["grazes"] == 2 and abs(row["from_m"] - 200.0) < 0.1
    for hi in (row["to_m"], 290.0):          # 290: past both grazes, 140 m of hug left open with none
        inside, n = _closed_gate(line, row["from_m"], hi)
        assert abs(inside[0][1] - hi) < 0.1, "closed to the row's end, no farther"
        assert set(n.values()) == {0}, (hi, n)


def test_a_closed_call_shifted_into_inside_water_or_past_the_stretch_is_stale():
    line, row = _klanawa()
    for lo, hi in ((50.0, 150.0), (row["from_m"] - 60.0, row["to_m"]), (row["from_m"], row["to_m"] + 30.0)):
        assert _closed_gate(line, lo, hi)[1]["G6_stale"] == 1, (lo, hi)


def test_a_closed_call_holds_an_end_moved_out_under_match_m_and_goes_stale_past_it():
    line, row = _klanawa()
    for off, stale in ((E.MATCH_M - 1.0, 0), (E.MATCH_M + 1.0, 1)):
        assert _closed_gate(line, row["from_m"] - off, row["to_m"])[1]["G6_stale"] == stale, ("from", off)
        assert _closed_gate(line, row["from_m"], row["to_m"] + off)[1]["G6_stale"] == stale, ("to", off)
