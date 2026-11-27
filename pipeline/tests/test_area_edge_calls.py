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


def test_the_curated_file_is_well_formed_and_accepts_nothing_yet():
    from pipeline.tests import under_test
    doc = json.loads(under_test.curated().waters.area_edge_calls.read_text())
    assert E.problems(doc) == []
    assert {r["water"] for r in doc["proposed"]} >= {"Beaverfoot River"}
