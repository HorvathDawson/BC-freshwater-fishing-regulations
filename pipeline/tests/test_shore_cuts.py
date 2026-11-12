"""T665 (rewriting T660): THE SHORE CUT AT THE TIDAL POLYGON — a creek joining tidal water is cut where its line leaves
the DFO tidal polygon, only where the piece below goes tidal; a line wholly inside the polygon is tidal water whole.

Synthetic FWA lines built into a graph by the real builder, cut by the real sectionizer through the real atlas stage,
then read by the reach run's `tidal_water` / `tidal_crossing_owner`: the atlas makes the pieces, the reach decides
tidal. No data.
"""
from __future__ import annotations

import json
from types import SimpleNamespace

import pytest
from shapely.geometry import LineString, box
from shapely.ops import linemerge

from pipeline.atlas.graph import cutting
from pipeline.atlas.graph.blk_chains import FidRow, build_blk_chains
from pipeline.atlas.graph.graph import build_stream_graph
from pipeline.atlas.splits import shore as S
from pipeline.atlas.splits.sectionizer import AUTO_SOURCES, minted_id, place_points, split_graph_at
from pipeline.common import fwa_edge_types as ET
from pipeline.common.models import AnchorType, SplitPoint

CROSS, LAND = ET.WATER_CROSSING, ET.SINGLE_LINE
#: the tidal water: south of y = 300, around C's mouth and E (D's mouth, at x = 1500, and Z's are not in it)
SEA = box(-200, -1000, 1000, 300).union(box(2000, -1000, 3000, 300))


def _fid(fid, blk, wsc, coords, down_m, up_m, et=LAND, name="", gnis=""):
    geom = LineString(coords)
    dn, un = cutting.blk_endpoints(geom)
    return FidRow(fid=fid, blk=blk, wsc=wsc, edge_type=et, wbk="", gnis_id=gnis or ("9" if name else ""),
                  gnis_name=name, stream_order=1, stream_magnitude=1, down_m=down_m, up_m=up_m, geometry=geom,
                  down_node=dn, up_node=un)


def _fids():
    """River X along y = 0 (into Z at x = 0). Creek C joins X at x = 500 and leaves the polygon at y = 300; D joins at
    x = 1500 on land; E (x = 2500, 0..200) lies wholly in the sea."""
    return [
        _fid("Z1", "Z", "50", [(-500, 0), (0, 0)], 0, 500, name="Z Inlet", gnis="7"),
        _fid("X1", "X", "50-100", [(0, 0), (500, 0)], 0, 500, name="X River", gnis="8"),
        _fid("X2", "X", "50-100", [(500, 0), (1500, 0)], 500, 1500, name="X River", gnis="8"),
        _fid("X3", "X", "50-100", [(1500, 0), (2500, 0)], 1500, 2500, name="X River", gnis="8"),
        _fid("C1", "C", "50-100-1", [(500, 0), (500, 150)], 0, 150, et=CROSS),
        _fid("C2", "C", "50-100-1", [(500, 150), (500, 300)], 150, 300, et=CROSS),
        _fid("C3", "C", "50-100-1", [(500, 300), (500, 1000)], 300, 1000, name="C Creek", gnis="9"),
        _fid("D1", "D", "50-100-5", [(1500, 0), (1500, 600)], 0, 600, et=CROSS),
        _fid("D2", "D", "50-100-5", [(1500, 600), (1500, 900)], 600, 900),
        _fid("E1", "E", "50-100-7", [(2500, 0), (2500, 200)], 0, 200, et=CROSS),
    ]


F_CREEK = _fid("F1", "F", "50-100-7-1", [(2500, 200), (2500, 800)], 0, 600, name="F Creek", gnis="11")


def _built(extra=()):
    fids = _fids() + list(extra)
    g = build_stream_graph(build_blk_chains(fids, {}), fids)
    by_fid = {f.fid: f.geometry for f in fids}
    geoms = {nid: linemerge([by_fid[f] for f in n.member_fids]) for nid, n in g.nodes.items()}
    idx = {f.fid: (f.down_m, f.up_m, f.stream_order, f.stream_magnitude) for f in fids}
    return g, geoms, idx, fids


def _run(sea=SEA, tidal=("X:0",), existing=None, extra=(), limited=(), held=None):
    g, geoms, _, _ = _built(extra)
    inside = S.polygon_inside(S.graph_lines(g, geoms), sea)
    pts, whole, rows = S.polygon_rule(g, inside, set(tidal), existing, frozenset(limited), held or {})
    return pts, whole, {r["blk"]: r for r in rows}, inside


def test_the_shore_family_is_minted():
    assert "shore" in AUTO_SOURCES and minted_id("shore:C:300") and minted_id("split:shore:C:300")


def test_a_creek_into_tidal_water_is_cut_exactly_where_it_leaves_the_polygon():
    pts, whole, rows, inside = _run()
    assert [(p.split_id, p.route_measure, p.source, p.label) for p in pts] == [("shore:C:300", 300.0, "shore", "shore")]
    assert rows["C"]["cut_m"] == 300.0 and rows["C"]["joins"] == "curated" and rows["C"]["below_cut_tidal"]
    # E lies wholly inside the polygon: tidal whole, not a cut; D never meets the polygon
    assert [(w["blk"], w["why"]) for w in whole] == [("E", "inside")] and "E" not in rows and "D" not in inside


def test_a_creek_into_fresh_water_is_not_cut():
    pts, _, rows, _ = _run(tidal=())
    assert pts == [] and rows["C"]["not_cut"] == S.FRESH_RECEIVER


def test_a_cut_whose_piece_below_stays_fresh_is_not_made(monkeypatch):
    """C's water reaches tidal Z through X, but X (whose own limit governs it) is fresh: owner 2026-11-11, "drop them"."""
    pts, _, rows, _ = _run(tidal=("Z:0",), limited={"X"})
    assert pts == [] and rows["C"]["not_cut"] == S.FRESH_BELOW and rows["C"]["below_cut_tidal"] is False
    monkeypatch.setattr(S, "A_CUT_THAT_MAKES_NOTHING_TIDAL_IS_NOT_MADE", False)
    assert [p.blk for p in _run(tidal=("Z:0",), limited={"X"})[0]] == ["C"]


def test_a_dip_under_5_m_is_inside_and_a_6_m_one_is_not():
    _, whole, _, _ = _run(sea=SEA.difference(box(2400, 100, 2600, 104)))     # E leaves the polygon for 4 m
    assert [w["blk"] for w in whole] == ["E"]
    _, whole, rows, _ = _run(sea=SEA.difference(box(2400, 100, 2600, 106)))  # 6 m out: bridged to its top instead
    assert [(w["blk"], w["why"]) for w in whole] == [("E", "exit_at_top")] and rows["E"]["not_cut"] == S.EXIT_AT_TOP


def test_a_line_that_comes_back_more_than_25_m_later_is_cut_at_its_first_exit():
    pts, _, rows, _ = _run(sea=SEA.union(box(400, 600, 600, 800)))           # C: in 0-300, out 300-600, in 600-800
    assert [p.route_measure for p in pts] == [300.0] and rows["C"]["reenters"] is True
    pts, _, rows, _ = _run(sea=SEA.union(box(400, 320, 600, 800)))           # out only 300-320: bridged
    assert [p.route_measure for p in pts] == [800.0] and rows["C"]["reenters"] is False


def test_an_exit_within_5_m_of_a_standing_boundary_is_that_boundary():
    pts, _, rows, _ = _run(existing={"C": [303.0]})
    assert [p.route_measure for p in pts] == [303.0] and rows["C"]["onto_existing"] is True


def test_a_mouth_outside_the_polygon_is_listed_not_cut():
    pts, _, rows, _ = _run(sea=box(1400, 300, 1600, 500))                    # D: inside 300-500 only
    assert pts == [] and rows["D"]["not_cut"] == S.MOUTH_OUTSIDE


def test_a_creek_into_a_line_wholly_inside_is_cut_and_tidal_below():
    """D2: F flows into E (wholly inside) and leaves the polygon 100 m up; nothing else is tidal."""
    pts, whole, rows, _ = _run(tidal=(), extra=[F_CREEK])
    assert [w["blk"] for w in whole] == ["E"]
    assert [(p.blk, p.route_measure) for p in pts] == [("F", 100.0)]
    assert rows["F"]["joins"] == "inside line" and rows["F"]["below_cut_tidal"] is True


def test_a_line_with_its_own_curated_limit_and_a_held_river_are_never_cut():
    """The Pitt and the Nitinat: a river whose owner limit stands lower would go tidal far above it. A held river
    waits for the owner's tick."""
    pts, _, rows, _ = _run(limited={"C"})
    assert pts == [] and rows["C"]["not_cut"] == S.OWNER_LIMIT
    pts, _, rows, _ = _run(held={"C": "C Creek"})
    assert pts == [] and rows["C"]["not_cut"] == S.HELD


def test_the_held_lists_are_the_owners():
    assert "360218493" in S.HELD_FOR_OWNER and len(S.HELD_FOR_OWNER) == 8 and len(S.HELD_UP_THE_CHANNEL) == 53
    assert not set(S.HELD_FOR_OWNER) & set(S.HELD_UP_THE_CHANNEL)


def test_the_build_holds_both_lists_by_default(monkeypatch):
    g, geoms, _, _ = _built()
    inside = S.polygon_inside(S.graph_lines(g, geoms), SEA)
    monkeypatch.setattr(S, "HELD_UP_THE_CHANNEL", {"C": "C Creek (300 m up the channel)"})
    pts, _, rows = S.polygon_rule(g, inside, {"X:0"})
    assert pts == [] and {r["blk"]: r.get("not_cut") for r in rows}["C"] == S.HELD


def test_owner_limited_follows_the_water_not_the_line():
    from pipeline.common.models import NodeKind
    n = lambda blk, g: SimpleNamespace(kind=NodeKind.stream, blk=blk, gnis_id=g)  # noqa: E731
    graph = SimpleNamespace(nodes={"A:0": n("A", "1"), "B:0": n("B", "1"), "C:0": n("C", "2"), "E:0": n("E", "")})
    assert S.owner_limited(graph, {"E:0"}, [{"blk": "A", "limit": "split"}, {"blk": "C", "limit": "mouth"}]) == \
        {"A", "B", "E"}


def test_owner_limited_takes_an_added_line_of_the_governed_water():
    """T685: an added line (no gnis) the registry puts in the governed water (Turner Creek in Noons Creek) is governed;
    one in another water is not, and without `waters` it is not (the near-miss the build hit)"""
    from pipeline.common.models import NodeKind
    n = lambda blk, g: SimpleNamespace(kind=NodeKind.stream, blk=blk, gnis_id=g)  # noqa: E731
    graph = SimpleNamespace(nodes={"A:0": n("A", "1"), "T:0": n("T", ""), "U:0": n("U", ""), "C:0": n("C", "2")})
    rows = [{"blk": "A", "limit": "split"}]
    assert S.owner_limited(graph, set(), rows) == {"A"}
    assert S.owner_limited(graph, set(), rows, [frozenset({"A", "T"}), frozenset({"C", "U"})]) == {"A", "T"}


def test_atlas_tidal_is_the_water_below_the_limit():
    from pipeline.atlas.splits.tidal import Limit
    g, geoms, idx, _ = _built()
    sp = SplitPoint(split_id="x_tidal", blk="X", route_measure=1000.0, fid="", label="tidal boundary",
                    anchor_type=AnchorType.point, source="curated")
    split_graph_at(g, geoms, place_points(g, [sp]), idx)
    got = S.atlas_tidal(g, [Limit(blk="X", limit="split", split="x_tidal")], {})
    assert got == {"X:0"}
    with pytest.raises(SystemExit, match="not a boundary"):
        S.atlas_tidal(g, [Limit(blk="X", limit="split", split="elsewhere")], {})


def test_mutation_each_policy_flag_moves_the_answer(monkeypatch):
    monkeypatch.setattr(S, "A_LINE_WHOLLY_INSIDE_THE_POLYGON_IS_TIDAL", False)
    assert not [w for w in _run()[1] if w["why"] == "inside"]
    monkeypatch.undo()
    monkeypatch.setattr(S, "A_CREEK_INTO_TIDAL_WATER_IS_CUT_AT_ITS_POLYGON_EXIT", False)
    assert _run()[0] == []
    monkeypatch.undo()
    monkeypatch.setattr(S, "THE_OWNERS_LIMIT_GOVERNS_ITS_OWN_WATER", False)
    assert [p.blk for p in _run(limited={"C"})[0]] == ["C"]


def test_standing_boundaries_skip_shore_cuts():
    g, geoms, idx, _ = _built()
    sp = SplitPoint(split_id="shore:C:300", blk="C", route_measure=300.0, fid="", label="shore",
                    anchor_type=AnchorType.point, source="shore")
    split_graph_at(g, geoms, place_points(g, [sp]), idx)
    assert "C" not in S.standing_boundaries(g)


# T678: the atlas stage no longer runs the polygon rule (`build.cut_shores` is the coast cut, `test_coast_cut.py`);
# the rule's functions above stay pinned for the review tools that still read a pre-T678 atlas.


def test_read_inside_refuses_a_polygon_atlas_without_its_sidecar(tmp_path):
    (tmp_path / S.RECORD).write_text(json.dumps({"rule": S.RULE, "runs": []}))
    with pytest.raises(SystemExit, match="has no inside_tidal.json"):
        S.read_inside(tmp_path)
    (tmp_path / S.RECORD).write_text(json.dumps({"runs": []}))          # a T660 atlas: none by construction
    assert S.read_inside(tmp_path) == ()
