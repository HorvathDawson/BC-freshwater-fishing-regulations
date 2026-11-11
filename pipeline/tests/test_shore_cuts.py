"""T660: THE SHORE CUT — a creek FWA draws on across tidal water to its river's line is cut where its own line begins.

Synthetic FWA lines (a river X, creeks C and D drawn across the water with `fwa_edge_types.WATER_CROSSING`), built into a
graph by the real builder, cut by the real sectionizer, then read by the reach run's `crossing_pieces`: the atlas makes
the piece, the reach recognises it by the cut. No data.
"""
from __future__ import annotations

from shapely.geometry import LineString, box

from pipeline.atlas.graph import cutting
from pipeline.atlas.graph.blk_chains import FidRow, build_blk_chains
from pipeline.atlas.graph.graph import build_stream_graph
from pipeline.atlas.splits import shore as S
from pipeline.atlas.splits.sectionizer import AUTO_SOURCES, minted_id, place_points, split_graph_at
from pipeline.common import fwa_edge_types as ET

CROSS, LAND = ET.WATER_CROSSING, ET.SINGLE_LINE
#: the tidal water: south of y = 300, around C's mouth and E's (D's mouth, at x = 1500, is not in it)
SEA = box(-1000, -1000, 1000, 300).union(box(2000, -1000, 3000, 300))


def _fid(fid, blk, wsc, coords, down_m, up_m, et=LAND, name=""):
    geom = LineString(coords)
    dn, un = cutting.blk_endpoints(geom)
    return FidRow(fid=fid, blk=blk, wsc=wsc, edge_type=et, wbk="", gnis_id="9" if name else "", gnis_name=name,
                  stream_order=1, stream_magnitude=1, down_m=down_m, up_m=up_m, geometry=geom, down_node=dn, up_node=un)


def _fids():
    """River X along y = 0. Creek C joins at x = 500: crossed from y = 0 to the shore at y = 300 (two crossing lines),
    then 700 m of its own line on land. Creek D joins at x = 1500 the same way but its crossing lies on LAND (an inland
    river polygon: the polygon misses it). Creek E is a crossing and nothing else."""
    return [
        _fid("X1", "X", "100", [(0, 0), (500, 0)], 0, 500, name="X River"),
        _fid("X2", "X", "100", [(500, 0), (1500, 0)], 500, 1500, name="X River"),
        _fid("X3", "X", "100", [(1500, 0), (2500, 0)], 1500, 2500, name="X River"),
        _fid("C1", "C", "100-1", [(500, 0), (500, 150)], 0, 150, et=CROSS),
        _fid("C2", "C", "100-1", [(500, 150), (500, 300)], 150, 300, et=CROSS),
        _fid("C3", "C", "100-1", [(500, 300), (500, 1000)], 300, 1000, name="C Creek"),
        _fid("D1", "D", "100-5", [(1500, 0), (1500, 600)], 0, 600, et=CROSS),
        _fid("D2", "D", "100-5", [(1500, 600), (1500, 900)], 600, 900),
        _fid("E1", "E", "100-7", [(2500, 0), (2500, 200)], 0, 200, et=CROSS),
    ]


def _graph():
    fids = _fids()
    g = build_stream_graph(build_blk_chains(fids, {}), fids)
    idx = {f.fid: (f.down_m, f.up_m, f.stream_order, f.stream_magnitude) for f in fids}
    cross = {f.fid: f.geometry for f in fids if f.edge_type == CROSS}
    return g, idx, cross, {f.fid: f.geometry for f in fids}


def test_a_crossing_in_tidal_water_is_cut_at_the_top_of_its_run():
    g, idx, cross, _ = _graph()
    rec: list = []
    pts = S.shore_cuts(g, idx, set(cross), S.share_in(SEA, cross), record=rec)
    assert [(p.split_id, p.blk, p.route_measure, p.source, p.label) for p in pts] == \
        [("shore:C:300", "C", 300, "shore", "shore")]
    # D's crossing lies on land (0 % in the polygon): no cut, no record; E is a crossing through and through
    assert [(r["blk"], r.get("not_cut")) for r in rec] == [("C", None), ("E", "the whole line is the crossing")]


def test_a_run_mostly_on_land_is_recorded_and_not_cut():
    g, idx, cross, _ = _graph()
    half = box(-1000, -1000, 3000, 200)            # C's crossing: 200 of 300 m in, D's: 200 of 600 m in
    rec: list = []
    pts = S.shore_cuts(g, idx, set(cross), S.share_in(half, cross), record=rec)
    assert [p.blk for p in pts] == ["C"]
    assert {r["blk"]: r.get("not_cut") for r in rec}["D"] == "under 50% in tidal water"


def test_the_cut_piece_is_what_the_reach_reads_as_the_crossing():
    """Atlas -> reach: after the real sectionizer cuts, `reach.outside.crossing_pieces` holds the crossing (by the
    shore cut: a cut piece keeps its parent's mixed edge types) and the creek above it is not one."""
    from pipeline.atlas.reach.outside import crossing_pieces
    g, idx, cross, geoms = _graph()
    pts = place_points(g, S.shore_cuts(g, idx, set(cross), S.share_in(SEA, cross)))
    split_graph_at(g, {}, pts, idx)
    c = {nid for nid, n in g.nodes.items() if n.blk == "C"}
    assert c == {"C:0", "C:300"}
    assert g.nodes["C:0"].upper_bound.boundary_id == "split:shore:C:300"
    got = crossing_pieces(g)
    assert "C:0" in got and "C:300" not in got and not any(n.startswith("D:") for n in got)


def test_the_shore_family_is_minted():
    assert "shore" in AUTO_SOURCES and minted_id("shore:C:300") and minted_id("split:shore:C:300")


def test_a_sliver_is_not_cut():
    g, idx, cross, _ = _graph()
    idx = dict(idx, C3=(300, 303, 1, 1))                 # 3 m of C's own line above its crossing
    n = g.nodes["C:0"]
    g.nodes["C:0"] = type(n)(**{**n.__dict__, "up_m": 303.0})
    rec: list = []
    assert S.shore_cuts(g, idx, set(cross), S.share_in(SEA, cross), record=rec) == []
    assert rec[0]["not_cut"] == "a piece under 5 m"


def test_the_atlas_stage_cuts_records_and_sweeps(tmp_path, monkeypatch):
    """`build.cut_shores` end to end on the synthetic lines: one cut, the record written, the sweep finds no braid."""
    import json
    from types import SimpleNamespace
    from shapely.ops import linemerge
    from pipeline.atlas import build as B
    g, idx, cross, by_fid = _graph()
    geoms = {nid: linemerge([by_fid[f] for f in n.member_fids]) for nid, n in g.nodes.items()}
    monkeypatch.setattr(S, "read_tidal_polygon", lambda gpkg, bbox=None: SEA)
    b = SimpleNamespace(args=SimpleNamespace(gpkg="unused.gpkg", out=str(tmp_path)), graph=g, geoms=geoms,
                        fid_index=idx, applied_splits=[], fids=_fids(), bbox=None, tick=lambda label: None)
    B.cut_shores(b)
    assert {nid for nid in g.nodes if nid.startswith("C:")} == {"C:0", "C:300"}
    assert [p.split_id for p in b.applied_splits] == ["shore:C:300"]
    rec = json.loads((tmp_path / S.RECORD).read_text())
    assert rec["cuts"] == 1 and [r["blk"] for r in rec["runs"]] == ["C", "E"]


def _braided():
    """A SIDE CHANNEL S of X River (its own water: same name and code) whose foot crosses tidal water to X at x = 500
    (a crossing run, 0-300 m) and whose head rejoins X at x = 1500: a braid of S's own water spans S's shore."""
    fids = [f for f in _fids() if f.blk == "X"] + [
        _fid("S1", "S", "100", [(500, 0), (500, 300)], 0, 300, et=CROSS, name="X River"),
        _fid("S2", "S", "100", [(500, 300), (1500, 300)], 300, 1300, name="X River"),
        _fid("S3", "S", "100", [(1500, 300), (1500, 0)], 1300, 1600, name="X River"),
    ]
    g = build_stream_graph(build_blk_chains(fids, {}), fids)
    idx = {f.fid: (f.down_m, f.up_m, f.stream_order, f.stream_magnitude) for f in fids}
    by_fid = {f.fid: f.geometry for f in fids}
    return g, idx, {f.fid: f.geometry for f in fids if f.edge_type == CROSS}, by_fid, fids


def _stage(g, idx, by_fid, fids, tmp_path, monkeypatch, sea=SEA):
    import json
    from types import SimpleNamespace
    from shapely.ops import linemerge
    from pipeline.atlas import build as B
    geoms = {nid: linemerge([by_fid[f] for f in n.member_fids]) for nid, n in g.nodes.items()}
    monkeypatch.setattr(S, "read_tidal_polygon", lambda gpkg, bbox=None: sea)
    b = SimpleNamespace(args=SimpleNamespace(gpkg="unused.gpkg", out=str(tmp_path)), graph=g, geoms=geoms,
                        fid_index=idx, applied_splits=[], fids=fids, bbox=None, tick=lambda label: None)
    B.cut_shores(b)
    return b, json.loads((tmp_path / S.RECORD).read_text())


def test_a_shore_a_braid_of_its_own_water_spans_is_listed_not_cut(tmp_path, monkeypatch):
    """Build 27 stopped here (403 braid cuts, 97 shores): a side channel crossing back into its own river. The shore
    is not cut, the run is listed with its reason, and the build goes on with nothing straddling."""
    g, idx, cross, by_fid, fids = _braided()
    b, rec = _stage(g, idx, by_fid, fids, tmp_path, monkeypatch)
    assert b.applied_splits == [] and not any(nid.startswith("S:") and nid != "S:0" for nid in g.nodes)
    assert [(r["blk"], r.get("not_cut")) for r in rec["runs"]] == [("S", S.SPANNED)] and rec["cuts"] == 0
    assert (tmp_path / "braid_sweep_shore.json").exists()


def test_mutation_cutting_a_braided_shore_stops_the_build(tmp_path, monkeypatch):
    """Without the gate the sweep must cut X River across its width for S's shore: the stage refuses, and the
    records are written before it does."""
    import pytest
    monkeypatch.setattr(S, "A_SHORE_A_BRAID_SPANS_IS_NOT_CUT", False)
    g, idx, cross, by_fid, fids = _braided()
    with pytest.raises(SystemExit, match="meet a braid"):
        _stage(g, idx, by_fid, fids, tmp_path, monkeypatch)
    assert (tmp_path / S.RECORD).exists() and (tmp_path / "braid_sweep_shore.json").exists()


def test_a_plain_crossing_is_still_cut_beside_a_braided_one(tmp_path, monkeypatch):
    g, idx, cross, by_fid, fids = _braided()
    extra = [f for f in _fids() if f.blk == "C"]
    fids = fids + extra
    g = build_stream_graph(build_blk_chains(fids, {}), fids)
    idx = {f.fid: (f.down_m, f.up_m, f.stream_order, f.stream_magnitude) for f in fids}
    by_fid = {f.fid: f.geometry for f in fids}
    b, rec = _stage(g, idx, by_fid, fids, tmp_path, monkeypatch)
    assert [p.split_id for p in b.applied_splits] == ["shore:C:300"]
    assert {r["blk"]: r.get("not_cut") for r in rec["runs"]} == {"C": None, "S": S.SPANNED}


# ------------------------------------------------------------------------------------------------
# T665: THE POLYGON RULE (`shore.polygon_rule`) — not wired into the build yet; the dry run reads it on the live atlas.
# ------------------------------------------------------------------------------------------------

def _poly_run(sea=SEA, tidal=("X:0",), mouth_rows=(), existing=None, extra=(), limited=()):
    from shapely.ops import linemerge
    fids = _fids() + list(extra)
    g = build_stream_graph(build_blk_chains(fids, {}), fids)
    by_fid = {f.fid: f.geometry for f in fids}
    geoms = {nid: linemerge([by_fid[f] for f in n.member_fids]) for nid, n in g.nodes.items()}
    inside = S.polygon_inside(S.graph_lines(g, geoms), sea)
    pts, rule_a, rows = S.polygon_rule(g, inside, set(tidal), frozenset(mouth_rows), existing, frozenset(limited))
    return pts, rule_a, {r["blk"]: r for r in rows}, inside


def test_t665_a_creek_into_tidal_water_is_cut_exactly_where_it_leaves_the_polygon():
    pts, rule_a, rows, inside = _poly_run()
    assert [(p.split_id, p.route_measure, p.source, p.label) for p in pts] == [("shore:C:300", 300.0, "shore", "shore")]
    assert rows["C"]["cut_m"] == 300.0 and rows["C"]["joins"] == "curated" and rows["C"]["below_cut_tidal"]
    # E lies wholly inside the polygon: rule A, not a cut; D never meets the polygon
    assert [r["blk"] for r in rule_a] == ["E"] and "E" not in rows and "D" not in inside


def test_t665_a_creek_into_fresh_water_is_not_cut():
    pts, _, rows, _ = _poly_run(tidal=())
    assert pts == [] and rows["C"]["not_cut"] == S.FRESH_RECEIVER


def test_t665_a_dip_under_5_m_is_inside_and_a_6_m_one_is_not():
    from shapely.geometry import box as _box
    dip4 = SEA.difference(_box(2400, 100, 2600, 104))         # E leaves the polygon for 4 m
    _, rule_a, _, _ = _poly_run(sea=dip4)
    assert [r["blk"] for r in rule_a] == ["E"]
    dip6 = SEA.difference(_box(2400, 100, 2600, 106))         # 6 m out: not wholly inside, but bridged to its top
    _, rule_a, rows, _ = _poly_run(sea=dip6)
    assert rule_a == [] and rows["E"]["not_cut"] == S.EXIT_AT_TOP


def test_t665_a_line_that_comes_back_more_than_25_m_later_is_cut_at_its_first_exit():
    from shapely.geometry import box as _box
    pts, _, rows, _ = _poly_run(sea=SEA.union(_box(400, 600, 600, 800)))     # C: in 0-300, out 300-600, in 600-800
    assert [p.route_measure for p in pts] == [300.0] and rows["C"]["reenters"] is True
    pts, _, rows, _ = _poly_run(sea=SEA.union(_box(400, 320, 600, 800)))     # out only 300-320: bridged
    assert [p.route_measure for p in pts] == [800.0] and rows["C"]["reenters"] is False


def test_t665_an_owner_mouth_row_keeps_the_limit_at_the_line_end():
    pts, _, rows, _ = _poly_run(mouth_rows={"C"})
    assert pts == [] and rows["C"]["not_cut"] == S.OWNER_MOUTH and rows["C"]["inside_m"] == 300.0


def test_t665_an_exit_within_5_m_of_a_standing_boundary_is_that_boundary():
    pts, _, rows, _ = _poly_run(existing={"C": [303.0]})
    assert [p.route_measure for p in pts] == [303.0] and rows["C"]["onto_existing"] is True


def test_t665_a_mouth_outside_the_polygon_is_listed_not_cut():
    from shapely.geometry import box as _box
    pts, _, rows, _ = _poly_run(sea=_box(1400, 300, 1600, 500))              # D: inside 300-500 only
    assert pts == [] and rows["D"]["not_cut"] == S.MOUTH_OUTSIDE


def test_t665_a_creek_into_a_line_wholly_inside_is_cut_and_tidal_below():
    """D2: F flows into E (wholly inside: rule A) and leaves the polygon 100 m up; nothing else is tidal."""
    f = _fid("F1", "F", "100-7-1", [(2500, 200), (2500, 800)], 0, 600, name="F Creek")
    pts, rule_a, rows, _ = _poly_run(tidal=(), extra=[f])
    assert [r["blk"] for r in rule_a] == ["E"]
    assert [(p.blk, p.route_measure) for p in pts] == [("F", 100.0)]
    assert rows["F"]["joins"] == "inside line" and rows["F"]["below_cut_tidal"] is True


def test_t665_a_line_with_its_own_curated_limit_is_never_cut_by_the_polygon():
    """The Pitt and the Nitinat: a river whose owner limit stands lower would go tidal far above it."""
    pts, _, rows, _ = _poly_run(limited={"C"})
    assert pts == [] and rows["C"]["not_cut"] == S.OWNER_LIMIT


def test_t665_owner_limited_follows_the_water_not_the_line():
    from types import SimpleNamespace
    from pipeline.common.models import NodeKind
    n = lambda blk, g: SimpleNamespace(kind=NodeKind.stream, blk=blk, gnis_id=g)
    graph = SimpleNamespace(nodes={"A:0": n("A", "1"), "B:0": n("B", "1"), "C:0": n("C", "2"), "D:0": n("D", "2"),
                                   "E:0": n("E", "")})
    lim, mouth = S.owner_limited(graph, {"E:0"}, [{"blk": "A", "limit": "split"}, {"blk": "C", "limit": "mouth"}])
    assert lim == {"A", "B", "E"} and mouth == {"C", "D"}


def test_t665_mutation_each_policy_flag_moves_the_answer(monkeypatch):
    monkeypatch.setattr(S, "A_LINE_WHOLLY_INSIDE_THE_POLYGON_IS_TIDAL", False)
    assert _poly_run()[1] == []
    monkeypatch.undo()
    monkeypatch.setattr(S, "A_CREEK_INTO_TIDAL_WATER_IS_CUT_AT_ITS_POLYGON_EXIT", False)
    assert _poly_run()[0] == []
    monkeypatch.undo()
    monkeypatch.setattr(S, "AN_OWNER_MOUTH_ROW_KEEPS_THE_LINE_END", False)
    assert [p.blk for p in _poly_run(mouth_rows={"C"})[0]] == ["C"]
    monkeypatch.undo()
    monkeypatch.setattr(S, "THE_OWNERS_LIMIT_GOVERNS_ITS_OWN_WATER", False)
    assert [p.blk for p in _poly_run(limited={"C"})[0]] == ["C"]


def test_t665_standing_boundaries_skip_the_old_shore_cuts():
    g, idx, cross, _ = _graph()
    pts = place_points(g, S.shore_cuts(g, idx, set(cross), S.share_in(SEA, cross)))
    split_graph_at(g, {}, pts, idx)
    assert "C" not in S.standing_boundaries(g)
