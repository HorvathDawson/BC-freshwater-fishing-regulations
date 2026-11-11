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
