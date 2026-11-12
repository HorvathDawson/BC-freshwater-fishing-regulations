"""T678 / T685: THE COAST CUT — a line entering a tidal river is cut at the river's edge (where it leaves the water
polygon its mouth sits in), a line reaching the sea is fresh to the shoreline; FWA's edge-1450 vertex is a recorded
check, never the decision; a DFO-written boundary governs its own water; a fresh stub under 25 m at the top is tidal;
the Fraser's named channels below its boundary are tidal whole, its sloughs and unnamed lines follow the rule; the
owner's ruling on a single line is applied as written (RULINGS E21-E24).

Synthetic FWA lines built into a graph by the real builder; measures from the ticket where it gives them (356242813:
top 1248.72 m, crossing 1229.34 m). No data.
"""
from __future__ import annotations

from types import SimpleNamespace

import pytest
from shapely.geometry import LineString, Point, box
from shapely.ops import linemerge

from pipeline.atlas.graph import cutting
from pipeline.atlas.graph.blk_chains import FidRow, build_blk_chains
from pipeline.atlas.graph.graph import build_stream_graph
from pipeline.atlas.splits import coast as C
from pipeline.common import fwa_edge_types as ET

CROSS, LAND = ET.WATER_CROSSING, ET.SINGLE_LINE
#: the tidal water: the river X's estuary south of y = 300, and the inlet T crosses (to y = 1229.34)
SEA = box(-200, -1000, 3000, 300).union(box(1400, 0, 1600, 1229.34))


def _fid(fid, blk, wsc, coords, down_m, up_m, et=LAND, name="", gnis=""):
    geom = LineString(coords)
    dn, un = cutting.blk_endpoints(geom)
    return FidRow(fid=fid, blk=blk, wsc=wsc, edge_type=et, wbk="", gnis_id=gnis or ("9" if name else ""),
                  gnis_name=name, stream_order=1, stream_magnitude=1, down_m=down_m, up_m=up_m, geometry=geom,
                  down_node=dn, up_node=un)


def _fids():
    """River X along y = 0. Creek K joins X at x = 500 and crosses the estuary (edge 1450) to y = 300, then runs on
    land to y = 1000. Line T (the ticket's 356242813) joins X at x = 1500: crossing to 1229.34 m, top 1248.72 m."""
    return [
        _fid("X1", "X", "50", [(0, 0), (500, 0)], 0, 500, name="X River", gnis="8"),
        _fid("X2", "X", "50", [(500, 0), (1500, 0)], 500, 1500, name="X River", gnis="8"),
        _fid("X3", "X", "50", [(1500, 0), (2500, 0)], 1500, 2500, name="X River", gnis="8"),
        _fid("K1", "K", "50-1", [(500, 0), (500, 300)], 0, 300, et=CROSS),
        _fid("K2", "K", "50-1", [(500, 300), (500, 1000)], 300, 1000, name="K Creek", gnis="9"),
        _fid("T1", "356242813", "50-2", [(1500, 0), (1500, 1229.34)], 0, 1229.34, et=CROSS),
        _fid("T2", "356242813", "50-2", [(1500, 1229.34), (1500, 1248.72)], 1229.34, 1248.72),
    ]


def _run(extra=(), tidal=("X:0",), target=SEA, held=None, governed=(), sea_mouth=(), fids=None):
    fids = (fids if fids is not None else _fids()) + list(extra)
    g = build_stream_graph(build_blk_chains(fids, {}), fids)
    by_fid = {f.fid: f.geometry for f in fids}
    lines: dict[str, list] = {}
    for f in sorted(fids, key=lambda f: (f.blk, f.down_m)):
        lines.setdefault(f.blk, []).append(f)

    def edge(blk, via):
        """the fake coast: `target` is the sea around a sea line and the river polygon a mouth sits in"""
        fs = lines[blk]
        geom = linemerge([by_fid[f.fid] for f in fs])
        if target is None or target.distance(Point(geom.coords[0])) > C.REACH_MOUTH_M:
            return fs[0].down_m, (C.SEA if via == C.SEA else C.NO_WATER_AREA)
        return C.water_edge(geom, fs[0].down_m, fs[-1].up_m, target), via
    pts, whole, rows = C.coast_rule(g, tidal=set(tidal), e1450=C.edge1450_tops(fids), edge=edge,
                                    sea_mouth=frozenset(sea_mouth), governed=frozenset(governed), held=held)
    return pts, {w["blk"]: w for w in whole}, {r["blk"]: r for r in rows}


def test_edge1450_top_is_the_lowest_run_only():
    rows = [SimpleNamespace(blk="A", edge_type=CROSS, down_m=0, up_m=40), SimpleNamespace(blk="A", edge_type=CROSS, down_m=40, up_m=90),
            SimpleNamespace(blk="A", edge_type=LAND, down_m=90, up_m=200), SimpleNamespace(blk="A", edge_type=CROSS, down_m=200, up_m=300),
            SimpleNamespace(blk="B", edge_type=LAND, down_m=0, up_m=10), SimpleNamespace(blk="B", edge_type=CROSS, down_m=10, up_m=20)]
    assert C.edge1450_tops(rows) == {"A": 90.0}            # B starts on land: no crossing run at its mouth


def test_a_creek_into_tidal_water_is_cut_at_its_coast_crossing():
    pts, whole, rows = _run()
    assert [(p.split_id, p.route_measure, p.source) for p in pts] == [("shore:K:300", 300.0, "shore")]
    assert rows["K"]["below_cut_tidal"] and rows["K"]["edge1450_m"] == 300.0 and abs(rows["K"]["edge_m"] - 300) <= 0.02
    assert rows["K"]["water"] == C.RIVER


def test_a_creek_into_fresh_water_is_not_looked_at():
    pts, whole, rows = _run(tidal=())
    assert pts == [] and whole == {} and rows == {}


def test_a_fresh_stub_under_25_m_at_the_top_is_tidal_whole():
    """356242813 (owner A): crossing 1229.34 m, top 1248.72 m — 19.4 m fresh above: the whole line is tidal."""
    _, whole, rows = _run()
    assert whole["356242813"]["why"] == "stub" and rows["356242813"]["not_cut"] == C.STUB


def test_the_stub_rule_is_load_bearing(monkeypatch):
    monkeypatch.setattr(C, "A_FRESH_STUB_AT_THE_TOP_IS_TIDAL", False)
    pts, whole, _ = _run()
    assert "356242813" not in whole and any(p.blk == "356242813" and int(p.route_measure) == 1229 for p in pts)


def test_the_cut_is_the_waters_edge_not_fwas_vertex_where_they_differ():
    """E21(b): the river's water ends at y = 100 while FWA's edge 1450 runs to 300: the cut is the polygon's edge (100),
    the vertex is recorded beside it"""
    pts, _, rows = _run(target=box(-200, -1000, 3000, 100))
    assert [(p.blk, round(p.route_measure, 1)) for p in pts if p.blk == "K"] == [("K", 100.0)]
    assert rows["K"]["edge1450_m"] == 300.0 and abs(rows["K"]["edge_m"] - 100) <= 0.02 and rows["K"]["below_cut_tidal"]


def test_the_waters_edge_rule_is_load_bearing(monkeypatch):
    monkeypatch.setattr(C, "THE_CUT_IS_THE_RECEIVING_WATERS_EDGE", False)
    pts, _, _ = _run(target=box(-200, -1000, 3000, 100))
    assert [(p.blk, p.route_measure) for p in pts if p.blk == "K"] == [("K", 300.0)]           # FWA's vertex decides


def test_fwas_vertex_on_the_edge_is_the_cut():
    """the same place within SAME_PLACE_M: the cut takes FWA's own vertex (no 30 cm piece between the two)"""
    pts, _, rows = _run(target=box(-200, -1000, 3000, 299.7).union(box(1400, 0, 1600, 1229.34)))
    assert [(p.blk, p.route_measure) for p in pts if p.blk == "K"] == [("K", 300.0)] and abs(rows["K"]["edge_m"] - 299.7) <= 0.02


def test_a_tidal_river_drawn_as_a_single_line_makes_nothing_on_the_creek_tidal():
    """E21(b): the river's edge of a river with no drawn water area is the creek's mouth (the Brunette, X5/X6)"""
    pts, whole, rows = _run(target=None)
    assert pts == [] and whole == {} and rows["K"]["water"] == C.NO_WATER_AREA and rows["K"]["not_cut"] == C.AT_MOUTH


def test_an_edge_within_a_slivers_length_of_the_mouth_is_the_mouth():
    pts, whole, rows = _run(target=box(-200, -1000, 3000, 4.0))
    assert all(p.blk != "K" for p in pts) and rows["K"]["not_cut"] == C.AT_MOUTH and abs(rows["K"]["edge_m"] - 4) <= 0.02


def test_the_owners_ruling_on_a_line_is_applied_as_written():
    pts, _, rows = _run(target=box(-200, -1000, 3000, 100), held={"K": {"keep": "cut", "cut_m": 250.0}})
    assert [(p.blk, p.route_measure) for p in pts if p.blk == "K"] == [("K", 250.0)] and rows["K"]["held"]
    _, whole, _ = _run(held={"K": {"keep": "whole"}})
    assert whole["K"]["why"] == "held"
    pts, whole, rows = _run(held={"K": {"keep": "fresh"}})
    assert all(p.blk != "K" for p in pts) and "K" not in whole and rows["K"]["not_cut"] == C.HELD


def test_a_dfo_written_boundary_governs_its_own_water():
    pts, _, rows = _run(governed={"K"})
    assert all(p.blk != "K" for p in pts) and "K" not in rows


def test_a_line_ending_in_the_sea_is_cut_where_it_comes_ashore():
    """E21(a): S has no outlet and is drawn 800 m over the sea: fresh to the shoreline, the 800 m over the sea tidal
    (Suter Brook Creek, X2)"""
    s = [_fid("S1", "S", "60", [(2800, -500), (2800, 300)], 0, 800, et=CROSS),
         _fid("S2", "S", "60", [(2800, 300), (2800, 900)], 800, 1400, name="S Creek", gnis="12")]
    pts, _, rows = _run(extra=s, sea_mouth={"S"})
    assert ("S", 800.0) in [(p.blk, p.route_measure) for p in pts] and rows["S"]["joins"] == "sea"
    pts, _, rows = _run(extra=s)                       # not in the sea: not looked at
    assert "S" not in rows


def _fraser(fr):
    xs = [0, 500, 600, 700, 1500, 2500]
    return [_fid(f"X{i}", "X", fr, [(a, 0), (b, 0)], a, b, name="Fraser River", gnis="8") for i, (a, b) in enumerate(zip(xs, xs[1:]))]


def test_the_frasers_named_channels_are_tidal_whole_its_sloughs_and_unnamed_lines_are_cut_at_the_rivers_edge():
    """E24: a slough is not a channel, and an unnamed line on the Fraser's code is unclear: both follow rule (b)"""
    fr = C.FRASER_WSC
    base = _fraser(fr)
    extra = [_fid("A1", "A", fr, [(500, 0), (500, 400), (1500, 0)], 0, 900, name="Annacis Channel", gnis="20"),
             _fid("B1", "B", fr, [(600, 0), (600, 700)], 0, 700, name="Crescent Slough", gnis="21"),
             _fid("U1", "U", fr, [(700, 0), (700, 600)], 0, 600)]
    pts, whole, rows = _run(fids=base, extra=extra)
    assert whole["A"]["why"] == "fraser" and rows["A"]["not_cut"] == C.FRASER
    got = {p.blk: round(p.route_measure, 1) for p in pts}
    assert got == {"B": 300.0, "U": 300.0} and "B" not in whole and "U" not in whole


def test_the_fraser_rule_is_load_bearing(monkeypatch):
    fr = C.FRASER_WSC
    base = _fraser(fr)
    extra = [_fid("A1", "A", fr, [(500, 0), (500, 400), (1500, 0)], 0, 900, name="Annacis Channel", gnis="20")]
    monkeypatch.setattr(C, "THE_FRASERS_CHANNELS_BELOW_ITS_BOUNDARY_ARE_TIDAL", False)
    _, whole, _ = _run(fids=base, extra=extra)
    assert "A" not in whole or whole["A"]["why"] != "fraser"


def test_the_coast_rule_is_load_bearing(monkeypatch):
    monkeypatch.setattr(C, "A_LINE_ENTERING_TIDAL_WATER_IS_CUT_AT_ITS_COAST_CROSSING", False)
    pts, whole, _ = _run()
    assert pts == [] and whole == {}


def test_a_creek_into_a_creek_cut_below_its_crossing_follows():
    """J joins K's tidal piece (below K's crossing at 300) at y = 200: J is looked at once K is cut"""
    j = [_fid("J1", "J", "50-1-1", [(500, 200), (900, 300)], 0, 412.31, et=CROSS),
         _fid("J2", "J", "50-1-1", [(900, 300), (900, 900)], 412.31, 1012.31, name="J Creek", gnis="13")]
    base = [f for f in _fids() if f.fid != "K1"] + [_fid("K1a", "K", "50-1", [(500, 0), (500, 200)], 0, 200, et=CROSS),
                                                     _fid("K1b", "K", "50-1", [(500, 200), (500, 300)], 200, 300, et=CROSS)]
    pts, _, rows = _run(fids=base, extra=j)
    got = {p.blk: p.route_measure for p in pts}
    assert got.get("K") == 300.0
    assert "J" in rows and rows["J"]["joins"] == "tidal water" and got.get("J") == 412.31


def test_authority_of_a_split_row():
    def lim(limit, split=""):
        return SimpleNamespace(limit=limit, split=split)
    table = {"a": "dfo", "b": "none", "o": "owner"}
    assert C.authority(lim("split", "a"), table) == "dfo"
    assert C.authority(lim("split", "b"), table) == "none"
    assert C.authority(lim("split", "c"), table) == "unclassified"
    assert C.authority(lim("split", "o"), table) == "owner"
    assert C.authority(lim("whole"), table) == "dfo" and C.authority(lim("mouth"), table) == "none"
    rows = [lim("split", "a"), lim("split", "b"), lim("split", "c"), lim("split", "o")]
    # owner 2026-11-12: a split the owner made his own decision governs its water like a DFO-written one
    assert [r.split for r in C.rank1(rows, table)] == ["a", "c", "o"]
    for r in rows:
        r.blk, r.name = "1", ""
    assert [r["split"] for r in C.unclassified(rows, table)] == ["b", "c"]


def test_the_authority_file_names_every_splits_authority(tmp_path):
    """a row with no known authority is refused, never read as no authority"""
    p = tmp_path / "a.json"
    p.write_text('{"splits": [{"split": "x", "authority": "owner"}, {"split": "y", "authority": "dfo"}]}')
    assert C.load_authority(p) == {"x": "owner", "y": "dfo"}
    p.write_text('{"splits": [{"split": "x", "dfo_written": true}]}')
    with pytest.raises(SystemExit, match="authority"):
        C.load_authority(p)


def test_the_waters_edge_is_found_exactly_and_is_the_mouth_when_the_line_does_not_end_in_the_water():
    line = LineString([(0, 0), (0, 2000)])
    assert C.water_edge(line, 0.0, 2000.0, box(-50, -50, 50, 1234.56)) == pytest.approx(1234.56, abs=0.02)
    assert C.water_edge(line, 100.0, 2100.0, box(-50, -50, 50, 1234.56)) == pytest.approx(1334.56, abs=0.02)   # route measures
    assert C.water_edge(line, 0.0, 2000.0, box(-50, -50, 50, 5000)) == 2000.0                                 # wholly in it
    assert C.water_edge(LineString([(0, 1000), (0, 2000)]), 0.0, 1000.0, SEA) == 0.0                           # ends away from it
    assert C.water_edge(line, 0.0, 2000.0, None) == 0.0


def _coast_geometry(rivers, keys, land=()):
    """a `CoastGeometry` over given polygons (no file)"""
    import shapely
    cg = object.__new__(C.CoastGeometry)
    cg.land, cg.rivers, cg.river_blk = list(land), list(rivers), list(keys)
    cg.land_tree, cg.river_tree = shapely.STRtree(cg.land), shapely.STRtree(cg.rivers)
    return cg


def test_the_river_polygon_is_chosen_by_where_the_mouth_is_never_by_a_key():
    """E22, the flaw: the Nass's water carries another blue line's key than the tidal line through it, so a choice by
    key found no water and read three creeks fresh. The polygon the creek's mouth sits in is the river's water."""
    nass = box(0, -900, 5000, 900)
    cg = _coast_geometry([nass, box(9000, 0, 9500, 500)], ["a key no tidal line carries", "another"])
    creek = LineString([(2500, 0), (2500, 2300)])                    # mouth on the centre line, 900 m of it across the water
    m, water = cg.edge(creek, 0.0, 2300.0, C.RIVER)
    assert water == C.RIVER and m == pytest.approx(900.0, abs=0.02)
    m, water = cg.edge(LineString([(7000, 0), (7000, 500)]), 0.0, 500.0, C.RIVER)     # joins where no water area is drawn
    assert (m, water) == (0.0, C.NO_WATER_AREA)


def test_a_creeks_own_drawn_water_is_never_the_water_it_enters():
    """A double-line creek's own polygon reaches its mouth where it meets a single-line tidal river: that polygon is the
    creek, so nothing is found to be tidal (no water area drawn for the river there). Without `own` it would be read as
    the river's water and the creek's whole double-line stretch would go tidal (the near-miss)."""
    cg = _coast_geometry([box(-20, 0, 20, 800)], ["creek"])
    creek = LineString([(0, 0), (0, 1500)])
    assert cg.edge(creek, 0.0, 1500.0, C.RIVER, own="creek") == (0.0, C.NO_WATER_AREA)
    m, water = cg.edge(creek, 0.0, 1500.0, C.RIVER)
    assert water == C.RIVER and m == pytest.approx(800.0, abs=0.02)


def test_a_mouth_on_the_seam_of_two_polygons_of_one_river_takes_both():
    cg = _coast_geometry([box(0, -900, 2500, 900), box(2500, -900, 5000, 900)], ["1", "2"])
    m, _ = cg.edge(LineString([(2500, 0), (2600, 400), (2600, 2300)]), 0.0, 2312.31, C.RIVER)
    assert m == pytest.approx(912.31, abs=0.05)


def test_the_sea_is_what_lies_outside_the_land():
    """E21(a): a line drawn 130 m past the shoreline is tidal for those 130 m; one ending on the coastline for none"""
    cg = _coast_geometry([], [], land=[box(0, 0, 1000, 1000)])
    m, water = cg.edge(LineString([(500, -130), (500, 600)]), 0.0, 730.0, C.SEA)
    assert water == C.SEA and m == pytest.approx(130.0, abs=0.02)
    m, _ = cg.edge(LineString([(500, 0), (500, 600)]), 0.0, 600.0, C.SEA)
    assert m <= 0.02


# ------------------------------------------------------------------------------------------------ the one record
def _graph():
    fids = _fids()
    return build_stream_graph(build_blk_chains(fids, {}), fids)


def test_coverage_is_the_water_not_the_cut():
    """the coast cut is decided before the length cap: the same water cut differently covers the same, other water not"""
    from pipeline.atlas.splits.sectionizer import place_points, split_graph_at
    from pipeline.common.models import AnchorType, SplitPoint
    g = _graph()
    before = C.water_cover(g, {"X:0"})
    fids = _fids()
    g2 = build_stream_graph(build_blk_chains(fids, {}), fids)
    geoms = {nid: linemerge([f.geometry for f in fids if f.fid in n.member_fids]) for nid, n in g2.nodes.items()}
    idx = {f.fid: (f.down_m, f.up_m, f.stream_order, f.stream_magnitude) for f in fids}
    pts = place_points(g2, [SplitPoint(split_id="len:X:900", blk="X", route_measure=900.0, fid="", label="l",
                                       anchor_type=AnchorType.point, source="length")])
    split_graph_at(g2, geoms, pts, idx)
    xs = {nid for nid, n in g2.nodes.items() if n.blk == "X"}
    assert len(xs) == 2 and C.water_cover(g2, xs) == before
    assert C.water_cover(g2, {min(xs)}) != before                       # half the water is not the water


def test_the_record_round_trips_and_a_missing_one_is_refused(tmp_path):
    C.write_record(tmp_path, {"K:0": "crossing:K:0", "X:0": "split:x"}, {"356242813": "stub"})
    assert C.read_record(tmp_path) == {"K:0": "crossing:K:0", "X:0": "split:x"}
    with pytest.raises(SystemExit):
        C.read_record(tmp_path / "nope")


def test_the_reach_reads_the_record_and_adds_only_the_books_tidal_rows():
    from pipeline.atlas.reach import outside as O
    reg = {"wbk:1": SimpleNamespace(section_ids=("L:0",))}
    rec = {"K:0": "crossing:K:0", "X:0": "split:x"}
    got = O.tidal_water([{"entry_id": "e1", "tidal": True, "matched": ["wbk:1"]}], reg, None, record=rec)
    assert got == {"L:0": "e1", "K:0": "crossing:K:0", "X:0": "split:x"}
    with pytest.raises(SystemExit):                                     # never both: the record or the old inputs
        O.tidal_water([], reg, None, ("x",), record=rec)


def test_the_held_file_refuses_a_cut_without_a_measure(tmp_path):
    import json
    from pipeline.atlas.splits import coast_held as H
    p = tmp_path / "held.json"
    p.write_text(json.dumps({"lines": [{"blk": "K", "keep": "cut"}]}))
    with pytest.raises(SystemExit):
        H.load(p)
    p.write_text(json.dumps({"lines": [{"blk": "K", "keep": "cut", "cut_m": 250.0}]}))
    with pytest.raises(SystemExit, match="decided"):       # T685: every row is a dated owner ruling, never "today's result"
        H.load(p)
    ok = {"decided": "2026-11-12", "why": "owner: ..."}
    p.write_text(json.dumps({"lines": [{"blk": "K", "keep": "cut", "cut_m": 250.0, **ok}, {"blk": "T", "keep": "whole", **ok}]}))
    assert sorted(H.load(p)) == ["K", "T"]
    with pytest.raises(SystemExit):
        H.load(tmp_path / "missing.json")


class FakeCoast:
    """`coast.CoastGeometry` over one polygon of sea (no file): what a stage test hands `build.cut_shores`."""

    def __init__(self, sea):
        self.sea = sea

    def in_sea(self, points, reach=C.REACH_M):
        return [self.sea.distance(p) <= reach for p in points]

    def edge(self, geometry, mouth_m, top_m, via, own=""):
        return C.water_edge(geometry, mouth_m, top_m, self.sea), via


def patch_stage(monkeypatch, sea, held=None):
    """`build.cut_shores` on a synthetic graph: the coast is `sea`, the held file `held` (default: none)."""
    from pipeline.atlas.splits import coast_held as H
    monkeypatch.setattr(C, "CoastGeometry", lambda gpkg, bbox=None: FakeCoast(sea))
    monkeypatch.setattr(H, "load", lambda path: dict(held or {}))


def test_the_atlas_stage_cuts_and_records_by_the_coast_rule(tmp_path, monkeypatch):
    """`build.cut_shores` end to end: X ends in the sea, crossing (edge 1450) to 100 m: cut there, the piece below
    tidal; the record names the coast rule and the owner record is written."""
    import json
    from pipeline.atlas import build as B
    fids = [f for f in _fids() if f.fid != "X1"] + [_fid("X0", "X", "50", [(0, 0), (100, 0)], 0, 100, et=CROSS),
                                                     _fid("X1", "X", "50", [(100, 0), (500, 0)], 100, 500, name="X River", gnis="8")]
    g = build_stream_graph(build_blk_chains(fids, {}), fids)
    by_fid = {f.fid: f.geometry for f in fids}
    geoms = {nid: linemerge([by_fid[f] for f in n.member_fids]) for nid, n in g.nodes.items()}
    idx = {f.fid: (f.down_m, f.up_m, f.stream_order, f.stream_magnitude) for f in fids}
    patch_stage(monkeypatch, box(-500, -500, 100, 500))
    b = SimpleNamespace(args=SimpleNamespace(gpkg="unused.gpkg", out=str(tmp_path)), graph=g, geoms=geoms,
                        fid_index=idx, applied_splits=[], fids=fids, chains=build_blk_chains(fids, {}), bbox=None,
                        tidal_limits=[], tidal_ids={}, tick=lambda label: None)
    B.cut_shores(b)
    assert [p.split_id for p in b.applied_splits] == ["shore:X:100"] and b.coast_ok_cuts == {"X": 100.0}
    rec = json.loads((tmp_path / "shore_cuts.json").read_text())
    assert rec["rule"] == C.RULE and rec["cuts"] == 1 and rec["runs"][0]["water"] == C.SEA
    assert json.loads((tmp_path / C.OWNER_RECORD).read_text())["lines"] == []
    assert C.built_by_coast_rule(tmp_path)


def test_the_stage_reads_the_edge_on_the_graphs_own_line_never_the_merged_chain(tmp_path, monkeypatch):
    """T685 build 2026-11-12: Power House Creek's key carries Burnaby's added pieces (two tributary branches), so its
    merged `BlkChain` geometry is not one mouth-to-top line and the edge read on it landed 61 m low. The stage reads
    the graph's own sections: a chain list made of nonsense geometry changes nothing."""
    from shapely.geometry import LineString as LS
    from pipeline.atlas import build as B
    fids = [f for f in _fids() if f.fid != "X1"] + [_fid("X0", "X", "50", [(0, 0), (100, 0)], 0, 100, et=CROSS),
                                                     _fid("X1", "X", "50", [(100, 0), (500, 0)], 100, 500, name="X River", gnis="8")]
    g = build_stream_graph(build_blk_chains(fids, {}), fids)
    by_fid = {f.fid: f.geometry for f in fids}
    geoms = {nid: linemerge([by_fid[f] for f in n.member_fids]) for nid, n in g.nodes.items()}
    idx = {f.fid: (f.down_m, f.up_m, f.stream_order, f.stream_magnitude) for f in fids}
    patch_stage(monkeypatch, box(-500, -500, 100, 500))
    bad = [SimpleNamespace(**{**vars(c), "geometry": LS([(400, 400), (900, 900)])}) for c in build_blk_chains(fids, {})]
    b = SimpleNamespace(args=SimpleNamespace(gpkg="unused.gpkg", out=str(tmp_path)), graph=g, geoms=geoms,
                        fid_index=idx, applied_splits=[], fids=fids, chains=bad, bbox=None,
                        tidal_limits=[], tidal_ids={}, tick=lambda label: None)
    B.cut_shores(b)
    assert [p.split_id for p in b.applied_splits] == ["shore:X:100"]


def test_an_atlas_cut_by_the_first_coast_rule_still_carries_the_record(tmp_path):
    """the live atlas until the T685 build is promoted: `coast/1` is read through the record too"""
    import json
    (tmp_path / "shore_cuts.json").write_text(json.dumps({"rule": "coast/1", "runs": []}))
    assert C.built_by_coast_rule(tmp_path)
    (tmp_path / "shore_cuts.json").write_text(json.dumps({"rule": "polygon/1", "runs": []}))
    assert not C.built_by_coast_rule(tmp_path)


def test_the_tiles_read_the_record_of_a_coast_cut_atlas(tmp_path):
    import json
    from pipeline.deliver.tiles.tidal import tidal_pieces
    (tmp_path / "shore_cuts.json").write_text(json.dumps({"rule": C.RULE, "runs": []}))
    C.write_record(tmp_path, {"K:0": "crossing:K:0", "X:0": "split:x"}, {})
    assert tidal_pieces(tmp_path) == frozenset({"K:0", "X:0"})
    (tmp_path / C.TIDAL_RECORD).unlink()
    with pytest.raises(SystemExit):                       # a coast-cut atlas without its record is refused
        tidal_pieces(tmp_path)
