"""T678: THE COAST CUT — a line entering the sea or a tidal river is cut at its coast crossing (the top of its lowest
edge-1450 run, cross-checked with the geometric crossing); a DFO-written boundary governs its own water; a fresh stub
under 25 m at the top is tidal; the Fraser's named channels below its boundary are tidal whole, its sloughs and
unnamed lines are the owner's; a line the two methods disagree on is the owner's and keeps today's result.

Synthetic FWA lines built into a graph by the real builder; measures from the ticket where it gives them (356242813:
top 1248.72 m, crossing 1229.34 m). No data.
"""
from __future__ import annotations

from types import SimpleNamespace

import pytest
from shapely.geometry import LineString, box
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

    def geo(blk):
        fs = lines[blk]
        return C.geometric_crossing(linemerge([by_fid[f.fid] for f in fs]), fs[0].down_m, fs[-1].up_m, target, step=1.0)
    pts, whole, rows, owner = C.coast_rule(g, tidal=set(tidal), e1450=C.edge1450_tops(fids), geo=geo,
                                           sea_mouth=frozenset(sea_mouth), governed=frozenset(governed), held=held)
    return pts, {w["blk"]: w for w in whole}, {r["blk"]: r for r in rows}, {r["blk"]: r for r in owner}


def test_edge1450_top_is_the_lowest_run_only():
    rows = [SimpleNamespace(blk="A", edge_type=CROSS, down_m=0, up_m=40), SimpleNamespace(blk="A", edge_type=CROSS, down_m=40, up_m=90),
            SimpleNamespace(blk="A", edge_type=LAND, down_m=90, up_m=200), SimpleNamespace(blk="A", edge_type=CROSS, down_m=200, up_m=300),
            SimpleNamespace(blk="B", edge_type=LAND, down_m=0, up_m=10), SimpleNamespace(blk="B", edge_type=CROSS, down_m=10, up_m=20)]
    assert C.edge1450_tops(rows) == {"A": 90.0}            # B starts on land: no crossing run at its mouth


def test_a_creek_into_tidal_water_is_cut_at_its_coast_crossing():
    pts, whole, rows, owner = _run()
    assert [(p.split_id, p.route_measure, p.source) for p in pts] == [("shore:K:300", 300.0, "shore")]
    assert rows["K"]["below_cut_tidal"] and rows["K"]["edge1450_m"] == 300.0 and abs(rows["K"]["geometric_m"] - 300) <= 1
    assert owner == {}


def test_a_creek_into_fresh_water_is_not_looked_at():
    pts, whole, rows, owner = _run(tidal=())
    assert pts == [] and whole == {} and rows == {} and owner == {}


def test_a_fresh_stub_under_25_m_at_the_top_is_tidal_whole():
    """356242813 (owner A): crossing 1229.34 m, top 1248.72 m — 19.4 m fresh above: the whole line is tidal."""
    _, whole, rows, _ = _run()
    assert whole["356242813"]["why"] == "stub" and rows["356242813"]["not_cut"] == C.STUB


def test_the_stub_rule_is_load_bearing(monkeypatch):
    monkeypatch.setattr(C, "A_FRESH_STUB_AT_THE_TOP_IS_TIDAL", False)
    pts, whole, _, _ = _run()
    assert "356242813" not in whole and any(p.blk == "356242813" and int(p.route_measure) == 1229 for p in pts)


def test_methods_that_disagree_make_the_line_the_owners_and_the_build_refuses_it_unheld():
    """the geometric water ends at y = 100 while edge 1450 runs to 300: 200 m apart, over AGREE_M"""
    pts, _, rows, owner = _run(target=box(-200, -1000, 3000, 100))
    assert "K" not in rows and owner["K"]["why"] == C.OWNER_DISAGREE and all(p.blk != "K" for p in pts)
    assert "K" in [r["blk"] for r in C.missing_held(owner.values(), {})]
    assert "K" not in [r["blk"] for r in C.missing_held(owner.values(), {"K": {"keep": "fresh"}})]


def test_a_held_line_keeps_todays_result_whatever_the_method_says():
    pts, _, rows, owner = _run(target=box(-200, -1000, 3000, 100), held={"K": {"keep": "cut", "cut_m": 250.0}})
    assert [(p.blk, p.route_measure) for p in pts] == [("K", 250.0)] and "K" not in owner and rows["K"]["held"]
    _, whole, _, _ = _run(held={"K": {"keep": "whole"}})
    assert whole["K"]["why"] == "held"
    pts, whole, rows, _ = _run(held={"K": {"keep": "fresh"}})
    assert all(p.blk != "K" for p in pts) and "K" not in whole and rows["K"]["not_cut"] == C.HELD


def test_a_dfo_written_boundary_governs_its_own_water():
    pts, _, rows, _ = _run(governed={"K"})
    assert all(p.blk != "K" for p in pts) and "K" not in rows


def test_a_line_ending_in_the_sea_is_cut_where_it_comes_ashore():
    """S has no outlet: its mouth lies in the sea, crossing 0..150 then land"""
    s = [_fid("S1", "S", "60", [(2800, -500), (2800, 300)], 0, 800, et=CROSS),
         _fid("S2", "S", "60", [(2800, 300), (2800, 900)], 800, 1400, name="S Creek", gnis="12")]
    pts, _, rows, _ = _run(extra=s, sea_mouth={"S"})
    assert ("S", 800.0) in [(p.blk, p.route_measure) for p in pts] and rows["S"]["joins"] == "sea"
    pts, _, rows, _ = _run(extra=s)                       # not in the sea: not looked at
    assert "S" not in rows


def _fraser(fr):
    xs = [0, 500, 600, 700, 1500, 2500]
    return [_fid(f"X{i}", "X", fr, [(a, 0), (b, 0)], a, b, name="Fraser River", gnis="8") for i, (a, b) in enumerate(zip(xs, xs[1:]))]


def test_the_frasers_named_channels_are_tidal_whole_its_sloughs_and_unnamed_lines_are_the_owners():
    fr = C.FRASER_WSC
    base = _fraser(fr)
    extra = [_fid("A1", "A", fr, [(500, 0), (500, 400), (1500, 0)], 0, 900, name="Annacis Channel", gnis="20"),
             _fid("B1", "B", fr, [(600, 0), (600, -400)], 0, 400, name="Ewen Slough", gnis="21"),
             _fid("U1", "U", fr, [(700, 0), (700, -300)], 0, 300)]
    _, whole, rows, owner = _run(fids=base, extra=extra)
    assert whole["A"]["why"] == "fraser" and rows["A"]["not_cut"] == C.FRASER
    assert owner["B"]["why"] == C.OWNER_SLOUGH and owner["U"]["why"] == C.OWNER_UNNAMED_FRASER


def test_the_fraser_rule_is_load_bearing(monkeypatch):
    fr = C.FRASER_WSC
    base = _fraser(fr)
    extra = [_fid("A1", "A", fr, [(500, 0), (500, 400), (1500, 0)], 0, 900, name="Annacis Channel", gnis="20")]
    monkeypatch.setattr(C, "THE_FRASERS_CHANNELS_BELOW_ITS_BOUNDARY_ARE_TIDAL", False)
    _, whole, _, _ = _run(fids=base, extra=extra)
    assert "A" not in whole or whole["A"]["why"] != "fraser"


def test_the_coast_rule_is_load_bearing(monkeypatch):
    monkeypatch.setattr(C, "A_LINE_ENTERING_TIDAL_WATER_IS_CUT_AT_ITS_COAST_CROSSING", False)
    pts, whole, _, _ = _run()
    assert pts == [] and whole == {}


def test_a_creek_into_a_creek_cut_below_its_crossing_follows():
    """J joins K's tidal piece (below K's crossing at 300) at y = 200: J is looked at once K is cut"""
    j = [_fid("J1", "J", "50-1-1", [(500, 200), (900, 300)], 0, 412.31, et=CROSS),
         _fid("J2", "J", "50-1-1", [(900, 300), (900, 900)], 412.31, 1012.31, name="J Creek", gnis="13")]
    base = [f for f in _fids() if f.fid != "K1"] + [_fid("K1a", "K", "50-1", [(500, 0), (500, 200)], 0, 200, et=CROSS),
                                                     _fid("K1b", "K", "50-1", [(500, 200), (500, 300)], 200, 300, et=CROSS)]
    pts, _, rows, _ = _run(fids=base, extra=j)
    got = {p.blk: p.route_measure for p in pts}
    assert got.get("K") == 300.0
    assert "J" in rows and rows["J"]["joins"] == "tidal water" and got.get("J") == 412.31


def test_authority_of_a_split_row():
    def lim(limit, **extra):
        return SimpleNamespace(limit=limit, extra=extra)
    assert C.authority(lim("split", source="dfo")) == "dfo"
    assert C.authority(lim("split", source="owner")) == "none"
    assert C.authority(lim("split")) == "unclassified"
    assert C.authority(lim("whole")) == "dfo" and C.authority(lim("mouth")) == "none"


def test_geometric_crossing_fails_when_the_line_ends_away_from_tidal_water():
    m, gap = C.geometric_crossing(LineString([(0, 1000), (0, 2000)]), 0.0, 1000.0, SEA)
    assert m is None and gap == pytest.approx(700.0)
