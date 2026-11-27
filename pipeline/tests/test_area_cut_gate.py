"""The area-cut gate (T1199): each closure area's built stream pieces against its polygon — asserts, never repairs.

G1 a member with more than floor - band of line beyond the band; G2 an open piece with line deeper than 5 m inside;
G3 a piece under the floor an area cut made; G4 a non-member partly inside, or a member the geometry does not
confirm; G5 a run of member pieces with no real inside water. Synthetic pieces and polygons, no gpkg.
"""
from __future__ import annotations

import json

import pytest
from shapely.geometry import LineString, box

from pipeline.atlas.splits import area_cut_gate as G
from pipeline.common.models import BoundaryKind, NodeKind, SectionBoundary, StreamGraph, StreamNode

RESERVE = box(1000, 0, 2000, 1000)          # west edge x = 1000
AID, NAME = "area:ecological_reserves:r", "R"
CUT = SectionBoundary(f"split:area:{NAME}", BoundaryKind.area, None, NAME, source="area")
OTHER = SectionBoundary("split:some_curated_cut", BoundaryKind.split, None, "", source="curated")


def _graph(*pieces):
    """pieces: (node id, x0, x1, in the area?, lower bound, upper bound) along y = 500."""
    g, geoms = StreamGraph(), {}
    for nid, x0, x1, member, lo, hi in pieces:
        g.nodes[nid] = StreamNode(node_id=nid, kind=NodeKind.stream, blk="X", down_m=x0, up_m=x1, length_m=x1 - x0,
                                  in_areas=(AID,) if member else (), lower_bound=lo, upper_bound=hi)
        geoms[nid] = LineString([(x0, 500), (x1, 500)])
    return g, geoms


def _run(g, geoms, tmp_path=None):
    return G.check(g, geoms, [(AID, NAME, RESERVE)], tmp_path)


def test_a_clean_cut_passes_and_writes_its_numbers(tmp_path):
    """Out to 1000, in from there (the cut ON the edge): every check passes, and the file says so."""
    g, geoms = _graph(("X:0", 0, 1000, False, None, CUT), ("X:1000", 1000, 1600, True, CUT, None))
    res = _run(g, geoms, tmp_path)
    assert res["counts"] == {"G1": 0, "G2": 0, "G3": 0, "G4": 0, "G5": 0}
    got = json.loads((tmp_path / G.FILENAME).read_text())
    assert got["worst"]["shortest_area_piece_m"] == 600.0 and got["allowed_beyond_band_m"] == 50.0


def test_g1_a_member_may_hold_at_most_floor_minus_band_beyond_the_band(tmp_path):
    """The member starts 140 m outside the edge: 90 m of it lie beyond the 50 m band (the 1,000 m rejoin rule's
    shape). 95 m out leaves 45 m beyond: allowed (the floor grew a short piece that far)."""
    g, geoms = _graph(("X:0", 0, 860, False, None, CUT), ("X:860", 860, 1600, True, CUT, None))
    with pytest.raises(SystemExit, match="G1 1"):
        _run(g, geoms, tmp_path)
    g, geoms = _graph(("X:0", 0, 905, False, None, CUT), ("X:905", 905, 1600, True, CUT, None))
    assert _run(g, geoms)["counts"]["G1"] == 0


def test_g2_an_open_piece_deeper_than_5m_inside_stops_the_build():
    g, geoms = _graph(("X:0", 0, 1000, False, None, CUT), ("X:1000", 1000, 1600, False, CUT, None))
    with pytest.raises(SystemExit, match="G2 1"):
        _run(g, geoms)


def test_g3_a_piece_under_the_floor_an_area_cut_made_stops_the_build():
    """A 60 m piece between the area's cut and a curated cut. The same piece whose bounds are both another
    cutter's (the area only an alias there) is not the area's."""
    g, geoms = _graph(("X:0", 0, 940, False, None, OTHER), ("X:940", 940, 1000, False, OTHER, CUT),
                      ("X:1000", 1000, 1600, True, CUT, None))
    with pytest.raises(SystemExit, match="G3 1"):
        _run(g, geoms)
    g, geoms = _graph(("X:0", 0, 940, False, None, OTHER), ("X:940", 940, 1000, False, OTHER, OTHER),
                      ("X:1000", 1000, 1600, True, OTHER, None))
    assert _run(g, geoms)["counts"]["G3"] == 0
    assert G.FLOOR_SLACK_M == 1.0
    g, geoms = _graph(("X:0", 0, 900.5, False, None, CUT), ("X:900.5", 900.5, 1000, True, CUT, OTHER),
                      ("X:1000", 1000, 1600, True, OTHER, None))
    assert _run(g, geoms)["counts"]["G3"] == 0, "99.5 m: at the floor (route measure against geometry)"


def test_g4_a_non_member_partly_inside_or_a_member_far_from_the_area():
    """A 60 m open piece with 6 m inside (10 %, a run over 5 m) is not wholly out. A member 500 m beyond the edge
    is not confirmed by the geometry."""
    g, geoms = _graph(("X:0", 0, 946, False, None, OTHER), ("X:946", 946, 1006, False, OTHER, CUT),
                      ("X:1006", 1006, 1600, True, CUT, None))
    res = G.measure(g, geoms, [(AID, NAME, RESERVE)])
    assert res["counts"]["G4"] == 1 and res["checks"]["G4"][0]["node"] == "X:946"
    g, geoms = _graph(("X:0", 0, 400, True, None, OTHER), ("X:400", 400, 1000, False, OTHER, CUT),
                      ("X:1000", 1000, 1600, True, CUT, None))
    res = G.measure(g, geoms, [(AID, NAME, RESERVE)])
    assert [r["why"] for r in res["checks"]["G4"]] == ["member with no line within the band"]


def test_g4_a_dip_under_5m_is_positional_error():
    """An open piece reaching 4 m in: under the dip, so neither G2 nor G4."""
    g, geoms = _graph(("X:0", 0, 1004, False, None, None))
    assert G.measure(g, geoms, [(AID, NAME, RESERVE)])["counts"] == {"G1": 0, "G2": 0, "G3": 0, "G4": 0, "G5": 0}


def test_g5_a_member_run_with_no_real_inside_water_stops_the_build():
    """Review of T1199: a creek beside the edge, 20 m out, that touches the polygon by a 2 m graze is a line that never
    crosses. Made inside (the band joined from the graze), its member run holds no inside run of 5 m: G5. The same
    run reaching 6 m in passes."""
    def graph_along(x_in):
        g, geoms = StreamGraph(), {}
        line = LineString([(980, 100), (980, 500), (x_in, 501), (980, 502), (980, 1600)])
        for nid, lo, hi, geom in (("Y:0", 0, 401, LineString(list(line.coords)[:3])),
                                  ("Y:401", 401, 1500, LineString(list(line.coords)[2:]))):
            g.nodes[nid] = StreamNode(node_id=nid, kind=NodeKind.stream, blk="Y", down_m=lo, up_m=hi,
                                      length_m=hi - lo, in_areas=(AID,), lower_bound=CUT if lo else None)
            geoms[nid] = geom
        return g, geoms
    res = G.measure(*graph_along(1001.0), [(AID, NAME, RESERVE)])
    assert res["counts"]["G5"] == 1 and res["checks"]["G5"][0]["from_m"] == 0
    assert G.measure(*graph_along(1003.0), [(AID, NAME, RESERVE)])["counts"]["G5"] == 0


def test_the_gate_reads_the_cut_rules_numbers():
    from pipeline.atlas.splits import clean_cut as CC
    from pipeline.common import tolerances as T
    assert G.DEEP_M == T.SLIVER_M and G.SHARE == 0.05
    res = G.measure(StreamGraph(), {}, [(AID, NAME, RESERVE)])
    assert (res["edge_m"], res["min_piece_m"]) == (CC.EDGE_M, CC.MIN_PIECE_M)


def test_closure_areas_are_the_clean_defs_polygons():
    defs = [{"id": "ecological_reserves", "cut": "clean"}, {"id": "regions", "kind": "region", "cut": "first_last",
                                                            "membership": "both_sides"},
            {"id": "provincial_parks", "kind": "park", "cut": False}]
    polys = {"ecological_reserves": {"R": RESERVE}, "regions": {"7B": RESERVE}, "provincial_parks": {"P": RESERVE}}
    assert G.closure_areas(defs, polys) == [("area:ecological_reserves:r", "R", RESERVE)]
