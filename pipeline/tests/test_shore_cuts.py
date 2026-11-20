"""THE SHORE HELPERS the coast cut reads (`atlas.splits.shore`; the cut itself is `test_coast_cut.py`), and the synthetic
FWA world the coast and next-rebuild tests build on: lines built into a graph by the real builder, cut by the real
sectionizer. T678 (2026-11-20) retired the T665 polygon rule (`polygon_rule`, `polygon_inside`, the `HELD_*` lists,
`atlas_tidal`, `inside_tidal.json`) and its tests with it. No data.
"""
from __future__ import annotations

from types import SimpleNamespace

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


def test_the_shore_family_is_minted():
    assert "shore" in AUTO_SOURCES and minted_id("shore:C:300") and minted_id("split:shore:C:300")


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


def test_standing_boundaries_skip_shore_cuts():
    g, geoms, idx, _ = _built()
    sp = SplitPoint(split_id="shore:C:300", blk="C", route_measure=300.0, fid="", label="shore",
                    anchor_type=AnchorType.point, source="shore")
    split_graph_at(g, geoms, place_points(g, [sp]), idx)
    assert "C" not in S.standing_boundaries(g)


def test_the_polygon_rule_is_retired():
    """T678 (2026-11-20): no reader is left for a pre-T678 atlas, so the polygon rule and its sidecar are gone."""
    for gone in ("polygon_rule", "polygon_inside", "atlas_tidal", "reach_tidal", "held_lines", "HELD_FOR_OWNER",
                 "HELD_KEEPS_ITS_CUT", "INSIDE", "read_inside", "write_inside"):
        assert not hasattr(S, gone), gone
