"""Anchor resolution (04): each SplitDef anchor -> SplitPoint(blk, route_measure). Synthetic.

Every anchor resolves from geometry alone (before/independent of the graph). Each test builds a
tiny straight mainstem X on the y=0 axis over measures 0..300 so a resolved measure equals the
x-coordinate of the cut.
"""

from shapely.geometry import LineString, box

from stream_sections import cutting
from stream_sections.anchors import resolve_split_defs
from stream_sections.blk_chains import FidRow, build_blk_chains
from stream_sections.models import AnchorType, SplitAnchor, SplitDef


def _fid(fid, blk, wsc, coords, down_m, up_m, wbk="", gnis_name=""):
    geom = LineString(coords)
    dn, un = cutting.blk_endpoints(geom)
    return FidRow(fid=fid, blk=blk, wsc=wsc, edge_type="1000", wbk=wbk,
                  gnis_id="9" if gnis_name else "", gnis_name=gnis_name,
                  stream_order=1, stream_magnitude=1, down_m=down_m, up_m=up_m,
                  geometry=geom, down_node=dn, up_node=un)


def _mainstem_chain(lake_kind=None):
    fids = [_fid("X1", "X", "100", [(0, 0), (150, 0)], 0, 150, gnis_name="X River"),
            _fid("X2", "X", "100", [(150, 0), (300, 0)], 150, 300, gnis_name="X River")]
    return build_blk_chains(fids, lake_kind or {}), fids


def test_point_anchor_projects_to_measure():
    chains, _ = _mainstem_chain()
    sd = SplitDef(id="p", anchor=SplitAnchor(type=AnchorType.point, coord=(120.0, 0.0)), blk="X")
    pts = resolve_split_defs([sd], chains)
    assert [(p.blk, round(p.route_measure)) for p in pts] == [("X", 120)]


def test_line_anchor_crosses_at_measure():
    chains, _ = _mainstem_chain()
    # a cut line crossing the mainstem at x=210
    sd = SplitDef(id="ln", anchor=SplitAnchor(type=AnchorType.line,
                  coords=((210.0, -50.0), (210.0, 50.0))), blk="X")
    pts = resolve_split_defs([sd], chains)
    assert [(p.blk, round(p.route_measure)) for p in pts] == [("X", 210)]


def test_confluence_anchor_by_blk_uses_tributary_mouth():
    # tributary Y joins X at (200,0); its mouth is coords[0] of its chain.
    fids = [_fid("X1", "X", "100", [(0, 0), (200, 0)], 0, 200, gnis_name="X River"),
            _fid("X2", "X", "100", [(200, 0), (300, 0)], 200, 300, gnis_name="X River"),
            _fid("Y1", "Y", "100-3", [(200, 0), (200, 90)], 0, 90, gnis_name="Y Creek")]
    chains = build_blk_chains(fids, {})
    sd = SplitDef(id="c", label="Y Creek", blk="X",
                  anchor=SplitAnchor(type=AnchorType.confluence, tributary_blk="Y"))
    pts = resolve_split_defs([sd], chains)
    assert [(p.blk, round(p.route_measure)) for p in pts] == [("X", 200)]
    assert pts[0].label == "Y Creek"        # boundary named after the tributary


def test_confluence_anchor_by_wsc_picks_main_channel():
    fids = [_fid("X1", "X", "100", [(0, 0), (300, 0)], 0, 300, gnis_name="X River"),
            _fid("Y1", "Y", "100-3", [(200, 0), (200, 90)], 0, 90, gnis_name="Y Creek")]
    chains = build_blk_chains(fids, {})
    sd = SplitDef(id="c", label="Y Creek", blk="X",
                  anchor=SplitAnchor(type=AnchorType.confluence, tributary_wsc="100-3"))
    pts = resolve_split_defs([sd], chains)
    assert [(p.blk, round(p.route_measure)) for p in pts] == [("X", 200)]


def test_lake_anchor_resolves_to_run_boundary():
    # X threads lake W over [100,200]; the lake anchor resolves to the run boundary (down_m=100).
    fids = [_fid("X1", "X", "100", [(0, 0), (100, 0)], 0, 100, gnis_name="X River"),
            _fid("X2", "X", "100", [(100, 0), (200, 0)], 100, 200, wbk="W", gnis_name="X River"),
            _fid("X3", "X", "100", [(200, 0), (300, 0)], 200, 300, gnis_name="X River")]
    chains = build_blk_chains(fids, {"W": "lake"})
    sd = SplitDef(id="lk", blk="X", anchor=SplitAnchor(type=AnchorType.lake, wbk="W"))
    pts = resolve_split_defs([sd], chains)
    assert [(p.blk, round(p.route_measure)) for p in pts] == [("X", 100)]


def test_mu_boundary_anchor_splits_where_shared_edge_crosses():
    chains, _ = _mainstem_chain()
    # two adjacent MUs sharing the vertical edge x=180; X crosses it there.
    mu_polys = {"2-17": box(-100, -100, 180, 100), "3-15": box(180, -100, 400, 100)}
    sd = SplitDef(id="mu", blk="X",
                  anchor=SplitAnchor(type=AnchorType.mu_boundary, mu_a="2-17", mu_b="3-15"))
    pts = resolve_split_defs([sd], chains, mu_polys=mu_polys)
    assert ("X", 180) in [(p.blk, round(p.route_measure)) for p in pts]


def test_mu_boundary_non_adjacent_makes_no_split():
    chains, _ = _mainstem_chain()
    mu_polys = {"a": box(-100, -100, 100, 100), "b": box(200, -100, 400, 100)}  # gap, not adjacent
    sd = SplitDef(id="mu", blk="X",
                  anchor=SplitAnchor(type=AnchorType.mu_boundary, mu_a="a", mu_b="b"))
    assert resolve_split_defs([sd], chains, mu_polys=mu_polys) == []
