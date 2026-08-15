"""Anchor resolution (04): each SplitDef anchor -> SplitPoint(blk, route_measure). Synthetic.

Every anchor resolves from geometry alone (before/independent of the graph). Each test builds a
tiny straight mainstem X on the y=0 axis over measures 0..300 so a resolved measure equals the
x-coordinate of the cut.
"""

import os

import pytest
from shapely.geometry import LineString, box

from data.data_extractor import FWADataAccessor
from pipeline.graph import cutting
from pipeline.splits.anchors import resolve_split_defs
from pipeline.graph.blk_chains import FidRow, build_blk_chains
from pipeline.models import AnchorType, SplitAnchor, SplitDef

_DATA = "data/bc_fisheries_data.gpkg"
_needs_data = pytest.mark.skipif(not os.path.exists(_DATA), reason="needs data/bc_fisheries_data.gpkg")


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


def test_point_anchor_offset_downstream_shifts_toward_mouth():
    chains, _ = _mainstem_chain()
    # falls at x=120; reg boundary is 30 m DOWNSTREAM (toward the mouth) => measure 90.
    sd = SplitDef(id="p", blk="X", anchor=SplitAnchor(
        type=AnchorType.point, coord=(120.0, 0.0), offset_m=30.0, offset_dir="downstream"))
    pts = resolve_split_defs([sd], chains)
    assert [(p.blk, round(p.route_measure)) for p in pts] == [("X", 90)]
    assert pts[0].concern == ""


def test_point_anchor_offset_upstream_shifts_toward_source():
    chains, _ = _mainstem_chain()
    # falls at x=120; boundary 40 m UPSTREAM (toward the source) => measure 160.
    sd = SplitDef(id="p", blk="X", anchor=SplitAnchor(
        type=AnchorType.point, coord=(120.0, 0.0), offset_m=40.0, offset_dir="upstream"))
    pts = resolve_split_defs([sd], chains)
    assert [(p.blk, round(p.route_measure)) for p in pts] == [("X", 160)]


def test_point_anchor_offset_clamps_at_mouth_with_concern():
    chains, _ = _mainstem_chain()
    # coord at x=20, 100 m downstream would be -80 => clamp to the mouth (measure 0) + concern.
    sd = SplitDef(id="p", blk="X", anchor=SplitAnchor(
        type=AnchorType.point, coord=(20.0, 0.0), offset_m=100.0, offset_dir="downstream"))
    pts = resolve_split_defs([sd], chains)
    assert [(p.blk, round(p.route_measure)) for p in pts] == [("X", 0)]
    assert "clamped at the mouth" in pts[0].concern


def test_offset_validation_rejects_bad_specs():
    # offset needs a valid direction
    with pytest.raises(ValueError):
        SplitAnchor.from_dict({"type": "point", "coord": [0, 0], "offset_m": 50})
    # offset only on point/confluence, not line
    with pytest.raises(ValueError):
        SplitAnchor.from_dict({"type": "line", "coords": [[0, 0], [1, 1]],
                               "offset_m": 50, "offset_dir": "upstream"})
    # negative magnitude is rejected (direction carries the sign)
    with pytest.raises(ValueError):
        SplitAnchor.from_dict({"type": "point", "coord": [0, 0],
                               "offset_m": -50, "offset_dir": "upstream"})
    # a clean offset spec round-trips
    a = SplitAnchor.from_dict({"type": "point", "coord": [0, 0],
                               "offset_m": 100, "offset_dir": "downstream"})
    assert (a.offset_m, a.offset_dir) == (100.0, "downstream")


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
    assert pts[0].concern == ""             # Y wsc 100-3 IS a descendant of X wsc 100 -> no concern


def test_confluence_wsc_mismatch_flags_concern():
    """Tributary WSC not a strict descendant of the parent's -> keep the split but flag a concern
    (the author likely picked the wrong parent/tributary). WSC self-validation, not a hard fail."""
    fids = [_fid("X1", "X", "100", [(0, 0), (300, 0)], 0, 300, gnis_name="X River"),
            _fid("Y1", "Y", "200-3", [(200, 0), (200, 90)], 0, 90, gnis_name="Y Creek")]
    chains = build_blk_chains(fids, {})
    sd = SplitDef(id="c", label="Y", blk="X",
                  anchor=SplitAnchor(type=AnchorType.confluence, tributary_blk="Y"))
    pts = resolve_split_defs([sd], chains)
    assert [(p.blk, round(p.route_measure)) for p in pts] == [("X", 200)]
    assert pts[0].concern            # 200-3 is NOT under 100


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
    # a lake resolves to its two boundaries on the BLK (entry 100, exit 200) — both no-op splits.
    assert sorted(round(p.route_measure) for p in pts) == [100, 200]


def test_mu_boundary_anchor_splits_where_shared_edge_crosses():
    chains, _ = _mainstem_chain()
    # two adjacent MUs sharing the vertical edge x=180; X crosses it there.
    mu_polys = {"2-17": box(-100, -100, 180, 100), "3-15": box(180, -100, 400, 100)}
    sd = SplitDef(id="mu", blk="X",
                  anchor=SplitAnchor(type=AnchorType.mu_boundary, mu_a="2-17", mu_b="3-15"))
    pts = resolve_split_defs([sd], chains, mu_polys=mu_polys)
    assert ("X", 180) in [(p.blk, round(p.route_measure)) for p in pts]


def test_mu_boundary_collapses_multiple_crossings_to_one():
    """A river that WEAVES across a region boundary (the Fraser case) must yield ONE split, not
    one per crossing. The resolver keeps the median crossing and records a concern."""
    coords = [(0, 0), (200, 0), (160, 100), (200, 200)]   # crosses x=180 three times
    ln = LineString(coords)
    fid = _fid("X1", "X", "100", coords, 0, ln.length, gnis_name="X River")
    chains = build_blk_chains([fid], {})
    mu_polys = {"a": box(-100, -100, 180, 300), "b": box(180, -100, 500, 300)}
    sd = SplitDef(id="mu", blk="X", proximity_m=10,
                  anchor=SplitAnchor(type=AnchorType.mu_boundary, mu_a="a", mu_b="b"))
    pts = resolve_split_defs([sd], chains, mu_polys=mu_polys)
    assert len(pts) == 1                     # collapsed to a single split
    assert "3" in pts[0].concern             # flags that it crossed 3x


def test_mu_boundary_non_adjacent_makes_no_split():
    chains, _ = _mainstem_chain()
    mu_polys = {"a": box(-100, -100, 100, 100), "b": box(200, -100, 400, 100)}  # gap, not adjacent
    sd = SplitDef(id="mu", blk="X",
                  anchor=SplitAnchor(type=AnchorType.mu_boundary, mu_a="a", mu_b="b"))
    assert resolve_split_defs([sd], chains, mu_polys=mu_polys) == []


def test_area_boundary_cuts_first_enter_and_last_exit():
    """Transition cutting: a stream crossing a park polygon is cut at its FIRST entry + LAST exit
    only (<=2), never once per boundary touch."""
    coords = [(0, 0), (100, 0), (150, 0), (200, 0), (300, 0)]      # X River along y=0, park spans x100..200
    chains = build_blk_chains([_fid("X1", "X", "100", coords, 0, 300, gnis_name="X River")], {})
    park = box(100, -50, 200, 50)
    sd = SplitDef(id="pk", blk="X",
                  anchor=SplitAnchor(type=AnchorType.area_boundary, area_layer="parks_bc", area_name="PARK"))
    pts = resolve_split_defs([sd], chains, area_polys={"PARK": park})
    assert sorted(round(p.route_measure) for p in pts) == [100, 200]   # enter + exit only


def test_area_boundary_weaving_stream_still_two_cuts():
    """A stream that weaves along the boundary (in/out/in/out) still yields just enter + exit."""
    coords = [(0, 0), (110, 0), (120, 60), (130, 0), (140, 60), (150, 0), (300, 0)]  # dips out at x120,x140
    chains = build_blk_chains([_fid("X1", "X", "100", coords, 0, 400, gnis_name="X River")], {})
    park = box(100, -50, 200, 50)                                  # y>50 (the dips) is outside
    sd = SplitDef(id="pk", blk="X",
                  anchor=SplitAnchor(type=AnchorType.area_boundary, area_layer="parks_bc", area_name="PARK"))
    pts = resolve_split_defs([sd], chains, area_polys={"PARK": park})
    assert len(pts) == 2                                           # first enter + last exit, oscillation absorbed


def test_area_boundary_source_inside_one_cut():
    """A stream that enters the park and runs to its headwater INSIDE gets only the ENTER cut
    (no last-exit, because it never leaves) — the case that breaks a naive 'always two cuts'."""
    coords = [(0, 0), (100, 0), (150, 0), (180, 0)]               # park x100..200; source x180 is inside
    chains = build_blk_chains([_fid("X1", "X", "100", coords, 0, 200, gnis_name="X River")], {})
    park = box(100, -50, 200, 50)
    sd = SplitDef(id="pk", blk="X",
                  anchor=SplitAnchor(type=AnchorType.area_boundary, area_layer="parks_bc", area_name="PARK"))
    pts = resolve_split_defs([sd], chains, area_polys={"PARK": park})
    assert sorted(round(p.route_measure) for p in pts) == [100]   # enter only; headwater stays inside


@_needs_data
def test_real_splits_json_resolves_on_bella_coola_extract():
    """The authored splits.json resolves on real Bella Coola-system data:
    - hunlen_falls (POINT, obstacle-grounded) lands on Hunlen Creek blk 360862431 at m≈1685;
    - young_hwy20 (POINT) lands on Young Creek blk 360862631;
    - burnt_bridge_at_sitkatapa (CONFLUENCE by tributary_wsc) lands on Burnt Bridge blk 360883785
      AND the WSC-descendant self-validation passes (no concern), because the tributary WSC
      910-275583-777225-504013 is a strict descendant of the parent's 910-275583-777225.
    """
    from pipeline.graph.blk_chains import load_stream_fids
    from pipeline.build import bbox_from_gnis, get_lake_wbk_kind
    from pipeline.splits.splits import load_split_defs

    fwa = FWADataAccessor(_DATA)
    defs = load_split_defs("pipeline/splits.json")
    bbox = bbox_from_gnis(fwa, ["Atnarko River", "Hunlen Creek", "Burnt Bridge Creek", "Young Creek"])
    chains = build_blk_chains(load_stream_fids(_DATA, bbox=bbox), get_lake_wbk_kind(fwa, bbox))
    pts = resolve_split_defs(defs, chains)
    blks = {p.blk for p in pts}

    # The tributary carve-outs (now their own by-waterbody cards) land on the right blue lines.
    assert "360862431" in blks   # Hunlen Falls point -> Hunlen Creek
    assert "360862631" in blks   # Hwy 20 point       -> Young Creek

    # Sitkatapa confluence resolves on Burnt Bridge, and the WSC-descendant self-check passes.
    bb = [p for p in pts if p.blk == "360883785"]
    assert bb, "Sitkatapa confluence did not resolve on Burnt Bridge Creek (blk 360883785)"
    assert all("WSC check failed" not in p.concern for p in bb)
