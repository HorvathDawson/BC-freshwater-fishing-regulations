"""Blanket area-split resolution (pipeline/atlas/splits/area_splits.py) — synthetic, no FWA."""

from shapely.geometry import LineString, box

from pipeline.atlas.fwa import FWADataAccessor  # noqa: F401 (kept parallel to other suites)
from pipeline.atlas.graph import cutting
from pipeline.atlas.graph.blk_chains import FidRow, build_blk_chains
from pipeline.atlas.splits.area_splits import resolve_area_splits


def _fid(fid, blk, wsc, coords, down_m, up_m, gnis_name=""):
    geom = LineString(coords)
    dn, un = cutting.blk_endpoints(geom)
    return FidRow(fid=fid, blk=blk, wsc=wsc, edge_type="1000", wbk="",
                  gnis_id="9" if gnis_name else "", gnis_name=gnis_name,
                  stream_order=1, stream_magnitude=1, down_m=down_m, up_m=up_m,
                  geometry=geom, down_node=dn, up_node=un)


def test_resolve_area_splits_cuts_all_intersecting_streams():
    # two separate streams both crossing the park box (x 100..200); a third fully outside
    c1 = _fid("X1", "X", "100", [(0, 0), (100, 0), (150, 0), (300, 0)], 0, 300, gnis_name="X River")
    c2 = _fid("Y1", "Y", "200", [(0, 100), (100, 100), (150, 100), (300, 100)], 0, 300, gnis_name="Y River")
    c3 = _fid("Z1", "Z", "300", [(0, 999), (300, 999)], 0, 300, gnis_name="Z River")  # never enters
    chains = build_blk_chains([c1, c2, c3], {})
    park = box(100, -50, 200, 150)
    pts = resolve_area_splits({"PARK": park}, chains)

    by_blk: dict[str, list[int]] = {}
    for p in pts:
        by_blk.setdefault(p.blk, []).append(round(p.route_measure))
        assert p.split_id == "area:PARK" and p.label == "PARK"
    assert sorted(by_blk["X"]) == [100, 200]     # enter + exit
    assert sorted(by_blk["Y"]) == [100, 200]
    assert "Z" not in by_blk                      # outside stream not cut


def test_apply_remap_moves_units_between_dissolve_groups():
    """Haida Gwaii is administered from Region 1 but the source layer still says 6.

    The remap is keyed on the MU id and rewrites the region id, so the dissolve puts 6-12/6-13
    into Region 1. Every other unit is untouched.
    """
    import pandas as pd
    from pipeline.atlas.splits.area_splits import apply_remap

    g = pd.DataFrame({
        "WILDLIFE_MGMT_UNIT_ID": ["6-11", "6-12", "6-13", "1-1", "5-3"],
        "REGION_RESPONSIBLE_ID": ["6", "6", "6", "1", "5"],
    })
    apply_remap(g, "REGION_RESPONSIBLE_ID",
                {"field": "WILDLIFE_MGMT_UNIT_ID", "values": {"6-12": "1", "6-13": "1"}})

    assert list(g.REGION_RESPONSIBLE_ID) == ["6", "1", "1", "1", "5"]


def test_apply_remap_is_a_noop_without_a_remap():
    import pandas as pd
    from pipeline.atlas.splits.area_splits import apply_remap

    g = pd.DataFrame({"MU": ["6-12"], "REG": ["6"]})
    apply_remap(g, "REG", None)
    assert list(g.REG) == ["6"]


def test_apply_remap_refuses_an_unknown_field():
    """A typo'd field must fail at build time, not silently leave the dissolve unchanged."""
    import pandas as pd
    import pytest
    from pipeline.atlas.splits.area_splits import apply_remap

    g = pd.DataFrame({"MU": ["6-12"], "REG": ["6"]})
    with pytest.raises(KeyError):
        apply_remap(g, "REG", {"field": "NOPE", "values": {"6-12": "1"}}, layer="wmu")


def test_the_edge_query_finds_the_same_cuts_as_the_bbox_query():
    """`resolve_area_splits` asks the STRtree for lines meeting the polygon's EDGE (88 s province-
    wide) instead of its bbox (2,020 s). Same cuts: a line not meeting the edge lies wholly inside or
    wholly outside and yields none. Reference: the transition cutter over every bbox candidate."""
    from shapely.geometry import LineString, box
    from shapely.strtree import STRtree

    from pipeline.atlas.splits.anchors import _area_transition_measures
    from pipeline.common.models import BlkChain
    park = box(100, -50, 200, 150)
    lines = {"through": LineString([(0, 0), (300, 0)]), "inside": LineString([(120, 0), (180, 0)]),
             "in_bbox_outside": LineString([(90, 160), (210, 160)]),
             "touch": LineString([(100, 10), (150, 10)]), "far": LineString([(900, 0), (999, 0)]),
             "weave": LineString([(0, 50), (110, 50), (120, 200), (130, 50), (300, 50)])}
    chains = [BlkChain(blk=k, fwa_watershed_code="1", fids=(), geometry=g, mouth_measure=0.0,
                       length_m=g.length, name_tuples=()) for k, g in sorted(lines.items())]
    got = sorted((p.blk, round(p.route_measure, 9)) for p in resolve_area_splits({"P": park}, chains))
    tree = STRtree([c.geometry for c in chains])
    want = sorted((chains[i].blk, round(m, 9)) for i in tree.query(park)
                  for m in _area_transition_measures(chains[i].geometry, park, park.boundary))
    assert got == want
