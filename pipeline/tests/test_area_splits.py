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
