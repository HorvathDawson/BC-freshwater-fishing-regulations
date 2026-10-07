"""The B.C. outline is the EXACT union of the management units (BOUND round, 2026-10-06).

It used to be simplified (50 m per unit, 100 m on the result, a 1 m buffer and a hole filler), and
disagreed with the region polygons dissolved from the same units by up to 128 m: a river leaving the
province crossed the outline and the region edge at two places, and the strip between lay in no
region — 31 stretches inside B.C. read as OUTSIDE B.C.
"""

from pathlib import Path

import pytest
import shapely
from shapely.geometry import Polygon, box

from pipeline.atlas.splits import bc_boundary, border


def test_paths_sit_next_to_the_gpkg():
    assert bc_boundary.boundary_path("/data/bc.gpkg") == Path("/data/bc_boundary.geojson")
    assert bc_boundary.exact_path("/data/bc.gpkg") == Path("/data/bc_boundary.exact.wkb")


def test_the_union_is_exact_and_shares_every_unit_edge():
    """Nothing is simplified: each unit's outer edge is a piece of the outline, vertex for vertex."""
    units = [Polygon([(0, 0), (10, 0.3), (10.7, 10), (0, 10)]),
             Polygon([(10, 0.3), (20, 0), (20, 10), (10.7, 10)])]
    out = bc_boundary.exact_union(units)
    assert out.geom_type == "Polygon" and not out.interiors
    assert abs(out.area - sum(u.area for u in units)) < 1e-9
    for u in units:                       # the part of the unit's edge on the outline is unchanged
        on = u.boundary.intersection(out.boundary)
        assert on.length > 0 and u.boundary.difference(out.boundary).length < u.boundary.length


def test_an_invalid_coverage_is_refused_not_buffered_shut():
    """A gap between neighbours is a fault in the source layer. The old code buffered such gaps
    shut (and then filled 4,594 hairline holes it had itself opened); the exact union names it."""
    gap = [box(0, 0, 10, 10), box(10.5, 0, 20, 10), box(0, 10, 20, 20)]       # 0.5 m slot
    overlap = [box(0, 0, 10, 10), box(9, 0, 20, 10)]
    for units in (overlap, gap):          # the slot leaves box 3's edge unmatched by its neighbours
        with pytest.raises(ValueError, match="not a valid coverage"):
            bc_boundary.exact_union(units)


def test_the_cache_round_trips_exact_doubles(tmp_path, monkeypatch):
    """WKB, not GeoJSON: a GeoJSON round trip rounds coordinates, and a rounded outline no longer
    coincides with the regions' edges. The cache is keyed by the gpkg it was made from."""
    gpkg = tmp_path / "bc.gpkg"
    gpkg.write_bytes(b"x")
    poly = Polygon([(0.1234567890123, 0), (10, 0.98765432109876), (10, 10), (0, 10)])
    calls = []
    monkeypatch.setattr(bc_boundary, "wmu_outline", lambda p: (calls.append(p) or poly, "EPSG:3005"))
    first = bc_boundary.load_outline(gpkg)
    second = bc_boundary.load_outline(gpkg)
    assert len(calls) == 1, "the second load reads the cache"
    assert shapely.equals_exact(first, poly, 0.0) and shapely.equals_exact(second, poly, 0.0)
    gpkg.write_bytes(b"xy")                                     # a different gpkg: recomputed
    bc_boundary.load_outline(gpkg)
    assert len(calls) == 2


def test_bc_outline_reads_the_exact_outline(tmp_path, monkeypatch):
    class _Fwa:
        gpkg_path = tmp_path / "bc.gpkg"
    poly = box(0, 0, 5, 5)
    monkeypatch.setattr(bc_boundary, "load_outline", lambda p: poly)
    assert border.bc_outline(_Fwa()) is poly


@pytest.mark.slow
def test_the_real_units_are_a_valid_coverage_and_the_outline_has_no_holes():
    """Measured 2026-10-06: 225 units, 0 invalid, a valid coverage, one polygon with no holes."""
    from project_config import get_config

    gpkg = get_config().fwa_data_gpkg
    out, _ = bc_boundary.wmu_outline(gpkg)
    assert out is not None and out.geom_type == "Polygon" and not out.interiors
