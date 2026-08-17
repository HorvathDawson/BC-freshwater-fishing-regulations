"""Cached BC boundary: path derivation + round-trip load, and border.bc_outline's cache fast path."""

from pathlib import Path

import shapely
from shapely.geometry import Polygon

from pipeline.splits import bc_boundary, border


def test_boundary_path_sits_next_to_gpkg():
    assert bc_boundary.boundary_path("/data/bc.gpkg") == Path("/data/bc_boundary.geojson")


def test_load_cached_boundary_missing_returns_none(tmp_path):
    assert bc_boundary.load_cached_boundary(tmp_path / "nope.gpkg") is None


def test_boundary_round_trip(tmp_path):
    import geopandas as gpd

    poly = Polygon([(0, 0), (0, 10), (10, 10), (10, 0)])
    out = tmp_path / "bc_boundary.geojson"
    gpd.GeoDataFrame({"name": ["BC"]}, geometry=[poly], crs="EPSG:3005").to_file(out, driver="GeoJSON")
    loaded = bc_boundary.load_cached_boundary(tmp_path / "bc.gpkg")
    assert loaded is not None and loaded.equals(poly)


class _Fwa:
    def __init__(self, gpkg_path):
        self.gpkg_path = Path(gpkg_path)
        self.layer_names = []          # no WMU layer -> only the cache path can return a geometry


def test_bc_outline_uses_cache(tmp_path):
    import geopandas as gpd

    poly = Polygon([(0, 0), (0, 5), (5, 5), (5, 0)])
    gpd.GeoDataFrame({"name": ["BC"]}, geometry=[poly], crs="EPSG:3005").to_file(
        tmp_path / "bc_boundary.geojson", driver="GeoJSON")
    outline = border.bc_outline(_Fwa(tmp_path / "bc.gpkg"))
    assert outline is not None and shapely.equals(outline, poly)


def test_bc_outline_no_cache_no_wmu_returns_none(tmp_path):
    assert border.bc_outline(_Fwa(tmp_path / "bc.gpkg")) is None
