"""
Unit tests for regulation_mapping.geo_exporter (GeoArtifactGenerator).

Tests layer filtering, file-lock checks, PMTiles column stripping, and
tool-availability error paths.  All store interactions are mocked.
"""

from __future__ import annotations

import subprocess
from pathlib import Path
from unittest.mock import MagicMock, patch

import geopandas as gpd
import pandas as pd
import pytest
from shapely.geometry import LineString, Point, Polygon

from fwa_pipeline.metadata_builder import FeatureType
from regulation_mapping.geo_exporter import (
    GeoArtifactGenerator,
    _PMTILES_COLUMNS,
)

from conftest import make_line, make_polygon


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_fake_store(canonical_features: list | None = None) -> MagicMock:
    """Create a mock CanonicalDataStore with controllable canonical features."""
    store = MagicMock()
    store.get_canonical_features.return_value = canonical_features or []
    store.get_lake_manmade_wbkeys.return_value = set()
    store.admin_area_reg_map = {}
    store.data_accessor = MagicMock()
    store.data_accessor.list_layers.return_value = []
    # Tidal boundary must explicitly return None when not configured,
    # otherwise MagicMock auto-creates a truthy return value.
    store.get_tidal_boundary_gdf.return_value = None
    return store


def _make_stream_feature(**overrides) -> dict:
    """Build a minimal canonical feature dict for a stream."""
    base = {
        "feature_type": FeatureType.STREAM.value,
        "group_id": "g1",
        "frontend_group_id": "abc123def456",
        "display_name": "Test Creek",
        "waterbody_key": None,
        "blue_line_key": "BLK_100",
        "fwa_watershed_code": "100-000000",
        "gnis_name": "Test Creek",
        "display_name_override": "",
        "inherited_gnis_name": "",
        "name_variants": "[]",
        "zones": "3",
        "mgmt_units": "3-15",
        "region_name": "Thompson",
        "feature_count": 1,
        "feature_ids": "f1",
        "stream_order": 3,
        "regulation_ids": "reg_001_rule0",
        "regulation_count": 1,
        "length_m": 5000.0,
        "tippecanoe:minzoom": 8,
        "geometry": make_line(x_start=1000000, y_start=500000, length=5000),
    }
    base.update(overrides)
    return base


def _make_lake_feature(**overrides) -> dict:
    """Build a minimal canonical feature dict for a lake."""
    base = {
        "feature_type": FeatureType.LAKE.value,
        "group_id": "lake_g1",
        "frontend_group_id": "lak123456789",
        "display_name": "Test Lake",
        "waterbody_key": "WBK_1",
        "blue_line_key": None,
        "fwa_watershed_code": None,
        "gnis_name": "Test Lake",
        "display_name_override": "",
        "inherited_gnis_name": "",
        "name_variants": "[]",
        "zones": "3",
        "mgmt_units": "3-15",
        "region_name": "Thompson",
        "feature_count": 1,
        "feature_ids": "L1",
        "area_sqm": 50000.0,
        "regulation_ids": "reg_002_rule0",
        "regulation_count": 1,
        "length_m": 50000.0,
        "tippecanoe:minzoom": 8,
        "geometry": make_polygon(x=1000000, y=500000, size=500),
    }
    base.update(overrides)
    return base


# ===================================================================
# GeoArtifactGenerator construction
# ===================================================================


class TestGeoArtifactGeneratorInit:
    """Sanity-check construction."""

    def test_creates_with_mock_store(self):
        store = _make_fake_store()
        gen = GeoArtifactGenerator(store)
        assert gen.store is store
        assert gen._layer_cache == {}


# ===================================================================
# Layer filtering
# ===================================================================


class TestLayerFiltering:
    """Test _get_cached_layer and layer creation methods."""

    def test_streams_layer_filters_correctly(self):
        feats = [_make_stream_feature(), _make_lake_feature()]
        store = _make_fake_store(feats)
        gen = GeoArtifactGenerator(store)
        gdf = gen._create_streams_layer(exclude_lake_streams=False)
        assert gdf is not None
        assert len(gdf) == 1
        assert gdf.iloc[0]["feature_type"] == FeatureType.STREAM.value

    def test_polygon_layer_filters_by_type(self):
        feats = [_make_stream_feature(), _make_lake_feature()]
        store = _make_fake_store(feats)
        gen = GeoArtifactGenerator(store)
        gdf = gen._create_polygon_layer(FeatureType.LAKE)
        assert gdf is not None
        assert len(gdf) == 1
        assert gdf.iloc[0]["feature_type"] == FeatureType.LAKE.value

    def test_no_matching_features_returns_none(self):
        store = _make_fake_store([_make_stream_feature()])
        gen = GeoArtifactGenerator(store)
        gdf = gen._create_polygon_layer(FeatureType.LAKE)
        assert gdf is None

    def test_layer_caching(self):
        feats = [_make_stream_feature()]
        store = _make_fake_store(feats)
        gen = GeoArtifactGenerator(store)
        gdf1 = gen._create_streams_layer()
        gdf2 = gen._create_streams_layer()
        # Should be the exact same object (cached)
        assert gdf1 is gdf2

    def test_exclude_lake_streams(self):
        """Streams with is_under_lake=True should be excluded when requested."""
        feats = [
            _make_stream_feature(
                waterbody_key="WBK_LAKE_1",
                is_under_lake=True,
            ),
            _make_stream_feature(
                group_id="g2",
                waterbody_key=None,
                display_name="Other Creek",
                is_under_lake=False,
            ),
        ]
        store = _make_fake_store(feats)
        gen = GeoArtifactGenerator(store)
        gdf = gen._create_streams_layer(exclude_lake_streams=True)
        assert gdf is not None
        assert len(gdf) == 1
        assert gdf.iloc[0]["group_id"] == "g2"

    def test_under_lake_streams_layer(self):
        """Only streams with is_under_lake=True appear in the under-lake layer."""
        feats = [
            _make_stream_feature(
                group_id="g_lake",
                waterbody_key="WBK_LAKE_1",
                is_under_lake=True,
            ),
            _make_stream_feature(
                group_id="g_open",
                waterbody_key=None,
                display_name="Open Creek",
                is_under_lake=False,
            ),
        ]
        store = _make_fake_store(feats)
        gen = GeoArtifactGenerator(store)
        gdf = gen._create_under_lake_streams_layer()
        assert gdf is not None
        assert len(gdf) == 1
        assert gdf.iloc[0]["group_id"] == "g_lake"


# ===================================================================
# PMTiles column allow-lists
# ===================================================================


class TestPmtilesColumns:
    """Verify the column allow-lists are sensible."""

    def test_all_layers_have_group_id(self):
        """Every canonical layer should include group_id for frontend matching."""
        for layer, cols in _PMTILES_COLUMNS.items():
            assert "group_id" in cols, f"{layer} missing group_id"

    def test_streams_has_stream_order(self):
        assert "stream_order" in _PMTILES_COLUMNS["streams"]

    def test_lakes_has_area_sqm(self):
        assert "area_sqm" in _PMTILES_COLUMNS["lakes"]

    def test_no_regulation_ids_in_pmtiles(self):
        """regulation_ids is debug data — should NOT be in lean tiles."""
        for layer, cols in _PMTILES_COLUMNS.items():
            assert "regulation_ids" not in cols, f"{layer} leaks regulation_ids"


# ===================================================================
# File lock check
# ===================================================================


class TestIsFileLocked:
    """Tests for the _is_file_locked static method."""

    def test_nonexistent_file_not_locked(self, tmp_path):
        assert GeoArtifactGenerator._is_file_locked(tmp_path / "nope.gpkg") is False

    def test_writable_file_not_locked(self, tmp_path):
        f = tmp_path / "test.gpkg"
        f.write_text("data")
        assert GeoArtifactGenerator._is_file_locked(f) is False


# ===================================================================
# export_gpkg
# ===================================================================


class TestExportGpkg:
    """Tests for GPKG export — uses mocked layer creation."""

    def test_creates_gpkg_file(self, tmp_path):
        """With at least one layer, export_gpkg should create the file."""
        feats = [_make_lake_feature()]
        store = _make_fake_store(feats)
        gen = GeoArtifactGenerator(store)

        output = tmp_path / "test.gpkg"
        result = gen.export_gpkg(output)

        assert result is not None
        assert output.exists()
        assert output.stat().st_size > 0

    def test_no_layers_returns_none(self, tmp_path):
        """With no features at all, export_gpkg should return None."""
        store = _make_fake_store([])
        gen = GeoArtifactGenerator(store)
        result = gen.export_gpkg(tmp_path / "empty.gpkg")
        assert result is None


# ===================================================================
# export_pmtiles — tool availability
# ===================================================================


class TestExportPmtilesToolCheck:
    """Tests that export_pmtiles raises FileNotFoundError when tippecanoe is missing."""

    def test_raises_when_tippecanoe_missing(self, tmp_path):
        store = _make_fake_store([_make_stream_feature()])
        gen = GeoArtifactGenerator(store)

        with patch("regulation_mapping.geo_exporter.shutil.which", return_value=None):
            with pytest.raises(FileNotFoundError, match="tippecanoe"):
                gen.export_pmtiles(tmp_path / "out.pmtiles")


# ===================================================================
# Layer config
# ===================================================================


class TestGetLayerConfigs:
    """Test _get_layer_configs returns expected layer names."""

    def test_includes_canonical_layers(self):
        store = _make_fake_store()
        gen = GeoArtifactGenerator(store)
        configs = gen._get_layer_configs()
        names = [name for name, _ in configs]
        assert "streams" in names
        assert "lakes" in names
        assert "wetlands" in names
        assert "manmade" in names
        assert "ungazetted" in names

    def test_includes_regions_by_default(self):
        store = _make_fake_store()
        gen = GeoArtifactGenerator(store)
        configs = gen._get_layer_configs(include_regions=True)
        names = [name for name, _ in configs]
        assert "regions" in names
        assert "management_units" in names
        assert "bc_mask" in names

    def test_excludes_regions_when_disabled(self):
        store = _make_fake_store()
        gen = GeoArtifactGenerator(store)
        configs = gen._get_layer_configs(include_regions=False)
        names = [name for name, _ in configs]
        assert "regions" not in names
        assert "management_units" not in names
        assert "bc_mask" not in names
