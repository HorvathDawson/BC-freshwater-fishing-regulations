"""
Unit tests for regulation_mapping.canonical_store.

Tests the scoring, zoom, merge, emit, and admin-clipping logic using
lightweight fakes (no real GPKG reads).
"""

from __future__ import annotations

import json
from collections import defaultdict
from pathlib import Path
from typing import Dict, Optional, Set
from unittest.mock import MagicMock, patch

import pytest
from shapely.geometry import LineString, MultiLineString, Point, Polygon
from shapely.ops import unary_union
from shapely.prepared import prep as shapely_prep

from fwa_pipeline.metadata_builder import FeatureType
from regulation_mapping.canonical_store import (
    MAIN_FLOW_CODES,
    WEIGHTS,
    CanonicalDataStore,
    _LAKE_ZOOM_LOOKUP,
)
from regulation_mapping.geometry_utils import merge_lines
from regulation_mapping.regulation_types import MergedGroup, PipelineResult

# Import helpers from conftest (pytest auto-discovers conftest fixtures,
# but the factory functions need explicit import).
from conftest import (
    FakeGazetteer,
    make_line,
    make_merged_group,
    make_pipeline_result,
    make_polygon,
)


# ===================================================================
# Static / classmethod helpers (no store instance needed)
# ===================================================================


class TestCalculateScore:
    """Tests for CanonicalDataStore._calculate_score (static)."""

    def test_zero_inputs_give_zero(self):
        assert CanonicalDataStore._calculate_score() == 0.0

    def test_magnitude_contributes(self):
        """With default WEIGHTS, magnitude × 1.0 is the dominant term."""
        score = CanonicalDataStore._calculate_score(magnitude=5)
        assert score == 5.0

    def test_length_km_capped_at_1(self):
        """length_km / 1000 is capped at 1.0."""
        score_big = CanonicalDataStore._calculate_score(length_km=5000)
        score_max = CanonicalDataStore._calculate_score(length_km=1000)
        assert score_big == score_max  # both cap at 1.0

    def test_all_inputs(self):
        score = CanonicalDataStore._calculate_score(
            max_order=3,
            magnitude=10,
            length_km=500,
            has_name=True,
            is_side_channel=True,
        )
        expected = (
            3 * WEIGHTS["order"]
            + 10 * WEIGHTS["magnitude"]
            + 1 * WEIGHTS["has_name"]
            + 1 * WEIGHTS["side_channel_penalty"]
            + min(500 / 1000.0, 1.0)
        )
        assert score == pytest.approx(expected)


class TestCalculateMinzoom:
    """Tests for CanonicalDataStore._calculate_minzoom (static)."""

    def test_large_value_returns_low_zoom(self):
        thresholds = [(100, 5), (50, 7), (10, 9), (0, 11)]
        assert CanonicalDataStore._calculate_minzoom(150, thresholds) == 5

    def test_exact_threshold_match(self):
        thresholds = [(100, 5), (50, 7), (10, 9), (0, 11)]
        assert CanonicalDataStore._calculate_minzoom(50, thresholds) == 7

    def test_below_all_thresholds_returns_default(self):
        thresholds = [(100, 5), (50, 7)]
        assert CanonicalDataStore._calculate_minzoom(10, thresholds, default=12) == 12

    def test_zero_value(self):
        thresholds = [(100, 5), (0, 11)]
        assert CanonicalDataStore._calculate_minzoom(0, thresholds) == 11

    def test_lake_zoom_lookup_descending_by_threshold(self):
        """_LAKE_ZOOM_LOOKUP thresholds are sorted descending so first-match works."""
        thresholds = [t for t, _ in _LAKE_ZOOM_LOOKUP]
        assert thresholds == sorted(thresholds, reverse=True)


class TestComputeFrontendGroupId:
    """Tests for CanonicalDataStore._compute_frontend_group_id (static)."""

    def test_deterministic(self):
        """Same inputs → same output."""
        a = CanonicalDataStore._compute_frontend_group_id("wsc", "name", ("r1", "r2"))
        b = CanonicalDataStore._compute_frontend_group_id("wsc", "name", ("r1", "r2"))
        assert a == b

    def test_different_inputs_differ(self):
        a = CanonicalDataStore._compute_frontend_group_id("wsc1", "n1", ("r1",))
        b = CanonicalDataStore._compute_frontend_group_id("wsc2", "n2", ("r2",))
        assert a != b

    def test_length_is_12(self):
        fgid = CanonicalDataStore._compute_frontend_group_id("x", "y", ("r",))
        assert len(fgid) == 12

    def test_regulation_order_independent(self):
        """Regulation IDs are sorted internally, so order shouldn't matter."""
        a = CanonicalDataStore._compute_frontend_group_id("w", "n", ("r1", "r2"))
        b = CanonicalDataStore._compute_frontend_group_id("w", "n", ("r2", "r1"))
        assert a == b


# ===================================================================
# Store instance tests (with fakes)
# ===================================================================


def _make_store(
    merged_groups: Optional[Dict[str, MergedGroup]] = None,
    stream_metadata: Optional[dict] = None,
    regulation_details: Optional[dict] = None,
    regulation_names: Optional[dict] = None,
    admin_area_reg_map: Optional[dict] = None,
    admin_regulation_ids: Optional[set] = None,
    feature_to_linked_regulation: Optional[dict] = None,
    tmp_path: Optional[Path] = None,
) -> CanonicalDataStore:
    """Build a CanonicalDataStore with fake internals (no GPKG I/O)."""
    gazetteer = FakeGazetteer(stream_metadata=stream_metadata or {})
    result = make_pipeline_result(
        merged_groups=merged_groups or {},
        gazetteer=gazetteer,
        regulation_details=regulation_details or {},
        regulation_names=regulation_names or {},
        admin_area_reg_map=admin_area_reg_map or {},
        admin_regulation_ids=admin_regulation_ids or set(),
        feature_to_linked_regulation=(feature_to_linked_regulation or defaultdict(set)),
    )
    cache_dir = (tmp_path or Path("/tmp")) / ".test_cache"
    cache_dir.mkdir(parents=True, exist_ok=True)

    # Patch FWADataAccessor so __init__ doesn't try to read a real GPKG
    with patch("regulation_mapping.canonical_store.FWADataAccessor"):
        store = CanonicalDataStore(
            pipeline_result=result,
            gpkg_path=Path("/fake/test.gpkg"),
            cache_dir=cache_dir,
        )
    # Ensure data_accessor is a mock that won't fail
    store.data_accessor = MagicMock()
    return store


class TestExpandAdminRegIds:
    """Tests for expand_admin_reg_ids."""

    def test_exact_key_preserved(self):
        store = _make_store(regulation_details={"prov_bc": {"name": "BC-wide"}})
        assert store.expand_admin_reg_ids({"prov_bc"}) == {"prov_bc"}

    def test_base_id_expanded_to_rules(self):
        store = _make_store(
            regulation_details={
                "reg_00618_rule0": {"name": "Rule 0"},
                "reg_00618_rule1": {"name": "Rule 1"},
                "reg_00999_rule0": {"name": "Other"},
            }
        )
        expanded = store.expand_admin_reg_ids({"reg_00618"})
        assert expanded == {"reg_00618_rule0", "reg_00618_rule1"}

    def test_missing_id_logged_and_skipped(self):
        store = _make_store(regulation_details={})
        result = store.expand_admin_reg_ids({"reg_99999"})
        assert result == set()


class TestGetRegNames:
    """Tests for get_reg_names."""

    def test_returns_matching_names(self):
        store = _make_store(
            regulation_names={"reg_001": "Adams River"},
            regulation_details={"reg_001_rule0": {}},
        )
        names = store.get_reg_names(["reg_001_rule0"])
        assert names == ["Adams River"]

    def test_excludes_provincial(self):
        store = _make_store(
            regulation_names={"prov_bc": "Province-wide"},
        )
        names = store.get_reg_names(["prov_bc"])
        assert names == []

    def test_excludes_zone(self):
        store = _make_store(
            regulation_names={"zone_3": "Region 3"},
        )
        names = store.get_reg_names(["zone_3"])
        assert names == []

    def test_excludes_admin_regulation_ids(self):
        store = _make_store(
            regulation_names={"reg_100": "Watershed Rule"},
            admin_regulation_ids={"reg_100"},
        )
        names = store.get_reg_names(["reg_100_rule0"])
        assert names == []


class TestBuildGroupBaseProps:
    """Tests for _build_group_base_props."""

    def test_all_fields_present(self):
        group = make_merged_group(
            gnis_name="Fraser River",
            zones=("3", "5"),
            mgmt_units=("3-15", "5-01"),
            region_names=("Thompson", "Cariboo"),
        )
        store = _make_store()
        props = store._build_group_base_props(group)

        assert props["feature_type"] == FeatureType.STREAM.value
        assert props["gnis_name"] == "Fraser River"
        assert props["zones"] == "3,5"
        assert props["mgmt_units"] == "3-15,5-01"
        assert props["region_name"] == "Thompson,Cariboo"
        assert props["feature_count"] == 1
        assert props["feature_ids"] == "f1"

    def test_name_variants_serialized_as_json(self):
        nv = ({"name": "Test", "from_tributary": False},)
        group = make_merged_group(name_variants=nv)
        store = _make_store()
        props = store._build_group_base_props(group)
        parsed = json.loads(props["name_variants"])
        assert parsed == [{"name": "Test", "from_tributary": False}]


class TestEmitGroupFeature:
    """Tests for _emit_group_feature."""

    def test_basic_output_shape(self):
        group = make_merged_group()
        store = _make_store()
        base_props = store._build_group_base_props(group)
        geom = make_line()

        feat = store._emit_group_feature(group, base_props, geometry=geom)

        assert feat["group_id"] == "g1"
        assert feat["frontend_group_id"]  # non-empty
        assert feat["regulation_ids"] == "reg_001_rule0"
        assert feat["regulation_count"] == 1
        assert feat["length_m"] == geom.length
        assert feat["geometry"] is geom

    def test_suffix_appended(self):
        group = make_merged_group()
        store = _make_store()
        base_props = store._build_group_base_props(group)

        feat = store._emit_group_feature(
            group, base_props, suffix="_admin_in", geometry=make_line()
        )
        assert feat["group_id"] == "g1_admin_in"

    def test_override_reg_ids(self):
        group = make_merged_group(regulation_ids=("r1", "r2", "r3"))
        store = _make_store()
        base_props = store._build_group_base_props(group)

        feat = store._emit_group_feature(
            group, base_props, reg_ids=("r1",), geometry=make_line()
        )
        assert feat["regulation_ids"] == "r1"
        assert feat["regulation_count"] == 1

    def test_polygon_uses_area(self):
        group = make_merged_group(feature_type=FeatureType.LAKE.value)
        store = _make_store()
        base_props = store._build_group_base_props(group)
        poly = make_polygon(size=100)

        feat = store._emit_group_feature(group, base_props, geometry=poly)
        assert feat["length_m"] == poly.area

    def test_no_geometry_gives_zero_length(self):
        group = make_merged_group()
        store = _make_store()
        base_props = store._build_group_base_props(group)

        feat = store._emit_group_feature(group, base_props, geometry=None)
        assert feat["length_m"] == 0.0


class TestMergeSameRegulationFeatures:
    """Tests for _merge_same_regulation_features."""

    def test_no_merge_needed(self):
        store = _make_store()
        features = [
            {
                "blue_line_key": "BLK1",
                "waterbody_key": "WBK1",
                "regulation_ids": "r1",
                "geometry": make_line(x_start=0),
                "group_id": "g1",
                "mgmt_units": "3-15",
                "length_m": 1000,
            },
            {
                "blue_line_key": "BLK2",
                "waterbody_key": "WBK2",
                "regulation_ids": "r2",
                "geometry": make_line(x_start=2000),
                "group_id": "g2",
                "mgmt_units": "3-15",
                "length_m": 1000,
            },
        ]
        result = store._merge_same_regulation_features(features)
        assert len(result) == 2

    def test_merges_matching_features(self):
        """Features with same BLK+WBK+regulation_ids should be merged."""
        store = _make_store()
        features = [
            {
                "blue_line_key": "BLK1",
                "waterbody_key": "WBK1",
                "regulation_ids": "r1",
                "geometry": make_line(x_start=0, length=500),
                "group_id": "g1",
                "mgmt_units": "3-15",
                "length_m": 500,
            },
            {
                "blue_line_key": "BLK1",
                "waterbody_key": "WBK1",
                "regulation_ids": "r1",
                "geometry": make_line(x_start=500, length=500),
                "group_id": "g1_admin_out",
                "mgmt_units": "5-01",
                "length_m": 500,
            },
        ]
        result = store._merge_same_regulation_features(features)
        assert len(result) == 1
        merged = result[0]
        assert merged["group_id"] == "g1"  # prefers non-admin-suffix
        assert "3-15" in merged["mgmt_units"]
        assert "5-01" in merged["mgmt_units"]

    def test_empty_list(self):
        store = _make_store()
        assert store._merge_same_regulation_features([]) == []

    def test_missing_regulation_ids_raises(self):
        store = _make_store()
        with pytest.raises(ValueError, match="regulation_ids"):
            store._merge_same_regulation_features(
                [{"blue_line_key": "x", "waterbody_key": "y", "group_id": "g1"}]
            )


class TestClipGroupAtAdminBoundary:
    """Tests for _clip_group_at_admin_boundary."""

    def test_fully_inside_returns_inside(self):
        """A line fully inside the admin polygon → 'inside' disposition."""
        store = _make_store()
        group = make_merged_group(regulation_ids=("r1", "admin_r"))

        # Line from (10,10) to (90,10) — inside the 100×100 polygon
        geom_list = [LineString([(10, 10), (90, 10)])]
        admin_poly = make_polygon(x=0, y=0, size=100)
        admin_prep = shapely_prep(admin_poly)

        features, disposition = store._clip_group_at_admin_boundary(
            group,
            geom_list,
            admin_poly,
            admin_prep,
            admin_reg_ids={"admin_r"},
            base_props=store._build_group_base_props(group),
        )
        assert disposition == "inside"
        assert len(features) == 1
        assert features[0]["group_id"].endswith("_admin_in")

    def test_fully_outside_returns_outside(self):
        """A line fully outside the admin polygon → 'outside', admin regs stripped."""
        store = _make_store()
        group = make_merged_group(regulation_ids=("r1", "admin_r"))

        geom_list = [LineString([(200, 200), (300, 200)])]
        admin_poly = make_polygon(x=0, y=0, size=100)
        admin_prep = shapely_prep(admin_poly)

        features, disposition = store._clip_group_at_admin_boundary(
            group,
            geom_list,
            admin_poly,
            admin_prep,
            admin_reg_ids={"admin_r"},
            base_props=store._build_group_base_props(group),
        )
        assert disposition == "outside"
        assert len(features) == 1
        # Admin regs should be stripped from the outside piece
        assert "admin_r" not in features[0]["regulation_ids"]
        assert "r1" in features[0]["regulation_ids"]

    def test_crossing_boundary_produces_two_pieces(self):
        """A line crossing the admin boundary → 'clipped' with inside+outside."""
        store = _make_store()
        group = make_merged_group(regulation_ids=("r1", "admin_r"))

        # Line crosses the right edge of the admin polygon
        geom_list = [LineString([(50, 50), (150, 50)])]
        admin_poly = make_polygon(x=0, y=0, size=100)
        admin_prep = shapely_prep(admin_poly)

        features, disposition = store._clip_group_at_admin_boundary(
            group,
            geom_list,
            admin_poly,
            admin_prep,
            admin_reg_ids={"admin_r"},
            base_props=store._build_group_base_props(group),
        )
        assert disposition == "clipped"
        assert len(features) == 2

        suffixes = {
            f["group_id"].split("_", 1)[-1] if "_" in f["group_id"] else ""
            for f in features
        }
        # Should have an _admin_in and _admin_out piece
        inside = [f for f in features if "admin_in" in f["group_id"]]
        outside = [f for f in features if "admin_out" in f["group_id"]]
        assert len(inside) == 1
        assert len(outside) == 1
        # Inside piece keeps all regs, outside strips admin_r
        assert "admin_r" in inside[0]["regulation_ids"]
        assert "admin_r" not in outside[0]["regulation_ids"]


class TestYieldFeatures:
    """Integration-level test: yield_features with pre-loaded geometry caches."""

    def test_stream_group_yields_feature(self, tmp_path):
        """A single stream group with pre-loaded geometry should yield one feature."""
        line = make_line(x_start=1000000, y_start=500000, length=5000)
        stream_meta = {
            "f1": {
                "blue_line_key": "BLK_100",
                "stream_order": 3,
                "stream_magnitude": 50,
                "length": 5000,
                "gnis_name": "Test Creek",
                "edge_type": 1000,
            }
        }
        group = make_merged_group(
            feature_ids=("f1",),
            blue_line_key="BLK_100",
        )
        store = _make_store(
            merged_groups={"g1": group},
            stream_metadata=stream_meta,
            tmp_path=tmp_path,
        )
        # Pre-load geometry caches manually (bypass GPKG I/O)
        store._stream_geometries = {"f1": line}
        store._polygon_geometries = {}

        features = list(store.yield_features())
        assert len(features) == 1
        assert features[0]["feature_type"] == FeatureType.STREAM.value
        assert features[0]["gnis_name"] == "Test Creek"
        assert features[0]["geometry"] is not None

    def test_polygon_group_yields_feature(self, tmp_path):
        """A lake group with pre-loaded polygon geometry should yield one feature."""
        poly = make_polygon(x=1000000, y=500000, size=500)
        lake_meta = {"L1": {"waterbody_key": "WBK_1"}}
        gazetteer = FakeGazetteer(
            metadata={FeatureType.LAKE: {"L1": lake_meta}},
        )
        group = make_merged_group(
            group_id="lake_g1",
            feature_ids=("L1",),
            feature_type=FeatureType.LAKE.value,
            gnis_name="Test Lake",
            waterbody_key="WBK_1",
            blue_line_key=None,
            fwa_watershed_code=None,
        )
        result = make_pipeline_result(
            merged_groups={"lake_g1": group},
            gazetteer=gazetteer,
        )
        cache_dir = tmp_path / ".test_cache"
        cache_dir.mkdir()

        with patch("regulation_mapping.canonical_store.FWADataAccessor"):
            store = CanonicalDataStore(result, Path("/fake.gpkg"), cache_dir)
        store.data_accessor = MagicMock()
        store._stream_geometries = {}
        store._polygon_geometries = {f"LAKES_{group.feature_ids[0]}": poly}

        features = list(store.yield_features())
        assert len(features) == 1
        assert features[0]["feature_type"] == FeatureType.LAKE.value
        assert features[0]["geometry"] is not None
        assert "area_sqm" in features[0]

    def test_empty_groups_yield_nothing(self, tmp_path):
        store = _make_store(merged_groups={}, tmp_path=tmp_path)
        store._stream_geometries = {}
        store._polygon_geometries = {}
        features = list(store.yield_features())
        assert features == []


# ===================================================================
# Polygon post-merge tests
# ===================================================================


class TestMergeSameWaterbodyPolygons:
    """Tests for _merge_same_waterbody_polygons.

    When a polygon (lake/wetland/manmade) straddles a zone boundary,
    upstream merging creates separate MergedGroups with different regulation
    sets.  The post-merge step reunifies them by waterbody_key.
    """

    def test_two_polygons_same_wbk_merged(self):
        """Two polygons with same WBK but different zone regs → one feature."""
        store = _make_store()
        poly_a = make_polygon(x=0, y=0, size=100)
        poly_b = make_polygon(x=100, y=0, size=100)
        features = [
            {
                "feature_type": FeatureType.LAKE.value,
                "waterbody_key": "WBK_500",
                "regulation_ids": "reg_001_rule0,zone_3",
                "regulation_count": 2,
                "geometry": poly_a,
                "area_sqm": poly_a.area,
                "group_id": "lake_g1",
                "display_name": "Big Lake",
                "frontend_group_id": "aaa",
                "zones": "3",
                "mgmt_units": "3-15",
                "region_name": "Thompson",
                "feature_ids": "L1,L2",
                "feature_count": 2,
                "tippecanoe:minzoom": 8,
            },
            {
                "feature_type": FeatureType.LAKE.value,
                "waterbody_key": "WBK_500",
                "regulation_ids": "reg_001_rule0,zone_5",
                "regulation_count": 2,
                "geometry": poly_b,
                "area_sqm": poly_b.area,
                "group_id": "lake_g2",
                "display_name": "Big Lake",
                "frontend_group_id": "bbb",
                "zones": "5",
                "mgmt_units": "5-01",
                "region_name": "Cariboo",
                "feature_ids": "L3",
                "feature_count": 1,
                "tippecanoe:minzoom": 8,
            },
        ]
        result = store._merge_same_waterbody_polygons(features)
        assert len(result) == 1
        merged = result[0]
        # Geometry is union
        assert merged["geometry"].area == pytest.approx(
            poly_a.area + poly_b.area, rel=1e-6
        )
        # Regulation IDs are unioned
        rids = set(merged["regulation_ids"].split(","))
        assert rids == {"reg_001_rule0", "zone_3", "zone_5"}
        assert merged["regulation_count"] == 3
        # Zones combined
        assert set(merged["zones"].split(",")) == {"3", "5"}
        # Mgmt units combined
        assert set(merged["mgmt_units"].split(",")) == {"3-15", "5-01"}
        # Region names combined
        assert set(merged["region_name"].split(",")) == {"Cariboo", "Thompson"}
        # Feature IDs combined
        assert set(merged["feature_ids"].split(",")) == {"L1", "L2", "L3"}
        assert merged["feature_count"] == 3

    def test_single_polygon_passes_through(self):
        """A single polygon for a WBK should not be modified."""
        store = _make_store()
        poly = make_polygon(x=0, y=0, size=200)
        features = [
            {
                "feature_type": FeatureType.LAKE.value,
                "waterbody_key": "WBK_100",
                "regulation_ids": "reg_001_rule0",
                "regulation_count": 1,
                "geometry": poly,
                "area_sqm": poly.area,
                "group_id": "lake_g1",
                "display_name": "Solo Lake",
                "frontend_group_id": "ccc",
                "zones": "3",
                "mgmt_units": "3-15",
                "region_name": "Thompson",
                "feature_ids": "L1",
                "feature_count": 1,
                "tippecanoe:minzoom": 8,
            },
        ]
        result = store._merge_same_waterbody_polygons(features)
        assert len(result) == 1
        assert result[0]["group_id"] == "lake_g1"
        assert result[0]["regulation_ids"] == "reg_001_rule0"

    def test_no_wbk_passes_through(self):
        """Polygon with no waterbody_key should not be merged."""
        store = _make_store()
        poly = make_polygon(x=0, y=0, size=50)
        features = [
            {
                "feature_type": FeatureType.MANMADE.value,
                "waterbody_key": None,
                "regulation_ids": "reg_001_rule0",
                "regulation_count": 1,
                "geometry": poly,
                "area_sqm": poly.area,
                "group_id": "mm_g1",
                "display_name": "Dam Pond",
                "frontend_group_id": "ddd",
                "zones": "3",
                "mgmt_units": "3-15",
                "region_name": "Thompson",
                "feature_ids": "M1",
                "feature_count": 1,
                "tippecanoe:minzoom": 10,
            },
        ]
        result = store._merge_same_waterbody_polygons(features)
        assert len(result) == 1
        assert result[0]["group_id"] == "mm_g1"

    def test_merged_frontend_group_id_recalculated(self):
        """Merged polygon should get a new frontend_group_id based on combined regs."""
        store = _make_store()
        poly_a = make_polygon(x=0, y=0, size=100)
        poly_b = make_polygon(x=200, y=0, size=100)
        features = [
            {
                "feature_type": FeatureType.LAKE.value,
                "waterbody_key": "WBK_700",
                "regulation_ids": "zone_3",
                "regulation_count": 1,
                "geometry": poly_a,
                "area_sqm": poly_a.area,
                "group_id": "g1",
                "display_name": "Split Lake",
                "frontend_group_id": "old_a",
                "zones": "3",
                "mgmt_units": "3-15",
                "region_name": "Thompson",
                "feature_ids": "L1",
                "feature_count": 1,
                "tippecanoe:minzoom": 8,
            },
            {
                "feature_type": FeatureType.LAKE.value,
                "waterbody_key": "WBK_700",
                "regulation_ids": "zone_5",
                "regulation_count": 1,
                "geometry": poly_b,
                "area_sqm": poly_b.area,
                "group_id": "g2",
                "display_name": "Split Lake",
                "frontend_group_id": "old_b",
                "zones": "5",
                "mgmt_units": "5-01",
                "region_name": "Cariboo",
                "feature_ids": "L2",
                "feature_count": 1,
                "tippecanoe:minzoom": 8,
            },
        ]
        result = store._merge_same_waterbody_polygons(features)
        assert len(result) == 1
        # frontend_group_id should be freshly computed, not "old_a" or "old_b"
        fgid = result[0]["frontend_group_id"]
        assert fgid not in ("old_a", "old_b")
        assert len(fgid) == 12  # MD5 hash prefix

        # Verify it matches the expected computation
        expected = CanonicalDataStore._compute_frontend_group_id(
            "WBK_700", "Split Lake", ("zone_3", "zone_5")
        )
        assert fgid == expected

    def test_empty_list(self):
        store = _make_store()
        assert store._merge_same_waterbody_polygons([]) == []

    def test_prefers_non_admin_suffix_group_id(self):
        """When merging, prefer group_id without admin suffix."""
        store = _make_store()
        poly_a = make_polygon(x=0, y=0, size=50)
        poly_b = make_polygon(x=50, y=0, size=50)
        features = [
            {
                "feature_type": FeatureType.LAKE.value,
                "waterbody_key": "WBK_800",
                "regulation_ids": "r1",
                "regulation_count": 1,
                "geometry": poly_a,
                "area_sqm": poly_a.area,
                "group_id": "g1_admin_in",
                "display_name": "Admin Lake",
                "frontend_group_id": "x",
                "zones": "3",
                "mgmt_units": "3-15",
                "region_name": "Thompson",
                "feature_ids": "L1",
                "feature_count": 1,
                "tippecanoe:minzoom": 8,
            },
            {
                "feature_type": FeatureType.LAKE.value,
                "waterbody_key": "WBK_800",
                "regulation_ids": "r2",
                "regulation_count": 1,
                "geometry": poly_b,
                "area_sqm": poly_b.area,
                "group_id": "g1",
                "display_name": "Admin Lake",
                "frontend_group_id": "y",
                "zones": "5",
                "mgmt_units": "5-01",
                "region_name": "Cariboo",
                "feature_ids": "L2",
                "feature_count": 1,
                "tippecanoe:minzoom": 8,
            },
        ]
        result = store._merge_same_waterbody_polygons(features)
        assert result[0]["group_id"] == "g1"


class TestYieldFeaturesPolygonPostMerge:
    """Integration test: yield_features merges zone-split polygons."""

    def test_two_lake_groups_same_wbk_yield_one_feature(self, tmp_path):
        """Two lake groups (same WBK, different zones) → one merged feature."""
        poly_a = make_polygon(x=1000000, y=500000, size=500)
        poly_b = make_polygon(x=1000500, y=500000, size=500)

        group_a = make_merged_group(
            group_id="lake_zone3",
            feature_ids=("L1",),
            regulation_ids=("reg_001_rule0", "zone_3"),
            feature_type=FeatureType.LAKE.value,
            gnis_name="Boundary Lake",
            waterbody_key="WBK_999",
            blue_line_key=None,
            fwa_watershed_code=None,
            zones=("3",),
            mgmt_units=("3-15",),
            region_names=("Thompson",),
        )
        group_b = make_merged_group(
            group_id="lake_zone5",
            feature_ids=("L2",),
            regulation_ids=("reg_001_rule0", "zone_5"),
            feature_type=FeatureType.LAKE.value,
            gnis_name="Boundary Lake",
            waterbody_key="WBK_999",
            blue_line_key=None,
            fwa_watershed_code=None,
            zones=("5",),
            mgmt_units=("5-01",),
            region_names=("Cariboo",),
        )

        gazetteer = FakeGazetteer()
        result = make_pipeline_result(
            merged_groups={"lake_zone3": group_a, "lake_zone5": group_b},
            gazetteer=gazetteer,
        )
        cache_dir = tmp_path / ".test_cache"
        cache_dir.mkdir()

        with patch("regulation_mapping.canonical_store.FWADataAccessor"):
            store = CanonicalDataStore(result, Path("/fake.gpkg"), cache_dir)
        store.data_accessor = MagicMock()
        store._stream_geometries = {}
        store._polygon_geometries = {
            "LAKES_L1": poly_a,
            "LAKES_L2": poly_b,
        }

        features = list(store.yield_features())
        assert len(features) == 1

        merged = features[0]
        assert merged["feature_type"] == FeatureType.LAKE.value
        # Combined regulation_ids
        rids = set(merged["regulation_ids"].split(","))
        assert rids == {"reg_001_rule0", "zone_3", "zone_5"}
        # Combined zones
        assert set(merged["zones"].split(",")) == {"3", "5"}
        # Geometry is the union
        assert merged["geometry"].area == pytest.approx(
            poly_a.area + poly_b.area, rel=1e-6
        )
        # Single frontend_group_id
        assert len(merged["frontend_group_id"]) == 12
