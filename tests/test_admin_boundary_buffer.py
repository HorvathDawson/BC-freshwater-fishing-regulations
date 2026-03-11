"""
Tests for admin boundary buffer — ensures streams near (but not touching)
admin boundaries are included via a small spatial buffer.

Uses synthetic geometries in EPSG:3005 (BC Albers, meters) to validate
that find_features_in_admin_area honours an optional buffer_m parameter.
"""

from pathlib import Path
from unittest.mock import MagicMock, patch, PropertyMock
import pytest
import numpy as np

from shapely.geometry import LineString, box

from fwa_pipeline.metadata_gazetteer import FWAFeature, MetadataGazetteer
from fwa_pipeline.metadata_builder import FeatureType


# ===================================================================
# Synthetic geometries  (EPSG:3005 — units are metres)
# ===================================================================

# A square admin boundary (park/reserve/aboriginal_lands): 1km × 1km
ADMIN_POLYGON = box(1_000_000, 500_000, 1_001_000, 501_000)

# Stream A: runs 15m outside the admin boundary (parallel, along east edge)
# This should be EXCLUDED without buffer, INCLUDED with 25m buffer.
STREAM_NEAR = LineString(
    [
        (1_001_015, 500_200),
        (1_001_015, 500_800),
    ]
)

# Stream B: runs 50m outside — should stay EXCLUDED even with 25m buffer.
STREAM_FAR = LineString(
    [
        (1_001_050, 500_200),
        (1_001_050, 500_800),
    ]
)

# Stream C: clearly inside the admin boundary — always included.
STREAM_INSIDE = LineString(
    [
        (1_000_300, 500_300),
        (1_000_700, 500_700),
    ]
)


# ===================================================================
# Helpers
# ===================================================================


def _make_fwa_feature(fwa_id: str) -> FWAFeature:
    return FWAFeature(
        fwa_id=fwa_id,
        geometry_type="multilinestring",
        zones=["3"],
        feature_type=FeatureType.STREAM,
        gnis_name=None,
    )


def _make_admin_feature(fwa_id: str) -> FWAFeature:
    return FWAFeature(
        fwa_id=fwa_id,
        geometry_type="polygon",
        zones=[],
        feature_type=FeatureType.ABORIGINAL_LANDS,
        gnis_name="Test Reserve",
    )


# ===================================================================
# Tests — buffer behaviour
# ===================================================================


class TestAdminBoundaryBuffer:
    """Stream 15m from admin boundary should be included with buffer_m=25."""

    @pytest.fixture()
    def mock_gazetteer(self):
        """Build a minimal mock gazetteer that returns controlled geometries."""
        import geopandas as gpd

        gaz = MagicMock(spec=MetadataGazetteer)
        gaz.gpkg_path = Path("/fake/path.gpkg")

        # Admin geometry: returns our square polygon
        admin_gdf = gpd.GeoDataFrame(
            {"osm_id": ["admin_1"]},
            geometry=[ADMIN_POLYGON],
            crs="EPSG:3005",
        )
        gaz.get_admin_geometry.return_value = admin_gdf

        # FWA streams layer: three streams
        streams_gdf = gpd.GeoDataFrame(
            {"LINEAR_FEATURE_ID": ["near_1", "far_1", "inside_1"]},
            geometry=[STREAM_NEAR, STREAM_FAR, STREAM_INSIDE],
            crs="EPSG:3005",
        )
        streams_gdf = streams_gdf.set_index("LINEAR_FEATURE_ID", drop=False)
        gaz._get_cached_fwa_layer.return_value = streams_gdf

        # Feature resolution: return FWAFeature objects for matched IDs
        def _resolve(ftype, fwa_id):
            return _make_fwa_feature(fwa_id)

        gaz.get_feature_by_type_and_id.side_effect = _resolve

        return gaz

    def test_no_buffer_excludes_near_stream(self, mock_gazetteer):
        """Without a buffer, the 15m-away stream should NOT be matched."""
        admin_feat = _make_admin_feature("admin_1")
        # Call the real method with buffer_m=0
        result = MetadataGazetteer.find_features_in_admin_area(
            mock_gazetteer,
            admin_features=[admin_feat],
            layer_key="aboriginal_lands",
            feature_types=[FeatureType.STREAM],
            gpkg_path=Path("/fake/path.gpkg"),
            buffer_m=0,
        )
        matched_ids = {f.fwa_id for f in result}
        assert "inside_1" in matched_ids, "Stream inside should always match"
        assert (
            "near_1" not in matched_ids
        ), "Stream 15m away should NOT match without buffer"
        assert "far_1" not in matched_ids

    def test_buffer_25m_includes_near_stream(self, mock_gazetteer):
        """With buffer_m=25, the 15m-away stream SHOULD be matched."""
        admin_feat = _make_admin_feature("admin_1")
        result = MetadataGazetteer.find_features_in_admin_area(
            mock_gazetteer,
            admin_features=[admin_feat],
            layer_key="aboriginal_lands",
            feature_types=[FeatureType.STREAM],
            gpkg_path=Path("/fake/path.gpkg"),
            buffer_m=25,
        )
        matched_ids = {f.fwa_id for f in result}
        assert "inside_1" in matched_ids, "Stream inside should always match"
        assert "near_1" in matched_ids, "Stream 15m away should match with 25m buffer"
        assert (
            "far_1" not in matched_ids
        ), "Stream 50m away should NOT match with 25m buffer"

    def test_buffer_default_no_buffer(self, mock_gazetteer):
        """Default call (no buffer_m) behaves like buffer_m=0."""
        admin_feat = _make_admin_feature("admin_1")
        result = MetadataGazetteer.find_features_in_admin_area(
            mock_gazetteer,
            admin_features=[admin_feat],
            layer_key="aboriginal_lands",
            feature_types=[FeatureType.STREAM],
            gpkg_path=Path("/fake/path.gpkg"),
        )
        matched_ids = {f.fwa_id for f in result}
        assert "inside_1" in matched_ids
        assert (
            "near_1" not in matched_ids
        ), "Default (no buffer) should not include 15m-away stream"


class TestAdminBoundaryBufferPolygonLayers:
    """Buffer should apply to polygon layers too but overlap filter still
    applies — a lake touching the buffered boundary with <1% overlap is
    still excluded."""

    @pytest.fixture()
    def mock_gazetteer(self):
        import geopandas as gpd

        gaz = MagicMock(spec=MetadataGazetteer)
        gaz.gpkg_path = Path("/fake/path.gpkg")

        admin_gdf = gpd.GeoDataFrame(
            {"osm_id": ["admin_1"]},
            geometry=[ADMIN_POLYGON],
            crs="EPSG:3005",
        )
        gaz.get_admin_geometry.return_value = admin_gdf

        # Lake A: fully inside admin boundary
        lake_inside = box(1_000_100, 500_100, 1_000_400, 500_400)
        # Lake B: 10m outside but within 25m buffer, small overlap after buffer
        lake_near = box(1_001_010, 500_200, 1_001_200, 500_400)

        lakes_gdf = gpd.GeoDataFrame(
            {"WATERBODY_POLY_ID": ["lake_in", "lake_near"]},
            geometry=[lake_inside, lake_near],
            crs="EPSG:3005",
        )
        lakes_gdf = lakes_gdf.set_index("WATERBODY_POLY_ID", drop=False)
        gaz._get_cached_fwa_layer.return_value = lakes_gdf

        def _resolve(ftype, fwa_id):
            return FWAFeature(
                fwa_id=fwa_id,
                geometry_type="polygon",
                zones=["3"],
                feature_type=FeatureType.LAKE,
                gnis_name="Test Lake",
            )

        gaz.get_feature_by_type_and_id.side_effect = _resolve

        return gaz

    def test_lake_inside_always_matched(self, mock_gazetteer):
        admin_feat = _make_admin_feature("admin_1")
        result = MetadataGazetteer.find_features_in_admin_area(
            mock_gazetteer,
            admin_features=[admin_feat],
            layer_key="aboriginal_lands",
            feature_types=[FeatureType.LAKE],
            gpkg_path=Path("/fake/path.gpkg"),
            buffer_m=25,
        )
        matched_ids = {f.fwa_id for f in result}
        assert "lake_in" in matched_ids


# ===================================================================
# Tests — hysteresis in lookup_admin_targets
# ===================================================================

# Streams for hysteresis testing — relative to ADMIN_POLYGON (1km square).
# Inside: clearly inside.
# Near-shared: 200m outside, shares BLK with inside stream → hysteresis keeps it.
# Near-isolated: 200m outside, different BLK → hysteresis discards it.

HYST_STREAM_INSIDE = LineString([(1_000_300, 500_300), (1_000_700, 500_700)])
HYST_STREAM_NEAR_SHARED = LineString([(1_001_200, 500_200), (1_001_200, 500_800)])
HYST_STREAM_NEAR_ISOLATED = LineString([(1_001_200, 500_050), (1_001_200, 500_150)])


def _make_fwa_feature_with_blk(fwa_id: str, blue_line_key: str) -> FWAFeature:
    return FWAFeature(
        fwa_id=fwa_id,
        geometry_type="multilinestring",
        zones=["3"],
        feature_type=FeatureType.STREAM,
        gnis_name=None,
        blue_line_key=blue_line_key,
    )


class TestAdminBoundaryHysteresis:
    """500m buffer with hysteresis: buffer-only features are kept only if
    they share a blue_line_key with a feature inside the exact boundary."""

    @pytest.fixture()
    def mock_gazetteer(self):
        import geopandas as gpd

        gaz = MagicMock(spec=MetadataGazetteer)
        gaz.gpkg_path = Path("/fake/path.gpkg")

        admin_gdf = gpd.GeoDataFrame(
            {"osm_id": ["admin_1"]},
            geometry=[ADMIN_POLYGON],
            crs="EPSG:3005",
        )
        gaz.get_admin_geometry.return_value = admin_gdf

        streams_gdf = gpd.GeoDataFrame(
            {
                "LINEAR_FEATURE_ID": [
                    "inside_1",
                    "near_shared_1",
                    "near_isolated_1",
                ]
            },
            geometry=[
                HYST_STREAM_INSIDE,
                HYST_STREAM_NEAR_SHARED,
                HYST_STREAM_NEAR_ISOLATED,
            ],
            crs="EPSG:3005",
        )
        streams_gdf = streams_gdf.set_index("LINEAR_FEATURE_ID", drop=False)
        gaz._get_cached_fwa_layer.return_value = streams_gdf

        def _resolve(ftype, fwa_id):
            blk_map = {
                "inside_1": "BLK_100",
                "near_shared_1": "BLK_100",  # same BLK as inside
                "near_isolated_1": "BLK_999",  # different BLK
            }
            return _make_fwa_feature_with_blk(fwa_id, blk_map[fwa_id])

        gaz.get_feature_by_type_and_id.side_effect = _resolve

        # search_admin_layer returns the admin feature itself
        gaz.search_admin_layer.return_value = [_make_admin_feature("admin_1")]
        # find_features_in_admin_area delegates to the real method
        gaz.find_features_in_admin_area = (
            lambda **kw: MetadataGazetteer.find_features_in_admin_area(gaz, **kw)
        )

        return gaz

    def test_hysteresis_keeps_shared_blk(self, mock_gazetteer):
        """Stream 200m outside sharing BLK with an inside stream is kept."""
        from regulation_mapping.regulation_resolvers import lookup_admin_targets
        from regulation_mapping.admin_target import AdminTarget

        targets = [AdminTarget(layer="aboriginal_lands", feature_id="admin_1")]
        matched, _ = lookup_admin_targets(
            mock_gazetteer,
            Path("/fake/path.gpkg"),
            targets,
            [FeatureType.STREAM],
            buffer_m=500,
        )
        matched_ids = {f.fwa_id for f in matched}
        assert "inside_1" in matched_ids
        assert (
            "near_shared_1" in matched_ids
        ), "Stream 200m outside with same BLK should be kept via hysteresis"

    def test_hysteresis_discards_isolated_blk(self, mock_gazetteer):
        """Stream 200m outside with different BLK is discarded."""
        from regulation_mapping.regulation_resolvers import lookup_admin_targets
        from regulation_mapping.admin_target import AdminTarget

        targets = [AdminTarget(layer="aboriginal_lands", feature_id="admin_1")]
        matched, _ = lookup_admin_targets(
            mock_gazetteer,
            Path("/fake/path.gpkg"),
            targets,
            [FeatureType.STREAM],
            buffer_m=500,
        )
        matched_ids = {f.fwa_id for f in matched}
        assert (
            "near_isolated_1" not in matched_ids
        ), "Stream 200m outside with different BLK should NOT be kept"

    def test_no_buffer_no_hysteresis(self, mock_gazetteer):
        """With buffer_m=0, only exact matches returned (no hysteresis)."""
        from regulation_mapping.regulation_resolvers import lookup_admin_targets
        from regulation_mapping.admin_target import AdminTarget

        targets = [AdminTarget(layer="aboriginal_lands", feature_id="admin_1")]
        matched, _ = lookup_admin_targets(
            mock_gazetteer,
            Path("/fake/path.gpkg"),
            targets,
            [FeatureType.STREAM],
            buffer_m=0,
        )
        matched_ids = {f.fwa_id for f in matched}
        assert "inside_1" in matched_ids
        assert "near_shared_1" not in matched_ids
        assert "near_isolated_1" not in matched_ids

    def test_exact_match_always_kept(self, mock_gazetteer):
        """Features inside the exact boundary are always included
        regardless of hysteresis logic."""
        from regulation_mapping.regulation_resolvers import lookup_admin_targets
        from regulation_mapping.admin_target import AdminTarget

        targets = [AdminTarget(layer="aboriginal_lands", feature_id="admin_1")]
        matched, _ = lookup_admin_targets(
            mock_gazetteer,
            Path("/fake/path.gpkg"),
            targets,
            [FeatureType.STREAM],
            buffer_m=500,
        )
        matched_ids = {f.fwa_id for f in matched}
        assert "inside_1" in matched_ids
