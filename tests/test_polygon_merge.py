"""
Tests for merge_overlapping_polygons — ensures spatially overlapping
admin boundary polygons are correctly merged into single features.

Uses synthetic geometries in EPSG:3005 (BC Albers, meters).
"""

import geopandas as gpd
import pytest
from shapely.geometry import box

from regulation_mapping.geometry_utils import merge_overlapping_polygons


# ===================================================================
# Helpers
# ===================================================================

def _make_gdf(features: list[dict]) -> gpd.GeoDataFrame:
    """Build a GeoDataFrame with osm_id, name, geometry columns."""
    return gpd.GeoDataFrame(features, crs="EPSG:3005")


# ===================================================================
# Basic behaviour
# ===================================================================


class TestMergeOverlappingPolygons:
    """Core merge logic: overlap detection, grouping, identity selection."""

    def test_no_overlap_passes_through(self):
        """Non-overlapping polygons are returned unchanged."""
        gdf = _make_gdf([
            {"osm_id": "1", "name": "A", "geometry": box(0, 0, 10, 10)},
            {"osm_id": "2", "name": "B", "geometry": box(100, 100, 110, 110)},
        ])
        result = merge_overlapping_polygons(gdf, "osm_id", "name")
        assert len(result) == 2
        assert set(result["osm_id"]) == {"1", "2"}

    def test_touching_edges_not_merged(self):
        """Polygons sharing only an edge (zero area overlap) stay separate."""
        gdf = _make_gdf([
            {"osm_id": "1", "name": "A", "geometry": box(0, 0, 10, 10)},
            {"osm_id": "2", "name": "B", "geometry": box(10, 0, 20, 10)},
        ])
        result = merge_overlapping_polygons(gdf, "osm_id", "name")
        assert len(result) == 2

    def test_two_overlapping_polygons_merge(self):
        """Two polygons with area overlap become one merged feature."""
        gdf = _make_gdf([
            {"osm_id": "1", "name": "Small Reserve", "geometry": box(0, 0, 10, 10)},
            {"osm_id": "2", "name": "Big Nation", "geometry": box(5, 5, 20, 20)},
        ])
        result = merge_overlapping_polygons(gdf, "osm_id", "name")
        assert len(result) == 1

    def test_largest_member_identity(self):
        """Merged group takes the osm_id and name of the largest polygon."""
        small = box(0, 0, 5, 5)       # area = 25
        large = box(2, 2, 20, 20)     # area = 324
        gdf = _make_gdf([
            {"osm_id": "small_id", "name": "Small Reserve", "geometry": small},
            {"osm_id": "large_id", "name": "Big Nation", "geometry": large},
        ])
        result = merge_overlapping_polygons(gdf, "osm_id", "name")
        assert len(result) == 1
        assert result.iloc[0]["osm_id"] == "large_id"
        assert result.iloc[0]["name"] == "Big Nation"

    def test_merged_geometry_is_union(self):
        """Merged polygon is the union of all group members."""
        a = box(0, 0, 10, 10)
        b = box(5, 5, 15, 15)
        expected_union = a.union(b)
        gdf = _make_gdf([
            {"osm_id": "1", "name": "A", "geometry": a},
            {"osm_id": "2", "name": "B", "geometry": b},
        ])
        result = merge_overlapping_polygons(gdf, "osm_id", "name")
        assert result.geometry.iloc[0].equals(expected_union)

    def test_small_fully_inside_large_merges(self):
        """A small polygon fully contained within a larger one is merged."""
        outer = box(0, 0, 100, 100)   # area = 10000
        inner = box(30, 30, 40, 40)   # area = 100
        gdf = _make_gdf([
            {"osm_id": "outer", "name": "Nation", "geometry": outer},
            {"osm_id": "inner", "name": "Reserve 1", "geometry": inner},
        ])
        result = merge_overlapping_polygons(gdf, "osm_id", "name")
        assert len(result) == 1
        assert result.iloc[0]["osm_id"] == "outer"
        assert result.iloc[0]["name"] == "Nation"

    def test_transitive_chaining(self):
        """A-overlaps-B, B-overlaps-C ⟹ A, B, C form one group."""
        a = box(0, 0, 10, 10)
        b = box(5, 0, 15, 10)
        c = box(12, 0, 22, 10)
        gdf = _make_gdf([
            {"osm_id": "1", "name": "A", "geometry": a},
            {"osm_id": "2", "name": "B", "geometry": b},
            {"osm_id": "3", "name": "C", "geometry": c},
        ])
        result = merge_overlapping_polygons(gdf, "osm_id", "name")
        assert len(result) == 1

    def test_multiple_groups(self):
        """Two separate overlapping clusters produce two merged features."""
        # Cluster 1
        a = box(0, 0, 10, 10)
        b = box(5, 5, 15, 15)
        # Cluster 2 (far away)
        c = box(1000, 1000, 1010, 1010)
        d = box(1005, 1005, 1015, 1015)
        # Solo feature
        e = box(5000, 5000, 5010, 5010)

        gdf = _make_gdf([
            {"osm_id": "1", "name": "A", "geometry": a},
            {"osm_id": "2", "name": "B", "geometry": b},
            {"osm_id": "3", "name": "C", "geometry": c},
            {"osm_id": "4", "name": "D", "geometry": d},
            {"osm_id": "5", "name": "E (solo)", "geometry": e},
        ])
        result = merge_overlapping_polygons(gdf, "osm_id", "name")
        # 2 merged groups + 1 solo = 3
        assert len(result) == 3

    def test_empty_gdf(self):
        """Empty GeoDataFrame returns empty."""
        gdf = gpd.GeoDataFrame(
            columns=["osm_id", "name", "geometry"],
            crs="EPSG:3005",
        )
        result = merge_overlapping_polygons(gdf, "osm_id", "name")
        assert len(result) == 0

    def test_single_feature(self):
        """Single feature passes through unchanged."""
        gdf = _make_gdf([
            {"osm_id": "1", "name": "Solo", "geometry": box(0, 0, 10, 10)},
        ])
        result = merge_overlapping_polygons(gdf, "osm_id", "name")
        assert len(result) == 1
        assert result.iloc[0]["osm_id"] == "1"

    def test_preserves_crs(self):
        """Output CRS matches input CRS."""
        gdf = _make_gdf([
            {"osm_id": "1", "name": "A", "geometry": box(0, 0, 10, 10)},
            {"osm_id": "2", "name": "B", "geometry": box(5, 5, 15, 15)},
        ])
        result = merge_overlapping_polygons(gdf, "osm_id", "name")
        assert result.crs == gdf.crs

    def test_preserves_extra_columns(self):
        """Non-id/name columns from the largest member are preserved."""
        gdf = _make_gdf([
            {"osm_id": "1", "name": "Small", "url": "http://small", "geometry": box(0, 0, 5, 5)},
            {"osm_id": "2", "name": "Large", "url": "http://large", "geometry": box(2, 2, 20, 20)},
        ])
        result = merge_overlapping_polygons(gdf, "osm_id", "name")
        assert len(result) == 1
        assert result.iloc[0]["url"] == "http://large"

    def test_many_small_inside_one_large(self):
        """Multiple small reserves inside one nation polygon → one merged feature."""
        nation = box(0, 0, 100, 100)
        reserves = [
            {"osm_id": f"r{i}", "name": f"Reserve {i}", "geometry": box(i*10, i*10, i*10+5, i*10+5)}
            for i in range(1, 8)
        ]
        features = [{"osm_id": "nation", "name": "Nation", "geometry": nation}] + reserves
        gdf = _make_gdf(features)
        result = merge_overlapping_polygons(gdf, "osm_id", "name")
        assert len(result) == 1
        assert result.iloc[0]["osm_id"] == "nation"
        assert result.iloc[0]["name"] == "Nation"

    def test_missing_id_field_raises(self):
        """Raises ValueError when id_field column is absent."""
        gdf = _make_gdf([{"osm_id": "1", "name": "A", "geometry": box(0, 0, 1, 1)}])
        with pytest.raises(ValueError, match="bad_field"):
            merge_overlapping_polygons(gdf, "bad_field", "name")

    def test_missing_name_field_raises(self):
        """Raises ValueError when name_field column is absent."""
        gdf = _make_gdf([{"osm_id": "1", "name": "A", "geometry": box(0, 0, 1, 1)}])
        with pytest.raises(ValueError, match="bad_name"):
            merge_overlapping_polygons(gdf, "osm_id", "bad_name")
