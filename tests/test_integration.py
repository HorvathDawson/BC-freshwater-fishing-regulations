"""
Integration tests for FWA preprocessing pipeline.

Tests complete workflow with small test dataset.

Run with: python -m pytest tests/test_integration.py -v -s
"""

import pytest
import tempfile
import shutil
from pathlib import Path
import geopandas as gpd
import pandas as pd
from shapely.geometry import LineString, Point, Polygon
import fiona

from fwa_modules.stream_preprocessing_simple import StreamPreprocessor
from fwa_modules.network_analysis import NetworkAnalyzer
from fwa_modules.models import ZoneAssignment


@pytest.fixture
def temp_workspace():
    """Create temporary workspace for tests."""
    temp_dir = Path(tempfile.mkdtemp())
    yield temp_dir
    shutil.rmtree(temp_dir)


@pytest.fixture
def mock_streams_gdb(temp_workspace):
    """Create mock streams GDB with test data."""
    gdb_path = temp_workspace / "test_streams.gdb"

    # Create simple test streams
    streams_data = {
        "LINEAR_FEATURE_ID": ["S1", "S2", "S3", "S4", "S5"],
        "FWA_WATERSHED_CODE": [
            "100-000000",  # Named main stem
            "100-239874",  # Named tributary
            "100-239874-112384",  # Unnamed 1 level away
            "100-239874-112384-456789",  # Unnamed 2 levels away (should be filtered)
            "100-239874",  # Unnamed braid (should get named)
        ],
        "GNIS_NAME": [
            "Fraser River",
            "Thompson River",
            None,
            None,
            None,
        ],
        "DOWNSTREAM_ROUTE_MEASURE": [0.0, 100.0, 200.0, 300.0, 150.0],
        "geometry": [
            LineString([(0, 0), (1, 0)]),
            LineString([(1, 0), (2, 0)]),
            LineString([(2, 0), (3, 0)]),
            LineString([(3, 0), (4, 0)]),
            LineString([(1, 0.1), (2, 0.1)]),  # Braid of Thompson
        ],
    }

    gdf = gpd.GeoDataFrame(streams_data, crs="EPSG:3005")
    gdf.to_file(str(gdb_path), layer="100A", driver="OpenFileGDB")

    return gdb_path


@pytest.fixture
def mock_lakes_gdb(temp_workspace):
    """Create mock lakes GDB with test data at realistic BC coordinates."""
    gdb_path = temp_workspace / "test_lakes.gdb"

    # Create test lakes using real BC WGS84 coordinates from unnamed_lakes.kml
    # Point 1: (-125.477748, 50.134073) - unnamed lake c - map b
    # Point 2: (-125.640115, 50.192516) - unnamed lake b - map a
    # Create small lakes around these points

    # Convert WGS84 points to EPSG:3005 to create polygons
    import geopandas as gpd
    from shapely.geometry import Point as ShapelyPoint

    # Points in WGS84
    point1_wgs84 = gpd.GeoSeries(
        [ShapelyPoint(-125.477748, 50.134073)], crs="EPSG:4326"
    )
    point2_wgs84 = gpd.GeoSeries(
        [ShapelyPoint(-125.640115, 50.192516)], crs="EPSG:4326"
    )

    # Convert to EPSG:3005
    point1_3005 = point1_wgs84.to_crs("EPSG:3005").iloc[0]
    point2_3005 = point2_wgs84.to_crs("EPSG:3005").iloc[0]

    # Create 200m x 200m boxes around each point
    x1, y1 = point1_3005.x, point1_3005.y
    x2, y2 = point2_3005.x, point2_3005.y

    lakes_data = {
        "WATERBODY_POLY_ID": [1, 2],
        "GNIS_NAME_1": ["Test Named Lake", None],  # One named, one unnamed
        "geometry": [
            Polygon(
                [
                    (x1 - 100, y1 - 100),
                    (x1 - 100, y1 + 100),
                    (x1 + 100, y1 + 100),
                    (x1 + 100, y1 - 100),
                ]
            ),
            Polygon(
                [
                    (x2 - 100, y2 - 100),
                    (x2 - 100, y2 + 100),
                    (x2 + 100, y2 + 100),
                    (x2 + 100, y2 - 100),
                ]
            ),
        ],
    }

    lakes_gdf = gpd.GeoDataFrame(lakes_data, crs="EPSG:3005")
    lakes_gdf.to_file(str(gdb_path), layer="FWA_LAKES_POLY", driver="OpenFileGDB")

    return gdb_path


class TestStreamPreprocessing:
    """Test stream preprocessing phase."""

    def test_stream_preprocessing(self, temp_workspace, mock_streams_gdb):
        """Test full stream preprocessing."""
        output_gdb = temp_workspace / "cleaned_streams.gdb"

        processor = StreamPreprocessor(mock_streams_gdb, output_gdb, test_mode=False)

        result = processor.run()

        # Simplified version outputs .gpkg instead of .gdb
        expected_output = temp_workspace / "cleaned_streams.gpkg"
        assert result == expected_output
        assert expected_output.exists()

        # Check stats
        assert processor.stats["total_streams_read"] == 5
        assert processor.stats["originally_named"] == 2
        assert processor.stats["braids_merged"] == 1  # S5 gets Thompson River name
        assert processor.stats["unnamed_filtered"] == 1  # S4 is 2 levels away
        assert processor.stats["final_count"] == 4  # S1, S2, S3, S5

    def test_braid_merging(self, temp_workspace, mock_streams_gdb):
        """Test that braids get proper names."""
        output_gdb = temp_workspace / "cleaned_streams.gdb"

        processor = StreamPreprocessor(mock_streams_gdb, output_gdb, test_mode=False)

        processor.run()

        # Load results (GPKG output, layer name normalized to _100A)
        output_gpkg = temp_workspace / "cleaned_streams.gpkg"
        result = gpd.read_file(str(output_gpkg), layer="_100A")

        # S5 should have Thompson River name now
        braid_stream = result[result["LINEAR_FEATURE_ID"] == "S5"]
        assert len(braid_stream) == 1
        assert braid_stream.iloc[0]["GNIS_NAME"] == "Thompson River"

    def test_unnamed_filtering(self, temp_workspace, mock_streams_gdb):
        """Test that distant unnamed streams are filtered."""
        output_gdb = temp_workspace / "cleaned_streams.gdb"

        processor = StreamPreprocessor(mock_streams_gdb, output_gdb, test_mode=False)

        processor.run()

        # Load results (GPKG output, layer name normalized to _100A)
        output_gpkg = temp_workspace / "cleaned_streams.gpkg"
        result = gpd.read_file(str(output_gpkg), layer="_100A")

        # S4 should be filtered (2 levels away)
        assert "S4" not in result["LINEAR_FEATURE_ID"].values

        # S3 should be kept (1 level away)
        assert "S3" in result["LINEAR_FEATURE_ID"].values


class TestZoneAssignment:
    """Test zone assignment functionality."""

    def test_single_zone_assignment(self):
        """Test feature entirely in one zone."""
        assignment = ZoneAssignment()
        assignment.add_zone("2", 1.0)

        assert assignment.primary_zone() == "2"
        assert not assignment.is_clipped

    def test_multi_zone_assignment(self):
        """Test feature spanning multiple zones."""
        assignment = ZoneAssignment()
        assignment.add_zone("2", 0.5)
        assignment.add_zone("3", 0.3)
        assignment.add_zone("1", 0.2)

        assert assignment.primary_zone() == "2"
        assert assignment.is_clipped
        assert assignment.zones == ["2", "3", "1"]
        assert sum(assignment.proportions) == pytest.approx(1.0)


if __name__ == "__main__":
    pytest.main([__file__, "-v", "-s"])
