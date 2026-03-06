"""
Unit tests for regulation_mapping.geometry_utils.

All functions are pure / stateless — no mocking required.
"""

import pytest
from shapely.geometry import (
    GeometryCollection,
    LineString,
    MultiLineString,
    MultiPolygon,
    Point,
    Polygon,
)

from regulation_mapping.geometry_utils import (
    extract_geoms,
    extract_line_components,
    geoms_to_wgs84_bbox,
    merge_lines,
    round_coords,
)


# ===================================================================
# round_coords
# ===================================================================


class TestRoundCoords:
    """Tests for coordinate-precision rounding."""

    def test_rounds_point_coordinates(self):
        geom_dict = {
            "type": "Point",
            "coordinates": [1.123456789012345, 2.987654321098765],
        }
        result = round_coords(geom_dict, precision=3)
        assert result["coordinates"] == [1.123, 2.988]

    def test_rounds_linestring_coordinates(self):
        geom_dict = {
            "type": "LineString",
            "coordinates": [
                [1.111111111, 2.222222222],
                [3.333333333, 4.444444444],
            ],
        }
        result = round_coords(geom_dict, precision=2)
        assert result["coordinates"] == [[1.11, 2.22], [3.33, 4.44]]

    def test_preserves_other_keys(self):
        geom_dict = {
            "type": "Point",
            "coordinates": [1.123456789, 2.987654321],
            "extra_key": "should_survive",
        }
        result = round_coords(geom_dict)
        assert result["extra_key"] == "should_survive"
        assert result["type"] == "Point"

    def test_default_precision_is_7(self):
        geom_dict = {
            "type": "Point",
            "coordinates": [1.12345678901234, 2.12345678901234],
        }
        result = round_coords(geom_dict)
        assert result["coordinates"] == [1.1234568, 2.1234568]

    def test_integer_coordinates_unchanged(self):
        geom_dict = {"type": "Point", "coordinates": [1, 2]}
        result = round_coords(geom_dict, precision=5)
        assert result["coordinates"] == [1, 2]

    def test_nested_polygon_coordinates(self):
        """Polygon coordinates are triply nested: [ring[coord[x,y]]]."""
        geom_dict = {
            "type": "Polygon",
            "coordinates": [
                [
                    [0.123456789, 0.987654321],
                    [1.123456789, 0.987654321],
                    [1.123456789, 1.987654321],
                    [0.123456789, 0.987654321],
                ]
            ],
        }
        result = round_coords(geom_dict, precision=2)
        ring = result["coordinates"][0]
        assert ring[0] == [0.12, 0.99]
        assert ring[1] == [1.12, 0.99]


# ===================================================================
# merge_lines
# ===================================================================


class TestMergeLines:
    """Tests for line geometry merging."""

    def test_single_line_returns_linestring(self):
        line = LineString([(0, 0), (1, 1)])
        result = merge_lines([line])
        assert isinstance(result, LineString)
        assert result.equals(line)

    def test_multiple_lines_returns_multilinestring(self):
        l1 = LineString([(0, 0), (1, 1)])
        l2 = LineString([(2, 2), (3, 3)])
        result = merge_lines([l1, l2])
        assert isinstance(result, MultiLineString)
        assert len(list(result.geoms)) == 2

    def test_empty_list_raises_valueerror(self):
        with pytest.raises(ValueError, match="empty"):
            merge_lines([])

    def test_three_lines(self):
        lines = [
            LineString([(0, 0), (1, 0)]),
            LineString([(1, 0), (2, 0)]),
            LineString([(2, 0), (3, 0)]),
        ]
        result = merge_lines(lines)
        assert isinstance(result, MultiLineString)
        assert len(list(result.geoms)) == 3


# ===================================================================
# geoms_to_wgs84_bbox
# ===================================================================


class TestGeomsToWgs84Bbox:
    """Tests for EPSG:3005 → WGS84 bounding box conversion."""

    def test_returns_four_element_tuple(self):
        """Bbox should be (minx, miny, maxx, maxy) in WGS84."""
        # Create geometry roughly in the middle of BC (EPSG:3005)
        geom = Polygon(
            [
                (1000000, 500000),
                (1100000, 500000),
                (1100000, 600000),
                (1000000, 600000),
                (1000000, 500000),
            ]
        )
        bbox = geoms_to_wgs84_bbox([geom])
        assert len(bbox) == 4
        minx, miny, maxx, maxy = bbox
        # WGS84 longitude for BC is roughly -140 to -114
        # These are approximate — just sanity-check the range
        assert -180 < minx < 0
        assert -180 < maxx < 0
        assert minx < maxx
        assert miny < maxy

    def test_empty_list_raises_valueerror(self):
        with pytest.raises(ValueError, match="empty"):
            geoms_to_wgs84_bbox([])

    def test_multiple_geometries_union_bbox(self):
        """Bbox should encompass all input geometries."""
        g1 = Point(1000000, 500000).buffer(100)
        g2 = Point(1200000, 700000).buffer(100)
        bbox = geoms_to_wgs84_bbox([g1, g2])
        minx, miny, maxx, maxy = bbox
        # The bbox of two distant points should be wider than a single point
        single_bbox = geoms_to_wgs84_bbox([g1])
        assert (maxx - minx) > (single_bbox[2] - single_bbox[0])


# ===================================================================
# extract_line_components
# ===================================================================


class TestExtractLineComponents:
    """Tests for extracting linear parts from geometry results."""

    def test_linestring_returned_in_list(self):
        line = LineString([(0, 0), (1, 1)])
        result = extract_line_components(line)
        assert len(result) == 1
        assert result[0].equals(line)

    def test_multilinestring_decomposed(self):
        ml = MultiLineString(
            [
                [(0, 0), (1, 1)],
                [(2, 2), (3, 3)],
            ]
        )
        result = extract_line_components(ml)
        assert len(result) == 2

    def test_geometry_collection_filters_points(self):
        """Points and zero-length lines should be dropped."""
        gc = GeometryCollection(
            [
                Point(0, 0),
                LineString([(0, 0), (1, 1)]),
                Point(5, 5),
            ]
        )
        result = extract_line_components(gc)
        assert len(result) == 1
        assert isinstance(result[0], LineString)

    def test_empty_geometry_returns_empty(self):
        empty = GeometryCollection()
        assert extract_line_components(empty) == []

    def test_none_returns_empty(self):
        assert extract_line_components(None) == []

    def test_zero_length_line_dropped(self):
        """A degenerate line (single point repeated) should be dropped."""
        line = LineString([(0, 0), (0, 0)])
        assert extract_line_components(line) == []

    def test_polygon_returns_empty(self):
        """Polygons are not linear — should return empty."""
        poly = Polygon([(0, 0), (1, 0), (1, 1), (0, 0)])
        assert extract_line_components(poly) == []

    def test_geometry_collection_with_multilinestring(self):
        """MultiLineString inside a GeometryCollection should be decomposed."""
        ml = MultiLineString([[(0, 0), (1, 1)], [(2, 2), (3, 3)]])
        gc = GeometryCollection([Point(0, 0), ml])
        result = extract_line_components(gc)
        assert len(result) == 2


# ===================================================================
# extract_geoms
# ===================================================================


class TestExtractGeoms:
    """Tests for decomposing multi-geometries."""

    def test_simple_geometry_wrapped_in_list(self):
        line = LineString([(0, 0), (1, 1)])
        result = extract_geoms(line)
        assert len(result) == 1
        assert result[0].equals(line)

    def test_multilinestring_decomposed(self):
        ml = MultiLineString([[(0, 0), (1, 1)], [(2, 2), (3, 3)]])
        result = extract_geoms(ml)
        assert len(result) == 2

    def test_multipolygon_decomposed(self):
        mp = MultiPolygon(
            [
                Polygon([(0, 0), (1, 0), (1, 1), (0, 0)]),
                Polygon([(2, 2), (3, 2), (3, 3), (2, 2)]),
            ]
        )
        result = extract_geoms(mp)
        assert len(result) == 2

    def test_point_wrapped_in_list(self):
        pt = Point(1, 2)
        result = extract_geoms(pt)
        assert len(result) == 1
        assert result[0].equals(pt)
