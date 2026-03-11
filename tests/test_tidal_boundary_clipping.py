"""
Tests for tidal boundary clipping — ensures streams are correctly split
at the tidal polygon boundary and wetlands inside are removed.

Uses synthetic shapely geometries (no GPKG required).
"""

import pytest
from shapely.geometry import LineString, Polygon, MultiLineString, box
from shapely.prepared import prep

from regulation_mapping.canonical_store import CanonicalDataStore


# ---------------------------------------------------------------------------
# Shared fixture: a simple rectangular tidal polygon
# ---------------------------------------------------------------------------


@pytest.fixture
def tidal_polygon():
    """A 100×100 square tidal polygon centred at the origin."""
    return box(-50, -50, 50, 50)


@pytest.fixture
def tidal_prep(tidal_polygon):
    return prep(tidal_polygon)


# ---------------------------------------------------------------------------
# Helper: build a minimal feature dict the static methods expect
# ---------------------------------------------------------------------------


def _stream(coords, **extra):
    geom = LineString(coords)
    return {
        "geometry": geom,
        "feature_type": "streams",
        "length_m": geom.length,
        **extra,
    }


def _polygon_feat(coords, feature_type, **extra):
    geom = Polygon(coords)
    return {"geometry": geom, "feature_type": feature_type, **extra}


# ===========================================================================
# Stream clipping tests
# ===========================================================================


class TestClipStreamsAtTidalBoundary:

    def test_stream_fully_outside_unchanged(self, tidal_polygon, tidal_prep):
        """A stream entirely outside the tidal polygon passes through."""
        feat = _stream([(100, 0), (200, 0)])
        result = CanonicalDataStore._clip_streams_at_tidal_boundary(
            [feat], tidal_polygon, tidal_prep
        )
        assert len(result) == 1
        assert result[0]["geometry"].equals(feat["geometry"])

    def test_stream_fully_inside_removed(self, tidal_polygon, tidal_prep):
        """A stream entirely inside the tidal polygon is removed."""
        feat = _stream([(0, 0), (10, 10)])
        result = CanonicalDataStore._clip_streams_at_tidal_boundary(
            [feat], tidal_polygon, tidal_prep
        )
        assert len(result) == 0

    def test_stream_crossing_boundary_split(self, tidal_polygon, tidal_prep):
        """A stream crossing the boundary is clipped; only the outside part remains."""
        # Runs from well outside (-100,0) through the tidal box to inside (0,0)
        feat = _stream([(-100, 0), (0, 0)])
        result = CanonicalDataStore._clip_streams_at_tidal_boundary(
            [feat], tidal_polygon, tidal_prep
        )
        assert len(result) == 1
        clipped = result[0]["geometry"]
        # The outside portion should start at -100 and end near -50
        assert clipped.coords[0][0] == pytest.approx(-100, abs=0.1)
        # Should end at or near the tidal boundary (x ≈ -50)
        assert clipped.coords[-1][0] == pytest.approx(-50, abs=0.1)

    def test_stream_crossing_twice_keeps_both_tails(self, tidal_polygon, tidal_prep):
        """A stream that enters and exits keeps both outside portions."""
        # Runs from outside-left through the tidal box to outside-right
        feat = _stream([(-100, 0), (100, 0)])
        result = CanonicalDataStore._clip_streams_at_tidal_boundary(
            [feat], tidal_polygon, tidal_prep
        )
        assert len(result) == 1
        geom = result[0]["geometry"]
        # Should be a MultiLineString with the two outside segments
        if isinstance(geom, MultiLineString):
            assert len(geom.geoms) == 2
        else:
            # merge_lines may produce a single LineString if they happen to connect
            assert geom.length > 0

    def test_tiny_clipped_fragment_kept(self, tidal_polygon, tidal_prep):
        """Short freshwater remnants are kept — they may be real river-mouth segments."""
        # Stream that barely pokes outside the boundary (0.5 units outside)
        feat = _stream([(-50.5, 0), (0, 0)])
        result = CanonicalDataStore._clip_streams_at_tidal_boundary(
            [feat], tidal_polygon, tidal_prep
        )
        # The outside portion is ~0.5 units — kept because it is real geometry.
        # Only zero-length degenerate artefacts are dropped (handled inside
        # extract_line_components, not via an arbitrary length threshold).
        assert len(result) == 1
        assert result[0]["geometry"].length == pytest.approx(0.5, abs=1e-6)

    def test_empty_geometry_skipped(self, tidal_polygon, tidal_prep):
        """Features with None geometry are silently skipped."""
        feat = {"geometry": None, "feature_type": "streams"}
        result = CanonicalDataStore._clip_streams_at_tidal_boundary(
            [feat], tidal_polygon, tidal_prep
        )
        assert len(result) == 0

    def test_multiple_streams_mixed(self, tidal_polygon, tidal_prep):
        """Process a batch: some inside, some outside, some crossing."""
        s_outside = _stream([(100, 0), (200, 0)])
        s_inside = _stream([(0, 0), (10, 10)])
        s_crossing = _stream([(-100, 0), (0, 0)])
        result = CanonicalDataStore._clip_streams_at_tidal_boundary(
            [s_outside, s_inside, s_crossing], tidal_polygon, tidal_prep
        )
        # outside kept, inside removed, crossing clipped → 2 features
        assert len(result) == 2

    def test_length_m_updated_after_clip(self, tidal_polygon, tidal_prep):
        """Clipped features get their length_m recalculated."""
        feat = _stream([(-100, 0), (0, 0)])
        result = CanonicalDataStore._clip_streams_at_tidal_boundary(
            [feat], tidal_polygon, tidal_prep
        )
        assert len(result) == 1
        assert result[0]["length_m"] == pytest.approx(result[0]["geometry"].length)

    def test_empty_input_returns_empty(self, tidal_polygon, tidal_prep):
        """No crash on empty input."""
        result = CanonicalDataStore._clip_streams_at_tidal_boundary(
            [], tidal_polygon, tidal_prep
        )
        assert result == []


# ===========================================================================
# Wetland removal tests
# ===========================================================================


class TestRemoveTidalWetlands:

    def test_wetland_inside_removed(self, tidal_polygon):
        """A wetland intersecting the tidal polygon is removed."""
        feat = _polygon_feat(
            [(0, 0), (10, 0), (10, 10), (0, 10), (0, 0)],
            feature_type="wetlands",
        )
        result = CanonicalDataStore._remove_tidal_wetlands([feat], tidal_polygon)
        assert len(result) == 0

    def test_wetland_outside_kept(self, tidal_polygon):
        """A wetland entirely outside the tidal polygon is kept."""
        feat = _polygon_feat(
            [(100, 100), (110, 100), (110, 110), (100, 110), (100, 100)],
            feature_type="wetlands",
        )
        result = CanonicalDataStore._remove_tidal_wetlands([feat], tidal_polygon)
        assert len(result) == 1

    def test_wetland_partially_overlapping_removed(self, tidal_polygon):
        """A wetland that partially overlaps tidal is removed (intersects=True)."""
        feat = _polygon_feat(
            [(40, 40), (60, 40), (60, 60), (40, 60), (40, 40)],
            feature_type="wetlands",
        )
        result = CanonicalDataStore._remove_tidal_wetlands([feat], tidal_polygon)
        assert len(result) == 0

    def test_lake_inside_kept(self, tidal_polygon):
        """Lakes inside the tidal polygon are NOT removed."""
        feat = _polygon_feat(
            [(0, 0), (10, 0), (10, 10), (0, 10), (0, 0)],
            feature_type="lakes",
        )
        result = CanonicalDataStore._remove_tidal_wetlands([feat], tidal_polygon)
        assert len(result) == 1

    def test_manmade_inside_kept(self, tidal_polygon):
        """Manmade waterbodies inside the tidal polygon are NOT removed."""
        feat = _polygon_feat(
            [(0, 0), (10, 0), (10, 10), (0, 10), (0, 0)],
            feature_type="manmade",
        )
        result = CanonicalDataStore._remove_tidal_wetlands([feat], tidal_polygon)
        assert len(result) == 1

    def test_mixed_batch(self, tidal_polygon):
        """Batch with wetland inside, wetland outside, and lake inside."""
        w_inside = _polygon_feat(
            [(0, 0), (10, 0), (10, 10), (0, 10), (0, 0)],
            feature_type="wetlands",
        )
        w_outside = _polygon_feat(
            [(200, 200), (210, 200), (210, 210), (200, 210), (200, 200)],
            feature_type="wetlands",
        )
        lake_inside = _polygon_feat(
            [(0, 0), (10, 0), (10, 10), (0, 10), (0, 0)],
            feature_type="lakes",
        )
        result = CanonicalDataStore._remove_tidal_wetlands(
            [w_inside, w_outside, lake_inside], tidal_polygon
        )
        # wetland inside removed, wetland outside kept, lake kept
        assert len(result) == 2
        types = [f["feature_type"] for f in result]
        assert "wetlands" in types
        assert "lakes" in types

    def test_empty_geometry_wetland_skipped(self, tidal_polygon):
        """Wetland with None geometry is silently skipped."""
        feat = {"geometry": None, "feature_type": "wetlands"}
        result = CanonicalDataStore._remove_tidal_wetlands([feat], tidal_polygon)
        assert len(result) == 0

    def test_empty_input_returns_empty(self, tidal_polygon):
        """No crash on empty input."""
        result = CanonicalDataStore._remove_tidal_wetlands([], tidal_polygon)
        assert result == []
