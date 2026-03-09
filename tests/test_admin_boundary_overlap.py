"""
Tests for admin area boundary-touch filtering — ensures large lakes that
barely touch an admin boundary (artifact overlap) are not matched.

Uses real geometries from bc_fisheries_data.gpkg:
    - Malcolm Knapp Research Forest (OSM admin 1166294466)
    - Pitt Lake (poly 700111132): 53.4M m², intersection 851 m² = 0.0016%
      → boundary artifact, should NOT be matched
    - Mike Lake (poly 700111364): 50k m², intersection 0.04 m² = 0.0001%
      → boundary artifact, should NOT be matched
    - Katherine Lake (poly 700111246): 180k m², 100% inside
      → fully inside, SHOULD be matched
    - Peaceful Lake (poly 700112184): 3.5k m², 36% overlap
      → real partial overlap, SHOULD be matched
    - Goose Lake (poly 700111314): 76k m², 2.2% overlap
      → small but real overlap, borderline
"""

import os
import sys
from pathlib import Path

import pytest
import shapely
import numpy as np

# ---------------------------------------------------------------------------
# Skip entire module if GeoPackage is not available
# ---------------------------------------------------------------------------

GPKG_PATH = Path(__file__).resolve().parent.parent / "data" / "bc_fisheries_data.gpkg"
pytestmark = pytest.mark.skipif(
    not GPKG_PATH.exists(), reason="GeoPackage not available"
)


# ---------------------------------------------------------------------------
# Helpers — load specific geometries from the GeoPackage
# ---------------------------------------------------------------------------


def _load_knapp_geometry():
    """Load the Malcolm Knapp Research Forest boundary polygon."""
    import geopandas as gpd

    gdf = gpd.read_file(GPKG_PATH, layer="osm_admin_boundaries")
    knapp = gdf[gdf["osm_id"] == 1166294466]
    assert len(knapp) == 1, "Expected exactly one Malcolm Knapp polygon"
    return knapp.geometry.values[0]


def _load_lake_geometry(poly_id: int):
    """Load a single lake polygon by WATERBODY_POLY_ID."""
    import geopandas as gpd

    gdf = gpd.read_file(
        GPKG_PATH,
        layer="lakes",
        where=f"WATERBODY_POLY_ID = {poly_id}",
    )
    assert len(gdf) == 1, f"Expected exactly one lake for poly_id={poly_id}"
    return gdf.geometry.values[0]


def _compute_overlap_pct(lake_geom, admin_geom) -> float:
    """Return intersection area as a percentage of the lake area."""
    if not lake_geom.intersects(admin_geom):
        return 0.0
    ixn = lake_geom.intersection(admin_geom)
    return (ixn.area / lake_geom.area) * 100.0 if lake_geom.area > 0 else 0.0


# ---------------------------------------------------------------------------
# Real geometry fixtures (cached per session — GPKG reads are expensive)
# ---------------------------------------------------------------------------


@pytest.fixture(scope="session")
def knapp_geom():
    return _load_knapp_geometry()


@pytest.fixture(scope="session")
def pitt_lake_geom():
    return _load_lake_geometry(700111132)


@pytest.fixture(scope="session")
def mike_lake_geom():
    return _load_lake_geometry(700111364)


@pytest.fixture(scope="session")
def katherine_lake_geom():
    return _load_lake_geometry(700111246)


@pytest.fixture(scope="session")
def peaceful_lake_geom():
    return _load_lake_geometry(700112184)


@pytest.fixture(scope="session")
def goose_lake_geom():
    return _load_lake_geometry(700111314)


# ===================================================================
# Test: raw intersection (current behaviour — demonstrates the bug)
# ===================================================================


class TestCurrentIntersectsBehaviour:
    """Show that shapely.intersects currently returns True for boundary
    artifacts — this is the root cause of the bug."""

    def test_pitt_lake_intersects_knapp(self, knapp_geom, pitt_lake_geom):
        """Pitt Lake registers as intersecting Knapp despite only 0.0016%
        overlap.  This is the bug — find_features_in_admin_area will
        include it."""
        assert pitt_lake_geom.intersects(knapp_geom) is True
        pct = _compute_overlap_pct(pitt_lake_geom, knapp_geom)
        assert pct < 0.01, f"Expected < 0.01% overlap, got {pct:.4f}%"

    def test_mike_lake_intersects_knapp(self, knapp_geom, mike_lake_geom):
        """Mike Lake has only 0.0001% overlap — also a boundary artifact."""
        assert mike_lake_geom.intersects(knapp_geom) is True
        pct = _compute_overlap_pct(mike_lake_geom, knapp_geom)
        assert pct < 0.001, f"Expected < 0.001% overlap, got {pct:.6f}%"

    def test_katherine_lake_fully_inside(self, knapp_geom, katherine_lake_geom):
        """Katherine Lake is 100% inside Knapp — correctly matched."""
        assert katherine_lake_geom.intersects(knapp_geom) is True
        pct = _compute_overlap_pct(katherine_lake_geom, knapp_geom)
        assert pct > 99.0

    def test_peaceful_lake_partial_overlap(self, knapp_geom, peaceful_lake_geom):
        """Peaceful Lake has ~36% overlap — a real intersection."""
        assert peaceful_lake_geom.intersects(knapp_geom) is True
        pct = _compute_overlap_pct(peaceful_lake_geom, knapp_geom)
        assert 30.0 < pct < 45.0

    def test_goose_lake_small_overlap(self, knapp_geom, goose_lake_geom):
        """Goose Lake has ~2.2% overlap — small but real area."""
        assert goose_lake_geom.intersects(knapp_geom) is True
        pct = _compute_overlap_pct(goose_lake_geom, knapp_geom)
        assert 1.0 < pct < 5.0


# ===================================================================
# Test: overlap filter should exclude boundary artifacts
# ===================================================================

# Proposed threshold: polygon features with < 1% overlap are considered
# boundary artifacts and should be excluded from admin matches.
OVERLAP_THRESHOLD_PCT = 1.0


def _should_match_admin(
    lake_geom, admin_geom, threshold_pct=OVERLAP_THRESHOLD_PCT
) -> bool:
    """Proposed filter: True if the lake has enough overlap to be a real
    match, not just a boundary touch."""
    if not lake_geom.intersects(admin_geom):
        return False
    ixn = lake_geom.intersection(admin_geom)
    pct = (ixn.area / lake_geom.area) * 100.0 if lake_geom.area > 0 else 0.0
    return pct >= threshold_pct


class TestOverlapFilterExcludesBoundaryArtifacts:
    """With a minimum overlap threshold, boundary artifacts are filtered
    out but real overlaps are preserved."""

    def test_pitt_lake_excluded(self, knapp_geom, pitt_lake_geom):
        """Pitt Lake (0.0016% overlap) should be EXCLUDED — boundary artifact."""
        assert _should_match_admin(pitt_lake_geom, knapp_geom) is False

    def test_mike_lake_excluded(self, knapp_geom, mike_lake_geom):
        """Mike Lake (0.0001% overlap) should be EXCLUDED — boundary artifact."""
        assert _should_match_admin(mike_lake_geom, knapp_geom) is False

    def test_katherine_lake_included(self, knapp_geom, katherine_lake_geom):
        """Katherine Lake (100% overlap) should be INCLUDED — fully inside."""
        assert _should_match_admin(katherine_lake_geom, knapp_geom) is True

    def test_peaceful_lake_included(self, knapp_geom, peaceful_lake_geom):
        """Peaceful Lake (~36% overlap) should be INCLUDED — real overlap."""
        assert _should_match_admin(peaceful_lake_geom, knapp_geom) is True

    def test_goose_lake_included(self, knapp_geom, goose_lake_geom):
        """Goose Lake (~2.2% overlap) should be INCLUDED — above threshold."""
        assert _should_match_admin(goose_lake_geom, knapp_geom) is True


# ===================================================================
# Test: stream features should NOT be filtered by overlap
#   (streams are linear — intersection area is always ~0)
# ===================================================================


class TestStreamFeaturesNotFiltered:
    """Overlap filtering should only apply to polygon features (lakes,
    wetlands, manmade).  Streams are linear and their area is 0, so
    percentage-based filtering must not be applied to them."""

    def test_line_intersect_not_filtered(self, knapp_geom):
        """A stream line crossing the admin boundary should always match,
        regardless of 'overlap' percentage (which is meaningless for lines)."""
        from shapely.geometry import LineString

        # Create a line that crosses the Knapp boundary
        bounds = knapp_geom.bounds  # (minx, miny, maxx, maxy)
        mid_y = (bounds[1] + bounds[3]) / 2
        # Line going from outside to inside
        line = LineString(
            [
                (bounds[0] - 1000, mid_y),
                (bounds[0] + 1000, mid_y),
            ]
        )
        assert line.intersects(knapp_geom) is True
        # Area is 0 for lines — should not be filtered
        assert line.area == 0.0
