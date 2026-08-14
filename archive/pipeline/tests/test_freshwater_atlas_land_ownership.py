"""Tests for FreshWaterAtlas._replace_untitled_provincial_with_complement().

Exercises the method directly against a tiny synthetic geometry (a 10x10
square "province") rather than the real ~700MB dataset — the pipeline
itself (GPKG read, tippecanoe, etc.) is out of scope here.
"""
from __future__ import annotations

from shapely.geometry import box

from pipeline.atlas.freshwater_atlas import FreshWaterAtlas
from pipeline.atlas.models import AdminRecord


def _make_atlas() -> FreshWaterAtlas:
    """Construct a FreshWaterAtlas instance without running the real GPKG-based build."""
    atlas = object.__new__(FreshWaterAtlas)
    atlas.land_parcels_crown = {}
    atlas.aboriginal_lands = {}
    atlas._bc_boundary = None
    return atlas


def _record(owner_type: str, geom) -> AdminRecord:
    return AdminRecord(
        admin_id=owner_type,
        geometry=geom,
        display_name=owner_type,
        admin_type="land_parcels_crown",
        area=geom.area,
    )


class TestReplaceUntitledProvincialWithComplement:
    def test_complement_fills_the_whole_gap(self):
        """With no _bc_boundary and nothing accounted for, the province
        boundary minus the accounted-for polygons should equal the leftover
        area — the whole point of the fix."""
        atlas = _make_atlas()
        atlas._bc_boundary = box(0, 0, 10, 10)  # 100 units² "province"
        # Private takes the left third, Federal a small corner — everything
        # else (including the old sparse "Untitled Provincial") is the gap.
        atlas.land_parcels_crown["Private"] = _record("Private", box(0, 0, 3, 10))
        atlas.land_parcels_crown["Federal"] = _record("Federal", box(9, 9, 10, 10))
        # A stale, sparse "Untitled Provincial" from the raw parcel fabric —
        # must be fully replaced, not merged with.
        atlas.land_parcels_crown["Untitled Provincial"] = _record(
            "Untitled Provincial", box(4, 4, 4.1, 4.1)
        )

        atlas._replace_untitled_provincial_with_complement()

        result = atlas.land_parcels_crown["Untitled Provincial"]
        # Expected area: 100 - (30 private + 1 federal) = 69 — note this is
        # NOT 69 + 0.01 (the old sparse sliver) or 0.01 alone, proving the
        # stale entry was fully replaced by the complement, not unioned
        # with it or left untouched.
        assert abs(result.area - 69.0) < 1e-6

    def test_aboriginal_lands_excluded_from_complement(self):
        """Indigenous/treaty land must not be swallowed into 'Untitled
        Provincial' — it's legally distinct and already carries its own
        advisory."""
        atlas = _make_atlas()
        atlas._bc_boundary = box(0, 0, 10, 10)
        atlas.aboriginal_lands["IR1"] = _record("Indian Reserve", box(0, 0, 4, 4))

        atlas._replace_untitled_provincial_with_complement()

        result = atlas.land_parcels_crown["Untitled Provincial"]
        assert abs(result.area - (100.0 - 16.0)) < 1e-6
        assert not result.geometry.intersects(box(1, 1, 2, 2))  # inside the reserve

    def test_no_bc_boundary_leaves_existing_data_untouched(self):
        """If the province boundary couldn't be built (e.g. WMU layer
        missing), fall back to leaving whatever the raw parcel fabric had —
        don't crash, don't silently produce an empty/wrong polygon."""
        atlas = _make_atlas()
        atlas._bc_boundary = None
        sparse = _record("Untitled Provincial", box(4, 4, 4.1, 4.1))
        atlas.land_parcels_crown["Untitled Provincial"] = sparse

        atlas._replace_untitled_provincial_with_complement()

        assert atlas.land_parcels_crown["Untitled Provincial"] is sparse

    def test_fully_accounted_for_province_yields_empty_complement(self):
        """If every OWNER_TYPE + aboriginal_lands polygon already tiles the
        whole province, the complement should end up empty rather than
        error, and the prior entry should be left in place."""
        atlas = _make_atlas()
        atlas._bc_boundary = box(0, 0, 10, 10)
        atlas.land_parcels_crown["Private"] = _record("Private", box(0, 0, 10, 10))
        sparse = _record("Untitled Provincial", box(0, 0, 0.1, 0.1))
        atlas.land_parcels_crown["Untitled Provincial"] = sparse

        atlas._replace_untitled_provincial_with_complement()

        # Empty-complement guard kicks in — existing entry untouched.
        assert atlas.land_parcels_crown["Untitled Provincial"] is sparse
