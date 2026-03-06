"""
Tests for regulation_mapping.linker – WaterbodyLinker.

Uses a lightweight FakeGazetteer that responds to search/lookup calls
without needing a real metadata pickle or GPKG file.  Fixtures are
drawn from patterns found in real parsed_results.json data.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, List, Optional
from unittest.mock import patch

import pytest

from fwa_pipeline.metadata_builder import FeatureType
from fwa_pipeline.metadata_gazetteer import FWAFeature
from regulation_mapping.linker import (
    DirectMatchError,
    LinkingResult,
    LinkStatus,
    WaterbodyLinker,
)
from regulation_mapping.linking_corrections import (
    AdminDirectMatch,
    DirectMatch,
    ManualCorrections,
    NameVariationLink,
    SkipEntry,
    UngazettedWaterbody,
)
from regulation_mapping.admin_target import AdminTarget


# ---------------------------------------------------------------------------
# Fake gazetteer that responds to the method calls the linker uses
# ---------------------------------------------------------------------------


class FakeLinkerGazetteer:
    """Minimal gazetteer stub for WaterbodyLinker tests.

    Pre-populate ``features_by_name``, ``features_by_gnis_id``,
    ``features_by_watershed_code``, ``features_by_waterbody_key``,
    ``features_by_blue_line_key``, etc. with lists of FWAFeature objects.
    """

    def __init__(self) -> None:
        # name (title-cased) → [FWAFeature, ...]
        self.features_by_name: Dict[str, List[FWAFeature]] = {}
        # gnis_id → [FWAFeature, ...]
        self.features_by_gnis_id: Dict[str, List[FWAFeature]] = {}
        # watershed_code → [FWAFeature, ...]
        self.features_by_watershed_code: Dict[str, List[FWAFeature]] = {}
        # waterbody_key → [FWAFeature, ...]
        self.features_by_waterbody_key: Dict[str, List[FWAFeature]] = {}
        # blue_line_key → [FWAFeature, ...]
        self.features_by_blue_line_key: Dict[str, List[FWAFeature]] = {}
        # poly_id → FWAFeature
        self.features_by_poly_id: Dict[str, FWAFeature] = {}
        # linear_feature_id → FWAFeature
        self.features_by_linear_id: Dict[str, FWAFeature] = {}
        # ungazetted_id → FWAFeature
        self.features_by_ungazetted_id: Dict[str, FWAFeature] = {}

    # --- Methods the linker calls through resolve_direct_match_features ---

    def search(self, name: str, zone_number: Optional[str] = None) -> List[FWAFeature]:
        normalized = name.strip().title()
        matches = list(self.features_by_name.get(normalized, []))
        if zone_number:
            matches = [f for f in matches if zone_number in f.zones]
        return matches

    def search_by_gnis_id(self, gnis_id: str) -> List[FWAFeature]:
        return list(self.features_by_gnis_id.get(str(gnis_id), []))

    def search_by_watershed_code(self, code: str) -> List[FWAFeature]:
        return list(self.features_by_watershed_code.get(code, []))

    def get_waterbody_by_key(self, key: str) -> List[FWAFeature]:
        return list(self.features_by_waterbody_key.get(str(key), []))

    def search_by_blue_line_key(self, blk: str) -> List[FWAFeature]:
        return list(self.features_by_blue_line_key.get(str(blk), []))

    def get_polygon_by_id(self, poly_id: str) -> Optional[FWAFeature]:
        return self.features_by_poly_id.get(str(poly_id))

    def get_stream_by_id(self, linear_id: str) -> Optional[FWAFeature]:
        return self.features_by_linear_id.get(str(linear_id))

    def get_ungazetted_by_id(self, ungaz_id: str) -> Optional[FWAFeature]:
        return self.features_by_ungazetted_id.get(ungaz_id)

    # Convenience to bulk-register features
    def add_feature(self, feature: FWAFeature) -> None:
        """Register a feature in all relevant lookup indices."""
        if feature.gnis_name:
            key = feature.gnis_name.strip().title()
            self.features_by_name.setdefault(key, []).append(feature)
        if feature.gnis_id:
            self.features_by_gnis_id.setdefault(feature.gnis_id, []).append(feature)
        if feature.fwa_watershed_code:
            self.features_by_watershed_code.setdefault(
                feature.fwa_watershed_code, []
            ).append(feature)
        if feature.waterbody_key:
            self.features_by_waterbody_key.setdefault(feature.waterbody_key, []).append(
                feature
            )
        if feature.blue_line_key:
            self.features_by_blue_line_key.setdefault(feature.blue_line_key, []).append(
                feature
            )
        # Poly / linear ID registration
        if feature.geometry_type == "multilinestring":
            self.features_by_linear_id[feature.fwa_id] = feature
        else:
            self.features_by_poly_id[feature.fwa_id] = feature


# ---------------------------------------------------------------------------
# Helper: create FWAFeature instances
# ---------------------------------------------------------------------------


def _lake(
    fwa_id: str = "100",
    gnis_name: str = "Test Lake",
    gnis_id: str = "99999",
    zones: list | None = None,
    mgmt_units: list | None = None,
    waterbody_key: str | None = None,
) -> FWAFeature:
    return FWAFeature(
        fwa_id=fwa_id,
        geometry_type="polygon",
        zones=zones or ["3"],
        feature_type=FeatureType.LAKE,
        gnis_name=gnis_name,
        gnis_id=gnis_id,
        mgmt_units=mgmt_units or ["3-15"],
        waterbody_key=waterbody_key or fwa_id,
    )


def _stream(
    fwa_id: str = "200",
    gnis_name: str = "Test Creek",
    gnis_id: str = "88888",
    fwa_watershed_code: str = "100-000000",
    blue_line_key: str | None = None,
    zones: list | None = None,
    mgmt_units: list | None = None,
) -> FWAFeature:
    return FWAFeature(
        fwa_id=fwa_id,
        geometry_type="multilinestring",
        zones=zones or ["3"],
        feature_type=FeatureType.STREAM,
        gnis_name=gnis_name,
        gnis_id=gnis_id,
        fwa_watershed_code=fwa_watershed_code,
        blue_line_key=blue_line_key or "BLK_200",
        mgmt_units=mgmt_units or ["3-15"],
    )


# ---------------------------------------------------------------------------
# Helpers: corrections and linker factory
# ---------------------------------------------------------------------------


def _empty_corrections(**overrides) -> ManualCorrections:
    """Build ManualCorrections with empty dicts, then apply overrides."""
    kwargs = dict(
        direct_matches={},
        skip_entries={},
        ungazetted_waterbodies={},
        admin_direct_matches={},
        name_variation_links={},
    )
    kwargs.update(overrides)
    return ManualCorrections(**kwargs)


def _make_linker(
    gazetteer: FakeLinkerGazetteer | None = None,
    corrections: ManualCorrections | None = None,
) -> WaterbodyLinker:
    return WaterbodyLinker(
        gazetteer=gazetteer or FakeLinkerGazetteer(),
        manual_corrections=corrections or _empty_corrections(),
    )


# ===================================================================
# Test: _feature_identity
# ===================================================================


class TestFeatureIdentity:
    """Static method that determines the unique identity key for deduplication."""

    def test_stream_uses_watershed_code(self):
        feat = _stream(fwa_watershed_code="999-000000")
        assert WaterbodyLinker._feature_identity(feat) == ("stream", "999-000000")

    def test_lake_uses_gnis_id(self):
        feat = _lake(gnis_id="12345")
        assert WaterbodyLinker._feature_identity(feat) == ("gnis", "12345")

    def test_polygon_without_gnis_uses_waterbody_key(self):
        feat = _lake(gnis_id=None, waterbody_key="WBK_42")
        assert WaterbodyLinker._feature_identity(feat) == ("waterbody_key", "WBK_42")

    def test_fallback_to_fwa_id(self):
        feat = FWAFeature(
            fwa_id="XYZ",
            geometry_type="polygon",
            zones=["1"],
            gnis_id=None,
            waterbody_key=None,
        )
        assert WaterbodyLinker._feature_identity(feat) == ("fwa_id", "XYZ")


# ===================================================================
# Test: SkipEntry handling
# ===================================================================


class TestSkipEntry:
    """STEP 0 – SkipEntry entries should produce NOT_IN_DATA or IGNORED."""

    def test_not_found_skip(self):
        corrections = _empty_corrections(
            skip_entries={
                "Region 3": {
                    "PHANTOM LAKE": SkipEntry(note="Not in FWA data", not_found=True),
                },
            }
        )
        linker = _make_linker(corrections=corrections)

        result = linker.link_waterbody(region="Region 3", name_verbatim="PHANTOM LAKE")
        assert result.status == LinkStatus.NOT_IN_DATA
        assert result.link_method == "skip_entry"

    def test_ignored_skip(self):
        corrections = _empty_corrections(
            skip_entries={
                "Region 5": {
                    "DUPLICATE CREEK": SkipEntry(note="Cross-listed", ignored=True),
                },
            }
        )
        linker = _make_linker(corrections=corrections)

        result = linker.link_waterbody(
            region="Region 5", name_verbatim="DUPLICATE CREEK"
        )
        assert result.status == LinkStatus.IGNORED
        assert result.link_method == "skip_entry"

    def test_skip_not_triggered_for_wrong_region(self):
        """Skip entries are region-scoped — wrong region falls through."""
        gaz = FakeLinkerGazetteer()
        corrections = _empty_corrections(
            skip_entries={
                "Region 5": {
                    "SOME LAKE": SkipEntry(note="Skip here", ignored=True),
                },
            }
        )
        linker = _make_linker(gazetteer=gaz, corrections=corrections)

        # Region 3 has no skip → falls through to natural search → NOT_FOUND
        result = linker.link_waterbody(region="Region 3", name_verbatim="SOME LAKE")
        assert result.status == LinkStatus.NOT_FOUND


# ===================================================================
# Test: NameVariationLink handling
# ===================================================================


class TestNameVariationLink:
    """STEP 0b – Alternate names that alias to a primary entry."""

    def test_name_variation_returns_primary_name(self):
        corrections = _empty_corrections(
            name_variation_links={
                "Region 4": {
                    "CAMERON SLOUGH": NameVariationLink(
                        primary_name='LEWIS ("Cameron") SLOUGH',
                        note="Same waterbody, historical name",
                    ),
                },
            }
        )
        linker = _make_linker(corrections=corrections)

        result = linker.link_waterbody(
            region="Region 4", name_verbatim="CAMERON SLOUGH"
        )
        assert result.status == LinkStatus.NAME_VARIATION
        assert result.link_method == "name_variation_link"
        assert result.matched_name == 'LEWIS ("Cameron") SLOUGH'
        assert result.name_variation_link is not None
        assert result.name_variation_link.primary_name == 'LEWIS ("Cameron") SLOUGH'

    def test_name_variation_stats_tracked(self):
        corrections = _empty_corrections(
            name_variation_links={
                "Region 1": {
                    "OLD NAME": NameVariationLink(
                        primary_name="NEW NAME",
                        note="Renamed",
                    ),
                },
            }
        )
        linker = _make_linker(corrections=corrections)
        linker.link_waterbody(region="Region 1", name_verbatim="OLD NAME")

        stats = linker.get_stats()
        assert stats[LinkStatus.NAME_VARIATION] == 1


# ===================================================================
# Test: DirectMatch handling
# ===================================================================


class TestDirectMatch:
    """STEP 1 – DirectMatch maps a regulation name to exact FWA IDs."""

    def test_single_gnis_id_match(self):
        """DirectMatch with one gnis_id returns SUCCESS with the feature."""
        gaz = FakeLinkerGazetteer()
        lake = _lake(fwa_id="P001", gnis_name="Long Lake", gnis_id="17501")
        gaz.add_feature(lake)

        corrections = _empty_corrections(
            direct_matches={
                "Region 1": {
                    "LONG LAKE (Nanaimo)": DirectMatch(
                        gnis_ids=["17501"],
                        note="Disambiguate using GNIS ID",
                    ),
                },
            }
        )
        linker = _make_linker(gazetteer=gaz, corrections=corrections)

        result = linker.link_waterbody(
            region="Region 1", name_verbatim="LONG LAKE (Nanaimo)"
        )
        assert result.status == LinkStatus.SUCCESS
        assert result.link_method == "direct_match"
        assert len(result.matched_features) == 1
        assert result.matched_features[0].gnis_id == "17501"

    def test_multiple_features_all_returned(self):
        """DirectMatch with a GNIS ID that maps to 3 polygon parts returns all 3."""
        gaz = FakeLinkerGazetteer()
        parts = [
            _lake(fwa_id=f"P00{i}", gnis_name="Williston Lake", gnis_id="50000")
            for i in range(1, 4)
        ]
        for p in parts:
            gaz.add_feature(p)

        corrections = _empty_corrections(
            direct_matches={
                "Region 7A": {
                    "WILLISTON LAKE": DirectMatch(
                        gnis_ids=["50000"],
                        note="Multiple polygons — all belong to same lake",
                    ),
                },
            }
        )
        linker = _make_linker(gazetteer=gaz, corrections=corrections)

        result = linker.link_waterbody(
            region="Region 7A", name_verbatim="WILLISTON LAKE"
        )
        assert result.status == LinkStatus.SUCCESS
        assert len(result.matched_features) == 3

    def test_direct_match_all_ids_missing_raises(self):
        """DirectMatch configured but GNIS ID not in gazetteer ⇒ DirectMatchError."""
        gaz = FakeLinkerGazetteer()  # empty

        corrections = _empty_corrections(
            direct_matches={
                "Region 2": {
                    "GHOST LAKE": DirectMatch(
                        gnis_ids=["00000"],
                        note="ID does not exist",
                    ),
                },
            }
        )
        linker = _make_linker(gazetteer=gaz, corrections=corrections)

        with pytest.raises(DirectMatchError, match="gnis_id=00000"):
            linker.link_waterbody(region="Region 2", name_verbatim="GHOST LAKE")

    def test_direct_match_partial_gnis_failure_raises(self):
        """2 gnis_ids configured, only one resolves ⇒ DirectMatchError for the missing one."""
        gaz = FakeLinkerGazetteer()
        gaz.add_feature(_lake(fwa_id="P050", gnis_name="Real Lake", gnis_id="11111"))
        # gnis_id "99999" is NOT in the gazetteer

        corrections = _empty_corrections(
            direct_matches={
                "Region 3": {
                    "SPLIT LAKE": DirectMatch(
                        gnis_ids=["11111", "99999"],
                        note="Second ID is invalid",
                    ),
                },
            }
        )
        linker = _make_linker(gazetteer=gaz, corrections=corrections)

        with pytest.raises(DirectMatchError, match="gnis_id=99999"):
            linker.link_waterbody(region="Region 3", name_verbatim="SPLIT LAKE")

    def test_direct_match_partial_watershed_failure_raises(self):
        """Watershed code configured but not in gazetteer ⇒ DirectMatchError."""
        gaz = FakeLinkerGazetteer()
        gaz.add_feature(_stream(fwa_id="S050", fwa_watershed_code="100-000000"))
        # "999-000000" is not in the gazetteer

        corrections = _empty_corrections(
            direct_matches={
                "Region 3": {
                    "MYSTERY CREEK": DirectMatch(
                        fwa_watershed_codes=["100-000000", "999-000000"],
                        note="Second code is invalid",
                    ),
                },
            }
        )
        linker = _make_linker(gazetteer=gaz, corrections=corrections)

        with pytest.raises(DirectMatchError, match="fwa_watershed_code=999-000000"):
            linker.link_waterbody(region="Region 3", name_verbatim="MYSTERY CREEK")

    def test_direct_match_missing_waterbody_key_raises(self):
        """waterbody_key that doesn't exist in gazetteer ⇒ DirectMatchError."""
        gaz = FakeLinkerGazetteer()

        corrections = _empty_corrections(
            direct_matches={
                "Region 1": {
                    "PHANTOM DAM": DirectMatch(
                        waterbody_keys=["NONEXISTENT_WBK"],
                        note="Bad key",
                    ),
                },
            }
        )
        linker = _make_linker(gazetteer=gaz, corrections=corrections)

        with pytest.raises(DirectMatchError, match="waterbody_key=NONEXISTENT_WBK"):
            linker.link_waterbody(region="Region 1", name_verbatim="PHANTOM DAM")

    def test_direct_match_missing_ungazetted_id_raises(self):
        """ungazetted_waterbody_id not in corrections ⇒ DirectMatchError."""
        gaz = FakeLinkerGazetteer()

        corrections = _empty_corrections(
            direct_matches={
                "Region 2": {
                    "MISSING MARSH": DirectMatch(
                        ungazetted_waterbody_id="UNGAZ_DOES_NOT_EXIST",
                        note="Orphaned reference",
                    ),
                },
            },
            # ungazetted_waterbodies is empty — the ID doesn't resolve
        )
        linker = _make_linker(gazetteer=gaz, corrections=corrections)

        with pytest.raises(
            DirectMatchError, match="ungazetted_waterbody_id=UNGAZ_DOES_NOT_EXIST"
        ):
            linker.link_waterbody(region="Region 2", name_verbatim="MISSING MARSH")

    def test_error_message_includes_all_missing_ids(self):
        """When multiple IDs fail, the error lists them all."""
        gaz = FakeLinkerGazetteer()

        corrections = _empty_corrections(
            direct_matches={
                "Region 4": {
                    "MULTI FAIL": DirectMatch(
                        gnis_ids=["AAA", "BBB"],
                        waterbody_keys=["CCC"],
                        note="All bad",
                    ),
                },
            }
        )
        linker = _make_linker(gazetteer=gaz, corrections=corrections)

        with pytest.raises(DirectMatchError) as exc_info:
            linker.link_waterbody(region="Region 4", name_verbatim="MULTI FAIL")

        msg = str(exc_info.value)
        assert "gnis_id=AAA" in msg
        assert "gnis_id=BBB" in msg
        assert "waterbody_key=CCC" in msg
        assert "3 unresolved ID(s)" in msg

    def test_multiple_id_types_combined(self):
        """DirectMatch using both gnis_ids and waterbody_keys returns all."""
        gaz = FakeLinkerGazetteer()
        lake = _lake(fwa_id="P010", gnis_name="Combo Lake", gnis_id="11111")
        gaz.add_feature(lake)

        # A second polygon found via waterbody_key
        extra = _lake(
            fwa_id="P011",
            gnis_name="Combo Lake Arm",
            gnis_id=None,
            waterbody_key="WBK_99",
        )
        gaz.add_feature(extra)

        corrections = _empty_corrections(
            direct_matches={
                "Region 3": {
                    "COMBO LAKE": DirectMatch(
                        gnis_ids=["11111"],
                        waterbody_keys=["WBK_99"],
                        note="Lake plus separate arm polygon",
                    ),
                },
            }
        )
        linker = _make_linker(gazetteer=gaz, corrections=corrections)

        result = linker.link_waterbody(region="Region 3", name_verbatim="COMBO LAKE")
        assert result.status == LinkStatus.SUCCESS
        assert len(result.matched_features) == 2

    def test_direct_match_additional_info_propagated(self):
        """additional_info from DirectMatch should appear on the LinkingResult."""
        gaz = FakeLinkerGazetteer()
        lake = _lake(fwa_id="P020", gnis_id="22222")
        gaz.add_feature(lake)

        corrections = _empty_corrections(
            direct_matches={
                "Region 4": {
                    "PERMIT LAKE": DirectMatch(
                        gnis_ids=["22222"],
                        note="Requires permit",
                        additional_info="Permit required from BC Parks.",
                    ),
                },
            }
        )
        linker = _make_linker(gazetteer=gaz, corrections=corrections)

        result = linker.link_waterbody(region="Region 4", name_verbatim="PERMIT LAKE")
        assert result.status == LinkStatus.SUCCESS
        assert result.additional_info == "Permit required from BC Parks."

    def test_direct_match_with_watershed_codes(self):
        """DirectMatch using fwa_watershed_codes returns all stream segments."""
        gaz = FakeLinkerGazetteer()
        seg1 = _stream(
            fwa_id="S001", gnis_name="Adams River", fwa_watershed_code="120-111111"
        )
        seg2 = _stream(
            fwa_id="S002", gnis_name="Adams River", fwa_watershed_code="120-111111"
        )
        gaz.add_feature(seg1)
        gaz.add_feature(seg2)

        corrections = _empty_corrections(
            direct_matches={
                "Region 3": {
                    "ADAMS RIVER": DirectMatch(
                        fwa_watershed_codes=["120-111111"],
                        note="All segments",
                    ),
                },
            }
        )
        linker = _make_linker(gazetteer=gaz, corrections=corrections)

        result = linker.link_waterbody(region="Region 3", name_verbatim="ADAMS RIVER")
        assert result.status == LinkStatus.SUCCESS
        assert len(result.matched_features) == 2


# ===================================================================
# Test: AdminDirectMatch handling
# ===================================================================


class TestAdminDirectMatch:
    """STEP 1b – Admin boundary polygon matching."""

    def test_admin_match_success(self):
        corrections = _empty_corrections(
            admin_direct_matches={
                "Region 3": {
                    "WELLS GRAY PARK": AdminDirectMatch(
                        admin_targets=[
                            AdminTarget(layer="parks_bc", feature_id="12345")
                        ],
                        note="All waters in Wells Gray Park",
                    ),
                },
            }
        )
        linker = _make_linker(corrections=corrections)

        result = linker.link_waterbody(
            region="Region 3", name_verbatim="WELLS GRAY PARK"
        )
        assert result.status == LinkStatus.ADMIN_MATCH
        assert result.link_method == "admin_direct_match"
        assert result.admin_match is not None
        assert result.admin_match.admin_targets[0].feature_id == "12345"

    def test_admin_match_empty_targets_is_error(self):
        """AdminDirectMatch with no admin_targets ⇒ ERROR."""
        corrections = _empty_corrections(
            admin_direct_matches={
                "Region 6": {
                    "INCOMPLETE PARK": AdminDirectMatch(
                        admin_targets=[],
                        note="Need to fill in admin targets",
                    ),
                },
            }
        )
        linker = _make_linker(corrections=corrections)

        result = linker.link_waterbody(
            region="Region 6", name_verbatim="INCOMPLETE PARK"
        )
        assert result.status == LinkStatus.ERROR
        assert "admin_targets is empty" in result.error_message

    def test_admin_match_additional_info(self):
        corrections = _empty_corrections(
            admin_direct_matches={
                "Region 2": {
                    "NATIONAL PARK WATERS": AdminDirectMatch(
                        admin_targets=[
                            AdminTarget(layer="parks_nat", feature_id="NP1")
                        ],
                        note="National park",
                        additional_info="Federal regulations also apply.",
                    ),
                },
            }
        )
        linker = _make_linker(corrections=corrections)

        result = linker.link_waterbody(
            region="Region 2", name_verbatim="NATIONAL PARK WATERS"
        )
        assert result.status == LinkStatus.ADMIN_MATCH
        assert result.additional_info == "Federal regulations also apply."


# ===================================================================
# Test: Natural search
# ===================================================================


class TestNaturalSearch:
    """STEP 2 – Gazetteer-based name search with deduplication."""

    def test_single_lake_match(self):
        gaz = FakeLinkerGazetteer()
        lake = _lake(
            fwa_id="P100",
            gnis_name="Silver Lake",
            gnis_id="30000",
            zones=["5"],
            mgmt_units=["5-10"],
        )
        gaz.add_feature(lake)

        linker = _make_linker(gazetteer=gaz)

        result = linker.link_waterbody(
            region="Region 5", mgmt_units=["5-10"], name_verbatim="SILVER LAKE"
        )
        assert result.status == LinkStatus.SUCCESS
        assert result.link_method == "natural_search"
        assert len(result.matched_features) == 1

    def test_no_match_returns_not_found(self):
        linker = _make_linker()
        result = linker.link_waterbody(
            region="Region 1", name_verbatim="NONEXISTENT LAKE"
        )
        assert result.status == LinkStatus.NOT_FOUND
        assert result.link_method == "natural_search"

    def test_stream_multiple_segments_is_success(self):
        """Multiple stream segments with the same watershed code → SUCCESS."""
        gaz = FakeLinkerGazetteer()
        for i in range(5):
            seg = _stream(
                fwa_id=f"S10{i}",
                gnis_name="Eagle Creek",
                gnis_id="77777",
                fwa_watershed_code="200-999999",
                zones=["4"],
                mgmt_units=["4-20"],
            )
            gaz.add_feature(seg)

        linker = _make_linker(gazetteer=gaz)

        result = linker.link_waterbody(
            region="Region 4", mgmt_units=["4-20"], name_verbatim="EAGLE CREEK"
        )
        assert result.status == LinkStatus.SUCCESS
        # The linker should return all segments via search_by_watershed_code
        assert len(result.matched_features) >= 5

    def test_two_distinct_waterbodies_is_ambiguous(self):
        """Two different waterbodies (different GNIS IDs) with the same name → AMBIGUOUS."""
        gaz = FakeLinkerGazetteer()
        lake_a = _lake(
            fwa_id="P200",
            gnis_name="Long Lake",
            gnis_id="A001",
            zones=["3"],
            mgmt_units=["3-1"],
        )
        lake_b = _lake(
            fwa_id="P201",
            gnis_name="Long Lake",
            gnis_id="B002",
            zones=["3"],
            mgmt_units=["3-1"],
        )
        gaz.add_feature(lake_a)
        gaz.add_feature(lake_b)

        linker = _make_linker(gazetteer=gaz)

        result = linker.link_waterbody(
            region="Region 3", mgmt_units=["3-1"], name_verbatim="LONG LAKE"
        )
        assert result.status == LinkStatus.AMBIGUOUS

    def test_mu_filtering_narrows_to_one(self):
        """Two candidates in different MUs — correct MU filter picks one."""
        gaz = FakeLinkerGazetteer()
        lake_a = _lake(
            fwa_id="P300",
            gnis_name="Round Lake",
            gnis_id="C001",
            zones=["5"],
            mgmt_units=["5-1"],
        )
        lake_b = _lake(
            fwa_id="P301",
            gnis_name="Round Lake",
            gnis_id="D002",
            zones=["5"],
            mgmt_units=["5-9"],
        )
        gaz.add_feature(lake_a)
        gaz.add_feature(lake_b)

        linker = _make_linker(gazetteer=gaz)

        result = linker.link_waterbody(
            region="Region 5", mgmt_units=["5-1"], name_verbatim="ROUND LAKE"
        )
        assert result.status == LinkStatus.SUCCESS
        assert result.matched_features[0].gnis_id == "C001"

    def test_cross_zone_fallback(self):
        """Feature exists in zone 5 but regulation says zone 4 → still found via fallback."""
        gaz = FakeLinkerGazetteer()
        lake = _lake(
            fwa_id="P400",
            gnis_name="Boundary Lake",
            gnis_id="E001",
            zones=["5"],
            mgmt_units=["5-2"],
        )
        gaz.add_feature(lake)

        linker = _make_linker(gazetteer=gaz)

        result = linker.link_waterbody(region="Region 4", name_verbatim="BOUNDARY LAKE")
        # Should find via cross-zone fallback, but region/MU validation may
        # cause AMBIGUOUS. The key assertion is that it doesn't return NOT_FOUND.
        assert result.status != LinkStatus.NOT_FOUND

    def test_single_gnis_multiple_polygons_is_ambiguous(self):
        """One GNIS ID with multiple polygon parts via natural search → AMBIGUOUS.

        This forces the user to add a DirectMatch entry.
        """
        gaz = FakeLinkerGazetteer()
        for i in range(3):
            lake = _lake(
                fwa_id=f"P50{i}",
                gnis_name="Big Lake",
                gnis_id="F001",
                zones=["6"],
                mgmt_units=["6-5"],
            )
            gaz.add_feature(lake)

        linker = _make_linker(gazetteer=gaz)

        result = linker.link_waterbody(
            region="Region 6", mgmt_units=["6-5"], name_verbatim="BIG LAKE"
        )
        assert result.status == LinkStatus.AMBIGUOUS
        assert "polygons with same GNIS ID" in result.error_message


# ===================================================================
# Test: Search variations (bracket / quote removal)
# ===================================================================


class TestSearchVariations:
    """Linker tries bracket-removed and quote-removed variations of the name."""

    def test_parenthetical_removed_matches(self):
        """'LAKE (Region 5)' fails exact match but 'LAKE' succeeds."""
        gaz = FakeLinkerGazetteer()
        lake = _lake(
            fwa_id="P600",
            gnis_name="Lake",
            gnis_id="G001",
            zones=["5"],
            mgmt_units=["5-1"],
        )
        gaz.add_feature(lake)

        linker = _make_linker(gazetteer=gaz)

        result = linker.link_waterbody(
            region="Region 5",
            mgmt_units=["5-1"],
            name_verbatim="LAKE (Region 5)",
        )
        assert result.status == LinkStatus.SUCCESS

    def test_quoted_name_variation(self):
        """Names with inner quotes are cleaned as a search variation."""
        gaz = FakeLinkerGazetteer()
        feat = _stream(
            fwa_id="S600",
            gnis_name="Lewis Slough",
            fwa_watershed_code="300-000000",
            zones=["4"],
            mgmt_units=["4-1"],
        )
        gaz.add_feature(feat)

        linker = _make_linker(gazetteer=gaz)

        result = linker.link_waterbody(
            region="Region 4",
            mgmt_units=["4-1"],
            name_verbatim='LEWIS ("Cameron") SLOUGH',
        )
        # After bracket removal: "LEWIS SLOUGH" → should match
        assert result.status == LinkStatus.SUCCESS


# ===================================================================
# Test: _validate_region_mu_match
# ===================================================================


class TestValidateRegionMuMatch:
    """Region / MU consistency validation."""

    def test_matching_region_mu(self):
        linker = _make_linker()
        feats = [_lake(mgmt_units=["5-10", "5-11"])]
        assert linker._validate_region_mu_match("Region 5", feats) is True

    def test_mismatching_region_mu(self):
        linker = _make_linker()
        feats = [_lake(mgmt_units=["3-15"])]
        assert linker._validate_region_mu_match("Region 5", feats) is False

    def test_no_region_returns_true(self):
        linker = _make_linker()
        assert linker._validate_region_mu_match(None, [_lake()]) is True

    def test_no_features_returns_true(self):
        linker = _make_linker()
        assert linker._validate_region_mu_match("Region 3", []) is True

    def test_feature_without_mus_skipped(self):
        """Features with no MU data are skipped (not fail)."""
        linker = _make_linker()
        feat = _lake(mgmt_units=None)
        # None MUs → skip validation for this feature → returns True
        assert linker._validate_region_mu_match("Region 3", [feat]) is True

    def test_region_7a_matches_mu_prefix_7(self):
        """Region '7A' should extract digit prefix '7' for MU matching."""
        linker = _make_linker()
        feats = [_lake(mgmt_units=["7-55"])]
        assert linker._validate_region_mu_match("Region 7A", feats) is True


# ===================================================================
# Test: Priority ordering (skip > name_variation > direct > admin > natural)
# ===================================================================


class TestPriorityOrdering:
    """Corrections are checked in strict priority order."""

    def test_skip_trumps_direct_match(self):
        """When both skip and direct are configured, skip wins."""
        gaz = FakeLinkerGazetteer()
        gaz.add_feature(_lake(fwa_id="P700", gnis_id="77777"))

        corrections = _empty_corrections(
            skip_entries={
                "Region 1": {
                    "CONFLICT LAKE": SkipEntry(note="Skipped", ignored=True),
                },
            },
            direct_matches={
                "Region 1": {
                    "CONFLICT LAKE": DirectMatch(
                        gnis_ids=["77777"],
                        note="This should never be reached",
                    ),
                },
            },
        )
        linker = _make_linker(gazetteer=gaz, corrections=corrections)

        result = linker.link_waterbody(region="Region 1", name_verbatim="CONFLICT LAKE")
        assert result.status == LinkStatus.IGNORED

    def test_name_variation_trumps_direct_match(self):
        """Name variation is checked before direct match."""
        gaz = FakeLinkerGazetteer()
        gaz.add_feature(_lake(fwa_id="P800", gnis_id="88888"))

        corrections = _empty_corrections(
            name_variation_links={
                "Region 2": {
                    "ALIAS LAKE": NameVariationLink(
                        primary_name="REAL LAKE",
                        note="Alias",
                    ),
                },
            },
            direct_matches={
                "Region 2": {
                    "ALIAS LAKE": DirectMatch(
                        gnis_ids=["88888"],
                        note="Should not be reached",
                    ),
                },
            },
        )
        linker = _make_linker(gazetteer=gaz, corrections=corrections)

        result = linker.link_waterbody(region="Region 2", name_verbatim="ALIAS LAKE")
        assert result.status == LinkStatus.NAME_VARIATION

    def test_direct_match_trumps_natural_search(self):
        """Direct match should win even when natural search would also match."""
        gaz = FakeLinkerGazetteer()
        # The lake in the gazetteer has a different GNIS ID
        lake_natural = _lake(
            fwa_id="P900",
            gnis_name="Overlap Lake",
            gnis_id="NAT01",
            zones=["3"],
            mgmt_units=["3-1"],
        )
        gaz.add_feature(lake_natural)

        # Direct match points to a DIFFERENT GNIS ID
        lake_direct = _lake(
            fwa_id="P901",
            gnis_name="Overlap Lake",
            gnis_id="DIR01",
            zones=["3"],
            mgmt_units=["3-1"],
        )
        gaz.add_feature(lake_direct)

        corrections = _empty_corrections(
            direct_matches={
                "Region 3": {
                    "OVERLAP LAKE": DirectMatch(
                        gnis_ids=["DIR01"],
                        note="Pick the direct match feature",
                    ),
                },
            },
        )
        linker = _make_linker(gazetteer=gaz, corrections=corrections)

        result = linker.link_waterbody(region="Region 3", name_verbatim="OVERLAP LAKE")
        assert result.status == LinkStatus.SUCCESS
        assert result.link_method == "direct_match"
        # Should only return the direct-match feature, not the natural one
        assert all(f.gnis_id == "DIR01" for f in result.matched_features)


# ===================================================================
# Test: Stats tracking
# ===================================================================


class TestStatsTracking:
    """get_stats() accurately counts each LinkStatus."""

    def test_mixed_statuses_counted(self):
        gaz = FakeLinkerGazetteer()
        lake = _lake(
            fwa_id="P1000",
            gnis_name="Counted Lake",
            gnis_id="CC01",
            zones=["4"],
            mgmt_units=["4-5"],
        )
        gaz.add_feature(lake)

        corrections = _empty_corrections(
            skip_entries={
                "Region 4": {
                    "SKIPPED LAKE": SkipEntry(note="Skip", not_found=True),
                },
            },
        )
        linker = _make_linker(gazetteer=gaz, corrections=corrections)

        # 1 × SUCCESS
        linker.link_waterbody(
            region="Region 4", mgmt_units=["4-5"], name_verbatim="COUNTED LAKE"
        )
        # 1 × NOT_IN_DATA
        linker.link_waterbody(region="Region 4", name_verbatim="SKIPPED LAKE")
        # 1 × NOT_FOUND
        linker.link_waterbody(region="Region 4", name_verbatim="NOPE")

        stats = linker.get_stats()
        assert stats[LinkStatus.SUCCESS] == 1
        assert stats[LinkStatus.NOT_IN_DATA] == 1
        assert stats[LinkStatus.NOT_FOUND] == 1


# ===================================================================
# Test: No region provided → skip corrections, go straight to search
# ===================================================================


class TestNoRegion:
    """When region is None, corrections are skipped entirely."""

    def test_no_region_natural_search(self):
        gaz = FakeLinkerGazetteer()
        lake = _lake(
            fwa_id="P1100", gnis_name="Orphan Lake", gnis_id="OR01", zones=["2"]
        )
        gaz.add_feature(lake)

        linker = _make_linker(gazetteer=gaz)

        result = linker.link_waterbody(region=None, name_verbatim="ORPHAN LAKE")
        assert result.status == LinkStatus.SUCCESS
        assert result.link_method == "natural_search"

    def test_no_region_skips_corrections(self):
        """Even with matching corrections, region=None bypasses them."""
        corrections = _empty_corrections(
            skip_entries={
                "Region 1": {
                    "ORPHAN LAKE": SkipEntry(note="Ignored", ignored=True),
                },
            },
        )
        gaz = FakeLinkerGazetteer()
        lake = _lake(fwa_id="P1101", gnis_name="Orphan Lake", zones=["1"])
        gaz.add_feature(lake)

        linker = _make_linker(gazetteer=gaz, corrections=corrections)

        result = linker.link_waterbody(region=None, name_verbatim="ORPHAN LAKE")
        # Without region, skip is NOT checked → falls through to nat search
        assert result.status == LinkStatus.SUCCESS


# ===================================================================
# Test: Region number extraction
# ===================================================================


class TestRegionParsing:
    """Zone number extraction from various region string formats."""

    def test_region_prefix_stripped(self):
        """'Region 4' extracts zone_number '4' for gazetteer search."""
        gaz = FakeLinkerGazetteer()
        lake = _lake(
            fwa_id="P1200",
            gnis_name="Zone Lake",
            gnis_id="ZZ01",
            zones=["4"],
            mgmt_units=["4-1"],
        )
        gaz.add_feature(lake)

        linker = _make_linker(gazetteer=gaz)

        result = linker.link_waterbody(
            region="Region 4", mgmt_units=["4-1"], name_verbatim="ZONE LAKE"
        )
        assert result.status == LinkStatus.SUCCESS

    def test_bare_number_works(self):
        """Region '4' (no prefix) also works."""
        gaz = FakeLinkerGazetteer()
        lake = _lake(
            fwa_id="P1201",
            gnis_name="Zone Lake",
            gnis_id="ZZ02",
            zones=["4"],
            mgmt_units=["4-1"],
        )
        gaz.add_feature(lake)

        linker = _make_linker(gazetteer=gaz)

        result = linker.link_waterbody(
            region="4", mgmt_units=["4-1"], name_verbatim="ZONE LAKE"
        )
        assert result.status == LinkStatus.SUCCESS


# ===================================================================
# Test: UngazettedWaterbody via DirectMatch
# ===================================================================


class TestUngazettedWaterbody:
    """DirectMatch with ungazetted_waterbody_id creates a synthetic feature."""

    def test_ungazetted_point(self):
        gaz = FakeLinkerGazetteer()
        corrections = _empty_corrections(
            direct_matches={
                "Region 2": {
                    "MARSH POND": DirectMatch(
                        ungazetted_waterbody_id="UNGAZ_MARSH_POND_R2",
                        note="Custom point for marsh pond",
                    ),
                },
            },
            ungazetted_waterbodies={
                "UNGAZ_MARSH_POND_R2": UngazettedWaterbody(
                    ungazetted_id="UNGAZ_MARSH_POND_R2",
                    name="Marsh Pond",
                    geometry_type="point",
                    coordinates=[1000000.0, 500000.0],
                    zones=["2"],
                    mgmt_units=["2-4"],
                    note="Custom point",
                ),
            },
        )
        linker = _make_linker(gazetteer=gaz, corrections=corrections)

        result = linker.link_waterbody(region="Region 2", name_verbatim="MARSH POND")
        assert result.status == LinkStatus.SUCCESS
        assert len(result.matched_features) == 1
        assert result.matched_features[0].fwa_id == "UNGAZ_MARSH_POND_R2"
        assert result.matched_features[0].is_ungazetted_waterbody is True

    def test_ungazetted_linestring(self):
        gaz = FakeLinkerGazetteer()
        corrections = _empty_corrections(
            direct_matches={
                "Region 3": {
                    "SIDE CHANNEL": DirectMatch(
                        ungazetted_waterbody_id="UNGAZ_SIDE_CH",
                        note="Side channel line",
                    ),
                },
            },
            ungazetted_waterbodies={
                "UNGAZ_SIDE_CH": UngazettedWaterbody(
                    ungazetted_id="UNGAZ_SIDE_CH",
                    name="Side Channel",
                    geometry_type="linestring",
                    coordinates=[[1000000.0, 500000.0], [1001000.0, 500100.0]],
                    zones=["3"],
                    mgmt_units=["3-5"],
                    note="Custom line",
                ),
            },
        )
        linker = _make_linker(gazetteer=gaz, corrections=corrections)

        result = linker.link_waterbody(region="Region 3", name_verbatim="SIDE CHANNEL")
        assert result.status == LinkStatus.SUCCESS
        assert result.matched_features[0].geometry_type == "linestring"

    def test_ungazetted_polygon(self):
        gaz = FakeLinkerGazetteer()
        coords = [
            [
                [1000000, 500000],
                [1001000, 500000],
                [1001000, 501000],
                [1000000, 501000],
                [1000000, 500000],
            ]
        ]
        corrections = _empty_corrections(
            direct_matches={
                "Region 1": {
                    "HIDDEN MARSH": DirectMatch(
                        ungazetted_waterbody_id="UNGAZ_HIDDEN_MARSH",
                        note="Custom polygon",
                    ),
                },
            },
            ungazetted_waterbodies={
                "UNGAZ_HIDDEN_MARSH": UngazettedWaterbody(
                    ungazetted_id="UNGAZ_HIDDEN_MARSH",
                    name="Hidden Marsh",
                    geometry_type="polygon",
                    coordinates=coords,
                    zones=["1"],
                    mgmt_units=["1-5"],
                    note="Custom polygon",
                ),
            },
        )
        linker = _make_linker(gazetteer=gaz, corrections=corrections)

        result = linker.link_waterbody(region="Region 1", name_verbatim="HIDDEN MARSH")
        assert result.status == LinkStatus.SUCCESS
        assert result.matched_features[0].geometry_type == "polygon"


# ===================================================================
# Test: LinkingResult dataclass defaults
# ===================================================================


class TestLinkingResult:
    """Verify LinkingResult post_init defaults."""

    def test_default_lists_are_empty(self):
        r = LinkingResult(status=LinkStatus.SUCCESS)
        assert r.matched_features == []
        assert r.candidate_features == []

    def test_explicit_features_preserved(self):
        feat = _lake()
        r = LinkingResult(
            status=LinkStatus.SUCCESS,
            matched_features=[feat],
        )
        assert len(r.matched_features) == 1
