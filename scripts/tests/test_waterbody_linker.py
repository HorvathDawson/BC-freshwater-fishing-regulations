"""
Tests for Waterbody Linking Module (MVP Implementation)

Test-Driven Development approach:
1. Write failing tests first
2. Implement minimal code to pass
3. Refactor
4. Repeat

Test categories:
- Basic linking (single match - SUCCESS)
- Disambiguation (multiple matches - AMBIGUOUS)
- Not found (no matches - NOT_FOUND)
- Name variations
- Name normalization
- Edge cases
"""

import pytest
from typing import List, Dict, Optional
from fwa_modules.linking import (
    WaterbodyLinker,
    FWAFeature,
    LinkingResult,
    LinkStatus,
    NameVariation,
)


# ==========================================
#       TEST GAZETTEER
# ==========================================


class TestGazetteer:
    """Simple in-memory gazetteer for testing."""

    def __init__(self, features: List[FWAFeature]):
        self.features = features
        self._build_index()

    def _build_index(self):
        """Build name-based index for fast lookups."""
        self.name_index: Dict[str, List[FWAFeature]] = {}

        for feature in self.features:
            normalized_name = feature.name.strip().title()
            if normalized_name not in self.name_index:
                self.name_index[normalized_name] = []
            self.name_index[normalized_name].append(feature)

            if feature.gnis_name:
                gnis_normalized = feature.gnis_name.strip().title()
                if gnis_normalized not in self.name_index:
                    self.name_index[gnis_normalized] = []
                if feature not in self.name_index[gnis_normalized]:
                    self.name_index[gnis_normalized].append(feature)

    def search(self, name: str, region: Optional[str] = None) -> List[FWAFeature]:
        """Search for FWA features by name."""
        normalized = name.strip().title()
        matches = self.name_index.get(normalized, [])

        if region:
            matches = [f for f in matches if region in f.zones]

        return matches

    def add_feature(self, feature: FWAFeature):
        """Add a feature and rebuild the index."""
        self.features.append(feature)
        self._build_index()


# ==========================================
#       FIXTURES
# ==========================================


@pytest.fixture
def sample_fwa_features():
    """Sample FWA features for testing."""
    return [
        # Unique waterbodies
        FWAFeature(
            fwa_id="fwa_elk_001",
            name="Elk River",
            geometry_type="multilinestring",
            zones=["4"],
            gnis_name="Elk River",
        ),
        FWAFeature(
            fwa_id="fwa_michel_001",
            name="Michel Creek",
            geometry_type="multilinestring",
            zones=["4"],
        ),
        FWAFeature(
            fwa_id="fwa_alouette_001",
            name="Alouette Lake",
            geometry_type="polygon",
            zones=["2"],
            gnis_name="Alouette Lake",
        ),
        # Ambiguous waterbodies (same name, different regions)
        FWAFeature(
            fwa_id="fwa_mill_region2",
            name="Mill Creek",
            geometry_type="multilinestring",
            zones=["2"],
        ),
        FWAFeature(
            fwa_id="fwa_mill_region4",
            name="Mill Creek",
            geometry_type="multilinestring",
            zones=["4"],
        ),
        # Same name, different geometry types
        FWAFeature(
            fwa_id="fwa_adams_lake_polygon",
            name="Adams Lake",
            geometry_type="polygon",
            zones=["3"],
        ),
        FWAFeature(
            fwa_id="fwa_adams_lake_point",
            name="Adams Lake",
            geometry_type="point",
            zones=["3"],
        ),
    ]


@pytest.fixture
def fwa_gazetteer(sample_fwa_features):
    """Gazetteer populated with sample features."""
    return TestGazetteer(sample_fwa_features)


@pytest.fixture
def name_variations():
    """Sample name variation database."""
    return {
        "Region 4": {
            "ELK R.": NameVariation(
                target_names=["Elk River"], note="Abbreviation expansion"
            ),
            "MICHEL CR.": NameVariation(
                target_names=["Michel Creek"], note="Abbreviation expansion"
            ),
            "BABIENE LAKE": NameVariation(
                target_names=["Babine Lake"], note="Typo correction"
            ),
        },
        "Region 2": {
            "ALOUETTE L.": NameVariation(
                target_names=["Alouette Lake"], note="Abbreviation expansion"
            )
        },
    }


@pytest.fixture
def linker(fwa_gazetteer, name_variations):
    """Waterbody linker with gazetteer and variations."""
    return WaterbodyLinker(fwa_gazetteer, name_variations)


# ==========================================
#       TEST 1: BASIC LINKING (SUCCESS)
# ==========================================


def test_link_single_match_exact_name(linker):
    """
    TDD Test 1: Single exact match returns SUCCESS.

    Given: "Elk River" exists uniquely in gazetteer
    When: link_waterbody("Elk River", "Region 4")
    Then: Returns SUCCESS with matched feature
    """
    result = linker.link_waterbody("Elk River", region="Region 4")

    assert result.status == LinkStatus.SUCCESS
    assert result.is_success
    assert result.matched_feature is not None
    assert result.matched_feature.fwa_id == "fwa_elk_001"
    assert result.matched_feature.name == "Elk River"
    assert len(result.candidate_features) == 1


def test_link_single_match_case_insensitive(linker):
    """
    TDD Test 2: Matching is case-insensitive.

    Given: "Elk River" in gazetteer
    When: link_waterbody("ELK RIVER", "Region 4")
    Then: Returns SUCCESS (case-insensitive match)
    """
    result = linker.link_waterbody("ELK RIVER", region="Region 4")

    assert result.is_success
    assert result.matched_feature.name == "Elk River"


def test_link_lake_polygon_geometry(linker):
    """
    TDD Test 3: Link to polygon geometry (lake).

    Given: "Alouette Lake" exists as polygon
    When: link_waterbody("Alouette Lake", "Region 2")
    Then: Returns SUCCESS with polygon feature
    """
    result = linker.link_waterbody("Alouette Lake", region="Region 2")

    assert result.is_success
    assert result.matched_feature.geometry_type == "polygon"
    assert result.matched_feature.fwa_id == "fwa_alouette_001"


# ==========================================
#       TEST 2: DISAMBIGUATION (AMBIGUOUS)
# ==========================================


def test_link_multiple_matches_no_region_filter(linker):
    """
    TDD Test 4: Multiple matches without region returns AMBIGUOUS.

    Given: "Mill Creek" exists in Region 2 and Region 4
    When: link_waterbody("Mill Creek") with no region
    Then: Returns AMBIGUOUS with all candidates
    """
    result = linker.link_waterbody("Mill Creek", region=None)

    assert result.status == LinkStatus.AMBIGUOUS
    assert result.needs_manual_review
    assert result.matched_feature is None
    assert len(result.candidate_features) == 2

    # Both candidates returned
    fwa_ids = {f.fwa_id for f in result.candidate_features}
    assert "fwa_mill_region2" in fwa_ids
    assert "fwa_mill_region4" in fwa_ids


def test_link_multiple_matches_with_region_filter_resolves(linker):
    """
    TDD Test 5: Region filter resolves ambiguity.

    Given: "Mill Creek" in multiple regions
    When: link_waterbody("Mill Creek", "Region 4")
    Then: Returns SUCCESS with region-specific match
    """
    result = linker.link_waterbody("Mill Creek", region="Region 4")

    assert result.is_success
    assert result.matched_feature.fwa_id == "fwa_mill_region4"
    assert "4" in result.matched_feature.zones


def test_link_multiple_geometry_types_same_name(linker):
    """
    TDD Test 6: Same name with different geometry types is ambiguous.

    Given: "Adams Lake" exists as polygon and point
    When: link_waterbody("Adams Lake", "Region 3")
    Then: Returns AMBIGUOUS (needs geometry type hint)
    """
    result = linker.link_waterbody("Adams Lake", region="Region 3")

    assert result.status == LinkStatus.AMBIGUOUS
    assert len(result.candidate_features) == 2

    geometry_types = {f.geometry_type for f in result.candidate_features}
    assert "polygon" in geometry_types
    assert "point" in geometry_types


# ==========================================
#       TEST 3: NOT FOUND
# ==========================================


def test_link_waterbody_not_in_gazetteer(linker):
    """
    TDD Test 7: Waterbody not in gazetteer returns NOT_FOUND.

    Given: "Nonexistent Creek" not in gazetteer
    When: link_waterbody("Nonexistent Creek", "Region 4")
    Then: Returns NOT_FOUND
    """
    result = linker.link_waterbody("Nonexistent Creek", region="Region 4")

    assert result.status == LinkStatus.NOT_FOUND
    assert result.needs_manual_review
    assert result.matched_feature is None
    assert len(result.candidate_features) == 0


def test_link_waterbody_wrong_region(linker):
    """
    TDD Test 8: Waterbody exists but not in specified region.

    Given: "Elk River" in Region 4
    When: link_waterbody("Elk River", "Region 2")
    Then: Returns NOT_FOUND (region filter excludes it)
    """
    result = linker.link_waterbody("Elk River", region="Region 2")

    assert result.status == LinkStatus.NOT_FOUND
    assert len(result.candidate_features) == 0


# ==========================================
#       TEST 4: NAME VARIATIONS
# ==========================================


def test_name_variation_lookup_abbreviation(linker):
    """
    TDD Test 9: Name variation database resolves abbreviations.

    Given: "ELK R." maps to "Elk River" in Region 4 variations
    When: link_waterbody("ELK R.", "Region 4")
    Then: Returns SUCCESS (variation applied before matching)
    """
    result = linker.link_waterbody("ELK R.", region="Region 4")

    assert result.is_success
    assert result.matched_feature.name == "Elk River"


def test_name_variation_typo_correction(linker):
    """
    TDD Test 10: Name variations correct typos.

    Given: "BABIENE LAKE" maps to "Babine Lake"
    When: link_waterbody("BABIENE LAKE", "Region 4")
    Then: Returns NOT_FOUND (Babine Lake not in our test gazetteer)

    Note: This tests variation application, not whether Babine exists.
    """
    # Add Babine Lake to gazetteer
    linker.gazetteer.add_feature(
        FWAFeature(
            fwa_id="fwa_babine_001",
            name="Babine Lake",
            geometry_type="polygon",
            zones=["4"],
        )
    )

    result = linker.link_waterbody("BABIENE LAKE", region="Region 4")

    assert result.is_success
    assert result.matched_feature.name == "Babine Lake"


def test_name_variation_region_specific(linker):
    """
    TDD Test 11: Name variations are region-specific.

    Given: "ALOUETTE L." variation only in Region 2
    When: link_waterbody("ALOUETTE L.", "Region 4")
    Then: Variation NOT applied (wrong region), returns NOT_FOUND
    """
    result = linker.link_waterbody("ALOUETTE L.", region="Region 4")

    # Variation not applied, so searches for literal "Alouette L."
    assert result.status == LinkStatus.NOT_FOUND


# ==========================================
#       TEST 5: NAME NORMALIZATION
# ==========================================


def test_normalize_possessive_removed(linker):
    """
    TDD Test 12: Possessive 'S removed during normalization.

    Given: "Elk River" in gazetteer
    When: link_waterbody("ELK RIVER'S", "Region 4")
    Then: Returns SUCCESS (possessive stripped)
    """
    result = linker.link_waterbody("ELK RIVER'S", region="Region 4")

    assert result.is_success
    assert result.matched_feature.name == "Elk River"


def test_normalize_abbreviation_creek(linker):
    """
    TDD Test 13: "Cr." expanded to "Creek".

    Given: "Michel Creek" in gazetteer
    When: link_waterbody("MICHEL CR.", "Region 4")
    Then: Returns SUCCESS (Cr. expanded to Creek)
    """
    result = linker.link_waterbody("MICHEL CR.", region="Region 4")

    assert result.is_success
    assert result.matched_feature.name == "Michel Creek"


def test_normalize_abbreviation_river(linker):
    """
    TDD Test 14: "R." expanded to "River".

    Given: "Elk River" in gazetteer
    When: link_waterbody("ELK R.", "Region 4")
    Then: Returns SUCCESS (R. expanded to River)

    Note: This also tests interaction with name variations.
    """
    result = linker.link_waterbody("ELK R.", region="Region 4")

    assert result.is_success


def test_normalize_whitespace_stripped(linker):
    """
    TDD Test 15: Leading/trailing whitespace stripped.

    Given: "Elk River" in gazetteer
    When: link_waterbody("  Elk River  ", "Region 4")
    Then: Returns SUCCESS (whitespace stripped)
    """
    result = linker.link_waterbody("  Elk River  ", region="Region 4")

    assert result.is_success


# ==========================================
#       TEST 6: EDGE CASES
# ==========================================


def test_link_empty_waterbody_name(linker):
    """
    TDD Test 16: Empty waterbody name.

    Given: Empty string ""
    When: link_waterbody("", "Region 4")
    Then: Returns NOT_FOUND
    """
    result = linker.link_waterbody("", region="Region 4")

    assert result.status == LinkStatus.NOT_FOUND


def test_link_no_region_provided(linker):
    """
    TDD Test 17: No region provided for unique waterbody.

    Given: "Elk River" exists in only one region
    When: link_waterbody("Elk River") with no region
    Then: Returns SUCCESS (unique match across all regions)
    """
    result = linker.link_waterbody("Elk River", region=None)

    assert result.is_success
    assert "4" in result.matched_feature.zones


def test_link_special_characters_in_name(linker):
    """
    TDD Test 18: Waterbody names with special characters.

    Given: Waterbody with quotes in name
    When: link_waterbody with special chars
    Then: Handles correctly
    """
    # Add feature with special characters
    special_feature = FWAFeature(
        fwa_id="fwa_special_001",
        name='"Little Grassy" Lake',
        geometry_type="polygon",
        zones=["3"],
    )
    linker.gazetteer.features.append(special_feature)
    linker.gazetteer._build_index()

    result = linker.link_waterbody('"Little Grassy" Lake', region="Region 3")

    assert result.is_success


def test_gazetteer_empty(name_variations):
    """
    TDD Test 19: Empty gazetteer returns NOT_FOUND.

    Given: Empty gazetteer
    When: link_waterbody("Any Name")
    Then: Returns NOT_FOUND
    """
    empty_gazetteer = TestGazetteer([])
    linker = WaterbodyLinker(empty_gazetteer, name_variations)

    result = linker.link_waterbody("Elk River", region="Region 4")

    assert result.status == LinkStatus.NOT_FOUND


def test_result_properties(linker):
    """
    TDD Test 20: LinkingResult properties work correctly.

    Test convenience properties on LinkingResult.
    """
    # Success case
    success_result = linker.link_waterbody("Elk River", "Region 4")
    assert success_result.is_success is True
    assert success_result.needs_manual_review is False

    # Ambiguous case
    ambiguous_result = linker.link_waterbody("Mill Creek", None)
    assert ambiguous_result.is_success is False
    assert ambiguous_result.needs_manual_review is True

    # Not found case
    notfound_result = linker.link_waterbody("Nonexistent", "Region 1")
    assert notfound_result.is_success is False
    assert notfound_result.needs_manual_review is True


# ==========================================
#       TEST 7: INTEGRATION
# ==========================================


def test_end_to_end_elk_river_linking(linker):
    """
    TDD Test 21: End-to-end test for ELK RIVER'S TRIBUTARIES example.

    Simulates the MVP document example.
    """
    # From MVP example: identity.waterbody_key = "ELK RIVER"
    waterbody_key = "ELK RIVER"
    region = "Region 4"

    result = linker.link_waterbody(waterbody_key, region)

    # Should succeed with single match
    assert result.is_success
    assert result.matched_feature.name == "Elk River"
    assert "4" in result.matched_feature.zones
    assert result.waterbody_key == "ELK RIVER"


def test_exclusion_linking_workflow(linker):
    """
    TDD Test 22: Link multiple exclusions from MVP example.

    Tests batch linking of exclusions:
    - Michel Creek (should succeed)
    - Alexander Creek (not in gazetteer - should fail)
    - Abruzzi Creek (not in gazetteer - should fail)
    """
    exclusions = [
        {"waterbody_key": "MICHEL CREEK", "region": "Region 4"},
        {"waterbody_key": "ALEXANDER CREEK", "region": "Region 4"},
        {"waterbody_key": "ABRUZZI CREEK", "region": "Region 4"},
    ]

    results = [
        linker.link_waterbody(exc["waterbody_key"], exc["region"]) for exc in exclusions
    ]

    # Michel Creek should succeed
    assert results[0].is_success
    assert results[0].matched_feature.name == "Michel Creek"

    # Alexander and Abruzzi not in our test gazetteer
    assert results[1].status == LinkStatus.NOT_FOUND
    assert results[2].status == LinkStatus.NOT_FOUND


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
