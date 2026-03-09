"""
Integration tests: WaterbodyLinker ↔ RegulationMapper.

Uses a REAL WaterbodyLinker with a FakeLinkerGazetteer wired to a
REAL RegulationMapper.  Validates the contract boundary between the
two modules — specifically that LinkingResult fields are consumed
correctly by the mapper's _pass1_prelink.

Also tests that DirectMatchError from invalid feature IDs propagates
through the mapper without being swallowed.
"""

from __future__ import annotations

from collections import defaultdict
from typing import Any, Dict, List, Optional, Set
from unittest.mock import patch

import pytest

from fwa_pipeline.metadata_builder import FeatureType
from fwa_pipeline.metadata_gazetteer import FWAFeature
from regulation_mapping.admin_target import AdminTarget
from regulation_mapping.linker import (
    DirectMatchError,
    LinkStatus,
    WaterbodyLinker,
)
from regulation_mapping.linking_corrections import (
    AdminDirectMatch,
    DirectMatch,
    ManualCorrections,
    NameVariationLink,
    SkipEntry,
)
from regulation_mapping.regulation_mapper import (
    RegulationMapper,
)
from regulation_mapping.regulation_types import PipelineResult
from regulation_mapping.regulation_resolvers import (
    generate_regulation_id,
    generate_rule_id,
)
from regulation_mapping.scope_filter import ScopeFilter
from regulation_mapping.tributary_enricher import TributaryEnricher


# ===================================================================
# Shared helpers — gazetteer, features, and wiring
# ===================================================================


class IntegrationGazetteer:
    """Gazetteer that serves both linker and mapper with the same features.

    Exposes the full set of search methods used by both modules:
    - Linker: search(), search_by_gnis_id, get_stream_by_id, etc.
    - Mapper: get_feature_by_id(), metadata dict
    """

    def __init__(self) -> None:
        self._features: Dict[str, FWAFeature] = {}
        self._by_name: Dict[str, List[FWAFeature]] = defaultdict(list)
        self._by_gnis: Dict[str, List[FWAFeature]] = defaultdict(list)
        self._by_watershed: Dict[str, List[FWAFeature]] = defaultdict(list)
        self._by_waterbody_key: Dict[str, List[FWAFeature]] = defaultdict(list)
        self._by_blk: Dict[str, List[FWAFeature]] = defaultdict(list)
        self._by_poly_id: Dict[str, FWAFeature] = {}
        self._by_linear_id: Dict[str, FWAFeature] = {}
        self._by_ungazetted: Dict[str, FWAFeature] = {}

        self.metadata: Dict[Any, Dict[str, dict]] = defaultdict(dict)
        self.data_accessor = None
        self._reprojected_admin_cache: dict = {}

    def add(self, feature: FWAFeature) -> FWAFeature:
        self._features[feature.fwa_id] = feature
        ftype = feature.feature_type or FeatureType.STREAM

        # Build metadata entry (used by mapper's merge_features + build_feature_index)
        self.metadata[ftype][feature.fwa_id] = {
            "gnis_name": feature.gnis_name or "",
            "gnis_id": feature.gnis_id or "",
            "blue_line_key": feature.blue_line_key or "",
            "fwa_watershed_code": feature.fwa_watershed_code or "",
            "waterbody_key": feature.waterbody_key or "",
            "zones": feature.zones,
            "zones_unbuffered": feature.zones,
            "mgmt_units": feature.mgmt_units or [],
            "mgmt_units_unbuffered": feature.mgmt_units or [],
            "region_names": [],
        }

        # Register in name-based search indexes (used by linker)
        if feature.gnis_name:
            self._by_name[feature.gnis_name.strip().title()].append(feature)
        if feature.gnis_id:
            self._by_gnis[feature.gnis_id].append(feature)
        if feature.fwa_watershed_code:
            self._by_watershed[feature.fwa_watershed_code].append(feature)
        if feature.waterbody_key:
            self._by_waterbody_key[feature.waterbody_key].append(feature)
        if feature.blue_line_key:
            self._by_blk[feature.blue_line_key].append(feature)
        if feature.geometry_type == "multilinestring":
            self._by_linear_id[feature.fwa_id] = feature
        else:
            self._by_poly_id[feature.fwa_id] = feature

        return feature

    # --- Linker-compatible methods ---

    def search(self, name: str, zone_number: str | None = None) -> List[FWAFeature]:
        normalized = name.strip().title()
        matches = list(self._by_name.get(normalized, []))
        if zone_number:
            matches = [f for f in matches if zone_number in f.zones]
        return matches

    def search_by_gnis_id(self, gnis_id: str) -> List[FWAFeature]:
        return list(self._by_gnis.get(str(gnis_id), []))

    def search_by_watershed_code(self, code: str) -> List[FWAFeature]:
        return list(self._by_watershed.get(code, []))

    def get_waterbody_by_key(self, key: str) -> List[FWAFeature]:
        return list(self._by_waterbody_key.get(str(key), []))

    def search_by_blue_line_key(self, blk: str) -> List[FWAFeature]:
        return list(self._by_blk.get(str(blk), []))

    def get_polygon_by_id(self, poly_id: str) -> Optional[FWAFeature]:
        return self._by_poly_id.get(str(poly_id))

    def get_stream_by_id(self, linear_id: str) -> Optional[FWAFeature]:
        return self._by_linear_id.get(str(linear_id))

    def get_ungazetted_by_id(self, feature_id: str) -> Optional[FWAFeature]:
        return self._by_ungazetted.get(feature_id)

    def search_unnamed_by_inherited_gnis_id(self, gnis_id: str) -> List[FWAFeature]:
        return []

    # --- Mapper-compatible methods ---

    def get_feature_by_id(self, feature_id: str) -> Optional[FWAFeature]:
        return self._features.get(feature_id)

    def get_features(self) -> list:
        return list(self._features.values())

    def get_valid_stream_ids(self) -> set:
        return set(self._by_linear_id.keys())


# --- Fake corrections stub ---


class FakeIntegrationCorrections:
    """Stub ManualCorrections with configurable entries."""

    def __init__(
        self,
        direct_matches: dict | None = None,
        name_variation_links: dict | None = None,
        skip_entries: dict | None = None,
        admin_direct_matches: dict | None = None,
    ):
        self._direct_matches = direct_matches or {}
        self._name_variation_links = name_variation_links or {}
        self._skip_entries = skip_entries or {}
        self._admin_direct_matches = admin_direct_matches or {}
        self._ungazetted_waterbodies = {}

    def get_direct_match(self, region: str, name: str) -> DirectMatch | None:
        return self._direct_matches.get(region, {}).get(name.upper())

    def get_skip_entry(self, region: str, name: str) -> SkipEntry | None:
        return self._skip_entries.get(region, {}).get(name.upper())

    def get_name_variation_link(
        self, region: str, name: str
    ) -> NameVariationLink | None:
        return self._name_variation_links.get(region, {}).get(name.upper())

    def get_admin_direct_match(self, region: str, name: str) -> AdminDirectMatch | None:
        return self._admin_direct_matches.get(region, {}).get(name.upper())

    def get_ungazetted_waterbody(self, waterbody_id: str):
        return self._ungazetted_waterbodies.get(waterbody_id)

    def get_all_feature_name_variations(self):
        return []


# --- Wiring helpers ---


def _make_stream(
    fwa_id: str,
    gnis_name: str = "Test Creek",
    gnis_id: str = "88888",
    fwa_watershed_code: str = "100-000000",
    blue_line_key: str = "BLK_100",
    zones: list | None = None,
) -> FWAFeature:
    return FWAFeature(
        fwa_id=fwa_id,
        geometry_type="multilinestring",
        zones=zones or ["3"],
        feature_type=FeatureType.STREAM,
        gnis_name=gnis_name,
        gnis_id=gnis_id,
        fwa_watershed_code=fwa_watershed_code,
        blue_line_key=blue_line_key,
        mgmt_units=["3-15"],
    )


def _make_lake(
    fwa_id: str,
    gnis_name: str = "Test Lake",
    gnis_id: str = "77777",
    waterbody_key: str = "WBK_100",
    zones: list | None = None,
) -> FWAFeature:
    return FWAFeature(
        fwa_id=fwa_id,
        geometry_type="polygon",
        zones=zones or ["3"],
        feature_type=FeatureType.LAKE,
        gnis_name=gnis_name,
        gnis_id=gnis_id,
        waterbody_key=waterbody_key,
        mgmt_units=["3-15"],
    )


def _make_regulation(
    name: str,
    region: str = "REGION 3 - Thompson-Nicola",
    rules: list | None = None,
) -> dict:
    return {
        "identity": {"name_verbatim": name},
        "region": region,
        "mu": ["3-15"],
        "rules": rules
        or [
            {
                "rule_text_verbatim": f"Daily quota 2 for {name}",
                "restriction": {"type": "DAILY_QUOTA", "details": "2 per day"},
                "scope": {},
            }
        ],
    }


def _build_integrated_mapper(
    gazetteer: IntegrationGazetteer,
    corrections: FakeIntegrationCorrections | None = None,
) -> RegulationMapper:
    """Wire a real linker + real mapper with shared gazetteer."""
    corr = corrections or FakeIntegrationCorrections()
    linker = WaterbodyLinker(gazetteer=gazetteer, manual_corrections=corr)
    return RegulationMapper(
        linker=linker,
        scope_filter=ScopeFilter(),
        tributary_enricher=TributaryEnricher(graph_source=None),
        gpkg_path=None,
    )


# ===================================================================
# TestDirectMatchFlow — linker resolves, mapper consumes
# ===================================================================


class TestDirectMatchFlow:
    """DirectMatch in corrections → linker resolves → mapper receives features."""

    def test_valid_direct_match_flows_to_mapper(self):
        """A properly configured DirectMatch → SUCCESS → mapper links features."""
        gaz = IntegrationGazetteer()
        stream = gaz.add(_make_stream("s1", gnis_name="Alpha Creek", gnis_id="111"))

        corrections = FakeIntegrationCorrections(
            direct_matches={
                "Region 3": {
                    "ALPHA CREEK": DirectMatch(
                        note="Known match",
                        gnis_ids=["111"],
                    ),
                }
            }
        )
        mapper = _build_integrated_mapper(gaz, corrections)
        regs = [_make_regulation("ALPHA CREEK")]

        with patch.object(mapper, "_process_provincial_regulations", return_value={}):
            result = mapper.run(regs, include_zone_regulations=False)

        assert "s1" in result.feature_to_regs
        assert result.stats.linked_regulations == 1

    def test_invalid_feature_id_raises_direct_match_error(self):
        """DirectMatch with a gnis_id not in gazetteer → DirectMatchError.

        This is the key safety test: a misconfigured DirectMatch must NEVER
        silently produce an empty/partial result. The error must propagate
        through the mapper.
        """
        gaz = IntegrationGazetteer()
        # Register stream_s1 with gnis_id="111"
        gaz.add(_make_stream("s1", gnis_name="Alpha Creek", gnis_id="111"))

        # Configure DirectMatch with BOTH valid (111) and invalid (999) IDs
        corrections = FakeIntegrationCorrections(
            direct_matches={
                "Region 3": {
                    "ALPHA CREEK": DirectMatch(
                        note="One valid, one bad",
                        gnis_ids=["111", "999"],  # 999 doesn't exist
                    ),
                }
            }
        )
        mapper = _build_integrated_mapper(gaz, corrections)
        regs = [_make_regulation("ALPHA CREEK")]

        with pytest.raises(DirectMatchError, match="999"):
            with patch.object(
                mapper, "_process_provincial_regulations", return_value={}
            ):
                mapper.run(regs, include_zone_regulations=False)

    def test_single_bogus_linear_feature_id_raises(self):
        """DirectMatch with a linear_feature_id not in gazetteer → error.

        Even a single invalid ID among valid ones must raise.
        """
        gaz = IntegrationGazetteer()
        gaz.add(_make_stream("s1", gnis_name="Alpha Creek", gnis_id="111"))

        corrections = FakeIntegrationCorrections(
            direct_matches={
                "Region 3": {
                    "ALPHA CREEK": DirectMatch(
                        note="Uses linear_feature_id",
                        linear_feature_ids=["s1", "NONEXISTENT_99"],
                    ),
                }
            }
        )
        mapper = _build_integrated_mapper(gaz, corrections)
        regs = [_make_regulation("ALPHA CREEK")]

        with pytest.raises(DirectMatchError, match="NONEXISTENT_99"):
            with patch.object(
                mapper, "_process_provincial_regulations", return_value={}
            ):
                mapper.run(regs, include_zone_regulations=False)

    def test_all_ids_bogus_raises(self):
        """DirectMatch where every ID is invalid → error."""
        gaz = IntegrationGazetteer()

        corrections = FakeIntegrationCorrections(
            direct_matches={
                "Region 3": {
                    "PHANTOM LAKE": DirectMatch(
                        note="All IDs bogus",
                        gnis_ids=["NOPE"],
                        waterbody_keys=["ALSO_NOPE"],
                    ),
                }
            }
        )
        mapper = _build_integrated_mapper(gaz, corrections)
        regs = [_make_regulation("PHANTOM LAKE")]

        with pytest.raises(DirectMatchError, match="unresolved"):
            with patch.object(
                mapper, "_process_provincial_regulations", return_value={}
            ):
                mapper.run(regs, include_zone_regulations=False)

    def test_additional_info_propagates_to_regulation_details(self):
        """DirectMatch.additional_info → synthetic Note rule in regulation_details."""
        gaz = IntegrationGazetteer()
        gaz.add(_make_stream("s1", gnis_name="Alpha Creek", gnis_id="111"))

        corrections = FakeIntegrationCorrections(
            direct_matches={
                "Region 3": {
                    "ALPHA CREEK": DirectMatch(
                        note="Has additional info",
                        gnis_ids=["111"],
                        additional_info="Electric motors only",
                    ),
                }
            }
        )
        mapper = _build_integrated_mapper(gaz, corrections)
        regs = [_make_regulation("ALPHA CREEK")]

        with patch.object(mapper, "_process_provincial_regulations", return_value={}):
            result = mapper.run(regs, include_zone_regulations=False)

        # Find the synthetic Note rule
        note_rules = [
            (rid, d)
            for rid, d in result.regulation_details.items()
            if d.get("restriction_type") == "Note"
        ]
        assert len(note_rules) == 1
        assert "Electric motors only" in note_rules[0][1]["restriction_details"]


# ===================================================================
# TestNameVariationFlow — alias resolution end-to-end
# ===================================================================


class TestNameVariationFlow:
    """NameVariationLink → linker returns NAME_VARIATION → mapper tracks alias."""

    def test_name_variation_creates_pending_alias(self):
        """NameVariation entry → mapper's pending_name_variation_aliases populated."""
        gaz = IntegrationGazetteer()
        gaz.add(_make_stream("s1", gnis_name="Alpha Creek", gnis_id="111"))

        corrections = FakeIntegrationCorrections(
            name_variation_links={
                "Region 3": {
                    "ALPHA CR": NameVariationLink(
                        primary_name="Alpha Creek",
                        note="Abbreviation",
                    ),
                }
            }
        )
        mapper = _build_integrated_mapper(gaz, corrections)
        regs = [_make_regulation("ALPHA CR")]
        result = mapper._pass1_prelink(regs)

        assert "ALPHA CREEK" in result.pending_name_variation_aliases
        aliases = result.pending_name_variation_aliases["ALPHA CREEK"]
        assert any(a[0] == "ALPHA CR" for a in aliases)

    def test_name_variation_does_not_create_linked_cache_entry(self):
        """NAME_VARIATION status → no entry in linked_cache (no features linked)."""
        gaz = IntegrationGazetteer()
        gaz.add(_make_stream("s1", gnis_name="Alpha Creek", gnis_id="111"))

        corrections = FakeIntegrationCorrections(
            name_variation_links={
                "Region 3": {
                    "ALPHA CR": NameVariationLink(
                        primary_name="Alpha Creek",
                        note="Abbreviation",
                    ),
                }
            }
        )
        mapper = _build_integrated_mapper(gaz, corrections)
        regs = [_make_regulation("ALPHA CR")]
        result = mapper._pass1_prelink(regs)

        assert len(result.linked_cache) == 0

    def test_name_variation_with_direct_match_primary(self):
        """Primary name matches via DirectMatch → name variation alias tracked."""
        gaz = IntegrationGazetteer()
        stream = gaz.add(_make_stream("s1", gnis_name="Alpha Creek", gnis_id="111"))

        corrections = FakeIntegrationCorrections(
            direct_matches={
                "Region 3": {
                    "ALPHA CREEK": DirectMatch(note="Primary", gnis_ids=["111"]),
                }
            },
            name_variation_links={
                "Region 3": {
                    "THE ALPHA": NameVariationLink(
                        primary_name="Alpha Creek",
                        note="Alias",
                    ),
                }
            },
        )
        mapper = _build_integrated_mapper(gaz, corrections)
        # Process both: primary first, then alias
        regs = [
            _make_regulation("ALPHA CREEK"),
            _make_regulation("THE ALPHA"),
        ]
        result = mapper._pass1_prelink(regs)

        # Primary linked normally
        assert len(result.linked_cache) == 1
        assert result.linked_cache[0].base_features == [stream]
        # Alias tracked
        assert "ALPHA CREEK" in result.pending_name_variation_aliases


# ===================================================================
# TestDirectMatchErrorPropagation — invalid IDs must surface
# ===================================================================


class TestDirectMatchErrorPropagation:
    """Ensure DirectMatchError is NOT caught/swallowed by the mapper.

    The user's requirement: "failed links should raise exceptions...
    everything should be focused on not hiding errors in the sauce."
    """

    def test_error_propagates_through_pass1(self):
        """DirectMatchError raised in linker → propagates through _pass1_prelink."""
        gaz = IntegrationGazetteer()
        # No features registered — every ID will fail

        corrections = FakeIntegrationCorrections(
            direct_matches={
                "Region 3": {
                    "BOGUS CREEK": DirectMatch(
                        note="Bad entry",
                        linear_feature_ids=["NONEXISTENT"],
                    ),
                }
            }
        )
        mapper = _build_integrated_mapper(gaz, corrections)
        regs = [_make_regulation("BOGUS CREEK")]

        with pytest.raises(DirectMatchError, match="NONEXISTENT"):
            mapper._pass1_prelink(regs)

    def test_error_propagates_through_run(self):
        """DirectMatchError raised in linker → propagates through run()."""
        gaz = IntegrationGazetteer()

        corrections = FakeIntegrationCorrections(
            direct_matches={
                "Region 3": {
                    "BOGUS CREEK": DirectMatch(
                        note="Bad entry",
                        waterbody_poly_ids=["FAKE_POLY_ID"],
                    ),
                }
            }
        )
        mapper = _build_integrated_mapper(gaz, corrections)
        regs = [_make_regulation("BOGUS CREEK")]

        with pytest.raises(DirectMatchError, match="FAKE_POLY_ID"):
            with patch.object(
                mapper, "_process_provincial_regulations", return_value={}
            ):
                mapper.run(regs, include_zone_regulations=False)

    def test_partial_resolution_raises(self):
        """One valid ID + one invalid ID → error, not partial success."""
        gaz = IntegrationGazetteer()
        gaz.add(_make_stream("s1", gnis_name="Alpha Creek", gnis_id="111"))

        corrections = FakeIntegrationCorrections(
            direct_matches={
                "Region 3": {
                    "ALPHA CREEK": DirectMatch(
                        note="Partial",
                        gnis_ids=["111"],  # valid
                        waterbody_keys=["NONEXISTENT_WBK"],  # invalid
                    ),
                }
            }
        )
        mapper = _build_integrated_mapper(gaz, corrections)
        regs = [_make_regulation("ALPHA CREEK")]

        with pytest.raises(DirectMatchError, match="NONEXISTENT_WBK"):
            mapper._pass1_prelink(regs)


# ===================================================================
# TestNaturalSearchFlow — name-based matching end-to-end
# ===================================================================


class TestNaturalSearchFlow:
    """Linker's natural name search → mapper receives features."""

    def test_natural_search_happy_path(self):
        """Name in gazetteer → linker finds it → mapper links it."""
        gaz = IntegrationGazetteer()
        gaz.add(_make_stream("s1", gnis_name="Alpha Creek", gnis_id="111"))

        mapper = _build_integrated_mapper(gaz)
        regs = [_make_regulation("Alpha Creek")]

        with patch.object(mapper, "_process_provincial_regulations", return_value={}):
            result = mapper.run(regs, include_zone_regulations=False)

        assert "s1" in result.feature_to_regs

    def test_not_found_increments_failed_stats(self):
        """Name NOT in gazetteer and no corrections → failed stat.

        When all regulations fail, feature_to_regs is empty and run()
        correctly asserts. We test at the pass1 level to check stats.
        """
        gaz = IntegrationGazetteer()
        mapper = _build_integrated_mapper(gaz)
        regs = [_make_regulation("Nonexistent River")]

        result = mapper._pass1_prelink(regs)
        assert result.stats_failed == 1
        assert len(result.linked_cache) == 0


# ===================================================================
# TestResolveDirectMatchFeaturesImportPath — deferred import
# ===================================================================


class TestResolveDirectMatchFeaturesImportPath:
    """Verify the deferred import from linker → resolvers works correctly.

    The linker uses ``from .regulation_resolvers import resolve_direct_match_features``
    as a deferred import inside _apply_direct_match. This test confirms the
    import path works by exercising the full DirectMatch flow.
    """

    def test_deferred_import_works_end_to_end(self):
        gaz = IntegrationGazetteer()
        gaz.add(
            _make_lake("l1", gnis_name="Big Lake", gnis_id="222", waterbody_key="WBK_1")
        )

        corrections = FakeIntegrationCorrections(
            direct_matches={
                "Region 3": {
                    "BIG LAKE": DirectMatch(
                        note="Test deferred import",
                        gnis_ids=["222"],
                    ),
                }
            }
        )
        mapper = _build_integrated_mapper(gaz, corrections)
        regs = [_make_regulation("BIG LAKE")]

        with patch.object(mapper, "_process_provincial_regulations", return_value={}):
            result = mapper.run(regs, include_zone_regulations=False)

        assert "l1" in result.feature_to_regs


# ===================================================================
# TestPipelineWiring — construction chain
# ===================================================================


class TestPipelineWiring:
    """Verify that mapper.linker is the same instance passed in."""

    def test_mapper_holds_linker_reference(self):
        gaz = IntegrationGazetteer()
        linker = WaterbodyLinker(
            gazetteer=gaz,
            manual_corrections=FakeIntegrationCorrections(),
        )
        mapper = RegulationMapper(
            linker=linker,
            scope_filter=ScopeFilter(),
            tributary_enricher=TributaryEnricher(graph_source=None),
        )
        assert mapper.linker is linker
        assert mapper.gazetteer is gaz

    def test_mapper_gets_corrections_through_linker(self):
        """Mapper accesses linker.corrections for feature name variations."""
        gaz = IntegrationGazetteer()
        corr = FakeIntegrationCorrections()
        linker = WaterbodyLinker(gazetteer=gaz, manual_corrections=corr)
        mapper = RegulationMapper(
            linker=linker,
            scope_filter=ScopeFilter(),
            tributary_enricher=TributaryEnricher(graph_source=None),
        )
        assert mapper.linker.corrections is corr
