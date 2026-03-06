"""
Tests for regulation resolver functions (pure/near-pure helpers).

Imports from ``regulation_resolvers.py`` — the canonical home for
stateless resolution functions and constants.
"""

from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Set

import pytest

from fwa_pipeline.metadata_gazetteer import FWAFeature, FeatureType
from regulation_mapping.linking_corrections import DirectMatch
from regulation_mapping.zone_base_regulations import ZoneRegulation
from regulation_mapping.regulation_resolvers import (
    ALL_FWA_TYPES,
    ZONE_REGION_NAMES,
    collect_features_from_index,
    exclude_features_from_index,
    generate_regulation_id,
    generate_rule_id,
    include_features_from_index,
    is_regulation_inherited,
    parse_base_regulation_id,
    parse_region,
    resolve_direct_match_features,
    resolve_direct_match_ids,
    resolve_zone_wide_ids,
)


# ===================================================================
# Minimal fake gazetteer for resolver tests
# ===================================================================


class FakeResolverGazetteer:
    """Mimics MetadataGazetteer lookup methods for resolver unit tests.

    Pre-populated with a small set of known features. Each search method
    returns features from the internal registry. This lets us test the
    resolver logic without needing a real GPKG or pickle file.
    """

    def __init__(self) -> None:
        # Pre-register features keyed by their primary lookup value
        self._streams: Dict[str, FWAFeature] = {}
        self._polygons: Dict[str, FWAFeature] = {}
        self._by_gnis: Dict[str, List[FWAFeature]] = defaultdict(list)
        self._by_watershed: Dict[str, List[FWAFeature]] = defaultdict(list)
        self._by_waterbody_key: Dict[str, List[FWAFeature]] = defaultdict(list)
        self._by_blk: Dict[str, List[FWAFeature]] = defaultdict(list)
        self._ungazetted: Dict[str, FWAFeature] = {}
        # inherited GNIS index: gnis_id → list of unnamed FWAFeatures
        self._by_inherited_gnis: Dict[str, List[FWAFeature]] = defaultdict(list)

        # Metadata dict matching real gazetteer structure
        self.metadata: Dict[FeatureType, Dict[str, dict]] = {
            FeatureType.STREAM: {},
            FeatureType.LAKE: {},
            FeatureType.WETLAND: {},
            FeatureType.MANMADE: {},
        }

    def add_stream(
        self,
        fwa_id: str,
        *,
        gnis_name: str = "",
        gnis_id: str = "",
        blue_line_key: str = "",
        fwa_watershed_code: str = "",
        waterbody_key: str = "",
        zones: list | None = None,
        mgmt_units: list | None = None,
        inherited_gnis_names: list | None = None,
    ) -> FWAFeature:
        feat = FWAFeature(
            fwa_id=fwa_id,
            geometry_type="multilinestring",
            zones=zones or ["3"],
            feature_type=FeatureType.STREAM,
            gnis_name=gnis_name or None,
            gnis_id=gnis_id or None,
            blue_line_key=blue_line_key or None,
            fwa_watershed_code=fwa_watershed_code or None,
            waterbody_key=waterbody_key or None,
            mgmt_units=mgmt_units or ["3-15"],
            inherited_gnis_names=inherited_gnis_names,
        )
        self._streams[fwa_id] = feat
        if gnis_id:
            self._by_gnis[gnis_id].append(feat)
        if fwa_watershed_code:
            self._by_watershed[fwa_watershed_code].append(feat)
        if blue_line_key:
            self._by_blk[blue_line_key].append(feat)
        if waterbody_key:
            self._by_waterbody_key[waterbody_key].append(feat)
        # Index inherited GNIS for unnamed streams
        if not gnis_name and inherited_gnis_names:
            for entry in inherited_gnis_names:
                inh_gnis_id = str(entry.get("gnis_id", ""))
                if inh_gnis_id:
                    self._by_inherited_gnis[inh_gnis_id].append(feat)

        # Add to metadata dict for build_feature_index compatibility
        self.metadata[FeatureType.STREAM][fwa_id] = {
            "gnis_name": gnis_name,
            "gnis_id": gnis_id,
            "blue_line_key": blue_line_key,
            "fwa_watershed_code": fwa_watershed_code,
            "waterbody_key": waterbody_key,
            "zones": zones or ["3"],
            "zones_unbuffered": zones or ["3"],
            "mgmt_units": mgmt_units or ["3-15"],
            "mgmt_units_unbuffered": mgmt_units or ["3-15"],
            "inherited_gnis_names": inherited_gnis_names,
        }
        return feat

    def add_polygon(
        self,
        fwa_id: str,
        feature_type: FeatureType = FeatureType.LAKE,
        *,
        gnis_name: str = "",
        gnis_id: str = "",
        waterbody_key: str = "",
        blue_line_key: str = "",
        zones: list | None = None,
        mgmt_units: list | None = None,
    ) -> FWAFeature:
        feat = FWAFeature(
            fwa_id=fwa_id,
            geometry_type="polygon",
            zones=zones or ["3"],
            feature_type=feature_type,
            gnis_name=gnis_name or None,
            gnis_id=gnis_id or None,
            waterbody_key=waterbody_key or None,
            blue_line_key=blue_line_key or None,
            mgmt_units=mgmt_units or ["3-15"],
        )
        self._polygons[fwa_id] = feat
        if gnis_id:
            self._by_gnis[gnis_id].append(feat)
        if waterbody_key:
            self._by_waterbody_key[waterbody_key].append(feat)
        if blue_line_key:
            self._by_blk[blue_line_key].append(feat)

        self.metadata[feature_type][fwa_id] = {
            "gnis_name": gnis_name,
            "gnis_id": gnis_id,
            "waterbody_key": waterbody_key,
            "blue_line_key": blue_line_key,
            "zones": zones or ["3"],
            "zones_unbuffered": zones or ["3"],
            "mgmt_units": mgmt_units or ["3-15"],
            "mgmt_units_unbuffered": mgmt_units or ["3-15"],
        }
        return feat

    def add_ungazetted(self, fwa_id: str) -> FWAFeature:
        feat = FWAFeature(
            fwa_id=fwa_id,
            geometry_type="polygon",
            zones=["3"],
            feature_type=FeatureType.UNGAZETTED,
        )
        self._ungazetted[fwa_id] = feat
        return feat

    # --- MetadataGazetteer-compatible search methods ---

    def get_stream_by_id(self, linear_feature_id: str) -> Optional[FWAFeature]:
        return self._streams.get(linear_feature_id)

    def get_polygon_by_id(self, poly_id: str) -> Optional[FWAFeature]:
        return self._polygons.get(poly_id)

    def search_by_gnis_id(self, gnis_id: str) -> List[FWAFeature]:
        return self._by_gnis.get(gnis_id, [])

    def search_by_watershed_code(self, fwa_watershed_code: str) -> List[FWAFeature]:
        return self._by_watershed.get(fwa_watershed_code, [])

    def get_waterbody_by_key(self, waterbody_key: str) -> List[FWAFeature]:
        return self._by_waterbody_key.get(waterbody_key, [])

    def search_by_blue_line_key(self, blue_line_key: str) -> List[FWAFeature]:
        return self._by_blk.get(blue_line_key, [])

    def search_unnamed_by_inherited_gnis_id(self, gnis_id: str) -> List[FWAFeature]:
        return self._by_inherited_gnis.get(gnis_id, [])

    def get_ungazetted_by_id(self, feature_id: str) -> Optional[FWAFeature]:
        return self._ungazetted.get(feature_id)


# ===================================================================
# Fixtures
# ===================================================================


@pytest.fixture
def gaz() -> FakeResolverGazetteer:
    """Pre-populated gazetteer with a small set of test features."""
    g = FakeResolverGazetteer()
    g.add_stream(
        "stream_1",
        gnis_name="Test Creek",
        gnis_id="111",
        blue_line_key="BLK_100",
        fwa_watershed_code="100-000000",
        zones=["3"],
        mgmt_units=["3-15"],
    )
    g.add_stream(
        "stream_2",
        gnis_name="Test Creek",
        gnis_id="111",
        blue_line_key="BLK_100",
        fwa_watershed_code="100-000000",
        zones=["3"],
        mgmt_units=["3-15"],
    )
    g.add_polygon(
        "lake_1",
        FeatureType.LAKE,
        gnis_name="Big Lake",
        gnis_id="222",
        waterbody_key="WBK_50",
        zones=["3"],
        mgmt_units=["3-15"],
    )
    g.add_polygon(
        "wetland_1",
        FeatureType.WETLAND,
        gnis_name="Foggy Marsh",
        gnis_id="333",
        zones=["4"],
        mgmt_units=["4-2"],
    )
    g.add_ungazetted("ungaz_1")
    return g


# ===================================================================
# parse_region
# ===================================================================


class TestParseRegion:
    def test_standard_format(self):
        assert parse_region("REGION 7A - Omineca") == "Region 7A"

    def test_already_normalised(self):
        assert parse_region("Region 4") == "Region 4"

    def test_empty_returns_none(self):
        assert parse_region("") is None

    def test_garbage_returns_none(self):
        assert parse_region("foobar") is None

    def test_numeric_zone(self):
        assert parse_region("REGION 3 - Thompson-Nicola") == "Region 3"


# ===================================================================
# generate_regulation_id / generate_rule_id / parse_base_regulation_id
# ===================================================================


class TestGenerateRegulationId:
    def test_deterministic(self):
        assert generate_regulation_id(42) == "reg_00042"

    def test_zero_padded(self):
        assert generate_regulation_id(1) == "reg_00001"

    def test_uniqueness(self):
        assert generate_regulation_id(1) != generate_regulation_id(2)


class TestGenerateRuleId:
    def test_format(self):
        assert generate_rule_id(42, 0) == "reg_00042_rule0"

    def test_based_on_regulation(self):
        assert generate_rule_id(1, 3) == "reg_00001_rule3"

    def test_deterministic(self):
        assert generate_rule_id(5, 2) == generate_rule_id(5, 2)


class TestParseBaseRegulationId:
    def test_extracts_base(self):
        assert parse_base_regulation_id("reg_00042_rule0") == "reg_00042"

    def test_multi_digit_rule(self):
        assert parse_base_regulation_id("reg_00042_rule12") == "reg_00042"

    def test_no_rule_suffix_returns_self(self):
        assert parse_base_regulation_id("reg_00042") == "reg_00042"


# ===================================================================
# collect / exclude / include features from index
# ===================================================================


class TestCollectFeaturesFromIndex:
    """Test the set-collection helpers against hand-built indexes."""

    @pytest.fixture
    def index(self):
        """Zone index with 2 zones, 2 feature types, a few features each."""
        return {
            "3": {
                FeatureType.STREAM: {"s1": {}, "s2": {}},
                FeatureType.LAKE: {"l1": {}},
            },
            "4": {
                FeatureType.STREAM: {"s3": {}},
            },
        }

    def test_single_zone_single_type(self, index):
        result = collect_features_from_index(index, ["3"], [FeatureType.STREAM])
        assert result == {"s1", "s2"}

    def test_multiple_zones(self, index):
        result = collect_features_from_index(index, ["3", "4"], [FeatureType.STREAM])
        assert result == {"s1", "s2", "s3"}

    def test_multiple_types(self, index):
        result = collect_features_from_index(
            index, ["3"], [FeatureType.STREAM, FeatureType.LAKE]
        )
        assert result == {"s1", "s2", "l1"}

    def test_missing_zone_returns_empty(self, index):
        result = collect_features_from_index(index, ["99"], [FeatureType.STREAM])
        assert result == set()


class TestExcludeIncludeFeatures:
    @pytest.fixture
    def mu_index(self):
        return {
            "3-15": {FeatureType.STREAM: {"s1": {}, "s2": {}}},
            "4-2": {FeatureType.STREAM: {"s3": {}}},
        }

    def test_exclude_removes_matching(self, mu_index):
        fids = {"s1", "s2", "s3"}
        result = exclude_features_from_index(
            fids, mu_index, ["3-15"], [FeatureType.STREAM]
        )
        assert result == {"s3"}

    def test_include_adds_matching(self, mu_index):
        fids = {"s1"}
        result = include_features_from_index(
            fids, mu_index, ["4-2"], [FeatureType.STREAM]
        )
        assert result == {"s1", "s3"}

    def test_exclude_nonexistent_key_no_change(self, mu_index):
        fids = {"s1", "s2"}
        result = exclude_features_from_index(
            fids, mu_index, ["99-99"], [FeatureType.STREAM]
        )
        assert result == {"s1", "s2"}


# ===================================================================
# resolve_direct_match_features
# ===================================================================


class TestResolveDirectMatchFeatures:
    """Test feature resolution from each ID field type."""

    def test_gnis_ids(self, gaz):
        dm = DirectMatch(note="test", gnis_ids=["111"])
        features = resolve_direct_match_features(gaz, dm)
        ids = {f.fwa_id for f in features}
        assert "stream_1" in ids
        assert "stream_2" in ids

    def test_waterbody_poly_ids(self, gaz):
        dm = DirectMatch(note="test", waterbody_poly_ids=["lake_1"])
        features = resolve_direct_match_features(gaz, dm)
        assert len(features) == 1
        assert features[0].fwa_id == "lake_1"

    def test_fwa_watershed_codes(self, gaz):
        dm = DirectMatch(note="test", fwa_watershed_codes=["100-000000"])
        features = resolve_direct_match_features(gaz, dm)
        ids = {f.fwa_id for f in features}
        assert ids == {"stream_1", "stream_2"}

    def test_linear_feature_ids(self, gaz):
        dm = DirectMatch(note="test", linear_feature_ids=["stream_1"])
        features = resolve_direct_match_features(gaz, dm)
        assert len(features) == 1
        assert features[0].fwa_id == "stream_1"

    def test_blue_line_keys(self, gaz):
        dm = DirectMatch(note="test", blue_line_keys=["BLK_100"])
        features = resolve_direct_match_features(gaz, dm)
        ids = {f.fwa_id for f in features}
        assert "stream_1" in ids

    def test_waterbody_keys(self, gaz):
        dm = DirectMatch(note="test", waterbody_keys=["WBK_50"])
        features = resolve_direct_match_features(gaz, dm)
        assert len(features) == 1
        assert features[0].fwa_id == "lake_1"

    def test_ungazetted_waterbody_id(self, gaz):
        dm = DirectMatch(note="test", ungazetted_waterbody_id="ungaz_1")
        features = resolve_direct_match_features(gaz, dm)
        assert len(features) == 1
        assert features[0].fwa_id == "ungaz_1"

    def test_empty_target_returns_empty(self, gaz):
        dm = DirectMatch(note="empty")
        features = resolve_direct_match_features(gaz, dm)
        assert features == []

    def test_combined_fields(self, gaz):
        """Multiple ID fields on one target → union of all resolved features."""
        dm = DirectMatch(
            note="combo",
            gnis_ids=["222"],  # → lake_1
            linear_feature_ids=["stream_1"],  # → stream_1
        )
        features = resolve_direct_match_features(gaz, dm)
        ids = {f.fwa_id for f in features}
        assert ids == {"lake_1", "stream_1"}

    def test_nonexistent_linear_feature_id_returns_empty(self, gaz):
        """A linear_feature_id that doesn't exist simply returns nothing.

        This is the resolver-level behaviour. The LINKER is responsible for
        raising DirectMatchError when configured IDs don't resolve — the
        resolver itself returns whatever it finds (possibly empty).
        """
        dm = DirectMatch(note="bad", linear_feature_ids=["BOGUS_999"])
        features = resolve_direct_match_features(gaz, dm)
        assert features == []

    def test_gnis_ids_auto_expands_to_inherited_unnamed_streams(self, gaz):
        """DirectMatch with gnis_ids should also include unnamed streams
        that inherit the target GNIS ID (e.g. unnamed side channels of
        a named river)."""
        # Add an unnamed side channel that inherits GNIS 111 (Test Creek)
        gaz.add_stream(
            "side_channel_1",
            blue_line_key="BLK_200",
            fwa_watershed_code="100-000000-123456",
            zones=["3"],
            mgmt_units=["3-15"],
            inherited_gnis_names=[{"gnis_id": "111", "gnis_name": "Test Creek"}],
        )

        dm = DirectMatch(note="test", gnis_ids=["111"])
        features = resolve_direct_match_features(gaz, dm)
        ids = {f.fwa_id for f in features}
        # Named streams + unnamed side channel
        assert "stream_1" in ids
        assert "stream_2" in ids
        assert "side_channel_1" in ids

    def test_inherited_expansion_excludes_named_streams(self, gaz):
        """Streams with their own gnis_name should NOT be pulled in
        by the inherited expansion (they have their own identity)."""
        # Add a named stream that also inherits GNIS 111
        gaz.add_stream(
            "named_trib",
            gnis_name="Named Tributary",
            gnis_id="444",
            blue_line_key="BLK_300",
            fwa_watershed_code="100-000000-999999",
            zones=["3"],
            mgmt_units=["3-15"],
            inherited_gnis_names=[{"gnis_id": "111", "gnis_name": "Test Creek"}],
        )

        dm = DirectMatch(note="test", gnis_ids=["111"])
        features = resolve_direct_match_features(gaz, dm)
        ids = {f.fwa_id for f in features}
        # Named trib has its own gnis_name, so NOT auto-expanded
        assert "named_trib" not in ids
        # Original named streams still present
        assert "stream_1" in ids
        assert "stream_2" in ids

    def test_inherited_expansion_no_duplicates(self, gaz):
        """If a feature is already matched by gnis_id AND inherited,
        it should appear only once."""
        # Add unnamed stream inheriting GNIS 111
        gaz.add_stream(
            "side_channel_2",
            blue_line_key="BLK_400",
            fwa_watershed_code="100-000000-654321",
            zones=["3"],
            mgmt_units=["3-15"],
            inherited_gnis_names=[{"gnis_id": "111", "gnis_name": "Test Creek"}],
        )

        dm = DirectMatch(note="test", gnis_ids=["111"])
        features = resolve_direct_match_features(gaz, dm)
        fwa_ids = [f.fwa_id for f in features]
        # No duplicates
        assert len(fwa_ids) == len(set(fwa_ids))

    def test_inherited_expansion_no_match_returns_only_named(self, gaz):
        """If no unnamed streams inherit the target GNIS, only named
        features are returned (no error, no change in behavior)."""
        dm = DirectMatch(note="test", gnis_ids=["222"])  # Big Lake (polygon)
        features = resolve_direct_match_features(gaz, dm)
        ids = {f.fwa_id for f in features}
        assert ids == {"lake_1"}


class TestResolveDirectMatchIds:
    """Thin wrapper — returns IDs instead of features."""

    def test_returns_id_set(self, gaz):
        dm = DirectMatch(note="test", gnis_ids=["111"])
        ids = resolve_direct_match_ids(gaz, dm)
        assert isinstance(ids, set)
        assert "stream_1" in ids
        assert "stream_2" in ids


# ===================================================================
# resolve_zone_wide_ids
# ===================================================================


class TestResolveZoneWideIds:
    """Test zone-wide resolution with MU modifiers."""

    @pytest.fixture
    def zone_index(self):
        return {
            "3": {
                FeatureType.STREAM: {"s1": {}, "s2": {}},
                FeatureType.LAKE: {"l1": {}},
            },
            "4": {
                FeatureType.STREAM: {"s3": {}},
            },
        }

    @pytest.fixture
    def mu_index(self):
        return {
            "3-15": {FeatureType.STREAM: {"s1": {}}},
            "3-16": {FeatureType.STREAM: {"s2": {}}, FeatureType.LAKE: {"l1": {}}},
            "4-2": {FeatureType.STREAM: {"s3": {}}},
        }

    def test_basic_zone_lookup(self, zone_index, mu_index):
        reg = ZoneRegulation(
            regulation_id="zone_t",
            zone_ids=["3"],
            rule_text="Test",
            restriction={"type": "DAILY_QUOTA", "details": {}},
            notes="test",
        )
        result = resolve_zone_wide_ids(reg, zone_index, mu_index)
        assert result == {"s1", "s2", "l1"}

    def test_feature_type_filter(self, zone_index, mu_index):
        reg = ZoneRegulation(
            regulation_id="zone_t",
            zone_ids=["3"],
            rule_text="Test",
            restriction={"type": "DAILY_QUOTA", "details": {}},
            notes="test",
            feature_types=[FeatureType.STREAM],
        )
        result = resolve_zone_wide_ids(reg, zone_index, mu_index)
        assert result == {"s1", "s2"}

    def test_mu_ids_intersection(self, zone_index, mu_index):
        """mu_ids narrows the zone match to intersection with those MUs."""
        reg = ZoneRegulation(
            regulation_id="zone_t",
            zone_ids=["3"],
            rule_text="Test",
            restriction={"type": "DAILY_QUOTA", "details": {}},
            notes="test",
            feature_types=[FeatureType.STREAM],
            mu_ids=["3-15"],
        )
        result = resolve_zone_wide_ids(reg, zone_index, mu_index)
        assert result == {"s1"}

    def test_exclude_mu_ids(self, zone_index, mu_index):
        reg = ZoneRegulation(
            regulation_id="zone_t",
            zone_ids=["3"],
            rule_text="Test",
            restriction={"type": "DAILY_QUOTA", "details": {}},
            notes="test",
            feature_types=[FeatureType.STREAM],
            exclude_mu_ids=["3-15"],
        )
        result = resolve_zone_wide_ids(reg, zone_index, mu_index)
        assert result == {"s2"}

    def test_include_mu_ids(self, zone_index, mu_index):
        """include_mu_ids adds features from extra MUs."""
        reg = ZoneRegulation(
            regulation_id="zone_t",
            zone_ids=["3"],
            rule_text="Test",
            restriction={"type": "DAILY_QUOTA", "details": {}},
            notes="test",
            feature_types=[FeatureType.STREAM],
            include_mu_ids=["4-2"],
        )
        result = resolve_zone_wide_ids(reg, zone_index, mu_index)
        assert result == {"s1", "s2", "s3"}


# ===================================================================
# is_regulation_inherited
# ===================================================================


class TestIsRegulationInherited:
    def test_all_tributary_returns_true(self):
        feature_to_regs = {"f1": ["reg_00001_rule0"], "f2": ["reg_00001_rule0"]}
        tributary_assignments = {
            "f1": {"reg_00001_rule0"},
            "f2": {"reg_00001_rule0"},
        }
        assert is_regulation_inherited(
            base_regulation_id="reg_00001",
            regulation_ids=("reg_00001_rule0",),
            group_fids={"f1", "f2"},
            feature_to_regs=feature_to_regs,
            tributary_assignments=tributary_assignments,
        )

    def test_any_direct_returns_false(self):
        feature_to_regs = {"f1": ["reg_00001_rule0"], "f2": ["reg_00001_rule0"]}
        tributary_assignments = {
            "f1": {"reg_00001_rule0"},
            # f2 has NO trib assignment → direct match
        }
        assert not is_regulation_inherited(
            base_regulation_id="reg_00001",
            regulation_ids=("reg_00001_rule0",),
            group_fids={"f1", "f2"},
            feature_to_regs=feature_to_regs,
            tributary_assignments=tributary_assignments,
        )

    def test_no_mappings_returns_false(self):
        assert not is_regulation_inherited(
            base_regulation_id="reg_00001",
            regulation_ids=("reg_00001_rule0",),
            group_fids={"f1"},
            feature_to_regs={},
            tributary_assignments={},
        )

    def test_unrelated_rule_ids_returns_false(self):
        """Rule IDs that don't match the base regulation → False."""
        feature_to_regs = {"f1": ["reg_00099_rule0"]}
        assert not is_regulation_inherited(
            base_regulation_id="reg_00001",
            regulation_ids=("reg_00099_rule0",),
            group_fids={"f1"},
            feature_to_regs=feature_to_regs,
            tributary_assignments={},
        )


# ===================================================================
# Constants sanity checks
# ===================================================================


class TestConstants:
    def test_all_fwa_types_complete(self):
        """ALL_FWA_TYPES should include the 5 standard feature types."""
        assert FeatureType.STREAM in ALL_FWA_TYPES
        assert FeatureType.LAKE in ALL_FWA_TYPES
        assert FeatureType.WETLAND in ALL_FWA_TYPES
        assert FeatureType.MANMADE in ALL_FWA_TYPES
        assert FeatureType.UNGAZETTED in ALL_FWA_TYPES

    def test_zone_region_names_keys(self):
        """Every BC fishing zone should have a region name."""
        expected_zones = {"1", "2", "3", "4", "5", "6", "7A", "7B", "8"}
        assert set(ZONE_REGION_NAMES.keys()) == expected_zones
