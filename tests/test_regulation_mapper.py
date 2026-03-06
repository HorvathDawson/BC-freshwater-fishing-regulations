"""
Tests for RegulationMapper orchestrator logic.

Uses mock linker / gazetteer with real ScopeFilter and TributaryEnricher
(both are pass-through / no-op in tests). Validates that the mapper wires
linking, scoping, enriching, and merging correctly.
"""

from __future__ import annotations

from collections import Counter, defaultdict
from typing import Any, Dict, List, Optional, Set
from unittest.mock import MagicMock, patch

import pytest

from fwa_pipeline.metadata_gazetteer import FWAFeature, FeatureType
from regulation_mapping.linker import LinkStatus
from regulation_mapping.regulation_mapper import (
    Pass1Result,
    Pass2Result,
    RegulationMapper,
)
from regulation_mapping.regulation_types import (
    LinkedRegulation,
    MergedGroup,
    PipelineResult,
    RegulationMappingStats,
)
from regulation_mapping.regulation_resolvers import (
    generate_regulation_id,
    generate_rule_id,
)
from regulation_mapping.scope_filter import ScopeFilter
from regulation_mapping.tributary_enricher import TributaryEnricher


# ===================================================================
# Helpers — fake features, linker, and gazetteer
# ===================================================================


def _make_feature(
    fwa_id: str,
    feature_type: FeatureType = FeatureType.STREAM,
    gnis_name: str = "",
    waterbody_key: str = "",
    blue_line_key: str = "",
    zones: list | None = None,
    mgmt_units: list | None = None,
) -> FWAFeature:
    """Build a minimal FWAFeature for mapper tests."""
    return FWAFeature(
        fwa_id=fwa_id,
        geometry_type=(
            "multilinestring" if feature_type == FeatureType.STREAM else "polygon"
        ),
        zones=zones or ["3"],
        feature_type=feature_type,
        gnis_name=gnis_name or None,
        waterbody_key=waterbody_key or None,
        blue_line_key=blue_line_key or None,
        mgmt_units=mgmt_units or ["3-15"],
    )


# Two canonical test features used across many tests
STREAM_A = _make_feature("stream_a", gnis_name="Alpha Creek", blue_line_key="BLK_1")
STREAM_B = _make_feature("stream_b", gnis_name="Beta Creek", blue_line_key="BLK_2")
LAKE_C = _make_feature(
    "lake_c",
    feature_type=FeatureType.LAKE,
    gnis_name="Gamma Lake",
    waterbody_key="WBK_1",
)


class _FakeLinkingResult:
    """Mimics ``LinkingResult`` from the linker module."""

    def __init__(
        self,
        status: LinkStatus = LinkStatus.SUCCESS,
        matched_features: list | None = None,
        matched_name: str = "",
        link_method: str = "",
        admin_match: object = None,
        additional_info: str | None = None,
    ):
        self.status = status
        self.matched_features = matched_features or []
        self.matched_name = matched_name
        self.link_method = link_method
        self.admin_match = admin_match
        self.additional_info = additional_info


class FakeMapperGazetteer:
    """Minimal gazetteer stub: get_feature_by_id + metadata dict."""

    def __init__(self, features: list[FWAFeature] | None = None):
        self._features: Dict[str, FWAFeature] = {}
        self.metadata: Dict[FeatureType, Dict[str, dict]] = defaultdict(dict)
        self.data_accessor = None
        self._reprojected_admin_cache: dict = {}
        for f in features or []:
            self._features[f.fwa_id] = f
            ftype = f.feature_type or FeatureType.STREAM
            self.metadata[ftype][f.fwa_id] = {
                "gnis_name": f.gnis_name or "",
                "gnis_id": "",
                "blue_line_key": f.blue_line_key or "",
                "waterbody_key": f.waterbody_key or "",
                "zones": f.zones,
                "zones_unbuffered": f.zones,
                "mgmt_units": f.mgmt_units or [],
                "mgmt_units_unbuffered": f.mgmt_units or [],
                "region_names": [],
            }

    def get_feature_by_id(self, feature_id: str) -> Optional[FWAFeature]:
        return self._features.get(feature_id)

    def get_features(self) -> list:
        return list(self._features.values())

    def get_valid_stream_ids(self) -> set:
        return {
            fid
            for fid, f in self._features.items()
            if f.feature_type == FeatureType.STREAM
        }


class _FakeCorrections:
    """Stub for ManualCorrections — returns empty name variations."""

    def get_all_feature_name_variations(self):
        return []


class FakeLinker:
    """Minimal WaterbodyLinker stub that returns pre-configured results."""

    def __init__(
        self,
        gazetteer: FakeMapperGazetteer,
        results: dict[str, _FakeLinkingResult] | None = None,
    ):
        self.gazetteer = gazetteer
        self.corrections = _FakeCorrections()
        self._results = results or {}
        self._default = _FakeLinkingResult(
            status=LinkStatus.NOT_FOUND, matched_features=[]
        )

    def link_waterbody(
        self,
        region: str | None = None,
        mgmt_units: list | None = None,
        name_verbatim: str = "",
    ) -> _FakeLinkingResult:
        return self._results.get(name_verbatim, self._default)


# ===================================================================
# Fixtures
# ===================================================================


@pytest.fixture
def gazetteer():
    return FakeMapperGazetteer([STREAM_A, STREAM_B, LAKE_C])


@pytest.fixture
def mapper(gazetteer):
    """Build a RegulationMapper with fake linker, real scope filter, and no-op enricher."""
    linker = FakeLinker(
        gazetteer,
        results={
            "Alpha Creek": _FakeLinkingResult(
                status=LinkStatus.SUCCESS,
                matched_features=[STREAM_A],
            ),
            "Beta Creek": _FakeLinkingResult(
                status=LinkStatus.SUCCESS,
                matched_features=[STREAM_B],
            ),
            "Gamma Lake": _FakeLinkingResult(
                status=LinkStatus.SUCCESS,
                matched_features=[LAKE_C],
            ),
        },
    )
    scope_filter = ScopeFilter()
    enricher = TributaryEnricher(graph_source=None)
    return RegulationMapper(
        linker=linker,
        scope_filter=scope_filter,
        tributary_enricher=enricher,
        gpkg_path=None,
    )


def _make_regulation(
    name: str,
    rules: list[dict] | None = None,
    region: str = "REGION 3 - Thompson-Nicola",
) -> dict:
    """Build a minimal synopsis regulation dict."""
    return {
        "identity": {
            "name_verbatim": name,
        },
        "region": region,
        "mu": ["3-15"],
        "rules": rules
        or [
            {
                "rule_text_verbatim": f"Daily quota 2 for {name}",
                "restriction": {
                    "type": "DAILY_QUOTA",
                    "details": "2 per day",
                },
                "scope": {},
            }
        ],
    }


# ===================================================================
# TestPass1Prelink — dispatch contract
# ===================================================================


class TestPass1Prelink:
    """Verify _pass1_prelink dispatches correctly for each LinkStatus."""

    def test_success_populates_linked_cache(self, mapper):
        regs = [_make_regulation("Alpha Creek")]
        result = mapper._pass1_prelink(regs)

        assert len(result.linked_cache) == 1
        assert result.linked_cache[0].regulation_id == generate_regulation_id(0)
        assert result.linked_cache[0].base_features == [STREAM_A]
        assert result.stats_linked == 1

    def test_not_found_increments_failed(self, mapper):
        regs = [_make_regulation("Nonexistent River")]
        result = mapper._pass1_prelink(regs)

        assert len(result.linked_cache) == 0
        assert result.stats_failed == 1

    def test_name_variation_populates_aliases(self, gazetteer):
        """NAME_VARIATION status → alias mapping, no linked_cache entry."""
        linker = FakeLinker(
            gazetteer,
            results={
                "Alpha Cr": _FakeLinkingResult(
                    status=LinkStatus.NAME_VARIATION,
                    matched_name="Alpha Creek",
                ),
            },
        )
        m = RegulationMapper(
            linker=linker,
            scope_filter=ScopeFilter(),
            tributary_enricher=TributaryEnricher(graph_source=None),
        )
        regs = [_make_regulation("Alpha Cr")]
        result = m._pass1_prelink(regs)

        assert len(result.linked_cache) == 0
        assert "ALPHA CREEK" in result.pending_name_variation_aliases
        aliases = result.pending_name_variation_aliases["ALPHA CREEK"]
        assert any(a[0] == "Alpha Cr" for a in aliases)

    def test_error_flag_skips_regulation(self, mapper):
        """Regulations with ``error`` flag are counted as bad, not processed."""
        regs = [{"error": "parse failure", "identity": {"name_verbatim": "Bad"}}]
        result = mapper._pass1_prelink(regs)

        assert len(result.linked_cache) == 0
        assert result.stats_bad == 1

    def test_additional_info_carried_to_linked_regulation(self, gazetteer):
        """additional_info from link result → stored on LinkedRegulation."""
        linker = FakeLinker(
            gazetteer,
            results={
                "Alpha Creek": _FakeLinkingResult(
                    status=LinkStatus.SUCCESS,
                    matched_features=[STREAM_A],
                    additional_info="Permit required",
                ),
            },
        )
        m = RegulationMapper(
            linker=linker,
            scope_filter=ScopeFilter(),
            tributary_enricher=TributaryEnricher(graph_source=None),
        )
        regs = [_make_regulation("Alpha Creek")]
        result = m._pass1_prelink(regs)

        assert result.linked_cache[0].additional_info == "Permit required"

    def test_polygon_waterbody_keys_tracked(self, gazetteer):
        """Polygon features get their waterbody_key tracked."""
        linker = FakeLinker(
            gazetteer,
            results={
                "Gamma Lake": _FakeLinkingResult(
                    status=LinkStatus.SUCCESS,
                    matched_features=[LAKE_C],
                ),
            },
        )
        m = RegulationMapper(
            linker=linker,
            scope_filter=ScopeFilter(),
            tributary_enricher=TributaryEnricher(graph_source=None),
        )
        regs = [_make_regulation("Gamma Lake")]
        result = m._pass1_prelink(regs)

        assert "WBK_1" in result.linked_waterbody_keys_of_polygon


# ===================================================================
# TestPass2RuleMapping — rule-to-feature index
# ===================================================================


class TestPass2RuleMapping:
    """Verify _pass2_map_rules builds the feature-to-rule index correctly."""

    def test_single_rule_mapped(self, mapper):
        linked = [
            LinkedRegulation(
                idx=0,
                regulation=_make_regulation("Alpha Creek"),
                regulation_id=generate_regulation_id(0),
                base_features=[STREAM_A],
                is_admin_match=False,
            )
        ]
        result = mapper._pass2_map_rules(linked)

        rule_id = generate_rule_id(0, 0)
        assert rule_id in result.feature_to_regs.get("stream_a", [])
        assert result.stats_rules_processed == 1
        assert result.stats_rule_to_feature_mappings == 1

    def test_multiple_rules_per_regulation(self, mapper):
        reg = _make_regulation(
            "Alpha Creek",
            rules=[
                {
                    "rule_text_verbatim": "Rule A",
                    "restriction": {"type": "DAILY_QUOTA", "details": "2"},
                    "scope": {},
                },
                {
                    "rule_text_verbatim": "Rule B",
                    "restriction": {"type": "CLOSURE", "details": "Closed"},
                    "scope": {},
                },
            ],
        )
        linked = [
            LinkedRegulation(
                idx=0,
                regulation=reg,
                regulation_id=generate_regulation_id(0),
                base_features=[STREAM_A],
                is_admin_match=False,
            )
        ]
        result = mapper._pass2_map_rules(linked)

        stream_rules = result.feature_to_regs.get("stream_a", [])
        assert generate_rule_id(0, 0) in stream_rules
        assert generate_rule_id(0, 1) in stream_rules
        assert result.stats_rules_processed == 2

    def test_additional_info_creates_synthetic_note_rule(self, mapper):
        """Additional info from linker → synthetic 'Note' rule appended."""
        reg = _make_regulation("Alpha Creek")
        linked = [
            LinkedRegulation(
                idx=0,
                regulation=reg,
                regulation_id=generate_regulation_id(0),
                base_features=[STREAM_A],
                is_admin_match=False,
                additional_info="Permit required for motorboats",
            )
        ]
        result = mapper._pass2_map_rules(linked)

        # Original rule + synthetic note = 2 rules
        stream_rules = result.feature_to_regs.get("stream_a", [])
        assert len(stream_rules) == 2
        # The synthetic note rule is at index 1
        note_rule_id = generate_rule_id(0, 1)
        assert note_rule_id in stream_rules
        assert result.regulation_details[note_rule_id]["restriction_type"] == "Note"

    def test_regulation_details_populated(self, mapper):
        linked = [
            LinkedRegulation(
                idx=0,
                regulation=_make_regulation("Alpha Creek"),
                regulation_id=generate_regulation_id(0),
                base_features=[STREAM_A],
                is_admin_match=False,
            )
        ]
        result = mapper._pass2_map_rules(linked)

        rule_id = generate_rule_id(0, 0)
        detail = result.regulation_details[rule_id]
        assert detail["source"] == "synopsis"
        assert detail["waterbody_name"] == "Alpha Creek"
        assert detail["restriction_type"] == "DAILY_QUOTA"


# ===================================================================
# TestPass1Pass2MergeInto — merge result objects into mapper state
# ===================================================================


class TestPass1Pass2MergeInto:
    """Verify the merge_into methods correctly populate mapper state."""

    def test_pass1_merge_populates_regulation_names(self, mapper):
        p1 = Pass1Result(regulation_names={"reg_00000": "Alpha Creek"})
        p1.merge_into(mapper)
        assert mapper.regulation_names["reg_00000"] == "Alpha Creek"

    def test_pass1_merge_accumulates_stats(self, mapper):
        p1 = Pass1Result(stats_linked=3, stats_failed=1, stats_bad=2)
        p1.merge_into(mapper)
        assert mapper.stats.linked_regulations == 3
        assert mapper.stats.failed_to_link_regulations == 1
        assert mapper.stats.bad_regulation == 2

    def test_pass2_merge_extends_feature_to_regs(self, mapper):
        """Pass2Result.merge_into appends (not overwrites) to feature_to_regs."""
        mapper.feature_to_regs = {"stream_a": ["existing_rule"]}
        p2 = Pass2Result(feature_to_regs={"stream_a": ["new_rule"]})
        p2.merge_into(mapper)
        assert mapper.feature_to_regs["stream_a"] == ["existing_rule", "new_rule"]

    def test_pass2_merge_accumulates_tributary_assignments(self, mapper):
        p2 = Pass2Result(tributary_assignments={"stream_a": {"rule_1"}})
        p2.merge_into(mapper)
        assert "rule_1" in mapper.tributary_assignments["stream_a"]

    def test_pass1_link_status_counter(self, mapper):
        p1 = Pass1Result()
        p1.link_status_counts["SUCCESS"] = 5
        p1.link_status_counts["NOT_FOUND"] = 2
        p1.merge_into(mapper)
        assert mapper.stats.link_status_counts["SUCCESS"] == 5


# ===================================================================
# TestRunEndToEnd — full pipeline happy path
# ===================================================================


class TestRunEndToEnd:
    """Verify the run() method produces a valid PipelineResult."""

    def test_happy_path_two_regulations(self, mapper):
        """Two synopsis regulations → PipelineResult with merged groups."""
        regs = [
            _make_regulation("Alpha Creek"),
            _make_regulation("Beta Creek"),
        ]
        with patch.object(mapper, "_process_provincial_regulations", return_value={}):
            result = mapper.run(regs, include_zone_regulations=False)

        assert isinstance(result, PipelineResult)
        assert len(result.merged_groups) >= 2
        # Both features should appear in feature_to_regs
        assert "stream_a" in result.feature_to_regs
        assert "stream_b" in result.feature_to_regs

    def test_stats_populated(self, mapper):
        regs = [_make_regulation("Alpha Creek")]
        with patch.object(mapper, "_process_provincial_regulations", return_value={}):
            result = mapper.run(regs, include_zone_regulations=False)

        assert result.stats is not None
        assert result.stats.total_regulations == 1
        assert result.stats.linked_regulations == 1

    def test_regulation_names_in_result(self, mapper):
        regs = [_make_regulation("Alpha Creek")]
        with patch.object(mapper, "_process_provincial_regulations", return_value={}):
            result = mapper.run(regs)

        reg_id = generate_regulation_id(0)
        assert result.regulation_names.get(reg_id) == "Alpha Creek"


# ===================================================================
# TestMergeFeatures — feature grouping logic
# ===================================================================


class TestMergeFeatures:
    """Verify merge_features groups features correctly."""

    def test_same_blk_same_regs_grouped_together(self, mapper):
        """Two features sharing BLK + regulation set → same group."""
        feature_to_regs = {
            "stream_a": [generate_rule_id(0, 0)],
        }
        groups = mapper.merge_features(feature_to_regs)
        # stream_a should be in exactly one group
        found = [g for g in groups.values() if "stream_a" in g.feature_ids]
        assert len(found) == 1

    def test_different_regs_separate_groups(self, mapper):
        """Two features with different regulation sets → separate groups."""
        feature_to_regs = {
            "stream_a": [generate_rule_id(0, 0)],
            "stream_b": [generate_rule_id(1, 0)],
        }
        groups = mapper.merge_features(feature_to_regs)
        assert len(groups) == 2

    def test_feature_count_accurate(self, mapper):
        feature_to_regs = {
            "stream_a": [generate_rule_id(0, 0)],
        }
        groups = mapper.merge_features(feature_to_regs)
        for g in groups.values():
            assert g.feature_count == len(g.feature_ids)

    def test_zones_populated(self, mapper):
        feature_to_regs = {
            "stream_a": [generate_rule_id(0, 0)],
        }
        groups = mapper.merge_features(feature_to_regs)
        group = list(groups.values())[0]
        assert "3" in group.zones

    def test_unknown_feature_skipped_gracefully(self, mapper):
        """Feature not in gazetteer → skipped, no crash."""
        feature_to_regs = {
            "nonexistent_123": [generate_rule_id(0, 0)],
        }
        groups = mapper.merge_features(feature_to_regs)
        assert len(groups) == 0


# ===================================================================
# TestLinkedWaterbodyKeysOfPolygon — polygon key tracking
# ===================================================================


class TestLinkedWaterbodyKeysOfPolygon:
    """Verify polygon waterbody keys are tracked correctly across phases."""

    def test_lake_waterbody_key_tracked(self, gazetteer):
        linker = FakeLinker(
            gazetteer,
            results={
                "Gamma Lake": _FakeLinkingResult(
                    status=LinkStatus.SUCCESS,
                    matched_features=[LAKE_C],
                ),
            },
        )
        m = RegulationMapper(
            linker=linker,
            scope_filter=ScopeFilter(),
            tributary_enricher=TributaryEnricher(graph_source=None),
        )
        regs = [_make_regulation("Gamma Lake")]
        result = m._pass1_prelink(regs)
        result.merge_into(m)

        assert "WBK_1" in m.linked_waterbody_keys_of_polygon

    def test_stream_does_not_add_waterbody_key(self, gazetteer):
        linker = FakeLinker(
            gazetteer,
            results={
                "Alpha Creek": _FakeLinkingResult(
                    status=LinkStatus.SUCCESS,
                    matched_features=[STREAM_A],
                ),
            },
        )
        m = RegulationMapper(
            linker=linker,
            scope_filter=ScopeFilter(),
            tributary_enricher=TributaryEnricher(graph_source=None),
        )
        regs = [_make_regulation("Alpha Creek")]
        result = m._pass1_prelink(regs)
        result.merge_into(m)

        assert len(m.linked_waterbody_keys_of_polygon) == 0
