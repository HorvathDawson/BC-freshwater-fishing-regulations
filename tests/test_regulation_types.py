"""
Tests for regulation types: protocols, dataclasses, and the ZoneScopeOptimizer.

Imports from ``regulation_types.py`` — the canonical home for shared types.
"""

from __future__ import annotations

from dataclasses import dataclass, FrozenInstanceError
from typing import Any, Dict, List, Optional

import pytest

from fwa_pipeline.metadata_gazetteer import FeatureType
from regulation_mapping.admin_target import AdminTarget
from regulation_mapping.regulation_types import (
    DirectMatchTarget,
    MergedGroup,
    PipelineResult,
    RegulationMappingStats,
    ZoneScopeClassification,
    ZoneScopeOptimizer,
    ZoneWideTarget,
)
from regulation_mapping.linking_corrections import DirectMatch
from regulation_mapping.zone_base_regulations import ZoneRegulation


# ===================================================================
# DirectMatchTarget protocol conformance
# ===================================================================


class TestDirectMatchTargetProtocol:
    """Verify that the documented implementors satisfy the protocol."""

    def test_direct_match_satisfies_protocol(self):
        dm = DirectMatch(
            note="test",
            gnis_ids=["12345"],
        )
        assert isinstance(dm, DirectMatchTarget)

    def test_zone_regulation_satisfies_protocol(self):
        zr = ZoneRegulation(
            regulation_id="zone_test",
            zone_ids=["1"],
            rule_text="Test rule",
            restriction={"type": "DAILY_QUOTA", "details": {}},
            notes="test",
            gnis_ids=["12345"],
        )
        assert isinstance(zr, DirectMatchTarget)

    def test_arbitrary_object_missing_fields_fails(self):
        """An object missing required protocol fields must NOT satisfy the check."""

        @dataclass
        class Incomplete:
            gnis_ids: Optional[List[str]] = None
            # Missing all other required fields

        obj = Incomplete()
        assert not isinstance(obj, DirectMatchTarget)


# ===================================================================
# ZoneWideTarget protocol conformance
# ===================================================================


class TestZoneWideTargetProtocol:
    """Verify ZoneWideTarget is satisfied by ZoneRegulation."""

    def test_zone_regulation_satisfies_zone_wide(self):
        zr = ZoneRegulation(
            regulation_id="zone_zw",
            zone_ids=["3"],
            rule_text="Test",
            restriction={"type": "DAILY_QUOTA", "details": {}},
            notes="test",
        )
        assert isinstance(zr, ZoneWideTarget)

    def test_object_missing_zone_ids_fails(self):
        @dataclass
        class NoZones:
            feature_types: Optional[List[FeatureType]] = None
            mu_ids: Optional[List[str]] = None
            exclude_mu_ids: Optional[List[str]] = None
            include_mu_ids: Optional[List[str]] = None
            # Missing zone_ids

        assert not isinstance(NoZones(), ZoneWideTarget)


# ===================================================================
# MergedGroup
# ===================================================================


class TestMergedGroup:
    """Verify frozen dataclass semantics and display_name logic."""

    def test_frozen_immutability(self):
        mg = MergedGroup(
            group_id="g1",
            feature_ids=("f1",),
            regulation_ids=("r1",),
        )
        with pytest.raises(FrozenInstanceError):
            mg.group_id = "g2"  # type: ignore[misc]

    def test_display_name_override_takes_priority(self):
        mg = MergedGroup(
            group_id="g1",
            feature_ids=("f1",),
            regulation_ids=("r1",),
            gnis_name="GNIS Name",
            display_name_override="Override",
            inherited_gnis_name="Inherited",
        )
        assert mg.display_name == "Override"

    def test_display_name_falls_to_gnis(self):
        mg = MergedGroup(
            group_id="g1",
            feature_ids=("f1",),
            regulation_ids=("r1",),
            gnis_name="GNIS Name",
        )
        assert mg.display_name == "GNIS Name"

    def test_display_name_falls_to_inherited(self):
        mg = MergedGroup(
            group_id="g1",
            feature_ids=("f1",),
            regulation_ids=("r1",),
            inherited_gnis_name="Inherited",
        )
        assert mg.display_name == "Inherited"

    def test_display_name_falls_to_first_non_tributary_variant(self):
        mg = MergedGroup(
            group_id="g1",
            feature_ids=("f1",),
            regulation_ids=("r1",),
            name_variants=(
                {"name": "Trib Name", "from_tributary": True},
                {"name": "Direct Name", "from_tributary": False},
            ),
        )
        assert mg.display_name == "Direct Name"

    def test_display_name_empty_when_nothing_set(self):
        mg = MergedGroup(
            group_id="g1",
            feature_ids=("f1",),
            regulation_ids=("r1",),
        )
        assert mg.display_name == ""


# ===================================================================
# PipelineResult
# ===================================================================


class TestPipelineResult:
    """Verify PipelineResult construction and immutability."""

    def test_defaults_produce_empty_result(self):
        pr = PipelineResult()
        assert pr.merged_groups == {}
        assert pr.feature_to_regs == {}
        assert pr.regulation_names == {}
        assert pr.admin_regulation_ids == set()
        assert pr.regulation_details == {}

    def test_frozen_immutability(self):
        pr = PipelineResult()
        with pytest.raises(FrozenInstanceError):
            pr.merged_groups = {"bad": None}  # type: ignore[misc]


# ===================================================================
# RegulationMappingStats
# ===================================================================


class TestRegulationMappingStats:
    """Verify stats dataclass defaults and mutation."""

    def test_defaults_zero(self):
        stats = RegulationMappingStats()
        assert stats.total_regulations == 0
        assert stats.linked_regulations == 0
        assert stats.failed_to_link_regulations == 0
        assert stats.total_rules_processed == 0

    def test_counter_accumulation(self):
        stats = RegulationMappingStats()
        stats.link_status_counts.update(["SUCCESS", "SUCCESS", "NOT_FOUND"])
        assert stats.link_status_counts["SUCCESS"] == 2
        assert stats.link_status_counts["NOT_FOUND"] == 1


# ===================================================================
# ZoneScopeOptimizer
# ===================================================================


class TestZoneScopeOptimizer:
    """Verify zone regulation classification logic."""

    @staticmethod
    def _make_zone_reg(
        *,
        reg_id: str = "zone_test",
        zone_ids: List[str] | None = None,
        admin_targets: List[AdminTarget] | None = None,
        gnis_ids: List[str] | None = None,
    ) -> ZoneRegulation:
        return ZoneRegulation(
            regulation_id=reg_id,
            zone_ids=zone_ids or ["1"],
            rule_text="test",
            restriction={"type": "DAILY_QUOTA", "details": {}},
            notes="test",
            admin_targets=admin_targets,
            gnis_ids=gnis_ids,
        )

    def test_classify_zone_wide(self):
        """Reg with no admin/direct fields → zone_wide_groups."""
        reg = self._make_zone_reg()
        result = ZoneScopeOptimizer.classify([reg])

        assert len(result.zone_wide_groups) == 1
        assert result.admin_regs == []
        assert result.direct_regs == []

    def test_classify_admin(self):
        """Reg with admin_targets → admin_regs."""
        reg = self._make_zone_reg(
            admin_targets=[AdminTarget(layer="parks_bc", feature_id="PARK_1")]
        )
        result = ZoneScopeOptimizer.classify([reg])

        assert len(result.admin_regs) == 1
        assert result.direct_regs == []
        assert result.zone_wide_groups == {}

    def test_classify_direct(self):
        """Reg with gnis_ids (direct target) → direct_regs."""
        reg = self._make_zone_reg(gnis_ids=["12345"])
        result = ZoneScopeOptimizer.classify([reg])

        assert len(result.direct_regs) == 1
        assert result.admin_regs == []
        assert result.zone_wide_groups == {}

    def test_scope_key_deduplicates_order(self):
        """Two regs with same scope fields in different order → same group."""
        reg_a = ZoneRegulation(
            regulation_id="zone_a",
            zone_ids=["1", "2"],
            rule_text="A",
            restriction={"type": "DAILY_QUOTA", "details": {}},
            notes="A",
            feature_types=[FeatureType.STREAM, FeatureType.LAKE],
        )
        reg_b = ZoneRegulation(
            regulation_id="zone_b",
            zone_ids=["2", "1"],
            rule_text="B",
            restriction={"type": "DAILY_QUOTA", "details": {}},
            notes="B",
            feature_types=[FeatureType.LAKE, FeatureType.STREAM],
        )
        result = ZoneScopeOptimizer.classify([reg_a, reg_b])

        # Both should land in the SAME zone_wide group
        assert len(result.zone_wide_groups) == 1
        group_regs = list(result.zone_wide_groups.values())[0]
        assert len(group_regs) == 2

    def test_mixed_classification(self):
        """A mix of all three types classified correctly."""
        regs = [
            self._make_zone_reg(reg_id="zone_zw"),
            self._make_zone_reg(
                reg_id="zone_admin",
                admin_targets=[AdminTarget(layer="parks_bc", feature_id="PK")],
            ),
            self._make_zone_reg(reg_id="zone_direct", gnis_ids=["99"]),
        ]
        result = ZoneScopeOptimizer.classify(regs)
        assert len(result.zone_wide_groups) == 1
        assert len(result.admin_regs) == 1
        assert len(result.direct_regs) == 1
