"""
Tests for feature_merger — the extracted merge/grouping logic.

Tests the module-level functions directly (no RegulationMapper instance needed),
using lightweight FWAFeature stubs and explicit keyword params.
"""

from __future__ import annotations

from collections import defaultdict
from typing import Dict, Optional

import pytest

from fwa_pipeline.metadata_gazetteer import FWAFeature, FeatureType
from regulation_mapping.feature_merger import (
    aggregate_group_metadata,
    build_name_variants_for_group,
    build_physical_grouping_key,
    merge_features,
)
from regulation_mapping.regulation_resolvers import generate_rule_id


# ===================================================================
# Helpers
# ===================================================================


def _feat(
    fwa_id: str = "f1",
    feature_type: FeatureType = FeatureType.STREAM,
    gnis_name: str = "",
    gnis_name_2: str = "",
    gnis_id: str = "",
    waterbody_key: str = "",
    blue_line_key: str = "BLK_1",
    zones: list | None = None,
    mgmt_units: list | None = None,
    region_names: list | None = None,
    fwa_watershed_code: str = "",
    inherited_gnis_names: list | None = None,
) -> FWAFeature:
    """Build a minimal FWAFeature for merger tests."""
    return FWAFeature(
        fwa_id=fwa_id,
        geometry_type=(
            "multilinestring" if feature_type == FeatureType.STREAM else "polygon"
        ),
        zones=zones or ["3"],
        feature_type=feature_type,
        gnis_name=gnis_name or None,
        gnis_name_2=gnis_name_2 or None,
        gnis_id=gnis_id or None,
        waterbody_key=waterbody_key or None,
        blue_line_key=blue_line_key or None,
        mgmt_units=mgmt_units or [],
        region_names=region_names or ["Kootenay"],
        fwa_watershed_code=fwa_watershed_code or None,
        inherited_gnis_names=inherited_gnis_names,
    )


def _empty_name_variant_kwargs() -> dict:
    """Default keyword-only args for build_name_variants_for_group."""
    return dict(
        admin_regulation_ids=set(),
        regulation_names={},
        feature_to_regs={},
        tributary_assignments={},
        regulation_parent_gnis={},
        feature_to_aliases={},
    )


class _StubGazetteer:
    """Minimal gazetteer for merge_features — just get_feature_by_id."""

    def __init__(self, features: list[FWAFeature]):
        self._features = {f.fwa_id: f for f in features}

    def get_feature_by_id(self, fid: str) -> Optional[FWAFeature]:
        return self._features.get(fid)


# ===================================================================
# build_physical_grouping_key
# ===================================================================


class TestBuildPhysicalGroupingKey:
    """Key construction rules for feature grouping."""

    def test_stream_with_blk(self):
        feat = _feat(blue_line_key="BLK_1")
        key = build_physical_grouping_key(feat, "f1", linked_waterbody_keys=set())
        assert key == "streams_blue_line_BLK_1"

    def test_stream_with_blk_and_linked_wbk(self):
        feat = _feat(blue_line_key="BLK_1", waterbody_key="WBK_1")
        key = build_physical_grouping_key(feat, "f1", linked_waterbody_keys={"WBK_1"})
        assert key == "streams_blue_line_BLK_1_waterbody_WBK_1"

    def test_stream_with_blk_and_unlinked_wbk(self):
        """WBK is not in the linked set → key based on BLK only."""
        feat = _feat(blue_line_key="BLK_1", waterbody_key="WBK_ORPHAN")
        key = build_physical_grouping_key(feat, "f1", linked_waterbody_keys=set())
        assert key == "streams_blue_line_BLK_1"

    def test_named_stream_with_gnis_id(self):
        feat = _feat(blue_line_key="BLK_1", gnis_id="GNIS_42")
        key = build_physical_grouping_key(feat, "f1", linked_waterbody_keys=set())
        assert key == "streams_blue_line_BLK_1_gnis_GNIS_42"

    def test_polygon_with_linked_wbk(self):
        feat = _feat(
            feature_type=FeatureType.LAKE,
            blue_line_key="",
            waterbody_key="WBK_1",
        )
        key = build_physical_grouping_key(feat, "f1", linked_waterbody_keys={"WBK_1"})
        assert key == "lakes_waterbody_WBK_1"

    def test_feature_without_ids_uses_feature_id(self):
        feat = _feat(blue_line_key="", waterbody_key="")
        key = build_physical_grouping_key(feat, "orphan_1", linked_waterbody_keys=set())
        assert key == "streams_feature_orphan_1"


# ===================================================================
# aggregate_group_metadata
# ===================================================================


class TestAggregateGroupMetadata:
    """Metadata aggregation across features in a group."""

    def test_single_feature_metadata(self):
        feat = _feat(
            gnis_name="Alpha Creek",
            blue_line_key="BLK_1",
            waterbody_key="WBK_1",
            zones=["3"],
            mgmt_units=["3-15"],
            fwa_watershed_code="100-000000",
        )
        meta = aggregate_group_metadata(
            [("f1", feat)],
            linked_waterbody_keys={"WBK_1"},
        )
        assert meta["gnis_name"] == "Alpha Creek"
        assert meta["blue_line_key"] == "BLK_1"
        assert meta["waterbody_key"] == "WBK_1"
        assert "3" in meta["zones"]
        assert "3-15" in meta["mgmt_units"]
        assert meta["fwa_watershed_code"] == "100-000000"

    def test_multiple_gnis_names_returns_empty(self):
        """Two different GNIS names → empty string (ambiguous)."""
        f1 = _feat(fwa_id="f1", gnis_name="Alpha Creek")
        f2 = _feat(fwa_id="f2", gnis_name="Beta Creek")
        meta = aggregate_group_metadata(
            [("f1", f1), ("f2", f2)],
            linked_waterbody_keys=set(),
        )
        assert meta["gnis_name"] == ""

    def test_single_gnis_name_across_features(self):
        f1 = _feat(fwa_id="f1", gnis_name="Alpha Creek")
        f2 = _feat(fwa_id="f2", gnis_name="Alpha Creek")
        meta = aggregate_group_metadata(
            [("f1", f1), ("f2", f2)],
            linked_waterbody_keys=set(),
        )
        assert meta["gnis_name"] == "Alpha Creek"

    def test_waterbody_key_only_for_linked_polygons(self):
        """WBK should only be used if it's in the linked set."""
        feat = _feat(waterbody_key="WBK_ORPHAN")
        meta = aggregate_group_metadata(
            [("f1", feat)],
            linked_waterbody_keys=set(),
        )
        assert meta["waterbody_key"] is None

    def test_zones_sorted(self):
        f1 = _feat(fwa_id="f1", zones=["5", "2"])
        f2 = _feat(fwa_id="f2", zones=["3"])
        meta = aggregate_group_metadata(
            [("f1", f1), ("f2", f2)],
            linked_waterbody_keys=set(),
        )
        assert meta["zones"] == ("2", "3", "5")

    def test_mgmt_units_merged(self):
        f1 = _feat(fwa_id="f1", mgmt_units=["3-15"])
        f2 = _feat(fwa_id="f2", mgmt_units=["3-16", "3-15"])
        meta = aggregate_group_metadata(
            [("f1", f1), ("f2", f2)],
            linked_waterbody_keys=set(),
        )
        assert set(meta["mgmt_units"]) == {"3-15", "3-16"}


# ===================================================================
# build_name_variants_for_group
# ===================================================================


class TestBuildNameVariantsForGroup:
    """Name variant collection and tributary tagging."""

    def test_gnis_names_collected(self):
        feat = _feat(gnis_name="ALPHA CREEK")
        variants = build_name_variants_for_group(
            features_data=[("f1", feat)],
            regulation_ids=(),
            **_empty_name_variant_kwargs(),
        )
        names = [v["name"] for v in variants]
        assert "Alpha Creek" in names

    def test_gnis_name_2_collected(self):
        feat = _feat(gnis_name="Alpha Creek", gnis_name_2="LITTLE ALPHA CREEK")
        variants = build_name_variants_for_group(
            features_data=[("f1", feat)],
            regulation_ids=(),
            **_empty_name_variant_kwargs(),
        )
        names = [v["name"] for v in variants]
        assert "Alpha Creek" in names
        assert "Little Alpha Creek" in names

    def test_regulation_name_added(self):
        feat = _feat(gnis_name="")
        kwargs = _empty_name_variant_kwargs()
        reg_id = generate_rule_id(0, 0)
        base_id = "reg_00000"  # parse_base_regulation_id(reg_id) returns this
        kwargs["regulation_names"] = {base_id: "Big River Special"}
        kwargs["feature_to_regs"] = {"f1": [reg_id]}
        variants = build_name_variants_for_group(
            features_data=[("f1", feat)],
            regulation_ids=(reg_id,),
            **kwargs,
        )
        names = [v["name"] for v in variants]
        assert "Big River Special" in names

    def test_admin_regulation_names_excluded(self):
        feat = _feat(gnis_name="")
        kwargs = _empty_name_variant_kwargs()
        reg_id = generate_rule_id(0, 0)
        base_id = "reg_00000"
        kwargs["regulation_names"] = {base_id: "Liard River Watershed"}
        kwargs["admin_regulation_ids"] = {base_id}
        kwargs["feature_to_regs"] = {"f1": [reg_id]}
        variants = build_name_variants_for_group(
            features_data=[("f1", feat)],
            regulation_ids=(reg_id,),
            **kwargs,
        )
        names = [v["name"] for v in variants]
        assert "Liard River Watershed" not in names

    def test_aliases_included(self):
        feat = _feat(gnis_name="Alpha Creek")
        kwargs = _empty_name_variant_kwargs()
        kwargs["feature_to_aliases"] = {"f1": {"Local Name"}}
        variants = build_name_variants_for_group(
            features_data=[("f1", feat)],
            regulation_ids=(),
            **kwargs,
        )
        names = [v["name"] for v in variants]
        assert "Local Name" in names

    def test_tributary_name_tagged(self):
        """Inherited regulation names should be tagged from_tributary=True."""
        feat = _feat(fwa_id="f1", gnis_name="")
        reg_id = generate_rule_id(0, 0)
        base_id = "reg_00000"
        kwargs = _empty_name_variant_kwargs()
        kwargs["regulation_names"] = {base_id: "Parent River"}
        kwargs["feature_to_regs"] = {"f1": [reg_id]}
        # All of f1's rules for this regulation came from tributary enrichment
        kwargs["tributary_assignments"] = {"f1": {reg_id}}
        variants = build_name_variants_for_group(
            features_data=[("f1", feat)],
            regulation_ids=(reg_id,),
            **kwargs,
        )
        trib_names = [v for v in variants if v["from_tributary"]]
        assert len(trib_names) >= 1
        assert trib_names[0]["name"] == "Parent River"

    def test_deduplication_by_name(self):
        """Same name from GNIS and regulation → single entry."""
        feat = _feat(gnis_name="ALPHA CREEK")
        reg_id = generate_rule_id(0, 0)
        base_id = "reg_00000"
        kwargs = _empty_name_variant_kwargs()
        kwargs["regulation_names"] = {base_id: "Alpha Creek"}
        kwargs["feature_to_regs"] = {"f1": [reg_id]}
        variants = build_name_variants_for_group(
            features_data=[("f1", feat)],
            regulation_ids=(reg_id,),
            **kwargs,
        )
        names = [v["name"] for v in variants]
        assert names.count("Alpha Creek") == 1

    def test_provincial_zone_names_excluded(self):
        """Names starting with prov_ or zone_ should not appear."""
        feat = _feat(gnis_name="")
        kwargs = _empty_name_variant_kwargs()
        kwargs["regulation_names"] = {
            "prov_bait_ban": "Bait Ban",
            "zone_3_default": "Zone 3",
        }
        kwargs["feature_to_regs"] = {"f1": ["prov_bait_ban", "zone_3_default"]}
        variants = build_name_variants_for_group(
            features_data=[("f1", feat)],
            regulation_ids=("prov_bait_ban", "zone_3_default"),
            **kwargs,
        )
        assert len(variants) == 0


# ===================================================================
# merge_features (integration-level)
# ===================================================================


class TestMergeFeatures:
    """Test the top-level merge_features function with explicit params."""

    @pytest.fixture()
    def features(self):
        return [
            _feat(fwa_id="s1", gnis_name="Alpha Creek", blue_line_key="BLK_1"),
            _feat(fwa_id="s2", gnis_name="Alpha Creek", blue_line_key="BLK_1"),
            _feat(fwa_id="s3", gnis_name="Beta Creek", blue_line_key="BLK_2"),
        ]

    @pytest.fixture()
    def gazetteer(self, features):
        return _StubGazetteer(features)

    def test_same_blk_same_regs_grouped(self, gazetteer):
        f2r = {"s1": ["rule_0"], "s2": ["rule_0"]}
        groups = merge_features(
            f2r,
            gazetteer=gazetteer,
            linked_waterbody_keys=set(),
            admin_regulation_ids=set(),
            regulation_names={},
            feature_to_regs_full=f2r,
            tributary_assignments={},
            regulation_parent_gnis={},
            feature_to_aliases={},
            feature_display_name_overrides={},
        )
        assert len(groups) == 1
        group = list(groups.values())[0]
        assert set(group.feature_ids) == {"s1", "s2"}

    def test_different_regs_separate_groups(self, gazetteer):
        f2r = {"s1": ["rule_0"], "s3": ["rule_1"]}
        groups = merge_features(
            f2r,
            gazetteer=gazetteer,
            linked_waterbody_keys=set(),
            admin_regulation_ids=set(),
            regulation_names={},
            feature_to_regs_full=f2r,
            tributary_assignments={},
            regulation_parent_gnis={},
            feature_to_aliases={},
            feature_display_name_overrides={},
        )
        assert len(groups) == 2

    def test_unknown_feature_skipped(self, gazetteer):
        f2r = {"nonexistent": ["rule_0"]}
        groups = merge_features(
            f2r,
            gazetteer=gazetteer,
            linked_waterbody_keys=set(),
            admin_regulation_ids=set(),
            regulation_names={},
            feature_to_regs_full=f2r,
            tributary_assignments={},
            regulation_parent_gnis={},
            feature_to_aliases={},
            feature_display_name_overrides={},
        )
        assert len(groups) == 0

    def test_feature_count_accurate(self, gazetteer):
        f2r = {"s1": ["rule_0"], "s2": ["rule_0"]}
        groups = merge_features(
            f2r,
            gazetteer=gazetteer,
            linked_waterbody_keys=set(),
            admin_regulation_ids=set(),
            regulation_names={},
            feature_to_regs_full=f2r,
            tributary_assignments={},
            regulation_parent_gnis={},
            feature_to_aliases={},
            feature_display_name_overrides={},
        )
        group = list(groups.values())[0]
        assert group.feature_count == 2

    def test_display_name_override_applied(self, gazetteer):
        f2r = {"s1": ["rule_0"]}
        groups = merge_features(
            f2r,
            gazetteer=gazetteer,
            linked_waterbody_keys=set(),
            admin_regulation_ids=set(),
            regulation_names={},
            feature_to_regs_full=f2r,
            tributary_assignments={},
            regulation_parent_gnis={},
            feature_to_aliases={},
            feature_display_name_overrides={"s1": "Custom Name"},
        )
        group = list(groups.values())[0]
        assert group.display_name_override == "Custom Name"

    def test_progress_wrapper_called(self, gazetteer):
        """progress_wrapper kwarg is invoked if provided."""
        calls = []

        def fake_progress(iterable, desc, unit):
            calls.append((desc, unit))
            return iterable

        f2r = {"s1": ["rule_0"]}
        merge_features(
            f2r,
            gazetteer=gazetteer,
            linked_waterbody_keys=set(),
            admin_regulation_ids=set(),
            regulation_names={},
            feature_to_regs_full=f2r,
            tributary_assignments={},
            regulation_parent_gnis={},
            feature_to_aliases={},
            feature_display_name_overrides={},
            progress_wrapper=fake_progress,
        )
        assert len(calls) == 1
        assert calls[0][0] == "Grouping features"
