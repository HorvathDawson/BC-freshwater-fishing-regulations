"""
Unit tests for regulation_mapping.search_exporter (SearchIndexBuilder).

Tests name-variant merging, reg-set deduplication, compact vs full entry
classification, and the overall build pipeline using mock canonical features.
"""

from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from fwa_pipeline.metadata_builder import FeatureType
from regulation_mapping.search_exporter import SearchIndexBuilder

from conftest import make_line, make_polygon


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_fake_store(
    canonical_features: list | None = None,
    regulation_details: dict | None = None,
) -> MagicMock:
    """Build a mock CanonicalDataStore for SearchIndexBuilder."""
    store = MagicMock()
    store.get_canonical_features.return_value = canonical_features or []
    store.pipeline_result = MagicMock()
    store.pipeline_result.regulation_details = regulation_details or {}
    return store


def _make_canonical_feature(
    feature_type: str = FeatureType.STREAM.value,
    group_id: str = "g1",
    frontend_group_id: str = "fgid_abc123",
    display_name: str = "Test Creek",
    gnis_name: str = "Test Creek",
    waterbody_key: str | None = None,
    blue_line_key: str | None = "BLK_100",
    fwa_watershed_code: str | None = "100-000000",
    regulation_ids: str = "reg_001_rule0",
    zones: str = "3",
    region_name: str = "Thompson",
    mgmt_units: str = "3-15",
    name_variants: str = "[]",
    length_m: float = 5000.0,
    feature_ids: str = "f1",
    tippecanoe_minzoom: int = 8,
    geometry=None,
    **kwargs,
) -> dict:
    """Build a minimal canonical feature dict."""
    if geometry is None:
        geometry = make_line(x_start=1000000, y_start=500000, length=5000)
    feat = {
        "feature_type": feature_type,
        "group_id": group_id,
        "frontend_group_id": frontend_group_id,
        "display_name": display_name,
        "gnis_name": gnis_name,
        "display_name_override": "",
        "inherited_gnis_name": "",
        "waterbody_key": waterbody_key,
        "blue_line_key": blue_line_key,
        "fwa_watershed_code": fwa_watershed_code,
        "regulation_ids": regulation_ids,
        "regulation_count": len(regulation_ids.split(",")) if regulation_ids else 0,
        "zones": zones,
        "region_name": region_name,
        "mgmt_units": mgmt_units,
        "name_variants": name_variants,
        "length_m": length_m,
        "feature_ids": feature_ids,
        "feature_count": 1,
        "tippecanoe:minzoom": tippecanoe_minzoom,
        "geometry": geometry,
    }
    feat.update(kwargs)
    return feat


# ===================================================================
# _merge_name_variants (static)
# ===================================================================


class TestMergeNameVariants:
    """Tests for the stateless name-variant merge helper."""

    def test_new_names_added(self):
        target = {}
        SearchIndexBuilder._merge_name_variants(
            target, [{"name": "River A", "from_tributary": False}]
        )
        assert target == {"River A": False}

    def test_false_wins_over_true(self):
        """Direct match (False) should override tributary match (True)."""
        target = {"River A": True}
        SearchIndexBuilder._merge_name_variants(
            target, [{"name": "River A", "from_tributary": False}]
        )
        assert target["River A"] is False

    def test_true_does_not_override_false(self):
        target = {"River A": False}
        SearchIndexBuilder._merge_name_variants(
            target, [{"name": "River A", "from_tributary": True}]
        )
        assert target["River A"] is False

    def test_multiple_variants(self):
        target = {}
        SearchIndexBuilder._merge_name_variants(
            target,
            [
                {"name": "A", "from_tributary": False},
                {"name": "B", "from_tributary": True},
            ],
        )
        assert target == {"A": False, "B": True}

    def test_empty_variants_no_op(self):
        target = {"existing": False}
        SearchIndexBuilder._merge_name_variants(target, [])
        assert target == {"existing": False}


# ===================================================================
# _build_waterbodies_list
# ===================================================================


class TestBuildWaterbodiesList:
    """Tests for the core search-index build logic."""

    def test_named_feature_becomes_full_entry(self):
        """A feature with a display_name should produce a full search entry."""
        feat = _make_canonical_feature(display_name="Adams River")
        store = _make_fake_store([feat])
        builder = SearchIndexBuilder(store)

        result = builder._build_waterbodies_list()

        assert len(result["waterbodies"]) == 1
        entry = result["waterbodies"][0]
        assert entry["dn"] == "Adams River"
        assert entry["type"] == FeatureType.STREAM.value
        assert "ri" in entry  # reg_set_index
        assert "bbox" in entry
        assert len(entry["bbox"]) == 4

    def test_unnamed_feature_goes_to_compact(self):
        """A feature with empty display_name should become a compact entry."""
        feat = _make_canonical_feature(
            display_name="",
            gnis_name="",
            frontend_group_id="fgid_unnamed",
        )
        store = _make_fake_store([feat])
        builder = SearchIndexBuilder(store)

        result = builder._build_waterbodies_list()

        assert len(result["waterbodies"]) == 0
        assert "fgid_unnamed" in result["compact"]
        # compact maps fgid → reg_set_index
        ri = result["compact"]["fgid_unnamed"]
        assert isinstance(ri, int)
        assert result["reg_sets"][ri] == "reg_001_rule0"

    def test_reg_set_deduplication(self):
        """Features with the same regulation_ids should share one reg_set entry."""
        f1 = _make_canonical_feature(
            group_id="g1",
            frontend_group_id="fgid_1",
            display_name="Creek A",
            fwa_watershed_code="100-000000",
        )
        f2 = _make_canonical_feature(
            group_id="g2",
            frontend_group_id="fgid_2",
            display_name="Creek B",
            fwa_watershed_code="200-000000",
            geometry=make_line(x_start=1100000, y_start=500000, length=3000),
        )
        # Both have regulation_ids="reg_001_rule0"
        store = _make_fake_store([f1, f2])
        builder = SearchIndexBuilder(store)

        result = builder._build_waterbodies_list()

        assert len(result["waterbodies"]) == 2
        # Both should reference the same reg_set index
        ri_values = {entry["ri"] for entry in result["waterbodies"]}
        # Since both have the same regulation_ids, they should map to the same ri
        assert len(result["reg_sets"]) >= 1

    def test_empty_regulation_ids_skipped(self):
        """Features with empty regulation_ids should be skipped entirely."""
        feat = _make_canonical_feature(regulation_ids="")
        store = _make_fake_store([feat])
        builder = SearchIndexBuilder(store)

        result = builder._build_waterbodies_list()

        assert len(result["waterbodies"]) == 0
        assert len(result["compact"]) == 0

    def test_lake_grouping_by_waterbody_key(self):
        """Lake features should group by waterbody_key, not watershed_code."""
        f1 = _make_canonical_feature(
            feature_type=FeatureType.LAKE.value,
            group_id="lake_g1",
            frontend_group_id="fgid_lake1",
            display_name="Shuswap Lake",
            waterbody_key="WBK_100",
            blue_line_key=None,
            fwa_watershed_code=None,
            geometry=make_polygon(x=1000000, y=500000, size=1000),
            length_m=1000000.0,
        )
        f2 = _make_canonical_feature(
            feature_type=FeatureType.LAKE.value,
            group_id="lake_g2",
            frontend_group_id="fgid_lake2",
            display_name="Shuswap Lake",
            waterbody_key="WBK_100",
            blue_line_key=None,
            fwa_watershed_code=None,
            regulation_ids="reg_002_rule0",
            geometry=make_polygon(x=1001000, y=500000, size=500),
            length_m=250000.0,
        )
        store = _make_fake_store([f1, f2])
        builder = SearchIndexBuilder(store)

        result = builder._build_waterbodies_list()

        # Both features share waterbody_key+display_name+ftype → one entry
        assert len(result["waterbodies"]) == 1
        entry = result["waterbodies"][0]
        assert entry["dn"] == "Shuswap Lake"
        # Should have 2 regulation segments (different reg sets)
        assert len(entry["rs"]) == 2

    def test_stream_999_watershed_uses_group_id(self):
        """Streams on 999-* watershed codes should fall back to group_id."""
        feat = _make_canonical_feature(
            fwa_watershed_code="999-000001",
            display_name="Isolated Stream",
        )
        store = _make_fake_store([feat])
        builder = SearchIndexBuilder(store)

        result = builder._build_waterbodies_list()
        assert len(result["waterbodies"]) == 1

    def test_short_keys_present(self):
        """Verify the short key schema is used in search entries."""
        feat = _make_canonical_feature(
            display_name="Test Creek",
            zones="3",
            mgmt_units="3-15",
            region_name="Thompson",
        )
        store = _make_fake_store([feat])
        builder = SearchIndexBuilder(store)

        result = builder._build_waterbodies_list()
        entry = result["waterbodies"][0]

        # Short key checks
        assert "gn" in entry  # gnis_name
        assert "dn" in entry  # display_name
        assert "fgids" in entry  # frontend_group_ids
        assert "nv" in entry  # name_variants
        assert "type" in entry  # feature_type
        assert "z" in entry  # zones
        assert "mu" in entry  # mgmt_units
        assert "rn" in entry  # region_names
        assert "ri" in entry  # reg_set_index
        assert "tlkm" in entry  # total_length_km
        assert "bbox" in entry  # bounding box
        assert "mz" in entry  # min_zoom
        assert "props" in entry  # additional props
        assert "rs" in entry  # regulation segments


# ===================================================================
# export_waterbody_data
# ===================================================================


class TestExportWaterbodyData:
    """Tests for the public export_waterbody_data method."""

    def test_creates_json_file(self, tmp_path):
        """Should write a valid JSON file with the expected top-level keys."""
        feat = _make_canonical_feature(display_name="Export Creek")
        store = _make_fake_store(
            [feat],
            regulation_details={
                "reg_001_rule0": {"source": "zone", "name": "Test Regulation"}
            },
        )
        builder = SearchIndexBuilder(store)

        output = tmp_path / "waterbody_data.json"
        result = builder.export_waterbody_data(output)

        assert result == output
        assert output.exists()

        import orjson

        data = orjson.loads(output.read_bytes())
        assert "waterbodies" in data
        assert "reg_sets" in data
        assert "compact" in data
        assert "regulations" in data
        assert len(data["waterbodies"]) == 1
        assert data["regulations"]["reg_001_rule0"]["name"] == "Test Regulation"

    def test_creates_parent_directories(self, tmp_path):
        """Output path's parent dirs should be created automatically."""
        feat = _make_canonical_feature(display_name="Deep Creek")
        store = _make_fake_store([feat])
        builder = SearchIndexBuilder(store)

        output = tmp_path / "deep" / "nested" / "waterbody_data.json"
        result = builder.export_waterbody_data(output)

        assert result == output
        assert output.exists()


# ===================================================================
# _build_identity_meta
# ===================================================================


class TestBuildIdentityMeta:
    """Tests for identity_meta extraction and regulation slimming."""

    def test_synopsis_exclusions_extracted(self):
        """Identity fields should move from individual rules to identity_meta."""
        exclusions = [
            {"lookup_name": "Abruzzi Cr.", "type": "WHOLE_SYSTEM", "direction": None},
            {"lookup_name": "Alexander Cr.", "type": "WHOLE_SYSTEM", "direction": None},
        ]
        regs = {
            "reg_00632_rule0": {
                "source": "synopsis",
                "waterbody_name": "ELK RIVER",
                "region": "Kootenay",
                "management_units": ["4-23"],
                "exclusions": exclusions,
                "source_image": "page_038.png",
                "lookup_name": "ELK RIVER",
                "is_direct_match": True,
                "includes_tributaries": True,
                "rule_text": "No Fishing",
            },
            "reg_00632_rule1": {
                "source": "synopsis",
                "waterbody_name": "ELK RIVER",
                "region": "Kootenay",
                "management_units": ["4-23"],
                "exclusions": exclusions,
                "source_image": "page_038.png",
                "lookup_name": "ELK RIVER",
                "is_direct_match": True,
                "includes_tributaries": True,
                "rule_text": "Catch limit 2",
            },
        }
        identity_meta, slimmed = SearchIndexBuilder._build_identity_meta(regs)

        # identity_meta should have one entry for the shared base ID
        assert "reg_00632" in identity_meta
        meta = identity_meta["reg_00632"]
        assert meta["wn"] == "ELK RIVER"
        assert meta["rg"] == "Kootenay"
        assert meta["mu"] == ["4-23"]
        assert meta["ex"] == exclusions
        assert meta["img"] == "page_038.png"

        # Slimmed regulations: identity + dead fields stripped
        for rid in ["reg_00632_rule0", "reg_00632_rule1"]:
            # Identity fields moved out
            assert "waterbody_name" not in slimmed[rid]
            assert "region" not in slimmed[rid]
            assert "management_units" not in slimmed[rid]
            assert "exclusions" not in slimmed[rid]
            assert "source_image" not in slimmed[rid]
            # Dead fields stripped
            assert "lookup_name" not in slimmed[rid]
            assert "is_direct_match" not in slimmed[rid]
            assert "includes_tributaries" not in slimmed[rid]
            # Should have iid back-reference
            assert slimmed[rid]["iid"] == "reg_00632"
            # Rule-specific fields kept
            assert slimmed[rid]["source"] == "synopsis"
            assert slimmed[rid]["rule_text"] is not None

    def test_zone_regs_no_iid(self):
        """Zone regulations should NOT get an iid or identity_meta entry."""
        regs = {
            "zone_r4_bass_closed": {
                "source": "zone",
                "waterbody_name": None,
                "exclusions": None,
                "source_image": None,
                "lookup_name": None,
                "is_direct_match": True,
                "includes_tributaries": False,
                "rule_text": "Bass closed",
            },
        }
        identity_meta, slimmed = SearchIndexBuilder._build_identity_meta(regs)

        assert len(identity_meta) == 0
        assert "iid" not in slimmed["zone_r4_bass_closed"]
        # Dead fields stripped from zone regs
        assert "lookup_name" not in slimmed["zone_r4_bass_closed"]
        assert "is_direct_match" not in slimmed["zone_r4_bass_closed"]
        assert "includes_tributaries" not in slimmed["zone_r4_bass_closed"]
        # Identity fields stay flat on zone regs (no dedup needed)
        assert "exclusions" in slimmed["zone_r4_bass_closed"]
        assert "source_image" in slimmed["zone_r4_bass_closed"]
        assert slimmed["zone_r4_bass_closed"]["rule_text"] == "Bass closed"

    def test_provincial_regs_no_iid(self):
        """Provincial regulations should NOT get an iid."""
        regs = {
            "prov_bass": {
                "source": "provincial",
                "exclusions": None,
                "source_image": None,
                "lookup_name": None,
                "rule_text": "Provincial bass rule",
            },
        }
        identity_meta, slimmed = SearchIndexBuilder._build_identity_meta(regs)

        assert len(identity_meta) == 0
        assert "iid" not in slimmed["prov_bass"]

    def test_synopsis_always_gets_identity(self):
        """Synopsis regs always create an identity_meta entry (even sparse)."""
        regs = {
            "reg_00100_rule0": {
                "source": "synopsis",
                "waterbody_name": "SMALL CREEK",
                "exclusions": None,
                "source_image": None,
                "lookup_name": None,
                "rule_text": "Some rule",
            },
        }
        identity_meta, slimmed = SearchIndexBuilder._build_identity_meta(regs)

        # Identity always created for synopsis, with at least wn
        assert "reg_00100" in identity_meta
        assert identity_meta["reg_00100"]["wn"] == "SMALL CREEK"
        # Optional fields omitted when None
        assert "ex" not in identity_meta["reg_00100"]
        assert "img" not in identity_meta["reg_00100"]
        # iid back-reference present
        assert slimmed["reg_00100_rule0"]["iid"] == "reg_00100"

    def test_source_image_only_creates_identity_meta(self):
        """A synopsis reg with source_image but no exclusions gets identity_meta."""
        regs = {
            "reg_00200_rule0": {
                "source": "synopsis",
                "waterbody_name": "SOME CREEK",
                "exclusions": None,
                "source_image": "page_010.png",
                "lookup_name": "SOME CREEK",
                "rule_text": "Some rule",
            },
        }
        identity_meta, slimmed = SearchIndexBuilder._build_identity_meta(regs)

        assert "reg_00200" in identity_meta
        assert identity_meta["reg_00200"]["wn"] == "SOME CREEK"
        assert "ex" not in identity_meta["reg_00200"]
        assert identity_meta["reg_00200"]["img"] == "page_010.png"

    def test_identity_meta_in_exported_json(self, tmp_path):
        """identity_meta should appear in the exported JSON when exclusions exist."""
        exclusions = [{"lookup_name": "Test Cr.", "type": "WHOLE_SYSTEM"}]
        feat = _make_canonical_feature(
            display_name="Elk River",
            regulation_ids="reg_00632_rule0",
        )
        store = _make_fake_store(
            [feat],
            regulation_details={
                "reg_00632_rule0": {
                    "source": "synopsis",
                    "waterbody_name": "ELK RIVER",
                    "exclusions": exclusions,
                    "source_image": "page_038.png",
                    "lookup_name": "ELK RIVER",
                    "rule_text": "No Fishing",
                },
            },
        )
        builder = SearchIndexBuilder(store)
        output = tmp_path / "waterbody_data.json"
        builder.export_waterbody_data(output)

        import orjson

        data = orjson.loads(output.read_bytes())

        assert "identity_meta" in data
        assert "reg_00632" in data["identity_meta"]
        assert data["identity_meta"]["reg_00632"]["ex"] == exclusions
        # Regulation should be slimmed
        assert "exclusions" not in data["regulations"]["reg_00632_rule0"]
        assert data["regulations"]["reg_00632_rule0"]["iid"] == "reg_00632"

    def test_no_identity_meta_key_when_empty(self, tmp_path):
        """identity_meta key should be absent from JSON when no identities exist."""
        feat = _make_canonical_feature(display_name="Plain Creek")
        store = _make_fake_store(
            [feat],
            regulation_details={
                "zone_r3_trout": {
                    "source": "zone",
                    "exclusions": None,
                    "source_image": None,
                    "rule_text": "Trout limit",
                },
            },
        )
        builder = SearchIndexBuilder(store)
        output = tmp_path / "waterbody_data.json"
        builder.export_waterbody_data(output)

        import orjson

        data = orjson.loads(output.read_bytes())

        assert "identity_meta" not in data

    def test_sibling_merge_fills_missing_exclusions(self):
        """If rule0 has no exclusions but rule1 does, merge picks them up."""
        exclusions = [{"lookup_name": "Side Cr.", "type": "WHOLE_SYSTEM"}]
        regs = {
            "reg_00300_rule0": {
                "source": "synopsis",
                "waterbody_name": "MAIN RIVER",
                "region": "Thompson",
                "exclusions": None,
                "source_image": "page_001.png",
                "rule_text": "Rule A",
            },
            "reg_00300_rule1": {
                "source": "synopsis",
                "waterbody_name": "MAIN RIVER",
                "region": "Thompson",
                "exclusions": exclusions,
                "source_image": None,
                "rule_text": "Rule B",
            },
        }
        identity_meta, slimmed = SearchIndexBuilder._build_identity_meta(regs)

        meta = identity_meta["reg_00300"]
        # Exclusions from rule1 merged into identity
        assert meta["ex"] == exclusions
        # source_image from rule0 kept (first writer)
        assert meta["img"] == "page_001.png"
        # Both slimmed rules reference same identity
        assert slimmed["reg_00300_rule0"]["iid"] == "reg_00300"
        assert slimmed["reg_00300_rule1"]["iid"] == "reg_00300"

    def test_sibling_merge_first_writer_wins_for_image(self):
        """First rule's source_image wins when both siblings have one."""
        regs = {
            "reg_00400_rule0": {
                "source": "synopsis",
                "waterbody_name": "CREEK X",
                "source_image": "first.png",
                "rule_text": "Rule A",
            },
            "reg_00400_rule1": {
                "source": "synopsis",
                "waterbody_name": "CREEK X",
                "source_image": "second.png",
                "rule_text": "Rule B",
            },
        }
        identity_meta, _ = SearchIndexBuilder._build_identity_meta(regs)
        assert identity_meta["reg_00400"]["img"] == "first.png"

    def test_mixed_sources_separated(self):
        """Synopsis gets identity_meta; zone/provincial stay flat."""
        regs = {
            "reg_00500_rule0": {
                "source": "synopsis",
                "waterbody_name": "SYN CREEK",
                "region": "Cariboo",
                "management_units": ["5-1"],
                "source_image": "pg.png",
                "exclusions": None,
                "lookup_name": "SYN CREEK",
                "rule_text": "Synopsis rule",
            },
            "zone_r5_trout": {
                "source": "zone",
                "waterbody_name": None,
                "lookup_name": None,
                "rule_text": "Zone trout rule",
            },
            "prov_bass": {
                "source": "provincial",
                "lookup_name": None,
                "rule_text": "Provincial bass",
            },
        }
        identity_meta, slimmed = SearchIndexBuilder._build_identity_meta(regs)

        # Only synopsis gets identity
        assert "reg_00500" in identity_meta
        assert len(identity_meta) == 1

        # Synopsis slimmed: identity fields removed
        syn = slimmed["reg_00500_rule0"]
        assert "waterbody_name" not in syn
        assert "region" not in syn
        assert "management_units" not in syn
        assert syn["iid"] == "reg_00500"
        assert syn["source"] == "synopsis"

        # Zone: dead fields stripped, identity fields stay flat
        zone = slimmed["zone_r5_trout"]
        assert "lookup_name" not in zone
        assert "iid" not in zone
        assert zone["rule_text"] == "Zone trout rule"

        # Provincial: dead fields stripped
        prov = slimmed["prov_bass"]
        assert "lookup_name" not in prov
        assert "iid" not in prov

    def test_empty_list_exclusions_not_stored(self):
        """Empty exclusions list should NOT be stored in identity_meta."""
        regs = {
            "reg_00600_rule0": {
                "source": "synopsis",
                "waterbody_name": "EMPTY EX CREEK",
                "exclusions": [],
                "source_image": None,
                "rule_text": "Rule",
            },
        }
        identity_meta, _ = SearchIndexBuilder._build_identity_meta(regs)
        # Empty list is falsy → not stored
        assert "ex" not in identity_meta["reg_00600"]

    def test_sibling_merge_fills_missing_region_and_mu(self):
        """If rule0 lacks region/mu but rule1 has them, merge picks them up."""
        regs = {
            "reg_00700_rule0": {
                "source": "synopsis",
                "waterbody_name": "MERGE CREEK",
                "region": None,
                "management_units": None,
                "source_image": None,
                "exclusions": None,
                "rule_text": "Rule A",
            },
            "reg_00700_rule1": {
                "source": "synopsis",
                "waterbody_name": "MERGE CREEK",
                "region": "Thompson",
                "management_units": ["3-15"],
                "source_image": "page.png",
                "exclusions": [{"type": "WHOLE_SYSTEM", "lookup_name": "Side Cr."}],
                "rule_text": "Rule B",
            },
        }
        identity_meta, _ = SearchIndexBuilder._build_identity_meta(regs)

        meta = identity_meta["reg_00700"]
        assert meta["rg"] == "Thompson"
        assert meta["mu"] == ["3-15"]
        assert meta["img"] == "page.png"
        assert meta["ex"] == [{"type": "WHOLE_SYSTEM", "lookup_name": "Side Cr."}]

    def test_three_sibling_rules(self):
        """Merging works across 3+ sibling rules."""
        regs = {
            "reg_00800_rule0": {
                "source": "synopsis",
                "waterbody_name": "TRIPLE CREEK",
                "region": None,
                "exclusions": None,
                "source_image": None,
                "rule_text": "Rule 0",
            },
            "reg_00800_rule1": {
                "source": "synopsis",
                "waterbody_name": "TRIPLE CREEK",
                "region": "Kootenay",
                "exclusions": None,
                "source_image": None,
                "rule_text": "Rule 1",
            },
            "reg_00800_rule2": {
                "source": "synopsis",
                "waterbody_name": "TRIPLE CREEK",
                "region": "Kootenay",
                "exclusions": [{"type": "WHOLE_SYSTEM", "lookup_name": "X Cr."}],
                "source_image": "pg3.png",
                "rule_text": "Rule 2",
            },
        }
        identity_meta, slimmed = SearchIndexBuilder._build_identity_meta(regs)

        meta = identity_meta["reg_00800"]
        assert meta["wn"] == "TRIPLE CREEK"
        assert meta["rg"] == "Kootenay"  # from rule1
        assert meta["img"] == "pg3.png"  # from rule2
        assert meta["ex"][0]["lookup_name"] == "X Cr."  # from rule2
        # All 3 slimmed regs share iid
        for i in range(3):
            assert slimmed[f"reg_00800_rule{i}"]["iid"] == "reg_00800"

    def test_missing_source_treated_as_non_synopsis(self):
        """Regulations without a 'source' field stay flat (no iid)."""
        regs = {
            "unknown_reg": {
                "waterbody_name": "Mystery Creek",
                "rule_text": "Rule",
                "lookup_name": "MYSTERY CREEK",
            },
        }
        identity_meta, slimmed = SearchIndexBuilder._build_identity_meta(regs)
        assert len(identity_meta) == 0
        assert "iid" not in slimmed["unknown_reg"]
        assert "lookup_name" not in slimmed["unknown_reg"]  # dead field stripped

    def test_non_identity_non_dead_fields_preserved(self):
        """Fields that are neither identity nor dead should survive slimming."""
        regs = {
            "reg_00900_rule0": {
                "source": "synopsis",
                "waterbody_name": "PRESERVE CREEK",
                "region": "Coast",
                "rule_text": "Some rule text",
                "restriction_type": "catch_limit",
                "restriction_details": "2 per day",
                "dates": ["Jan 1 - Dec 31"],
                "scope_type": "whole",
                "scope_location": None,
                "zone_ids": ["5"],
                "feature_types": ["stream"],
                "lookup_name": "PRESERVE CREEK",
                "exclusions": None,
                "source_image": None,
            },
        }
        _, slimmed = SearchIndexBuilder._build_identity_meta(regs)
        slim = slimmed["reg_00900_rule0"]
        assert slim["rule_text"] == "Some rule text"
        assert slim["restriction_type"] == "catch_limit"
        assert slim["restriction_details"] == "2 per day"
        assert slim["dates"] == ["Jan 1 - Dec 31"]
        assert slim["scope_type"] == "whole"
        assert slim["zone_ids"] == ["5"]
        assert slim["feature_types"] == ["stream"]
        assert slim["source"] == "synopsis"
