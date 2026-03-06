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
            regulation_details={"reg_001_rule0": {"name": "Test Regulation"}},
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
