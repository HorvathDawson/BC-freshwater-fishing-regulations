"""
Tests for zone buffer hygiene — ensures the 500m buffer does not leak
neighbouring zones into features that are fully within a single zone.

Uses real metadata shapes from the BC fisheries data:
    - Bear River (blk 360886378): fully in zone 2, but a segment near the
      zone 1/2 boundary gets zones=['1','2'] from the 500m buffer while
      zones_unbuffered=['2'].  This is the bug case.
    - Similkameen River (blk 356570170): genuinely straddles zones 2/8.
      Both buffered and unbuffered agree: zones=['2','8'].
    - Campbell River (blk 356461464): cleanly in zone 1.  No mismatch.
"""

import pytest
from fwa_pipeline.metadata_gazetteer import FeatureType, MetadataGazetteer
from regulation_mapping.regulation_resolvers import build_feature_index
from regulation_mapping.feature_merger import aggregate_group_metadata


# ---------------------------------------------------------------------------
# Real metadata shapes — copied from the pickle for the specific segments.
# Only the fields relevant to zone indexing are included.
# ---------------------------------------------------------------------------


def _bear_river_bug_segment():
    """Bear River segment 206000766 — single-zone but buffer leaks zone 1."""
    return "206000766", {
        "linear_feature_id": "206000766",
        "gnis_name": "Bear River",
        "blue_line_key": "360886378",
        "fwa_watershed_code": "100-339600-000000",
        "zones": ["1", "2"],
        "zones_unbuffered": ["2"],
        "mgmt_units": ["1-1", "2-2"],
        "mgmt_units_unbuffered": ["2-2"],
        "region_names": ["Vancouver Island", "Lower Mainland"],
        "region_names_unbuffered": ["Lower Mainland"],
    }


def _bear_river_clean_segment():
    """Bear River segment 206002256 — fully in zone 2, no buffer mismatch."""
    return "206002256", {
        "linear_feature_id": "206002256",
        "gnis_name": "Bear River",
        "blue_line_key": "360886378",
        "fwa_watershed_code": "100-339600-000000",
        "zones": ["2"],
        "zones_unbuffered": ["2"],
        "mgmt_units": ["2-2"],
        "mgmt_units_unbuffered": ["2-2"],
        "region_names": ["Lower Mainland"],
        "region_names_unbuffered": ["Lower Mainland"],
    }


def _similkameen_straddle_segment():
    """Similkameen River segment 707527306 — genuinely straddles zones 2/8."""
    return "707527306", {
        "linear_feature_id": "707527306",
        "gnis_name": "Similkameen River",
        "blue_line_key": "356570170",
        "fwa_watershed_code": "100-561400-000000",
        "zones": ["2", "8"],
        "zones_unbuffered": ["2", "8"],
        "mgmt_units": ["2-2", "8-1"],
        "mgmt_units_unbuffered": ["2-2", "8-1"],
        "region_names": ["Lower Mainland", "Okanagan"],
        "region_names_unbuffered": ["Lower Mainland", "Okanagan"],
    }


def _campbell_river_clean_segment():
    """Campbell River segment 710157676 — cleanly in zone 1."""
    return "710157676", {
        "linear_feature_id": "710157676",
        "gnis_name": "Campbell River",
        "blue_line_key": "356461464",
        "fwa_watershed_code": "920-800000-000000",
        "zones": ["1"],
        "zones_unbuffered": ["1"],
        "mgmt_units": ["1-1"],
        "mgmt_units_unbuffered": ["1-1"],
        "region_names": ["Vancouver Island"],
        "region_names_unbuffered": ["Vancouver Island"],
    }


# ---------------------------------------------------------------------------
# Fake gazetteer that holds the test metadata entries
# ---------------------------------------------------------------------------


class _FakeGazetteer:
    """Minimal gazetteer holding STREAM metadata for index-building tests."""

    def __init__(self, stream_entries):
        self.metadata = {
            FeatureType.STREAM: {fid: meta for fid, meta in stream_entries},
        }


# ===================================================================
# Test: build_feature_index — buffered index hygiene
# ===================================================================


class TestBuildFeatureIndexBufferHygiene:
    """Verify that build_feature_index only adds multi-zone features
    to neighbouring zones in the buffered index."""

    @pytest.fixture()
    def indexes(self):
        entries = [
            _bear_river_bug_segment(),
            _bear_river_clean_segment(),
            _similkameen_straddle_segment(),
            _campbell_river_clean_segment(),
        ]
        gaz = _FakeGazetteer(entries)
        zone_idx, mu_idx, zone_idx_buf, mu_idx_buf = build_feature_index(
            gaz, feature_types=[FeatureType.STREAM]
        )
        return zone_idx, mu_idx, zone_idx_buf, mu_idx_buf

    # --- Bear River bug segment (single-zone, buffer bleeds) ---

    def test_bear_bug_unbuffered_index_only_zone2(self, indexes):
        """Bear River 206000766 has zones_ub=['2'] so it should ONLY appear
        in the unbuffered index under zone '2'."""
        zone_idx, _, _, _ = indexes
        # Present in zone 2
        assert "206000766" in zone_idx.get("2", {}).get(FeatureType.STREAM, {})
        # NOT in zone 1
        assert "206000766" not in zone_idx.get("1", {}).get(FeatureType.STREAM, {})

    def test_bear_bug_buffered_index_only_zone2(self, indexes):
        """Bug case: Bear River 206000766 has zones=['1','2'] but
        zones_ub=['2'].  The buffered index must NOT include it under
        zone 1 — it is a single-zone feature."""
        _, _, zone_idx_buf, _ = indexes
        # Present in zone 2 buffered
        assert "206000766" in zone_idx_buf.get("2", {}).get(FeatureType.STREAM, {})
        # Must NOT be in zone 1 buffered (the bug was that it was)
        zone1_streams = zone_idx_buf.get("1", {}).get(FeatureType.STREAM, {})
        assert "206000766" not in zone1_streams, (
            "Single-zone Bear River segment should not appear in zone 1 "
            "buffered index — the 500m buffer leaked a neighbouring zone"
        )

    def test_bear_bug_buffered_mu_only_zone2(self, indexes):
        """Bear River 206000766 MU should only be '2-2' in buffered index."""
        _, _, _, mu_idx_buf = indexes
        # Present under MU 2-2
        assert "206000766" in mu_idx_buf.get("2-2", {}).get(FeatureType.STREAM, {})
        # NOT under MU 1-1
        mu_1_1_streams = mu_idx_buf.get("1-1", {}).get(FeatureType.STREAM, {})
        assert "206000766" not in mu_1_1_streams

    # --- Bear River clean segment (no mismatch) ---

    def test_bear_clean_both_indexes_zone2_only(self, indexes):
        """Bear River 206002256 has zones=zones_ub=['2'], should appear
        in zone 2 only in both indexes."""
        zone_idx, _, zone_idx_buf, _ = indexes
        assert "206002256" in zone_idx["2"][FeatureType.STREAM]
        assert "206002256" in zone_idx_buf["2"][FeatureType.STREAM]
        assert "206002256" not in zone_idx.get("1", {}).get(FeatureType.STREAM, {})
        assert "206002256" not in zone_idx_buf.get("1", {}).get(FeatureType.STREAM, {})

    # --- Similkameen River (true straddle) ---

    def test_similkameen_unbuffered_both_zones(self, indexes):
        """Similkameen 707527306 has zones_ub=['2','8'] — appears in both
        unbuffered zone indexes."""
        zone_idx, _, _, _ = indexes
        assert "707527306" in zone_idx["2"][FeatureType.STREAM]
        assert "707527306" in zone_idx["8"][FeatureType.STREAM]

    def test_similkameen_buffered_both_zones(self, indexes):
        """Similkameen straddle segment should appear in both buffered
        zone indexes — it genuinely crosses the boundary."""
        _, _, zone_idx_buf, _ = indexes
        assert "707527306" in zone_idx_buf["2"][FeatureType.STREAM]
        assert "707527306" in zone_idx_buf["8"][FeatureType.STREAM]

    def test_similkameen_buffered_mu_both(self, indexes):
        """Similkameen straddle should appear in both MU buffered indexes."""
        _, _, _, mu_idx_buf = indexes
        assert "707527306" in mu_idx_buf["2-2"][FeatureType.STREAM]
        assert "707527306" in mu_idx_buf["8-1"][FeatureType.STREAM]

    # --- Campbell River (clean single zone) ---

    def test_campbell_zone1_only(self, indexes):
        """Campbell River is cleanly in zone 1 — appears in zone 1 only
        in both unbuffered and buffered indexes."""
        zone_idx, _, zone_idx_buf, _ = indexes
        assert "710157676" in zone_idx["1"][FeatureType.STREAM]
        assert "710157676" in zone_idx_buf["1"][FeatureType.STREAM]
        # Not in any other zone
        for z in ("2", "3", "5", "6", "7A", "8"):
            assert "710157676" not in zone_idx.get(z, {}).get(FeatureType.STREAM, {})
            assert "710157676" not in zone_idx_buf.get(z, {}).get(
                FeatureType.STREAM, {}
            )


# ===================================================================
# Test: FWAFeature construction — zone collapse for single-zone
# ===================================================================


class TestFWAFeatureZoneCollapse:
    """Verify that _build_feature collapses zones to unbuffered for
    single-zone features, preserving buffered zones for straddle features."""

    def test_bear_bug_segment_collapsed(self):
        """Bear River bug segment: zones=['1','2'], zones_ub=['2'].
        FWAFeature.zones should be collapsed to ['2']."""
        gaz = MetadataGazetteer.__new__(MetadataGazetteer)
        _, meta = _bear_river_bug_segment()
        feat = gaz._build_feature("206000766", meta, FeatureType.STREAM)
        assert feat.zones == [
            "2"
        ], f"Expected zones=['2'] (collapsed to unbuffered) but got {feat.zones}"
        assert feat.region_names == ["Lower Mainland"]
        assert feat.mgmt_units == ["2-2"]

    def test_bear_clean_segment_unchanged(self):
        """Bear River clean segment: already zones=['2'], zones_ub=['2'].
        Should remain ['2']."""
        gaz = MetadataGazetteer.__new__(MetadataGazetteer)
        _, meta = _bear_river_clean_segment()
        feat = gaz._build_feature("206002256", meta, FeatureType.STREAM)
        assert feat.zones == ["2"]
        assert feat.region_names == ["Lower Mainland"]

    def test_similkameen_straddle_keeps_both(self):
        """Similkameen straddle: zones=['2','8'], zones_ub=['2','8'].
        Multi-zone feature should keep buffered zones."""
        gaz = MetadataGazetteer.__new__(MetadataGazetteer)
        _, meta = _similkameen_straddle_segment()
        feat = gaz._build_feature("707527306", meta, FeatureType.STREAM)
        assert feat.zones == ["2", "8"]
        assert feat.region_names == ["Lower Mainland", "Okanagan"]
        assert feat.mgmt_units == ["2-2", "8-1"]

    def test_campbell_clean_unchanged(self):
        """Campbell River: zones=['1'], zones_ub=['1'].  No change needed."""
        gaz = MetadataGazetteer.__new__(MetadataGazetteer)
        _, meta = _campbell_river_clean_segment()
        feat = gaz._build_feature("710157676", meta, FeatureType.STREAM)
        assert feat.zones == ["1"]
        assert feat.region_names == ["Vancouver Island"]


# ===================================================================
# Test: aggregate_group_metadata — display zones
# ===================================================================


class TestAggregateGroupMetadataZones:
    """Verify that aggregate_group_metadata (used for display) produces
    the correct zone set when groups contain buffer-affected features."""

    def _make_feature(self, meta):
        gaz = MetadataGazetteer.__new__(MetadataGazetteer)
        return gaz._build_feature(meta["linear_feature_id"], meta, FeatureType.STREAM)

    def test_bear_river_group_shows_zone2_only(self):
        """A group of Bear River segments (including the bug segment near
        the boundary) should display zone 2 only — never zone 1."""
        _, bug_meta = _bear_river_bug_segment()
        _, clean_meta = _bear_river_clean_segment()
        features_data = [
            ("206000766", self._make_feature(bug_meta)),
            ("206002256", self._make_feature(clean_meta)),
        ]
        result = aggregate_group_metadata(features_data, linked_waterbody_keys=set())
        assert result["zones"] == (
            "2",
        ), f"Bear River group should display zone 2 only, got {result['zones']}"
        assert result["region_names"] == ("Lower Mainland",)

    def test_similkameen_group_shows_both_zones(self):
        """Similkameen straddle segment group should display both zones."""
        _, meta = _similkameen_straddle_segment()
        features_data = [("707527306", self._make_feature(meta))]
        result = aggregate_group_metadata(features_data, linked_waterbody_keys=set())
        assert result["zones"] == ("2", "8")
        assert result["region_names"] == ("Lower Mainland", "Okanagan")

    def test_campbell_group_shows_zone1_only(self):
        """Campbell River group should display zone 1 only."""
        _, meta = _campbell_river_clean_segment()
        features_data = [("710157676", self._make_feature(meta))]
        result = aggregate_group_metadata(features_data, linked_waterbody_keys=set())
        assert result["zones"] == ("1",)
        assert result["region_names"] == ("Vancouver Island",)
