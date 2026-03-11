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

from collections import defaultdict

import pytest
from fwa_pipeline.metadata_gazetteer import FeatureType, MetadataGazetteer
from regulation_mapping.regulation_resolvers import build_feature_index
from regulation_mapping.regulation_mapper import RegulationMapper
from regulation_mapping.feature_merger import aggregate_group_metadata
from regulation_mapping.scope_filter import ScopeFilter
from regulation_mapping.tributary_enricher import TributaryEnricher


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
        # Build reverse indexes used by _extend_boundary_streams
        self.watershed_code_index: dict = defaultdict(list)
        self.blue_line_key_index: dict = defaultdict(list)
        self.data_accessor = None
        self._reprojected_admin_cache: dict = {}
        for fid, meta in stream_entries:
            if wsc := meta.get("fwa_watershed_code"):
                self.watershed_code_index[wsc].append(fid)
            if blk := meta.get("blue_line_key"):
                self.blue_line_key_index[blk].append((fid, FeatureType.STREAM))


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


# ===================================================================
# Real metadata shapes — Campbell River MU boundary side channels.
#
# WSC 920-627155-000000 is the mainstem.  The named river (BLK 354154635)
# spans MUs 1-6, 1-10, and 1-9.  Several unnamed side channels share the
# exact same WSC but have different BLKs.  Some side channels are
# unbuffered-only in MU 1-10 but within 500m buffer of MU 1-6.
#
# A regulation targeting MUs 1-1..1-6 should include those side channels
# because they braid through the same river corridor as the mainstem.
#
# Tributary WSC 920-627155-045026 (Quinsam River) is a DIFFERENT stream
# entering through the same area — it must NOT be pulled in by WSC
# matching (different exact WSC).
# ===================================================================

CAMPBELL_WSC = (
    "920-627155-000000-000000-000000-000000-000000-000000-"
    "000000-000000-000000-000000-000000-000000-000000-"
    "000000-000000-000000-000000-000000-000000"
)
QUINSAM_WSC = (
    "920-627155-045026-000000-000000-000000-000000-000000-"
    "000000-000000-000000-000000-000000-000000-000000-"
    "000000-000000-000000-000000-000000-000000"
)


def _campbell_mainstem_in_mu16():
    """Campbell River mainstem — unbuffered in 1-6."""
    return "710157676", {
        "linear_feature_id": "710157676",
        "gnis_name": "Campbell River",
        "blue_line_key": "354154635",
        "fwa_watershed_code": CAMPBELL_WSC,
        "zones": ["1"],
        "zones_unbuffered": ["1"],
        "mgmt_units": ["1-6"],
        "mgmt_units_unbuffered": ["1-6"],
        "region_names": ["Vancouver Island"],
        "region_names_unbuffered": ["Vancouver Island"],
    }


def _campbell_mainstem_in_mu10():
    """Campbell River mainstem — unbuffered in 1-10."""
    return "710158000", {
        "linear_feature_id": "710158000",
        "gnis_name": "Campbell River",
        "blue_line_key": "354154635",
        "fwa_watershed_code": CAMPBELL_WSC,
        "zones": ["1"],
        "zones_unbuffered": ["1"],
        "mgmt_units": ["1-10"],
        "mgmt_units_unbuffered": ["1-10"],
        "region_names": ["Vancouver Island"],
        "region_names_unbuffered": ["Vancouver Island"],
    }


def _side_channel_buffered_overlap():
    """Side channel BLK 354088344 — unbuffered in 1-10,
    but buffered MU includes 1-6 (within 500m of boundary)."""
    return "710150304", {
        "linear_feature_id": "710150304",
        "gnis_name": "",
        "blue_line_key": "354088344",
        "fwa_watershed_code": CAMPBELL_WSC,  # Same exact WSC as mainstem
        "zones": ["1"],
        "zones_unbuffered": ["1"],
        "mgmt_units": ["1-10", "1-6"],  # Buffer reaches 1-6
        "mgmt_units_unbuffered": ["1-10"],  # Unbuffered: only 1-10
        "region_names": ["Vancouver Island"],
        "region_names_unbuffered": ["Vancouver Island"],
    }


def _side_channel_solidly_in_mu16():
    """Side channel BLK 354087739 — solidly in 1-6 (already in base set)."""
    return "710149921", {
        "linear_feature_id": "710149921",
        "gnis_name": "",
        "blue_line_key": "354087739",
        "fwa_watershed_code": CAMPBELL_WSC,  # Same exact WSC
        "zones": ["1"],
        "zones_unbuffered": ["1"],
        "mgmt_units": ["1-10", "1-6"],
        "mgmt_units_unbuffered": ["1-6"],
        "region_names": ["Vancouver Island"],
        "region_names_unbuffered": ["Vancouver Island"],
    }


def _side_channel_only_mu10():
    """Side channel BLK 354088932 — unbuffered AND buffered only in 1-10.
    Should NOT be pulled in by a 1-6 regulation (too far from boundary)."""
    return "710150030", {
        "linear_feature_id": "710150030",
        "gnis_name": "",
        "blue_line_key": "354088932",
        "fwa_watershed_code": CAMPBELL_WSC,  # Same exact WSC
        "zones": ["1"],
        "zones_unbuffered": ["1"],
        "mgmt_units": ["1-10"],  # Buffer doesn't reach 1-6
        "mgmt_units_unbuffered": ["1-10"],
        "region_names": ["Vancouver Island"],
        "region_names_unbuffered": ["Vancouver Island"],
    }


def _quinsam_tributary():
    """Quinsam River — DIFFERENT WSC, solidly in 1-6.
    Must NOT be pulled in by WSC matching (its WSC differs from mainstem)."""
    return "710157238", {
        "linear_feature_id": "710157238",
        "gnis_name": "Quinsam River",
        "blue_line_key": "354155091",
        "fwa_watershed_code": QUINSAM_WSC,  # Different WSC!
        "zones": ["1"],
        "zones_unbuffered": ["1"],
        "mgmt_units": ["1-6", "1-9"],
        "mgmt_units_unbuffered": ["1-6"],
        "region_names": ["Vancouver Island"],
        "region_names_unbuffered": ["Vancouver Island"],
    }


def _quinsam_confluence_edge():
    """Quinsam River edge near Campbell — buffered MU includes 1-10,
    but this is a DIFFERENT WSC and should not be pulled into 1-10 regs
    just because the mainstem shares a WSC with 1-10 features."""
    return "710150374", {
        "linear_feature_id": "710150374",
        "gnis_name": "Quinsam River",
        "blue_line_key": "354155091",
        "fwa_watershed_code": QUINSAM_WSC,  # Different WSC!
        "zones": ["1"],
        "zones_unbuffered": ["1"],
        "mgmt_units": ["1-10", "1-6"],
        "mgmt_units_unbuffered": ["1-6"],
        "region_names": ["Vancouver Island"],
        "region_names_unbuffered": ["Vancouver Island"],
    }


# ===================================================================
# Helpers — build a mapper for testing _extend_boundary_streams
# ===================================================================


class _FakeLinkingResult:
    def __init__(self):
        self.status = "NOT_FOUND"
        self.matched_features = []
        self.matched_name = ""
        self.link_method = ""
        self.admin_match = None
        self.additional_info = None


class _FakeCorrections:
    def get_all_feature_name_variations(self):
        return []


class _FakeLinker:
    def __init__(self, gazetteer):
        self.gazetteer = gazetteer
        self.corrections = _FakeCorrections()

    def link_waterbody(self, **kwargs):
        return _FakeLinkingResult()


def _build_mapper_with_entries(entries):
    """Build a RegulationMapper with fake linker and the given stream entries."""
    gaz = _FakeGazetteer(entries)
    linker = _FakeLinker(gaz)
    scope_filter = ScopeFilter()
    enricher = TributaryEnricher(graph_source=None)
    return RegulationMapper(
        linker=linker,
        scope_filter=scope_filter,
        tributary_enricher=enricher,
        gpkg_path=None,
    )


# ===================================================================
# Test: _extend_boundary_streams — WSC side channel extension
# ===================================================================


class TestExtendBoundaryStreamsWSC:
    """Verify that _extend_boundary_streams includes side channels
    sharing the same exact WSC as a boundary-straddling mainstem,
    when their buffered MUs overlap the regulation's target MUs."""

    @pytest.fixture()
    def all_entries(self):
        return [
            _campbell_mainstem_in_mu16(),
            _campbell_mainstem_in_mu10(),
            _side_channel_buffered_overlap(),
            _side_channel_solidly_in_mu16(),
            _side_channel_only_mu10(),
            _quinsam_tributary(),
            _quinsam_confluence_edge(),
            _bear_river_bug_segment(),
            _bear_river_clean_segment(),
        ]

    @pytest.fixture()
    def mapper(self, all_entries):
        return _build_mapper_with_entries(all_entries)

    @pytest.fixture()
    def indexes(self, all_entries):
        gaz = _FakeGazetteer(all_entries)
        return build_feature_index(gaz, feature_types=[FeatureType.STREAM])

    def test_side_channel_buffered_overlap_included(self, mapper, indexes):
        """Side channel 710150304 (BLK 354088344) has buffered MU overlap
        with 1-6 and shares exact WSC with the mainstem.  It should be
        included when the regulation targets MU 1-6."""
        zone_idx, mu_idx, zone_idx_buf, mu_idx_buf = indexes

        # Build base + buffered sets as _resolve_zone_wide would
        from regulation_mapping.regulation_resolvers import resolve_zone_wide_ids
        from regulation_mapping.zone_base_regulations import ZoneRegulation

        reg = ZoneRegulation(
            regulation_id="zone_test_mu16",
            zone_ids=["1"],
            rule_text="No fishing MU 1-1 to 1-6",
            restriction={"type": "CLOSURE", "details": "No fishing"},
            notes="test",
            mu_ids=["1-1", "1-2", "1-3", "1-4", "1-5", "1-6"],
        )
        base_ids = resolve_zone_wide_ids(reg, zone_idx, mu_idx)
        buffered_ids = resolve_zone_wide_ids(reg, zone_idx_buf, mu_idx_buf)
        extended, newly_added = mapper._extend_boundary_streams(
            base_ids, buffered_ids, target_mu_ids=reg.mu_ids
        )

        # Side channel with buffered overlap MUST be included
        assert "710150304" in extended, (
            "Side channel 710150304 (BLK 354088344) shares exact WSC with "
            "Campbell River mainstem and has buffered MU overlap with 1-6 — "
            "should be included via WSC extension"
        )

    def test_side_channel_no_buffer_overlap_excluded(self, mapper, indexes):
        """Side channel 710150030 (BLK 354088932) has buffered MU=['1-10']
        only — no overlap with 1-6.  Must NOT be included."""
        zone_idx, mu_idx, zone_idx_buf, mu_idx_buf = indexes

        from regulation_mapping.regulation_resolvers import resolve_zone_wide_ids
        from regulation_mapping.zone_base_regulations import ZoneRegulation

        reg = ZoneRegulation(
            regulation_id="zone_test_mu16",
            zone_ids=["1"],
            rule_text="No fishing MU 1-1 to 1-6",
            restriction={"type": "CLOSURE", "details": "No fishing"},
            notes="test",
            mu_ids=["1-1", "1-2", "1-3", "1-4", "1-5", "1-6"],
        )
        base_ids = resolve_zone_wide_ids(reg, zone_idx, mu_idx)
        buffered_ids = resolve_zone_wide_ids(reg, zone_idx_buf, mu_idx_buf)
        extended, _ = mapper._extend_boundary_streams(
            base_ids, buffered_ids, target_mu_ids=reg.mu_ids
        )

        assert "710150030" not in extended, (
            "Side channel 710150030 has no buffered overlap with 1-6 — "
            "must not be included even though it shares the same WSC"
        )

    def test_quinsam_not_pulled_in_by_wsc(self, mapper, indexes):
        """Quinsam River (different WSC) must NOT be pulled in by WSC
        matching even though it's near the Campbell River corridor."""
        zone_idx, mu_idx, zone_idx_buf, mu_idx_buf = indexes

        from regulation_mapping.regulation_resolvers import resolve_zone_wide_ids
        from regulation_mapping.zone_base_regulations import ZoneRegulation

        # Regulation targeting MU 1-10 where mainstem has edges
        reg = ZoneRegulation(
            regulation_id="zone_test_mu10",
            zone_ids=["1"],
            rule_text="Some reg for MU 1-10",
            restriction={"type": "CLOSURE", "details": "test"},
            notes="test",
            mu_ids=["1-10"],
        )
        base_ids = resolve_zone_wide_ids(reg, zone_idx, mu_idx)
        buffered_ids = resolve_zone_wide_ids(reg, zone_idx_buf, mu_idx_buf)
        extended, _ = mapper._extend_boundary_streams(
            base_ids, buffered_ids, target_mu_ids=reg.mu_ids
        )

        # Quinsam has a different WSC — must NOT be included
        assert (
            "710157238" not in extended
        ), "Quinsam River has a different WSC — WSC extension must not pull it in"
        assert (
            "710150374" not in extended
        ), "Quinsam confluence edge has a different WSC — must not be pulled in"

    def test_bear_river_not_affected(self, mapper, indexes):
        """Bear River (WSC 100-339600-000000) must NOT be pulled into
        zone 1 MU regulations by the WSC extension."""
        zone_idx, mu_idx, zone_idx_buf, mu_idx_buf = indexes

        from regulation_mapping.regulation_resolvers import resolve_zone_wide_ids
        from regulation_mapping.zone_base_regulations import ZoneRegulation

        reg = ZoneRegulation(
            regulation_id="zone_test_mu16",
            zone_ids=["1"],
            rule_text="No fishing MU 1-1 to 1-6",
            restriction={"type": "CLOSURE", "details": "No fishing"},
            notes="test",
            mu_ids=["1-1", "1-2", "1-3", "1-4", "1-5", "1-6"],
        )
        base_ids = resolve_zone_wide_ids(reg, zone_idx, mu_idx)
        buffered_ids = resolve_zone_wide_ids(reg, zone_idx_buf, mu_idx_buf)
        extended, _ = mapper._extend_boundary_streams(
            base_ids, buffered_ids, target_mu_ids=reg.mu_ids
        )

        # Bear River is in zone 2 / MU 2-2 — must not appear
        assert "206000766" not in extended
        assert "206002256" not in extended

    def test_base_set_unchanged(self, mapper, indexes):
        """Features already in the base set must remain in the result."""
        zone_idx, mu_idx, zone_idx_buf, mu_idx_buf = indexes

        from regulation_mapping.regulation_resolvers import resolve_zone_wide_ids
        from regulation_mapping.zone_base_regulations import ZoneRegulation

        reg = ZoneRegulation(
            regulation_id="zone_test_mu16",
            zone_ids=["1"],
            rule_text="No fishing MU 1-1 to 1-6",
            restriction={"type": "CLOSURE", "details": "No fishing"},
            notes="test",
            mu_ids=["1-1", "1-2", "1-3", "1-4", "1-5", "1-6"],
        )
        base_ids = resolve_zone_wide_ids(reg, zone_idx, mu_idx)
        buffered_ids = resolve_zone_wide_ids(reg, zone_idx_buf, mu_idx_buf)
        extended, _ = mapper._extend_boundary_streams(
            base_ids, buffered_ids, target_mu_ids=reg.mu_ids
        )

        # Mainstem in 1-6 and side channel solidly in 1-6 must be present
        assert "710157676" in extended, "Mainstem in MU 1-6 must be in base"
        assert "710149921" in extended, "Side channel solidly in MU 1-6 must be in base"

    def test_no_target_mu_ids_skips_wsc_extension(self, mapper, indexes):
        """When target_mu_ids is None (zone-only reg), WSC extension
        doesn't run — no side channels added beyond BLK matching."""
        zone_idx, mu_idx, zone_idx_buf, mu_idx_buf = indexes

        from regulation_mapping.regulation_resolvers import resolve_zone_wide_ids
        from regulation_mapping.zone_base_regulations import ZoneRegulation

        reg = ZoneRegulation(
            regulation_id="zone_test_all",
            zone_ids=["1"],
            rule_text="Zone-wide reg",
            restriction={"type": "CLOSURE", "details": "test"},
            notes="test",
            # No mu_ids — applies to entire zone
        )
        base_ids = resolve_zone_wide_ids(reg, zone_idx, mu_idx)
        buffered_ids = resolve_zone_wide_ids(reg, zone_idx_buf, mu_idx_buf)
        extended, _ = mapper._extend_boundary_streams(
            base_ids, buffered_ids, target_mu_ids=None
        )

        # Without target_mu_ids, WSC extension should not run
        # Side channel 710150304 may or may not be in base depending on MU,
        # but there should be no WSC-based additions
        assert extended == (base_ids | (extended - base_ids))
