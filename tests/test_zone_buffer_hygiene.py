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

    def test_bear_bug_in_zone1_buffered_index(self, indexes):
        """Bear River 206000766 has zones=['1','2'], zones_ub=['2'].
        The buffered index SHOULD include it under zone 1 — the 500m
        buffer legitimately touches zone 1.  Protection against false
        zone assignment happens at the extension level (WSC matching),
        not the index level."""
        _, _, zone_idx_buf, _ = indexes
        # Present in zone 2 buffered
        assert "206000766" in zone_idx_buf.get("2", {}).get(FeatureType.STREAM, {})
        # SHOULD also be in zone 1 buffered (index must reflect geometry)
        zone1_streams = zone_idx_buf.get("1", {}).get(FeatureType.STREAM, {})
        assert "206000766" in zone1_streams, (
            "Bear River buffer touches zone 1 — it should be in zone 1 "
            "buffered index. Protection from false zone assignment must "
            "happen at the extension level (WSC matching), not the index."
        )

    def test_bear_bug_in_mu11_buffered_index(self, indexes):
        """Bear River 206000766 has mgmt_units=['1-1','2-2'], ub=['2-2'].
        The buffered index SHOULD include it under MU 1-1 — the 500m
        buffer legitimately touches MU 1-1.  Protection is at the
        extension level."""
        _, _, _, mu_idx_buf = indexes
        # Present under MU 2-2
        assert "206000766" in mu_idx_buf.get("2-2", {}).get(FeatureType.STREAM, {})
        # SHOULD also be under MU 1-1 (index must reflect geometry)
        mu_1_1_streams = mu_idx_buf.get("1-1", {}).get(FeatureType.STREAM, {})
        assert "206000766" in mu_1_1_streams, (
            "Bear River buffer touches MU 1-1 — it should be in MU 1-1 "
            "buffered index. Protection is at the extension level."
        )

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
# Fixtures: Zone boundary straddle (zone 3/4 boundary)
#
# "Boundary Creek" — a fictional stream crossing the zone 3/4 boundary.
# The mainstem has BLK 999000001; a braided side channel has BLK 999000002.
# Both share the same WSC.
#
# BUG: segments near the boundary whose geometry is in zone 4 but whose
# 500m buffer reaches zone 3 should appear in zone 3's buffered index.
# The `len(zones_ub) > 1` guard in build_feature_index prevents this,
# so neighboring-zone resolution never sees them and boundary streams
# get split into unrelated sections.
# ===================================================================

BOUNDARY_CREEK_WSC = (
    "300-456789-000000-0000000-0000000-0000000-"
    "000000-000000-000000-000000-000000-000000"
)
BOUNDARY_CREEK_BLK_MAIN = "999000001"
BOUNDARY_CREEK_BLK_SIDE = "999000002"


def _boundary_creek_fully_zone3():
    """Mainstem segment fully in zone 3 — no buffer bleed."""
    return "900000001", {
        "linear_feature_id": "900000001",
        "gnis_name": "Boundary Creek",
        "blue_line_key": BOUNDARY_CREEK_BLK_MAIN,
        "fwa_watershed_code": BOUNDARY_CREEK_WSC,
        "zones": ["3"],
        "zones_unbuffered": ["3"],
        "mgmt_units": ["3-1"],
        "mgmt_units_unbuffered": ["3-1"],
        "region_names": ["Thompson-Nicola"],
        "region_names_unbuffered": ["Thompson-Nicola"],
    }


def _boundary_creek_near_boundary():
    """Mainstem near zone 3/4 boundary — geometry in zone 4, 500m buffer
    reaches zone 3.  zones_ub=["4"], zones=["3","4"]."""
    return "900000002", {
        "linear_feature_id": "900000002",
        "gnis_name": "Boundary Creek",
        "blue_line_key": BOUNDARY_CREEK_BLK_MAIN,
        "fwa_watershed_code": BOUNDARY_CREEK_WSC,
        "zones": ["3", "4"],
        "zones_unbuffered": ["4"],
        "mgmt_units": ["3-1", "4-1"],
        "mgmt_units_unbuffered": ["4-1"],
        "region_names": ["Thompson-Nicola", "Kootenay"],
        "region_names_unbuffered": ["Kootenay"],
    }


def _boundary_creek_fully_zone4():
    """Mainstem segment fully in zone 4 — no buffer bleed."""
    return "900000003", {
        "linear_feature_id": "900000003",
        "gnis_name": "Boundary Creek",
        "blue_line_key": BOUNDARY_CREEK_BLK_MAIN,
        "fwa_watershed_code": BOUNDARY_CREEK_WSC,
        "zones": ["4"],
        "zones_unbuffered": ["4"],
        "mgmt_units": ["4-1"],
        "mgmt_units_unbuffered": ["4-1"],
        "region_names": ["Kootenay"],
        "region_names_unbuffered": ["Kootenay"],
    }


def _boundary_creek_side_channel_near_boundary():
    """Side channel near zone 3/4 boundary — DIFFERENT BLK, same WSC.
    Geometry in zone 4, 500m buffer reaches zone 3."""
    return "900000004", {
        "linear_feature_id": "900000004",
        "gnis_name": "",
        "blue_line_key": BOUNDARY_CREEK_BLK_SIDE,
        "fwa_watershed_code": BOUNDARY_CREEK_WSC,
        "zones": ["3", "4"],
        "zones_unbuffered": ["4"],
        "mgmt_units": ["3-1", "4-1"],
        "mgmt_units_unbuffered": ["4-1"],
        "region_names": ["Thompson-Nicola", "Kootenay"],
        "region_names_unbuffered": ["Kootenay"],
    }


def _boundary_creek_side_channel_fully_zone4():
    """Side channel fully in zone 4 — no buffer bleed."""
    return "900000005", {
        "linear_feature_id": "900000005",
        "gnis_name": "",
        "blue_line_key": BOUNDARY_CREEK_BLK_SIDE,
        "fwa_watershed_code": BOUNDARY_CREEK_WSC,
        "zones": ["4"],
        "zones_unbuffered": ["4"],
        "mgmt_units": ["4-1"],
        "mgmt_units_unbuffered": ["4-1"],
        "region_names": ["Kootenay"],
        "region_names_unbuffered": ["Kootenay"],
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
            base_ids, buffered_ids,
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
            base_ids, buffered_ids,
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
            base_ids, buffered_ids,
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
            base_ids, buffered_ids,
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
            base_ids, buffered_ids,
        )

        # Mainstem in 1-6 and side channel solidly in 1-6 must be present
        assert "710157676" in extended, "Mainstem in MU 1-6 must be in base"
        assert "710149921" in extended, "Side channel solidly in MU 1-6 must be in base"

    def test_zone_only_reg_still_extends_via_wsc(self, mapper, indexes):
        """Zone-only regs (no mu_ids) should still extend via WSC matching.
        All features in the base set stay, and buffered features sharing a
        WSC with the base are added."""
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
            base_ids, buffered_ids,
        )

        # All base features must survive
        assert base_ids <= extended


# ===================================================================
# Test: Zone boundary straddle — buffered index level
#
# Demonstrates the core bug: the `len(zones_ub) > 1` guard in
# build_feature_index prevents straddling segments from appearing
# in the neighboring zone's buffered index.
# ===================================================================


class TestZoneBoundaryStraddleIndex:
    """Verify that segments whose 500m buffer touches a neighboring zone
    appear in that neighbor's buffered index.

    BUG: build_feature_index only places features in the full buffered
    index when ``len(zones_unbuffered) > 1`` (multi-zone features).
    Single-zone features whose buffer legitimately bleeds into a
    neighbor are mirrored from unbuffered, making them invisible to the
    neighboring zone's resolution."""

    @pytest.fixture()
    def indexes(self):
        entries = [
            _boundary_creek_fully_zone3(),
            _boundary_creek_near_boundary(),
            _boundary_creek_fully_zone4(),
            _boundary_creek_side_channel_near_boundary(),
            _boundary_creek_side_channel_fully_zone4(),
        ]
        gaz = _FakeGazetteer(entries)
        return build_feature_index(gaz, feature_types=[FeatureType.STREAM])

    def test_boundary_mainstem_in_neighbor_buffered_zone(self, indexes):
        """Segment 900000002: zones_ub=["4"], zones=["3","4"].
        Must appear in zone 3's buffered index."""
        _, _, zone_idx_buf, _ = indexes
        zone3_streams = zone_idx_buf.get("3", {}).get(FeatureType.STREAM, {})
        assert "900000002" in zone3_streams, (
            "Mainstem near boundary (zones_ub=['4'], zones=['3','4']) "
            "should appear in zone 3's buffered index — the 500m buffer "
            "legitimately reaches zone 3"
        )

    def test_boundary_mainstem_in_own_buffered_zone(self, indexes):
        """Segment 900000002 must also be in zone 4 buffered (its actual zone)."""
        _, _, zone_idx_buf, _ = indexes
        zone4_streams = zone_idx_buf.get("4", {}).get(FeatureType.STREAM, {})
        assert "900000002" in zone4_streams

    def test_boundary_side_channel_in_neighbor_buffered(self, indexes):
        """Side channel 900000004: zones_ub=["4"], zones=["3","4"].
        Must appear in zone 3's buffered index (different BLK, same WSC)."""
        _, _, zone_idx_buf, _ = indexes
        zone3_streams = zone_idx_buf.get("3", {}).get(FeatureType.STREAM, {})
        assert "900000004" in zone3_streams, (
            "Side channel near boundary (zones_ub=['4'], zones=['3','4']) "
            "should appear in zone 3's buffered index"
        )

    def test_boundary_mainstem_in_neighbor_buffered_mu(self, indexes):
        """Segment 900000002: mgmt_units=["3-1","4-1"], ub=["4-1"].
        Must appear in MU 3-1's buffered index."""
        _, _, _, mu_idx_buf = indexes
        mu31_streams = mu_idx_buf.get("3-1", {}).get(FeatureType.STREAM, {})
        assert "900000002" in mu31_streams, (
            "Mainstem near boundary should appear in MU 3-1's buffered index"
        )

    def test_side_channel_in_neighbor_buffered_mu(self, indexes):
        """Side channel 900000004: mgmt_units=["3-1","4-1"], ub=["4-1"].
        Must appear in MU 3-1's buffered index."""
        _, _, _, mu_idx_buf = indexes
        mu31_streams = mu_idx_buf.get("3-1", {}).get(FeatureType.STREAM, {})
        assert "900000004" in mu31_streams, (
            "Side channel near boundary should appear in MU 3-1's buffered index"
        )

    def test_fully_zone3_not_in_zone4(self, indexes):
        """900000001 (zones=["3"], ub=["3"]) must NOT leak into zone 4."""
        _, _, zone_idx_buf, _ = indexes
        zone4_streams = zone_idx_buf.get("4", {}).get(FeatureType.STREAM, {})
        assert "900000001" not in zone4_streams

    def test_fully_zone4_not_in_zone3(self, indexes):
        """900000003 (zones=["4"], ub=["4"]) must NOT leak into zone 3."""
        _, _, zone_idx_buf, _ = indexes
        zone3_streams = zone_idx_buf.get("3", {}).get(FeatureType.STREAM, {})
        assert "900000003" not in zone3_streams


# ===================================================================
# Test: Zone boundary straddle — end-to-end resolution
#
# Tests the full pipeline: build_feature_index → resolve_zone_wide_ids
# → _extend_boundary_streams.  Demonstrates that boundary segments
# are missing from zone resolution results.
# ===================================================================


class TestZoneBoundaryStraddleResolution:
    """End-to-end: zone regulations on both sides of a boundary should
    pick up boundary-straddling segments via WSC-based extension."""

    @pytest.fixture()
    def all_entries(self):
        return [
            _boundary_creek_fully_zone3(),
            _boundary_creek_near_boundary(),
            _boundary_creek_fully_zone4(),
            _boundary_creek_side_channel_near_boundary(),
            _boundary_creek_side_channel_fully_zone4(),
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

    def _resolve(self, reg, mapper, indexes):
        """Helper: resolve a zone reg through the full pipeline."""
        from regulation_mapping.regulation_resolvers import resolve_zone_wide_ids

        zone_idx, mu_idx, zone_idx_buf, mu_idx_buf = indexes
        base_ids = resolve_zone_wide_ids(reg, zone_idx, mu_idx)
        buffered_ids = resolve_zone_wide_ids(reg, zone_idx_buf, mu_idx_buf)
        extended, _ = mapper._extend_boundary_streams(
            base_ids, buffered_ids,
        )
        return base_ids, buffered_ids, extended

    def test_zone3_includes_boundary_mainstem(self, mapper, indexes):
        """Zone 3 reg must include boundary mainstem 900000002 whose
        500m buffer reaches zone 3."""
        from regulation_mapping.zone_base_regulations import ZoneRegulation

        reg = ZoneRegulation(
            regulation_id="zone3_test",
            zone_ids=["3"],
            rule_text="Zone 3 regulation",
            restriction={"type": "CLOSURE", "details": "test"},
            notes="test",
        )
        base_ids, _, extended = self._resolve(reg, mapper, indexes)

        assert "900000001" in base_ids, "Fully zone 3 segment must be in base"
        assert "900000002" in extended, (
            "Boundary mainstem (buffer reaches zone 3) must be included "
            "in zone 3 resolution via boundary extension"
        )

    def test_zone3_includes_side_channel_via_wsc(self, mapper, indexes):
        """Zone 3 must include side channel 900000004 (different BLK,
        same WSC) via WSC extension — handles braided rivers."""
        from regulation_mapping.zone_base_regulations import ZoneRegulation

        reg = ZoneRegulation(
            regulation_id="zone3_test",
            zone_ids=["3"],
            rule_text="Zone 3 regulation",
            restriction={"type": "CLOSURE", "details": "test"},
            notes="test",
        )
        _, _, extended = self._resolve(reg, mapper, indexes)

        assert "900000004" in extended, (
            "Side channel (different BLK, same WSC, buffer reaches zone 3) "
            "must be included via WSC extension for braided river support"
        )

    def test_zone4_includes_all_local_segments(self, mapper, indexes):
        """Zone 4 must include all its segments + boundary segments."""
        from regulation_mapping.zone_base_regulations import ZoneRegulation

        reg = ZoneRegulation(
            regulation_id="zone4_test",
            zone_ids=["4"],
            rule_text="Zone 4 regulation",
            restriction={"type": "CLOSURE", "details": "test"},
            notes="test",
        )
        _, _, extended = self._resolve(reg, mapper, indexes)

        assert "900000002" in extended, "Boundary mainstem must be in zone 4"
        assert "900000003" in extended, "Fully zone 4 mainstem must be in zone 4"
        assert "900000004" in extended, "Boundary side channel must be in zone 4"
        assert "900000005" in extended, "Fully zone 4 side channel must be in zone 4"

    def test_zone3_excludes_fully_zone4_segments(self, mapper, indexes):
        """Zone 3 must NOT pull in segments fully inside zone 4."""
        from regulation_mapping.zone_base_regulations import ZoneRegulation

        reg = ZoneRegulation(
            regulation_id="zone3_test",
            zone_ids=["3"],
            rule_text="Zone 3 regulation",
            restriction={"type": "CLOSURE", "details": "test"},
            notes="test",
        )
        _, _, extended = self._resolve(reg, mapper, indexes)

        assert "900000003" not in extended, (
            "Fully zone 4 mainstem must NOT be in zone 3 result"
        )
        assert "900000005" not in extended, (
            "Fully zone 4 side channel must NOT be in zone 3 result"
        )

    def test_bear_river_not_in_zone1(self, mapper, indexes):
        """Bear River must NOT appear in zone 1 results — protected by
        WSC extension logic (no Bear River segments in zone 1 base set,
        so its WSC never enters the extension seed)."""
        from regulation_mapping.zone_base_regulations import ZoneRegulation

        reg = ZoneRegulation(
            regulation_id="zone1_test",
            zone_ids=["1"],
            rule_text="Zone 1 regulation",
            restriction={"type": "CLOSURE", "details": "test"},
            notes="test",
        )
        _, _, extended = self._resolve(reg, mapper, indexes)

        assert "206000766" not in extended, (
            "Bear River bug segment must not appear in zone 1 — "
            "no Bear River segments are unbuffered in zone 1"
        )
        assert "206002256" not in extended


# ===================================================================
# Test: WSC as primary extension key (isolated from index bug)
#
# Manually crafts base/buffered sets to bypass the index-level bug
# and directly test whether WSC extension handles zone boundaries.
# Demonstrates that the current BLK-only extension is insufficient
# for braided rivers (different BLK, same WSC).
# ===================================================================


class TestWSCZoneBoundaryExtension:
    """Test WSC-based extension for zone boundaries, isolated from the
    build_feature_index bug.

    The shared ``extend_boundary_hysteresis`` function uses WSC as the
    sole extension key.  Since all segments sharing a BLK also share a
    WSC, WSC matching is a strict superset of BLK matching and handles
    braided side channels (different BLK, same WSC) automatically."""

    @pytest.fixture()
    def all_entries(self):
        return [
            _boundary_creek_fully_zone3(),
            _boundary_creek_near_boundary(),
            _boundary_creek_fully_zone4(),
            _boundary_creek_side_channel_near_boundary(),
            _boundary_creek_side_channel_fully_zone4(),
        ]

    @pytest.fixture()
    def mapper(self, all_entries):
        return _build_mapper_with_entries(all_entries)

    def test_wsc_extension_picks_up_side_channel_zone_only(self, mapper):
        """With manually crafted base/buffered sets (as they WOULD be
        after the index fix), WSC extension must pick up side channel
        900000004 even for zone-only regs."""

        # Simulate correct base/buffered for a zone 3 regulation
        base_ids = {"900000001"}  # fully in zone 3
        buffered_ids = {
            "900000001",  # fully zone 3
            "900000002",  # mainstem near boundary, same BLK
            "900000004",  # side channel near boundary, DIFFERENT BLK
        }

        extended, _ = mapper._extend_boundary_streams(
            base_ids, buffered_ids,
        )

        # WSC extension picks up 900000002 (same WSC as base)
        assert "900000002" in extended, (
            "Mainstem boundary segment (same WSC) should be picked up"
        )
        # WSC extension MUST also pick up 900000004 (different BLK, same WSC)
        assert "900000004" in extended, (
            "Side channel (different BLK, same WSC as mainstem) must be "
            "included via WSC extension — WSC matching captures both "
            "mainstem continuation and braided side channels"
        )

    def test_wsc_extension_excludes_unrelated_wsc(self, mapper):
        """Features with a different WSC must NOT be pulled in by WSC
        extension, even if they have buffered zone overlap."""

        base_ids = {"900000001"}
        buffered_ids = {"900000001", "900000002"}

        extended, _ = mapper._extend_boundary_streams(
            base_ids, buffered_ids,
        )

        # Only same-WSC features are added
        assert "900000003" not in extended, (
            "Fully zone 4 segment must not be pulled in (not in buffered set)"
        )
        assert "900000005" not in extended


# ===================================================================
# Test: extend_boundary_hysteresis directly — admin polygon straddle
#
# Verifies the shared helper function works correctly for the admin
# polygon path (eco reserves, national parks).  Uses manually
# crafted base/buffered sets simulating admin polygon intersection.
# ===================================================================


class TestAdminBoundaryHysteresis:
    """Test extend_boundary_hysteresis for admin polygon scenarios.

    An eco reserve boundary crosses a river — the exact polygon picks
    up some segments, the buffered polygon picks up more.  The shared
    helper should include buffered segments with matching WSC and
    exclude those with different WSCs."""

    @pytest.fixture()
    def stream_meta(self):
        """Build stream metadata simulating an eco reserve boundary
        crossing Boundary Creek."""
        entries = [
            _boundary_creek_fully_zone3(),
            _boundary_creek_near_boundary(),
            _boundary_creek_fully_zone4(),
            _boundary_creek_side_channel_near_boundary(),
            _boundary_creek_side_channel_fully_zone4(),
        ]
        return {fid: meta for fid, meta in entries}

    def test_admin_hysteresis_includes_same_wsc(self, stream_meta):
        """Buffered features with matching WSC are included."""
        from regulation_mapping.regulation_resolvers import extend_boundary_hysteresis

        # Eco reserve exact boundary catches fully_zone3 only
        base_fids = {"900000001"}
        # Buffered boundary catches the boundary mainstem + side channel
        buffered_fids = {"900000001", "900000002", "900000004"}

        extended, added = extend_boundary_hysteresis(
            base_fids, buffered_fids, stream_meta,
        )

        assert "900000001" in extended, "Base feature must survive"
        assert "900000002" in extended, (
            "Boundary mainstem (same WSC) should be added"
        )
        assert "900000004" in extended, (
            "Side channel (different BLK, same WSC) should be added"
        )
        assert added == 2

    def test_admin_hysteresis_excludes_different_wsc(self, stream_meta):
        """Buffered features with a different WSC are excluded."""
        from regulation_mapping.regulation_resolvers import extend_boundary_hysteresis

        # Add an unrelated stream to the metadata
        stream_meta["UNRELATED_001"] = {
            "linear_feature_id": "UNRELATED_001",
            "gnis_name": "Other Creek",
            "blue_line_key": "888000001",
            "fwa_watershed_code": "999-000000-000000",
        }

        base_fids = {"900000001"}
        buffered_fids = {"900000001", "900000002", "UNRELATED_001"}

        extended, added = extend_boundary_hysteresis(
            base_fids, buffered_fids, stream_meta,
        )

        assert "900000002" in extended, "Same WSC should be included"
        assert "UNRELATED_001" not in extended, (
            "Different WSC must NOT be included"
        )
        assert added == 1

    def test_admin_hysteresis_non_stream_passthrough(self, stream_meta):
        """Features NOT in stream_meta (lakes, wetlands) pass through
        from base_fids unchanged, and are not added from buffered."""
        from regulation_mapping.regulation_resolvers import extend_boundary_hysteresis

        base_fids = {"900000001", "LAKE_001"}
        buffered_fids = {"900000001", "900000002", "LAKE_001", "LAKE_002"}

        extended, added = extend_boundary_hysteresis(
            base_fids, buffered_fids, stream_meta,
        )

        assert "LAKE_001" in extended, "Base lake must survive"
        assert "LAKE_002" not in extended, (
            "Buffered-only lake must NOT be added (no WSC match possible)"
        )
        assert "900000002" in extended

    def test_admin_empty_base_returns_empty(self, stream_meta):
        """No base features → no WSC seeds → nothing from buffer added."""
        from regulation_mapping.regulation_resolvers import extend_boundary_hysteresis

        extended, added = extend_boundary_hysteresis(
            set(), {"900000001", "900000002"}, stream_meta,
        )

        assert len(extended) == 0
        assert added == 0


# ===================================================================
# Test: Segments that leave a straddle zone adopt only local regs
#
# A regulation for zone 3 should NOT include Boundary Creek segments
# that are fully inside zone 4 (no buffer bleed to zone 3).  Only
# the segments whose 500m buffer touches zone 3 get extended.
# This ensures that once a stream leaves the straddle region, only
# the zone it's actually in applies.
# ===================================================================


class TestSegmentsLeaveStraddleZone:
    """Verify that once a stream passes through the straddle region and
    moves fully into one zone, it adopts only that zone's regulations.

    Boundary Creek flows: zone 3 → boundary → zone 4.
    - Zone 3 reg: picks up fully_zone3 (base) + near_boundary (buffer).
      Must NOT pick up fully_zone4 or side_channel_fully_zone4.
    - Zone 4 reg: picks up near_boundary + fully_zone4 + both side
      channels (base).  Must NOT pick up fully_zone3."""

    @pytest.fixture()
    def all_entries(self):
        return [
            _boundary_creek_fully_zone3(),
            _boundary_creek_near_boundary(),
            _boundary_creek_fully_zone4(),
            _boundary_creek_side_channel_near_boundary(),
            _boundary_creek_side_channel_fully_zone4(),
        ]

    @pytest.fixture()
    def mapper(self, all_entries):
        return _build_mapper_with_entries(all_entries)

    @pytest.fixture()
    def indexes(self, all_entries):
        gaz = _FakeGazetteer(all_entries)
        return build_feature_index(gaz, feature_types=[FeatureType.STREAM])

    def test_zone3_excludes_all_fully_zone4(self, mapper, indexes):
        """Zone 3 regulation must NOT include any segment fully in zone 4."""
        from regulation_mapping.regulation_resolvers import resolve_zone_wide_ids
        from regulation_mapping.zone_base_regulations import ZoneRegulation

        reg = ZoneRegulation(
            regulation_id="zone3_test",
            zone_ids=["3"],
            rule_text="Zone 3 reg",
            restriction={"type": "CLOSURE", "details": "test"},
            notes="test",
        )
        zone_idx, mu_idx, zone_idx_buf, mu_idx_buf = indexes
        base_ids = resolve_zone_wide_ids(reg, zone_idx, mu_idx)
        buffered_ids = resolve_zone_wide_ids(reg, zone_idx_buf, mu_idx_buf)
        extended, _ = mapper._extend_boundary_streams(
            base_ids, buffered_ids,
        )

        # Segments fully in zone 4 (no buffer bleed to zone 3)
        assert "900000003" not in extended, (
            "Mainstem fully in zone 4 must NOT be in zone 3 result"
        )
        assert "900000005" not in extended, (
            "Side channel fully in zone 4 must NOT be in zone 3 result"
        )

        # Segments that ARE included
        assert "900000001" in extended, "Fully zone 3 must be included"
        assert "900000002" in extended, (
            "Near-boundary mainstem (buffer touches zone 3) must be included"
        )
        assert "900000004" in extended, (
            "Near-boundary side channel (buffer touches zone 3, same WSC) "
            "must be included"
        )

    def test_zone4_excludes_fully_zone3(self, mapper, indexes):
        """Zone 4 regulation must NOT include the segment fully in zone 3."""
        from regulation_mapping.regulation_resolvers import resolve_zone_wide_ids
        from regulation_mapping.zone_base_regulations import ZoneRegulation

        reg = ZoneRegulation(
            regulation_id="zone4_test",
            zone_ids=["4"],
            rule_text="Zone 4 reg",
            restriction={"type": "CLOSURE", "details": "test"},
            notes="test",
        )
        zone_idx, mu_idx, zone_idx_buf, mu_idx_buf = indexes
        base_ids = resolve_zone_wide_ids(reg, zone_idx, mu_idx)
        buffered_ids = resolve_zone_wide_ids(reg, zone_idx_buf, mu_idx_buf)
        extended, _ = mapper._extend_boundary_streams(
            base_ids, buffered_ids,
        )

        assert "900000001" not in extended, (
            "Fully zone 3 segment must NOT be in zone 4 result"
        )

        # All zone 4 segments present
        assert "900000002" in extended  # near boundary
        assert "900000003" in extended  # fully zone 4
        assert "900000004" in extended  # side channel near boundary
        assert "900000005" in extended  # side channel fully zone 4

    def test_straddle_segments_in_both_zones(self, mapper, indexes):
        """Near-boundary segments (900000002, 900000004) should appear
        in BOTH zone 3 and zone 4 results — they straddle the boundary."""
        from regulation_mapping.regulation_resolvers import resolve_zone_wide_ids
        from regulation_mapping.zone_base_regulations import ZoneRegulation

        zone_idx, mu_idx, zone_idx_buf, mu_idx_buf = indexes

        zone3_reg = ZoneRegulation(
            regulation_id="z3", zone_ids=["3"],
            rule_text="zone3", restriction={"type": "CLOSURE", "details": ""},
            notes="",
        )
        zone4_reg = ZoneRegulation(
            regulation_id="z4", zone_ids=["4"],
            rule_text="zone4", restriction={"type": "CLOSURE", "details": ""},
            notes="",
        )

        base3 = resolve_zone_wide_ids(zone3_reg, zone_idx, mu_idx)
        buf3 = resolve_zone_wide_ids(zone3_reg, zone_idx_buf, mu_idx_buf)
        ext3, _ = mapper._extend_boundary_streams(base3, buf3)

        base4 = resolve_zone_wide_ids(zone4_reg, zone_idx, mu_idx)
        buf4 = resolve_zone_wide_ids(zone4_reg, zone_idx_buf, mu_idx_buf)
        ext4, _ = mapper._extend_boundary_streams(base4, buf4)

        # Near-boundary segments in both results
        for seg_id in ("900000002", "900000004"):
            assert seg_id in ext3, f"{seg_id} should be in zone 3 result"
            assert seg_id in ext4, f"{seg_id} should be in zone 4 result"

        # Non-straddle segments only in their own zone
        assert "900000001" in ext3 and "900000001" not in ext4
        assert "900000003" not in ext3 and "900000003" in ext4
        assert "900000005" not in ext3 and "900000005" in ext4
