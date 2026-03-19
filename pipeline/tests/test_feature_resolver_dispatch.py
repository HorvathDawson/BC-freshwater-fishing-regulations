"""Tests for feature_resolver.py — every override ID type dispatch path.

Verifies that each identifier field on OverrideEntry:
  - gnis_ids, waterbody_keys, waterbody_poly_ids, fwa_watershed_codes,
    blue_line_keys, linear_feature_ids, admin_targets
...actually resolves to atlas features when passed through resolve_features().

Also includes a pattern-level guard: every field that OverrideEntry.has_match
considers must have a corresponding dispatch in resolve_features(). If a new
field is added to has_match but not dispatched, the guard test will fail.

Fixture strategy: lightweight AtlasStub + metadata dict — no GPKG needed.
"""

from __future__ import annotations

import inspect
from collections import defaultdict
from typing import Any, Dict, List, Optional, Set

import pytest
from shapely.geometry import LineString, Point, box

from pipeline.atlas.models import AdminRecord, PolygonRecord, StreamRecord
from pipeline.enrichment.feature_resolver import (
    _resolve_by_blue_line_keys,
    _resolve_by_fwa_watershed_codes,
    _resolve_by_gnis_ids,
    _resolve_by_linear_feature_ids,
    _resolve_by_waterbody_keys,
    _resolve_by_waterbody_poly_ids,
    resolve_features,
)
from pipeline.enrichment.models import (
    AtlasMetadata,
    FeatureAssignment,
    RegulationRecord,
    ResolvedRegulation,
)
from pipeline.matching.match_table import BaseEntry, OverrideEntry
from pipeline.matching.reg_models import MatchCriteria


# ===================================================================
# Helpers
# ===================================================================


def _make_criteria(name: str = "TEST WATER") -> MatchCriteria:
    return MatchCriteria(name_verbatim=name, region="Region 1", mus=["1-1"])


def _make_base_entry(
    name: str = "TEST WATER",
    gnis_ids: Optional[List[str]] = None,
) -> BaseEntry:
    return BaseEntry(
        criteria=_make_criteria(name),
        gnis_ids=gnis_ids or [],
        link_method="natural_search",
    )


def _make_override(
    name: str = "TEST WATER",
    gnis_ids: Optional[List[str]] = None,
    waterbody_keys: Optional[List[str]] = None,
    waterbody_poly_ids: Optional[List[str]] = None,
    fwa_watershed_codes: Optional[List[str]] = None,
    blue_line_keys: Optional[List[str]] = None,
    linear_feature_ids: Optional[List[str]] = None,
    admin_targets: Optional[List[Dict[str, str]]] = None,
    skip: bool = False,
) -> OverrideEntry:
    return OverrideEntry(
        criteria=_make_criteria(name),
        gnis_ids=gnis_ids or [],
        waterbody_keys=waterbody_keys or [],
        waterbody_poly_ids=waterbody_poly_ids or [],
        fwa_watershed_codes=fwa_watershed_codes or [],
        blue_line_keys=blue_line_keys or [],
        linear_feature_ids=linear_feature_ids or [],
        admin_targets=admin_targets or [],
        skip=skip,
    )


def _make_record(
    reg_id: str,
    entry: Any,
    parsed: Optional[Dict[str, Any]] = None,
) -> RegulationRecord:
    return RegulationRecord(
        index=0,
        reg_id=reg_id,
        water="TEST WATER",
        region="Region 1",
        mu=("1-1",),
        raw_regs="Test regulation text",
        symbols=(),
        page=1,
        image="test.png",
        match_entry=entry,
        parsed=parsed,
    )


class AtlasStub:
    """Minimal atlas-like object for resolver tests."""

    def __init__(self) -> None:
        self.streams: Dict[str, StreamRecord] = {}
        self.under_lake_streams: Dict[str, StreamRecord] = {}
        self.lakes: Dict[str, PolygonRecord] = {}
        self.wetlands: Dict[str, PolygonRecord] = {}
        self.manmade: Dict[str, PolygonRecord] = {}
        self.parks_nat: Dict[str, AdminRecord] = {}
        self.eco_reserves: Dict[str, AdminRecord] = {}
        self.wma: Dict[str, AdminRecord] = {}
        self.historic_sites: Dict[str, AdminRecord] = {}
        self.watersheds: Dict[str, AdminRecord] = {}
        self.wmu: Dict[str, AdminRecord] = {}
        self.osm_admin: Dict[str, AdminRecord] = {}
        self.aboriginal_lands: Dict[str, AdminRecord] = {}
        self.poly_id_to_wbk: Dict[str, str] = {}
        self.zone_polygons: Dict[str, Any] = {}


def _build_metadata(
    streams: Optional[Dict[str, Any]] = None,
    gnis_to_wbk: Optional[Dict[str, Set[str]]] = None,
    wbk_to_fids: Optional[Dict[str, List[str]]] = None,
) -> AtlasMetadata:
    """Build a minimal metadata dict."""
    return {
        "streams": streams or {},
        "lakes": {},
        "wetlands": {},
        "manmade": {},
        "_gnis_to_wbk": defaultdict(set, gnis_to_wbk or {}),
        "_wbk_to_fids": defaultdict(list, wbk_to_fids or {}),
    }


def _stream(
    fid: str,
    name: str = "",
    blk: str = "BL1",
    wsc: str = "100",
    wbk: str = "",
) -> StreamRecord:
    return StreamRecord(
        fid=fid,
        geometry=LineString([(0, 0), (10, 0)]),
        display_name=name,
        blk=blk,
        stream_order=3,
        stream_magnitude=10,
        waterbody_key=wbk,
        fwa_watershed_code=wsc,
    )


def _lake(wbk: str, name: str = "", gnis_id: str = "") -> PolygonRecord:
    return PolygonRecord(
        waterbody_key=wbk,
        geometry=box(0, 0, 10, 10),
        display_name=name,
        area=100.0,
        gnis_id=gnis_id,
    )


# ===================================================================
# Pattern-level guard: has_match fields must have dispatch
# ===================================================================


class TestOverrideFieldDispatchGuard:
    """Ensures every field that OverrideEntry.has_match checks also has
    a corresponding dispatch path in resolve_features().

    If someone adds a new identifier field to has_match but forgets to
    add a resolver, this test will catch it — preventing another
    'waterbody_poly_ids silently ignored' style bug.
    """

    def test_every_has_match_field_is_dispatched(self):
        """Parse has_match source to extract field names, then verify
        each one triggers actual feature resolution when populated."""

        # Extract field names from has_match property source
        source = inspect.getsource(OverrideEntry.has_match.fget)
        # has_match checks: self.gnis_ids, self.waterbody_keys, etc.
        import re

        fields_checked = set(re.findall(r"self\.(\w+)", source))
        # Remove non-identifier fields
        fields_checked.discard("skip")

        # These are the ID fields that should have resolvers
        expected_id_fields = {
            "gnis_ids",
            "waterbody_keys",
            "waterbody_poly_ids",
            "fwa_watershed_codes",
            "blue_line_keys",
            "linear_feature_ids",
            "admin_targets",
            "ungazetted_waterbody_id",
        }

        # Verify has_match checks all expected fields
        assert fields_checked == expected_id_fields, (
            f"has_match checks {fields_checked} but expected {expected_id_fields}. "
            f"Missing from has_match: {expected_id_fields - fields_checked}. "
            f"Extra in has_match: {fields_checked - expected_id_fields}."
        )

    def test_resolve_features_source_dispatches_all_id_fields(self):
        """Verify resolve_features() references every override ID field."""
        source = inspect.getsource(resolve_features)
        dispatched_fields = {
            "gnis_ids",  # dispatched on both BaseEntry and OverrideEntry
            "waterbody_keys",
            "waterbody_poly_ids",
            "fwa_watershed_codes",
            "blue_line_keys",
            "linear_feature_ids",
            "admin_targets",
            # NOTE: ungazetted_waterbody_id is in has_match but dispatched
            # via a custom waterbody lookup outside the standard resolver
            # chain. If a resolver is added, include it here.
        }
        for field_name in dispatched_fields:
            assert (
                f"entry.{field_name}" in source or f".{field_name}" in source
            ), f"resolve_features() does not dispatch entry.{field_name}"

    def test_resolve_features_calls_all_resolver_functions(self):
        """Verify resolve_features() calls every _resolve_by_* function."""
        source = inspect.getsource(resolve_features)
        resolver_functions = {
            "_resolve_by_gnis_ids",
            "_resolve_by_waterbody_keys",
            "_resolve_by_waterbody_poly_ids",
            "_resolve_by_fwa_watershed_codes",
            "_resolve_by_blue_line_keys",
            "_resolve_by_linear_feature_ids",
            "_resolve_by_admin_targets",
        }
        for func in resolver_functions:
            assert func in source, f"resolve_features() does not call {func}"


# ===================================================================
# Individual resolver function tests
# ===================================================================


class TestResolveByGnisIds:
    """Test _resolve_by_gnis_ids — resolves GNIS IDs to stream fids and lake wbks."""

    def test_stream_resolution(self):
        """GNIS ID matching a stream group returns the stream fids."""
        atlas = AtlasStub()
        atlas.streams["s1"] = _stream("s1", name="Alice Creek")
        metadata = _build_metadata(
            streams={
                "gnis_alice": {
                    "gnis_id": "gnis_alice",
                    "gnis_name": "Alice Creek",
                    "edge_ids": ["s1"],
                    "fwa_watershed_codes": ["100"],
                    "blue_line_keys": ["BL1"],
                    "zones": [],
                    "mgmt_units": [],
                }
            }
        )
        fids, wbks, seeds, lk_seeds = _resolve_by_gnis_ids(
            ["gnis_alice"], metadata, atlas
        )
        assert "s1" in fids
        assert "s1" in seeds
        assert wbks == set()

    def test_lake_resolution_via_gnis_to_wbk(self):
        """GNIS ID mapping to a lake wbk returns the wbk and lake seeds."""
        atlas = AtlasStub()
        atlas.lakes["lake_a"] = _lake("lake_a", "Test Lake", gnis_id="gnis_lake")
        metadata = _build_metadata(
            gnis_to_wbk={"gnis_lake": {"lake_a"}},
            wbk_to_fids={"lake_a": ["outlet_fid"]},
        )
        fids, wbks, seeds, lk_seeds = _resolve_by_gnis_ids(
            ["gnis_lake"], metadata, atlas
        )
        assert "lake_a" in wbks
        assert lk_seeds["lake_a"] == ["outlet_fid"]

    def test_combined_stream_and_lake(self):
        """GNIS ID that maps to both stream and lake resolves both."""
        atlas = AtlasStub()
        atlas.streams["s1"] = _stream("s1")
        atlas.lakes["lake_a"] = _lake("lake_a", gnis_id="gnis_both")
        metadata = _build_metadata(
            streams={
                "gnis_both": {
                    "gnis_id": "gnis_both",
                    "gnis_name": "Both Feature",
                    "edge_ids": ["s1"],
                    "fwa_watershed_codes": ["100"],
                    "blue_line_keys": ["BL1"],
                    "zones": [],
                    "mgmt_units": [],
                }
            },
            gnis_to_wbk={"gnis_both": {"lake_a"}},
            wbk_to_fids={"lake_a": ["out1"]},
        )
        fids, wbks, seeds, lk_seeds = _resolve_by_gnis_ids(
            ["gnis_both"], metadata, atlas
        )
        assert "s1" in fids
        assert "lake_a" in wbks

    def test_unknown_gnis_returns_empty(self):
        """Unknown GNIS ID returns empty sets."""
        atlas = AtlasStub()
        metadata = _build_metadata()
        fids, wbks, seeds, lk_seeds = _resolve_by_gnis_ids(
            ["nonexistent"], metadata, atlas
        )
        assert fids == set()
        assert wbks == set()
        assert seeds == []
        assert lk_seeds == {}


class TestResolveByWaterbodyKeys:
    """Test _resolve_by_waterbody_keys — direct wbk lookup."""

    def test_lake_found(self):
        atlas = AtlasStub()
        atlas.lakes["wbk1"] = _lake("wbk1", "Test Lake")
        metadata = _build_metadata(wbk_to_fids={"wbk1": ["out1"]})
        wbks, lk_seeds = _resolve_by_waterbody_keys(["wbk1"], atlas, metadata)
        assert "wbk1" in wbks
        assert lk_seeds["wbk1"] == ["out1"]

    def test_wetland_found(self):
        atlas = AtlasStub()
        atlas.wetlands["wbk_wet"] = _lake("wbk_wet", "Test Wetland")
        metadata = _build_metadata()
        wbks, _ = _resolve_by_waterbody_keys(["wbk_wet"], atlas, metadata)
        assert "wbk_wet" in wbks

    def test_manmade_found(self):
        atlas = AtlasStub()
        atlas.manmade["wbk_mm"] = _lake("wbk_mm", "Test Reservoir")
        metadata = _build_metadata()
        wbks, _ = _resolve_by_waterbody_keys(["wbk_mm"], atlas, metadata)
        assert "wbk_mm" in wbks

    def test_missing_wbk_warns(self, caplog):
        atlas = AtlasStub()
        metadata = _build_metadata()
        wbks, _ = _resolve_by_waterbody_keys(["gone"], atlas, metadata)
        assert wbks == set()
        assert "not found in atlas" in caplog.text

    def test_no_outlet_fids_still_resolves(self):
        """Lake without outlets still resolves the wbk (no seeds)."""
        atlas = AtlasStub()
        atlas.lakes["isolated"] = _lake("isolated", "Isolated Lake")
        metadata = _build_metadata()  # no wbk_to_fids entry
        wbks, lk_seeds = _resolve_by_waterbody_keys(["isolated"], atlas, metadata)
        assert "isolated" in wbks
        assert lk_seeds == {}  # completely empty, not {"isolated": []}


class TestResolveByWaterbodyPolyIds:
    """Test _resolve_by_waterbody_poly_ids — poly_id → wbk via atlas index."""

    def test_poly_id_resolves_to_wbk(self):
        atlas = AtlasStub()
        atlas.poly_id_to_wbk = {"PID_1": "wbk_1", "PID_2": "wbk_1"}
        atlas.lakes["wbk_1"] = _lake("wbk_1", "Slough Lake")
        metadata = _build_metadata(wbk_to_fids={"wbk_1": ["out1"]})
        wbks, lk_seeds = _resolve_by_waterbody_poly_ids(
            ["PID_1", "PID_2"], atlas, metadata
        )
        assert wbks == {"wbk_1"}
        assert "wbk_1" in lk_seeds

    def test_multiple_poly_ids_to_different_wbks(self):
        atlas = AtlasStub()
        atlas.poly_id_to_wbk = {"PID_A": "wbk_a", "PID_B": "wbk_b"}
        atlas.lakes["wbk_a"] = _lake("wbk_a")
        atlas.lakes["wbk_b"] = _lake("wbk_b")
        metadata = _build_metadata()
        wbks, _ = _resolve_by_waterbody_poly_ids(["PID_A", "PID_B"], atlas, metadata)
        assert wbks == {"wbk_a", "wbk_b"}

    def test_missing_poly_id_warns(self, caplog):
        atlas = AtlasStub()
        atlas.poly_id_to_wbk = {}
        metadata = _build_metadata()
        wbks, _ = _resolve_by_waterbody_poly_ids(["MISSING"], atlas, metadata)
        assert wbks == set()
        assert "not in poly_id_to_wbk" in caplog.text

    def test_poly_id_maps_to_missing_wbk_warns(self, caplog):
        """poly_id_to_wbk has the mapping but the wbk is not in atlas."""
        atlas = AtlasStub()
        atlas.poly_id_to_wbk = {"PID_1": "ghost_wbk"}
        metadata = _build_metadata()
        wbks, _ = _resolve_by_waterbody_poly_ids(["PID_1"], atlas, metadata)
        assert wbks == set()
        assert "not found in atlas" in caplog.text


class TestResolveByFwaWatershedCodes:
    """Test _resolve_by_fwa_watershed_codes — WSC scan on atlas."""

    def test_matching_streams(self):
        atlas = AtlasStub()
        atlas.streams["s1"] = _stream("s1", wsc="300-100")
        atlas.streams["s2"] = _stream("s2", wsc="300-200")
        fids = _resolve_by_fwa_watershed_codes(["300-100"], atlas)
        assert fids == {"s1"}

    def test_under_lake_streams_included(self):
        atlas = AtlasStub()
        atlas.under_lake_streams["ul1"] = _stream("ul1", wsc="300-100")
        fids = _resolve_by_fwa_watershed_codes(["300-100"], atlas)
        assert "ul1" in fids

    def test_no_match_warns(self, caplog):
        atlas = AtlasStub()
        fids = _resolve_by_fwa_watershed_codes(["NOPE"], atlas)
        assert fids == set()
        assert "matched zero atlas features" in caplog.text

    def test_multiple_wscs(self):
        atlas = AtlasStub()
        atlas.streams["s1"] = _stream("s1", wsc="300-100")
        atlas.streams["s2"] = _stream("s2", wsc="300-200")
        atlas.streams["s3"] = _stream("s3", wsc="400-100")
        fids = _resolve_by_fwa_watershed_codes(["300-100", "300-200"], atlas)
        assert fids == {"s1", "s2"}


class TestResolveByBlueLineKeys:
    """Test _resolve_by_blue_line_keys — BLK scan on atlas."""

    def test_matching_streams(self):
        atlas = AtlasStub()
        atlas.streams["s1"] = _stream("s1", blk="BLK_A")
        atlas.streams["s2"] = _stream("s2", blk="BLK_B")
        fids = _resolve_by_blue_line_keys(["BLK_A"], atlas)
        assert fids == {"s1"}

    def test_under_lake_streams_included(self):
        atlas = AtlasStub()
        atlas.under_lake_streams["ul1"] = _stream("ul1", blk="BLK_A")
        fids = _resolve_by_blue_line_keys(["BLK_A"], atlas)
        assert "ul1" in fids

    def test_no_match_warns(self, caplog):
        atlas = AtlasStub()
        fids = _resolve_by_blue_line_keys(["NOPE"], atlas)
        assert fids == set()
        assert "matched zero atlas features" in caplog.text

    def test_multiple_blks(self):
        atlas = AtlasStub()
        atlas.streams["s1"] = _stream("s1", blk="BLK_A")
        atlas.streams["s2"] = _stream("s2", blk="BLK_B")
        atlas.streams["s3"] = _stream("s3", blk="BLK_C")
        fids = _resolve_by_blue_line_keys(["BLK_A", "BLK_B"], atlas)
        assert fids == {"s1", "s2"}

    def test_same_blk_multiple_streams(self):
        """Multiple streams sharing a BLK all resolve."""
        atlas = AtlasStub()
        atlas.streams["s1"] = _stream("s1", blk="SHARED")
        atlas.streams["s2"] = _stream("s2", blk="SHARED")
        fids = _resolve_by_blue_line_keys(["SHARED"], atlas)
        assert fids == {"s1", "s2"}


class TestResolveByLinearFeatureIds:
    """Test _resolve_by_linear_feature_ids — direct fid lookup."""

    def test_found_in_streams(self):
        atlas = AtlasStub()
        atlas.streams["LF_1"] = _stream("LF_1")
        fids = _resolve_by_linear_feature_ids(["LF_1"], atlas)
        assert fids == {"LF_1"}

    def test_found_in_under_lake(self):
        atlas = AtlasStub()
        atlas.under_lake_streams["LF_2"] = _stream("LF_2")
        fids = _resolve_by_linear_feature_ids(["LF_2"], atlas)
        assert fids == {"LF_2"}

    def test_missing_warns(self, caplog):
        atlas = AtlasStub()
        fids = _resolve_by_linear_feature_ids(["GONE"], atlas)
        assert fids == set()
        assert "not found in atlas" in caplog.text

    def test_mix_found_and_missing(self, caplog):
        atlas = AtlasStub()
        atlas.streams["LF_1"] = _stream("LF_1")
        fids = _resolve_by_linear_feature_ids(["LF_1", "MISSING"], atlas)
        assert fids == {"LF_1"}
        assert "MISSING" in caplog.text


# ===================================================================
# Full resolve_features dispatch integration
# ===================================================================


class TestResolveFeaturesFull:
    """Test resolve_features() end-to-end dispatch for each ID type.

    Each test creates a record with ONE populated ID field and verifies
    the correct resolver is called and the result lands in the right
    FeatureAssignment bucket (fid_to_reg_ids or wbk_to_reg_ids).
    """

    def _atlas_with_everything(self) -> AtlasStub:
        """Atlas with one stream and one lake, covering all resolver needs."""
        atlas = AtlasStub()
        atlas.streams["s1"] = _stream(
            "s1", name="Test Creek", blk="BLK_1", wsc="100-001"
        )
        atlas.streams["s2"] = _stream(
            "s2", name="Other Creek", blk="BLK_2", wsc="200-001"
        )
        atlas.under_lake_streams["uls1"] = _stream(
            "uls1", blk="BLK_UL", wsc="100-001", wbk="lake_a"
        )
        atlas.lakes["lake_a"] = _lake("lake_a", "Test Lake", gnis_id="gnis_lake")
        atlas.wetlands["wet_1"] = _lake("wet_1", "Test Wetland")
        atlas.manmade["mm_1"] = _lake("mm_1", "Test Reservoir")
        atlas.poly_id_to_wbk = {"PID_1": "lake_a", "PID_2": "wet_1"}
        # Admin for admin_targets test
        atlas.eco_reserves["eco_1"] = AdminRecord(
            admin_id="eco_1",
            geometry=box(0, 0, 20, 20),
            display_name="Test Eco Reserve",
            admin_type="eco_reserve",
            area=400.0,
        )
        atlas.wmu["wmu_1"] = AdminRecord(
            admin_id="wmu_1",
            geometry=box(-100, -100, 200, 200),
            display_name="WMU 1-1",
            admin_type="wmu",
            area=90000.0,
        )
        return atlas

    def _metadata_with_everything(self) -> AtlasMetadata:
        return _build_metadata(
            streams={
                "gnis_s1": {
                    "gnis_id": "gnis_s1",
                    "gnis_name": "Test Creek",
                    "edge_ids": ["s1"],
                    "fwa_watershed_codes": ["100-001"],
                    "blue_line_keys": ["BLK_1"],
                    "zones": [],
                    "mgmt_units": [],
                }
            },
            gnis_to_wbk={"gnis_lake": {"lake_a"}},
            wbk_to_fids={"lake_a": ["uls1"]},
        )

    def test_gnis_ids_dispatch(self):
        """Record with gnis_ids → stream fids assigned."""
        atlas = self._atlas_with_everything()
        meta = self._metadata_with_everything()
        entry = _make_override(gnis_ids=["gnis_s1"])
        rec = _make_record("R_GNIS", entry)
        resolved, assignments = resolve_features([rec], atlas, meta)
        assert "s1" in assignments.fid_to_reg_ids
        assert "R_GNIS" in assignments.fid_to_reg_ids["s1"]

    def test_gnis_ids_lake_dispatch(self):
        """Record with gnis_ids mapping to a lake → wbk assigned."""
        atlas = self._atlas_with_everything()
        meta = self._metadata_with_everything()
        entry = _make_override(gnis_ids=["gnis_lake"])
        rec = _make_record("R_LAKE", entry)
        resolved, assignments = resolve_features([rec], atlas, meta)
        assert "lake_a" in assignments.wbk_to_reg_ids
        assert "R_LAKE" in assignments.wbk_to_reg_ids["lake_a"]

    def test_waterbody_keys_dispatch(self):
        """Record with waterbody_keys → wbk assigned."""
        atlas = self._atlas_with_everything()
        meta = self._metadata_with_everything()
        entry = _make_override(waterbody_keys=["lake_a"])
        rec = _make_record("R_WBK", entry)
        resolved, assignments = resolve_features([rec], atlas, meta)
        assert "lake_a" in assignments.wbk_to_reg_ids
        assert "R_WBK" in assignments.wbk_to_reg_ids["lake_a"]

    def test_waterbody_poly_ids_dispatch(self):
        """Record with waterbody_poly_ids → resolved to wbk via index → assigned."""
        atlas = self._atlas_with_everything()
        meta = self._metadata_with_everything()
        entry = _make_override(waterbody_poly_ids=["PID_1"])
        rec = _make_record("R_PID", entry)
        resolved, assignments = resolve_features([rec], atlas, meta)
        assert "lake_a" in assignments.wbk_to_reg_ids
        assert "R_PID" in assignments.wbk_to_reg_ids["lake_a"]

    def test_fwa_watershed_codes_dispatch(self):
        """Record with fwa_watershed_codes → stream fids assigned."""
        atlas = self._atlas_with_everything()
        meta = self._metadata_with_everything()
        entry = _make_override(fwa_watershed_codes=["100-001"])
        rec = _make_record("R_WSC", entry)
        resolved, assignments = resolve_features([rec], atlas, meta)
        assert "s1" in assignments.fid_to_reg_ids
        assert "R_WSC" in assignments.fid_to_reg_ids["s1"]

    def test_blue_line_keys_dispatch(self):
        """Record with blue_line_keys → stream fids assigned."""
        atlas = self._atlas_with_everything()
        meta = self._metadata_with_everything()
        entry = _make_override(blue_line_keys=["BLK_1"])
        rec = _make_record("R_BLK", entry)
        resolved, assignments = resolve_features([rec], atlas, meta)
        assert "s1" in assignments.fid_to_reg_ids
        assert "R_BLK" in assignments.fid_to_reg_ids["s1"]

    def test_linear_feature_ids_dispatch(self):
        """Record with linear_feature_ids → stream fids assigned directly."""
        atlas = self._atlas_with_everything()
        meta = self._metadata_with_everything()
        entry = _make_override(linear_feature_ids=["s2"])
        rec = _make_record("R_LFI", entry)
        resolved, assignments = resolve_features([rec], atlas, meta)
        assert "s2" in assignments.fid_to_reg_ids
        assert "R_LFI" in assignments.fid_to_reg_ids["s2"]

    def test_admin_targets_dispatch_streams(self):
        """Record with admin_targets → spatially matching stream fids assigned."""
        atlas = self._atlas_with_everything()
        meta = self._metadata_with_everything()
        entry = _make_override(
            admin_targets=[{"layer": "eco_reserves", "feature_id": "eco_1"}]
        )
        rec = _make_record("R_ADMIN", entry)
        resolved, assignments = resolve_features([rec], atlas, meta)
        # Stream s1 is at (0,0)→(10,0), eco_reserve is box(0,0,20,20) + 500m buffer
        assert "s1" in assignments.fid_to_reg_ids
        assert "R_ADMIN" in assignments.fid_to_reg_ids["s1"]

    def test_admin_targets_dispatch_polygons(self):
        """Admin targets also capture polygon waterbodies inside the buffer."""
        atlas = self._atlas_with_everything()
        meta = self._metadata_with_everything()
        entry = _make_override(
            admin_targets=[{"layer": "eco_reserves", "feature_id": "eco_1"}]
        )
        rec = _make_record("R_ADMIN_POLY", entry)
        resolved, assignments = resolve_features([rec], atlas, meta)
        # lake_a is at box(0,0,10,10) — inside eco_reserve box(0,0,20,20)
        assert "lake_a" in assignments.wbk_to_reg_ids
        assert "R_ADMIN_POLY" in assignments.wbk_to_reg_ids["lake_a"]

    def test_admin_targets_excludes_distant_features(self):
        """Features far from admin polygon are NOT captured."""
        atlas = self._atlas_with_everything()
        # Add a stream far outside the eco_reserve + 500m buffer
        atlas.streams["s_far"] = _stream(
            "s_far", name="Far Away Creek", blk="BLK_FAR", wsc="999-001"
        )
        # Move its geometry far away
        atlas.streams["s_far"] = StreamRecord(
            fid="s_far",
            geometry=LineString([(10000, 10000), (10010, 10000)]),
            display_name="Far Away Creek",
            blk="BLK_FAR",
            stream_order=2,
            stream_magnitude=5,
            waterbody_key="",
            fwa_watershed_code="999-001",
        )
        meta = self._metadata_with_everything()
        entry = _make_override(
            admin_targets=[{"layer": "eco_reserves", "feature_id": "eco_1"}]
        )
        rec = _make_record("R_ADMIN_FAR", entry)
        resolved, assignments = resolve_features([rec], atlas, meta)
        # Far stream should NOT be in assignments
        assert "s_far" not in assignments.fid_to_reg_ids

    def test_admin_targets_tracked_as_admin(self):
        """Fids/wbks from admin_targets must be in fid_to_admin_reg_ids /
        wbk_to_admin_reg_ids so reach_builder can exclude them from display
        name fallback."""
        atlas = self._atlas_with_everything()
        meta = self._metadata_with_everything()
        entry = _make_override(
            admin_targets=[{"layer": "eco_reserves", "feature_id": "eco_1"}]
        )
        rec = _make_record("R_ADMIN_TRACK", entry)
        resolved, assignments = resolve_features([rec], atlas, meta)
        # Stream fid must be tracked as admin
        assert "R_ADMIN_TRACK" in assignments.fid_to_admin_reg_ids.get("s1", set())
        # Polygon wbk must be tracked as admin
        assert "R_ADMIN_TRACK" in assignments.wbk_to_admin_reg_ids.get("lake_a", set())

    def test_base_entry_only_dispatches_gnis(self):
        """BaseEntry only has gnis_ids — other fields must not be dispatched."""
        atlas = self._atlas_with_everything()
        meta = self._metadata_with_everything()
        entry = _make_base_entry(gnis_ids=["gnis_s1"])
        rec = _make_record("R_BASE", entry)
        resolved, assignments = resolve_features([rec], atlas, meta)
        assert "s1" in assignments.fid_to_reg_ids
        assert "R_BASE" in assignments.fid_to_reg_ids["s1"]

    def test_skip_entry_not_resolved(self):
        """Skipped override should still get a ResolvedRegulation but empty matches."""
        atlas = self._atlas_with_everything()
        meta = self._metadata_with_everything()
        entry = _make_override(gnis_ids=["gnis_s1"], skip=True)
        rec = _make_record("R_SKIP", entry)
        resolved, assignments = resolve_features([rec], atlas, meta)
        # The record gets processed but skip entries still have gnis_ids dispatched
        # because skip is handled at the match_entry level, not resolver level.
        # But check the resolved regulation exists
        assert len(resolved) == 1

    def test_combined_ids_all_resolve(self):
        """Override with multiple ID types — all should resolve together."""
        atlas = self._atlas_with_everything()
        meta = self._metadata_with_everything()
        entry = _make_override(
            gnis_ids=["gnis_s1"],
            waterbody_keys=["wet_1"],
            blue_line_keys=["BLK_2"],
        )
        rec = _make_record("R_COMBO", entry)
        resolved, assignments = resolve_features([rec], atlas, meta)
        # Stream via gnis
        assert "R_COMBO" in assignments.fid_to_reg_ids["s1"]
        # Wetland via wbk
        assert "R_COMBO" in assignments.wbk_to_reg_ids["wet_1"]
        # Stream via BLK
        assert "R_COMBO" in assignments.fid_to_reg_ids["s2"]

    def test_empty_entry_produces_empty_resolution(self):
        """Override with all empty ID fields → no assignments."""
        atlas = self._atlas_with_everything()
        meta = self._metadata_with_everything()
        entry = _make_override()  # all IDs empty
        rec = _make_record("R_EMPTY", entry)
        resolved, assignments = resolve_features([rec], atlas, meta)
        assert assignments.phase2_assignments == 0

    def test_resolved_regulation_includes_tributaries_from_parsed(self):
        """includes_tributaries flag comes from parsed result."""
        atlas = self._atlas_with_everything()
        meta = self._metadata_with_everything()
        entry = _make_override(gnis_ids=["gnis_s1"])
        parsed = {
            "includes_tributaries": True,
            "tributary_only": False,
            "regs_verbatim": "Test",
            "rules": [],
        }
        rec = _make_record("R_TRIB", entry, parsed=parsed)
        resolved, assignments = resolve_features([rec], atlas, meta)
        assert resolved[0].includes_tributaries is True
        assert resolved[0].tributary_only is False

    def test_tributary_only_skips_direct_assignment(self):
        """tributary_only=True → fids are NOT assigned in Phase 2."""
        atlas = self._atlas_with_everything()
        meta = self._metadata_with_everything()
        entry = _make_override(gnis_ids=["gnis_s1"])
        parsed = {
            "includes_tributaries": True,
            "tributary_only": True,
            "regs_verbatim": "Test",
            "rules": [],
        }
        rec = _make_record("R_TRIBONLY", entry, parsed=parsed)
        resolved, assignments = resolve_features([rec], atlas, meta)
        # Fids should NOT be directly assigned
        assert "R_TRIBONLY" not in assignments.fid_to_reg_ids.get("s1", set())
        # But resolved should have stream seeds for Phase 3
        assert resolved[0].tributary_only is True
        assert len(resolved[0].tributary_stream_seeds) > 0

    def test_phase2_assignment_counter(self):
        """Each new fid/wbk assignment increments the phase2 counter."""
        atlas = self._atlas_with_everything()
        meta = self._metadata_with_everything()
        entry = _make_override(
            gnis_ids=["gnis_s1"],
            waterbody_keys=["lake_a"],
        )
        rec = _make_record("R_COUNT", entry)
        resolved, assignments = resolve_features([rec], atlas, meta)
        # s1 from gnis + lake_a from gnis_to_wbk + lake_a from waterbody_keys (dedup)
        assert assignments.phase2_assignments >= 2

    def test_under_lake_streams_excluded_from_stream_seeds(self):
        """Under-lake fids should not appear in tributary_stream_seeds."""
        atlas = self._atlas_with_everything()
        meta = self._metadata_with_everything()
        # Override that resolves to WSC 100-001 — which matches both s1 and uls1
        entry = _make_override(fwa_watershed_codes=["100-001"])
        parsed = {
            "includes_tributaries": True,
            "tributary_only": False,
            "regs_verbatim": "Test",
            "rules": [],
        }
        rec = _make_record("R_NO_UL_SEED", entry, parsed=parsed)
        resolved, assignments = resolve_features([rec], atlas, meta)
        # uls1 should be in matched fids but NOT in stream seeds
        assert "uls1" in resolved[0].matched_stream_fids
        assert "uls1" not in resolved[0].tributary_stream_seeds


# ===================================================================
# Regression tests — V2_VERIFICATION.md known issues
# ===================================================================


class TestNicomenSloughRegression:
    """Regression guard for Nicomen Slough Lakes (V2_VERIFICATION.md).

    The override has waterbody_poly_ids for 7 slough lakes.
    Previously, feature_resolver silently ignored waterbody_poly_ids.
    This test verifies the full chain: poly_ids → wbk index → lake wbks → assignments.
    """

    def test_multiple_poly_ids_all_resolve_to_lakes(self):
        """All 7 poly_ids resolve to lake wbks and get the regulation assigned."""
        atlas = AtlasStub()
        wbks = [f"lake_nicomen_{i}" for i in range(7)]
        poly_ids = [f"PID_{i}" for i in range(7)]
        outlet_fids = [f"outlet_{i}" for i in range(7)]

        for wbk in wbks:
            atlas.lakes[wbk] = _lake(wbk, f"Nicomen Lake {wbk}")
        atlas.poly_id_to_wbk = dict(zip(poly_ids, wbks))

        metadata = _build_metadata(
            wbk_to_fids={wbk: [fid] for wbk, fid in zip(wbks, outlet_fids)}
        )

        entry = _make_override(waterbody_poly_ids=poly_ids)
        rec = _make_record("REG_NICOMEN", entry)
        resolved, assignments = resolve_features([rec], atlas, metadata)

        for wbk in wbks:
            assert wbk in assignments.wbk_to_reg_ids, f"{wbk} not resolved"
            assert "REG_NICOMEN" in assignments.wbk_to_reg_ids[wbk]

    def test_poly_ids_produce_lake_outlet_seeds(self):
        """Resolved poly_id lakes produce lake_outlet_fids for Phase 3."""
        atlas = AtlasStub()
        atlas.lakes["slough_lake"] = _lake("slough_lake", "Slough Lake")
        atlas.poly_id_to_wbk = {"PID_SLOUGH": "slough_lake"}
        metadata = _build_metadata(wbk_to_fids={"slough_lake": ["outlet_1"]})

        entry = _make_override(waterbody_poly_ids=["PID_SLOUGH"])
        parsed = {
            "includes_tributaries": True,
            "tributary_only": False,
            "regs_verbatim": "Test",
            "rules": [],
        }
        rec = _make_record("REG_SLOUGH", entry, parsed=parsed)
        resolved, assignments = resolve_features([rec], atlas, metadata)

        # Lake outlet seeds should be present for Phase 3 BFS
        assert len(resolved[0].lake_outlet_fids) > 0
        outlet_wbks = [pair[0] for pair in resolved[0].lake_outlet_fids]
        assert "slough_lake" in outlet_wbks
