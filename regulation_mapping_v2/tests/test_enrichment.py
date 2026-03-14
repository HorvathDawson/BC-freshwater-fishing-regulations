"""Tests for v2 enrichment pipeline — models, loader, resolver, reach builder."""

from __future__ import annotations

import json
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Set

import pytest

from regulation_mapping_v2.enrichment.models import (
    BaseRegulationDef,
    FeatureAssignment,
    RegulationRecord,
    ResolvedRegulation,
)
from regulation_mapping_v2.enrichment.loader import _make_reg_id, _flatten_raw_pages
from regulation_mapping_v2.matching.match_table import BaseEntry, OverrideEntry
from regulation_mapping_v2.matching.reg_models import MatchCriteria


# ===================================================================
# Helpers — lightweight mocks
# ===================================================================


def _base_entry(
    name: str = "TEST LAKE",
    region: str = "Region 2",
    mus: Optional[List[str]] = None,
    gnis_ids: Optional[List[str]] = None,
) -> BaseEntry:
    """Build a minimal BaseEntry for tests."""
    return BaseEntry(
        criteria=MatchCriteria(
            name_verbatim=name,
            region=region,
            mus=mus or ["2-7"],
        ),
        gnis_ids=gnis_ids or ["9999"],
        link_method="natural_search",
    )


def _override_entry(
    name: str = "TEST CREEK",
    region: str = "Region 3",
    mus: Optional[List[str]] = None,
    waterbody_keys: Optional[List[str]] = None,
    fwa_watershed_codes: Optional[List[str]] = None,
    skip: bool = False,
) -> OverrideEntry:
    """Build a minimal OverrideEntry for tests."""
    return OverrideEntry(
        criteria=MatchCriteria(
            name_verbatim=name,
            region=region,
            mus=mus or ["3-5"],
        ),
        waterbody_keys=waterbody_keys or [],
        fwa_watershed_codes=fwa_watershed_codes or [],
        skip=skip,
    )


def _parsed_result(includes_tributaries: bool = False) -> Dict[str, Any]:
    """Build a minimal parsed result dict."""
    return {
        "regs_verbatim": "Test regulation text",
        "includes_tributaries": includes_tributaries,
        "tributary_only": False,
        "entry_location_text": "",
        "rules": [
            {
                "rule_text": "Test regulation text",
                "restriction_type": "harvest",
                "details": "Test details",
                "location_text": "",
                "dates": [],
            }
        ],
        "audit_log": "ok",
    }


# ===================================================================
# Tests: models.py
# ===================================================================


class TestRegulationRecord:
    """Tests for the frozen RegulationRecord dataclass."""

    def test_creation(self):
        entry = _base_entry()
        rec = RegulationRecord(
            index=0,
            reg_id="R2_TEST_LAKE_2-7",
            water="TEST LAKE",
            region="Region 2",
            mu=("2-7",),
            raw_regs="No fishing",
            symbols=("Stocked",),
            page=10,
            image="p10_r0.png",
            match_entry=entry,
            parsed=_parsed_result(),
            parse_status="success",
        )
        assert rec.reg_id == "R2_TEST_LAKE_2-7"
        assert rec.parse_status == "success"
        assert rec.source == "synopsis"

    def test_frozen(self):
        entry = _base_entry()
        rec = RegulationRecord(
            index=0,
            reg_id="R2_TEST_LAKE_2-7",
            water="TEST LAKE",
            region="Region 2",
            mu=("2-7",),
            raw_regs="No fishing",
            symbols=(),
            page=10,
            image="p10_r0.png",
            match_entry=entry,
        )
        with pytest.raises(AttributeError):
            rec.reg_id = "changed"  # type: ignore[misc]

    def test_defaults(self):
        rec = RegulationRecord(
            index=0,
            reg_id="R2_X_2-7",
            water="X",
            region="Region 2",
            mu=("2-7",),
            raw_regs="",
            symbols=(),
            page=0,
            image="",
        )
        assert rec.parsed is None
        assert rec.parse_status == "failed"
        assert rec.match_entry is None


class TestResolvedRegulation:
    """Tests for the frozen ResolvedRegulation dataclass."""

    def test_defaults(self):
        rec = RegulationRecord(
            index=0,
            reg_id="R2_X_2-7",
            water="X",
            region="R2",
            mu=("2-7",),
            raw_regs="",
            symbols=(),
            page=0,
            image="",
        )
        res = ResolvedRegulation(record=rec)
        assert res.matched_stream_fids == frozenset()
        assert res.matched_waterbody_keys == frozenset()
        assert res.includes_tributaries is False
        assert res.tributary_stream_seeds == ()
        assert res.lake_outlet_fids == ()

    def test_with_matches(self):
        rec = RegulationRecord(
            index=0,
            reg_id="R2_X_2-7",
            water="X",
            region="R2",
            mu=("2-7",),
            raw_regs="",
            symbols=(),
            page=0,
            image="",
        )
        res = ResolvedRegulation(
            record=rec,
            matched_stream_fids=frozenset({"fid1", "fid2"}),
            matched_waterbody_keys=frozenset({"wbk1"}),
            includes_tributaries=True,
            tributary_stream_seeds=("fid1", "fid2"),
        )
        assert len(res.matched_stream_fids) == 2
        assert "wbk1" in res.matched_waterbody_keys
        assert res.includes_tributaries is True


class TestFeatureAssignment:
    """Tests for the mutable FeatureAssignment accumulator."""

    def test_assign_fid(self):
        fa = FeatureAssignment()
        fa.assign_fid("fid1", "REG_A", phase=2)
        fa.assign_fid("fid1", "REG_B", phase=2)
        fa.assign_fid("fid2", "REG_A", phase=3)

        assert fa.fid_to_reg_ids["fid1"] == {"REG_A", "REG_B"}
        assert fa.fid_to_reg_ids["fid2"] == {"REG_A"}
        assert fa.phase2_assignments == 2
        assert fa.phase3_tributary_additions == 1

    def test_assign_wbk(self):
        fa = FeatureAssignment()
        fa.assign_wbk("wbk1", "REG_A", phase=4)
        fa.assign_wbk("wbk1", "REG_A", phase=4)  # duplicate — no increment

        assert fa.wbk_to_reg_ids["wbk1"] == {"REG_A"}
        assert fa.phase4_base_additions == 1

    def test_summary(self):
        fa = FeatureAssignment()
        fa.assign_fid("fid1", "R1", phase=2)
        fa.assign_fid("fid2", "R1", phase=3)
        fa.assign_wbk("wbk1", "R2", phase=4)

        s = fa.summary()
        assert s["stream_fids_with_regs"] == 2
        assert s["polygon_wbks_with_regs"] == 1
        assert s["phase2_direct"] == 1
        assert s["phase3_tributaries"] == 1
        assert s["phase4_base"] == 1

    def test_duplicate_does_not_increment(self):
        fa = FeatureAssignment()
        fa.assign_fid("fid1", "REG_A", phase=2)
        fa.assign_fid("fid1", "REG_A", phase=2)  # same reg_id again
        assert fa.phase2_assignments == 1  # only counted once


class TestBaseRegulationDef:
    """Tests for BaseRegulationDef loading from dict."""

    def test_from_dict_zone(self):
        d = {
            "reg_id": "ZONE_1_TROUT",
            "source": "zone",
            "zone_ids": ["1"],
            "rule_text": "Trout quota 4",
            "restriction": {"type": "Quota", "details": "4 trout"},
            "feature_types": ["stream", "lake"],
        }
        reg = BaseRegulationDef.from_dict(d)
        assert reg.reg_id == "ZONE_1_TROUT"
        assert reg.source == "zone"
        assert reg.zone_ids == ("1",)
        assert reg.feature_types == ("stream", "lake")
        assert reg.buffer_m == 500.0

    def test_from_dict_provincial_admin(self):
        d = {
            "reg_id": "PROV_PARKS",
            "source": "provincial",
            "zone_ids": [],
            "rule_text": "Closed in parks",
            "restriction": {"type": "Closed", "details": "Fishing prohibited"},
            "admin_targets": [{"layer": "parks_nat"}],
            "buffer_m": 300.0,
        }
        reg = BaseRegulationDef.from_dict(d)
        assert reg.admin_targets == ({"layer": "parks_nat"},)
        assert reg.buffer_m == 300.0
        assert reg.feature_types is None  # all types

    def test_from_dict_defaults(self):
        d = {
            "reg_id": "TEST",
            "source": "zone",
            "rule_text": "Test",
            "restriction": {"type": "T", "details": "T"},
        }
        reg = BaseRegulationDef.from_dict(d)
        assert reg.zone_ids == ()
        assert reg.feature_types is None
        assert reg.mu_ids is None
        assert reg.admin_targets is None
        assert reg.notes == ""
        assert reg.dates is None
        assert reg.scope_location is None
        assert reg.include_mu_ids is None
        assert reg.gnis_ids is None
        assert reg.blue_line_keys is None
        assert reg.disabled is False
        assert reg.has_direct_target is False

    def test_from_dict_new_fields(self):
        d = {
            "reg_id": "ZONE_R1_CLOSURE",
            "source": "zone",
            "zone_ids": ["1"],
            "rule_text": "Summer closure",
            "restriction": {"type": "Closed", "details": "Closed Jul–Aug"},
            "mu_ids": ["1-1", "1-2"],
            "include_mu_ids": ["6-12", "6-13"],
            "exclude_mu_ids": ["1-5"],
            "dates": ["Jul 15 – Aug 31"],
            "scope_location": "Region 1 Streams",
            "feature_types": ["stream"],
        }
        reg = BaseRegulationDef.from_dict(d)
        assert reg.include_mu_ids == ("6-12", "6-13")
        assert reg.exclude_mu_ids == ("1-5",)
        assert reg.dates == ("Jul 15 – Aug 31",)
        assert reg.scope_location == "Region 1 Streams"

    def test_from_dict_direct_match(self):
        d = {
            "reg_id": "ZONE_R2_BLK",
            "source": "zone",
            "zone_ids": ["2"],
            "rule_text": "Specific stream",
            "restriction": {"type": "Closed", "details": "Closed"},
            "blue_line_keys": ["123456"],
            "gnis_ids": ["99999"],
        }
        reg = BaseRegulationDef.from_dict(d)
        assert reg.blue_line_keys == ("123456",)
        assert reg.gnis_ids == ("99999",)
        assert reg.has_direct_target is True

    def test_from_dict_disabled(self):
        d = {
            "reg_id": "ZONE_DISABLED",
            "source": "zone",
            "zone_ids": ["2"],
            "rule_text": "Disabled",
            "restriction": {"type": "T", "details": "T"},
            "disabled": True,
            "notes": "TODO: Needs direct-match IDs to enable.",
        }
        reg = BaseRegulationDef.from_dict(d)
        assert reg.disabled is True
        assert "TODO" in reg.notes


# ===================================================================
# Tests: loader.py
# ===================================================================


class TestMakeRegId:
    """Tests for the _make_reg_id function."""

    def test_basic(self):
        assert _make_reg_id("ALICE LAKE", ["2-7"], "Region 2") == "R2_ALICE_LAKE_2-7"

    def test_multiple_mus(self):
        rid = _make_reg_id("ELK RIVER", ["4-23", "4-2"], "Region 4")
        assert rid == "R4_ELK_RIVER_4-2_4-23"  # sorted

    def test_special_characters(self):
        rid = _make_reg_id("CHILLIWACK / VEDDER RIVERS", ["2-4"], None)
        assert rid == "R2_CHILLIWACK_VEDDER_RIVERS_2-4"

    def test_no_mu(self):
        rid = _make_reg_id("MYSTERY LAKE", [], None)
        assert rid == "RX_MYSTERY_LAKE_NOMU"

    def test_apostrophe(self):
        rid = _make_reg_id("MCLEAN'S LAKE", ["3-2"], "Region 3")
        assert rid == "R3_MCLEAN_S_LAKE_3-2"

    def test_zone_from_mu(self):
        """Zone is derived from the first MU prefix, not the region string."""
        rid = _make_reg_id("TEST", ["6-12"], "Region 1")
        assert rid.startswith("R6_")  # zone from MU, not region


class TestFlattenRawPages:
    """Tests for the _flatten_raw_pages function."""

    def test_basic_flatten(self):
        pages = [
            {
                "context": {"page_number": 6, "region": "Region 2"},
                "rows": [
                    {"water": "A", "mu": ["2-1"], "raw_regs": "r1", "page": 6},
                    {"water": "B", "mu": ["2-2"], "raw_regs": "r2", "page": 6},
                ],
            },
            {
                "context": {"page_number": 7, "region": None},
                "rows": [
                    {
                        "water": "C",
                        "mu": ["3-1"],
                        "raw_regs": "r3",
                        "page": 7,
                        "region": "Region 3",
                    },
                ],
            },
        ]
        rows = _flatten_raw_pages(pages)
        assert len(rows) == 3
        assert rows[0]["region"] == "Region 2"
        assert rows[1]["region"] == "Region 2"
        assert rows[2]["region"] == "Region 3"  # preserved from row itself

    def test_empty_pages(self):
        assert _flatten_raw_pages([]) == []

    def test_page_with_no_rows(self):
        pages = [{"context": {"region": "R1"}, "rows": []}]
        assert _flatten_raw_pages(pages) == []


class TestLoadAndMerge:
    """Integration test for the full Phase 1 loader using temp files."""

    def _write_temp_files(self, tmp: Path, n_rows: int = 3):
        """Write raw, match_table, session temp files for n_rows."""
        # Raw pages
        raw_pages = [
            {
                "context": {"page_number": 1, "region": None},
                "rows": [
                    {
                        "water": f"LAKE_{i}",
                        "mu": [f"2-{i}"],
                        "raw_regs": f"Rule for lake {i}",
                        "symbols": [],
                        "page": 1,
                        "image": f"p1_r{i}.png",
                    }
                    for i in range(n_rows)
                ],
            }
        ]
        raw_path = tmp / "raw.json"
        raw_path.write_text(json.dumps(raw_pages))

        # Match table (base entries)
        base_entries = [
            {
                "type": "base",
                "criteria": {
                    "name_verbatim": f"LAKE_{i}",
                    "region": "Region 2",
                    "mus": [f"2-{i}"],
                },
                "gnis_ids": [str(1000 + i)],
                "name_variants": [],
                "link_method": "natural_search",
            }
            for i in range(n_rows)
        ]
        mt_path = tmp / "match_table.json"
        mt_path.write_text(json.dumps(base_entries))

        # Overrides (empty)
        ovr_path = tmp / "overrides.json"
        ovr_path.write_text("[]")

        # Session state
        session = {
            "version": 1,
            "total": n_rows,
            "results": [
                _parsed_result() if i < n_rows - 1 else None for i in range(n_rows)
            ],
            "status": [
                "success" if i < n_rows - 1 else "failed" for i in range(n_rows)
            ],
            "started_at": "2026-01-01T00:00:00",
            "updated_at": "2026-01-01T00:00:00",
        }
        sess_path = tmp / "session.json"
        sess_path.write_text(json.dumps(session))

        return raw_path, mt_path, ovr_path, sess_path

    def test_basic_load(self):
        from regulation_mapping_v2.enrichment.loader import load_and_merge

        with tempfile.TemporaryDirectory() as tmpdir:
            tmp = Path(tmpdir)
            raw_p, mt_p, ovr_p, sess_p = self._write_temp_files(tmp, n_rows=3)

            records = load_and_merge(raw_p, mt_p, ovr_p, sess_p)

            assert len(records) == 3
            assert records[0].water == "LAKE_0"
            assert records[0].parse_status == "success"
            assert records[2].parse_status == "failed"
            assert records[2].parsed is None

    def test_skip_override(self):
        from regulation_mapping_v2.enrichment.loader import load_and_merge

        with tempfile.TemporaryDirectory() as tmpdir:
            tmp = Path(tmpdir)
            raw_p, mt_p, _, sess_p = self._write_temp_files(tmp, n_rows=2)

            # Override that skips LAKE_0
            overrides = [
                {
                    "type": "override",
                    "criteria": {
                        "name_verbatim": "LAKE_0",
                        "region": "Region 2",
                        "mus": ["2-0"],
                    },
                    "skip": True,
                    "skip_reason": "test skip",
                }
            ]
            ovr_path = tmp / "overrides.json"
            ovr_path.write_text(json.dumps(overrides))

            records = load_and_merge(raw_p, mt_p, ovr_path, sess_p)
            assert len(records) == 1
            assert records[0].water == "LAKE_1"

    def test_mismatched_lengths_raises(self):
        from regulation_mapping_v2.enrichment.loader import load_and_merge

        with tempfile.TemporaryDirectory() as tmpdir:
            tmp = Path(tmpdir)
            raw_p, mt_p, ovr_p, sess_p = self._write_temp_files(tmp, n_rows=3)

            # Corrupt session to have wrong length
            session = json.loads(sess_p.read_text())
            session["results"].append(None)
            session["status"].append("pending")
            sess_p.write_text(json.dumps(session))

            with pytest.raises(ValueError, match="session has 4 results"):
                load_and_merge(raw_p, mt_p, ovr_p, sess_p)


# ===================================================================
# Tests: reach_builder.py
# ===================================================================


class TestReachId:
    """Tests for the _reach_id hash function."""

    def test_deterministic(self):
        from regulation_mapping_v2.enrichment.reach_builder import _reach_id

        a = _reach_id("300-123", "Kootenay River", "REG_A,REG_B")
        b = _reach_id("300-123", "Kootenay River", "REG_A,REG_B")
        assert a == b
        assert len(a) == 12

    def test_different_regs_different_id(self):
        from regulation_mapping_v2.enrichment.reach_builder import _reach_id

        a = _reach_id("300-123", "River", "REG_A")
        b = _reach_id("300-123", "River", "REG_A,REG_B")
        assert a != b

    def test_different_wsc_different_id(self):
        from regulation_mapping_v2.enrichment.reach_builder import _reach_id

        a = _reach_id("300-123", "River", "REG_A")
        b = _reach_id("300-456", "River", "REG_A")
        assert a != b


class TestBuildSynopsisRegulations:
    """Tests for _build_synopsis_regulations."""

    def test_basic(self):
        from regulation_mapping_v2.enrichment.reach_builder import (
            _build_synopsis_regulations,
        )

        rec = RegulationRecord(
            index=0,
            reg_id="R2_TEST_2-7",
            water="TEST",
            region="Region 2",
            mu=("2-7",),
            raw_regs="No fishing",
            symbols=("Stocked",),
            page=10,
            image="p10.png",
            parsed=_parsed_result(),
            parse_status="success",
        )
        regs = _build_synopsis_regulations([rec])

        assert "R2_TEST_2-7" in regs
        info = regs["R2_TEST_2-7"]
        assert info["water"] == "TEST"
        assert info["source"] == "synopsis"
        assert info["page"] == 10
        assert "parsed" in info

    def test_failed_parse_no_parsed(self):
        from regulation_mapping_v2.enrichment.reach_builder import (
            _build_synopsis_regulations,
        )

        rec = RegulationRecord(
            index=0,
            reg_id="R2_X_2-7",
            water="X",
            region="R2",
            mu=("2-7",),
            raw_regs="text",
            symbols=(),
            page=1,
            image="p1.png",
            parsed=None,
            parse_status="failed",
        )
        regs = _build_synopsis_regulations([rec])
        assert "parsed" not in regs["R2_X_2-7"]


class TestRegSetDedup:
    """Tests for _dedup_reg_sets."""

    def test_dedup(self):
        from regulation_mapping_v2.enrichment.reach_builder import _dedup_reg_sets

        reaches = {
            "r1": {"reg_set_str": "A,B"},
            "r2": {"reg_set_str": "A,B"},
            "r3": {"reg_set_str": "C"},
        }
        reg_sets, index = _dedup_reg_sets(reaches)
        assert len(reg_sets) == 2
        assert index["A,B"] == 0
        assert index["C"] == 1


# ===================================================================
# Tests: base_regulations.json validity
# ===================================================================


class TestBaseRegulationsJson:
    """Validate that base_regulations.json loads and parses correctly."""

    def test_loads(self):
        path = (
            Path(__file__).resolve().parent.parent
            / "enrichment"
            / "base_regulations.json"
        )
        assert path.exists(), f"base_regulations.json not found at {path}"

        with open(path) as f:
            data = json.load(f)

        assert isinstance(data, list)
        assert len(data) > 0

        for entry in data:
            reg = BaseRegulationDef.from_dict(entry)
            assert reg.reg_id
            assert reg.source in ("zone", "provincial")
            assert reg.rule_text
            assert isinstance(reg.restriction, dict)

    def test_counts(self):
        path = (
            Path(__file__).resolve().parent.parent
            / "enrichment"
            / "base_regulations.json"
        )
        with open(path) as f:
            data = json.load(f)

        provincial = [d for d in data if d["source"] == "provincial"]
        zone = [d for d in data if d["source"] == "zone"]
        disabled = [d for d in data if d.get("disabled")]

        assert len(provincial) == 2
        assert len(zone) >= 220  # 229 zone regs from v1
        assert len(disabled) == 7  # 7 disabled regs from v1

    def test_disabled_have_notes(self):
        path = (
            Path(__file__).resolve().parent.parent
            / "enrichment"
            / "base_regulations.json"
        )
        with open(path) as f:
            data = json.load(f)

        for entry in data:
            if entry.get("disabled"):
                assert "TODO" in entry.get(
                    "notes", ""
                ), f"Disabled reg {entry['reg_id']} missing TODO in notes"

    def test_no_duplicate_reg_ids(self):
        path = (
            Path(__file__).resolve().parent.parent
            / "enrichment"
            / "base_regulations.json"
        )
        with open(path) as f:
            data = json.load(f)

        reg_ids = [d["reg_id"] for d in data]
        assert len(reg_ids) == len(set(reg_ids)), "Duplicate reg_ids found"


# ===================================================================
# Tests: polygon_filter.py
# ===================================================================


class TestStreamPolygonMask:
    """Tests for the two-pass hysteresis polygon filter."""

    def test_empty_geometries(self):
        from shapely.geometry import box
        from regulation_mapping_v2.enrichment.polygon_filter import stream_polygon_mask

        polygon = box(0, 0, 10, 10)
        assert stream_polygon_mask(polygon, []) == []

    def test_stream_inside(self):
        from shapely.geometry import LineString, box
        from regulation_mapping_v2.enrichment.polygon_filter import stream_polygon_mask

        polygon = box(0, 0, 10, 10)
        segments = [LineString([(2, 5), (8, 5)])]
        mask = stream_polygon_mask(polygon, segments, buffer_m=2.0)
        assert mask == [True]

    def test_stream_outside(self):
        from shapely.geometry import LineString, box
        from regulation_mapping_v2.enrichment.polygon_filter import stream_polygon_mask

        polygon = box(0, 0, 10, 10)
        segments = [LineString([(20, 5), (25, 5)])]
        mask = stream_polygon_mask(polygon, segments, buffer_m=2.0)
        assert mask == [False]

    def test_buffer_only_no_enter(self):
        """Stream only touches buffer zone, never enters exact polygon → rejected."""
        from shapely.geometry import LineString, box
        from regulation_mapping_v2.enrichment.polygon_filter import stream_polygon_mask

        polygon = box(0, 0, 10, 10)
        segments = [LineString([(11, 5), (11.5, 5)])]  # just outside, within 2m buffer
        mask = stream_polygon_mask(polygon, segments, buffer_m=2.0)
        assert mask == [False]

    def test_hysteresis_grants_buffer_leniency(self):
        """Stream enters polygon, so sibling segment in buffer zone is included."""
        from shapely.geometry import LineString, box
        from regulation_mapping_v2.enrichment.polygon_filter import stream_polygon_mask

        polygon = box(0, 0, 10, 10)
        segments = [
            LineString([(5, 5), (9, 5)]),  # inside polygon (enters)
            LineString([(9, 5), (11.5, 5)]),  # straddles border into buffer
        ]
        mask = stream_polygon_mask(polygon, segments, buffer_m=3.0)
        assert mask == [True, True]  # both included due to hysteresis

    def test_wsc_grouping_matters(self):
        """Two WSC groups: one enters, one doesn't. Must call per-group."""
        from shapely.geometry import LineString, box
        from regulation_mapping_v2.enrichment.polygon_filter import stream_polygon_mask

        polygon = box(0, 0, 10, 10)

        # WSC group A: enters the polygon
        group_a = [LineString([(5, 5), (9, 5)])]
        mask_a = stream_polygon_mask(polygon, group_a, buffer_m=3.0)
        assert mask_a == [True]

        # WSC group B: only in buffer zone, never enters
        group_b = [LineString([(11, 5), (12, 5)])]
        mask_b = stream_polygon_mask(polygon, group_b, buffer_m=3.0)
        assert mask_b == [False]  # rejected because it never enters

        # If you incorrectly combine both groups into one call:
        combined = group_a + group_b
        mask_combined = stream_polygon_mask(polygon, combined, buffer_m=3.0)
        # group_a enters → hysteresis triggers → group_b gets buffer leniency
        assert mask_combined == [True, True]  # WRONG if they're different streams


# ===================================================================
# Tests: base_reg_assigner.py helpers
# ===================================================================


class TestLoadBaseRegulations:
    """Tests for _load_base_regulations with disabled filtering."""

    def test_filters_disabled(self):
        import tempfile
        from regulation_mapping_v2.enrichment.base_reg_assigner import (
            _load_base_regulations,
        )

        data = [
            {
                "reg_id": "ACTIVE",
                "source": "zone",
                "zone_ids": ["1"],
                "rule_text": "Active reg",
                "restriction": {"type": "T", "details": "T"},
            },
            {
                "reg_id": "DISABLED",
                "source": "zone",
                "zone_ids": ["1"],
                "rule_text": "Disabled reg",
                "restriction": {"type": "T", "details": "T"},
                "disabled": True,
            },
        ]
        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
            json.dump(data, f)
            f.flush()
            regs = _load_base_regulations(Path(f.name))

        assert len(regs) == 1
        assert regs[0].reg_id == "ACTIVE"


class TestFeatureTypeMatches:
    """Tests for _feature_type_matches."""

    def test_none_allowed_matches_all(self):
        from regulation_mapping_v2.enrichment.base_reg_assigner import (
            _feature_type_matches,
        )

        assert _feature_type_matches("stream", None) is True
        assert _feature_type_matches("lake", None) is True

    def test_allowed_list(self):
        from regulation_mapping_v2.enrichment.base_reg_assigner import (
            _feature_type_matches,
        )

        assert _feature_type_matches("stream", ("stream", "lake")) is True
        assert _feature_type_matches("wetland", ("stream", "lake")) is False


# ===================================================================
# Tests: base_reg_assigner.py — precompute and zone assignment
# ===================================================================


class TestPrecomputeZoneMuMap:
    """Tests for _precompute_zone_mu_map."""

    def test_groups_by_zone(self):
        import geopandas as gpd
        from shapely.geometry import box

        from regulation_mapping_v2.enrichment.base_reg_assigner import (
            _precompute_zone_mu_map,
        )

        gdf = gpd.GeoDataFrame(
            {
                "REGION_RESPONSIBLE_ID": ["1", "1", "2"],
                "WILDLIFE_MGMT_UNIT_ID": ["1-1", "1-2", "2-1"],
            },
            geometry=[box(0, 0, 1, 1), box(1, 0, 2, 1), box(2, 0, 3, 1)],
        )
        zone_map = _precompute_zone_mu_map(gdf)
        assert zone_map == {"1": {"1-1", "1-2"}, "2": {"2-1"}}


class TestPrecomputeMuFeatures:
    """Tests for _precompute_mu_features with real shapely geometries."""

    def _make_atlas_stub(self):
        """Build a minimal FreshWaterAtlas-like object with streams and lakes."""
        from regulation_mapping_v2.atlas.models import PolygonRecord, StreamRecord
        from shapely.geometry import LineString, box

        class AtlasStub:
            pass

        atlas = AtlasStub()
        # Two streams: one in MU polygon, one outside
        atlas.streams = {
            "s1": StreamRecord(
                fid="s1",
                geometry=LineString([(5, 5), (8, 5)]),
                display_name="Inside Stream",
                blk="100",
                stream_order=3,
                stream_magnitude=10,
                waterbody_key="",
                fwa_watershed_code="300-100",
            ),
            "s2": StreamRecord(
                fid="s2",
                geometry=LineString([(50, 50), (55, 50)]),
                display_name="Outside Stream",
                blk="200",
                stream_order=2,
                stream_magnitude=5,
                waterbody_key="",
                fwa_watershed_code="300-200",
            ),
        }
        # One lake inside, one outside
        atlas.lakes = {
            "lk1": PolygonRecord(
                waterbody_key="lk1",
                geometry=box(3, 3, 7, 7),
                display_name="Inside Lake",
                area=16.0,
            ),
            "lk2": PolygonRecord(
                waterbody_key="lk2",
                geometry=box(80, 80, 90, 90),
                display_name="Outside Lake",
                area=100.0,
            ),
        }
        atlas.wetlands = {}
        atlas.manmade = {}
        return atlas

    def _make_wmu_gdf(self):
        import geopandas as gpd
        from shapely.geometry import box

        return gpd.GeoDataFrame(
            {
                "WILDLIFE_MGMT_UNIT_ID": ["1-1", "1-2"],
                "REGION_RESPONSIBLE_ID": ["1", "1"],
            },
            geometry=[
                box(0, 0, 20, 20),  # MU 1-1: contains s1, lk1
                box(40, 40, 60, 60),  # MU 1-2: contains s2
            ],
        )

    def test_mu_features_basic(self):
        from regulation_mapping_v2.enrichment.base_reg_assigner import (
            _group_streams_by_wsc,
            _precompute_mu_features,
        )

        atlas = self._make_atlas_stub()
        wsc_groups = _group_streams_by_wsc(atlas)
        wmu_gdf = self._make_wmu_gdf()

        mu_feats = _precompute_mu_features(wmu_gdf, wsc_groups, atlas, buffer_m=2.0)

        assert "1-1" in mu_feats
        assert "1-2" in mu_feats

        fids_11, wbks_11 = mu_feats["1-1"]
        assert "s1" in fids_11  # stream inside MU 1-1
        assert "s2" not in fids_11  # stream outside MU 1-1
        assert "lk1" in wbks_11  # lake inside MU 1-1
        assert "lk2" not in wbks_11  # lake outside MU 1-1

        fids_12, wbks_12 = mu_feats["1-2"]
        assert "s2" in fids_12  # stream inside MU 1-2
        assert "s1" not in fids_12  # stream outside MU 1-2

    def test_boundary_stream_in_both_mus(self):
        """A stream crossing two MUs appears in both MU feature sets."""
        import geopandas as gpd
        from shapely.geometry import LineString, box

        from regulation_mapping_v2.atlas.models import StreamRecord
        from regulation_mapping_v2.enrichment.base_reg_assigner import (
            _group_streams_by_wsc,
            _precompute_mu_features,
        )

        class AtlasStub:
            pass

        atlas = AtlasStub()
        # Stream crossing the MU boundary at x=10
        atlas.streams = {
            "cross": StreamRecord(
                fid="cross",
                geometry=LineString([(8, 5), (12, 5)]),
                display_name="Boundary Stream",
                blk="100",
                stream_order=3,
                stream_magnitude=10,
                waterbody_key="",
                fwa_watershed_code="300-100",
            ),
        }
        atlas.lakes = {}
        atlas.wetlands = {}
        atlas.manmade = {}

        wmu_gdf = gpd.GeoDataFrame(
            {
                "WILDLIFE_MGMT_UNIT_ID": ["A", "B"],
                "REGION_RESPONSIBLE_ID": ["1", "1"],
            },
            geometry=[
                box(0, 0, 10, 10),  # MU A: left side
                box(10, 0, 20, 10),  # MU B: right side
            ],
        )
        wsc_groups = _group_streams_by_wsc(atlas)
        mu_feats = _precompute_mu_features(wmu_gdf, wsc_groups, atlas, buffer_m=2.0)

        # Stream crosses boundary → in both MUs
        assert "cross" in mu_feats["A"][0]
        assert "cross" in mu_feats["B"][0]


class TestAssignZoneWide:
    """Tests for _assign_zone_wide using precomputed MU features."""

    def _setup(self):
        """Build mu_features, zone_mu_map, and atlas stub."""
        from regulation_mapping_v2.atlas.models import PolygonRecord
        from regulation_mapping_v2.enrichment.models import FeatureAssignment
        from shapely.geometry import box

        class AtlasStub:
            pass

        atlas = AtlasStub()
        atlas.lakes = {
            "lk1": PolygonRecord(
                waterbody_key="lk1",
                geometry=box(0, 0, 1, 1),
                display_name="Lake",
                area=1.0,
            ),
        }
        atlas.wetlands = {}
        atlas.manmade = {}

        mu_features = {
            "1-1": ({"s1", "s2"}, {"lk1"}),
            "1-2": ({"s3"}, set()),
            "2-1": ({"s4", "s5"}, set()),
        }
        zone_mu_map = {
            "1": {"1-1", "1-2"},
            "2": {"2-1"},
        }
        assignments = FeatureAssignment()
        return mu_features, zone_mu_map, atlas, assignments

    def test_zone_assignment(self):
        from regulation_mapping_v2.enrichment.base_reg_assigner import (
            _assign_zone_wide,
        )

        mu_feats, zone_map, atlas, assignments = self._setup()
        reg = BaseRegulationDef.from_dict(
            {
                "reg_id": "zone1_reg",
                "source": "zone",
                "zone_ids": ["1"],
                "rule_text": "Test",
                "restriction": {"type": "T", "details": "T"},
            }
        )
        count = _assign_zone_wide(reg, mu_feats, zone_map, atlas, assignments)

        # Zone 1 = MU 1-1 + 1-2 → s1, s2, s3 + lk1
        assert count == 4
        assert "s1" in assignments.fid_to_reg_ids
        assert "s2" in assignments.fid_to_reg_ids
        assert "s3" in assignments.fid_to_reg_ids
        assert "s4" not in assignments.fid_to_reg_ids  # zone 2 only
        assert "lk1" in assignments.wbk_to_reg_ids

    def test_provincial_gets_all(self):
        from regulation_mapping_v2.enrichment.base_reg_assigner import (
            _assign_zone_wide,
        )

        mu_feats, zone_map, atlas, assignments = self._setup()
        reg = BaseRegulationDef.from_dict(
            {
                "reg_id": "prov_reg",
                "source": "provincial",
                "zone_ids": [],
                "rule_text": "Test",
                "restriction": {"type": "T", "details": "T"},
            }
        )
        count = _assign_zone_wide(reg, mu_feats, zone_map, atlas, assignments)

        # Provincial = all MUs → s1, s2, s3, s4, s5 + lk1
        assert count == 6
        assert "s5" in assignments.fid_to_reg_ids

    def test_exclude_mu(self):
        from regulation_mapping_v2.enrichment.base_reg_assigner import (
            _assign_zone_wide,
        )

        mu_feats, zone_map, atlas, assignments = self._setup()
        reg = BaseRegulationDef.from_dict(
            {
                "reg_id": "zone1_excl",
                "source": "zone",
                "zone_ids": ["1"],
                "exclude_mu_ids": ["1-2"],
                "rule_text": "Test",
                "restriction": {"type": "T", "details": "T"},
            }
        )
        count = _assign_zone_wide(reg, mu_feats, zone_map, atlas, assignments)

        # Zone 1 minus MU 1-2 → only MU 1-1 → s1, s2, lk1
        assert count == 3
        assert "s3" not in assignments.fid_to_reg_ids  # excluded MU

    def test_exclude_mu_boundary_stream_survives(self):
        """Stream in both excluded MU X and included MU Y is kept."""
        from regulation_mapping_v2.enrichment.base_reg_assigner import (
            _assign_zone_wide,
        )
        from regulation_mapping_v2.enrichment.models import FeatureAssignment
        from shapely.geometry import box
        from regulation_mapping_v2.atlas.models import PolygonRecord

        class AtlasStub:
            pass

        atlas = AtlasStub()
        atlas.lakes = {}
        atlas.wetlands = {}
        atlas.manmade = {}

        # "cross" stream is in both MU A and MU B
        mu_features = {
            "A": ({"cross", "a_only"}, set()),
            "B": ({"cross", "b_only"}, set()),
        }
        zone_mu_map = {"1": {"A", "B"}}
        assignments = FeatureAssignment()

        reg = BaseRegulationDef.from_dict(
            {
                "reg_id": "excl_test",
                "source": "zone",
                "zone_ids": ["1"],
                "exclude_mu_ids": ["B"],
                "rule_text": "Test",
                "restriction": {"type": "T", "details": "T"},
            }
        )
        count = _assign_zone_wide(reg, mu_features, zone_mu_map, atlas, assignments)

        # MU B excluded, but "cross" is also in MU A → survives
        assert "cross" in assignments.fid_to_reg_ids
        assert "a_only" in assignments.fid_to_reg_ids
        assert "b_only" not in assignments.fid_to_reg_ids

    def test_include_mu_cross_zone(self):
        from regulation_mapping_v2.enrichment.base_reg_assigner import (
            _assign_zone_wide,
        )

        mu_feats, zone_map, atlas, assignments = self._setup()
        reg = BaseRegulationDef.from_dict(
            {
                "reg_id": "zone1_incl",
                "source": "zone",
                "zone_ids": ["1"],
                "include_mu_ids": ["2-1"],
                "rule_text": "Test",
                "restriction": {"type": "T", "details": "T"},
            }
        )
        count = _assign_zone_wide(reg, mu_feats, zone_map, atlas, assignments)

        # Zone 1 + MU 2-1 → s1, s2, s3, s4, s5 + lk1
        assert count == 6
        assert "s4" in assignments.fid_to_reg_ids
        assert "s5" in assignments.fid_to_reg_ids

    def test_specific_mu_ids(self):
        from regulation_mapping_v2.enrichment.base_reg_assigner import (
            _assign_zone_wide,
        )

        mu_feats, zone_map, atlas, assignments = self._setup()
        reg = BaseRegulationDef.from_dict(
            {
                "reg_id": "specific_mu",
                "source": "zone",
                "zone_ids": ["1"],
                "mu_ids": ["1-1"],
                "rule_text": "Test",
                "restriction": {"type": "T", "details": "T"},
            }
        )
        count = _assign_zone_wide(reg, mu_feats, zone_map, atlas, assignments)

        # Only MU 1-1 → s1, s2, lk1
        assert count == 3
        assert "s3" not in assignments.fid_to_reg_ids

    def test_feature_type_filter(self):
        from regulation_mapping_v2.enrichment.base_reg_assigner import (
            _assign_zone_wide,
        )

        mu_feats, zone_map, atlas, assignments = self._setup()
        reg = BaseRegulationDef.from_dict(
            {
                "reg_id": "lakes_only",
                "source": "zone",
                "zone_ids": ["1"],
                "rule_text": "Test",
                "restriction": {"type": "T", "details": "T"},
                "feature_types": ["lake"],
            }
        )
        count = _assign_zone_wide(reg, mu_feats, zone_map, atlas, assignments)

        # Only lakes, no streams
        assert count == 1
        assert "lk1" in assignments.wbk_to_reg_ids
        assert len(assignments.fid_to_reg_ids) == 0


# ===================================================================
# Tests: integration — dry-run with real data (if available)
# ===================================================================


class TestDryRunIntegration:
    """Run Phase 1 against real data files if they exist."""

    _RAW_PATH = Path("output/synopsis/extract_synopsis/synopsis_raw_data.json")
    _MT_PATH = Path("output/regulation_mapping_v2/match_table.json")
    _SESS_PATH = Path("output/regulation_mapping_v2/parsing/session_state.json")

    @pytest.mark.skipif(
        not _RAW_PATH.exists(),
        reason="Real data files not available",
    )
    def test_phase1_real_data(self):
        from regulation_mapping_v2.enrichment.loader import load_and_merge
        from regulation_mapping_v2.matching.match_table import OVERRIDES_PATH

        records = load_and_merge(
            raw_path=self._RAW_PATH,
            match_table_path=self._MT_PATH,
            overrides_path=OVERRIDES_PATH,
            session_path=self._SESS_PATH,
        )

        # Basic sanity: should have most of the 1395 rows (minus skips)
        assert len(records) > 1300
        assert len(records) < 1400

        # All records should have valid reg_ids
        reg_ids = [r.reg_id for r in records]
        assert len(reg_ids) == len(set(reg_ids)), "Duplicate reg_ids found"

        # Most should have successful parses
        success = sum(1 for r in records if r.parse_status == "success")
        assert success > 1300

        # Every record should have a match_entry
        for rec in records:
            assert rec.match_entry is not None
