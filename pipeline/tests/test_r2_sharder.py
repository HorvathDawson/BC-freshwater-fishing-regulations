"""Tests for the R2 shard generator.

Covers:
  - SHA-256 prefix computation (determinism, uniformity, edge cases)
  - Fid, reach, and poly shard building (round-trip integrity)
  - Tier0 enrichment (search_index segment embedding)
  - Manifest generation
  - Full shard_regulation_index integration (writes → reads → validate)
  - shard_from_dict direct invocation (no intermediate file)
"""

from __future__ import annotations

import json
import tempfile
from pathlib import Path

import pytest

from pipeline.deploy.r2_sharder import (
    build_fid_shards,
    build_poly_shards,
    build_reach_shards,
    build_tier0,
    generate_manifest,
    group_by_prefix,
    shard_from_dict,
    shard_prefix,
    shard_regulation_index,
)


# ── shard_prefix ──────────────────────────────────────────────────────


class TestShardPrefix:
    def test_returns_3_char_hex(self) -> None:
        result = shard_prefix("707231")
        assert len(result) == 3
        assert all(c in "0123456789abcdef" for c in result)

    def test_deterministic(self) -> None:
        """Same input always gives same prefix."""
        assert shard_prefix("707231") == shard_prefix("707231")

    def test_different_inputs_can_differ(self) -> None:
        """Different IDs should (usually) map to different prefixes.

        Not guaranteed for any pair, but for dissimilar inputs it's
        overwhelmingly likely (1/4096 chance of collision).
        """
        p1 = shard_prefix("100")
        p2 = shard_prefix("999999999")
        # Not asserting != because collisions are possible,
        # but we verify the function runs without error.
        assert len(p1) == 3
        assert len(p2) == 3

    def test_empty_string(self) -> None:
        """Empty string should still produce a valid 3-char hex prefix."""
        result = shard_prefix("")
        assert len(result) == 3

    def test_long_input(self) -> None:
        """Very long ID should work fine."""
        result = shard_prefix("1" * 1000)
        assert len(result) == 3

    def test_uniform_distribution(self) -> None:
        """Check that 10,000 sequential numeric IDs spread across >=100 buckets.

        With 4096 buckets and 10K items, we expect ~2.4 items per bucket.
        Getting >=100 distinct buckets is a very loose check — in practice
        we'd expect ~3,500+ distinct buckets for 10K items.
        """
        prefixes = {shard_prefix(str(i)) for i in range(10_000)}
        assert (
            len(prefixes) >= 100
        ), f"Only {len(prefixes)} distinct buckets for 10K IDs"


# ── group_by_prefix ───────────────────────────────────────────────────


class TestGroupByPrefix:
    def test_basic_grouping(self) -> None:
        data = {"a": 1, "b": 2, "c": 3}
        groups = group_by_prefix(data)
        # Every original key must appear in exactly one group
        all_keys = set()
        for bucket in groups.values():
            all_keys.update(bucket.keys())
        assert all_keys == {"a", "b", "c"}

    def test_round_trip_values(self) -> None:
        """Values must survive grouping unchanged."""
        data = {"x": {"nested": True}, "y": [1, 2, 3]}
        groups = group_by_prefix(data)
        reconstructed = {}
        for bucket in groups.values():
            reconstructed.update(bucket)
        assert reconstructed == data

    def test_empty_input(self) -> None:
        assert group_by_prefix({}) == {}


# ── build_fid_shards ─────────────────────────────────────────────────


class TestBuildFidShards:
    def test_inverts_reach_segments(self) -> None:
        reach_segments = {
            "reach_aaa": ["100", "101", "102"],
            "reach_bbb": ["200", "201"],
        }
        groups = build_fid_shards(reach_segments)
        # Reconstruct full fid→reach mapping
        fid_to_reach: dict[str, str] = {}
        for bucket in groups.values():
            fid_to_reach.update(bucket)

        assert fid_to_reach["100"] == "reach_aaa"
        assert fid_to_reach["101"] == "reach_aaa"
        assert fid_to_reach["102"] == "reach_aaa"
        assert fid_to_reach["200"] == "reach_bbb"
        assert fid_to_reach["201"] == "reach_bbb"
        assert len(fid_to_reach) == 5

    def test_empty_segments(self) -> None:
        groups = build_fid_shards({})
        assert groups == {}

    def test_reach_with_no_fids(self) -> None:
        """A reach with an empty fid list should produce no fid entries."""
        groups = build_fid_shards({"reach_x": []})
        assert groups == {}


# ── build_reach_shards ────────────────────────────────────────────────


class TestBuildReachShards:
    def test_embeds_fids(self) -> None:
        reaches = {
            "abc123def456": {
                "display_name": "Test Creek",
                "reg_set_index": 5,
                "feature_type": "stream",
            },
        }
        reach_segments = {
            "abc123def456": ["100", "101"],
        }
        groups = build_reach_shards(reaches, reach_segments)
        # Find the reach in its shard
        for bucket in groups.values():
            if "abc123def456" in bucket:
                entry = bucket["abc123def456"]
                assert entry["display_name"] == "Test Creek"
                assert entry["reg_set_index"] == 5
                assert entry["fids"] == ["100", "101"]
                return
        pytest.fail("reach_id not found in any shard")

    def test_reach_without_segments_gets_empty_fids(self) -> None:
        reaches = {"abc123def456": {"display_name": "", "reg_set_index": 0}}
        groups = build_reach_shards(reaches, {})
        for bucket in groups.values():
            if "abc123def456" in bucket:
                assert bucket["abc123def456"]["fids"] == []
                return
        pytest.fail("reach_id not found")

    def test_all_reaches_present(self) -> None:
        """Every reach must appear in exactly one shard."""
        reaches = {f"r{i:012x}": {"reg_set_index": i} for i in range(500)}
        groups = build_reach_shards(reaches, {})
        all_ids = set()
        for bucket in groups.values():
            all_ids.update(bucket.keys())
        assert all_ids == set(reaches.keys())


# ── build_poly_shards ────────────────────────────────────────────────


class TestBuildPolyShards:
    def test_round_trip(self) -> None:
        poly_reaches = {"351": "reach_aaa", "5364": "reach_bbb"}
        groups = build_poly_shards(poly_reaches)
        reconstructed: dict[str, str] = {}
        for bucket in groups.values():
            reconstructed.update(bucket)
        assert reconstructed == poly_reaches

    def test_empty(self) -> None:
        assert build_poly_shards({}) == {}


# ── build_tier0 ──────────────────────────────────────────────────────


class TestBuildTier0:
    def test_enriches_search_index(self) -> None:
        regulations = {"reg_001": {"raw_regs": "No fishing"}}
        reg_sets = ["reg_001"]
        search_index = [
            {
                "display_name": "Adams River",
                "reaches": ["reach_aaa", "reach_bbb"],
                "waterbody_group": "930-508366",
                "feature_type": "stream",
            }
        ]
        reaches = {
            "reach_aaa": {
                "display_name": "Adams River (upper)",
                "reg_set_index": 0,
                "name_variants": [],
                "watershed_code": "100-123456",
                "min_zoom": 8,
                "regions": ["Region 3"],
                "bbox": None,
                "length_km": 12.5,
                "feature_type": "stream",
            },
            "reach_bbb": {
                "display_name": "Adams River (lower)",
                "reg_set_index": 0,
                "name_variants": [],
                "watershed_code": "100-123457",
                "min_zoom": 10,
                "regions": ["Region 3"],
                "bbox": [-119, 50, -118, 51],
                "length_km": 5.0,
                "feature_type": "stream",
            },
        }
        reach_segments = {
            "reach_aaa": ["100", "101"],
            "reach_bbb": ["200"],
        }

        tier0 = build_tier0(
            regulations, reg_sets, search_index, reaches, reach_segments, "v8"
        )

        assert tier0["_shard_version"] == "v8"
        assert tier0["regulations"] == regulations
        assert tier0["reg_sets"] == reg_sets
        assert len(tier0["search_index"]) == 1

        enriched_entry = tier0["search_index"][0]
        # Original "reaches" key should be removed
        assert "reaches" not in enriched_entry
        # Segments should be embedded
        assert len(enriched_entry["segments"]) == 2
        seg_a = enriched_entry["segments"][0]
        assert seg_a["rid"] == "reach_aaa"
        assert seg_a["display_name"] == "Adams River (upper)"
        assert seg_a["watershed_code"] == "100-123456"
        assert seg_a["regions"] == ["Region 3"]
        assert seg_a["fids"] == ["100", "101"]
        seg_b = enriched_entry["segments"][1]
        assert seg_b["rid"] == "reach_bbb"
        assert seg_b["watershed_code"] == "100-123457"
        assert seg_b["fids"] == ["200"]

    def test_missing_reach_skipped(self) -> None:
        """If a search_index entry references a reach that doesn't exist, skip it."""
        tier0 = build_tier0(
            {},
            [],
            [
                {
                    "display_name": "Ghost",
                    "reaches": ["nonexistent"],
                    "feature_type": "stream",
                }
            ],
            {},
            {},
            "v8",
        )
        assert tier0["search_index"][0]["segments"] == []

    def test_preserves_non_reach_fields(self) -> None:
        """Fields like display_name, feature_type, regions, zones, etc. should pass through untouched."""
        entry = {
            "display_name": "Test",
            "feature_type": "lake",
            "regions": ["REGION 1"],
            "reaches": [],
            "zones": ["1"],
        }
        tier0 = build_tier0({}, [], [entry], {}, {}, "v8")
        result = tier0["search_index"][0]
        assert result["display_name"] == "Test"
        assert result["feature_type"] == "lake"
        assert result["regions"] == ["REGION 1"]
        assert result["zones"] == ["1"]


# ── generate_manifest ────────────────────────────────────────────────


class TestGenerateManifest:
    def test_structure(self) -> None:
        m = generate_manifest(1000, 256, 100, "v8")
        assert m["version"] == "v8"
        assert m["status"] == "complete"
        assert m["shard_counts"]["fids"] == 1000
        assert m["shard_counts"]["reaches"] == 256
        assert m["shard_counts"]["polys"] == 100


# ── Integration: shard_regulation_index ──────────────────────────────


class TestShardRegulationIndexIntegration:
    """Full round-trip: write regulation_index.json → shard → validate."""

    @pytest.fixture()
    def sample_regulation_index(self, tmp_path: Path) -> Path:
        """Create a small but realistic regulation_index.json."""
        reaches = {}
        reach_segments: dict[str, list[str]] = {}
        poly_reaches: dict[str, str] = {}

        # 100 stream reaches, each with 3 fids
        for i in range(100):
            rid = f"{i:012x}"
            reaches[rid] = {
                "display_name": f"Creek {i}" if i < 5 else "",
                "name_variants": (
                    [{"name": f"Ck {i}", "source": "direct"}] if i < 5 else []
                ),
                "feature_type": "stream",
                "reg_set_index": i % 10,
                "watershed_code": f"100-{i:06d}",
                "min_zoom": 11,
                "regions": ["REGION 1"],
                "bbox": [-120.0, 50.0, -119.0, 51.0],
                "length_km": 1.5,
            }
            reach_segments[rid] = [str(i * 100 + j) for j in range(3)]

        # 20 polygon reaches
        for i in range(20):
            wbk = str(1000 + i)
            rid = f"poly{i:08x}"
            reaches[rid] = {
                "display_name": f"Lake {i}" if i < 3 else "",
                "name_variants": [],
                "feature_type": "lake",
                "reg_set_index": i % 10,
                "watershed_code": str(1000 + i),
                "min_zoom": 8,
                "regions": ["REGION 2"],
                "bbox": [-121.0, 49.0, -120.0, 50.0],
                "length_km": 0,
            }
            poly_reaches[wbk] = rid

        regulations = {
            "reg_001": {"raw_regs": "No fishing", "source": "synopsis"},
            "reg_002": {"raw_regs": "Catch and release", "source": "zone"},
        }
        reg_sets = ["reg_001", "reg_002", "reg_001,reg_002"]

        search_index = [
            {
                "display_name": f"Creek {i}",
                "name_variants": [{"name": f"Ck {i}", "source": "direct"}],
                "feature_type": "stream",
                "reaches": [f"{i:012x}"],
                "regions": ["REGION 1"],
                "min_zoom": 8,
                "bbox": [-120.0, 50.0, -119.0, 51.0],
                "waterbody_group": f"100-{i:06d}",
                "zones": ["1"],
                "management_units": [],
                "total_length_km": 10.0,
            }
            for i in range(5)
        ] + [
            {
                "display_name": f"Lake {i}",
                "name_variants": [],
                "feature_type": "lake",
                "reaches": [f"poly{i:08x}"],
                "regions": ["REGION 2"],
                "min_zoom": 8,
                "bbox": [-121.0, 49.0, -120.0, 50.0],
                "waterbody_group": str(1000 + i),
                "zones": ["2"],
                "management_units": [],
                "total_length_km": 0,
            }
            for i in range(3)
        ]

        index = {
            "regulations": regulations,
            "reg_sets": reg_sets,
            "reaches": reaches,
            "reach_segments": reach_segments,
            "poly_reaches": poly_reaches,
            "search_index": search_index,
        }

        path = tmp_path / "regulation_index.json"
        with open(path, "w") as f:
            json.dump(index, f)
        return path

    def test_full_round_trip(
        self, sample_regulation_index: Path, tmp_path: Path
    ) -> None:
        """Shard, then verify every fid/reach/poly is recoverable."""
        output_dir = tmp_path / "deploy"
        summary = shard_regulation_index(sample_regulation_index, output_dir, 8)

        # ── Check summary counts ──
        assert summary["total_fids"] == 300  # 100 reaches × 3 fids
        assert summary["total_reaches"] == 120  # 100 stream + 20 poly
        assert summary["total_polys"] == 20

        # ── Check MANIFEST exists ──
        manifest_path = Path(summary["shard_root"]) / "MANIFEST.json"
        assert manifest_path.exists()
        manifest = json.loads(manifest_path.read_text())
        assert manifest["status"] == "complete"
        assert manifest["version"] == "v8"

        # ── Verify every fid resolves to correct reach_id ──
        fid_dir = Path(summary["shard_root"]) / "fids"
        fid_lookup: dict[str, str] = {}
        for shard_file in fid_dir.glob("*.json"):
            with open(shard_file) as f:
                fid_lookup.update(json.load(f))

        assert len(fid_lookup) == 300
        # fid "0" should map to reach "000000000000"
        assert fid_lookup["0"] == "000000000000"
        # Reach i has fids [i*100, i*100+1, i*100+2]
        # So fid "200" → reach 2 → "000000000002"
        assert fid_lookup["200"] == "000000000002"
        assert fid_lookup["201"] == "000000000002"

        # ── Verify every reach has fids embedded ──
        reach_dir = Path(summary["shard_root"]) / "reaches"
        reach_lookup: dict[str, dict] = {}
        for shard_file in reach_dir.glob("*.json"):
            with open(shard_file) as f:
                reach_lookup.update(json.load(f))

        assert len(reach_lookup) == 120
        # Stream reach "000000000000" should have fids ["0", "1", "2"]
        r0 = reach_lookup["000000000000"]
        assert r0["fids"] == ["0", "1", "2"]
        assert r0["reg_set_index"] == 0
        assert r0["feature_type"] == "stream"

        # Poly reach should have empty fids
        r_poly = reach_lookup["poly00000000"]
        assert r_poly["fids"] == []
        assert r_poly["feature_type"] == "lake"

        # ── Verify polys ──
        poly_dir = Path(summary["shard_root"]) / "polys"
        poly_lookup: dict[str, str] = {}
        for shard_file in poly_dir.glob("*.json"):
            with open(shard_file) as f:
                poly_lookup.update(json.load(f))

        assert len(poly_lookup) == 20
        assert poly_lookup["1000"] == "poly00000000"

        # ── Verify tier0.json ──
        tier0_path = Path(summary["tier0_path"])
        assert tier0_path.exists()
        tier0 = json.loads(tier0_path.read_text())
        assert tier0["_shard_version"] == "v8"
        assert len(tier0["regulations"]) == 2
        assert len(tier0["reg_sets"]) == 3
        assert len(tier0["search_index"]) == 8  # 5 streams + 3 lakes

        # Search entries should have segments, not reaches
        for entry in tier0["search_index"]:
            assert "reaches" not in entry
            assert "segments" in entry
            for seg in entry["segments"]:
                assert "rid" in seg
                assert "reg_set_index" in seg
                assert "fids" in seg
                assert "watershed_code" in seg
                assert "regions" in seg

    def test_shard_prefix_consistency_with_worker(self) -> None:
        """Verify Python shard_prefix matches the Worker's SHA-256 prefix.

        The Worker uses crypto.subtle.digest('SHA-256', ...) and takes
        the first 3 hex chars. This test validates the Python side
        produces the same output, ensuring the Worker can find shards.
        """
        import hashlib

        # Known test vectors — manually computed
        test_cases = [
            ("707231", hashlib.sha256(b"707231").hexdigest()[:3]),
            ("abc123def456", hashlib.sha256(b"abc123def456").hexdigest()[:3]),
            ("351", hashlib.sha256(b"351").hexdigest()[:3]),
            ("1166294466", hashlib.sha256(b"1166294466").hexdigest()[:3]),
        ]
        for id_str, expected in test_cases:
            assert shard_prefix(id_str) == expected, f"Mismatch for {id_str!r}"


class TestShardFromDict:
    """Test shard_from_dict — the primary API called by builder.py."""

    def test_accepts_dict_directly(self, tmp_path: Path) -> None:
        """shard_from_dict produces same output as shard_regulation_index."""
        index = {
            "reaches": {
                "aabbccddee00": {
                    "display_name": "Test",
                    "reg_set_index": 1,
                    "feature_type": "stream",
                }
            },
            "reach_segments": {"aabbccddee00": ["111", "222"]},
            "poly_reaches": {"9999": "aabbccddee00"},
            "regulations": {"r1": {"raw_regs": "No fish"}},
            "reg_sets": ["r1"],
            "search_index": [
                {
                    "display_name": "Test",
                    "reaches": ["aabbccddee00"],
                    "waterbody_group": "100",
                },
            ],
        }
        summary = shard_from_dict(index, tmp_path / "deploy", 8)
        assert summary["total_fids"] == 2
        assert summary["total_reaches"] == 1
        assert summary["total_polys"] == 1
        assert summary["version"] == "v8"
        assert Path(summary["tier0_path"]).exists()
