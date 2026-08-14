"""Tests for the mobile SQLite bundler.

Covers:
  - Full build_mobile_sqlite round-trip (writes DB + gz + manifest)
  - fid → reach_id and wbk → reach_id tap resolution
  - reach payload embeds fids + bathymetry
  - FTS5 search over display_name, name_variants, and nearby_towns
  - manifest integrity (counts + sha256)
"""

from __future__ import annotations

import gzip
import hashlib
import json
import sqlite3
import tempfile
from pathlib import Path

import pytest

from pipeline.deploy.mobile_sharder import build_mobile_sqlite


@pytest.fixture
def sample_index() -> dict:
    """A small regulation index dict shaped like build_regulation_index output."""
    return {
        "regulations": {
            "R2_ALICE_LAKE": {"display_name": "Alice Lake", "text": "No fishing"},
        },
        "reg_sets": ["R2_ALICE_LAKE", ""],
        "reaches": {
            "abc123def456": {
                "display_name": "Alice Lake",
                "name_variants": [{"name": "Alice Lk", "source": "direct"}],
                "feature_type": "lake",
                "reg_set_index": 0,
                "watershed_code": "100-000",
                "min_zoom": 8,
                "regions": ["1"],
                "bbox": [-123.0, 49.0, -122.9, 49.1],
                "length_km": 0,
                "bathymetry": [{"pdf": "00045501.pdf", "title": "ALICE LAKE"}],
            },
        },
        "reach_segments": {"abc123def456": ["1001", "1002"]},
        "poly_reaches": {"55501": "abc123def456"},
        "search_index": [
            {
                "display_name": "Alice Lake",
                "name_variants": [{"name": "Alice Lk", "source": "direct"}],
                "reaches": ["abc123def456"],
                "feature_type": "lake",
                "regions": ["1"],
                "min_zoom": 8,
                "bbox": [-123.0, 49.0, -122.9, 49.1],
                "waterbody_group": "wbg1",
                "zones": [],
                "management_units": [],
                "total_length_km": 0,
                "nearby_towns": [
                    {"name": "Squamish", "km": 3.2},
                    {"name": "Brackendale", "km": 5.7},
                ],
            },
        ],
    }


def _open(summary: dict) -> sqlite3.Connection:
    return sqlite3.connect(summary["db_path"])


def test_build_creates_artifacts(sample_index: dict) -> None:
    with tempfile.TemporaryDirectory() as tmp:
        summary = build_mobile_sqlite(sample_index, tmp, 8)
        assert Path(summary["db_path"]).exists()
        assert Path(summary["gz_path"]).exists()
        assert Path(summary["manifest_path"]).exists()
        assert summary["version"] == "v8"
        assert "mobile/v8" in summary["db_path"].replace("\\", "/")


def test_fid_resolution(sample_index: dict) -> None:
    with tempfile.TemporaryDirectory() as tmp:
        summary = build_mobile_sqlite(sample_index, tmp, 8)
        conn = _open(summary)
        try:
            row = conn.execute(
                "SELECT reach_id FROM fids WHERE fid = ?", ("1001",)
            ).fetchone()
            assert row is not None
            assert row[0] == "abc123def456"
        finally:
            conn.close()


def test_poly_resolution(sample_index: dict) -> None:
    with tempfile.TemporaryDirectory() as tmp:
        summary = build_mobile_sqlite(sample_index, tmp, 8)
        conn = _open(summary)
        try:
            row = conn.execute(
                "SELECT reach_id FROM polys WHERE wbk = ?", ("55501",)
            ).fetchone()
            assert row is not None and row[0] == "abc123def456"
        finally:
            conn.close()


def test_reach_payload_has_fids_and_bathymetry(sample_index: dict) -> None:
    with tempfile.TemporaryDirectory() as tmp:
        summary = build_mobile_sqlite(sample_index, tmp, 8)
        conn = _open(summary)
        try:
            row = conn.execute(
                "SELECT json FROM reaches WHERE reach_id = ?", ("abc123def456",)
            ).fetchone()
            payload = json.loads(row[0])
            assert payload["fids"] == ["1001", "1002"]
            assert payload["bathymetry"][0]["pdf"] == "00045501.pdf"
        finally:
            conn.close()


def test_fts_matches_town(sample_index: dict) -> None:
    """A town query should surface the waterbody beside it via FTS."""
    with tempfile.TemporaryDirectory() as tmp:
        summary = build_mobile_sqlite(sample_index, tmp, 8)
        conn = _open(summary)
        try:
            rows = conn.execute(
                "SELECT s.display_name FROM search_fts f "
                "JOIN search s ON s.id = f.rowid "
                "WHERE search_fts MATCH ?",
                ("Squamish",),
            ).fetchall()
            assert any(r[0] == "Alice Lake" for r in rows)
        finally:
            conn.close()


def test_fts_matches_name_variant(sample_index: dict) -> None:
    with tempfile.TemporaryDirectory() as tmp:
        summary = build_mobile_sqlite(sample_index, tmp, 8)
        conn = _open(summary)
        try:
            rows = conn.execute(
                "SELECT s.display_name FROM search_fts f "
                "JOIN search s ON s.id = f.rowid WHERE search_fts MATCH ?",
                ("Lk",),
            ).fetchall()
            assert any(r[0] == "Alice Lake" for r in rows)
        finally:
            conn.close()


def test_search_row_carries_nearby_towns(sample_index: dict) -> None:
    with tempfile.TemporaryDirectory() as tmp:
        summary = build_mobile_sqlite(sample_index, tmp, 8)
        conn = _open(summary)
        try:
            row = conn.execute(
                "SELECT nearby_towns, segments FROM search WHERE display_name = ?",
                ("Alice Lake",),
            ).fetchone()
            assert json.loads(row[0]) == [
                {"name": "Squamish", "km": 3.2},
                {"name": "Brackendale", "km": 5.7},
            ]
            # Enriched segments embedded (same shape as tier0).
            segments = json.loads(row[1])
            assert segments[0]["rid"] == "abc123def456"
            assert segments[0]["fids"] == ["1001", "1002"]
        finally:
            conn.close()


def test_manifest_integrity(sample_index: dict) -> None:
    with tempfile.TemporaryDirectory() as tmp:
        summary = build_mobile_sqlite(sample_index, tmp, 8)
        manifest = json.loads(Path(summary["manifest_path"]).read_text())
        assert manifest["version"] == "v8"
        assert manifest["counts"]["fids"] == 2
        assert manifest["counts"]["search"] == 1
        # sha256 in manifest matches the actual db file.
        actual = hashlib.sha256(Path(summary["db_path"]).read_bytes()).hexdigest()
        assert manifest["files"][0]["sha256"] == actual


def test_gzip_roundtrips_to_valid_sqlite(sample_index: dict) -> None:
    with tempfile.TemporaryDirectory() as tmp:
        summary = build_mobile_sqlite(sample_index, tmp, 8)
        with gzip.open(summary["gz_path"], "rb") as f:
            decompressed = f.read()
        assert decompressed == Path(summary["db_path"]).read_bytes()


def test_rebuild_is_idempotent(sample_index: dict) -> None:
    """Rebuilding into the same dir must not duplicate rows (fresh file each run)."""
    with tempfile.TemporaryDirectory() as tmp:
        build_mobile_sqlite(sample_index, tmp, 8)
        summary = build_mobile_sqlite(sample_index, tmp, 8)
        conn = _open(summary)
        try:
            (fid_count,) = conn.execute("SELECT COUNT(*) FROM fids").fetchone()
            assert fid_count == 2
        finally:
            conn.close()
