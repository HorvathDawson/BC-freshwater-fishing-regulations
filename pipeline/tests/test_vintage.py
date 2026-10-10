"""Tiles and bundle from one atlas — the check, and why "no digest" is not "no opinion".

A section is an integer handle into the atlas's `section_handles.txt`, carried by both the
tile and the bundle. A mixed pair does not fail to match: it matches the WRONG SECTION, so
every lookup succeeds and every answer is about a different river.

That shipped once. The tiles were rebuilt with handles while `bundle.sqlite` was left behind
keying sections by string, and the Conditions map painted every river as unmeasured — a state
the app draws on purpose, so it read as a slow feed. Nothing errored anywhere, which is why
the check has to be a build step rather than a habit.
"""

from __future__ import annotations

import json
import sqlite3
from pathlib import Path

from pipeline.common.vintage import bundle_vintage, report, tile_vintage


def _tiles(d: Path, digest: str | None) -> Path:
    d.mkdir(parents=True, exist_ok=True)
    (d / "atlas.pmtiles").write_bytes(b"not really an archive")
    if digest is not None:
        (d / "atlas.meta.json").write_text(json.dumps({"section_handles": digest}))
    return d


def _bundle(p: Path, digest: str | None) -> Path:
    db = sqlite3.connect(p)
    db.execute("CREATE TABLE meta (k TEXT, v TEXT)")
    if digest is not None:
        db.execute("INSERT INTO meta VALUES ('section_handles', ?)", (digest,))
    db.commit()
    db.close()
    return p


def test_a_matching_pair_passes(tmp_path: Path):
    ok, msg = report(_tiles(tmp_path / "t", "abc123"),
                     _bundle(tmp_path / "b.sqlite", "abc123"))
    assert ok and "agree" in msg


def test_two_different_digests_fail(tmp_path: Path):
    ok, msg = report(_tiles(tmp_path / "t", "abc123"),
                     _bundle(tmp_path / "b.sqlite", "def456"))
    assert not ok
    assert "MISMATCH" in msg and "abc123" in msg and "def456" in msg


def test_a_bundle_with_NO_digest_against_tiles_that_have_one_FAILS(tmp_path: Path):
    """THE ACTUAL FAILURE, and the case an earlier version of this let through.

    Treating a missing digest as "nothing to compare" is the same mistake as treating a Map
    miss as "no data": an artifact with no digest predates the handle table, so it cannot be
    the partner of one that has it.
    """
    ok, msg = report(_tiles(tmp_path / "t", "abc123"),
                     _bundle(tmp_path / "b.sqlite", None))
    assert not ok
    assert "predates the section handle table" in msg


def test_tiles_with_no_digest_against_a_bundle_that_has_one_FAILS(tmp_path: Path):
    ok, msg = report(_tiles(tmp_path / "t", None),
                     _bundle(tmp_path / "b.sqlite", "abc123"))
    assert not ok


def test_neither_having_a_digest_is_not_a_mismatch(tmp_path: Path):
    """Both predate handles — an old but self-consistent pair."""
    ok, _ = report(_tiles(tmp_path / "t", None), _bundle(tmp_path / "b.sqlite", None))
    assert ok


def test_an_unbuilt_artifact_is_not_a_mismatch(tmp_path: Path):
    """A checkout that has not produced both is not a fault, and must not fail a build."""
    ok, msg = report(tmp_path / "no-tiles-here", _bundle(tmp_path / "b.sqlite", "abc123"))
    assert ok and "not been built" in msg
    ok, msg = report(_tiles(tmp_path / "t", "abc123"), tmp_path / "nothing.sqlite")
    assert ok and "not been built" in msg


def test_readers_return_none_rather_than_raising(tmp_path: Path):
    """A malformed sidecar or an unreadable bundle must not take a build down."""
    d = tmp_path / "t"
    d.mkdir()
    (d / "atlas.meta.json").write_text("{ not json")
    assert tile_vintage(d) is None
    assert tile_vintage(tmp_path / "absent") is None
    (tmp_path / "junk.sqlite").write_bytes(b"definitely not sqlite")
    assert bundle_vintage(tmp_path / "junk.sqlite") is None
    assert bundle_vintage(tmp_path / "absent.sqlite") is None


def test_strict_refuses_an_unbuilt_artifact_and_a_pair_without_digests(tmp_path: Path):
    """P2: the rebuild command's last stage has just built both, so "not checked" is a failure."""
    tiles, bundle = tmp_path / "tiles", tmp_path / "bundle.sqlite"
    tiles.mkdir()
    assert report(tiles, bundle)[0] is True
    assert report(tiles, bundle, strict=True)[0] is False
    (tiles / "atlas.pmtiles").write_bytes(b"x")
    sqlite3.connect(bundle).execute("CREATE TABLE meta (k TEXT, v TEXT)")
    assert report(tiles, bundle)[0] is True
    assert report(tiles, bundle, strict=True)[0] is False


def _pair(tmp_path: Path, tiles_meta: dict, bundle_meta: dict):
    tiles, bundle = tmp_path / "tiles", tmp_path / "bundle.sqlite"
    tiles.mkdir()
    (tiles / "atlas.pmtiles").write_bytes(b"x")
    (tiles / "atlas.meta.json").write_text(json.dumps(tiles_meta))
    db = sqlite3.connect(bundle)
    db.execute("CREATE TABLE meta (k TEXT, v TEXT)")
    db.executemany("INSERT INTO meta VALUES (?, ?)", list(bundle_meta.items()))
    db.commit()
    db.close()
    return tiles, bundle


def test_one_handle_table_two_registries_is_a_mismatch(tmp_path: Path):
    """P2: the registry pairs names and kinds; a sidecar rewrite leaves the handles alone."""
    tiles, bundle = _pair(tmp_path, {"section_handles": "a" * 16, "registry": "1" * 16},
                          {"section_handles": "a" * 16, "registry_digest": "2" * 16})
    ok, msg = report(tiles, bundle)
    assert not ok and "two registries" in msg


def test_matching_registries_pass_and_strict_wants_both(tmp_path: Path):
    tiles, bundle = _pair(tmp_path, {"section_handles": "a" * 16, "registry": "1" * 16},
                          {"section_handles": "a" * 16, "registry_digest": "1" * 16})
    assert report(tiles, bundle, strict=True)[0]
    (tiles / "atlas.meta.json").write_text(json.dumps({"section_handles": "a" * 16}))
    assert report(tiles, bundle)[0] and not report(tiles, bundle, strict=True)[0]
