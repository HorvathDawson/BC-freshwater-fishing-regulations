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


# ---- P07: report_all — every artifact of a delivery against the bundle's digests ------------------------------

import hashlib

import pytest

from pipeline.common.vintage import Artifacts, report_all

H, REACH, RIDS = "147b20dce7d8576c", "ad6279cad5f3fc76", "16be720be08b0fca"


def _delivery(d: Path, *, handles=H, reach=REACH, store_handles=None, answers_text='x', stamp_ok=True,
              fmt="bundle/1") -> Artifacts:
    """One delivery's artifacts in `d`, every digest agreeing unless told otherwise."""
    from pipeline.deliver import status_index as SI
    from pipeline.deliver.answers.card_check import STAMP_NAME
    from pipeline.deliver.bundle.build import BUNDLE_FORMAT, schema_sha256
    d.mkdir(parents=True, exist_ok=True)
    _tiles(d / "tiles", handles)
    (d / "tiles" / "atlas.meta.json").write_text(json.dumps({"section_handles": handles, "registry": "reg1"}))
    db = sqlite3.connect(d / "bundle.sqlite")
    db.execute("CREATE TABLE meta (k TEXT, v TEXT)")
    db.executemany("INSERT INTO meta VALUES (?, ?)", [
        ("section_handles", handles), ("reach_digest", reach), ("rule_ids_sha256", RIDS),
        ("format", fmt if fmt else BUNDLE_FORMAT), ("schema_sha256", schema_sha256()), ("registry_digest", "reg1")])
    db.commit()
    db.close()
    (d / "status_index.bin").write_bytes(SI.encode({"handles": handles, "reach_digest": reach, "profiles": [],
                                                    "sections": {}, "items": {}}))
    about = {"bundle": {"section_handles": handles, "reach_digest": reach}}
    from pipeline.deliver.answers.encode import rule_ids_digest
    rule_ids = ["a", "b"]
    rids = rule_ids_digest(rule_ids)
    (d / "ui-rules-export.json").write_text(json.dumps({"about": about, "rule_ids": rule_ids}))
    (d / "ui-rules-guide.json").write_text(json.dumps({"about": about}))
    ans = json.dumps({"about": {**about, "export": {"rule_ids_sha256": rids}}, "x": answers_text}).encode()
    (d / "ui-rules-answers.json").write_bytes(ans)
    sha = hashlib.sha256(ans).hexdigest()
    (d / STAMP_NAME).write_text(json.dumps({"card_check": {"problems": 0 if stamp_ok else 3, "answers_sha256": sha,
                                                           "parts_checked": 5}}))
    st = sqlite3.connect(d / "regs.sqlite")
    st.execute("CREATE TABLE meta (k TEXT, v TEXT)")
    st.executemany("INSERT INTO meta VALUES (?, ?)", [
        ("section_handles", store_handles or handles), ("reach_digest", reach), ("rule_ids_sha256", rids),
        ("answers_sha256", sha)])
    st.commit()
    st.close()
    # the bundle's rule_ids digest is the export's (they are cut from one bundle)
    db = sqlite3.connect(d / "bundle.sqlite")
    db.execute("UPDATE meta SET v = ? WHERE k = 'rule_ids_sha256'", (rids,))
    db.commit()
    db.close()
    return Artifacts.in_dir(d)


def test_report_all_passes_a_delivery_whose_every_digest_agrees(tmp_path: Path):
    ok, msg = report_all(tmp_path / "tiles", _delivery(tmp_path), strict=True)
    assert ok, msg


@pytest.mark.parametrize("what, kw, says", [
    ("the store from another atlas", {"store_handles": "0000000000000000"}, "regs.sqlite"),
    ("a stamp with problems", {"stamp_ok": False}, "card-check.json"),
    ("an older bundle format", {"fmt": "bundle/0"}, "format"),
])
def test_report_all_refuses_one_artifact_out_of_step(tmp_path: Path, what, kw, says):
    ok, msg = report_all(tmp_path / "tiles", _delivery(tmp_path, **kw), strict=True)
    assert not ok and says in msg, (what, msg)


def test_report_all_refuses_answers_changed_after_the_store_and_the_gate(tmp_path: Path):
    arts = _delivery(tmp_path)
    arts.answers.write_bytes(arts.answers.read_bytes() + b" ")
    ok, msg = report_all(tmp_path / "tiles", arts, strict=True)
    assert not ok and "answers_sha256" in msg


def test_report_all_strict_refuses_a_missing_artifact_lenient_notes_it(tmp_path: Path):
    arts = _delivery(tmp_path)
    arts.store.unlink()
    assert not report_all(tmp_path / "tiles", arts, strict=True)[0]
    ok, msg = report_all(tmp_path / "tiles", arts, strict=False)
    assert ok and "not checked" in msg
