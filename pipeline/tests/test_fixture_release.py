"""P11: the app's FIXTURE RELEASE is the release builder's bundle half over the dev fixture bundle — no data needed.

It is what `app/conformance/src/release.test.ts` runs the conformance suite against, so it must be the builder's output
(the same functions `build` calls), deterministic, and only the files it can honestly make.
"""
from __future__ import annotations

import gzip
import json
import sqlite3
from pathlib import Path

from pipeline.deliver.release import build as B
from pipeline.deliver.release import paint as P
from pipeline.tools import fixture_release as FR

DEV = Path(__file__).resolve().parents[2] / "app" / "packages" / "data" / "dev" / "bundle.sqlite"


def _raw(rel: Path, entry: dict) -> bytes:
    return gzip.decompress((rel.parent / entry["url"]).read_bytes())


def test_the_bundle_half_only_paired_to_one_derived_handle_table(tmp_path):
    rel = FR.build(DEV, tmp_path, valid_until="2027-03-31")
    man = json.loads((rel / "manifest.json").read_text())
    assert sorted(man["files"]) == ["conditions", "core", "flow-area", "near", "paint"]
    handles = FR.hex16("fixture-588ceed04ef45f27")
    assert man["tiles"]["compatible"] == [f"{handles}.*"]
    assert man["regulations"]["valid_until"] == "2027-03-31"
    for name, e in man["files"].items():
        assert e["section_handles"] == handles and e["format"] == B.FORMATS[name]
        raw = _raw(rel, e)
        assert len(raw) == e["bytes"]
        if name in ("core", "near", "conditions"):
            p = tmp_path / f"{name}.sqlite"
            p.write_bytes(raw)
            meta = dict(sqlite3.connect(p).execute("SELECT k, v FROM meta"))
            assert (meta["format"], meta["section_handles"]) == (B.FORMATS[name], handles)
    # the paint's flow half is the fixture bundle's panels, row for row
    d = P.decode(_raw(rel, man["files"]["paint"]))
    b = sqlite3.connect(DEV)
    assert d["section_panel"] == [tuple(r) for r in b.execute("SELECT sid, panel_id FROM section_panel ORDER BY sid")]


def test_two_builds_are_byte_identical(tmp_path):
    a = FR.build(DEV, tmp_path / "a")
    c = FR.build(DEV, tmp_path / "c")
    assert a.name == c.name
    assert sorted(p.name for p in a.iterdir()) == sorted(p.name for p in c.iterdir())
    for p in a.iterdir():
        assert p.read_bytes() == (c / p.name).read_bytes(), p.name


def test_hex16_keeps_a_real_digest_and_derives_one_otherwise():
    assert FR.hex16("d768d6e135e62b99") == "d768d6e135e62b99"
    assert FR.hex16("") == "e3b0c44298fc1c14"
